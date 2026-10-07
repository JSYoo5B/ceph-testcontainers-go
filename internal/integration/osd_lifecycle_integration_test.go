//go:build integration && features

package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// Execute the real purge before losing its reply, then create a foreign native
// registration with another owned ID. These exercise cleanup retry and UUID
// rejection independently while preserving the same client data.
func TestOSDRemovalLifecycle(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
			defer cancel()
			opts := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(3)}
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, client := newServiceCluster(t, opts...)
			const pool = "tc-osd-removal"
			if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rados", Replicas: 2, MinSize: 1, PGNum: 8}); err != nil {
				t.Fatal(err)
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			for i := range 8 {
				payload := bytes.Repeat([]byte{byte(i + 1)}, 32<<10)
				if err := client.CopyToContainer(ctx, payload, "/tmp/osd-removal-payload", 0o600); err != nil {
					t.Fatal(err)
				}
				execCommand(t, ctx, client, "rados", "-p", pool, "put", fmt.Sprintf("object-%d", i), "/tmp/osd-removal-payload")
			}
			verify := func() {
				t.Helper()
				for i := range 8 {
					execCommand(t, ctx, client, "rados", "-p", pool, "get", fmt.Sprintf("object-%d", i), "/tmp/osd-removal-result")
					reader, err := client.CopyFileFromContainer(ctx, "/tmp/osd-removal-result")
					if err != nil {
						t.Fatal(err)
					}
					data, readErr := io.ReadAll(reader)
					reader.Close()
					if readErr != nil || !bytes.Equal(data, bytes.Repeat([]byte{byte(i + 1)}, 32<<10)) {
						t.Fatalf("retained object-%d changed: bytes=%d error=%v", i, len(data), readErr)
					}
				}
			}
			verify()
			initial, err := cluster.OSDStates(ctx)
			if err != nil {
				t.Fatal(err)
			}
			original := cluster.OSDs()[0]
			originalCalls := &osdRemovalContainerCalls{Container: original.Container}
			original.Container = originalCalls
			// Single-MON fixtures use the embedded handle for their CLI, allowing
			// a response fault without replacing any Docker/native resource.
			if cluster.ControlContainer() != cluster.Container {
				t.Fatal("expected the initial MON to provide the control CLI")
			}
			fault := &osdRemovalPurgeReplyFault{Container: cluster.Container, id: original.ID}
			cluster.Container = fault
			if err := cluster.RemoveOSD(ctx, original.ID); !errors.Is(err, errOSDPurgeReplyLost) {
				t.Fatalf("real purge did not retain its lost reply error: %v", err)
			}
			if fault.purges.Load() != 1 || len(cluster.OSDs()) != 3 {
				t.Fatal("uncertain purge lost cleanup ownership or repeated the purge")
			}
			actions, stops, terminations := fault.mutations.Load(), originalCalls.stops.Load(), originalCalls.terminations.Load()
			if _, err := cluster.AddOSD(ctx); err == nil {
				t.Fatal("pending purge allowed OSD ID reuse before cleanup")
			}
			if err := cluster.RemoveOSD(ctx, original.ID); err != nil {
				t.Fatal("retry uncertain purge", err)
			}
			if fault.purges.Load() != 1 || len(cluster.OSDs()) != 2 {
				t.Fatal("purge retry repeated native purge or retained completed cleanup")
			}
			if fault.mutations.Load() != actions || originalCalls.stops.Load() != stops || originalCalls.terminations.Load() != terminations+1 {
				t.Fatal("pending Add or purge retry issued another native mutation/Stop or did not exclusively finish owned Docker cleanup")
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			verify()
			added, err := cluster.AddOSD(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			states, err := cluster.OSDStates(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if added.ID != original.ID || states[0].UUID == initial[0].UUID {
				t.Fatal("expected a newly owned registration to reuse the purged numeric ID with a different UUID")
			}
			verify()

			// This test actor explicitly removes a different OSD outside the API
			// and creates an unowned, down registration with its old number.
			foreignTarget := cluster.OSDs()[1]
			cephCommand(t, ctx, cluster, "osd", "crush", "reweight", fmt.Sprintf("osd.%d", foreignTarget.ID), "0")
			cephCommand(t, ctx, cluster, "osd", "out", strconv.Itoa(foreignTarget.ID))
			if err := cluster.WaitForPGClean(ctx); err != nil {
				t.Fatal(err)
			}
			safetyCtx, safetyCancel := context.WithTimeout(ctx, 30*time.Second)
			for {
				_, err := cluster.Ceph(safetyCtx, "osd", "safe-to-destroy", strconv.Itoa(foreignTarget.ID))
				if err == nil {
					break
				}
				select {
				case <-safetyCtx.Done():
					safetyCancel()
					t.Fatal("test actor did not observe safe-to-destroy", errors.Join(safetyCtx.Err(), err))
				case <-time.After(500 * time.Millisecond):
				}
			}
			safetyCancel()
			grace := 5 * time.Second
			if err := foreignTarget.Stop(ctx, &grace); err != nil {
				t.Fatal(err)
			}
			waitOSDPolicyState(t, ctx, cluster, foreignTarget.ID, func(state ceph.OSDState) bool { return !state.Up })
			cephCommand(t, ctx, cluster, "osd", "purge", strconv.Itoa(foreignTarget.ID), "--yes-i-really-mean-it")
			foreignUUID := uuid.NewString()
			cephCommand(t, ctx, cluster, "osd", "create", foreignUUID, strconv.Itoa(foreignTarget.ID))
			before, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
			if err != nil || !bytes.Contains(before, []byte(foreignUUID)) {
				t.Fatal("foreign OSD registration was not independently observed", err)
			}
			mutations := fault.mutations.Load()
			foreignCalls := &osdRemovalContainerCalls{Container: foreignTarget.Container}
			foreignTarget.Container = foreignCalls
			if err := cluster.RemoveOSD(ctx, foreignTarget.ID); err == nil || !strings.Contains(err.Error(), "UUID") {
				t.Fatalf("foreign same-ID OSD was not rejected by UUID: %v", err)
			}
			after, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
			if err != nil || !bytes.Equal(before, after) || fault.mutations.Load() != mutations || foreignCalls.stops.Load() != 0 || foreignCalls.terminations.Load() != 0 {
				t.Fatal("foreign OSD rejection changed the native map or issued a mutation", err)
			}
			if len(cluster.OSDs()) != 3 {
				t.Fatal("foreign OSD rejection lost the old owned container cleanup handle")
			}
			verify()
			t.Log("real purge reply loss reconciled without another purge; pending ID reuse refused; fresh owned UUID and foreign same-ID rejection verified with eight retained 32KiB objects")
		})
	}
}

type osdRemovalContainerCalls struct {
	testcontainers.Container
	stops, terminations atomic.Int32
}

func (calls *osdRemovalContainerCalls) Stop(ctx context.Context, timeout *time.Duration) error {
	calls.stops.Add(1)
	return calls.Container.Stop(ctx, timeout)
}

func (calls *osdRemovalContainerCalls) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	calls.terminations.Add(1)
	return calls.Container.Terminate(ctx, opts...)
}

var errOSDPurgeReplyLost = errors.New("injected lost native purge reply")

type osdRemovalPurgeReplyFault struct {
	testcontainers.Container
	id        int
	lost      atomic.Bool
	purges    atomic.Int32
	mutations atomic.Int32
}

func (fault *osdRemovalPurgeReplyFault) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	command := strings.Join(args, " ")
	isPurge := strings.Contains(command, " osd purge "+strconv.Itoa(fault.id)+" ")
	if strings.Contains(command, " osd crush reweight ") || strings.Contains(command, " osd out ") || strings.Contains(command, " osd purge ") || strings.Contains(command, " osd create ") || strings.Contains(command, " osd new ") {
		fault.mutations.Add(1)
	}
	code, reader, err := fault.Container.Exec(ctx, args, opts...)
	if isPurge && err == nil && code == 0 {
		fault.purges.Add(1)
		if fault.lost.CompareAndSwap(false, true) {
			return 0, nil, errOSDPurgeReplyLost
		}
	}
	return code, reader, err
}
