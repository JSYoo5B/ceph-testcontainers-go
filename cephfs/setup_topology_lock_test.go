package cephfs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/testcontainers/testcontainers-go"
)

// setupTopologyOps are the four operations that take the setup gate and then
// the topology mutex before their own ownership predicate runs: filesystem
// removal, MDS scaling, stopped-MDS removal and MDS replacement. Each runs the
// real function over the replacement fixture.
func setupTopologyOps(r *replacementMDSFixture) map[string]func(context.Context) error {
	return map[string]func(context.Context) error{
		"remove-filesystem": func(ctx context.Context) error { return removeFilesystem(ctx, r.fs.cluster, r.fs) },
		"scale-mds":         func(ctx context.Context) error { return r.fs.ScaleMDS(ctx, 1, 0) },
		"remove-stopped":    func(ctx context.Context) error { return r.fs.removeStoppedMDS(ctx, r.old) },
		"replace-mds": func(ctx context.Context) error {
			_, err := r.fs.addMDSReplacement(ctx, r.old, func(context.Context, string, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
				return nil, errors.New("must not start")
			})
			return err
		},
	}
}

func setupGateFree(r *replacementMDSFixture) bool {
	mu := &filesystems(r.fs.cluster).setupMu
	if !mu.TryLock() {
		return false
	}
	mu.Unlock()
	return true
}

func topologyFree(t *testing.T, r *replacementMDSFixture) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := servicehook.LockTopology(ctx, r.fs.cluster); err != nil {
		return false
	}
	servicehook.Unlock(r.fs.cluster)
	return true
}

// TestSetupThenTopologyAcquisitionKeepsItsOrderAndRelease characterizes the
// shared prefix of the four operations: a held setup gate stops the operation
// before the topology mutex is touched, a held topology mutex stops it after
// the setup gate was taken and the gate is released again, a deadline is
// reported as the context error, and after an operation that reaches its own
// refusal neither lock is retained. It is a Docker-free control, not native
// evidence.
func TestSetupThenTopologyAcquisitionKeepsItsOrderAndRelease(t *testing.T) {
	names := []string{"remove-filesystem", "scale-mds", "remove-stopped", "replace-mds"}
	for _, name := range names {
		t.Run(name+"/setup-gate-held", func(t *testing.T) {
			r := newReplacementMDSFixture(t)
			mu := &filesystems(r.fs.cluster).setupMu
			mu.Lock()
			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			err := setupTopologyOps(r)[name](ctx)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("held setup gate: want the deadline error, got %v", err)
			}
			if !topologyFree(t, r) {
				t.Fatal("the topology mutex was taken or retained while the setup gate was held elsewhere")
			}
			mu.Unlock()
			if !setupGateFree(r) {
				t.Fatal("the setup gate is not free after its holder released it")
			}
		})
		t.Run(name+"/topology-held", func(t *testing.T) {
			r := newReplacementMDSFixture(t)
			if err := servicehook.LockTopology(t.Context(), r.fs.cluster); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			err := setupTopologyOps(r)[name](ctx)
			servicehook.Unlock(r.fs.cluster)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("held topology mutex: want the deadline error, got %v", err)
			}
			if !setupGateFree(r) {
				t.Fatal("the setup gate was retained after the topology mutex could not be taken")
			}
			if !topologyFree(t, r) {
				t.Fatal("the topology mutex is not free after its holder released it")
			}
		})
		t.Run(name+"/canceled-before-start", func(t *testing.T) {
			r := newReplacementMDSFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if err := setupTopologyOps(r)[name](ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled context: want the cancellation error, got %v", err)
			}
			if !setupGateFree(r) || !topologyFree(t, r) {
				t.Fatal("a lock is retained after a canceled call")
			}
		})
	}
	t.Run("refusals-release-both-locks", func(t *testing.T) {
		r := newReplacementMDSFixture(t)
		stale := &Filesystem{cluster: r.fs.cluster, FilesystemName: "target", config: r.fs.config}
		refused := map[string]error{
			"remove-filesystem": removeFilesystem(t.Context(), r.fs.cluster, stale),
			"scale-mds":         stale.ScaleMDS(t.Context(), 1, 0),
		}
		for name, err := range refused {
			if err == nil {
				t.Fatalf("%s: a stale handle was admitted", name)
			}
			if !setupGateFree(r) || !topologyFree(t, r) {
				t.Fatalf("%s: a lock is retained after the refusal %v", name, err)
			}
		}
	})
}
