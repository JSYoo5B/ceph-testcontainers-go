//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_cluster_fixtures))))

//ci: timeout=40m job-timeout=50 image-roles=essential,client,cephfs,rgw

package integration_test

import (
	"context"
	"errors"
	"io"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// heldNativeWrite inserts an independently released FIFO before a real CLI
// mutation. Docker Exec and Ceph both run normally after the FIFO is released.
type heldNativeWrite struct {
	testcontainers.Container
	path, verb string
	reads      atomic.Int64
}

func (d *heldNativeWrite) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if len(args) > 4 && slices.Equal(args[3:5], []string{"config", "dump"}) {
		d.reads.Add(1)
	}
	if len(args) > 4 && slices.Equal(args[3:5], []string{"config", d.verb}) {
		args = append([]string{"sh", "-c", `printf ready > "$1.ready"; read line < "$1"; shift; exec "$@"`, "sh", d.path}, args...)
	}
	return d.Container.Exec(ctx, args, opts...)
}

func TestOverrideWaitsForNativeMutationCompletion(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	cluster, client := harness.NewServiceCluster(t)
	// The facade substitutes only the transport so the real cluster remains
	// independently available for observations and normal fixture cleanup.
	path := "/tmp/tc-override-" + uuid.NewString()
	exec := func(args ...string) {
		t.Helper()
		code, _, err := client.Exec(ctx, args)
		if err != nil || code != 0 {
			t.Fatalf("control: exit %d: %v", code, err)
		}
	}
	exec("mkfifo", path)
	d := &heldNativeWrite{Container: client, path: path, verb: "set"}
	facade := testhelper.TestCluster(d, testhelper.TestConfig{StartupTimeout: time.Minute})
	setting := ceph.ConfigSetting{Section: "client.pending", Name: "debug_ms", Value: "1/1"}
	callCtx, stop := context.WithCancel(ctx)
	var handle *override.ConfigHandle
	done := make(chan error, 1)
	go func() {
		var err error
		handle, err = override.SetConfig(callCtx, facade, setting)
		done <- err
	}()
	until := time.Now().Add(time.Minute)
	for {
		code, _, err := client.Exec(ctx, []string{"test", "-f", path + ".ready"})
		if err == nil && code == 0 {
			break
		}
		if time.Now().After(until) {
			t.Fatal("native CLI did not reach FIFO")
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("apply cancellation: %v", err)
	}
	beforeReads := d.reads.Load()
	short, end := context.WithTimeout(ctx, 2*time.Second)
	if err := handle.Restore(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unfinished mutation accepted: %v", err)
	}
	end()
	if d.reads.Load() != beforeReads {
		t.Fatal("restoration read native state before mutation completion")
	}
	entries, err := harness.ObserveCluster(cluster).Configuration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Section == setting.Section && entry.Name == setting.Name {
			t.Fatal("held write already visible")
		}
	}
	if _, err := override.SetConfig(ctx, facade, setting); err == nil {
		t.Fatal("successor admitted while original write is pending")
	}
	exec("sh", "-c", `printf 'continue\n' > "$1"`, "sh", path)
	testhelper.TestSetControlPlane(facade, client)
	if err := handle.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	entries, err = harness.ObserveCluster(cluster).Configuration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Section == setting.Section && entry.Name == setting.Name {
			t.Fatal("restored entry remains")
		}
	}
	successor, err := override.SetConfig(ctx, facade, setting)
	if err != nil {
		t.Fatal(err)
	}
	if err := successor.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	exec("rm", "-f", path, path+".ready")
}
