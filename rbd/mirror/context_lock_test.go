package mirror

import (
	"context"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
)

func TestSerializedMirrorMutationQueuedDeadline(t *testing.T) {
	probe := &clustertest.NativeCallCounter{}
	daemon := &Daemon{Container: probe, DaemonName: "a"}
	m := &Mirror{Container: daemon, daemons: []*Daemon{daemon}, initialDaemonName: "a", sourceClient: probe, destinationClient: probe}
	daemon.owner = m
	operations := map[string]func(context.Context) error{
		"AddDaemon":         func(ctx context.Context) error { _, err := m.AddDaemon(ctx, "queued"); return err },
		"Remove":      func(ctx context.Context) error { return daemon.Remove(ctx) },
		"Rebootstrap":       m.Rebootstrap,
		"EnableImage":       func(ctx context.Context) error { return m.EnableImage(ctx, "queued") },
		"PolicyStatus":      func(ctx context.Context) error { _, err := m.PolicyStatus(ctx); return err },
		"InterruptPeerLink": func(ctx context.Context) error { _, err := daemon.InterruptPeerLink(ctx); return err },
		"Terminate":         func(ctx context.Context) error { return m.Terminate(ctx) },
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			clustertest.AssertContextLockQueued(t, &m.mu, operation)
			if probe.Execs.Load() != 0 || probe.Stops.Load() != 0 || probe.Terminations.Load() != 0 || m.closed || daemon.terminated {
				t.Fatal("queued caller changed native runtime, membership or fixture lifecycle")
			}
		})
	}
	// A new context must enter the same fixture and reach real cleanup.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := daemon.Remove(ctx); err != nil || len(m.daemons) != 0 || !daemon.terminated {
		t.Fatalf("fresh RBD cleanup: %v", err)
	}
	if probe.Terminations.Load() != 1 || probe.Execs.Load() != 0 || probe.Stops.Load() != 0 {
		t.Fatal("fresh cleanup did not terminate exactly its owned runtime")
	}
}

func TestSerializedDaemonCleanupNestedGateDeadlinePreservesRetry(t *testing.T) {
	probe := &clustertest.NativeCallCounter{}
	daemon := &Daemon{Container: probe, DaemonName: "a"}
	m := &Mirror{daemons: []*Daemon{daemon}}
	daemon.owner = m
	operation := func(ctx context.Context) error { return daemon.Remove(ctx) }
	clustertest.AssertContextLockQueued(t, &daemon.mu, operation)
	if probe.Terminations.Load() != 0 || daemon.terminated || len(m.daemons) != 1 {
		t.Fatal("nested deadline lost cleanup ownership")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := operation(ctx); err != nil || probe.Terminations.Load() != 1 {
		t.Fatalf("nested cleanup could not retry with fresh context: %v", err)
	}
}
