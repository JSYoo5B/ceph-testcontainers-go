package mirror

import (
	"context"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
)

func TestSerializedMirrorMutationQueuedDeadline(t *testing.T) {
	probe := &clustertest.ContextLockRuntime{}
	daemon := &Daemon{Container: probe, DaemonName: "a"}
	m := &Mirror{Container: daemon, daemons: []*Daemon{daemon}, source: nil}
	operations := map[string]func(context.Context) error{
		"AddDaemon":            func(ctx context.Context) error { _, err := m.AddDaemon(ctx, "queued"); return err },
		"RemoveDaemon":         func(ctx context.Context) error { return m.RemoveDaemon(ctx, "a") },
		"AttachManagers":       m.AttachManagers,
		"AddDirectory":         func(ctx context.Context) error { return m.AddDirectory(ctx, "/queued") },
		"RemoveDirectory":      func(ctx context.Context) error { return m.RemoveDirectory(ctx, "/queued") },
		"RebalanceDirectories": m.RebalanceDirectories,
		"PeerIDs":              func(ctx context.Context) error { _, err := m.PeerIDs(ctx); return err },
		"RemovePeer":           func(ctx context.Context) error { return m.RemovePeer(ctx, "e8dded8c-93a6-4c12-ab99-5b429cdba6c5") },
		"RebootstrapPeer":      func(ctx context.Context) error { _, err := m.RebootstrapPeer(ctx); return err },
		"InterruptPeerLink":    func(ctx context.Context) error { _, err := m.InterruptPeerLink(ctx, "a"); return err },
		"Terminate":            func(ctx context.Context) error { return m.Terminate(ctx) },
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			clustertest.AssertContextLockQueued(t, &m.mu, operation)
			if probe.Execs.Load() != 0 || probe.Stops.Load() != 0 || probe.Terminations.Load() != 0 || m.closed || daemon.removed {
				t.Fatal("queued caller changed native runtime, membership or fixture lifecycle")
			}
		})
	}
	// A new context must enter the same fixture and reach real cleanup.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := m.RemoveDaemon(ctx, "a"); err != nil || len(m.daemons) != 0 || !daemon.removed {
		t.Fatalf("fresh CephFS cleanup: %v", err)
	}
	if probe.Terminations.Load() != 1 || probe.Execs.Load() != 0 || probe.Stops.Load() != 0 {
		t.Fatal("fresh cleanup did not terminate exactly its owned runtime")
	}
}

func TestSerializedDaemonCleanupNestedGateDeadlinePreservesRetry(t *testing.T) {
	probe := &clustertest.ContextLockRuntime{}
	daemon := &Daemon{Container: probe, DaemonName: "a"}
	m := &Mirror{daemons: []*Daemon{daemon}}
	operation := func(ctx context.Context) error { return m.RemoveDaemon(ctx, "a") }
	clustertest.AssertContextLockQueued(t, &daemon.mu, operation)
	if probe.Terminations.Load() != 0 || daemon.removed || len(m.daemons) != 1 {
		t.Fatal("nested deadline lost cleanup ownership")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := operation(ctx); err != nil || probe.Terminations.Load() != 1 {
		t.Fatalf("nested cleanup could not retry with fresh context: %v", err)
	}
}
