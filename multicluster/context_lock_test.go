package multicluster

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type contextLockRuntime struct {
	testcontainers.Container
	execs, stops, terminations atomic.Int32
}

func (runtime *contextLockRuntime) GetContainerID() string { return "context-lock-runtime" }

func (runtime *contextLockRuntime) Exec(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
	runtime.execs.Add(1)
	return 1, nil, errors.New("unexpected native command while queued")
}
func (runtime *contextLockRuntime) Stop(context.Context, *time.Duration) error {
	runtime.stops.Add(1)
	return nil
}
func (runtime *contextLockRuntime) Terminate(ctx context.Context, _ ...testcontainers.TerminateOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	runtime.terminations.Add(1)
	return nil
}

func TestSerializedMulticlusterMutationQueuedDeadline(t *testing.T) {
	for _, family := range []string{"RBD", "CephFS", "RGW"} {
		t.Run(family, func(t *testing.T) {
			probe := &contextLockRuntime{}
			rbdDaemon := &RBDMirrorDaemon{Container: probe, DaemonName: "a"}
			rbd := &RBDMirror{Container: rbdDaemon, daemons: []*RBDMirrorDaemon{rbdDaemon}, initialDaemonName: "a", sourceClient: probe, destinationClient: probe}
			cephfsDaemon := &CephFSMirrorDaemon{Container: probe, DaemonName: "a"}
			cephfs := &CephFSMirror{Container: cephfsDaemon, daemons: []*CephFSMirrorDaemon{cephfsDaemon}, source: nil}
			rgw := &RGWMultisite{sourceClient: probe, destinationClient: probe}
			group := &RGWSyncGroup{owner: rgw, state: &rgwSyncGroupState{id: "queued"}}
			var mutex *sync.Mutex
			var operations map[string]func(context.Context) error
			switch family {
			case "RBD":
				mutex = &rbd.mu
				operations = map[string]func(context.Context) error{
					"AddDaemon":         func(ctx context.Context) error { _, err := rbd.AddDaemon(ctx, "queued"); return err },
					"RemoveDaemon":      func(ctx context.Context) error { return rbd.RemoveDaemon(ctx, "a") },
					"Rebootstrap":       rbd.Rebootstrap,
					"EnableImage":       func(ctx context.Context) error { return rbd.EnableImage(ctx, "queued") },
					"PolicyStatus":      func(ctx context.Context) error { _, err := rbd.PolicyStatus(ctx); return err },
					"InterruptPeerLink": func(ctx context.Context) error { _, err := rbd.InterruptPeerLink(ctx, "a"); return err },
					"Terminate":         func(ctx context.Context) error { return rbd.Terminate(ctx) },
				}
			case "CephFS":
				mutex = &cephfs.mu
				operations = map[string]func(context.Context) error{
					"AddDaemon":            func(ctx context.Context) error { _, err := cephfs.AddDaemon(ctx, "queued"); return err },
					"RemoveDaemon":         func(ctx context.Context) error { return cephfs.RemoveDaemon(ctx, "a") },
					"AttachManagers":       cephfs.AttachManagers,
					"AddDirectory":         func(ctx context.Context) error { return cephfs.AddDirectory(ctx, "/queued") },
					"RemoveDirectory":      func(ctx context.Context) error { return cephfs.RemoveDirectory(ctx, "/queued") },
					"RebalanceDirectories": cephfs.RebalanceDirectories,
					"PeerIDs":              func(ctx context.Context) error { _, err := cephfs.PeerIDs(ctx); return err },
					"RemovePeer":           func(ctx context.Context) error { return cephfs.RemovePeer(ctx, "e8dded8c-93a6-4c12-ab99-5b429cdba6c5") },
					"RebootstrapPeer":      func(ctx context.Context) error { _, err := cephfs.RebootstrapPeer(ctx); return err },
					"InterruptPeerLink":    func(ctx context.Context) error { _, err := cephfs.InterruptPeerLink(ctx, "a"); return err },
					"Terminate":            func(ctx context.Context) error { return cephfs.Terminate(ctx) },
				}
			case "RGW":
				mutex = &rgw.topologyMu
				operations = map[string]func(context.Context) error{
					"AddZone": func(ctx context.Context) error {
						_, err := rgw.AddZone(ctx, "unused", RGWZoneConfig{Name: "queued"})
						return err
					},
					"AddZonegroup": func(ctx context.Context) error {
						_, err := rgw.AddZonegroup(ctx, "unused", RGWZonegroupConfig{Name: "queued", Zones: []RGWZoneConfig{{Name: "queued"}}})
						return err
					},
					"RemoveZone": func(ctx context.Context) error { return rgw.RemoveZone(ctx, "queued") },
					"ZoneAdmin":  func(ctx context.Context) error { _, err := rgw.ZoneAdmin(ctx, "queued", "user", "create"); return err },
					"CreateSyncGroup": func(ctx context.Context) error {
						_, err := rgw.CreateSyncGroup(ctx, RGWSyncPolicyScope{}, RGWSyncGroupConfig{ID: "queued", Status: RGWSyncAllowed})
						return err
					},
					"RemoveSyncGroup":    func(ctx context.Context) error { return rgw.RemoveSyncGroup(ctx, group) },
					"ApplySyncGroup":     func(ctx context.Context) error { return rgw.ApplySyncGroup(ctx, group) },
					"SetSyncGroupStatus": func(ctx context.Context) error { return rgw.SetSyncGroupStatus(ctx, group, RGWSyncEnabled) },
					"InterruptZoneLink":  func(ctx context.Context) error { _, err := rgw.InterruptZoneLink(ctx, "queued"); return err },
					"Terminate":          func(ctx context.Context) error { return rgw.Terminate(ctx) },
				}
			}
			for name, operation := range operations {
				t.Run(name, func(t *testing.T) {
					assertMulticlusterContextLockQueued(t, mutex, operation)
					if probe.execs.Load() != 0 || probe.stops.Load() != 0 || probe.terminations.Load() != 0 || rbd.closed || cephfs.closed || rgw.closed || rbdDaemon.terminated || cephfsDaemon.removed {
						t.Fatal("queued caller changed native runtime, membership or fixture lifecycle")
					}
				})
			}
			// A new context must enter the same fixture and reach real cleanup.
			freshCtx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			switch family {
			case "RBD":
				if err := rbd.RemoveDaemon(freshCtx, "a"); err != nil || len(rbd.daemons) != 0 || !rbdDaemon.terminated {
					t.Fatalf("fresh RBD cleanup: %v", err)
				}
			case "CephFS":
				if err := cephfs.RemoveDaemon(freshCtx, "a"); err != nil || len(cephfs.daemons) != 0 || !cephfsDaemon.removed {
					t.Fatalf("fresh CephFS cleanup: %v", err)
				}
			case "RGW":
				rgw.owned.addContainer(probe)
				if err := rgw.Terminate(freshCtx); err != nil || !rgw.closed {
					t.Fatalf("fresh RGW cleanup: %v", err)
				}
			}
			if probe.terminations.Load() != 1 || probe.execs.Load() != 0 || probe.stops.Load() != 0 {
				t.Fatal("fresh cleanup did not terminate exactly its owned runtime")
			}
		})
	}
}

func TestSerializedCleanupNestedGateDeadlinePreservesRetry(t *testing.T) {
	for _, family := range []string{"RBD daemon", "CephFS daemon", "resources"} {
		t.Run(family, func(t *testing.T) {
			probe := &contextLockRuntime{}
			daemon := &RBDMirrorDaemon{Container: probe, DaemonName: "a"}
			rbd := &RBDMirror{daemons: []*RBDMirrorDaemon{daemon}}
			fsDaemon := &CephFSMirrorDaemon{Container: probe, DaemonName: "a"}
			cephfs := &CephFSMirror{daemons: []*CephFSMirrorDaemon{fsDaemon}}
			var owned resources
			owned.addContainer(probe)
			var mutex *sync.Mutex
			var operation func(context.Context) error
			switch family {
			case "RBD daemon":
				mutex = &daemon.mu
				operation = func(ctx context.Context) error { return rbd.RemoveDaemon(ctx, "a") }
			case "CephFS daemon":
				mutex = &fsDaemon.mu
				operation = func(ctx context.Context) error { return cephfs.RemoveDaemon(ctx, "a") }
			case "resources":
				mutex = &owned.mu
				operation = func(ctx context.Context) error { return owned.terminate(ctx) }
			}
			assertMulticlusterContextLockQueued(t, mutex, operation)
			if probe.terminations.Load() != 0 || daemon.terminated || fsDaemon.removed || len(rbd.daemons) != 1 || len(cephfs.daemons) != 1 || owned.actions[0].done {
				t.Fatal("nested deadline lost cleanup ownership")
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if err := operation(ctx); err != nil || probe.terminations.Load() != 1 {
				t.Fatalf("nested cleanup could not retry with fresh context: %v", err)
			}
		})
	}
}

func TestMulticlusterContextMutexCanceledFreeGate(t *testing.T) {
	var mutex sync.Mutex
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := lockRGWSyncObservation(ctx, &mutex); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled free gate returned %v", err)
	}
	if !mutex.TryLock() {
		t.Fatal("canceled caller consumed the free gate")
	}
	mutex.Unlock()
}

func assertMulticlusterContextLockQueued(t *testing.T, mutex *sync.Mutex, operation func(context.Context) error) {
	t.Helper()
	mutex.Lock()
	locked := true
	defer func() {
		if locked {
			mutex.Unlock()
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- operation(ctx) }()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("queued operation returned %v; want caller deadline", err)
		}
	case <-time.After(500 * time.Millisecond):
		mutex.Unlock()
		locked = false
		select {
		case <-result:
		case <-time.After(time.Second):
		}
		t.Fatal("queued operation ignored its short caller deadline")
	}
	if mutex.TryLock() {
		mutex.Unlock()
		t.Fatal("caller stole or unlocked the busy fixture gate")
	}
	mutex.Unlock()
	locked = false
}
