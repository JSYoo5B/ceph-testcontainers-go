package ceph

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestTopologySnapshotContextsHonorBusyOwnerDeadline(t *testing.T) {
	for _, getter := range []string{"config", "managers", "gateways"} {
		t.Run(getter, func(t *testing.T) {
			cluster := &Container{}
			assertContextLockQueued(t, &cluster.mu, func(ctx context.Context) error {
				var err error
				switch getter {
				case "config":
					_, _, err = cluster.ConnectionConfigContext(ctx)
				case "managers":
					_, err = cluster.ManagersContext(ctx)
				case "gateways":
					_, err = cluster.GatewaysContext(ctx)
				}
				return err
			})
		})
	}
}

func TestConnectionConfigContextBoundsConfigurationCacheWait(t *testing.T) {
	cluster := &Container{config: []byte("config"), keyring: []byte("keyring")}
	assertSnapshotReadLockQueued(t, &cluster.configMu, func(ctx context.Context) error {
		_, _, err := cluster.ConnectionConfigContext(ctx)
		return err
	})
	if !cluster.mu.TryLock() {
		t.Fatal("failed config cache snapshot retained owner mutex")
	}
	cluster.mu.Unlock()
}

func assertSnapshotReadLockQueued(t *testing.T, mutex *sync.RWMutex, operation func(context.Context) error) {
	t.Helper()
	mutex.Lock()
	locked := true
	defer func() {
		if locked {
			mutex.Unlock()
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- operation(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("queued snapshot lost caller deadline: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		mutex.Unlock()
		locked = false
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		t.Fatal("snapshot remained queued after caller deadline")
	}
	if mutex.TryRLock() {
		mutex.RUnlock()
		t.Fatal("snapshot unlocked the busy writer gate")
	}
	mutex.Unlock()
	locked = false
}

func TestTopologySnapshotContextsPreserveCopiesAndOwnedDescriptors(t *testing.T) {
	a, b := &ManagerContainer{DaemonName: "a"}, &ManagerContainer{DaemonName: "b"}
	x, y := &RGWContainer{GatewayName: "x"}, &RGWContainer{GatewayName: "y"}
	cluster := &Container{config: []byte("config"), keyring: []byte("keyring"), managers: map[string]*ManagerContainer{"b": b, "a": a}, gateways: map[string]*RGWContainer{"y": y, "x": x}}
	config, keyring, err := cluster.ConnectionConfigContext(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	config[0], keyring[0] = 'X', 'Y'
	if !bytes.Equal(cluster.config, []byte("config")) || !bytes.Equal(cluster.keyring, []byte("keyring")) {
		t.Fatal("bootstrap snapshot aliased private cache")
	}
	managers, err := cluster.ManagersContext(t.Context())
	if err != nil || len(managers) != 2 || managers[0] != a || managers[1] != b {
		t.Fatalf("manager descriptors/order: %v %v", managers, err)
	}
	managers[0] = nil
	if cluster.managers["a"] != a {
		t.Fatal("manager snapshot modified ownership map")
	}
	gateways, err := cluster.GatewaysContext(t.Context())
	if err != nil || len(gateways) != 2 || gateways[0] != x || gateways[1] != y {
		t.Fatalf("gateway descriptors/order: %v %v", gateways, err)
	}
	gateways[0] = nil
	if cluster.gateways["x"] != x {
		t.Fatal("gateway snapshot modified ownership map")
	}
	cluster.closed = true
	if _, _, err := cluster.ConnectionConfigContext(t.Context()); err == nil {
		t.Fatal("closed cluster supplied bootstrap credentials")
	}
	// Legacy descriptor getters permit inspection after termination.
	if managers, err := cluster.ManagersContext(t.Context()); err != nil || len(managers) != 2 {
		t.Fatalf("closed descriptor inspection changed: %v %v", managers, err)
	}
}

func TestTopologySnapshotContextsRejectExpiredContextAndAllowRetry(t *testing.T) {
	cluster := &Container{config: []byte("config"), keyring: []byte("keyring")}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := cluster.ConnectionConfigContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("config: %v", err)
	}
	if _, err := cluster.ManagersContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("managers: %v", err)
	}
	if _, err := cluster.GatewaysContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("gateways: %v", err)
	}
	if _, _, err := cluster.ConnectionConfigContext(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestControlContainerContextBoundsConcurrentTermination(t *testing.T) {
	cluster := &Container{}
	assertSnapshotReadLockQueued(t, &cluster.controlMu, func(ctx context.Context) error {
		_, err := cluster.ControlContainerContext(ctx)
		return err
	})
}

func TestCephStatusQueriesHonorBusyControlWhileOwnerIsHeld(t *testing.T) {
	for _, operation := range []string{"ceph", "status", "manager-status", "quorum"} {
		t.Run(operation, func(t *testing.T) {
			cluster := &Container{}
			cluster.mu.Lock()
			defer cluster.mu.Unlock()
			cluster.controlMu.Lock()
			locked := true
			defer func() {
				if locked {
					cluster.controlMu.Unlock()
				}
			}()
			ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				var err error
				switch operation {
				case "ceph":
					_, err = cluster.Ceph(ctx, "status")
				case "status":
					_, err = cluster.Status(ctx)
				case "manager-status":
					_, err = cluster.ManagerStatus(ctx)
				case "quorum":
					_, err = cluster.QuorumStatus(ctx)
				}
				done <- err
			}()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("control deadline cause: %v", err)
				}
			case <-time.After(400 * time.Millisecond):
				cluster.controlMu.Unlock()
				locked = false
				select {
				case <-done:
				case <-time.After(time.Second):
				}
				t.Fatal("native wrapper remained in plain control read wait")
			}
		})
	}
}

type snapshotCommandControl struct {
	testcontainers.Container
	calls int
}

var errSnapshotCommandReached = errors.New("snapshot command reached")

func (control *snapshotCommandControl) Exec(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
	control.calls++
	return 0, nil, errSnapshotCommandReached
}

func TestCephControlSnapshotDoesNotReenterTopologyOwner(t *testing.T) {
	control := &snapshotCommandControl{}
	cluster := &Container{Container: control}
	cluster.mu.Lock()
	locked := true
	defer func() {
		if locked {
			cluster.mu.Unlock()
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := cluster.Ceph(ctx, "status"); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, errSnapshotCommandReached) || control.calls != 1 {
			t.Fatalf("Ceph reacquired caller-owned topology gate: err=%v calls=%d", err, control.calls)
		}
	case <-time.After(500 * time.Millisecond):
		cluster.mu.Unlock()
		locked = false
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		t.Fatal("Ceph tried to reacquire the topology owner")
	}
}

func TestControlContainerContextRetainsStableCLIAndFallbackIdentity(t *testing.T) {
	original, control := &snapshotCommandControl{}, &snapshotCommandControl{}
	cluster := &Container{Container: original, controlPlane: control}
	selected, err := cluster.ControlContainerContext(t.Context())
	if err != nil || selected != control {
		t.Fatalf("stable CLI identity differs: %v", err)
	}
	cluster.controlPlane = nil
	selected, err = cluster.ControlContainerContext(t.Context())
	if err != nil || selected != original {
		t.Fatalf("default monitor CLI fallback identity differs: %v", err)
	}
	cluster.Container = nil
	if selected, err := cluster.ControlContainerContext(t.Context()); err != nil || selected != nil {
		t.Fatalf("missing control snapshot changed compatibility semantics: %v", err)
	}
}
