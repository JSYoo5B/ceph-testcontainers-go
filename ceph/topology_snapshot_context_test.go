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
	for _, getter := range []string{"config", "managers", "monitors", "osds"} {
		t.Run(getter, func(t *testing.T) {
			cluster := &Container{}
			assertContextLockQueued(t, &cluster.mu, func(ctx context.Context) error {
				var err error
				switch getter {
				case "config":
					_, err = cluster.ConnectionConfig(ctx)
				case "managers":
					_, err = cluster.Managers(ctx)
				case "monitors":
					_, err = cluster.Monitors(ctx)
				case "osds":
					_, err = cluster.OSDs(ctx)
				}
				return err
			})
		})
	}
}

func TestConnectionConfigContextBoundsConfigurationCacheWait(t *testing.T) {
	cluster := &Container{config: []byte("config"), keyring: []byte("keyring")}
	assertSnapshotReadLockQueued(t, &cluster.configMu, func(ctx context.Context) error {
		_, err := cluster.ConnectionConfig(ctx)
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
	cluster := &Container{config: []byte("config"), keyring: []byte("keyring"), managers: map[string]*ManagerContainer{"b": b, "a": a}}
	cc, err := cluster.ConnectionConfig(t.Context())
	config := cc.Conf
	keyring := cc.Keyring
	if err != nil {
		t.Fatal(err)
	}
	config[0], keyring[0] = 'X', 'Y'
	if !bytes.Equal(cluster.config, []byte("config")) || !bytes.Equal(cluster.keyring, []byte("keyring")) {
		t.Fatal("bootstrap snapshot aliased private cache")
	}
	managers, err := cluster.Managers(t.Context())
	if err != nil || len(managers) != 2 || managers[0] != a || managers[1] != b {
		t.Fatalf("manager descriptors/order: %v %v", managers, err)
	}
	managers[0] = nil
	if cluster.managers["a"] != a {
		t.Fatal("manager snapshot modified ownership map")
	}
	cluster.closed = true
	if _, err := cluster.ConnectionConfig(t.Context()); err == nil {
		t.Fatal("closed cluster supplied bootstrap credentials")
	}
	// Descriptor snapshots permit inspection after termination.
	if managers, err := cluster.Managers(t.Context()); err != nil || len(managers) != 2 {
		t.Fatalf("closed descriptor inspection changed: %v %v", managers, err)
	}
}

func TestMonitorAndOSDSnapshotsMatchTheLockedListings(t *testing.T) {
	primary := &testcontainers.DockerContainer{}
	c, b := &MonitorContainer{DaemonName: "c"}, &MonitorContainer{DaemonName: "b"}
	osd0, osd2 := &OSDContainer{ID: 0}, &OSDContainer{ID: 2}
	cluster := &Container{Container: primary, monitors: map[string]*MonitorContainer{"c": c, "b": b},
		osds: map[int]*OSDContainer{2: osd2, 0: osd0}}
	for _, terminated := range []bool{false, true} {
		cluster.monitorTerminated = terminated
		monitors, err := cluster.Monitors(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		locked := cluster.monitorsLocked()
		if len(monitors) != len(locked) {
			t.Fatalf("terminated=%v: monitors %v, locked %v", terminated, monitors, locked)
		}
		for i := range monitors {
			if monitors[i].DaemonName != locked[i].DaemonName || monitors[i].Container != locked[i].Container {
				t.Fatalf("terminated=%v: monitor %d is %+v, locked %+v", terminated, i, monitors[i], locked[i])
			}
		}
		if want := map[bool]int{false: 3, true: 2}[terminated]; len(monitors) != want || monitors[len(monitors)-1] != c {
			t.Fatalf("terminated=%v: monitor membership/order %v", terminated, monitors)
		}
	}
	osds, err := cluster.OSDs(t.Context())
	if err != nil || len(osds) != 2 || osds[0] != osd0 || osds[1] != osd2 {
		t.Fatalf("osd descriptors/order: %v %v", osds, err)
	}
	osds[0] = nil
	if cluster.osds[0] != osd0 {
		t.Fatal("osd snapshot modified ownership map")
	}
}

func TestTopologySnapshotContextsRejectExpiredContextAndAllowRetry(t *testing.T) {
	cluster := &Container{config: []byte("config"), keyring: []byte("keyring")}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := cluster.ConnectionConfig(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("config: %v", err)
	}
	if _, err := cluster.Managers(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("managers: %v", err)
	}
	if _, err := cluster.Monitors(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("monitors: %v", err)
	}
	if _, err := cluster.OSDs(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("osds: %v", err)
	}
	if _, err := cluster.ConnectionConfig(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestControlContainerContextBoundsConcurrentTermination(t *testing.T) {
	cluster := &Container{}
	assertSnapshotReadLockQueued(t, &cluster.controlMu, func(ctx context.Context) error {
		_, err := cluster.ControlContainer(ctx)
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
					_, err = readStatus(ctx, cluster.cephCLI)
				case "manager-status":
					_, err = readManagerStatus(ctx, cluster.cephCLI)
				case "quorum":
					_, err = cluster.quorumStatus(ctx)
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
	// No caller deadline: a correct Ceph reaches the fake whatever a slow
	// (race-instrumented) run spends first, and one that waits for the held
	// topology owner is caught by the watchdog below.
	ctx := t.Context()
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
	selected, err := cluster.ControlContainer(t.Context())
	if err != nil || selected != control {
		t.Fatalf("stable CLI identity differs: %v", err)
	}
	cluster.controlPlane = nil
	selected, err = cluster.ControlContainer(t.Context())
	if err != nil || selected != original {
		t.Fatalf("default monitor CLI fallback identity differs: %v", err)
	}
	cluster.Container = nil
	if selected, err := cluster.ControlContainer(t.Context()); err != nil || selected != nil {
		t.Fatalf("missing control snapshot changed compatibility semantics: %v", err)
	}
}

func TestTopologyAccessorsRejectANilCluster(t *testing.T) {
	var cluster *Container
	ctx := t.Context()
	if _, err := cluster.ConnectionConfig(ctx); err == nil {
		t.Error("ConnectionConfig accepted a nil cluster")
	}
	if control, err := cluster.ControlContainer(ctx); err == nil || control != nil {
		t.Errorf("ControlContainer accepted a nil cluster: %v %v", control, err)
	}
	if client, err := cluster.ClientContainer(ctx); err == nil || client != nil {
		t.Errorf("ClientContainer accepted a nil cluster: %v %v", client, err)
	}
	if managers, err := cluster.Managers(ctx); err == nil || managers != nil {
		t.Errorf("Managers accepted a nil cluster: %v %v", managers, err)
	}
	if monitors, err := cluster.Monitors(ctx); err == nil || monitors != nil {
		t.Errorf("Monitors accepted a nil cluster: %v %v", monitors, err)
	}
	if osds, err := cluster.OSDs(ctx); err == nil || osds != nil {
		t.Errorf("OSDs accepted a nil cluster: %v %v", osds, err)
	}
}
