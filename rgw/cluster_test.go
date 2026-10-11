package rgw

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/testcontainers/testcontainers-go"
)

// testCluster builds a cluster value whose control container is control and
// whose owned service containers are services.
func testCluster(control testcontainers.Container, config servicehook.TestConfig, services map[string]testcontainers.Container) *ceph.Container {
	c := servicehook.TestCluster(control, config)
	for name, ctr := range services {
		servicehook.SetService(c, name, ctr)
	}
	return c
}

// terminationFixtureContainer counts terminations and can fail the first.
type terminationFixtureContainer struct {
	testcontainers.Container
	terminations        int
	failTerminationOnce bool
}

func (ctr *terminationFixtureContainer) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	ctr.terminations++
	if ctr.failTerminationOnce && ctr.terminations == 1 {
		return errors.New("Docker removal failed")
	}
	return nil
}

// assertSnapshotReadLockQueued holds mutex as a writer and checks that
// operation waits for it, returns the caller's deadline and leaves the gate
// to the writer.
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
