package ceph

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestSerializedTopologyMutationQueuedDeadline(t *testing.T) {
	operations := map[string]func(context.Context, *Container) error{
		"AddMonitor":    func(ctx context.Context, c *Container) error { _, err := c.AddMonitor(ctx, "queued"); return err },
		"RemoveMonitor": func(ctx context.Context, c *Container) error { return c.RemoveMonitor(ctx, "queued") },
		"AddManager":    func(ctx context.Context, c *Container) error { _, err := c.AddManager(ctx, "queued"); return err },
		"RemoveManager": func(ctx context.Context, c *Container) error { return c.RemoveManager(ctx, "queued") },
		"AddOSD":        func(ctx context.Context, c *Container) error { _, err := c.AddOSD(ctx); return err },
		"RemoveOSD":     func(ctx context.Context, c *Container) error { return c.RemoveOSD(ctx, 3) },
		"Terminate":     func(ctx context.Context, c *Container) error { return c.Terminate(ctx) },
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			cluster, control, daemon := newOSDLifecycleFixture()
			owned := cluster.osds[3]
			assertContextLockQueued(t, &cluster.mu, func(ctx context.Context) error { return operation(ctx, cluster) })
			if control.dumps != 0 || len(control.actions) != 0 || daemon.stops != 0 || daemon.terminations != 0 || cluster.closed || cluster.osds[3] != owned || owned.purged || owned.purgeIssued {
				t.Fatal("queued deadline changed native membership, Docker lifecycle or fixture ownership")
			}
			// The canceled waiter must not retain the mutex or consume a future
			// operation. Execute a real drain/purge with the same owned handle.
			freshCtx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if err := cluster.RemoveOSD(freshCtx, 3); err != nil {
				t.Fatalf("fresh context could not finish existing OSD removal: %v", err)
			}
			if control.purges != 1 || daemon.stops != 1 || daemon.terminations != 1 || cluster.osds[3] != nil || cluster.osds[4] == nil {
				t.Fatal("fresh operation failed to preserve neighbor identity and remove the owned OSD")
			}
		})
	}
}

func TestContextMutexAlreadyCanceledDoesNotAcquireFreeGate(t *testing.T) {
	var mutex sync.Mutex
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := lockTopologyMutex(ctx, &mutex); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled free gate returned %v", err)
	}
	if !mutex.TryLock() {
		t.Fatal("canceled context consumed the free gate")
	}
	mutex.Unlock()
}

func TestSerializedAuxiliaryCleanupQueuedDeadline(t *testing.T) {
	t.Run("host port lease", func(t *testing.T) {
		_, _, daemon := newOSDLifecycleFixture()
		lease := &hostPortLease{ctr: daemon}
		assertContextLockQueued(t, &lease.mu, lease.Release)
		if daemon.terminations != 0 || lease.ctr != daemon {
			t.Fatal("queued lease release closed reservations or discarded ownership")
		}
		if err := lease.Release(t.Context()); err != nil || daemon.terminations != 1 || lease.ctr != nil {
			t.Fatalf("fresh lease cleanup could not remove its owned allocator: %v", err)
		}
	})
}

func assertContextLockQueued(t *testing.T, mutex *sync.Mutex, operation func(context.Context) error) {
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
		// Release before failing so a regression leaves no abandoned worker.
		mutex.Unlock()
		locked = false
		select {
		case <-result:
		case <-time.After(time.Second):
		}
		t.Fatal("queued operation did not honor its short caller deadline")
	}
	if mutex.TryLock() {
		mutex.Unlock()
		t.Fatal("operation stole or unlocked the busy fixture gate")
	}
	mutex.Unlock()
	locked = false
}
