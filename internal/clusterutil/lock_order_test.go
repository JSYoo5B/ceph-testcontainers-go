package clusterutil

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// lockOrderProbe starts LockMutex callers at chosen times and records the
// order in which they got the mutex.
type lockOrderProbe struct {
	mutex   sync.Mutex
	orderMu sync.Mutex
	order   []string
	done    sync.WaitGroup
}

func (p *lockOrderProbe) record(entry string) {
	p.orderMu.Lock()
	defer p.orderMu.Unlock()
	p.order = append(p.order, entry)
}

// arrive calls LockMutex after delay and keeps the mutex for hold.
func (p *lockOrderProbe) arrive(ctx context.Context, name string, delay, hold time.Duration) {
	p.done.Go(func() {
		time.Sleep(delay)
		if err := LockMutex(ctx, &p.mutex); err != nil {
			p.record(name + ":" + err.Error())
			return
		}
		p.record(name)
		time.Sleep(hold)
		p.mutex.Unlock()
	})
}

// A caller that has waited through a whole hold must not lose the mutex to a
// caller that arrived just before the release. With unordered polling the late
// caller's next attempt (12ms) precedes the early caller's (15ms).
func TestLockMutexServesWaitersInArrivalOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var p lockOrderProbe
		p.mutex.Lock()
		p.arrive(t.Context(), "early", 0, 20*time.Millisecond)
		p.arrive(t.Context(), "late", 7*time.Millisecond, 0)
		time.Sleep(11 * time.Millisecond)
		p.mutex.Unlock()
		// A third caller arrives while early holds the mutex and late waits:
		// it is behind late, not level with it.
		p.arrive(t.Context(), "last", 9*time.Millisecond, 0)
		p.done.Wait()
		if want := []string{"early", "late", "last"}; !slices.Equal(p.order, want) {
			t.Fatalf("mutex was granted in order %v, want %v", p.order, want)
		}
		if len(lockQueues.waiting) != 0 {
			t.Fatalf("a finished queue was retained: %v", lockQueues.waiting)
		}
	})
}

// A caller whose context ends leaves the queue: the callers behind it move up
// and the mutex is not taken on its behalf.
func TestLockMutexCanceledWaiterPassesItsTurnOn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var p lockOrderProbe
		p.mutex.Lock()
		head, cancelHead := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancelHead()
		middle, cancelMiddle := context.WithTimeout(t.Context(), 30*time.Millisecond)
		defer cancelMiddle()
		p.arrive(head, "head", 0, 0)
		p.arrive(middle, "middle", time.Millisecond, 0)
		p.arrive(t.Context(), "tail", 2*time.Millisecond, 0)
		time.Sleep(40 * time.Millisecond)
		if p.mutex.TryLock() {
			t.Fatal("a canceled caller released the held mutex")
		}
		p.mutex.Unlock()
		p.done.Wait()
		deadline := context.DeadlineExceeded.Error()
		if want := []string{"head:" + deadline, "middle:" + deadline, "tail"}; !slices.Equal(p.order, want) {
			t.Fatalf("callers finished as %v, want %v", p.order, want)
		}
		if len(lockQueues.waiting) != 0 {
			t.Fatalf("a finished queue was retained: %v", lockQueues.waiting)
		}
	})
}

// A free mutex with nobody waiting is taken at once, and a context that has
// already ended never takes it.
func TestLockMutexFreeMutex(t *testing.T) {
	var mutex sync.Mutex
	if err := LockMutex(t.Context(), &mutex); err != nil {
		t.Fatal(err)
	}
	if mutex.TryLock() {
		t.Fatal("LockMutex returned without holding the mutex")
	}
	mutex.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := LockMutex(ctx, &mutex); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled caller returned %v", err)
	}
	if !mutex.TryLock() {
		t.Fatal("canceled caller kept the free mutex")
	}
	mutex.Unlock()
}
