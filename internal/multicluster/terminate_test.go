package multicluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/testcontainers/testcontainers-go"
)

type terminateProbe struct {
	testcontainers.Container
	calls int
	err   error
}

func (p *terminateProbe) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	p.calls++
	return p.err
}

func TestTerminateOnceRemembersSuccessAndRetriesFailure(t *testing.T) {
	var mu sync.Mutex
	var done bool
	probe := &terminateProbe{err: errors.New("hook failed")}
	if err := TerminateOnce(t.Context(), &mu, &done, probe); !errors.Is(err, probe.err) || done || probe.calls != 1 {
		t.Fatalf("failed removal must stay retryable: err=%v done=%t calls=%d", err, done, probe.calls)
	}
	probe.err = nil
	if err := TerminateOnce(t.Context(), &mu, &done, probe); err != nil || !done || probe.calls != 2 {
		t.Fatalf("retry: err=%v done=%t calls=%d", err, done, probe.calls)
	}
	if err := TerminateOnce(t.Context(), &mu, &done, probe); err != nil || probe.calls != 2 {
		t.Fatalf("completed removal repeated hooks: err=%v calls=%d", err, probe.calls)
	}
}

func TestTerminateOnceTreatsMissingAsRemovedAndNilAsNoop(t *testing.T) {
	var mu sync.Mutex
	var done bool
	missing := &terminateProbe{err: fmt.Errorf("container gone: %w", errdefs.ErrNotFound)}
	if err := TerminateOnce(t.Context(), &mu, &done, missing); err != nil || !done {
		t.Fatalf("missing container: err=%v done=%t", err, done)
	}
	done = false
	if err := TerminateOnce(t.Context(), &mu, &done, nil); err != nil || done {
		t.Fatalf("nil container must be a no-op that stays unmarked: err=%v done=%t", err, done)
	}
}

func TestTerminateOnceLockWaitHonorsContext(t *testing.T) {
	var mu sync.Mutex
	var done bool
	probe := &terminateProbe{}
	mu.Lock()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err := TerminateOnce(ctx, &mu, &done, probe)
	mu.Unlock()
	if !errors.Is(err, context.DeadlineExceeded) || probe.calls != 0 || done {
		t.Fatalf("queued caller changed state: err=%v calls=%d done=%t", err, probe.calls, done)
	}
	if err := TerminateOnce(t.Context(), &mu, &done, probe); err != nil || !done || probe.calls != 1 {
		t.Fatalf("fresh context could not retry: err=%v done=%t calls=%d", err, done, probe.calls)
	}
}

func TestTerminateFixtureForgetsMembershipOnlyAfterCleanupSucceeds(t *testing.T) {
	var mu sync.Mutex
	var closed bool
	var owned Resources
	failing := true
	calls := 0
	owned.AddCleanup("probe", func(context.Context) error {
		calls++
		if failing {
			return errors.New("cleanup failed")
		}
		return nil
	})
	forgotten := 0
	forget := func() { forgotten++ }

	if err := TerminateFixture(t.Context(), &mu, &closed, &owned, forget); err == nil || !closed || forgotten != 0 || calls != 1 {
		t.Fatalf("failed cleanup: err=%v closed=%t forgotten=%d calls=%d", err, closed, forgotten, calls)
	}
	failing = false
	if err := TerminateFixture(t.Context(), &mu, &closed, &owned, forget); err != nil || !closed || forgotten != 1 || calls != 2 {
		t.Fatalf("retry: err=%v closed=%t forgotten=%d calls=%d", err, closed, forgotten, calls)
	}
	if err := TerminateFixture(t.Context(), &mu, &closed, &owned, forget); err != nil || forgotten != 2 || calls != 2 {
		t.Fatalf("completed cleanup repeated: err=%v forgotten=%d calls=%d", err, forgotten, calls)
	}
}

func TestTerminateFixtureLockWaitHonorsContext(t *testing.T) {
	var mu sync.Mutex
	var closed bool
	var owned Resources
	calls := 0
	owned.AddCleanup("probe", func(context.Context) error { calls++; return nil })
	mu.Lock()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err := TerminateFixture(ctx, &mu, &closed, &owned, func() { t.Error("forget ran without the lock") })
	mu.Unlock()
	if !errors.Is(err, context.DeadlineExceeded) || closed || calls != 0 {
		t.Fatalf("queued caller changed state: err=%v closed=%t calls=%d", err, closed, calls)
	}
}
