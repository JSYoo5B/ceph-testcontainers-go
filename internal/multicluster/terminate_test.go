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
