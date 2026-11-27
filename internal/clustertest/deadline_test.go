package clustertest

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A context derived from a SignalDeadline is cancelled when Expire returns,
// as one derived from a standard context is; a goroutine that watches Done
// would leave the derived context live for a moment after Expire.
func TestSignalDeadlineCancelsDerivedContextsSynchronously(t *testing.T) {
	for range 1000 {
		deadline := NewSignalDeadline(t.Context())
		timeout, cancelTimeout := context.WithTimeout(deadline, time.Hour)
		canceled, cancel := context.WithCancel(deadline)
		deadline.Expire()
		if !errors.Is(timeout.Err(), context.DeadlineExceeded) || !errors.Is(canceled.Err(), context.DeadlineExceeded) {
			t.Fatalf("derived contexts after Expire: %v, %v", timeout.Err(), canceled.Err())
		}
		cancelTimeout()
		cancel()
	}
	expired := NewSignalDeadline(t.Context())
	expired.Expire()
	derived, cancel := context.WithTimeout(expired, time.Hour)
	defer cancel()
	if !errors.Is(derived.Err(), context.DeadlineExceeded) {
		t.Fatalf("derived from an expired deadline: %v", derived.Err())
	}
	// A derived context cancelled first unregisters itself; Expire still works.
	deadline := NewSignalDeadline(t.Context())
	early, cancelEarly := context.WithCancel(deadline)
	cancelEarly()
	deadline.Expire()
	if !errors.Is(early.Err(), context.Canceled) || !errors.Is(deadline.Err(), context.DeadlineExceeded) || len(deadline.after) != 0 {
		t.Fatalf("early cancel: %v, deadline %v, pending %d", early.Err(), deadline.Err(), len(deadline.after))
	}
	// Cancelling the parent does not expire it.
	parent, cancelParent := context.WithCancel(t.Context())
	unexpired := NewSignalDeadline(parent)
	cancelParent()
	if unexpired.Err() != nil {
		t.Fatalf("a cancelled parent expired the deadline: %v", unexpired.Err())
	}
}

// pausedRegistration holds the registration of a derived context until the
// test releases it, so that Expire runs between the context package's check
// of Done and its call of AfterFunc, which it makes with the derived
// context's lock held.
type pausedRegistration struct {
	*SignalDeadline
	entered, release chan struct{}
}

func (p pausedRegistration) AfterFunc(f func()) func() bool {
	close(p.entered)
	<-p.release
	return p.SignalDeadline.AfterFunc(f)
}

// A context derived while Expire runs is cancelled, and deriving it does not
// deadlock on its own lock.
func TestSignalDeadlineExpireDuringRegistration(t *testing.T) {
	for _, derive := range map[string]func(context.Context) (context.Context, context.CancelFunc){
		"WithCancel": context.WithCancel,
		"WithTimeout": func(parent context.Context) (context.Context, context.CancelFunc) {
			return context.WithTimeout(parent, time.Hour)
		},
	} {
		deadline := NewSignalDeadline(t.Context())
		paused := pausedRegistration{SignalDeadline: deadline, entered: make(chan struct{}), release: make(chan struct{})}
		derived := make(chan context.Context, 1)
		go func() {
			ctx, cancel := derive(paused)
			t.Cleanup(cancel)
			derived <- ctx
		}()
		<-paused.entered
		deadline.Expire()
		close(paused.release)
		select {
		case ctx := <-derived:
			select {
			case <-ctx.Done():
			case <-time.After(10 * time.Second):
				t.Fatal("the context derived during Expire was not cancelled")
			}
			if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Fatalf("derived during Expire: %v", ctx.Err())
			}
		case <-time.After(10 * time.Second):
			t.Fatal("deriving a context during Expire deadlocked")
		}
	}
}
