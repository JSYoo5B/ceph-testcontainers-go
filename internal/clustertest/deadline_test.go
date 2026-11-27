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
