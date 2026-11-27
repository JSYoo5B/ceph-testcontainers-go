package clustertest

import (
	"context"
	"sync"
	"time"
)

// SignalDeadline is a context whose deadline passes when the test says so:
// Expire closes Done and Err then reports context.DeadlineExceeded, as an
// expired timeout would. A test calls Expire from a fake once the operation
// reached the state the test asserts on, so the assertion does not depend on
// how much work a slow (race-instrumented, loaded) run finishes before a
// wall-clock deadline. It reports no Deadline, so code that sizes its own
// timeouts from the caller's deadline treats it as unbounded until it expires.
//
// A context derived from it (context.WithTimeout, WithCancel, ...) is
// cancelled before Expire returns, as one derived from a standard context
// is when that is cancelled: the code under test sees the deadline at its
// next check, never a moment later.
//
//testdouble:shared-double
type SignalDeadline struct {
	context.Context
	done chan struct{}
	once sync.Once
	// mu guards expired and after, the functions that AfterFunc registered
	// and Expire has not run yet.
	mu      sync.Mutex
	expired bool
	after   map[*func()]struct{}
}

// NewSignalDeadline derives a SignalDeadline from parent. Cancelling parent
// does not expire it; Expire does.
//
//testdouble:shared-double
func NewSignalDeadline(parent context.Context) *SignalDeadline {
	return &SignalDeadline{Context: parent, done: make(chan struct{})}
}

// Expire passes the deadline and cancels every context derived from d before
// it returns. It is safe to call more than once and from any goroutine.
func (d *SignalDeadline) Expire() {
	d.once.Do(func() {
		d.mu.Lock()
		close(d.done)
		d.expired = true
		after := d.after
		d.after = nil
		d.mu.Unlock()
		for f := range after {
			(*f)()
		}
	})
}

// AfterFunc arranges for f to run when d expires: within Expire, or at once
// when d has expired already. The context package uses it to cancel the
// contexts derived from d synchronously instead of from a goroutine that
// watches Done. stop reports whether it kept f from running.
func (d *SignalDeadline) AfterFunc(f func()) (stop func() bool) {
	d.mu.Lock()
	if d.expired {
		d.mu.Unlock()
		f()
		return func() bool { return false }
	}
	key := &f
	if d.after == nil {
		d.after = map[*func()]struct{}{}
	}
	d.after[key] = struct{}{}
	d.mu.Unlock()
	return func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		_, pending := d.after[key]
		delete(d.after, key)
		return pending
	}
}

// Deadline reports no deadline: the context expires on Expire, not at a time.
func (d *SignalDeadline) Deadline() (time.Time, bool) { return time.Time{}, false }

// Done is closed by Expire.
func (d *SignalDeadline) Done() <-chan struct{} { return d.done }

// Err reports context.DeadlineExceeded once Expire was called.
func (d *SignalDeadline) Err() error {
	select {
	case <-d.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}
