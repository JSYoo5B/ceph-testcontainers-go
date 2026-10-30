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
//testdouble:shared-double
type SignalDeadline struct {
	context.Context
	done chan struct{}
	once sync.Once
}

// NewSignalDeadline derives a SignalDeadline from parent. Cancelling parent
// does not expire it; Expire does.
//
//testdouble:shared-double
func NewSignalDeadline(parent context.Context) *SignalDeadline {
	return &SignalDeadline{Context: parent, done: make(chan struct{})}
}

// Expire passes the deadline. It is safe to call more than once and from any
// goroutine.
func (d *SignalDeadline) Expire() { d.once.Do(func() { close(d.done) }) }

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
