// Package watch is the polling producer behind the public Watch APIs. Each
// public package keeps its own config validation, event type and identity
// rules; this package only runs the reads, numbers the events, reports gaps
// and overflow, and joins the producer on Stop.
package watch

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Kind names what an event reports. The public packages convert it to
// ceph.WatchEventKind.
type Kind string

// The kinds of event.
const (
	// Baseline is the first event: the read that Start made before it
	// returned.
	Baseline Kind = "baseline"
	// Changed is a successful read that differs from the previous
	// successful read.
	Changed Kind = "changed"
	// Gap is one failed read; the watch keeps polling.
	Gap Kind = "gap"
	// Resumed is the first successful read after one or more gaps, whether
	// or not the sample changed across them.
	Resumed Kind = "resumed"
)

// ErrOverflow ends a watch whose consumer left the event buffer full for one
// polling interval while an event waited. ceph.ErrWatchOverflow is this
// value.
var ErrOverflow = errors.New("watch event buffer overflow")

// ErrStopped is the cancellation cause of Stream.Stop. Stop reports it as a
// nil cause.
var ErrStopped = errors.New("watch stopped")

// Limits of a Settings.
const (
	MinInterval    = 100 * time.Millisecond
	DefaultTimeout = 30 * time.Second
	DefaultBuffer  = 16
	MaxBuffer      = 1024
)

// Settings is a validated watch configuration.
type Settings struct {
	Interval, Timeout time.Duration
	Buffer            int
}

// Validate checks the raw configuration fields of a public WatchConfig and
// fills its defaults. defaultInterval is the target's own default. The error
// is unclassified; the caller marks it as an invalid request and names its
// operation.
func Validate(interval, timeout time.Duration, buffer int, defaultInterval time.Duration) (Settings, error) {
	s := Settings{Interval: interval, Timeout: timeout, Buffer: buffer}
	switch {
	case s.Interval < 0, s.Timeout < 0, s.Buffer < 0:
		return Settings{}, errors.New("negative interval, query timeout or buffer")
	case s.Interval != 0 && s.Interval < MinInterval:
		return Settings{}, fmt.Errorf("interval %v is below %v", s.Interval, MinInterval)
	case s.Buffer > MaxBuffer:
		return Settings{}, fmt.Errorf("buffer %d exceeds %d", s.Buffer, MaxBuffer)
	}
	if s.Interval == 0 {
		s.Interval = defaultInterval
	}
	if s.Timeout == 0 {
		s.Timeout = DefaultTimeout
	}
	if s.Buffer == 0 {
		s.Buffer = DefaultBuffer
	}
	return s, nil
}

// Meta is the target-independent part of one event.
type Meta struct {
	Sequence          uint64
	Kind              Kind
	Started, Finished time.Time
}

// Spec describes one watch target.
//
// Read makes one read. Start calls it once for the baseline and then the
// producer calls it again, never concurrently, so Read may keep state such as
// identities pinned by the baseline. A Read error is a gap unless it is
// wrapped with Fatal, which ends the watch with the wrapped error. Every
// baseline error ends Start. Read must release every fixture lock before it
// returns.
//
// On a gap, Event receives what Read returned together with its error, which
// may be partial evidence. Equal compares two successful samples; equal
// samples produce no event.
// Final, when set, reports a sample after which nothing can change: the watch
// delivers it and ends with a nil cause. Event builds the public event; it
// must copy every slice and map of the sample, because the producer keeps it.
type Spec[S, E any] struct {
	Name     string
	Settings Settings
	Read     func(context.Context) (S, error)
	Equal    func(a, b S) bool
	Final    func(S) bool
	Event    func(meta Meta, sample S, err error) E
}

// Fatal marks a Read error that a later read cannot undo, such as an identity
// change or a terminated cluster. The watch ends with err.
func Fatal(err error) error {
	if err == nil {
		return nil
	}
	return fatalError{err}
}

type fatalError struct{ err error }

func (f fatalError) Error() string { return f.err.Error() }
func (f fatalError) Unwrap() error { return f.err }

// fatalCause returns the error inside a Fatal mark, or nil.
func fatalCause(err error) error {
	var f fatalError
	if errors.As(err, &f) {
		return f.err
	}
	return nil
}

// Stream is one running watch. Events closes after the producer goroutine has
// stopped using the fixture.
type Stream[E any] struct {
	events chan E
	cancel context.CancelCauseFunc
	done   chan struct{}
	err    error
}

// Events returns the receive-only event channel; nil on a nil Stream.
func (s *Stream[E]) Events() <-chan E {
	if s == nil {
		return nil
	}
	return s.events
}

// Started reports whether s came from Start.
func (s *Stream[E]) Started() bool { return s != nil && s.done != nil }

// Done closes when the producer has returned, after Events closes.
func (s *Stream[E]) Done() <-chan struct{} { return s.done }

// Stop cancels the watch, waits for the producer and returns the cause that
// ended the watch: nil for Stop or a final sample, otherwise the context's
// cause, an ErrOverflow error or the Fatal read error. Every call returns the
// same cause. The caller checks Started first.
func (s *Stream[E]) Stop() error {
	s.cancel(ErrStopped)
	<-s.done
	return s.err
}

// run is the producer state of one watch. Only its goroutine uses it after
// Start returns.
type run[S, E any] struct {
	spec     Spec[S, E]
	stream   *Stream[E]
	last     S
	sequence uint64
	gap      bool
}

// Start makes the baseline read under ctx and the query timeout, buffers the
// baseline event and starts the producer. A failed baseline returns its error
// and starts nothing.
func Start[S, E any](ctx context.Context, spec Spec[S, E]) (*Stream[E], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r := &run[S, E]{spec: spec}
	started := time.Now()
	sample, err := r.read(ctx)
	finished := time.Now()
	if err != nil {
		if cause := fatalCause(err); cause != nil {
			return nil, cause
		}
		return nil, err
	}
	r.last = sample
	watchCtx, cancel := context.WithCancelCause(ctx)
	r.stream = &Stream[E]{events: make(chan E, spec.Settings.Buffer), cancel: cancel, done: make(chan struct{})}
	r.stream.events <- r.event(Baseline, started, finished, sample, nil)
	if spec.Final != nil && spec.Final(sample) {
		cancel(nil)
		close(r.stream.events)
		close(r.stream.done)
		return r.stream, nil
	}
	go r.loop(watchCtx, started)
	return r.stream, nil
}

func (r *run[S, E]) read(ctx context.Context) (S, error) {
	ctx, cancel := context.WithTimeout(ctx, r.spec.Settings.Timeout)
	defer cancel()
	return r.spec.Read(ctx)
}

func (r *run[S, E]) event(kind Kind, started, finished time.Time, sample S, err error) E {
	r.sequence++
	return r.spec.Event(Meta{Sequence: r.sequence, Kind: kind, Started: started, Finished: finished}, sample, err)
}

func (r *run[S, E]) loop(ctx context.Context, previous time.Time) {
	s := r.stream
	defer func() {
		s.cancel(nil)
		close(s.events)
		close(s.done)
	}()
	s.err = r.poll(ctx, previous)
	if errors.Is(s.err, ErrStopped) {
		s.err = nil
	}
}

// poll runs the reads until one ends the watch, and returns that cause.
func (r *run[S, E]) poll(ctx context.Context, previous time.Time) error {
	for {
		if err := r.wait(ctx, time.Until(previous.Add(r.spec.Settings.Interval))); err != nil {
			return err
		}
		started := time.Now()
		previous = started
		sample, err := r.read(ctx)
		finished := time.Now()
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if cause := fatalCause(err); cause != nil {
			return cause
		}
		var kind Kind
		switch {
		case err != nil:
			r.gap = true
			kind = Gap
		case r.gap:
			r.gap = false
			kind = Resumed
		case !r.spec.Equal(sample, r.last):
			kind = Changed
		default:
			continue
		}
		if err == nil {
			r.last = sample
		}
		if err := r.deliver(ctx, r.event(kind, started, finished, sample, err), kind); err != nil {
			return err
		}
		if err == nil && r.spec.Final != nil && r.spec.Final(sample) {
			return nil
		}
	}
}

// wait sleeps for d unless ctx ends first. Every return of an ended ctx is
// its cause, so poll can tell Stop from the Start context ending, also when
// the next read is already overdue.
func (r *run[S, E]) wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}

// deliver sends event, waiting at most one interval for buffer space.
func (r *run[S, E]) deliver(ctx context.Context, event E, kind Kind) error {
	select {
	case r.stream.events <- event:
		return nil
	default:
	}
	timer := time.NewTimer(r.spec.Settings.Interval)
	defer timer.Stop()
	select {
	case r.stream.events <- event:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return fmt.Errorf("%w: %s: event %d (%s) undelivered: the consumer left all %d buffered events unread for %v", ErrOverflow, r.spec.Name, r.sequence, kind, r.spec.Settings.Buffer, r.spec.Settings.Interval)
	}
}
