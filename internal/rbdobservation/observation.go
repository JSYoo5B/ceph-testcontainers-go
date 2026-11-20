// Package rbdobservation carries opt-in RBD observation timing to internal callers.
package rbdobservation

import (
	"context"
	"errors"
	"time"
)

type recorderKey struct{}

// Sample contains fixed stage labels and timing, with no image or native output.
type Sample struct {
	Operation, Stage                      string
	Started, Acquired, Released, Finished time.Time
	CallerContext, AttemptContext         string
	Outcome                               string
}

// WithRecorder attaches a sink that must be safe for concurrent observations.
func WithRecorder(ctx context.Context, record func(Sample)) context.Context {
	return context.WithValue(ctx, recorderKey{}, record)
}

type Attempt struct {
	record func(Sample)
	caller context.Context
	sample Sample
}

// Begin starts recording at admission. A nil attempt has no recording overhead
// apart from the context lookup and nil checks.
func Begin(caller context.Context, operation string) *Attempt {
	record, _ := caller.Value(recorderKey{}).(func(Sample))
	if record == nil {
		return nil
	}
	return &Attempt{record: record, caller: caller, sample: Sample{
		Operation: operation, Stage: "admission", Started: time.Now(),
	}}
}

func (a *Attempt) Acquired() {
	if a != nil {
		a.sample.Acquired = time.Now()
		a.sample.Stage = "validate-fixture"
	}
}

func (a *Attempt) Stage(stage string) {
	if a != nil {
		a.sample.Stage = stage
	}
}

func (a *Attempt) Releasing() {
	if a != nil {
		a.sample.Released = time.Now()
	}
}

// Finish reports after the caller releases the owner mutex.
func (a *Attempt) Finish(ctx context.Context, err error) {
	if a == nil {
		return
	}
	a.sample.Finished = time.Now()
	a.sample.Outcome = outcome(err)
	a.sample.CallerContext = outcome(a.caller.Err())
	a.sample.AttemptContext = outcome(ctx.Err())
	a.record(a.sample)
}

func outcome(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline-exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "error"
	}
}
