//go:build all || integration

package integration_test

import (
	"context"
	"fmt"
	"time"
)

// observation is what observe saw: the last value that a probe returned
// within the phase, how many probes ran and how long the phase took.
type observation[T any] struct {
	Value    T
	Attempts int
	Elapsed  time.Duration
}

// observe polls a read-only probe until it reports the condition reached,
// returns an error, or the phase of bound ends. Probes run one after another,
// interval apart, each with its own context of probeBound inside the phase,
// so that one slow probe cannot outlast the phase. A probe that returns after
// the phase ended is not accepted, even when it reports the condition
// reached: its sample is not evidence of the phase.
//
// The probe returns ready false for a condition that is not reached yet and
// an error only for a failure that more waiting cannot fix, including its
// own deadline; observe returns that error at once. It must not mutate
// anything or call t.Fatal, and it must return once its context ends.
func observe[T any](ctx context.Context, bound, probeBound, interval time.Duration, probe func(context.Context) (T, bool, error)) (observation[T], error) {
	started := time.Now()
	phase, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	var seen observation[T]
	for {
		probeCtx, cancelProbe := context.WithTimeout(phase, probeBound)
		value, ready, err := probe(probeCtx)
		cancelProbe()
		seen.Attempts++
		seen.Elapsed = time.Since(started)
		if phase.Err() != nil {
			return seen, fmt.Errorf("not observed within %s after %d probes: %w", bound, seen.Attempts, phase.Err())
		}
		seen.Value = value
		if err != nil {
			return seen, fmt.Errorf("probe %d after %s: %w", seen.Attempts, seen.Elapsed.Round(time.Millisecond), err)
		}
		if ready {
			return seen, nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-phase.Done():
			timer.Stop()
			return seen, fmt.Errorf("not observed within %s after %d probes: %w", bound, seen.Attempts, phase.Err())
		case <-timer.C:
		}
	}
}
