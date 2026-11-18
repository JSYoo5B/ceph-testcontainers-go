// Package mutation retains completion evidence for a reversible native write.
package mutation

import (
	"context"
	"errors"
	"time"
)

// Fence keeps an unfinished write owned until its native reply is confirmed.
// Its caller serializes Record and Wait with the owning resource's lock.
type Fence struct{ pending *pending }

type pending struct {
	done     chan struct{}
	err      error
	complete bool
	cause    error
}

func (p *pending) Error() string {
	return "native mutation completion is uncertain; retain ownership or terminate the cluster"
}
func (p *pending) Unwrap() []error {
	select {
	case <-p.done:
		return []error{p.cause, p.err}
	default:
		return []error{p.cause}
	}
}

// Run lets a dispatched write finish under its own bound after the caller
// cancels. The callback reports whether it obtained a terminal native reply.
// A transport error cannot establish completion, even if the callback ended.
func Run(ctx context.Context, bound time.Duration, fn func(context.Context) (bool, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if bound <= 0 {
		bound = time.Minute
	}
	runCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bound)
	p := &pending{done: make(chan struct{})}
	go func() {
		defer cancel()
		p.complete, p.err = fn(runCtx)
		close(p.done)
	}()
	select {
	case <-p.done:
		if p.complete {
			return p.err
		}
		p.cause = p.err
	case <-ctx.Done():
		p.cause = ctx.Err()
	}
	return p
}

// Record retains uncertain completion while preserving the operation's error.
func (f *Fence) Record(err error) error {
	var p *pending
	if errors.As(err, &p) {
		f.pending = p
	}
	return err
}

// Wait confirms the earlier write's terminal reply before any reconciliation.
// If the transport lost that reply, the resource stays owned until disposal.
func (f *Fence) Wait(ctx context.Context) error {
	p := f.pending
	if p == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		if !p.complete {
			return p
		}
		f.pending = nil
		return nil
	}
}

// Uncertain reports an invocation whose completion must be awaited before a
// native error can be used to release restoration ownership.
func Uncertain(err error) bool {
	var p *pending
	return errors.As(err, &p)
}
