package mutation

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestFenceWaitsForTerminalReply(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, time.Second, func(context.Context) (bool, error) {
			close(started)
			<-release
			return true, nil
		})
	}()
	<-started
	cancel()
	var f Fence
	if err := f.Record(<-done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	wait, end := context.WithCancel(t.Context())
	end()
	if err := f.Wait(wait); err == nil {
		t.Fatal("unfinished write released")
	}
	close(release)
	if err := f.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := f.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestLostTransportCannotBeSettledByCallbackExit(t *testing.T) {
	lost := errors.New("transport lost")
	var f Fence
	if err := f.Record(Run(t.Context(), time.Second, func(context.Context) (bool, error) { return false, lost })); !errors.Is(err, lost) {
		t.Fatal(err)
	}
	for range 2 {
		if err := f.Wait(t.Context()); !errors.Is(err, lost) {
			t.Fatalf("completion inferred: %v", err)
		}
	}
}

func TestCanceledBeforeDispatchRunsNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := Run(ctx, time.Second, func(context.Context) (bool, error) {
		t.Fatal("dispatched")
		return true, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestCanceledMutationKeepsLaterFailure(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	late := errors.New("completion was lost")
	go func() {
		result <- Run(ctx, time.Second, func(context.Context) (bool, error) {
			close(started)
			<-release
			return false, late
		})
	}()
	<-started
	cancel()
	var f Fence
	original := f.Record(<-result)
	close(release)
	if err := f.Wait(t.Context()); !errors.Is(err, late) || !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !Uncertain(original) || !errors.Is(original, late) {
		t.Fatal("later failure became inaccessible")
	}
}
