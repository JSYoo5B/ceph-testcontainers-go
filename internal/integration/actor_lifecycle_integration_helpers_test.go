//go:build all || integration

package integration_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"
)

// integrationActor publishes its result once and lets readiness and cleanup
// observe the same completion without consuming one another's result.
type integrationActor[T any] struct {
	done   chan struct{}
	result T
	cancel context.CancelFunc
}

func launchIntegrationActor[T any](ctx context.Context, run func(context.Context) T) *integrationActor[T] {
	ctx, cancel := context.WithCancel(ctx)
	a := &integrationActor[T]{done: make(chan struct{}), cancel: cancel}
	go func() {
		a.result = run(ctx)
		close(a.done)
	}()
	return a
}

func (a *integrationActor[T]) wait(ctx context.Context) (T, error) {
	select {
	case <-a.done:
		return a.result, nil
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

func (a *integrationActor[T]) cleanup(t interface {
	Helper()
	Errorf(string, ...any)
}, release func(context.Context) error) {
	t.Helper()
	defer a.cancel()
	select {
	case <-a.done:
		return
	default:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := release(ctx); err != nil {
		t.Errorf("release actor during cleanup: %v", err)
	}
	if _, err := a.wait(ctx); err != nil {
		a.cancel()
		joinCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		_, joinErr := a.wait(joinCtx)
		t.Errorf("actor cleanup incomplete after release: %v; command reader join: %v", err, joinErr)
	}
}

// startFixtureProcess owns Wait immediately after Start, including when a
// later producer fails to start. Only read command buffers after done closes.
func startFixtureProcess(t *testing.T, cmd *exec.Cmd) *integrationActor[error] {
	t.Helper()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	a := launchIntegrationActor(context.Background(), func(context.Context) error { return cmd.Wait() })
	t.Cleanup(func() {
		select {
		case <-a.done:
			return
		default:
		}
		if err := cmd.Process.Kill(); err != nil && !errors.Is(err, exec.ErrNotFound) {
			// Wait below establishes completion even if the process just exited.
			t.Logf("kill fixture child: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := a.wait(ctx); err != nil {
			t.Errorf("fixture child cleanup incomplete: %v", err)
		}
	})
	return a
}

func waitFixtureReady(t *testing.T, a *integrationActor[error], path string) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		select {
		case <-a.done:
			t.Fatalf("fixture child ended before readiness: %v", a.result)
		case <-deadline.C:
			t.Fatal("fixture child did not report readiness")
		case <-tick.C:
		}
	}
}
