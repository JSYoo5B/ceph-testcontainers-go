package clustertest

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// NativeCallCounter counts native calls a queued operation must not make.
type NativeCallCounter struct {
	testcontainers.Container
	Execs, Stops, Terminations atomic.Int32
}

func (runtime *NativeCallCounter) GetContainerID() string { return "context-lock-runtime" }

func (runtime *NativeCallCounter) Exec(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
	runtime.Execs.Add(1)
	return 1, nil, errors.New("unexpected native command while queued")
}

func (runtime *NativeCallCounter) Stop(context.Context, *time.Duration) error {
	runtime.Stops.Add(1)
	return nil
}

func (runtime *NativeCallCounter) Terminate(ctx context.Context, _ ...testcontainers.TerminateOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	runtime.Terminations.Add(1)
	return nil
}

// AssertContextLockQueued holds mutex and checks that operation returns its
// own short deadline without taking or releasing the busy gate.
func AssertContextLockQueued(t testing.TB, mutex *sync.Mutex, operation func(context.Context) error) {
	t.Helper()
	mutex.Lock()
	locked := true
	defer func() {
		if locked {
			mutex.Unlock()
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- operation(ctx) }()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("queued operation returned %v; want caller deadline", err)
		}
	case <-time.After(500 * time.Millisecond):
		mutex.Unlock()
		locked = false
		select {
		case <-result:
		case <-time.After(time.Second):
		}
		t.Fatal("queued operation ignored its short caller deadline")
	}
	if mutex.TryLock() {
		mutex.Unlock()
		t.Fatal("caller stole or unlocked the busy fixture gate")
	}
	mutex.Unlock()
	locked = false
}

// ControlQueryCounter counts control queries that admission must not reach.
type ControlQueryCounter struct {
	testcontainers.Container
	Calls *atomic.Int32
}

func (c *ControlQueryCounter) GetContainerID() string {
	c.Calls.Add(1)
	return "unexpected-control-query"
}

// TerminateRecorder records Terminate calls and returns Err.
type TerminateRecorder struct {
	testcontainers.Container
	Calls int
	Err   error
}

func (c *TerminateRecorder) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	c.Calls++
	return c.Err
}
