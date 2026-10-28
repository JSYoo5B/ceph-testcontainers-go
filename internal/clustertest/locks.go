//go:build test_support

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

// ContextLockRuntime counts native calls a queued operation must not make.
type ContextLockRuntime struct {
	testcontainers.Container
	Execs, Stops, Terminations atomic.Int32
}

func (runtime *ContextLockRuntime) GetContainerID() string { return "context-lock-runtime" }

func (runtime *ContextLockRuntime) Exec(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
	runtime.Execs.Add(1)
	return 1, nil, errors.New("unexpected native command while queued")
}

func (runtime *ContextLockRuntime) Stop(context.Context, *time.Duration) error {
	runtime.Stops.Add(1)
	return nil
}

func (runtime *ContextLockRuntime) Terminate(ctx context.Context, _ ...testcontainers.TerminateOption) error {
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

// AdmissionContainer counts control queries that admission must not reach.
type AdmissionContainer struct {
	testcontainers.Container
	Calls *atomic.Int32
}

func (c *AdmissionContainer) GetContainerID() string {
	c.Calls.Add(1)
	return "unexpected-control-query"
}

// CleanupContainer records Terminate calls and returns Err.
type CleanupContainer struct {
	testcontainers.Container
	Calls int
	Err   error
}

func (c *CleanupContainer) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	c.Calls++
	return c.Err
}
