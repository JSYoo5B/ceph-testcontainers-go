package cephfs

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fstest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// waitControl counts native queries and can act after a reply, for example
// cancel the caller, without making Exec itself fail.
type waitControl struct {
	testcontainers.Container
	calls        atomic.Int64
	after        func([]string)
	failureQuery string
	failure      error
}

func (c *waitControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.calls.Add(1)
	code, reader, err := c.Container.Exec(ctx, args, opts...)
	if c.after != nil {
		c.after(args)
	}
	if c.failure != nil && strings.Join(clustertest.ModuleArgs(args), " ") == c.failureQuery {
		return 0, nil, c.failure
	}
	return code, reader, err
}

type waitFixture struct {
	cluster    *ceph.Container
	fs         *Filesystem
	control    *waitControl
	wait       func(context.Context) error
	finalQuery string
}

func newWaitFixture(t *testing.T, name string) waitFixture {
	t.Helper()
	f := waitFixture{}
	switch name {
	case "Ready":
		f.fs, _, _, _ = cephFSContextAdmissionFixture()
		f.cluster = f.fs.base().Cluster()
		f.control = &waitControl{Container: f.cluster.Container}
		f.cluster.Container = f.control
		f.wait = f.fs.WaitReady
		f.finalQuery = "fs dump --format json"
	default:
		t.Fatalf("unknown wait fixture %q", name)
	}
	testhelper.TestSetStartupTimeout(f.cluster, 2*time.Second)
	return f
}

func TestWaitAlreadyCanceledOrExpiredDoesNotQuery(t *testing.T) {
	for _, name := range []string{"Ready"} {
		for _, expired := range []bool{false, true} {
			label := name + "/canceled"
			if expired {
				label = name + "/expired"
			}
			t.Run(label, func(t *testing.T) {
				f := newWaitFixture(t, name)
				ctx, cancel := context.WithCancel(t.Context())
				want := error(context.Canceled)
				if expired {
					cancel()
					ctx, cancel = context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
					want = context.DeadlineExceeded
				} else {
					cancel()
				}
				defer cancel()
				err := f.wait(ctx)
				if err == nil || !errors.Is(err, want) || f.control.calls.Load() != 0 {
					t.Fatalf("inactive caller queried/published readiness: err=%v queries=%d", err, f.control.calls.Load())
				}
			})
		}
	}
}

func TestWaitHeldOwnerAdmissionDoesNotQuery(t *testing.T) {
	for _, name := range []string{"Ready"} {
		t.Run(name, func(t *testing.T) {
			f := newWaitFixture(t, name)
			mutex := testhelper.TestState(f.cluster).Mutex
			waitHeldGate(t, mutex.Lock, mutex.Unlock, mutex.TryLock, f.wait)
			if f.control.calls.Load() != 0 {
				t.Fatal("busy owner issued native query")
			}
			if err := f.wait(t.Context()); err != nil {
				t.Fatalf("fresh waiter could not reuse owner: %v", err)
			}
		})
	}
}

func TestWaitHeldControlAdmissionDoesNotQuery(t *testing.T) {
	for _, name := range []string{"Ready"} {
		t.Run(name, func(t *testing.T) {
			f := newWaitFixture(t, name)
			control := testhelper.TestState(f.cluster).ControlMutex
			waitHeldGate(t, control.Lock, control.Unlock, control.TryRLock, f.wait, control.RUnlock)
			if f.control.calls.Load() != 0 {
				t.Fatal("busy control issued native query")
			}
			if err := f.wait(t.Context()); err != nil {
				t.Fatalf("fresh waiter could not reuse control: %v", err)
			}
		})
	}
}

func waitHeldGate(t *testing.T, lock, unlock func(), tryLock func() bool, wait func(context.Context) error, readUnlock ...func()) {
	t.Helper()
	fstest.AssertWaitHeldGate(t, lock, unlock, tryLock, wait, readUnlock...)
}

func TestWaitLateReadyCancellationNeverPublishesSuccess(t *testing.T) {
	for _, name := range []string{"Ready"} {
		t.Run(name, func(t *testing.T) {
			f := newWaitFixture(t, name)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reached := false
			f.control.after = func(args []string) {
				if strings.Join(clustertest.ModuleArgs(args), " ") == f.finalQuery {
					reached = true
					cancel()
				}
			}
			err := f.wait(ctx)
			if !reached || err == nil || !errors.Is(err, context.Canceled) {
				t.Fatalf("late valid reply published readiness: reached=%v err=%v", reached, err)
			}
			f.control.after = nil
			if err := f.wait(t.Context()); err != nil {
				t.Fatalf("fresh readiness failed after canceled observation: %v", err)
			}
		})
	}
}

func TestWaitReadyCanceledProbeRetainsNativeCause(t *testing.T) {
	f := newWaitFixture(t, "Ready")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cause := errors.New("injected final native probe failure")
	f.control.failureQuery, f.control.failure = f.finalQuery, cause
	f.control.after = func(args []string) {
		if strings.Join(clustertest.ModuleArgs(args), " ") == f.finalQuery {
			cancel()
		}
	}
	err := f.wait(ctx)
	if err == nil || !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
		t.Fatalf("canceled probe masked native cause: %v", err)
	}
}
