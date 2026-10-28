package cephfs

import (
	"context"
	"errors"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
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
	clone      *SubvolumeClone
	control    *waitControl
	wait       func(context.Context) error
	finalQuery string
}

const cloneStatusQuery = "fs clone status fixture copy --group_name restores --format json"

func newWaitFixture(t *testing.T, name string) waitFixture {
	t.Helper()
	f := waitFixture{}
	switch name {
	case "Ready":
		f.fs, _, _, _ = cephFSContextAdmissionFixture()
		f.cluster = f.fs.cluster
		f.control = &waitControl{Container: f.cluster.Container}
		f.cluster.Container = f.control
		f.wait = f.fs.WaitReady
		f.finalQuery = "fs dump --format json"
	case "Clone":
		var base *cloneLifecycleFixtureContainer
		f.fs, base, f.clone = cloneLifecycleFixture(t)
		base.Output[cloneStatusQuery] = cloneLifecycleStatus("complete")
		base.Output["fs subvolume info fixture copy --group_name restores --format json"] = `{"path":"/volumes/restores/copy/10000000-0000-4000-8000-000000000001","bytes_quota":32768,"bytes_used":8192,"data_pool":"data","pool_namespace":"","created_at":"2026-10-03 00:02:00.123456","state":"complete","type":"clone"}`
		f.cluster = f.fs.cluster
		f.control = &waitControl{Container: base}
		f.cluster.Container = f.control
		f.wait = func(ctx context.Context) error { _, err := f.fs.WaitForSubvolumeClone(ctx, f.clone); return err }
		f.finalQuery = "fs subvolume info fixture copy --group_name restores --format json"
	default:
		t.Fatalf("unknown wait fixture %q", name)
	}
	testhelper.TestSetStartupTimeout(f.cluster, 2*time.Second)
	return f
}

func TestWaitAlreadyCanceledOrExpiredDoesNotQuery(t *testing.T) {
	for _, name := range []string{"Ready", "Clone"} {
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
	for _, name := range []string{"Ready", "Clone"} {
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
	for _, name := range []string{"Ready", "Clone"} {
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

// Cleanup is registered before launch. A regression can only be reported after
// cancel+release+join, so even an ordinary blocking Lock does not leak a worker.
func waitHeldGate(t *testing.T, lock, unlock func(), tryLock func() bool, wait func(context.Context) error, readUnlock ...func()) {
	t.Helper()
	lock()
	var release sync.Once
	releaseGate := func() { release.Do(unlock) }
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	result, exited := make(chan error, 1), make(chan struct{})
	join := func() {
		select {
		case <-exited:
		case <-time.After(time.Second):
			t.Error("wait worker did not exit after gate release")
		}
	}
	t.Cleanup(func() { cancel(); releaseGate(); join() })
	go func() { defer close(exited); result <- wait(ctx) }()
	select {
	case err := <-result:
		if err == nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("held admission lost deadline: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		cancel()
		releaseGate()
		join()
		t.Fatal("wait blocked beyond caller deadline while gate remained held")
	}
	if tryLock() {
		if len(readUnlock) != 0 {
			readUnlock[0]()
		} else {
			unlock()
		}
		t.Fatal("waiter released or stole original held gate")
	}
	releaseGate()
	join()
}

func TestWaitLateReadyCancellationNeverPublishesSuccess(t *testing.T) {
	for _, name := range []string{"Ready", "Clone"} {
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
			if name == "Clone" && f.clone.identity.subvolume == nil {
				t.Fatal("cancellation discarded positive native clone authority")
			}
			if err := f.wait(t.Context()); err != nil {
				t.Fatalf("fresh readiness failed after canceled observation: %v", err)
			}
		})
	}
}

func TestWaitCloneLateCancellationPreservesSharedHandle(t *testing.T) {
	f := newWaitFixture(t, "Clone")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	f.control.after = func(args []string) {
		if strings.Join(clustertest.ModuleArgs(args), " ") == f.finalQuery {
			cancel()
		}
	}
	result, err := f.fs.WaitForSubvolumeClone(ctx, f.clone)
	if result != nil || err == nil || !errors.Is(err, context.Canceled) || f.clone.identity.subvolume == nil {
		t.Fatalf("late cancel result=%v err=%v capture=%v", result, err, f.clone.identity.subvolume)
	}
	retained := f.clone.identity.subvolume
	copy := *f.clone
	f.control.after = nil
	recovered, err := f.fs.WaitForSubvolumeClone(t.Context(), &copy)
	if err != nil || recovered == nil || recovered.identity != retained.identity || recovered == retained {
		t.Fatalf("fresh copied request lost retained private identity: %v", err)
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

func TestWaitClonePendingObservationReleasesSetupGateBeforeDelay(t *testing.T) {
	f := newWaitFixture(t, "Clone")
	f.control.Container.(*cloneLifecycleFixtureContainer).Output[cloneStatusQuery] = cloneLifecycleStatus("pending")
	gate := &filesystems(f.cluster).setupMu
	entered := make(chan struct{})
	var signal sync.Once
	// The test must take the gate before the wait reads the clone status again;
	// a slow (race-instrumented) observation that still holds the gate cannot
	// expire this admission, only a wait that keeps it across its poll delay.
	admission := clustertest.NewSignalDeadline(t.Context())
	statusReads := 0
	f.control.after = func(args []string) {
		if strings.Join(clustertest.ModuleArgs(args), " ") == cloneStatusQuery {
			if statusReads++; statusReads > 1 {
				admission.Expire()
			}
			signal.Do(func() { close(entered) })
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	result, exited := make(chan error, 1), make(chan struct{})
	held := false
	join := func() {
		select {
		case <-exited:
		case <-time.After(time.Second):
			t.Error("pending wait worker remained after cancellation/release")
		}
	}
	t.Cleanup(func() {
		cancel()
		if held {
			gate.Unlock()
			held = false
		}
		join()
	})
	go func() { defer close(exited); result <- f.wait(ctx) }()
	select {
	case <-entered:
	case err := <-result:
		t.Fatalf("pending fixture completed before observation: %v", err)
	case <-time.After(time.Second):
		t.Fatal("pending observation did not execute")
	}
	if err := clusterutil.LockMutex(admission, gate); err != nil {
		t.Fatalf("wait held the setup gate during poll delay: %v", err)
	}
	held = true
	before := f.control.calls.Load()
	cancel()
	select {
	case err := <-result:
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("pending cancellation lost canonical cause: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		gate.Unlock()
		held = false
		join()
		t.Fatal("pending wait did not cancel while reusable gate was held")
	}
	if f.control.calls.Load() != before {
		t.Fatal("canceled pending wait issued another native query")
	}
	gate.Unlock()
	held = false
	join()
}

func TestWaitCompletedCloneReentryRetainsCaptureOnCancel(t *testing.T) {
	f := newWaitFixture(t, "Clone")
	first, err := f.fs.WaitForSubvolumeClone(t.Context(), f.clone)
	if err != nil || first == nil {
		t.Fatalf("initial complete clone: %v", err)
	}
	retained := f.clone.identity.subvolume
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	f.control.after = func(args []string) {
		if strings.Join(clustertest.ModuleArgs(args), " ") == cloneStatusQuery {
			cancel()
		}
	}
	result, err := f.fs.WaitForSubvolumeClone(ctx, f.clone)
	if result != nil || err == nil || !errors.Is(err, context.Canceled) || f.clone.identity.subvolume != retained || retained.identity != first.identity {
		t.Fatalf("reentry canceled result=%v err=%v", result, err)
	}
}
