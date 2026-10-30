package ceph

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// The wrapper can cancel after a valid reply without making Exec itself fail.
// This exercises the wait's own final cancellation check rather than relying on
// a cooperative Docker implementation to reject an already-canceled request.
type waitContextControl struct {
	testcontainers.Container
	calls        atomic.Int64
	after        func([]string)
	failureQuery string
	failure      error
}

func (c *waitContextControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.calls.Add(1)
	code, reader, err := c.Container.Exec(ctx, args, opts...)
	if c.after != nil {
		c.after(args)
	}
	if c.failure != nil && strings.Join(monitorQuorumTestModuleArgs(args), " ") == c.failureQuery {
		return 0, nil, c.failure
	}
	return code, reader, err
}

type waitContextFixture struct {
	cluster    *Container
	control    *waitContextControl
	wait       func(context.Context) error
	finalQuery string
}

func newWaitContextFixture(t *testing.T, name string) waitContextFixture {
	t.Helper()
	f := waitContextFixture{}
	switch name {
	case "Clean", "Quorum", "PGClean":
		base := &poolFixtureContainer{Output: map[string]string{
			"status --format json":        `{"mgrmap":{"available":true},"osdmap":{"num_osds":1,"num_up_osds":1,"num_in_osds":1},"pgmap":{"num_pgs":8,"pgs_by_state":[{"state_name":"active+clean","count":8}]}}`,
			"quorum_status --format json": `{"quorum_names":["a"],"monmap":{"mons":[{"name":"a"}]}}`,
		}}
		f.cluster = poolFixtureCluster(base, 1)
		f.control = &waitContextControl{Container: base}
		f.cluster.Container = f.control
		f.finalQuery = "status --format json"
		if name == "Clean" {
			f.wait = f.cluster.WaitForClean
		}
		if name == "PGClean" {
			f.wait = f.cluster.WaitForPGClean
		}
		if name == "Quorum" {
			f.wait = f.cluster.WaitForQuorum
			f.finalQuery = "quorum_status --format json"
		}
	case "Module":
		base := newMGRModuleFixture()
		f.cluster = mgrModuleCluster(base)
		f.control = &waitContextControl{Container: base}
		f.cluster.Container = f.control
		f.wait = func(ctx context.Context) error { return f.cluster.WaitMGRModuleReady(ctx, "rbd_support") }
		f.finalQuery = "rbd mirror snapshot schedule list --format json"
	default:
		t.Fatalf("unknown wait fixture %q", name)
	}
	f.cluster.settings.startupTimeout = 2 * time.Second
	return f
}

func TestWaitContextPollRejectsCanceledObservation(t *testing.T) {
	for _, phase := range []string{"before", "after-ready", "after-not-ready", "after-error"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if phase == "before" {
				cancel()
			}
			calls := 0
			cause := errors.New("last native observation failed")
			err := clusterutil.Poll(ctx, func() (bool, error) {
				calls++
				cancel()
				if phase == "after-error" {
					return true, cause
				}
				return phase != "after-not-ready", nil
			})
			if err == nil || !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled observation published success: %v", err)
			}
			wantCalls := 1
			if phase == "before" {
				wantCalls = 0
			}
			if calls != wantCalls {
				t.Fatalf("canceled callback count=%d want=%d", calls, wantCalls)
			}
			if phase == "after-error" && !errors.Is(err, cause) {
				t.Fatalf("lost last observation cause: %v", err)
			}
		})
	}
}

func TestWaitContextPollRetainsLatestCauseAndTransientSuccess(t *testing.T) {
	t.Run("latest", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		first, latest := errors.New("first failure"), errors.New("latest failure")
		calls := 0
		err := clusterutil.Poll(ctx, func() (bool, error) {
			calls++
			if calls == 1 {
				return false, first
			}
			cancel()
			return false, latest
		})
		if !errors.Is(err, context.Canceled) || !errors.Is(err, latest) || errors.Is(err, first) || calls != 2 {
			t.Fatalf("last cause/callback count changed: %v calls=%d", err, calls)
		}
	})
	t.Run("deadline", func(t *testing.T) {
		// The deadline passes during the first observation rather than after a
		// wall-clock interval that a slow (race-instrumented) run can spend
		// before poll observes at all.
		ctx := clustertest.NewSignalDeadline(t.Context())
		cause := errors.New("native observation before timeout")
		calls := 0
		err := clusterutil.Poll(ctx, func() (bool, error) { calls++; ctx.Expire(); return false, cause })
		if err == nil || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, cause) || calls != 1 {
			t.Fatalf("deadline lost prior observation: %v calls=%d", err, calls)
		}
	})
	t.Run("transient", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		calls := 0
		err := clusterutil.Poll(ctx, func() (bool, error) {
			calls++
			if calls == 1 {
				return false, errors.New("transient")
			}
			return true, nil
		})
		if err != nil || calls != 2 {
			t.Fatalf("transient convergence failed: %v calls=%d", err, calls)
		}
	})
}

func TestWaitContextAlreadyCanceledOrExpiredDoesNotQuery(t *testing.T) {
	for _, name := range []string{"Clean", "Quorum", "PGClean", "Module"} {
		for _, expired := range []bool{false, true} {
			label := name + "/canceled"
			if expired {
				label = name + "/expired"
			}
			t.Run(label, func(t *testing.T) {
				f := newWaitContextFixture(t, name)
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

func TestWaitContextHeldOwnerAdmissionDoesNotQuery(t *testing.T) {
	for _, name := range []string{"Clean", "PGClean", "Module"} {
		t.Run(name, func(t *testing.T) {
			f := newWaitContextFixture(t, name)
			waitContextHeldGate(t, f.cluster.mu.Lock, f.cluster.mu.Unlock, f.cluster.mu.TryLock, f.wait)
			if f.control.calls.Load() != 0 {
				t.Fatal("busy owner issued native query")
			}
			if err := f.wait(t.Context()); err != nil {
				t.Fatalf("fresh waiter could not reuse owner: %v", err)
			}
		})
	}
}

func TestWaitContextHeldControlAdmissionDoesNotQuery(t *testing.T) {
	for _, name := range []string{"Clean", "Quorum", "PGClean", "Module"} {
		t.Run(name, func(t *testing.T) {
			f := newWaitContextFixture(t, name)
			waitContextHeldGate(t, f.cluster.controlPlane.mu.Lock, f.cluster.controlPlane.mu.Unlock, f.cluster.controlPlane.mu.TryRLock, f.wait, f.cluster.controlPlane.mu.RUnlock)
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
func waitContextHeldGate(t *testing.T, lock, unlock func(), tryLock func() bool, wait func(context.Context) error, readUnlock ...func()) {
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

func TestWaitContextLateReadyCancellationNeverPublishesSuccess(t *testing.T) {
	for _, name := range []string{"Clean", "Quorum", "PGClean", "Module"} {
		t.Run(name, func(t *testing.T) {
			f := newWaitContextFixture(t, name)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reached := false
			f.control.after = func(args []string) {
				if strings.Join(monitorQuorumTestModuleArgs(args), " ") == f.finalQuery {
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

func TestWaitContextCleanRetainsInitialOwnedCount(t *testing.T) {
	f := newWaitContextFixture(t, "Clean")
	original := f.cluster.osdHandles.get(0)
	f.control.after = func(args []string) {
		if strings.Join(monitorQuorumTestModuleArgs(args), " ") == f.finalQuery {
			f.cluster.osdHandles.register(1, &OSDContainer{ID: 1})
		}
	}
	if err := f.wait(t.Context()); err != nil || f.cluster.osdHandles.get(0) != original || f.cluster.osdHandles.count() != 2 {
		t.Fatalf("wait adopted later count or changed original descriptor: %v", err)
	}
}

func TestWaitContextCanceledProbeRetainsNativeCause(t *testing.T) {
	for _, name := range []string{"Clean", "Quorum", "PGClean", "Module"} {
		t.Run(name, func(t *testing.T) {
			f := newWaitContextFixture(t, name)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			cause := errors.New("injected final native probe failure")
			f.control.failureQuery, f.control.failure = f.finalQuery, cause
			f.control.after = func(args []string) {
				if strings.Join(monitorQuorumTestModuleArgs(args), " ") == f.finalQuery {
					cancel()
				}
			}
			err := f.wait(ctx)
			if err == nil || !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
				t.Fatalf("canceled probe masked native cause: %v", err)
			}
		})
	}
}

func TestWaitContextPendingObservationReleasesGateBeforeDelay(t *testing.T) {
	for _, name := range []string{"PGClean", "Module"} {
		t.Run(name, func(t *testing.T) {
			f := newWaitContextFixture(t, name)
			query := f.finalQuery
			gate := &f.cluster.mu
			switch name {
			case "PGClean":
				f.control.Container.(*poolFixtureContainer).Output[query] = `{"mgrmap":{"available":false},"pgmap":{"num_pgs":8}}`
			case "Module":
				f.control.Container.(*mgrModuleFixture).rawDump = `{"available":true,"modules":[],"available_modules":null}`
				query = "mgr dump --format json"
			}
			entered := make(chan struct{})
			var signal sync.Once
			// The admission below fails once the wait queries again after the
			// pending observation, that is once a whole poll delay passed without
			// the test getting the gate, not after a fixed wall-clock interval.
			admission := clustertest.NewSignalDeadline(t.Context())
			f.control.after = func(args []string) {
				select {
				case <-entered:
					admission.Expire()
					return
				default:
				}
				if strings.Join(monitorQuorumTestModuleArgs(args), " ") == query {
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
			err := lockTopologyMutex(admission, gate)
			if err != nil {
				t.Fatalf("wait held owner/setup during poll delay: %v", err)
			}
			held = true
			before := f.control.calls.Load()
			cancel()
			select {
			case err := <-result:
				if err == nil || !errors.Is(err, context.Canceled) {
					t.Fatalf("pending cancellation lost canonical cause: %v", err)
				}
				if name == "Module" && !strings.Contains(err.Error(), "decode active MGR module metadata: missing or invalid native dump") {
					t.Fatalf("pending cancellation lost last invalid-metadata observation: %v", err)
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
		})
	}
}
