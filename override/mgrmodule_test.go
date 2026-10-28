package override

import (
	"context"
	"errors"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// refusedMGR reports whether h is the non-nil handle of a call that changed
// nothing.
func refusedMGR(h *MGRModuleHandle) bool { return h != nil && h.state != nil && h.state.refused != nil }

func TestOverrideMGRModuleRestoresSharedCopiesAndOriginalMembership(t *testing.T) {
	for _, prior := range []bool{false, true} {
		ctr := newMGRModuleFixture()
		ctr.enabled["mirroring"] = prior
		c := mgrModuleCluster(ctr)
		change, err := SetMGRModule(t.Context(), c, "mirroring", !prior)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := SetMGRModule(t.Context(), c, "mirroring", prior); err == nil {
			t.Fatal("overlap accepted")
		}
		copy := *change
		if err := copy.Restore(t.Context()); err != nil {
			t.Fatal(err)
		}
		before := len(ctr.calls)
		if err := change.Restore(t.Context()); err != nil || len(ctr.calls) != before {
			t.Fatal("shared restoration is not idempotent")
		}
		if ctr.enabled["mirroring"] != prior || len(heldMGR(c)) != 0 || !ctr.enabled["rbd_support"] {
			t.Fatal("original or unrelated module state lost")
		}
	}
}

func TestOverrideMGRModuleInitialSnapshotWaitsForNativeConvergence(t *testing.T) {
	for _, operation := range []string{"apply", "restore"} {
		t.Run(operation, func(t *testing.T) {
			ctr := newMGRModuleFixture()
			c := mgrModuleCluster(ctr)
			setStartupTimeout(c, 3*time.Second)
			var change *MGRModuleHandle
			if operation == "restore" {
				var err error
				change, err = SetMGRModule(t.Context(), c, "mirroring", true)
				if err != nil {
					t.Fatal(err)
				}
			}
			beforeMutations, beforeDumps := ctr.mutations, ctr.dumps
			ctr.dumpResponses = []string{
				`{"available":false,"modules":[],"available_modules":[]}`,
				`{"available":true,"modules":["other"],"available_modules":[]}`,
			}
			if operation == "apply" {
				var err error
				change, err = SetMGRModule(t.Context(), c, "mirroring", true)
				if err != nil || change == nil || !ctr.enabled["mirroring"] {
					t.Fatal("transient native snapshots prevented apply", err)
				}
			} else if err := change.Restore(t.Context()); err != nil || ctr.enabled["mirroring"] {
				t.Fatal("transient native snapshots prevented restoration", err)
			}
			if len(ctr.dumpResponses) != 0 || ctr.dumps-beforeDumps < 4 || ctr.mutations-beforeMutations != 1 {
				t.Fatalf("native convergence must precede one mutation: dumps=%d mutations=%d pending=%d", ctr.dumps-beforeDumps, ctr.mutations-beforeMutations, len(ctr.dumpResponses))
			}
			if !ctr.enabled["rbd_support"] || !ctr.enabled["volumes"] {
				t.Fatal("unrelated module membership changed")
			}
			if operation == "apply" {
				if err := change.Restore(t.Context()); err != nil || len(heldMGR(c)) != 0 {
					t.Fatal("converged apply did not preserve restoration ownership", err)
				}
			} else if len(heldMGR(c)) != 0 || !change.state.restored {
				t.Fatal("successful restoration retained its lease")
			}
		})
	}
}

func TestOverrideMGRModuleInitialSnapshotFailurePreservesStateAndOwnership(t *testing.T) {
	for _, operation := range []string{"apply", "restore"} {
		for _, failure := range []string{"unavailable", "inconsistent", "canceled-before-read", "canceled-after-read"} {
			t.Run(operation+"/"+failure, func(t *testing.T) {
				ctr := newMGRModuleFixture()
				c := mgrModuleCluster(ctr)
				var change *MGRModuleHandle
				if operation == "restore" {
					var err error
					change, err = SetMGRModule(t.Context(), c, "mirroring", true)
					if err != nil {
						t.Fatal(err)
					}
				}
				beforeMutations, beforeCalls := ctr.mutations, len(ctr.calls)
				setStartupTimeout(c, 120*time.Millisecond)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				wantErr := error(context.DeadlineExceeded)
				switch failure {
				case "unavailable":
					ctr.rawDump = `{"available":false,"modules":[],"available_modules":[]}`
				case "inconsistent":
					ctr.rawDump = `{"available":true,"modules":["other"],"available_modules":[]}`
				case "canceled-before-read":
					cancel()
					wantErr = context.Canceled
				case "canceled-after-read":
					ctr.cancelOnDump = cancel
					wantErr = context.Canceled
				}
				started := time.Now()
				var err error
				if operation == "apply" {
					change, err = SetMGRModule(ctx, c, "mirroring", true)
					if !refusedMGR(change) || len(heldMGR(c)) != 0 || ctr.enabled["mirroring"] {
						t.Fatal("failed initial snapshot adopted or changed native membership")
					}
				} else {
					err = change.Restore(ctx)
					if change.state.restored || heldMGR(c)["mirroring"] != change || !ctr.enabled["mirroring"] {
						t.Fatal("failed initial snapshot lost applied membership or restoration ownership")
					}
				}
				if !errors.Is(err, wantErr) || time.Since(started) > time.Second || ctr.mutations != beforeMutations {
					t.Fatalf("initial read must fail within context without mutation: err=%v elapsed=%v mutations=%d", err, time.Since(started), ctr.mutations-beforeMutations)
				}
				if failure == "canceled-before-read" && len(ctr.calls) != beforeCalls {
					t.Fatal("canceled admission performed a native read")
				}
				if operation == "restore" {
					ctr.rawDump, ctr.cancelOnDump = "", nil
					if err := change.Restore(t.Context()); err != nil || ctr.enabled["mirroring"] || len(heldMGR(c)) != 0 {
						t.Fatal("retry after failed initial snapshot did not restore original membership", err)
					}
				}
			})
		}
	}
}

func TestOverrideMGRModuleRejectsMissingCannotRunAndAlwaysOn(t *testing.T) {
	for _, name := range []string{"--force", "bad name", "mirroring;exit", "unknown", "missing", "broken"} {
		ctr := newMGRModuleFixture()
		ctr.enabled["missing"], ctr.enabled["broken"] = false, false
		ctr.canRun["broken"] = false
		c := mgrModuleCluster(ctr)
		if change, err := SetMGRModule(t.Context(), c, name, true); err == nil || !refusedMGR(change) || len(heldMGR(c)) != 0 || ctr.mutations != 0 {
			t.Fatal("invalid/unavailable module reached mutation", name)
		}
	}
	ctr := newMGRModuleFixture()
	c := mgrModuleCluster(ctr)
	if _, err := SetMGRModule(t.Context(), c, "rbd_support", false); err == nil || ctr.mutations != 0 {
		t.Fatal("always-on module disabled")
	}
	change, err := SetMGRModule(t.Context(), c, "rbd_support", true)
	if err != nil || ctr.mutations != 0 {
		t.Fatal("always-on no-op rejected", err)
	}
	ctr.forceDisabled["rbd_support"] = true
	ctr.enabled["rbd_support"] = false
	if err := change.Restore(t.Context()); err == nil {
		t.Fatal("outside force-disable overwritten")
	}
	delete(ctr.forceDisabled, "rbd_support")
	ctr.enabled["rbd_support"] = true
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctr.forceDisabled["rbd_support"] = true
	ctr.enabled["rbd_support"] = false
	if _, err := SetMGRModule(t.Context(), c, "rbd_support", true); err == nil {
		t.Fatal("unrestorable always-on enable accepted")
	}
}

func TestOverrideMGRModuleProtectsFilesystemsAndNativeMirroringOnRestore(t *testing.T) {
	ctr := newMGRModuleFixture()
	c := mgrModuleCluster(ctr)
	change, err := SetMGRModule(t.Context(), c, "mirroring", true)
	if err != nil {
		t.Fatal(err)
	}
	ctr.mirrored = true
	before := ctr.mutations
	if err := change.Restore(t.Context()); err == nil || ctr.mutations != before || !ctr.enabled["mirroring"] || len(heldMGR(c)) != 1 {
		t.Fatal("new mirror policy did not prevent module restoration")
	}
	ctr.mirrored = false
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	// A non-always-on volumes variant still protects native filesystems.
	delete(ctr.always, "volumes")
	ctr.filesystems = 1
	if _, err := SetMGRModule(t.Context(), c, "volumes", false); err == nil {
		t.Fatal("native filesystem's volumes module disabled")
	}
}

func TestOverrideMGRModuleNoopRefusesOutsideMembershipChange(t *testing.T) {
	ctr := newMGRModuleFixture()
	c := mgrModuleCluster(ctr)
	change, err := SetMGRModule(t.Context(), c, "mirroring", false)
	if err != nil {
		t.Fatal(err)
	}
	ctr.enabled["mirroring"] = true
	if err := change.Restore(t.Context()); err == nil || ctr.mutations != 0 || !ctr.enabled["mirroring"] {
		t.Fatal("outside edit was overwritten by no-op restore")
	}
}

func TestOverrideMGRModuleLostRepliesAndReadbackReconcile(t *testing.T) {
	ctr := newMGRModuleFixture()
	ctr.failAfter = "enable"
	c := mgrModuleCluster(ctr)
	change, err := SetMGRModule(t.Context(), c, "mirroring", true)
	if err == nil || change == nil || !ctr.enabled["mirroring"] || heldMGR(c)["mirroring"] != change {
		t.Fatal("uncertain apply lost handle")
	}
	ctr.failAfter = "disable"
	if err := change.Restore(t.Context()); err == nil || ctr.enabled["mirroring"] {
		t.Fatal("uncertain restoration reply hidden")
	}
	ctr.failAfter = ""
	before := ctr.mutations
	if err := change.Restore(t.Context()); err != nil || ctr.mutations != before || len(heldMGR(c)) != 0 {
		t.Fatal("native completed restore wasn't reconciled")
	}
	ctr.failDumpAt = ctr.dumps + 2
	change, err = SetMGRModule(t.Context(), c, "mirroring", true)
	if err != nil || change == nil {
		t.Fatal("transient apply readback did not converge", err)
	}
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctr.failDumpFrom = ctr.dumps + 2
	change, err = SetMGRModule(t.Context(), c, "mirroring", true)
	if err == nil || change == nil || len(heldMGR(c)) != 1 {
		t.Fatal("lost persistent apply readback lost handle")
	}
	ctr.failDumpFrom = 0
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestOverrideMGRModuleRestorationRequiresRunnableMetadata(t *testing.T) {
	ctr := newMGRModuleFixture()
	ctr.enabled["mirroring"] = true
	c := mgrModuleCluster(ctr)
	change, err := SetMGRModule(t.Context(), c, "mirroring", false)
	if err != nil {
		t.Fatal(err)
	}
	ctr.canRun["mirroring"] = false
	before := ctr.mutations
	if err := change.Restore(t.Context()); err == nil || ctr.mutations != before {
		t.Fatal("cannot-run restoration forced enable")
	}
	ctr.canRun["mirroring"] = true
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestOverrideMGRModuleCancellationKeepsAppliedHandleWithFreshReadback(t *testing.T) {
	ctr := newMGRModuleFixture()
	ctx, cancel := context.WithCancel(t.Context())
	ctr.cancelOnSet = cancel
	c := mgrModuleCluster(ctr)
	change, err := SetMGRModule(ctx, c, "mirroring", true)
	if !errors.Is(err, context.Canceled) || change == nil || !ctr.enabled["mirroring"] {
		t.Fatal("canceled apply lost native state/handle", err)
	}
	if ctr.canceledReads != 0 {
		t.Fatal("apply readback reused canceled context")
	}
	if heldMGR(c)["mirroring"] != change {
		t.Fatal("the canceled apply did not track its handle before the write")
	}
	// The readback after the canceled write ran on a fresh context: a native
	// read follows the enable command.
	enableAt := -1
	for i, call := range ctr.calls {
		if slices.Equal(call, []string{"mgr", "module", "enable", "mirroring"}) {
			enableAt = i
		}
	}
	if enableAt < 0 || enableAt == len(ctr.calls)-1 {
		t.Fatalf("no native readback followed the canceled write: %v", ctr.calls)
	}
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestSetMGRModuleNilClusterReturnsNotAppliedHandle(t *testing.T) {
	handle, err := SetMGRModule(t.Context(), nil, "mirroring", true)
	var op *ceph.OperationError
	if !errors.Is(err, ceph.ErrInvalidRequest) || !errors.As(err, &op) || op.Op != "override.SetMGRModule" {
		t.Fatalf("a nil cluster was not refused as an invalid request: %v", err)
	}
	if !refusedMGR(handle) {
		t.Fatalf("a nil cluster returned %+v", handle)
	}
	if err := handle.Restore(t.Context()); err != nil {
		t.Fatalf("the restore of a not-applied handle: %v", err)
	}
	// The nil cluster is reported before the input error.
	if _, err := SetMGRModule(t.Context(), nil, "--force", true); err == nil || !strings.Contains(err.Error(), "the cluster is nil") {
		t.Fatalf("a nil cluster with an invalid name reported %v", err)
	}
}

func TestSetMGRModuleSharedClusterIsRefusedBeforeAnyEffect(t *testing.T) {
	ctr := newMGRModuleFixture()
	c := testhelper.TestCluster(ctr, testhelper.TestConfig{Shared: true, StartupTimeout: time.Second})
	handle, err := SetMGRModule(t.Context(), c, "--force", true)
	var op *ceph.OperationError
	if !errors.Is(err, ceph.ErrSharedCluster) || !errors.As(err, &op) || op.Op != "override.SetMGRModule" {
		t.Fatalf("a shared cluster was not refused with its class and Op: %v", err)
	}
	if !refusedMGR(handle) || len(ctr.calls) != 0 || len(heldMGR(c)) != 0 {
		t.Fatalf("a refusal returned %+v after %d commands", handle, len(ctr.calls))
	}
}

func TestSetMGRModuleInvalidNameKeepsTextAndClass(t *testing.T) {
	ctr := newMGRModuleFixture()
	c := mgrModuleCluster(ctr)
	for _, name := range []string{"", "--force", "bad name", "Mirroring", "mirroring;exit"} {
		handle, err := SetMGRModule(t.Context(), c, name, true)
		if err == nil || err.Error() != "invalid Ceph manager module name" || errors.Is(err, ceph.ErrInvalidRequest) {
			t.Fatalf("name %q: the refusal changed: %v", name, err)
		}
		if !refusedMGR(handle) || len(ctr.calls) != 0 || len(heldMGR(c)) != 0 {
			t.Fatalf("name %q reached the cluster", name)
		}
	}
}

func TestNotAppliedMGRModuleRestoreIsANoOpThatTakesNoLock(t *testing.T) {
	ctr := newMGRModuleFixture()
	c := mgrModuleCluster(ctr)
	handle, err := SetMGRModule(t.Context(), c, "rbd_support", false)
	if err == nil || !refusedMGR(handle) {
		t.Fatalf("setup: %v", err)
	}
	calls := len(ctr.calls)
	// Hold the cluster mutex: a restore that waited for it would never return.
	servicehook.Lock(c)
	done := make(chan error, 1)
	go func() { done <- handle.Restore(t.Context()) }()
	select {
	case err := <-done:
		servicehook.Unlock(c)
		if err != nil {
			t.Fatalf("a not-applied restore returned %v", err)
		}
	case <-time.After(5 * time.Second):
		servicehook.Unlock(c)
		if err := <-done; err != nil {
			t.Fatalf("the late restore returned %v", err)
		}
		t.Fatal("a not-applied restore waited for the cluster lock")
	}
	copy := *handle
	if err := copy.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if handle.state.restored || len(ctr.calls) != calls || ctr.mutations != 0 || len(heldMGR(c)) != 0 {
		t.Fatal("a not-applied restore recorded a restoration, ran a command or touched the registry")
	}
}

func TestMGRModuleZeroAndNilHandlesAreUnavailable(t *testing.T) {
	var missing *MGRModuleHandle
	if err := missing.Restore(t.Context()); err == nil {
		t.Fatal("nil handle restored")
	}
	if err := (&MGRModuleHandle{}).Restore(t.Context()); err == nil {
		t.Fatal("zero handle restored")
	}
}

func TestSetMGRModuleAlreadyInRequestedStateHoldsALeaseWithoutAWrite(t *testing.T) {
	ctr := newMGRModuleFixture()
	c := mgrModuleCluster(ctr)
	handle, err := SetMGRModule(t.Context(), c, "mirroring", false)
	if err != nil || refusedMGR(handle) || ctr.mutations != 0 || len(heldMGR(c)) != 1 {
		t.Fatalf("a no-op lease: %v %+v", err, handle)
	}
	if err := handle.Restore(t.Context()); err != nil || ctr.mutations != 0 || len(heldMGR(c)) != 0 {
		t.Fatalf("restoring a no-op lease: %v", err)
	}
}

func TestSetMGRModuleOverlapIsRefusedWithoutDisturbingTheHeldModule(t *testing.T) {
	ctr := newMGRModuleFixture()
	c := mgrModuleCluster(ctr)
	first, err := SetMGRModule(t.Context(), c, "mirroring", true)
	if err != nil {
		t.Fatal(err)
	}
	second, err := SetMGRModule(t.Context(), c, "mirroring", false)
	if err == nil || err.Error() != "restore the existing MGR module override first" || errors.Is(err, ceph.ErrInvalidRequest) || !refusedMGR(second) {
		t.Fatalf("overlap: %v", err)
	}
	if heldMGR(c)["mirroring"] != first || ctr.mutations != 1 {
		t.Fatal("a refused overlap disturbed the held module")
	}
	if err := second.Restore(t.Context()); err != nil || heldMGR(c)["mirroring"] != first || !ctr.enabled["mirroring"] {
		t.Fatal("the not-applied handle released the module of the first")
	}
	if err := first.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestSetMGRModuleEarlyReturnsLeaveNoReservation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		module  string
		enabled bool
		arrange func(*mgrModuleFixture)
	}{
		{"listing fails", "mirroring", true, func(ctr *mgrModuleFixture) { ctr.failDumpFrom = 1 }},
		{"module missing", "missing", true, func(*mgrModuleFixture) {}},
		{"always-on disable", "rbd_support", false, func(*mgrModuleFixture) {}},
		{"cannot run", "mirroring", true, func(ctr *mgrModuleFixture) { ctr.canRun["mirroring"] = false }},
		{"protection refuses", "mirroring", false, func(ctr *mgrModuleFixture) { ctr.enabled["mirroring"], ctr.mirrored = true, true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctr := newMGRModuleFixture()
			c := mgrModuleCluster(ctr)
			setStartupTimeout(c, 200*time.Millisecond)
			tc.arrange(ctr)
			if change, err := SetMGRModule(t.Context(), c, tc.module, tc.enabled); err == nil || !refusedMGR(change) || ctr.mutations != 0 {
				t.Fatalf("the attempt did not fail before mutation: %v %v", change, err)
			}
			if held := heldMGR(c); len(held) != 0 {
				t.Fatalf("%s kept a registry entry: %v", tc.name, held)
			}
			if tc.module != "mirroring" {
				return
			}
			// The key is free again for a retry.
			ctr.failDumpFrom, ctr.mirrored = 0, false
			ctr.canRun["mirroring"] = true
			change, err := SetMGRModule(t.Context(), c, "mirroring", ctr.enabled["mirroring"])
			if err != nil || refusedMGR(change) || len(heldMGR(c)) != 1 {
				t.Fatalf("a freed key stayed refused: %v", err)
			}
		})
	}
}

// A handle whose state is a copy of a restored predecessor's is stale: its
// key now maps to the successor, and Restore must refuse it without a native
// call or a change to the successor.
func TestMGRModuleRestoreRefusesStaleAndUntrackedHandles(t *testing.T) {
	ctr := newMGRModuleFixture()
	c := mgrModuleCluster(ctr)
	first, err := SetMGRModule(t.Context(), c, "mirroring", true)
	if err != nil {
		t.Fatal(err)
	}
	stale := &MGRModuleHandle{owner: c, name: "mirroring", state: &mgrModuleState{applied: true, token: first.state.token}}
	if err := first.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	second, err := SetMGRModule(t.Context(), c, "mirroring", true)
	if err != nil {
		t.Fatal(err)
	}
	before := len(ctr.calls)
	if err := stale.Restore(t.Context()); err == nil || err.Error() != "MGR module override is not tracked by this cluster" || len(ctr.calls) != before || heldMGR(c)["mirroring"] != second {
		t.Fatalf("a stale handle was not refused: %v", err)
	}
	// A handle with no token never maps to a key.
	forged := &MGRModuleHandle{owner: c, name: "mirroring", state: &mgrModuleState{applied: true}}
	if err := forged.Restore(t.Context()); err == nil || err.Error() != "MGR module override is not tracked by this cluster" || len(ctr.calls) != before {
		t.Fatalf("an untracked handle was not refused: %v", err)
	}
}

func TestMGRModuleRestoreAfterTerminateIsRefusedAndKeepsTheModule(t *testing.T) {
	ctr := newMGRModuleFixture()
	c := mgrModuleCluster(ctr)
	change, err := SetMGRModule(t.Context(), c, "mirroring", true)
	if err != nil {
		t.Fatal(err)
	}
	view := testhelper.TestState(c)
	view.Mutex.Lock()
	testhelper.TestSetClosed(c, true)
	view.Mutex.Unlock()
	before := len(ctr.calls)
	if err := change.Restore(t.Context()); err != overridehook.Terminated() {
		t.Fatalf("a restore on a terminated cluster returned %v", err)
	}
	if len(ctr.calls) != before || change.state.restored || len(heldMGR(c)) != 1 || !ctr.enabled["mirroring"] {
		t.Fatal("a restore after Terminate ran a command or forgot the module")
	}
	later, err := SetMGRModule(t.Context(), c, "volumes", true)
	if err != overridehook.Terminated() || !refusedMGR(later) || len(ctr.calls) != before {
		t.Fatalf("a set on a terminated cluster: %v", err)
	}
}
