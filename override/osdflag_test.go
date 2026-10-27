package override

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// heldOSDFlags returns the OSD flag handles that the cluster's registry holds.
// A nil value is a reservation that was never settled.
func heldOSDFlags(c *ceph.Container) map[string]any {
	servicehook.Lock(c)
	defer servicehook.Unlock(c)
	return overridehook.TestHeld(c, overridehook.OSDFlag)
}

func TestOSDFlagConstantsAreExactlyTheReversibleSet(t *testing.T) {
	want := map[ceph.OSDFlag]string{
		ceph.OSDFlagNoUp: "noup", ceph.OSDFlagNoDown: "nodown", ceph.OSDFlagNoOut: "noout", ceph.OSDFlagNoIn: "noin",
		ceph.OSDFlagNoBackfill: "nobackfill", ceph.OSDFlagNoRebalance: "norebalance", ceph.OSDFlagNoRecover: "norecover",
		ceph.OSDFlagNoScrub: "noscrub", ceph.OSDFlagNoDeepScrub: "nodeep-scrub", ceph.OSDFlagNoSnapTrim: "nosnaptrim",
		ceph.OSDFlagNoAutoscale: "noautoscale",
	}
	if len(want) != 11 || len(supportedOSDFlags) != 11 {
		t.Fatalf("flag set sizes: %d constants, %d accepted", len(want), len(supportedOSDFlags))
	}
	for flag, token := range want {
		if string(flag) != token || flag.String() != token || !slices.Contains(supportedOSDFlags, flag) {
			t.Fatalf("constant %q is not the accepted token %q", flag, token)
		}
	}
}

func TestSetOSDFlagAcceptsEveryConstantAndRefusesOthers(t *testing.T) {
	for _, flag := range supportedOSDFlags {
		c, native := newOSDFlagFixture(t)
		change, err := SetOSDFlag(t.Context(), c, flag, true)
		if err != nil || !slices.Contains(native.flags, flag.String()) {
			t.Fatalf("%s was not applied: %v", flag, err)
		}
		if err := change.Restore(t.Context()); err != nil || slices.Contains(native.flags, flag.String()) {
			t.Fatalf("%s was not restored: %v", flag, err)
		}
	}
	// A value outside the constants is representable in the open type and is
	// refused before any native command, even a token the cluster reports.
	for _, flag := range []ceph.OSDFlag{"sortbitwise", "pause", "pauserd", "full", "pglog_hardlimit", "norecover,noout", "noout ", "--noout", "NOOUT", ""} {
		c, native := newOSDFlagFixture(t, "sortbitwise")
		handle, err := SetOSDFlag(t.Context(), c, flag, false)
		if err == nil || err.Error() != "unsupported reversible global OSD flag" || handle == nil || handle.state.refused == nil || len(native.calls) != 0 || len(heldOSDFlags(c)) != 0 {
			t.Fatalf("flag %q: handle %v, err %v, %d native calls", flag, handle, err, len(native.calls))
		}
		if err := handle.Restore(t.Context()); err != nil {
			t.Fatalf("flag %q: the restore of a not-applied handle: %v", flag, err)
		}
	}
}

func TestSetOSDFlagRestoresOriginalAndLeavesOtherTokens(t *testing.T) {
	for _, original := range []bool{false, true} {
		flags := []string{"sortbitwise", "noscrub", "noout-unrelated"}
		if original {
			flags = append(flags, "noout")
		}
		c, native := newOSDFlagFixture(t, flags...)
		change, err := SetOSDFlag(t.Context(), c, "noout", !original)
		if err != nil || slices.Contains(native.flags, "noout") != !original {
			t.Fatal("temporary flag was not applied", err)
		}
		again, err := SetOSDFlag(t.Context(), c, "noout", original)
		if err == nil || err.Error() != "restore the existing OSD flag override first" || again == nil || again.state.refused == nil {
			t.Fatalf("overlapping flag lease accepted: %v", err)
		}
		if held := heldOSDFlags(c); len(held) != 1 || held["noout"] != change {
			t.Fatal("a refused overlap disturbed the held flag")
		}
		// The refused handle must not release the flag of the first.
		if err := again.Restore(t.Context()); err != nil || heldOSDFlags(c)["noout"] != change {
			t.Fatalf("the not-applied handle released the flag of the first: %v", err)
		}
		copy := *change
		if err := copy.Restore(t.Context()); err != nil {
			t.Fatal(err)
		}
		before := len(native.calls)
		if err := change.Restore(t.Context()); err != nil || len(native.calls) != before || len(heldOSDFlags(c)) != 0 {
			t.Fatal("copies did not share restored state")
		}
		if slices.Contains(native.flags, "noout") != original || !slices.Contains(native.flags, "noscrub") || !slices.Contains(native.flags, "noout-unrelated") {
			t.Fatal("flag restore changed unrelated tokens or original state")
		}
	}
}

func TestSetOSDFlagInRequestedStateIsANoWriteLease(t *testing.T) {
	c, native := newOSDFlagFixture(t, "noout")
	change, err := SetOSDFlag(t.Context(), c, "noout", true)
	if err != nil || change.state.refused != nil || native.mutations() != 0 || heldOSDFlags(c)["noout"] != change {
		t.Fatalf("a flag already in the requested state: %v", err)
	}
	if err := change.Restore(t.Context()); err != nil || native.mutations() != 0 || len(heldOSDFlags(c)) != 0 {
		t.Fatalf("the lease without a write wrote on restore: %v", err)
	}
}

func TestSetOSDFlagLostApplyAndRestoreRepliesReconcile(t *testing.T) {
	c, native := newOSDFlagFixture(t)
	native.failAfter = "set"
	change, err := SetOSDFlag(t.Context(), c, "norecover", true)
	if change == nil || err == nil || change.state.refused != nil || !slices.Contains(native.flags, "norecover") || heldOSDFlags(c)["norecover"] != change {
		t.Fatal("uncertain apply lost restoration handle")
	}
	native.failAfter = "unset"
	if err := change.Restore(t.Context()); err == nil || slices.Contains(native.flags, "norecover") {
		t.Fatal("uncertain restore reply unexpectedly hidden")
	}
	native.failAfter = ""
	before := len(native.calls)
	if err := change.Restore(t.Context()); err != nil || len(native.calls) != before+1 || len(heldOSDFlags(c)) != 0 {
		t.Fatal("lost restore was not reconciled without another mutation")
	}
}

func TestSetOSDFlagReadbackFailureKeepsRestorableHandle(t *testing.T) {
	c, native := newOSDFlagFixture(t)
	// The set command succeeds, but the readback after it does not decode.
	native.breakDumpAfterSet = true
	change, err := SetOSDFlag(t.Context(), c, "noscrub", true)
	if err == nil || change.state.refused != nil || !slices.Contains(native.flags, "noscrub") || heldOSDFlags(c)["noscrub"] != change {
		t.Fatalf("an unreadable readback did not keep the handle: %v", err)
	}
	mutations := native.mutations()
	if err := change.Restore(t.Context()); err == nil || native.mutations() != mutations || !slices.Contains(native.flags, "noscrub") {
		t.Fatalf("a restore without a readable dump changed the flag: %v", err)
	}
	native.raw = ""
	if err := change.Restore(t.Context()); err != nil || slices.Contains(native.flags, "noscrub") || len(heldOSDFlags(c)) != 0 {
		t.Fatalf("the handle was not restorable after the failures: %v", err)
	}
}

func TestSetOSDFlagRejectsOutsideChangesToNoopLease(t *testing.T) {
	c, native := newOSDFlagFixture(t, "noout")
	change, err := SetOSDFlag(t.Context(), c, "noout", true)
	if err != nil {
		t.Fatal(err)
	}
	native.flags = nil
	before := len(native.calls)
	err = change.Restore(t.Context())
	if err == nil || !errors.Is(err, ceph.ErrInvalidRequest) || len(native.calls) != before+1 || slices.Contains(native.flags, "noout") {
		t.Fatalf("outside flag clear overwritten by a no-op lease: %v", err)
	}
	native.flags = []string{"noout"}
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestSetOSDFlagEarlyReturnsLeaveNoReservation(t *testing.T) {
	c, native := newOSDFlagFixture(t)
	native.raw = "not json"
	change, err := SetOSDFlag(t.Context(), c, ceph.OSDFlagNoOut, true)
	if err == nil || change == nil || change.state.refused == nil || len(heldOSDFlags(c)) != 0 || native.mutations() != 0 {
		t.Fatalf("failed dump: %v %v", change, err)
	}
	native.raw = ""
	change, err = SetOSDFlag(t.Context(), c, ceph.OSDFlagNoOut, true)
	if err != nil || heldOSDFlags(c)[string(ceph.OSDFlagNoOut)] != change {
		t.Fatalf("a freed key stayed refused: %v", err)
	}
	if err := change.Restore(t.Context()); err != nil || len(heldOSDFlags(c)) != 0 {
		t.Fatalf("restore: %v", err)
	}
}

// A handle whose state is a copy of a restored predecessor's is stale: its
// key now maps to the successor, and Restore must refuse it without a native
// call or a change to the successor.
func TestSetOSDFlagRestoreRefusesStaleAndForgedHandles(t *testing.T) {
	c, native := newOSDFlagFixture(t)
	first, err := SetOSDFlag(t.Context(), c, ceph.OSDFlagNoOut, true)
	if err != nil {
		t.Fatal(err)
	}
	stale := &OSDFlagHandle{owner: c, flag: first.flag, state: &osdFlagState{applied: true, token: first.state.token}}
	if err := first.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	second, err := SetOSDFlag(t.Context(), c, ceph.OSDFlagNoOut, true)
	if err != nil {
		t.Fatal(err)
	}
	before := len(native.calls)
	if err := stale.Restore(t.Context()); err == nil || err.Error() != "OSD flag override is not tracked by this cluster" || len(native.calls) != before || heldOSDFlags(c)[string(ceph.OSDFlagNoOut)] != second {
		t.Fatalf("a stale handle was not refused: %v", err)
	}
	// A handle that no registration ever issued has no token at all.
	forged := &OSDFlagHandle{owner: c, flag: first.flag, state: &osdFlagState{applied: true}}
	if err := forged.Restore(t.Context()); err == nil || len(native.calls) != before || heldOSDFlags(c)[string(ceph.OSDFlagNoOut)] != second {
		t.Fatalf("a forged handle was not refused: %v", err)
	}
}

func TestOSDFlagRestoreRefusedAfterTerminate(t *testing.T) {
	c, native := newOSDFlagFixture(t)
	change, err := SetOSDFlag(t.Context(), c, ceph.OSDFlagNoOut, true)
	if err != nil {
		t.Fatal(err)
	}
	view := servicehook.TestState(c)
	view.Mutex.Lock()
	*view.Closed = true
	view.Mutex.Unlock()
	before := len(native.calls)
	if err := change.Restore(t.Context()); err != overridehook.Terminated() {
		t.Fatalf("a restore on a terminated cluster returned %v", err)
	}
	if len(native.calls) != before || change.state.restored || len(heldOSDFlags(c)) != 1 {
		t.Fatal("a restore after Terminate ran a command or forgot the flag")
	}
	later, err := SetOSDFlag(t.Context(), c, ceph.OSDFlagNoScrub, true)
	if err != overridehook.Terminated() || later == nil || later.state.refused == nil || native.mutations() != 1 {
		t.Fatalf("a set on a terminated cluster: %v", err)
	}
}

func TestSetOSDFlagCanceledRestoreKeepsOwnership(t *testing.T) {
	c, native := newOSDFlagFixture(t)
	change, err := SetOSDFlag(t.Context(), c, ceph.OSDFlagNoOut, true)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := change.Restore(ctx); !errors.Is(err, context.Canceled) || heldOSDFlags(c)["noout"] != change || !slices.Contains(native.flags, "noout") {
		t.Fatalf("cancelled restore lost ownership or mutated: %v", err)
	}
	if err := change.Restore(t.Context()); err != nil || len(heldOSDFlags(c)) != 0 {
		t.Fatalf("fresh restore did not release ownership: %v", err)
	}
}

func TestSetOSDFlagNilClusterReturnsNotAppliedHandle(t *testing.T) {
	handle, err := SetOSDFlag(t.Context(), nil, ceph.OSDFlagNoOut, true)
	var op *ceph.OperationError
	if !errors.Is(err, ceph.ErrInvalidRequest) || !errors.As(err, &op) || op.Op != "override.SetOSDFlag" {
		t.Fatalf("a nil cluster was not refused as an invalid request: %v", err)
	}
	if handle == nil || handle.state == nil || handle.state.refused == nil {
		t.Fatalf("a nil cluster returned %+v", handle)
	}
	if err := handle.Restore(t.Context()); err != nil {
		t.Fatalf("the restore of a not-applied handle: %v", err)
	}
	// The nil cluster is reported before the input error.
	if _, err := SetOSDFlag(t.Context(), nil, "unsupported", true); err == nil || !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a nil cluster with an unsupported flag reported %v", err)
	}
}

func TestSetOSDFlagSharedClusterIsRefusedBeforeAnyEffect(t *testing.T) {
	native := &osdFlagFixture{}
	c := servicehook.TestCluster(native, servicehook.TestConfig{Shared: true, StartupTimeout: time.Second})
	handle, err := SetOSDFlag(t.Context(), c, "unsupported", true)
	var op *ceph.OperationError
	if !errors.Is(err, ceph.ErrSharedCluster) || !errors.As(err, &op) || op.Op != "override.SetOSDFlag" {
		t.Fatalf("a shared cluster was not refused with its class and Op: %v", err)
	}
	if handle == nil || handle.state.refused == nil || len(native.calls) != 0 {
		t.Fatalf("a refusal returned %+v after %d commands", handle, len(native.calls))
	}
}

func TestNotAppliedOSDFlagRestoreIsANoOpThatTakesNoLock(t *testing.T) {
	c, native := newOSDFlagFixture(t, "noout")
	holder, err := SetOSDFlag(t.Context(), c, ceph.OSDFlagNoOut, false)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := SetOSDFlag(t.Context(), c, ceph.OSDFlagNoOut, false)
	if err == nil || handle == nil || handle.state.refused == nil {
		t.Fatalf("setup: %v", err)
	}
	commands := len(native.calls)
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
	if handle.state.restored || len(native.calls) != commands || heldOSDFlags(c)["noout"] != holder {
		t.Fatal("a not-applied restore recorded a restoration, ran a command or touched the registry")
	}
}

func TestOSDFlagZeroAndNilHandlesAreUnavailable(t *testing.T) {
	var missing *OSDFlagHandle
	if err := missing.Restore(t.Context()); err == nil {
		t.Fatal("nil handle restored")
	}
	if err := (&OSDFlagHandle{}).Restore(t.Context()); err == nil {
		t.Fatal("zero handle restored")
	}
}
