package override

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// held returns the blocklist handles that the cluster's registry holds.
func held(c *ceph.Container) map[string]any {
	servicehook.Lock(c)
	defer servicehook.Unlock(c)
	return overridehook.TestHeld(c, overridehook.Blocklist)
}

func TestAddBlocklistRestoresOnlyOwnedEntryAndSharesCopies(t *testing.T) {
	c, native := newBlocklistFixture(t)
	other := "192.0.2.7:0/5678"
	native.entries[other] = time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	prior := native.entries[other]
	change, err := AddBlocklist(t.Context(), c, "v2:"+blocklistTestAddress, time.Minute)
	if err != nil || change.Address() != blocklistTestAddress {
		t.Fatal(err)
	}
	if again, err := AddBlocklist(t.Context(), c, blocklistTestAddress, time.Minute); err == nil || again == nil || again.state.refused == nil {
		t.Fatal("overlap accepted")
	}
	copy := *change
	if err := copy.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(native.entries) != 1 || !native.entries[other].Equal(prior) || len(native.ranges) != 1 {
		t.Fatal("restoration changed another session or range")
	}
	newChange, err := AddBlocklist(t.Context(), c, blocklistTestAddress, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	before := native.removals
	if err := change.Restore(t.Context()); err != nil || native.removals != before || len(native.entries) != 2 {
		t.Fatalf("a stale copy removed a later entry: %v", err)
	}
	if err := newChange.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestAddBlocklistRefusesExistingAndOutsideRenewal(t *testing.T) {
	c, native := newBlocklistFixture(t)
	native.entries[blocklistTestAddress] = time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	change, err := AddBlocklist(t.Context(), c, blocklistTestAddress, time.Minute)
	if err == nil || !errors.Is(err, ceph.ErrInvalidRequest) || change == nil || change.state.refused == nil || native.additions != 0 {
		t.Fatalf("existing entry was adopted or renewed: %v", err)
	}
	if len(held(c)) != 0 {
		t.Fatal("a refused call kept its reservation")
	}
	delete(native.entries, blocklistTestAddress)
	change, err = AddBlocklist(t.Context(), c, blocklistTestAddress, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	native.entries[blocklistTestAddress] = native.entries[blocklistTestAddress].Add(time.Hour)
	if err := change.Restore(t.Context()); err == nil || native.removals != 0 || !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatal("outside renewal was removed")
	}
	// Natural native expiration converges even after outside renewal.
	delete(native.entries, blocklistTestAddress)
	if err := change.Restore(t.Context()); err != nil || len(held(c)) != 0 {
		t.Fatal("native absence not reconciled")
	}
}

func TestAddBlocklistLostRepliesAndCanceledApplyAreRecoverable(t *testing.T) {
	c, native := newBlocklistFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	native.cancelAfterAdd, native.loseAdd = cancel, true
	change, err := AddBlocklist(ctx, c, blocklistTestAddress, time.Minute)
	if err == nil || change == nil || change.state.refused != nil || !change.state.known {
		t.Fatal("canceled apply lost its fresh readback handle")
	}
	native.loseRemove = true
	if err := change.Restore(t.Context()); err == nil || len(native.entries) != 0 {
		t.Fatal("lost removal reply not preserved")
	}
	if err := change.Restore(t.Context()); err != nil || native.removals != 1 {
		t.Fatal("removal retry did not reconcile absence")
	}
}

func TestAddBlocklistUnknownApplyCannotRemovePresentEntry(t *testing.T) {
	c, native := newBlocklistFixture(t)
	native.failReadAfterAdd = true
	change, err := AddBlocklist(t.Context(), c, blocklistTestAddress, time.Minute)
	if err == nil || change == nil || change.state.refused != nil || change.state.known {
		t.Fatal("unknown readback claimed confirmed creation")
	}
	// The expiry check would also refuse, so the reason pins the unknown readback.
	if err := change.Restore(t.Context()); err == nil || native.removals != 0 || !strings.Contains(err.Error(), "readback was unavailable") {
		t.Fatalf("unknown creation authorized removal or was refused for another reason: %v", err)
	}
	delete(native.entries, blocklistTestAddress)
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestAddBlocklistSuccessWithoutStoredEntryKeepsRestorableHandle(t *testing.T) {
	c, native := newBlocklistFixture(t)
	// The add reports success, but the entry is gone at the readback.
	native.cancelAfterAdd = func() { delete(native.entries, blocklistTestAddress) }
	change, err := AddBlocklist(t.Context(), c, blocklistTestAddress, time.Minute)
	if err == nil || !strings.Contains(err.Error(), "without a stored entry") || change == nil || change.state.refused != nil || change.state.known {
		t.Fatalf("a success without an entry was not reported as uncertain: %v", err)
	}
	if err := change.Restore(t.Context()); err != nil || len(held(c)) != 0 {
		t.Fatalf("the uncertain handle did not converge on absence: %v", err)
	}
}

func TestBlocklistFailedPreflightReleasesReservation(t *testing.T) {
	c, native := newBlocklistFixture(t)
	native.failNextRead = true
	change, err := AddBlocklist(t.Context(), c, blocklistTestAddress, time.Minute)
	if err == nil || change == nil || change.state.refused == nil || len(held(c)) != 0 || native.additions != 0 {
		t.Fatalf("failed preflight retained ownership or mutated: %v", err)
	}
	change, err = AddBlocklist(t.Context(), c, blocklistTestAddress, time.Minute)
	if err != nil || held(c)[blocklistTestAddress] != change {
		t.Fatalf("freed reservation was not reusable: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := change.Restore(ctx); !errors.Is(err, context.Canceled) || held(c)[blocklistTestAddress] != change || native.removals != 0 {
		t.Fatalf("cancelled restore lost ownership or mutated: %v", err)
	}
	if err := change.Restore(t.Context()); err != nil || len(held(c)) != 0 {
		t.Fatalf("fresh restore did not release ownership: %v", err)
	}
}

func TestBlocklistRestoreRefusedAfterTerminate(t *testing.T) {
	c, native := newBlocklistFixture(t)
	change, err := AddBlocklist(t.Context(), c, blocklistTestAddress, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	view := servicehook.TestState(c)
	view.Mutex.Lock()
	servicehook.TestSetClosed(c, true)
	view.Mutex.Unlock()
	before := native.commands
	err = change.Restore(t.Context())
	if !errors.Is(err, overridehook.Terminated()) || err != overridehook.Terminated() {
		t.Fatalf("a restore on a terminated cluster returned %v", err)
	}
	if native.commands != before || native.removals != 0 || change.state.restored || len(held(c)) != 1 {
		t.Fatal("a restore after Terminate ran a command or forgot the entry")
	}
	// Admission after Terminate is a refusal with the same error.
	later, err := AddBlocklist(t.Context(), c, "192.0.2.8:0/1", time.Minute)
	if err != overridehook.Terminated() || later == nil || later.state.refused == nil || native.additions != 1 {
		t.Fatalf("an add on a terminated cluster: %v", err)
	}
}

func TestBlocklistRestoreRefusesUntrackedHandle(t *testing.T) {
	c, native := newBlocklistFixture(t)
	change, err := AddBlocklist(t.Context(), c, blocklistTestAddress, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	forged := &BlocklistHandle{owner: c, address: change.address, state: &blocklistState{until: change.state.until, known: true}}
	if err := forged.Restore(t.Context()); err == nil || !strings.Contains(err.Error(), "not tracked") || native.removals != 0 {
		t.Fatalf("an untracked override removed the entry: %v", err)
	}
}

func TestBlocklistAdmissionDoesNotMutateUnavailableCluster(t *testing.T) {
	for _, input := range []struct {
		address  string
		duration time.Duration
	}{{"192.0.2.7", time.Minute}, {blocklistTestAddress, time.Millisecond}} {
		c, native := newBlocklistFixture(t)
		change, err := AddBlocklist(t.Context(), c, input.address, input.duration)
		if err == nil || change == nil || change.state.refused == nil || native.commands != 0 || len(held(c)) != 0 {
			t.Fatalf("invalid request admitted: %v", err)
		}
	}
	c, native := newBlocklistFixture(t)
	servicehook.TestSetClosed(c, true)
	if change, err := AddBlocklist(t.Context(), c, blocklistTestAddress, time.Minute); err == nil || change == nil || change.state.refused == nil || native.commands != 0 || len(held(c)) != 0 {
		t.Fatalf("closed cluster admitted: %v", err)
	}
}

func TestBlocklistInvalidInputKeepsClassesAndTexts(t *testing.T) {
	c, _ := newBlocklistFixture(t)
	_, err := AddBlocklist(t.Context(), c, "192.0.2.7", time.Minute)
	if !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a bare address is not classified: %v", err)
	}
	_, err = AddBlocklist(t.Context(), c, blocklistTestAddress, time.Millisecond)
	if err == nil || err.Error() != "client blocklist duration must be at least one second" || errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("the duration refusal changed: %v", err)
	}
}

func TestAddBlocklistNilClusterReturnsNotAppliedHandle(t *testing.T) {
	handle, err := AddBlocklist(t.Context(), nil, blocklistTestAddress, time.Minute)
	var op *ceph.OperationError
	if !errors.Is(err, ceph.ErrInvalidRequest) || !errors.As(err, &op) || op.Op != "override.AddBlocklist" {
		t.Fatalf("a nil cluster was not refused as an invalid request: %v", err)
	}
	if handle == nil || handle.state == nil || handle.state.refused == nil || handle.Address() != blocklistTestAddress {
		t.Fatalf("a nil cluster returned %+v", handle)
	}
	if err := handle.Restore(t.Context()); err != nil {
		t.Fatalf("the restore of a not-applied handle: %v", err)
	}
	// The nil cluster is reported before the input error.
	if _, err := AddBlocklist(t.Context(), nil, "192.0.2.7", 0); err == nil || !strings.Contains(err.Error(), "the cluster is nil") {
		t.Fatalf("a nil cluster with invalid input reported %v", err)
	}
}

func TestAddBlocklistSharedClusterIsRefusedBeforeAnyEffect(t *testing.T) {
	native := &blocklistFixture{entries: map[string]time.Time{}}
	c := servicehook.TestCluster(native, servicehook.TestConfig{Shared: true, StartupTimeout: time.Second})
	handle, err := AddBlocklist(t.Context(), c, "not an address", 0)
	var op *ceph.OperationError
	if !errors.Is(err, ceph.ErrSharedCluster) || !errors.As(err, &op) || op.Op != "override.AddBlocklist" {
		t.Fatalf("a shared cluster was not refused with its class and Op: %v", err)
	}
	if handle == nil || handle.state.refused == nil || native.commands != 0 {
		t.Fatalf("a refusal returned %+v after %d commands", handle, native.commands)
	}
}

func TestNotAppliedBlocklistRestoreIsANoOpThatTakesNoLock(t *testing.T) {
	c, native := newBlocklistFixture(t)
	native.entries[blocklistTestAddress] = time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	handle, err := AddBlocklist(t.Context(), c, blocklistTestAddress, time.Minute)
	if err == nil || handle == nil || handle.state.refused == nil {
		t.Fatalf("setup: %v", err)
	}
	commands := native.commands
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
	if handle.state.restored || native.commands != commands || native.removals != 0 || len(held(c)) != 0 {
		t.Fatal("a not-applied restore recorded a restoration, ran a command or touched the registry")
	}
	if _, ok := native.entries[blocklistTestAddress]; !ok {
		t.Fatal("a not-applied restore removed the foreign entry")
	}
}

func TestBlocklistZeroAndNilHandlesAreUnavailable(t *testing.T) {
	var missing *BlocklistHandle
	if err := missing.Restore(t.Context()); err == nil {
		t.Fatal("nil handle restored")
	}
	if err := (&BlocklistHandle{}).Restore(t.Context()); err == nil {
		t.Fatal("zero handle restored")
	}
	if missing.Address() != "" {
		t.Fatal("a nil handle reports an address")
	}
}

func TestAddBlocklistOverlapIsRefusedWithoutDisturbingTheHeldEntry(t *testing.T) {
	c, native := newBlocklistFixture(t)
	first, err := AddBlocklist(t.Context(), c, blocklistTestAddress, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	second, err := AddBlocklist(t.Context(), c, "v1:"+blocklistTestAddress, time.Minute)
	if err == nil || !strings.Contains(err.Error(), "restore the existing client blocklist override first") || second == nil || second.state.refused == nil {
		t.Fatalf("overlap: %v", err)
	}
	if held(c)[blocklistTestAddress] != first || native.additions != 1 {
		t.Fatal("a refused overlap disturbed the held entry")
	}
	if err := second.Restore(t.Context()); err != nil || held(c)[blocklistTestAddress] != first {
		t.Fatal("the not-applied handle released the entry of the first")
	}
	if err := first.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}
