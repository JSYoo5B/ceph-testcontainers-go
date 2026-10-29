package harness

import (
	"slices"
	"testing"
)

// Lifecycle controls of the Exclusive stale-handle audit (design note section
// "Exclusive stale-handle audit"). The admission controls over the handle
// families, and the check that no code lowers a cluster's sharing mode, are in
// ceph/shared_exclusive_audit_test.go.

// TestExclusiveAuditInstanceOfAnExclusiveLeaseIsNeverReused follows one
// cluster through a tenant lease, the exclusive interval and a later lease, and
// shows the model evidence for the audited properties: the exclusive interval
// takes the cluster that tenants used (so a tenant-held handle can meet an
// exclusive mode, see the handle controls below), and afterwards no lease gets
// that cluster again.
func TestExclusiveAuditInstanceOfAnExclusiveLeaseIsNeverReused(t *testing.T) {
	fleet := &fakeFleet{}
	shared := newFakeShared(fleet)
	tenantTB := newFakeTB(t)
	tenant := shared.Lease(tenantTB)
	first := backendOf(tenant)
	tenantTB.finish()
	if first.mode() != sharedWithTenants {
		t.Fatalf("the tenant cluster has mode %v", first.mode())
	}

	exclusiveTB := newFakeTB(t)
	shared.Exclusive(exclusiveTB)
	if got := fleet.clusters(); len(got) != 1 || got[0] != first || first.mode() != sharedExclusively {
		t.Fatalf("the exclusive interval did not take the cluster that tenants used: %d clusters, mode %v", len(got), first.mode())
	}
	exclusiveTB.finish()
	if !first.isTerminated() {
		t.Fatal("the cluster of the exclusive interval was not terminated by its release")
	}

	laterTB := newFakeTB(t)
	later := shared.Lease(laterTB)
	if backendOf(later) == first || backendOf(later).mode() != sharedWithTenants {
		t.Fatal("a lease after the exclusive interval got the exclusive cluster")
	}
	laterTB.finish()
	if first.mode() != sharedRetired {
		t.Fatalf("the retired cluster has mode %v, want the retired mode", first.mode())
	}
}

// TestExclusiveAuditRetiredClusterRefusesAfterFailedTermination closes gap 1
// of the note: the release of an exclusive interval marks the cluster retired
// before it terminates it. When the termination fails, the cluster stays owned
// by Close and keeps running, but with the mode that makes the gate refuse the
// retained Container and handles, not the mode that admits them; Close still
// ends it.
func TestExclusiveAuditRetiredClusterRefusesAfterFailedTermination(t *testing.T) {
	fleet := &fakeFleet{prepare: func(f *fakeShared) { f.terminateFailures = 1 }}
	shared := newFakeShared(fleet)
	tb := newFakeTB(t)
	shared.Exclusive(tb)
	cluster := fleet.clusters()[0]
	if got := cluster.mode(); got != sharedExclusively {
		t.Fatalf("the cluster of the interval has mode %v while the lease is held", got)
	}
	tb.finish()
	if cluster.isTerminated() {
		t.Fatal("the failed termination ended the cluster")
	}
	if got := cluster.mode(); got != sharedRetired {
		t.Fatalf("the retired, still running cluster has mode %v, want the retired mode", got)
	}
	if err := shared.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	cluster.mu.Lock()
	seen := slices.Clone(cluster.modeAtTerminate)
	cluster.mu.Unlock()
	if want := []sharingMode{sharedRetired, sharedRetired}; !slices.Equal(seen, want) {
		t.Fatalf("modes seen by the termination attempts = %v, want %v: the release must retire before it terminates", seen, want)
	}
	if !cluster.isTerminated() {
		t.Fatal("Close did not end the retired cluster")
	}
}

// TestExclusiveAuditRetiredModeOnlyAfterTheLeaseEnds shows that the retirement
// does not reach a lease that is still held, or a tenant lease's cluster.
func TestExclusiveAuditRetiredModeOnlyAfterTheLeaseEnds(t *testing.T) {
	fleet := &fakeFleet{}
	shared := newFakeShared(fleet)
	tenantTB := newFakeTB(t)
	shared.Lease(tenantTB)
	tenantTB.finish()
	later := newFakeTB(t)
	shared.Lease(later)
	defer later.finish()
	for _, c := range fleet.clusters() {
		if c.mode() == sharedRetired {
			t.Fatal("a tenant lease marked its cluster retired")
		}
	}
}
