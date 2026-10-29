package harness

import (
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
	if first.mode() != sharedExclusively {
		t.Fatalf("the retired cluster has mode %v, want it unchanged", first.mode())
	}
}

// TestExclusiveAuditKnownGapRetiredClusterKeepsExclusiveMode characterizes
// gap 1 of the note: the release of an exclusive interval does not lower the
// cluster's mode. When the termination fails, the cluster stays owned by Close
// and keeps the mode that admits every cluster-wide handle operation, so a
// retained Container or handle is admitted by the gate until Close ends the
// cluster. A change that makes the retired cluster refuse (a retired mode)
// inverts the mode assertion here.
func TestExclusiveAuditKnownGapRetiredClusterKeepsExclusiveMode(t *testing.T) {
	fleet := &fakeFleet{prepare: func(f *fakeShared) { f.terminateFailures = 1 }}
	shared := newFakeShared(fleet)
	tb := newFakeTB(t)
	shared.Exclusive(tb)
	tb.finish()
	cluster := fleet.clusters()[0]
	if cluster.isTerminated() {
		t.Fatal("the failed termination ended the cluster")
	}
	if got := cluster.mode(); got != sharedExclusively {
		t.Fatalf("the retired, still running cluster has mode %v; gap 1 is closed, update the note and this control", got)
	}
	if err := shared.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}
