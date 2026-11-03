package snapschedule

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
)

// Restore waits for another call on the same lease only until the caller's
// context ends. The queued Restore removes no schedule and keeps the lease
// and its directory reservation, leaves no waiter behind, and a later Restore
// with a fresh context removes the schedule.
func TestLeaseRestoreWaitsForTheLeaseOnlyUntilTheContextEnds(t *testing.T) {
	fake := newFakeSnapSchedule()
	fs := snapScheduleFilesystem(fake)
	lease, err := add(t.Context(), fs, leaseSchedule)
	if err != nil {
		t.Fatal(err)
	}
	before := fake.state()
	clustertest.AssertContextLockQueued(t, &lease.mu, lease.Restore)
	if fake.state() != before || lease.restored {
		t.Fatal("a queued Restore changed the schedule or gave up the lease")
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatalf("a fresh context after the queued deadline: %v", err)
	}
	if state := fake.state(); state != "" || !lease.restored {
		t.Fatalf("after the fresh Restore: %s", state)
	}
}
