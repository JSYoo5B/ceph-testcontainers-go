package harness

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// pending runs fn in a goroutine; the channel is closed when it returns.
func pending(fn func()) chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	return done
}

func requireBlocked(t *testing.T, done chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
		t.Fatalf("%s did not wait", what)
	case <-time.After(50 * time.Millisecond):
	}
}

func requireDone(t *testing.T, done chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s did not return", what)
	}
}

func TestSharedExclusiveWaitsForTenantsAndBlocksNewLeases(t *testing.T) {
	fleet := &fakeFleet{}
	shared := newFakeShared(fleet)
	holderTB := newFakeTB(t)
	holder := shared.Lease(holderTB)
	first := backendOf(holder)

	exclusiveTB := newFakeTB(t)
	exclusive := pending(func() { shared.Exclusive(exclusiveTB) })
	requireBlocked(t, exclusive, "an Exclusive while a tenant holds a lease")

	// A lease that asks after the Exclusive waits behind it, so a stream of
	// leases cannot starve it.
	laterTB := newFakeTB(t)
	var later *Tenant
	leased := pending(func() { later = shared.Lease(laterTB) })
	requireBlocked(t, leased, "a lease behind a waiting Exclusive")

	holderTB.finish()
	requireDone(t, exclusive, "the Exclusive after the last tenant ended")
	if exclusiveTB.Failed() {
		t.Fatalf("Exclusive failed: %s", exclusiveTB.errorText())
	}
	if first.mode() != sharedExclusively || first.isTerminated() {
		t.Fatalf("the exclusive lease got mode %v, terminated=%v", first.mode(), first.isTerminated())
	}
	requireBlocked(t, leased, "a lease while the exclusive lease is held")

	exclusiveTB.finish()
	requireDone(t, leased, "the lease after the exclusive lease ended")
	if laterTB.Failed() || later == nil {
		t.Fatalf("the later lease failed: %s", laterTB.errorText())
	}
	if !first.isTerminated() || backendOf(later) == first {
		t.Fatal("the cluster of the exclusive lease was reused")
	}
	if backendOf(later).mode() != sharedWithTenants {
		t.Fatal("the fresh cluster is not shared with tenants")
	}
	laterTB.finish()
	if stats := shared.Stats(); stats.Exclusives != 1 || stats.Starts != 2 || stats.Recycles != 0 || stats.Poisonings != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestSharedLeaseAskedBeforeExclusiveGoesFirst(t *testing.T) {
	fleet := &fakeFleet{}
	shared := newFakeShared(fleet, WithMaxTenants(1))
	holderTB := newFakeTB(t)
	shared.Lease(holderTB)
	earlierTB := newFakeTB(t)
	earlier := pending(func() { shared.Lease(earlierTB) })
	requireBlocked(t, earlier, "a lease beyond WithMaxTenants")
	exclusiveTB := newFakeTB(t)
	exclusive := pending(func() { shared.Exclusive(exclusiveTB) })
	requireBlocked(t, exclusive, "an Exclusive behind tenants")

	// Once the slot is free, both could run; only the earlier lease may.
	shared.mu.Lock()
	queue := slices.Clone(shared.waiters)
	shared.slots--
	exclusiveFirst := len(queue) == 2 && shared.admissibleLocked(queue[1])
	shared.slots++
	shared.mu.Unlock()
	if len(queue) != 2 || !queue[1].exclusive || exclusiveFirst {
		t.Fatalf("waiters %d; the Exclusive could pass the earlier lease: %v", len(queue), exclusiveFirst)
	}
	holderTB.finish()
	requireDone(t, earlier, "the lease that asked before the Exclusive")
	requireBlocked(t, exclusive, "an Exclusive while the earlier lease holds the cluster")
	earlierTB.finish()
	requireDone(t, exclusive, "the Exclusive")
	exclusiveTB.finish()
	if earlierTB.Failed() || exclusiveTB.Failed() {
		t.Fatalf("lease: %s; exclusive: %s", earlierTB.errorText(), exclusiveTB.errorText())
	}
}

func TestSharedExclusiveFailsFastInsteadOfWaitingForItself(t *testing.T) {
	fleet := &fakeFleet{}
	shared := newFakeShared(fleet)
	holderTB := newFakeTB(t)
	shared.Lease(holderTB)
	for _, tb := range []*fakeTB{holderTB, holderTB.subtest("child")} {
		done := pending(func() {
			if shared.Exclusive(tb) != nil || !strings.Contains(tb.errorText(), "holds a tenant lease") {
				t.Errorf("Exclusive of %s while it holds a lease: %q", tb.Name(), tb.errorText())
			}
		})
		requireDone(t, done, "an Exclusive of a test that holds a lease")
	}
	holderTB.finish()

	exclusiveTB := newFakeTB(t)
	shared.Exclusive(exclusiveTB)
	if exclusiveTB.Failed() {
		t.Fatalf("Exclusive failed: %s", exclusiveTB.errorText())
	}
	for _, tb := range []*fakeTB{exclusiveTB.subtest("lease"), exclusiveTB.subtest("parallel/child")} {
		done := pending(func() {
			if shared.Lease(tb) != nil || !strings.Contains(tb.errorText(), "holds the exclusive lease") {
				t.Errorf("Lease of %s under its own exclusive lease: %q", tb.Name(), tb.errorText())
			}
		})
		requireDone(t, done, "a Lease under the test's own exclusive lease")
	}
	again := exclusiveTB.subtest("again")
	requireDone(t, pending(func() { shared.Exclusive(again) }), "a second Exclusive of the same test")
	if !strings.Contains(again.errorText(), "holds the exclusive lease") {
		t.Fatalf("a second Exclusive: %q", again.errorText())
	}
	exclusiveTB.finish()
}

func TestSharedRelatedMatchesOnlyTheTestAndItsSubtests(t *testing.T) {
	for _, c := range []struct {
		caller, holder string
		want           bool
	}{
		{"TestA", "TestA", true},
		{"TestA/sub", "TestA", true},
		{"TestA/sub/deeper", "TestA", true},
		{"TestAB", "TestA", false},
		{"TestA/fake10", "TestA/fake1", false},
		{"TestA", "TestA/sub", false},
		{"", "", false},
		{"TestA", "", false},
	} {
		if got := related(c.caller, c.holder); got != c.want {
			t.Errorf("related(%q, %q) = %v, want %v", c.caller, c.holder, got, c.want)
		}
	}
}

func TestSharedNestedLeaseDoesNotWaitBehindExclusive(t *testing.T) {
	fleet := &fakeFleet{}
	shared := newFakeShared(fleet)
	holderTB := newFakeTB(t)
	shared.Lease(holderTB)
	exclusiveTB := newFakeTB(t)
	exclusive := pending(func() { shared.Exclusive(exclusiveTB) })
	requireBlocked(t, exclusive, "an Exclusive while a tenant holds a lease")

	// The Exclusive waits for holderTB's leases; a second lease of the same
	// test, or of its subtest, must not wait for the Exclusive in turn.
	child := holderTB.subtest("child")
	for _, tb := range []*fakeTB{holderTB, child} {
		requireDone(t, pending(func() { shared.Lease(tb) }), "a second lease of a test that holds one")
		if tb.Failed() {
			t.Fatalf("the nested lease failed: %s", tb.errorText())
		}
	}
	requireBlocked(t, exclusive, "an Exclusive while nested leases are held")
	child.finish()
	holderTB.finish()
	requireDone(t, exclusive, "the Exclusive after the nested leases ended")
	exclusiveTB.finish()
}

func TestSharedExclusiveWaitEndsWithTheContext(t *testing.T) {
	t.Run("behind a tenant", func(t *testing.T) {
		fleet := &fakeFleet{}
		shared := newFakeShared(fleet)
		holderTB := newFakeTB(t)
		shared.Lease(holderTB)
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		began := time.Now()
		if _, err := shared.exclusiveLease(ctx, "TestWaiter", t.Logf); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("a canceled Exclusive returned %v", err)
		}
		if waited := time.Since(began); waited > time.Second {
			t.Fatalf("a canceled Exclusive returned after %v", waited)
		}
		// The canceled Exclusive no longer holds back new leases.
		nextTB := newFakeTB(t)
		requireDone(t, pending(func() { shared.Lease(nextTB) }), "a lease after a canceled Exclusive")
		nextTB.finish()
		holderTB.finish()
		if nextTB.Failed() || shared.Stats().Exclusives != 0 {
			t.Fatalf("lease: %s; stats %+v", nextTB.errorText(), shared.Stats())
		}
	})
	t.Run("while the cluster starts", func(t *testing.T) {
		fleet := &fakeFleet{entered: make(chan struct{}, 1), gate: make(chan struct{})}
		shared := newFakeShared(fleet)
		t.Cleanup(func() { _ = shared.Close(context.Background()) })
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		go func() {
			_, err := shared.exclusiveLease(ctx, "TestWaiter", t.Logf)
			result <- err
		}()
		<-fleet.entered
		cancel()
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Exclusive: %v, want context.Canceled", err)
			}
		case <-time.After(time.Second):
			close(fleet.gate)
			t.Fatal("the Exclusive did not return after its context was canceled")
		}
		tb := newFakeTB(t)
		leased := pending(func() { shared.Lease(tb) })
		close(fleet.gate)
		requireDone(t, leased, "a lease after a canceled Exclusive")
		tb.finish()
		if tb.Failed() {
			t.Fatalf("lease: %s", tb.errorText())
		}
	})
}

func TestSharedExclusiveRetirementsDoNotSpendRecycles(t *testing.T) {
	fleet := &fakeFleet{}
	shared := newFakeShared(fleet, WithMaxRecycles(0))
	for range 3 {
		tb := newFakeTB(t)
		shared.Exclusive(tb)
		tb.finish()
		if tb.Failed() {
			t.Fatalf("Exclusive failed: %s", tb.errorText())
		}
	}
	tb := newFakeTB(t)
	tenant := shared.Lease(tb)
	tb.finish()
	if tenant == nil || tb.Failed() {
		t.Fatalf("a lease after exclusive leases failed: %s", tb.errorText())
	}
	clusters := fleet.clusters()
	for _, cluster := range clusters[:3] {
		if !cluster.isTerminated() {
			t.Fatal("the cluster of an exclusive lease outlived it")
		}
	}
	if stats := shared.Stats(); stats.Starts != 4 || stats.Exclusives != 3 || stats.Recycles != 0 || stats.Poisonings != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestSharedExclusiveKeepsAClusterWhoseTerminationFailed(t *testing.T) {
	fleet := &fakeFleet{prepare: func(f *fakeShared) { f.terminateFailures = 1 }}
	shared := newFakeShared(fleet)
	tb := newFakeTB(t)
	shared.Exclusive(tb)
	tb.finish()
	cluster := fleet.clusters()[0]
	if cluster.isTerminated() || !strings.Contains(strings.Join(tb.logs, "\n"), "Close tries again") {
		t.Fatalf("terminated=%v logs=%q, want a failed termination that is reported", cluster.isTerminated(), tb.logs)
	}
	if err := shared.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !cluster.isTerminated() || cluster.terminations() != 2 {
		t.Fatalf("terminated=%v calls=%d, want Close to retry", cluster.isTerminated(), cluster.terminations())
	}
}

func TestSharedExclusiveGetsAClusterAtItsBaseline(t *testing.T) {
	fleet := &fakeFleet{}
	shared := newFakeShared(fleet)
	tb := newFakeTB(t)
	shared.Lease(tb)
	tb.finish()
	first := fleet.clusters()[0]
	first.change(func(cluster *fakeShared) { cluster.flags = append(cluster.flags, "noout") })
	exclusiveTB := newFakeTB(t)
	shared.Exclusive(exclusiveTB)
	clusters := fleet.clusters()
	if len(clusters) != 2 || !first.isTerminated() || clusters[1].mode() != sharedExclusively {
		t.Fatalf("the exclusive lease got a cluster that drifted from its baseline: %d clusters", len(clusters))
	}
	exclusiveTB.finish()
	if stats := shared.Stats(); stats.Poisonings != 1 || stats.Recycles != 1 || stats.Exclusives != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestSharedCloseEndsAWaitingExclusive(t *testing.T) {
	fleet := &fakeFleet{}
	shared := newFakeShared(fleet)
	holderTB := newFakeTB(t)
	shared.Lease(holderTB)
	exclusiveTB := newFakeTB(t)
	exclusive := pending(func() { shared.Exclusive(exclusiveTB) })
	requireBlocked(t, exclusive, "an Exclusive while a tenant holds a lease")
	if err := shared.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	requireDone(t, exclusive, "an Exclusive after Close")
	if !strings.Contains(exclusiveTB.errorText(), "closed") {
		t.Fatalf("Exclusive after Close: %q", exclusiveTB.errorText())
	}
	holderTB.finish()
}

// A parallel subtest that took a lease is paused until its parent returns,
// so its parent's exclusive lease must fail instead of waiting for it.
func TestSharedExclusiveOfAParentFailsWhileAParallelSubtestHoldsALease(t *testing.T) {
	shared := newFakeShared(&fakeFleet{})
	t.Cleanup(func() { _ = shared.Close(context.Background()) })
	var err error
	t.Run("group", func(t *testing.T) {
		t.Run("child", func(t *testing.T) {
			shared.Lease(t)
			t.Parallel()
		})
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		_, err = shared.exclusiveLease(ctx, t.Name(), t.Logf)
	})
	if !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("exclusive lease of the parent: %v, want ErrInvalidRequest", err)
	}
}

// The same holds for a parent's lease while a paused parallel subtest holds
// the exclusive lease.
func TestSharedLeaseOfAParentFailsWhileAParallelSubtestHoldsTheExclusiveLease(t *testing.T) {
	shared := newFakeShared(&fakeFleet{})
	t.Cleanup(func() { _ = shared.Close(context.Background()) })
	var err error
	t.Run("group", func(t *testing.T) {
		t.Run("child", func(t *testing.T) {
			shared.Exclusive(t)
			t.Parallel()
		})
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		_, err = shared.lease(ctx, leaseSettings{holder: t.Name()}, t.Logf)
	})
	if !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("lease of the parent: %v, want ErrInvalidRequest", err)
	}
}

// A parent's lease must not queue behind an independent Exclusive while its
// paused parallel subtest holds a lease: the Exclusive waits for the
// subtest, which waits for the parent.
func TestSharedLeaseOfAParentGoesBeforeAnExclusiveThatWaitsForItsParallelSubtest(t *testing.T) {
	shared := newFakeShared(&fakeFleet{})
	t.Cleanup(func() { _ = shared.Close(context.Background()) })
	exclusiveTB := newFakeTB(t)
	var exclusive chan struct{}
	var err error
	t.Run("group", func(t *testing.T) {
		t.Run("child", func(t *testing.T) {
			shared.Lease(t)
			t.Parallel()
		})
		exclusive = pending(func() { shared.Exclusive(exclusiveTB) })
		requireBlocked(t, exclusive, "an Exclusive while a paused subtest holds a lease")
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		var tenant *Tenant
		tenant, err = shared.lease(ctx, leaseSettings{holder: t.Name()}, t.Logf)
		if err == nil {
			t.Cleanup(func() { shared.release(tenant, t) })
		}
	})
	if err != nil {
		t.Fatalf("lease of the parent behind a queued Exclusive: %v", err)
	}
	requireDone(t, exclusive, "the Exclusive after the subtest ended")
	exclusiveTB.finish()
	if exclusiveTB.Failed() {
		t.Fatalf("Exclusive failed: %s", exclusiveTB.errorText())
	}
}
