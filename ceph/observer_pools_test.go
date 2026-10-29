package ceph

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestObserverPoolsListsPoolObjectsBoundToTheNativeID(t *testing.T) {
	ctr := policyFixture("2", "1", "1")
	cluster := poolFixtureCluster(ctr, 3)
	observer, err := Observe(cluster)
	if err != nil {
		t.Fatal(err)
	}
	pools, err := observer.Pools(t.Context())
	if err != nil || len(pools) != 1 {
		t.Fatalf("Pools = %v, %v", pools, err)
	}
	pool := pools[0]
	if pool.Name != "fixture" || pool.Replicas != 2 || pool.MinSize != 1 || pool.PGNum != 8 {
		t.Fatalf("listed pool = %+v", pool.PoolConfig)
	}
	// A later pool that reuses the name has another native ID, so the listed
	// pool, which the listing already bound to ID 7, does not reach it.
	original := ctr.Output["osd pool ls detail --format json"]
	ctr.Output["osd pool ls detail --format json"] = strings.Replace(original, `"pool_id":7`, `"pool_id":8`, 1)
	if _, err := pool.Status(t.Context()); !errors.Is(err, errStalePool) {
		t.Fatalf("Status through a replaced pool = %v, want a stale pool error", err)
	}
	ctr.Output["osd pool ls detail --format json"] = original
	state, err := pool.Status(t.Context())
	if err != nil || state.ID != 7 || state.Name != "fixture" {
		t.Fatalf("Status through the listed pool = %+v, %v", state, err)
	}
}

func TestObserverPoolsRefusesAMalformedListingAndAnUnconstructedObserver(t *testing.T) {
	ctr := policyFixture("2", "1", "1")
	ctr.Output["osd pool ls detail --format json"] = "null"
	observer, err := Observe(poolFixtureCluster(ctr, 3))
	if err != nil {
		t.Fatal(err)
	}
	if pools, err := observer.Pools(t.Context()); err == nil || pools != nil {
		t.Fatalf("malformed listing returned %v, %v", pools, err)
	}
	var none *Observer
	if _, err := none.Pools(t.Context()); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("a nil observer returned %v, want ErrInvalidRequest", err)
	}
}

// leasePools leases a tenant of a shared cluster whose cluster is a pool
// fixture, and returns the pool that the tenant's Observer lists.
func leasePools(t *testing.T) (*leaseDouble, *Container, *poolFixtureContainer, *Pool) {
	t.Helper()
	ctr := policyFixture("2", "1", "1")
	cluster := poolFixtureCluster(ctr, 3)
	cluster.sharing.Store(int32(sharedWithTenants))
	lease := newLeaseDouble(cluster)
	observer := lease.observer()
	pools, err := observer.Pools(t.Context())
	if err != nil || len(pools) != 1 {
		t.Fatalf("Pools = %v, %v", pools, err)
	}
	return lease, cluster, ctr, pools[0]
}

// poolOperations runs every operation of a Pool and returns the error of each.
func poolOperations(ctx context.Context, pool *Pool) map[string]error {
	errs := map[string]error{}
	_, errs["Status"] = pool.Status(ctx)
	_, errs["Usage"] = pool.Usage(ctx)
	_, errs["PGs"] = pool.PGs(ctx)
	errs["SetPGCount"] = pool.SetPGCount(ctx, 16)
	errs["WaitForPGCount"] = pool.WaitForPGCount(ctx, 16)
	errs["SetPlacement"] = pool.SetPlacement(ctx, PoolPlacement{FailureDomain: "osd"})
	errs["SetQuota"] = pool.SetQuota(ctx, PoolQuota{MaxBytes: 1})
	errs["SetReplication"] = pool.SetReplication(ctx, 2, 1)
	errs["Remove"] = pool.Remove(ctx)
	return errs
}

func requireNoPoolOperationRuns(t *testing.T, pool *Pool, ctr *poolFixtureContainer, want error) {
	t.Helper()
	before := len(ctr.Calls)
	for op, err := range poolOperations(t.Context(), pool) {
		if !errors.Is(err, want) {
			t.Errorf("Pool.%s returned %v, want %v", op, err, want)
		}
	}
	if len(ctr.Calls) != before {
		t.Fatalf("a refused operation reached the cluster: %v", ctr.Calls[before:])
	}
}

func TestTenantDerivedPoolRefusesEveryOperationAfterRelease(t *testing.T) {
	lease, _, ctr, pool := leasePools(t)
	lease.release()
	requireNoPoolOperationRuns(t, pool, ctr, ErrInvalidRequest)
}

func TestTenantDerivedPoolRefusesEveryOperationAfterTermination(t *testing.T) {
	lease, _, ctr, pool := leasePools(t)
	lease.terminate()
	requireNoPoolOperationRuns(t, pool, ctr, ErrInvalidRequest)
}

func TestTenantDerivedPoolDoesNotInheritALaterExclusiveLease(t *testing.T) {
	lease, cluster, ctr, pool := leasePools(t)
	lease.release()
	// A later Exclusive lease lends the cluster to another holder.
	cluster.sharing.Store(int32(sharedExclusively))
	requireNoPoolOperationRuns(t, pool, ctr, ErrInvalidRequest)
}

func TestTenantDerivedPoolKeepsTheTenantPolicyWhileTheLeaseIsLive(t *testing.T) {
	_, cluster, ctr, pool := leasePools(t)
	if _, err := pool.Status(t.Context()); err != nil {
		t.Fatalf("a read through a live tenant's pool failed: %v", err)
	}
	// Even if the cluster is switched to exclusive use while the tenant still
	// holds its lease, a tenant-derived handle keeps the tenant's policy.
	cluster.sharing.Store(int32(sharedExclusively))
	before := len(ctr.Calls)
	for op, err := range map[string]error{
		"SetPGCount":     pool.SetPGCount(t.Context(), 16),
		"SetPlacement":   pool.SetPlacement(t.Context(), PoolPlacement{FailureDomain: "osd"}),
		"SetQuota":       pool.SetQuota(t.Context(), PoolQuota{MaxBytes: 1}),
		"SetReplication": pool.SetReplication(t.Context(), 2, 1),
		"Remove":         pool.Remove(t.Context()),
	} {
		if !errors.Is(err, ErrSharedCluster) {
			t.Errorf("Pool.%s returned %v, want ErrSharedCluster", op, err)
		}
	}
	if len(ctr.Calls) != before {
		t.Fatalf("a refused mutation reached the cluster: %v", ctr.Calls[before:])
	}
}

func TestClusterDerivedPoolIsUnchangedByTheTenantGuard(t *testing.T) {
	ctr := policyFixture("2", "1", "1")
	cluster := poolFixtureCluster(ctr, 3)
	pool := poolHandle(t, t.Context(), cluster, "fixture")
	if pool.lease != nil {
		t.Fatal("a cluster-derived pool carries a tenant lease")
	}
	cluster.sharing.Store(int32(sharedExclusively))
	if err := pool.SetQuota(t.Context(), PoolQuota{}); err != nil {
		t.Fatalf("an exclusive holder's pool was refused: %v", err)
	}
}
