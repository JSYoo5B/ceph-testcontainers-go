package ceph

import (
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
