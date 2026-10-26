//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rados_fixtures))))

//ci: timeout=120m job-timeout=130 image-roles=essential,client,cephfs

package integration_test

import (
	"context"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
)

// Self-managed snapshot write contexts are client state: the fixture supplies
// an owned pool, a file-backed BlueStore OSD and a supervised native client.
// The snapshots and HEAD must read back the same bytes, sizes and versions
// after a clean OSD restart.
func TestRADOSSelfManagedSnapshots(t *testing.T) {
	parallelWhenEnabled(t)
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t, ceph.WithOSDCount(1))
	const pool, namespace = "tc-selfsnap", "fixture"
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rados", PGNum: 1}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	written := selfManagedSnapshotPhase(t, ctx, cluster, client, pool, namespace, "write", nil)

	osd := clusteraccess.OSDs(t, cluster)[0]
	stop := 20 * time.Second
	if err := osd.Stop(ctx, &stop); err != nil {
		t.Fatal(err)
	}
	if err := osd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	selfManagedSnapshotPhase(t, ctx, cluster, client, pool, namespace, "verify", written)
	selfManagedSnapshotPhase(t, ctx, cluster, client, pool, namespace, "contexts", written)

	// Removing the pool removes the clones that no snapshot removal trims,
	// such as the one of the unallocated id.
	if err := cluster.RemovePool(ctx, pool); err != nil {
		t.Fatal(err)
	}
	pools, err := cluster.Pools(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range pools {
		if state.Name == pool {
			t.Fatalf("pool %s remains after removal: %+v", pool, state)
		}
	}
}
