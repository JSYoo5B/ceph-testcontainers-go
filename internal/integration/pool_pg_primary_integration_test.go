//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_default))))

//ci: timeout=20m job-timeout=35 image-roles=essential,client,cephfs,rgw architecture=arm64

package integration_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// TestPoolPGPrimaryOverrideServesKnownBytesAndRestores moves one PG's acting
// primary to another member of its acting set through Pool.OverridePGPrimary
// and restores it. The oracle is independent of the API: the client
// container's own "ceph osd map" for an object and "ceph pg query" of its PG,
// the stored OSDMap entries and require_min_compat_client, and exact RADOS
// bytes before, during, across a stop of the new serving primary and after
// the restoration.
func TestPoolPGPrimaryOverrideServesKnownBytesAndRestores(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	cluster, client := harness.NewServiceCluster(t)
	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pgPrimaryPool, Application: "rados", Replicas: 2, MinSize: 1, PGNum: 8})
	if err != nil {
		t.Fatal(err)
	}
	observer := harness.ObserveCluster(cluster)
	if err := observer.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	seed := bytes.Repeat([]byte("primary-move-known-bytes\n"), 64)
	pgPrimaryPut(t, ctx, client, "record", seed)
	original := pgPrimaryOracle(t, ctx, client, "record")
	before := pgPrimaryStoredMap(t, ctx, cluster)
	if _, held := before.entry(original.PGID); held {
		t.Fatalf("fresh pool PG %s already has an upmap primary entry", original.PGID)
	}
	if original.ServingPrimary != original.ActingPrimary || !original.ActiveClean {
		t.Fatalf("baseline PG is not served by its mapped primary: %+v", original)
	}
	newPrimary, oldPrimary := -1, original.ActingPrimary
	for _, osd := range original.Acting {
		if osd != oldPrimary {
			newPrimary = osd
		}
	}
	owned := map[int]*ceph.OSDContainer{}
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		owned[osd.ID] = osd
	}
	target, current := owned[newPrimary], owned[oldPrimary]
	if target == nil || current == nil {
		t.Fatalf("acting set %v of PG %s is not owned", original.Acting, original.PGID)
	}
	t.Logf("baseline: PG %s acting %v primary osd.%d, require_min_compat_client %s", original.PGID, original.Acting, oldPrimary, before.Require)

	// Control: the OSD that already is the primary takes no native write.
	noop, err := pool.OverridePGPrimary(ctx, ceph.PGPrimary{PG: original.PGID, OSD: current})
	if err != nil || noop == nil {
		t.Fatalf("override to the current primary = %v, %v", noop, err)
	}
	if after := pgPrimaryStoredMap(t, ctx, cluster); after.Require != before.Require || len(after.Entries) != len(before.Entries) {
		t.Fatalf("a no-op override changed the OSDMap: %+v -> %+v", before, after)
	}
	if err := noop.Restore(ctx); err != nil {
		t.Fatal(err)
	}

	// Control: a cluster below the required level is refused without opt-in
	// and left unchanged.
	raise := false
	refused, err := pool.OverridePGPrimary(ctx, ceph.PGPrimary{PG: original.PGID, OSD: target})
	switch {
	case errors.Is(err, ceph.ErrInvalidRequest):
		raise = true
		if refused != nil {
			t.Fatalf("a refused override returned a handle")
		}
		if after := pgPrimaryStoredMap(t, ctx, cluster); after.Require != before.Require || len(after.Entries) != 0 {
			t.Fatalf("a refused override changed the OSDMap: %+v", after)
		}
		t.Logf("refused without opt-in: %v", err)
	case err != nil:
		t.Fatal(err)
	default:
		// The cluster already allows pg-upmap-primary.
		if err := refused.Restore(ctx); err != nil {
			t.Fatal(err)
		}
	}

	started := time.Now()
	handle, err := pool.OverridePGPrimary(ctx, ceph.PGPrimary{PG: original.PGID, OSD: target, RaiseMinCompatClient: raise})
	if handle != nil {
		t.Cleanup(func() {
			restoreCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			_ = handle.Restore(restoreCtx)
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("override ready in %v", time.Since(started))
	changed := pgPrimaryOracle(t, ctx, client, "record")
	stored := pgPrimaryStoredMap(t, ctx, cluster)
	if changed.PGID != original.PGID || changed.ActingPrimary != newPrimary || changed.ServingPrimary != newPrimary || !changed.ActiveClean {
		t.Fatalf("serving state after the override = %+v, want osd.%d", changed, newPrimary)
	}
	if osd, held := stored.entry(original.PGID); !held || osd != newPrimary {
		t.Fatalf("stored entries = %+v, want %s -> osd.%d", stored.Entries, original.PGID, newPrimary)
	}
	if raise && stored.Require == before.Require {
		t.Fatalf("require_min_compat_client stayed %s", stored.Require)
	}
	snapshot, err := pool.PGs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, pg := range snapshot.PGs {
		if pg.PGID == original.PGID && pg.ActingPrimary != newPrimary {
			t.Fatalf("Pool.PGs reports %+v", pg)
		}
	}
	pgPrimaryGet(t, ctx, client, "record", seed)
	during := bytes.Repeat([]byte("written through the moved primary\n"), 64)
	pgPrimaryPut(t, ctx, client, "record", during)
	pgPrimaryGet(t, ctx, client, "record", during)

	// The fault reaches the intended serving target: stopping it moves the
	// PG's primary away, and the data stays readable through the survivor.
	grace := 10 * time.Second
	if err := target.Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
	failedOver := pgPrimaryAwait(t, ctx, client, "record", "serving target stopped", func(o pgPrimaryObservation) bool {
		return o.ActingPrimary == oldPrimary && o.ServingPrimary == oldPrimary
	})
	t.Logf("stopped osd.%d: PG %s acting %v primary osd.%d", newPrimary, failedOver.PGID, failedOver.Acting, failedOver.ActingPrimary)
	pgPrimaryGet(t, ctx, client, "record", during)
	if err := target.Start(ctx); err != nil {
		t.Fatal(err)
	}
	pgPrimaryAwait(t, ctx, client, "record", "serving target restarted", func(o pgPrimaryObservation) bool {
		return o.ActingPrimary == newPrimary && o.ServingPrimary == newPrimary && o.ActiveClean
	})
	pgPrimaryGet(t, ctx, client, "record", during)

	if err := handle.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	restored := pgPrimaryAwait(t, ctx, client, "record", "restored", func(o pgPrimaryObservation) bool {
		return o.ActingPrimary == oldPrimary && o.ServingPrimary == oldPrimary && o.ActiveClean
	})
	after := pgPrimaryStoredMap(t, ctx, cluster)
	if _, held := after.entry(original.PGID); held || after.Require != before.Require {
		t.Fatalf("restoration left %+v, want no entry and %s", after, before.Require)
	}
	if err := handle.Restore(ctx); err != nil {
		t.Fatalf("a second Restore = %v", err)
	}
	pgPrimaryGet(t, ctx, client, "record", during)
	if err := observer.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("restored: PG %s acting %v primary osd.%d, require_min_compat_client %s", restored.PGID, restored.Acting, restored.ActingPrimary, after.Require)
}
