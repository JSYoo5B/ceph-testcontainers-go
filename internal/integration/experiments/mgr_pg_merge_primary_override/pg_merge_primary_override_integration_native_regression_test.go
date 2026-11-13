//go:build (all || (integration && topology)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_mgr_pg_merge_primary_override)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package mgr_pg_merge_primary_override_test

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

// image is the Ceph image the case runs on: Tentacle 20.2.4, which
// reproduces the defect. A run selects another image with
//
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/mgr_pg_merge_primary_override_test.image=<image>"
//
// and a fixed one (Tentacle 20.2.5) does not reproduce it.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7a6791b2b6c67d63439a307e50a0f560f4c98323e2a17a7a9ab4b"

// The pools of the case: the pool whose PG count is reduced with a primary
// override on its merge target, the pool that keeps its PG count and its
// override, and the pool whose PG count is reduced without an override.
const (
	mergeTargetPool   = "tc-merge-target"
	mergeControlPool  = "tc-merge-control"
	mergeEligiblePool = "tc-merge-eligible"
	mergeObjects      = 16
)

// mergePool is one pool of the case with its merge source and the parent
// the source merges into.
type mergePool struct {
	*ceph.Pool
	Source, Parent ceph.PGID
}

// TestPGMergeWithPrimaryOverride reports whether a reduction of a pool's PG
// count never starts its merge while the merge target has a primary
// override.
//
// Given three replicated pools of two PGs on three OSDs, each holding known
// bytes, an OSDMap without any primary override, and then one override that
// gives the merge target of the first pool another primary than the merge
// source has and one override in the second pool, both read back from the
// OSDMap,
// when the PG count of the first and of the third pool is reduced to one and
// the third pool, which has no override, has finished its merge, then the
// defect is REPRODUCED if the first pool stays for two more minutes with
// both PGs active and clean, no merge under way and its override in place,
// and merges once the test removes the override, and NOT_REPRODUCED if the
// first pool merges while the test leaves its override alone, which the
// manager then has removed. The second pool's override must be unchanged in
// every sample. Tentacle 20.2.4's manager clears the upmap entries of merge
// participants but not their primary overrides, and merges two PGs only when
// their acting sets are equal, so it reproduces; Tentacle 20.2.5 does not.
//
// A failure of the given or when steps or of a control fails the test, and
// so does a pool that neither merges nor stays blocked; the result does not.
func TestPGMergeWithPrimaryOverride(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 19*time.Minute)
	defer cancel()
	t.Logf("PG_MERGE_PRIMARY image=%s", image)
	cluster, client := harness.NewSingleImageCluster(t, ceph.Run, image, harness.FunctionalOSDConfig(t), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(3)))
	observer := harness.ObserveCluster(cluster)
	if err := observer.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("PG_MERGE_PRIMARY client=%s", strings.TrimSpace(harness.ExecOutput(t, ctx, client, "ceph", "--version")))
	versions, err := cluster.Ceph(ctx, "versions")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("PG_MERGE_PRIMARY daemons=%s", strings.Join(strings.Fields(string(versions)), " "))
	osds, err := cluster.OSDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	osdMap := func() mergeMap {
		t.Helper()
		dump, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
		if err != nil {
			t.Fatal(err)
		}
		got, err := parseMergeMap(dump)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	// reports returns the PGs the manager reports for pool.
	reports := func(pool mergePool) map[string]pgReport {
		t.Helper()
		snapshot, err := pool.PGs(ctx)
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]pgReport{}
		for _, pg := range snapshot.PGs {
			got[string(pg.PGID)] = pgReport{State: pg.State, Acting: pg.Acting}
		}
		return got
	}

	// Given: the three pools, clean, with known bytes, and no primary
	// override anywhere.
	pools := map[string]mergePool{}
	for _, name := range []string{mergeTargetPool, mergeControlPool, mergeEligiblePool} {
		pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: name, PGNum: mergeFrom, Replicas: 3, MinSize: 2, Application: "rados"})
		if err != nil {
			t.Fatal(err)
		}
		pools[name] = mergePool{Pool: pool}
	}
	if err := observer.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{mergeTargetPool, mergeControlPool, mergeEligiblePool} {
		harness.ExecOutput(t, ctx, client, "sh", "-ec", `for i in $(seq 1 "$2"); do printf '%s' "$1 object $i" | timeout 60 rados -p "$1" put "object-$i" -; done`, "sh", name, fmt.Sprint(mergeObjects))
		mergeRequireBytes(t, ctx, client, name)
	}
	before := osdMap()
	if len(before.Primaries) != 0 {
		t.Fatalf("condition not established: the OSDMap already has primary overrides: %v", before.Primaries)
	}
	for _, name := range []string{mergeTargetPool, mergeControlPool, mergeEligiblePool} {
		counts, ok := before.Pools[name]
		if want := (poolCounts{ID: counts.ID, PGNum: mergeFrom, Target: mergeFrom, PGPNum: mergeFrom, Pending: mergeFrom}); !ok || counts != want {
			t.Fatalf("condition not established: pool %s has the PG counts %+v, want %d in each", name, counts, mergeFrom)
		}
		pool := pools[name]
		pool.Source, pool.Parent = ceph.PGID(fmt.Sprintf("%d.%x", counts.ID, mergeFrom-1)), ceph.PGID(fmt.Sprintf("%d.0", counts.ID))
		pools[name] = pool
	}
	// One override per pool: on the parent, which is the merge target, to a
	// member of its acting set that is the primary of neither the parent nor
	// the source. The parent's primary then differs from the source's, also
	// once both PGs are placed alike for the merge, and the override stays
	// valid.
	plan := mergePlan{}
	handles := map[string]*ceph.PGPrimaryOverrideHandle{}
	for _, name := range []string{mergeTargetPool, mergeControlPool} {
		pool, pgs := pools[name], reports(pools[name])
		source, parent := pgs[string(pool.Source)], pgs[string(pool.Parent)]
		if len(pgs) != mergeFrom || !source.activeClean() || !parent.activeClean() || len(source.Acting) != 3 || len(parent.Acting) != 3 {
			t.Fatalf("condition not established: pool %s does not report its two PGs active and clean on three OSDs: %+v", name, pgs)
		}
		choice := slices.IndexFunc(osds, func(osd *ceph.OSDContainer) bool {
			return slices.Contains(parent.Acting, osd.ID) && osd.ID != source.Acting[0] && osd.ID != parent.Acting[0]
		})
		if choice < 0 {
			t.Fatalf("condition not established: no OSD of %v is the primary of neither %s nor %s (%v)", parent.Acting, pool.Parent, pool.Source, source.Acting)
		}
		handle, err := pool.OverridePGPrimary(ctx, ceph.PGPrimary{PG: pool.Parent, OSD: osds[choice], RaiseMinCompatClient: true})
		if handle != nil {
			handles[name] = handle
			t.Cleanup(func() {
				restoreCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				if err := handle.Restore(restoreCtx); err != nil {
					t.Errorf("restore the primary override of %s: %v", name, err)
				}
			})
		}
		if err != nil {
			t.Fatal(err)
		}
		if name == mergeTargetPool {
			plan.Source, plan.Parent, plan.ParentOSD = string(pool.Source), string(pool.Parent), osds[choice].ID
		} else {
			plan.Control, plan.ControlOSD = string(pool.Parent), osds[choice].ID
		}
	}
	overridden := osdMap()
	if want := map[string]int{plan.Parent: plan.ParentOSD, plan.Control: plan.ControlOSD}; !maps.Equal(overridden.Primaries, want) {
		t.Fatalf("condition not established: the OSDMap has the primary overrides %v, want %v", overridden.Primaries, want)
	}
	if pgs := reports(pools[mergeTargetPool]); pgs[plan.Parent].Acting[0] != plan.ParentOSD || pgs[plan.Source].Acting[0] == plan.ParentOSD {
		t.Fatalf("condition not established: the override does not make osd.%d the primary of %s only: %+v", plan.ParentOSD, plan.Parent, pgs)
	}
	t.Logf("PG_MERGE_PRIMARY plan=%+v epoch=%d", plan, overridden.Epoch)

	// When: the PG count of the pool with the override and of the pool
	// without one is reduced. The pool without one must finish its merge:
	// the manager merges an eligible pool on this cluster.
	for _, name := range []string{mergeEligiblePool, mergeTargetPool} {
		if err := pools[name].SetPGCount(ctx, mergeTo); err != nil {
			t.Fatal(err)
		}
	}
	reduced := time.Now()
	eligibleCtx, cancelEligible := context.WithTimeout(ctx, 5*time.Minute)
	err = pools[mergeEligiblePool].WaitForPGCount(eligibleCtx, mergeTo)
	cancelEligible()
	if err != nil {
		t.Fatalf("condition not established: the pool without an override did not merge: %v", err)
	}
	t.Logf("PG_MERGE_PRIMARY eligible pool merged after %v", time.Since(reduced).Round(time.Second))

	// Then: the pool with the override is sampled until it has merged, has
	// been blocked for the hold or the limit has passed, before anything is
	// restored.
	sample := func() mergeSample {
		t.Helper()
		current := osdMap()
		counts, ok := current.Pools[mergeTargetPool]
		if !ok {
			t.Fatalf("the OSDMap has no pool %s", mergeTargetPool)
		}
		got := mergeSample{Counts: counts, Primaries: current.Primaries, PGs: reports(pools[mergeTargetPool])}
		t.Logf("PG_MERGE_PRIMARY sample epoch=%d counts=%+v primaries=%v pgs=%+v", current.Epoch, got.Counts, got.Primaries, got.PGs)
		return got
	}
	got := mergeOutcome{}
	for started := time.Now(); ; {
		next := sample()
		next.At = time.Since(started)
		got.Held = append(got.Held, next)
		state, err := plan.state(next)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := plan.hold(got.Held); state == mergeMerged || err == nil || next.At >= mergeLimit {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(5 * time.Second):
		}
	}
	for _, name := range []string{mergeTargetPool, mergeControlPool, mergeEligiblePool} {
		mergeRequireBytes(t, ctx, client, name)
	}
	// A pool that stayed blocked gets its override removed by the test, so
	// the judgement can tell a merge that waited for the override from one
	// that does not happen at all.
	held, _, err := plan.hold(got.Held)
	if err != nil {
		t.Fatal(err)
	}
	if held == mergeBlocked {
		if err := handles[mergeTargetPool].Restore(ctx); err != nil {
			t.Fatal(err)
		}
		removedCtx, cancelRemoved := context.WithTimeout(ctx, 5*time.Minute)
		err := pools[mergeTargetPool].WaitForPGCount(removedCtx, mergeTo)
		cancelRemoved()
		if err != nil {
			t.Fatalf("another fault: the pool did not merge after its override was removed: %v", err)
		}
		if err := observer.WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
		after := sample()
		got.AfterRemoval = &after
	}
	symptoms, err := judgeMerge(plan, got)
	if err != nil {
		t.Fatal(err)
	}
	// The overrides are given back and every pool still has its bytes.
	for _, name := range []string{mergeTargetPool, mergeControlPool} {
		if err := handles[name].Restore(ctx); err != nil {
			t.Fatalf("restore the primary override of %s: %v", name, err)
		}
	}
	if left := osdMap().Primaries; len(left) != 0 {
		t.Fatalf("the OSDMap keeps primary overrides after their restoration: %v", left)
	}
	if err := observer.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{mergeTargetPool, mergeControlPool, mergeEligiblePool} {
		mergeRequireBytes(t, ctx, client, name)
	}
	if t.Failed() {
		return
	}
	verdict := "NOT_REPRODUCED"
	if len(symptoms) > 0 {
		verdict = "REPRODUCED"
	}
	t.Logf("EXPERIMENT_RESULT case=mgr_pg_merge_primary_override result=%s image=%s symptoms=%q", verdict, image, symptoms)
}

// mergeRequireBytes requires that every object of pool reads back the bytes
// the case stored in it.
func mergeRequireBytes(t *testing.T, ctx context.Context, client testcontainers.Container, pool string) {
	t.Helper()
	var want strings.Builder
	for i := 1; i <= mergeObjects; i++ {
		fmt.Fprintf(&want, "%s object %d\n", pool, i)
	}
	got := harness.ExecOutput(t, ctx, client, "sh", "-ec", `for i in $(seq 1 "$2"); do timeout 60 rados -p "$1" get "object-$i" -; echo; done`, "sh", pool, fmt.Sprint(mergeObjects))
	if got != strings.TrimSpace(want.String()) {
		t.Fatalf("pool %s does not read back its bytes: %q", pool, got)
	}
}
