//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

// ecInstance identifies one running incarnation of an OSD container.
type ecInstance struct {
	Pid     int
	Started string
}

// ecRunningInstance refuses a container that is not running or was killed for
// memory, and names the incarnation so that a later check can tell a changed
// one.
func ecRunningInstance(ctx context.Context, osd *ceph.OSDContainer) (ecInstance, error) {
	state, err := osd.State(ctx)
	if err != nil {
		return ecInstance{}, err
	}
	if !state.Running || state.OOMKilled || state.Pid == 0 {
		return ecInstance{}, fmt.Errorf("osd.%d is not a running, healthy container: status %s, running %t, oom %t, pid %d", osd.ID, state.Status, state.Running, state.OOMKilled, state.Pid)
	}
	return ecInstance{Pid: state.Pid, Started: state.StartedAt}, nil
}

// ecStripeGeometry reads the pool's effective stripe width and refuses a
// profile with a chunk mapping, where a fragment index is not the raw shard.
func ecStripeGeometry(t *testing.T, ctx context.Context, cluster *ceph.Container, pool *ceph.Pool) uint64 {
	t.Helper()
	detail, err := cluster.Ceph(ctx, "osd", "pool", "ls", "detail", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var pools []struct {
		Name        string  `json:"pool_name"`
		StripeWidth *uint64 `json:"stripe_width"`
		Profile     string  `json:"erasure_code_profile"`
	}
	if err := json.Unmarshal(detail, &pools); err != nil {
		t.Fatalf("osd pool ls detail: %v: %.2048s", err, detail)
	}
	i := slices.IndexFunc(pools, func(p struct {
		Name        string  `json:"pool_name"`
		StripeWidth *uint64 `json:"stripe_width"`
		Profile     string  `json:"erasure_code_profile"`
	}) bool {
		return p.Name == pool.Name
	})
	if i < 0 || pools[i].StripeWidth == nil || *pools[i].StripeWidth == 0 {
		t.Fatalf("pool %s has no readable stripe_width: %.2048s", pool.Name, detail)
	}
	profile, err := cluster.Ceph(ctx, "osd", "erasure-code-profile", "get", pools[i].Profile, "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var native map[string]string
	if err := json.Unmarshal(profile, &native); err != nil {
		t.Fatalf("erasure-code-profile get: %v: %.2048s", err, profile)
	}
	if native["mapping"] != "" {
		t.Fatalf("profile %s has a chunk mapping %q: fragment indices are not raw shards", pools[i].Profile, native["mapping"])
	}
	t.Logf("EC_CONTRIBUTION_GEOMETRY pool %s stripe_width %d chunk %d", pool.Name, *pools[i].StripeWidth, *pools[i].StripeWidth/2)
	return *pools[i].StripeWidth
}

// ecContributionReceipt is printed on every outcome, with whatever the run
// reached, before the cluster is destroyed.
type ecContributionReceipt struct {
	PG              string   `json:"pg,omitempty"`
	Acting          []int    `json:"acting,omitempty"`
	Primary         int      `json:"primary"`
	RestoredOSD     int      `json:"restored_osd"`
	RestoredIndex   int      `json:"restored_index"`
	SecondOSD       int      `json:"second_osd"`
	SecondIndex     int      `json:"second_index"`
	ExactKState     string   `json:"exact_k_state,omitempty"`
	ExactKActing    []int    `json:"exact_k_acting,omitempty"`
	RecoveredDelta  int64    `json:"recovered_objects"`
	RequiredHead    string   `json:"required_head,omitempty"`
	RequiredClone   string   `json:"required_clone,omitempty"`
	ReadbackHead    string   `json:"readback_head_sha256,omitempty"`
	ReadbackSnap    string   `json:"readback_snap_sha256,omitempty"`
	ReadbackClients []uint64 `json:"readback_clients,omitempty"`
	StripeWidth     uint64   `json:"stripe_width"`
	Stage           string   `json:"stage"`
}

// testECRecoveredContribution shows that the recovered fragment index is what
// serves the object when exactly K indices remain.
//
// Why the restored index cannot be bypassed: the first holder is stopped
// before the overwrite is acknowledged, so its fragments of the new head and
// of the clone exist only if recovery wrote them. The second outage leaves the
// primary's index and the restored one, and a read of an EC object needs K
// distinct indices, so a correct readback cannot come from anywhere else.
//
// With min_size 2 the PG stays active with two acting members (pinned
// PeeringState::acting_set_writeable compares the acting set size with
// min_size); with the default K+1 it would be peered and refuse the read.
// The exact-K state is expected to be active with undersized and degraded and
// is not clean. Because a recovered peer keeps a stale last_complete in a PG
// that is not clean, the per-object classification is only asserted in the
// clean state before the second outage, never in the exact-K state.
func testECRecoveredContribution(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, client := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(3)))
	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{
		Name: "tc-ec-contribution", Application: "rados", PGNum: 1, MinSize: 2,
		ErasureCode: &ceph.ErasureCodeConfig{K: 2, M: 1, AllowOverwrites: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	ecPoolFacts(t, ctx, cluster, pool)
	name := pool.Name
	clean := ecWaitPG(t, ctx, cluster, name, "clean on three OSDs", func(pg ceph.PGState) bool {
		return pg.State == "active+clean" && len(pg.Acting) == 3 && !slices.Contains(pg.Acting, ecFragmentNone)
	})
	byID := map[int]*ceph.OSDContainer{}
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		byID[osd.ID] = osd
	}
	restoredIndex := -1
	for index, id := range clean.Acting {
		if id != clean.ActingPrimary && byID[id] != nil && restoredIndex < 0 {
			restoredIndex = index
		}
	}
	if restoredIndex < 0 {
		t.Fatalf("no owned non-primary fragment holder: %+v", clean)
	}
	plan, err := planECExactK(clean.Acting, clean.ActingPrimary, restoredIndex, 2)
	if err != nil {
		t.Fatal(err)
	}
	restored, second := byID[clean.Acting[restoredIndex]], byID[plan.SecondOSD]
	if restored == nil || second == nil || restored == second {
		t.Fatalf("holders are not two distinct owned OSDs: %+v, plan %+v", clean, plan)
	}
	receipt := ecContributionReceipt{
		PG: clean.PGID.String(), Acting: clean.Acting, Primary: clean.ActingPrimary,
		RestoredOSD: restored.ID, RestoredIndex: restoredIndex, SecondOSD: second.ID, SecondIndex: plan.SecondIndex,
		Stage: "placement",
	}
	// The receipt is printed on every outcome, a failure included.
	defer func() {
		raw, err := json.Marshal(receipt)
		if err != nil {
			t.Errorf("receipt: %v", err)
			return
		}
		t.Logf("EC_CONTRIBUTION_RECEIPT %s", raw)
	}()
	t.Logf("EC_CONTRIBUTION_PLACEMENT pg %s acting %v primary osd.%d restored osd.%d (fragment %d) second osd.%d (fragment %d), exact K keeps fragments %v",
		clean.PGID, clean.Acting, clean.ActingPrimary, restored.ID, restoredIndex, second.ID, plan.SecondIndex, plan.Remaining)

	// The primary and the second holder run through the first outage and the
	// recovery without a restart; the second outage must hit that very instance.
	primaryOSD := byID[clean.ActingPrimary]
	primaryStart, err := ecRunningInstance(ctx, primaryOSD)
	if err != nil {
		t.Fatal(err)
	}
	secondStart, err := ecRunningInstance(ctx, second)
	if err != nil {
		t.Fatal(err)
	}

	earlier := map[uint64]bool{}
	remember := func(r ecEligibleReceipt) {
		earlier[r.Writer], earlier[r.Reader] = true, true
	}
	written := ecEligiblePhase(t, ctx, cluster, client, name, "write", nil)
	remember(written)
	// The witness needs every remaining index to store bytes of this object;
	// otherwise zero padding could stand in for it. The overwrite keeps the size.
	receipt.StripeWidth = ecStripeGeometry(t, ctx, cluster, pool)
	if written.Size <= 0 {
		t.Fatalf("write receipt has no object size: %+v", written)
	}
	if err := requireECShardCoverage([]uint64{uint64(written.Size)}, receipt.StripeWidth, 2, plan.Remaining); err != nil {
		t.Fatal(err)
	}

	noout, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoOut, true)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		if err := noout.Restore(cleanup); err != nil {
			t.Errorf("restore noout: %v", err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	// First outage: the restored holder is down while the overwrite is acknowledged.
	receipt.Stage = "first outage"
	if _, err := ecRunningInstance(ctx, restored); err != nil {
		t.Fatal(err)
	}
	stopTimeout := 20 * time.Second
	if err := restored.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	firstDown := slices.Clone(clean.Acting)
	firstDown[restoredIndex] = ecFragmentNone
	ecWaitPG(t, ctx, cluster, name, "active without the restored holder", func(pg ceph.PGState) bool {
		return strings.HasPrefix(pg.State, "active+") && strings.Contains(pg.State, "degraded") && slices.Equal(pg.Acting, firstDown)
	})
	overwritten := ecEligiblePhase(t, ctx, cluster, client, name, "overwrite", &written)
	remember(overwritten)
	degraded := ecObserve(t, ctx, cluster, name, "degraded")
	deg, degClone := ecClassify(t, degraded, overwritten.Writer, overwritten.Snap, true)
	for _, c := range []ecClassification{deg, degClone} {
		ecExpect(t, "degraded", c, ecSufficient, ecUnobserved, []int{restoredIndex})
	}
	recoveredBefore := degraded.After.Info.Stats.StatSum.Recovered

	// Recovery: the holder returns and the PG becomes clean again.
	receipt.Stage = "first recovery"
	if err := restored.Start(ctx); err != nil {
		t.Fatal(err)
	}
	restoredStart, err := ecRunningInstance(ctx, restored)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	ecWaitPG(t, ctx, cluster, name, "clean on three OSDs again", func(pg ceph.PGState) bool {
		return pg.State == "active+clean" && slices.Equal(pg.Acting, clean.Acting)
	})
	final := ecObserve(t, ctx, cluster, name, "recovered")
	head, clone := ecClassify(t, final, overwritten.Writer, overwritten.Snap, true)
	for _, c := range []ecClassification{head, clone} {
		ecExpect(t, "recovered", c, ecSufficient, ecEligible, []int{0, 1, 2})
	}
	receipt.RequiredHead, receipt.RequiredClone = fmt.Sprint(head.Need), fmt.Sprint(clone.Need)
	receipt.RecoveredDelta = final.After.Info.Stats.StatSum.Recovered - recoveredBefore
	if receipt.RecoveredDelta < 1 {
		t.Fatalf("num_objects_recovered went from %d to %d across the recovery", recoveredBefore, final.After.Info.Stats.StatSum.Recovered)
	}
	remember(ecEligiblePhase(t, ctx, cluster, client, name, "read", &overwritten))

	// Second outage: a different holder, so exactly K indices remain.
	receipt.Stage = "second outage"
	states, err := harness.ObserveCluster(cluster).OSDStates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range clean.Acting {
		if i := slices.IndexFunc(states, func(s ceph.OSDState) bool { return s.ID == id }); i < 0 || !states[i].Up || !states[i].In {
			t.Fatalf("osd.%d is not up and in before the second outage: %+v", id, states)
		}
	}
	// Admission: the same running instances as before. The restored holder is
	// the incarnation that Start created, and neither the primary nor the second
	// holder may have restarted, crashed or been killed for memory.
	for _, check := range []struct {
		osd  *ceph.OSDContainer
		want ecInstance
	}{{primaryOSD, primaryStart}, {restored, restoredStart}, {second, secondStart}} {
		now, err := ecRunningInstance(ctx, check.osd)
		if err != nil {
			t.Fatal(err)
		}
		if now != check.want {
			t.Fatalf("osd.%d changed instance before the second outage: %+v, then %+v", check.osd.ID, check.want, now)
		}
	}
	if err := second.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	exact := ecWaitPG(t, ctx, cluster, name, "active with exactly K fragments", func(pg ceph.PGState) bool {
		return strings.HasPrefix(pg.State, "active+") && slices.Equal(pg.Acting, plan.WantActing)
	})
	receipt.ExactKState, receipt.ExactKActing = exact.State, exact.Acting
	t.Logf("EC_CONTRIBUTION_EXACT_K pg %s state %s acting %v up %v primary osd.%d", exact.PGID, exact.State, exact.Acting, exact.Up, exact.ActingPrimary)
	if exact.ActingPrimary != clean.ActingPrimary || strings.Contains(exact.State, "clean") ||
		!strings.Contains(exact.State, "undersized") || !strings.Contains(exact.State, "degraded") {
		t.Fatalf("exact-K state %+v: want the same primary osd.%d and active+undersized+degraded", exact, clean.ActingPrimary)
	}

	// The independent readback that can only be served by the restored index.
	receipt.Stage = "exact-K readback"
	got := ecEligiblePhase(t, ctx, cluster, client, name, "read", &overwritten)
	receipt.ReadbackHead, receipt.ReadbackSnap, receipt.ReadbackClients = got.HeadSHA256, got.SnapSHA256, []uint64{got.Writer, got.Reader}
	if err := checkECRecoveredRead(overwritten, got, earlier); err != nil {
		t.Fatal(err)
	}
	remember(got)

	// Restore the second holder independently and close the data.
	receipt.Stage = "restore"
	if err := second.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := noout.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	ecWaitPG(t, ctx, cluster, name, "clean on three OSDs after the second outage", func(pg ceph.PGState) bool {
		return pg.State == "active+clean" && slices.Equal(pg.Acting, clean.Acting)
	})
	ecEligiblePhase(t, ctx, cluster, client, name, "verify", &overwritten)
	receipt.Stage = "complete"
}
