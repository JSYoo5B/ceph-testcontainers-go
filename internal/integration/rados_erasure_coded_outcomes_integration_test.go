//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rados_fixtures))))

//ci: timeout=120m job-timeout=130 image-roles=essential,client,cephfs

package integration_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
)

// crushItemNone marks a missing shard in an EC PG's up and acting sets.
const crushItemNone = 2147483647

// Erasure-coded pools accept fewer write operations than replicated pools,
// and the set depends on allow_ec_overwrites. The fixture supplies owned
// k=2, m=1 pools on three BlueStore OSDs; native clients record each
// outcome, and an independent client reads every object back, also while a
// shard is missing.
func TestRADOSErasureCodedOutcomes(t *testing.T) {
	parallelWhenEnabled(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t, ceph.WithOSDCount(3))
	const appendOnly, overwrite = "tc-ec-append", "tc-ec-overwrite"
	// MinSize 2 (K) keeps the PGs active while one shard is missing.
	for _, config := range []ceph.PoolConfig{
		{Name: appendOnly, Application: "rados", PGNum: 1, MinSize: 2, ErasureCode: &ceph.ErasureCodeConfig{K: 2, M: 1}},
		{Name: overwrite, Application: "rados", PGNum: 1, MinSize: 2, ErasureCode: &ceph.ErasureCodeConfig{K: 2, M: 1, AllowOverwrites: true}},
	} {
		if _, err := cluster.CreatePool(ctx, config); err != nil {
			t.Fatal(err)
		}
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	erasureCodedPhase(t, ctx, cluster, client, "opcodes", appendOnly, "0")
	erasureCodedPhase(t, ctx, cluster, client, "opcodes", overwrite, "1")

	// A missing data shard: reads reconstruct the object, and a write under a
	// snapshot context still preserves a clone. Recovery restores the shard.
	receipt := erasureCodedPhase(t, ctx, cluster, client, "shard-write", overwrite)
	pgs, err := poolHandle(t, ctx, cluster, overwrite).PGs(ctx)
	if err != nil || len(pgs.PGs) != 1 || len(pgs.PGs[0].Acting) != 3 || slices.Contains(pgs.PGs[0].Acting, crushItemNone) {
		t.Fatalf("PG of %s before the failure: %+v, %v", overwrite, pgs.PGs, err)
	}
	clean := pgs.PGs[0]
	victim := clean.Acting[1]
	var osd *ceph.OSDContainer
	for _, candidate := range clusteraccess.OSDs(t, cluster) {
		if candidate.ID == victim {
			osd = candidate
		}
	}
	if osd == nil || victim == clean.ActingPrimary {
		t.Fatalf("shard 1 is on osd.%d, which is not an owned non-primary OSD: %+v", victim, clean)
	}
	noout, err := cluster.OverrideOSDFlag(ctx, ceph.OSDFlagNoOut, true)
	if noout != nil {
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := noout.Restore(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	stop := 20 * time.Second
	if err := osd.Stop(ctx, &stop); err != nil {
		t.Fatal(err)
	}
	var degraded ceph.PGState
	for deadline := time.Now().Add(2 * time.Minute); ; {
		pgs, err := poolHandle(t, ctx, cluster, overwrite).PGs(ctx)
		if err == nil && len(pgs.PGs) == 1 {
			degraded = pgs.PGs[0]
			if strings.HasPrefix(degraded.State, "active+") && strings.Contains(degraded.State, "degraded") &&
				slices.Equal(degraded.Acting, []int{clean.Acting[0], crushItemNone, clean.Acting[2]}) {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("PG of %s did not go active without shard 1: %+v, %v", overwrite, degraded, err)
		}
		time.Sleep(time.Second)
	}
	receipt = erasureCodedPhase(t, ctx, cluster, client, "shard-degraded", overwrite, string(receipt))
	if err := osd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := noout.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	pgs, err = poolHandle(t, ctx, cluster, overwrite).PGs(ctx)
	if err != nil || len(pgs.PGs) != 1 || !slices.Equal(pgs.PGs[0].Acting, clean.Acting) {
		t.Fatalf("PG of %s after recovery: %+v, %v", overwrite, pgs.PGs, err)
	}
	erasureCodedPhase(t, ctx, cluster, client, "shard-verify", overwrite, string(receipt))
	t.Logf("%s served reads and a write as %s with acting %v and recovered to %v", overwrite, degraded.State, degraded.Acting, clean.Acting)

	for _, pool := range []string{appendOnly, overwrite} {
		if err := poolHandle(t, ctx, cluster, pool).Remove(ctx); err != nil {
			t.Fatal(err)
		}
	}
}
