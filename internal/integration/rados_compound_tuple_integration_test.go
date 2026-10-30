//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_rados_compound_tuple))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package integration_test

import (
	"context"
	"encoding/json"
	"maps"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// TestRADOSCompoundTupleNoFault is the no-fault native control of the compound
// tuple oracle. One object is seeded with tuple A by one compound write
// operation. A compound read observes A before any submission. One compound
// write then replaces it with tuple B, once and without replay, and the
// original completion must be a success; the compound read must then observe B.
//
// It exercises the compound write (write_full, xattr, OMAP in one WriteOp) and
// the compound read (stat, data read, xattrs and OMAP value in one read op)
// against a real cluster. No daemon is stopped or killed: it says nothing about
// an interrupted write, crash durability or the OSD's commit path.
func TestRADOSCompoundTupleNoFault(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	opts = append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	cluster, err := ceph.Run(ctx, image, opts...)
	if cluster != nil {
		t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
			defer done()
			if err := cluster.Terminate(cleanup); err != nil {
				t.Error("compound tuple cleanup independently failed", err)
			}
		})
	}
	if err != nil {
		t.Fatal("fixture setup", err)
	}
	const pool, ledger = "tuple-data", "/tmp/tc-compound-tuple.jsonl"
	_, err = cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rados", Replicas: 1, MinSize: 1, PGNum: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	client, err := cluster.ClientContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	poolState, err := harness.PoolHandle(t, ctx, cluster, pool).Status(ctx)
	if err != nil || poolState.ID <= 0 {
		t.Fatalf("pool identity unavailable: %+v %v", poolState, err)
	}
	quorum, err := harness.ObserveCluster(cluster).QuorumStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	identity := tupleIdentity{Object: tupleObject, Pool: pool, PoolID: int64(poolState.ID), FSID: quorum.MonMap.FSID}
	osds := osdMemoryIdentities(t, ctx, cluster)
	platform := osdMemoryExec(t, ctx, client, "uname", "-sm")
	t.Logf("RADOS_TUPLE_RUNTIME ceph=%s platform=%s fsid=%s pool=%s pool_id=%d osds=%v", harness.StaticFacts(cluster).CephVersion, platform, identity.FSID, pool, identity.PoolID, osds)

	// judge records the receipt before it asserts, so that a failed phase keeps
	// its ledger and observation.
	judge := func(label, want string, wantOutcome string, stopped bool) tupleResult {
		t.Helper()
		events := tupleLedgerOf(t, ctx, cluster, client, pool, ledger)
		observation := tuplePhase(t, ctx, cluster, client, "read", pool, ledger).Stdout
		result := tupleJudge(events, observation, identity, stopped)
		ledgerJSON, _ := json.Marshal(events)
		t.Logf("RADOS_TUPLE_%s outcome=%s status=%s matched=%s reason=%q\n ledger=%s\n observation=%s", label, result.Outcome, result.Status, result.Matched, result.Reason, ledgerJSON, observation)
		if result.Status != "accepted" || result.Outcome != wantOutcome || result.Matched != want {
			t.Fatalf("%s: %+v, want accepted outcome %s matching tuple %s", label, result, wantOutcome, want)
		}
		return result
	}

	tuplePhase(t, ctx, cluster, client, "seed", pool, ledger)
	judge("SEED", "A", "pre_submission", false)
	// The pre-submission control: reading again changes nothing and still shows A.
	judge("PRESUBMIT", "A", "pre_submission", false)

	submit := tuplePhase(t, ctx, cluster, client, "submit", pool, ledger)
	events := tupleLedgerOf(t, ctx, cluster, client, pool, ledger)
	stopped := tupleStopped(events, submit.ExitCode, submit.Complete())
	if !stopped || tupleChannel(events) != "complete" {
		t.Fatalf("the actor's stop or callback channel was not recorded: stopped=%t channel=%s %+v", stopped, tupleChannel(events), events)
	}
	judge("SUBMIT", "B", "known_success", stopped)
	// A second, independent compound read still observes the same tuple.
	judge("REREAD", "B", "known_success", stopped)

	after := osdMemoryIdentities(t, ctx, cluster)
	if !maps.Equal(osds, after) {
		t.Fatalf("OSD identity changed: %v then %v", osds, after)
	}
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		state, err := osd.State(ctx)
		if err != nil || state == nil || !state.Running || state.OOMKilled {
			t.Fatalf("OSD %d did not stay running: %+v %v", osd.ID, state, err)
		}
	}
	t.Logf("RADOS_TUPLE_CONTROL seeded=A presubmit=A submitted=1 completion=success observed=B compound_read=1 osds=%d", len(after))
}
