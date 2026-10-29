//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_mon_majority_loss))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package integration_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
)

// TestMonMajorityLossLocalOracle loses and regains the majority of a five-MON
// quorum and decides every state from the MONs' own local mon_status, which
// is served without a quorum. Three of five MONs lose their public endpoint,
// one after another and without a process restart, then regain it in two
// steps. The identity of every MON (FSID, container generation, public
// address, monmap, uptime continuity) is captured before the first cut.
//
// This is the local-MON oracle only. It establishes that the majority was
// actually lost and regained, not what a retained client, a fresh connection
// or a write does meanwhile; those receipts are separate scenarios.
func TestMonMajorityLossLocalOracle(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, _ := newServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(5)))
	monitors := clusteraccess.Monitors(t, cluster)
	if len(monitors) != 5 {
		t.Fatal("fixture did not construct five monitors")
	}
	rec := newMonReplacementRecorder("majority-loss", withSockets(t, cluster), monReplacementBounds{
		Operation: 4 * time.Minute, Quorum: 90 * time.Second, QuorumInterval: 500 * time.Millisecond,
		Query: 8 * time.Second, Snapshot: 30 * time.Second, Bundle: 3 * time.Minute,
	})
	baseline, err := captureMajorityBaseline(ctx, rec, withSockets(t, cluster), 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("MON_MAJORITY_BASELINE ceph=%s fsid=%s members=%v monmap_epoch=%d election_epoch=%d", staticFacts(cluster).CephVersion, baseline.FSID, baseline.Members, baseline.MonMapEpoch, baseline.ElectionEpoch)
	for _, name := range baseline.Members {
		m := baseline.Mons[name]
		t.Logf("MON_MAJORITY_BASELINE mon.%s container=%.12s started=%s address=%s uptime_ms=%d", name, m.ContainerID, m.StartedAt, m.Address, m.UptimeMS)
	}

	var cuts []*fault.NetworkInterruption
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 2*time.Minute)
		defer done()
		for _, cut := range cuts {
			if err := cut.Restore(cleanup); err != nil {
				t.Error("restore interrupted MON endpoint", err)
			}
		}
	})
	cut := func(name string) {
		t.Helper()
		for _, mon := range monitors {
			if mon.DaemonName != name {
				continue
			}
			link, err := fault.InterruptNetwork(ctx, cluster, mon, ceph.PublicNetworkPlane)
			if link != nil {
				cuts = append(cuts, link)
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("MON_MAJORITY_CUT mon.%s endpoint=%s", name, link.OriginalAddress)
			return
		}
		t.Fatalf("no owned monitor %s", name)
	}
	wait := func(step string, want []string, w majorityWait) []majorityRound {
		t.Helper()
		rounds, err := waitMajorityState(ctx, rec, withSockets(t, cluster), baseline, want, w, func(format string, args ...any) {
			t.Logf("MON_MAJORITY_"+step+" "+format, args...)
		})
		if err != nil {
			t.Fatal(step, err)
		}
		return rounds
	}
	majority := baseline.Members[2:]
	// A quorum state needs to persist over a few rounds; an outage must hold
	// long enough to outlast the lease and election timeouts of the cut MONs.
	quorumWait := majorityWait{Window: 4 * time.Second, Interval: 2 * time.Second, Bound: 2 * time.Minute, MinRounds: 2}
	outageWait := majorityWait{Window: 20 * time.Second, Interval: 2 * time.Second, Bound: 2 * time.Minute, MinRounds: 5}

	// Control: two MONs are isolated and the other three keep the quorum.
	cut(baseline.Members[0])
	cut(baseline.Members[1])
	wait("MINORITY_CUT", majority, quorumWait)

	// The third MON is isolated alone: a connected pair and three singletons,
	// none of which can reach three of five.
	cut(baseline.Members[2])
	rounds := wait("MAJORITY_LOSS", nil, outageWait)
	management := probeMajorityManagement(ctx, withSockets(t, cluster), 30*time.Second)
	t.Logf("MON_MAJORITY_LOSS_MANAGEMENT quorum_query_duration=%v quorum=%v error=%q (evidence only)", management.Duration.Round(time.Millisecond), management.Quorum, management.Error)
	if after := observeMajorityRound(ctx, rec, withSockets(t, cluster), baseline); len(evaluateMajorityRound(baseline, after, nil)) > 0 {
		t.Fatalf("the outage did not persist through the management query: %s", after.summary())
	}
	for _, mon := range monitors {
		if !mon.IsRunning() {
			t.Fatalf("mon.%s stopped during the network cut", mon.DaemonName)
		}
	}
	t.Logf("MON_MAJORITY_LOSS rounds=%d first=%s last=%s", len(rounds), rounds[0].At.UTC().Format(time.RFC3339), rounds[len(rounds)-1].At.UTC().Format(time.RFC3339))

	// Restore the third endpoint first: the original pair plus it form 3/5.
	if err := cuts[2].Restore(ctx); err != nil {
		t.Fatal(err)
	}
	wait("MAJORITY_REGAINED", majority, quorumWait)
	if err := cuts[0].Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cuts[1].Restore(ctx); err != nil {
		t.Fatal(err)
	}
	rounds = wait("FULL_QUORUM", baseline.Members, quorumWait)

	last := rounds[len(rounds)-1]
	// A pair of queries too slow to tell a restart from a continuing process
	// is taken again under the same bounds, not accepted.
	for attempt := 1; ; attempt++ {
		problems, undecided := checkMajorityContinuity(baseline, last)
		if len(problems) > 0 {
			t.Fatalf("a monitor process did not continue across the outage: %v", problems)
		}
		if len(undecided) == 0 {
			break
		}
		if attempt == 3 {
			t.Fatalf("process continuity stayed undecided after %d observations: %v", attempt, undecided)
		}
		t.Logf("MON_MAJORITY_CONTINUITY attempt %d undecided, observing again: %v", attempt, undecided)
		last = observeMajorityRound(ctx, rec, withSockets(t, cluster), baseline)
		if reasons := evaluateMajorityRound(baseline, last, baseline.Members); len(reasons) > 0 {
			t.Fatalf("the recovered quorum changed while continuity was re-observed: %v", reasons)
		}
	}
	// A fresh quorum-dependent management query works again, on the same
	// monmap and the original public addresses.
	status, err := observeCluster(cluster).QuorumStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(sortedClone(status.QuorumNames), baseline.Members) || status.MonMap.FSID != baseline.FSID {
		t.Fatalf("control quorum after recovery: %v fsid %s", status.QuorumNames, status.MonMap.FSID)
	}
	if problems := checkMajorityAddresses(baseline, status); len(problems) > 0 {
		t.Fatalf("monmap members or addresses changed: %v", problems)
	}
	t.Logf("MON_MAJORITY_RECOVERED quorum=%v election_epoch=%d monmap_epoch=%d uptime_continuity=ok addresses=unchanged", status.QuorumNames, last.Samples[0].ElectionEpoch, last.Samples[0].MonMapEpoch)
}
