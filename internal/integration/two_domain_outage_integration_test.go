//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_two_domain_outage))))

//ci: timeout=25m job-timeout=40 image-roles=essential,client

package integration_test

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
)

const twoDomainPool = "tc-two-domain-ledger"

// TestTwoDomainLogicalOutageLedger stops the MON, the OSD and the MGR of two
// of three logical hosts, one after the other, while a retained librados
// client outside the stopped containers keeps writing unique objects into a
// ledger. One MON of three cannot form a quorum and one OSD of three is below
// the pool's min_size of two, so no write can be acknowledged while the outage
// is fully applied. A write still pending when the restoration starts may be
// acknowledged afterwards; it is counted apart and never credited to the outage. The hosts are
// labels on one container engine: this is a correlated stop of six daemons and
// says nothing about a physical host, rack, power or switch failure.
//
// The scenario keeps the outcomes of the retained client apart. Operations
// that began under the outage token and returned an error, or that never
// returned, are unknown outcomes: they are classified, never replayed, and
// never promoted to an acknowledgement by the readback. Only an acknowledged
// operation must be present and exact after recovery. The survivor MON is
// decided from its attributed local mon_status, not from a client call.
func TestTwoDomainLogicalOutageLedger(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	hosts := []string{"host-a", "host-b", "host-c"}
	layout := make([]ceph.OSDConfig, 0, len(hosts))
	for i, host := range hosts {
		layout = append(layout, ceph.OSDConfig{Host: host, Rack: "rack-" + string(rune('a'+i)), Root: "tc-storage", DeviceClass: "fast"})
	}
	cluster, client := newServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(3).WithManagerCount(2)), ceph.WithStartupTimeout(3*time.Minute),
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithInitialOSDs(layout...).WithPoolDefaults(3, 2).WithDefaultCRUSHRoot("tc-storage")))
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: twoDomainPool, PGNum: 1, Application: "rados", Replicas: 3, MinSize: 2,
		FailureDomain: "host", CRUSHRoot: "tc-storage", DeviceClass: "fast"}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}

	// Map 1: native CRUSH placement of the OSDs, read from Ceph.
	osds := clusteraccess.OSDs(t, cluster)
	hostByOSD := map[int]string{}
	for _, osd := range osds {
		var location struct {
			OSD   int `json:"osd"`
			CRUSH struct {
				Host string `json:"host"`
			} `json:"crush_location"`
		}
		twoDomainJSON(t, ctx, cluster, &location, "osd", "find", strconv.Itoa(osd.ID))
		if location.OSD != osd.ID || location.CRUSH.Host != osd.Placement().Host || !slices.Contains(hosts, location.CRUSH.Host) {
			t.Fatalf("osd.%d native CRUSH host %q differs from its requested %q", osd.ID, location.CRUSH.Host, osd.Placement().Host)
		}
		hostByOSD[osd.ID] = location.CRUSH.Host
	}
	monitors, managers := clusteraccess.Monitors(t, cluster), clusteraccess.Managers(t, cluster)
	if len(osds) != 3 || len(monitors) != 3 || len(managers) != 2 {
		t.Fatalf("membership: %d OSDs, %d MONs, %d MGRs", len(osds), len(monitors), len(managers))
	}
	states, err := observeCluster(cluster).OSDStates(ctx)
	if err != nil || len(states) != 3 {
		t.Fatalf("initial OSD identities: %v %+v", err, states)
	}
	for _, state := range states {
		if !state.Up || !state.In || state.UUID == "" {
			t.Fatalf("initial OSD identity: %+v", state)
		}
	}
	acting := func() []int {
		var mapping struct {
			Acting []int `json:"acting"`
		}
		twoDomainJSON(t, ctx, cluster, &mapping, "osd", "map", twoDomainPool, "op-probe")
		return mapping.Acting
	}
	spanHosts := func() int {
		span := map[string]bool{}
		for _, id := range acting() {
			span[hostByOSD[id]] = true
		}
		return len(span)
	}
	if len(acting()) != 3 || spanHosts() != 3 {
		t.Fatalf("ledger PG replicas %v do not span three hosts", acting())
	}

	// Map 2: outage membership, which also places MONs and MGRs. Nothing in
	// Ceph says where they live; this manifest is the simulation's claim.
	managerBefore, err := observeCluster(cluster).ManagerStatus(ctx)
	if err != nil || !managerBefore.Available || len(managerBefore.Standbys) != 1 {
		t.Fatalf("initial MGR identities: %+v %v", managerBefore, err)
	}
	managerHost := map[string]string{managerBefore.ActiveName: hosts[0], managerBefore.Standbys[0].Name: hosts[1]}
	owned := map[string]bool{}
	var members []outageMember
	for i, mon := range monitors {
		owned[mon.GetContainerID()] = true
		name := mon.DaemonName
		members = append(members, outageMember{Role: "mon", Name: name, Host: hosts[i], Container: mon.GetContainerID(), daemon: outageContainer{mon.Container},
			current: func() string {
				for _, m := range clusteraccess.Monitors(t, cluster) {
					if m.DaemonName == name {
						return m.GetContainerID()
					}
				}
				return ""
			}})
	}
	for _, mgr := range managers {
		owned[mgr.GetContainerID()] = true
		name := mgr.DaemonName
		host, ok := managerHost[name]
		if !ok {
			t.Fatalf("manager %s is neither the active nor the standby identity", name)
		}
		members = append(members, outageMember{Role: "mgr", Name: name, Host: host, Container: mgr.GetContainerID(), daemon: outageContainer{mgr.Container},
			current: func() string {
				for _, m := range clusteraccess.Managers(t, cluster) {
					if m.DaemonName == name {
						return m.GetContainerID()
					}
				}
				return ""
			}})
	}
	survivorOSD := -1
	for _, osd := range osds {
		owned[osd.GetContainerID()] = true
		id := osd.ID
		if hostByOSD[id] == hosts[2] {
			survivorOSD = id
		}
		members = append(members, outageMember{Role: "osd", Name: strconv.Itoa(id), Host: hostByOSD[id], Container: osd.GetContainerID(), daemon: outageContainer{osd.Container},
			current: func() string {
				for _, o := range clusteraccess.OSDs(t, cluster) {
					if o.ID == id {
						return o.GetContainerID()
					}
				}
				return ""
			}})
	}
	manifest, err := newOutageManifest(hosts, members, owned)
	if err != nil {
		t.Fatal(err)
	}
	if owned[client.GetContainerID()] || owned[clusteraccess.ControlContainer(t, cluster).GetContainerID()] {
		t.Fatal("the client or control container is inside the outage membership")
	}
	if monitors[0].DaemonName != "a" || survivorOSD < 0 {
		t.Fatalf("primary MON %q, surviving OSD %d", monitors[0].DaemonName, survivorOSD)
	}
	// The fixed-address primary MON stops last and starts first. A native run
	// of the one-host scenario failed when other containers started before it
	// and "Address already in use" followed; that cause is an inference.
	selection, err := newTwoDomainPlan(manifest, []string{hosts[0], hosts[1]}, "a")
	if err != nil {
		t.Fatal(err)
	}
	var survivorMON outageMember
	for _, m := range manifest.onHost(hosts[2]) {
		if m.Role == "mon" {
			survivorMON = m
		}
	}
	survivorContainers := []outageMember{survivorMON}
	for _, m := range manifest.onHost(hosts[2]) {
		if m.Role == "osd" {
			survivorContainers = append(survivorContainers, m)
		}
	}
	if len(selection.SurvivingMONs) != 1 || selection.SurvivingMONs[0] != survivorMON.Name || len(survivorContainers) != 2 {
		t.Fatalf("survivors: %+v %+v", selection, survivorContainers)
	}
	rec := newMonReplacementRecorder("two-domain", withSockets(t, cluster), monReplacementBounds{
		Operation: 4 * time.Minute, Quorum: 90 * time.Second, QuorumInterval: 500 * time.Millisecond,
		Query: 8 * time.Second, Snapshot: 30 * time.Second, Bundle: 3 * time.Minute,
	})
	baseline, err := captureMajorityBaseline(ctx, rec, withSockets(t, cluster), 3)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	plan := newOutagePlan(func() time.Duration { return time.Since(start) }, selection.Members)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 4*time.Minute)
		defer stop()
		outageFinish(cleanup, plan, t.Logf, t.Errorf)
	})
	manifestJSON, _ := json.Marshal(manifest)
	var order []string
	for _, m := range selection.Members {
		order = append(order, m.Role+"."+m.Name)
	}
	t.Logf("TWO_DOMAIN_OUTAGE_MANIFEST ceph=%s platform=%s essential_image=%s client_image=%s stop_order=%s survivor_mon=%s survivor_osd=%d election_epoch=%d manifest=%s",
		staticFacts(cluster).CephVersion, strings.TrimSpace(string(topologyExecOutput(t, ctx, client, "uname", "-sm"))), imageID(t, ctx, cluster.Container), imageID(t, ctx, client),
		strings.Join(order, ","), survivorMON.Name, survivorOSD, baseline.ElectionEpoch, manifestJSON)

	// Baseline: the retained client writes under the baseline token.
	run := "r" + strconv.FormatInt(time.Now().Unix(), 10)
	outageToken(t, ctx, cluster, client, twoDomainPool, run, outagePhaseBaseline)
	actor := startNative(ctx, cluster, client, outageCommand(twoDomainPool, run, "actor"))
	finished := false
	t.Cleanup(func() {
		if finished {
			return
		}
		actor.cancel()
		select {
		case outcome := <-actor.done:
			t.Logf("TWO_DOMAIN_OUTAGE_ACTOR cleanup error=%v", outcome.err)
		case <-time.After(30 * time.Second):
			t.Error("the retained actor could not be confirmed stopped")
		}
	})
	waitOps := func(want func(map[string]int, []outageOp) bool, what string, bound time.Duration) []outageOp {
		t.Helper()
		deadline := time.Now().Add(bound)
		for {
			select {
			case outcome := <-actor.done:
				finished = true
				t.Fatalf("the retained actor ended while waiting for %s: err=%v exit=%d stdout=%s stderr=%s", what, outcome.err, outcome.result.ExitCode, outcome.result.Stdout, outcome.result.Stderr)
			default:
			}
			ops, err := outageOps(outageLedger(t, ctx, cluster, client, twoDomainPool, run), run)
			if err != nil {
				t.Fatal("ledger refused:", err)
			}
			if _, err := twoDomainLedgerOutcomes(ops); err != nil {
				t.Fatal(err)
			}
			if want(outageTally(ops), ops) {
				return ops
			}
			if time.Now().After(deadline) {
				t.Fatalf("waiting for %s: tally %v", what, outageTally(ops))
			}
			time.Sleep(2 * time.Second)
		}
	}
	waitOps(func(tally map[string]int, _ []outageOp) bool { return tally[outagePhaseBaseline+"/acked"] >= 3 }, "baseline acknowledgements", 3*time.Minute)

	// Inject. The token precedes the first stop request, so every operation
	// that begins under it overlaps a partial injection and is never credited.
	outageToken(t, ctx, cluster, client, twoDomainPool, run, outagePhaseApplying)
	if err := plan.Apply(ctx, 5*time.Second); err != nil {
		t.Fatalf("partly applied outage, restore follows in cleanup: %v", err)
	}
	interval, ok := plan.AppliedInterval()
	if !ok || interval <= 0 {
		t.Fatalf("no applied interval: %+v", plan.Receipts)
	}

	// Admission after the fault: the surviving containers are the captured
	// instances and still running. Only then does the lost majority count.
	for _, m := range survivorContainers {
		if m.current() != m.Container {
			t.Fatalf("surviving %s.%s now runs in another container", m.Role, m.Name)
		}
		if running, err := m.daemon.Running(ctx); err != nil || !running {
			t.Fatalf("surviving %s.%s is not running: %v", m.Role, m.Name, err)
		}
	}
	survivors, stopped := selection.SurvivingMONs, selection.LostMONs
	rounds, err := waitStableRounds(majorityWait{Window: 20 * time.Second, Interval: 2 * time.Second, Bound: 2 * time.Minute, MinRounds: 5},
		func() majorityRound { return observeMajorityRound(ctx, rec, withSockets(t, cluster), baseline) },
		func(round majorityRound) []string { return evaluateSurvivorRound(round, survivors, stopped) })
	if err != nil {
		t.Fatal("lost majority not shown by the survivor: ", err)
	}
	last := rounds[len(rounds)-1]
	for _, name := range stopped {
		plan.Observe("mon", name)
	}
	t.Logf("TWO_DOMAIN_OUTAGE_QUORUM_LOST rounds=%d applied_interval_ms=%d last=%s", len(rounds), interval.Milliseconds(), last.summary())
	management := probeMajorityManagement(ctx, withSockets(t, cluster), 20*time.Second)
	t.Logf("TWO_DOMAIN_OUTAGE_MANAGEMENT quorum_query_duration=%v quorum=%v error=%q (evidence only)", management.Duration.Round(time.Millisecond), management.Quorum, management.Error)

	// The token is issued once the lost majority was shown. OSD and MGR stops
	// are known from the container engine, which Apply checked; Ceph cannot
	// report them without a quorum.
	outageToken(t, ctx, cluster, client, twoDomainPool, run, outagePhaseOutage)
	ops := waitOps(func(_ map[string]int, ops []outageOp) bool {
		out, _ := twoDomainLedgerOutcomes(ops)
		return out.Failed >= 1
	}, "an unacknowledged operation that began under the fully applied outage", 4*time.Minute)
	outcomes, _ := twoDomainLedgerOutcomes(ops)
	after := observeMajorityRound(ctx, rec, withSockets(t, cluster), baseline)
	if reasons := evaluateSurvivorRound(after, survivors, stopped); len(reasons) > 0 {
		t.Fatalf("the lost majority did not persist through the client observation: %v", reasons)
	}
	outcomesJSON, _ := json.Marshal(outcomes)
	t.Logf("TWO_DOMAIN_OUTAGE_DURING outcomes=%s tally=%v persisted=%s", outcomesJSON, outageTally(ops), after.summary())

	// Restore the same containers and stores; the primary MON starts first.
	outageToken(t, ctx, cluster, client, twoDomainPool, run, outagePhaseRestoring)
	if err := plan.Restore(ctx, "primary"); err != nil {
		t.Fatal(err)
	}
	recoverCtx, recoverCancel := context.WithTimeout(ctx, 4*time.Minute)
	defer recoverCancel()
	for {
		q, qerr := observeCluster(cluster).QuorumStatus(recoverCtx)
		m, merr := observeCluster(cluster).ManagerStatus(recoverCtx)
		s, serr := observeCluster(cluster).OSDStates(recoverCtx)
		if qerr == nil && merr == nil && serr == nil && len(q.QuorumNames) == 3 && m.Available && len(m.Standbys) == 1 &&
			slices.EqualFunc(states, s, func(a, b ceph.OSDState) bool { return a.ID == b.ID && a.UUID == b.UUID && b.Up && b.In }) {
			break
		}
		select {
		case <-recoverCtx.Done():
			t.Fatalf("no recovery: quorum %v/%v manager %v/%v osds %v/%v", q.QuorumNames, qerr, m, merr, s, serr)
		case <-time.After(time.Second):
		}
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	for _, m := range manifest.Members {
		if m.current() != m.Container {
			t.Fatalf("%s.%s now runs in another container", m.Role, m.Name)
		}
	}
	if len(acting()) != 3 || spanHosts() != 3 {
		t.Fatalf("redundancy not restored across hosts: %v", acting())
	}
	// The survivor MON was never restarted: its uptime continues.
	for attempt := 1; ; attempt++ {
		round := observeMajorityRound(ctx, rec, withSockets(t, cluster), baseline)
		var own []majoritySample
		for _, s := range round.Samples {
			if s.Name == survivorMON.Name {
				own = append(own, s)
			}
		}
		if len(own) != 1 || own[0].Verdict != majorityService {
			t.Fatalf("the surviving MON is not back in service: %s", round.summary())
		}
		problems, undecided := checkMajorityContinuity(baseline, majorityRound{At: round.At, Samples: own})
		if len(problems) > 0 {
			t.Fatalf("the surviving MON did not continue across the outage: %v", problems)
		}
		if len(undecided) == 0 {
			break
		}
		if attempt == 3 {
			t.Fatalf("surviving MON continuity stayed undecided: %v", undecided)
		}
	}
	outageToken(t, ctx, cluster, client, twoDomainPool, run, outagePhaseRecovered)
	waitOps(func(tally map[string]int, _ []outageOp) bool { return tally[outagePhaseRecovered+"/acked"] >= 2 }, "acknowledged writes after recovery", 4*time.Minute)
	outageToken(t, ctx, cluster, client, twoDomainPool, run, outagePhaseStop)
	select {
	case outcome := <-actor.done:
		finished = true
		if outcome.err != nil || outcome.result.ExitCode != 0 {
			t.Fatalf("the retained actor did not end cleanly: err=%v exit=%d stderr=%s", outcome.err, outcome.result.ExitCode, outcome.result.Stderr)
		}
	case <-time.After(3 * time.Minute):
		t.Fatal("the retained actor did not end after the stop token")
	}

	// Readback: a fresh connection, and bytes derived by the controller.
	events := outageLedger(t, ctx, cluster, client, twoDomainPool, run)
	ops, err = outageOps(events, run)
	if err != nil {
		t.Fatal("final ledger refused:", err)
	}
	if !slices.ContainsFunc(events, func(e outageEvent) bool { return e.Event == "stopped" }) {
		t.Fatal("the actor never reported its end")
	}
	outcomes, err = twoDomainLedgerOutcomes(ops)
	if err != nil {
		t.Fatal(err)
	}
	var objects map[string]outageObject
	if err := json.Unmarshal(outageRun(t, ctx, cluster, client, twoDomainPool, run, "read", outageNames(ops)), &objects); err != nil {
		t.Fatal(err)
	}
	unconfirmed, err := outageVerify(ops, objects)
	if err != nil {
		t.Fatal(err)
	}
	tally := outageTally(ops)
	if tally[outagePhaseBaseline+"/acked"] < 3 || tally[outagePhaseRecovered+"/acked"] < 2 || outcomes.Failed < 1 {
		t.Fatalf("tally %v outcomes %+v", tally, outcomes)
	}
	acknowledged := 0
	for _, op := range ops {
		if op.Outcome() == "acked" {
			acknowledged++
		}
	}
	tallyJSON, _ := json.Marshal(tally)
	outcomesJSON, _ = json.Marshal(outcomes)
	t.Logf("TWO_DOMAIN_OUTAGE_LEDGER run=%s operations=%d tally=%s outage_outcomes=%s unconfirmed_present=%d ops=%s", run, len(ops), tallyJSON, outcomesJSON, unconfirmed, outageShort(ops))
	t.Logf("TWO_DOMAIN_OUTAGE_READBACK acknowledged_exact=%d unconfirmed_present=%d", acknowledged, unconfirmed)
}

func twoDomainJSON(t *testing.T, ctx context.Context, cluster *ceph.Container, target any, args ...string) {
	t.Helper()
	data, err := cluster.Ceph(ctx, append(args, "--format", "json")...)
	if err != nil || json.Unmarshal(data, target) != nil {
		t.Fatalf("native observation %v: %v: %s", args, err, data)
	}
}
