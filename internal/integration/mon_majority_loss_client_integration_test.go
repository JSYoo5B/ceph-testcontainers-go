//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_mon_majority_loss_client))))

//ci: timeout=25m job-timeout=35 image-roles=essential,client

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
)

const monLossClientPool = "tc-mon-loss-client"

// monLossCallsShort lists calls as kind:seq phase>phase_after outcome ms.
func monLossCallsShort(calls []monLossCall) string {
	var b strings.Builder
	for _, call := range calls {
		fmt.Fprintf(&b, "%s:%s %s>%s %s %dms; ", call.Kind, call.Op[strings.LastIndex(call.Op, "-")+1:], call.Phase, call.PhaseAfter, call.Detail(), call.MS)
	}
	return strings.TrimSpace(b.String())
}

// TestMonMajorityLossClientCalls records what an established librados client
// and a new librados connection do while a five-MON cluster has no majority,
// and what the data looks like after the majority is back. The outage is the
// one that TestMonMajorityLossLocalOracle establishes from the MONs' own local
// mon_status: three of five MONs lose their public endpoint, one after
// another, without a restart. The client receipts are separate from that
// oracle and never decide whether a majority exists.
//
// A retained client opens its session and ioctx before the first interruption
// and keeps them. It repeats one read of a seeded object and one write of a
// unique object, each a separate call with its own intent and return record,
// each bounded by librados timeouts, none replayed. Fresh probes (a new process
// and librados instance) are run before the interruption, at 3/5 and during
// the loss; each records its MON connection, ioctx open and OSD read as three
// stages, and the connection result is recorded as soon as connect() returns.
// What the calls do during the loss is observed and recorded, not required:
// an established session needs no MON for an OSD op, and a fresh connection
// may or may not be served by a MON that has no quorum.
//
// After recovery the same retained client must work again, a fresh probe
// must succeed, and every acknowledged write must read back byte for byte.
// A write that timed out or has no return is an unknown outcome: it is never
// replayed, and the readback may find it absent or exact without turning it
// into an acknowledgement.
//
// Not covered: ticket expiry (the cephx service ticket TTL is far longer than
// this outage, and no expiry is observed), longer losses, OSD reconnects or
// restarts, a loss that begins during a client's connection attempt, other
// pool types, other client libraries, and the client's selected MON address.
func TestMonMajorityLossClientCalls(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 22*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t, ceph.WithMonitorCount(5))
	monitors := clusteraccess.Monitors(t, cluster)
	if len(monitors) != 5 {
		t.Fatal("fixture did not construct five monitors")
	}
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: monLossClientPool, PGNum: 1, Application: "rados", Replicas: 2, MinSize: 1}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	rec := newMonReplacementRecorder("majority-loss-client", withSockets(t, cluster), monReplacementBounds{
		Operation: 4 * time.Minute, Quorum: 90 * time.Second, QuorumInterval: 500 * time.Millisecond,
		Query: 8 * time.Second, Snapshot: 30 * time.Second, Bundle: 3 * time.Minute,
	})
	baseline, err := captureMajorityBaseline(ctx, rec, withSockets(t, cluster), 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("MON_LOSS_CLIENT_BASELINE ceph=%s fsid=%s members=%v monmap_epoch=%d election_epoch=%d", staticFacts(cluster).CephVersion, baseline.FSID, baseline.Members, baseline.MonMapEpoch, baseline.ElectionEpoch)
	for _, name := range baseline.Members {
		m := baseline.Mons[name]
		t.Logf("MON_LOSS_CLIENT_BASELINE mon.%s container=%.12s started=%s address=%s uptime_ms=%d", name, m.ContainerID, m.StartedAt, m.Address, m.UptimeMS)
	}
	platform := strings.TrimSpace(string(topologyExecOutput(t, ctx, client, "uname", "-sm")))
	t.Logf("MON_LOSS_CLIENT_RUNTIME platform=%s essential_image=%s client_image=%s", platform, imageID(t, ctx, cluster.Container), imageID(t, ctx, client))
	// The ticket policy is recorded, not changed. Expiry is neither forced nor
	// observed: the outage is far shorter than the service ticket's lifetime.
	ttl, ttlErr := cluster.Ceph(ctx, "config", "get", "mon", "auth_service_ticket_ttl")
	t.Logf("MON_LOSS_CLIENT_POLICY auth_service_ticket_ttl=%q error=%v ticket_expiry_observed=false client_mount_timeout=30 rados_mon_op_timeout=15 rados_osd_op_timeout=15", strings.TrimSpace(string(ttl)), ttlErr)
	var placement struct {
		Epoch  *int   `json:"epoch"`
		PGID   string `json:"pgid"`
		Acting *[]int `json:"acting"`
	}
	mapOut, mapErr := cluster.Ceph(ctx, "osd", "map", monLossClientPool, monLossSeed, "--format", "json")
	if mapErr != nil || json.Unmarshal(mapOut, &placement) != nil || placement.Epoch == nil || placement.Acting == nil || len(*placement.Acting) != 2 {
		t.Fatalf("seed placement is not a two-OSD acting set: %v %s", mapErr, mapOut)
	}
	t.Logf("MON_LOSS_CLIENT_PLACEMENT pool=%s pg=%s osdmap_epoch=%d acting=%v", monLossClientPool, placement.PGID, *placement.Epoch, *placement.Acting)

	run := "m" + strconv.FormatInt(time.Now().Unix(), 10)
	runner := monLossRunner{cluster: cluster, client: client, pool: monLossClientPool, run: run}
	var cuts []*fault.NetworkInterruption
	finished := false
	var actor *nativeCall
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 4*time.Minute)
		defer done()
		// Stop the owned retained client first, so that the receipt below holds
		// its last records, then emit the receipt before anything else can
		// fail, then restore every acquired interruption.
		if actor != nil && !finished {
			actor.cancel()
			select {
			case outcome := <-actor.done:
				t.Logf("MON_LOSS_CLIENT_ACTOR cleanup error=%v", outcome.err)
			case <-time.After(30 * time.Second):
				t.Error("the retained client could not be confirmed stopped")
			}
		}
		if ledger, raw, err := runner.ledger(cleanup); err != nil {
			t.Errorf("final receipt: the ledger is unavailable or refused: %v (%d bytes)", err, len(raw))
		} else {
			t.Logf("MON_LOSS_CLIENT_RECEIPT tokens=%v retained_instance=%d calls=%d tally=[%s] probes=%v", ledger.Tokens, ledger.Instance, len(ledger.Calls), monLossTallyString(ledger.Tally()), ledger.Order)
			for _, name := range ledger.Order {
				t.Logf("MON_LOSS_CLIENT_RECEIPT probe %s: %s", name, ledger.Probes[name].Verdict())
			}
		}
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
			t.Logf("MON_LOSS_CLIENT_CUT mon.%s endpoint=%s", name, link.OriginalAddress)
			return
		}
		t.Fatalf("no owned monitor %s", name)
	}
	wait := func(step string, want []string, w majorityWait) []majorityRound {
		t.Helper()
		rounds, err := waitMajorityState(ctx, rec, withSockets(t, cluster), baseline, want, w, func(format string, args ...any) {
			t.Logf("MON_LOSS_CLIENT_"+step+" "+format, args...)
		})
		if err != nil {
			t.Fatal(step, err)
		}
		return rounds
	}
	token := func(value string) {
		t.Helper()
		if err := runner.token(ctx, value); err != nil {
			t.Fatal(err)
		}
		t.Logf("MON_LOSS_CLIENT_TOKEN %s at %s", value, time.Now().UTC().Format("15:04:05.000"))
	}
	ledgerNow := func() *monLossLedger {
		t.Helper()
		ledger, _, err := runner.ledger(ctx)
		if err != nil {
			t.Fatal("ledger refused:", err)
		}
		return ledger
	}
	// waitCalls polls the ledger until check accepts it. A retained client that
	// ended early is a failure, not a pending call.
	waitCalls := func(what string, bound time.Duration, check func(*monLossLedger) bool) (*monLossLedger, bool) {
		t.Helper()
		select {
		case outcome := <-actor.done:
			finished = true
			t.Fatalf("the retained client ended before %s: err=%v exit=%d stdout=%s stderr=%s", what, outcome.err, outcome.result.ExitCode, outcome.result.Stdout, outcome.result.Stderr)
		default:
		}
		ledger, ok, err := monLossPoll(ctx, bound, 2*time.Second, func(c context.Context) (*monLossLedger, error) {
			select {
			case outcome := <-actor.done:
				finished = true
				return nil, fmt.Errorf("the retained client ended while waiting for %s: err=%v exit=%d stdout=%s stderr=%s", what, outcome.err, outcome.result.ExitCode, outcome.result.Stdout, outcome.result.Stderr)
			default:
			}
			l, _, err := runner.ledger(c)
			return l, err
		}, check)
		if finished {
			t.Fatal(err)
		}
		if !ok && ledger == nil {
			t.Fatalf("no usable ledger within %v while waiting for %s: %v", bound, what, err)
		}
		if !ok && err != nil {
			t.Logf("MON_LOSS_CLIENT_WAIT %s: last ledger read failed: %v", what, err)
		}
		return ledger, ok
	}
	steady := func(phase string) func(*monLossLedger) bool {
		return func(l *monLossLedger) bool {
			return l.StayedAcked("read", phase) >= 2 && l.StayedAcked("write", phase) >= 2
		}
	}
	probe := func(name string, wantOK bool) *monLossProbe {
		t.Helper()
		started := time.Now()
		if err := runner.fresh(ctx, name, 75*time.Second); err != nil {
			t.Fatal(err)
		}
		p := ledgerNow().Probes[name]
		if p == nil || !p.Ended || !p.Reaped {
			t.Fatalf("probe %s was not confirmed ended and reaped: %+v", name, p)
		}
		t.Logf("MON_LOSS_CLIENT_PROBE %s %s duration=%v", name, p.Verdict(), time.Since(started).Round(time.Millisecond))
		if wantOK && !p.AllOK() {
			t.Fatalf("control probe %s did not connect, open the pool and read: %s", name, p.Verdict())
		}
		return p
	}

	// Baseline: seed exact bytes, a fresh positive control, then the retained client.
	token(monLossPhaseBaseline)
	out, err := runner.do(ctx, "seed", 3*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var seeded struct {
		SHA256 string `json:"sha256"`
		Len    int    `json:"len"`
	}
	if err := json.Unmarshal(out, &seeded); err != nil || seeded.SHA256 != monLossSeedDigest || seeded.Len != monLossSeedLen() {
		t.Fatalf("seed readback is not the independently derived bytes: %v %s", err, out)
	}
	baselineProbe := probe("fresh-baseline", true)
	actor = startNative(ctx, cluster, client, monLossCommand(monLossClientPool, run, "actor", 25*time.Minute))
	if _, ok := waitCalls("baseline calls", 3*time.Minute, steady(monLossPhaseBaseline)); !ok {
		t.Fatal("the retained client did not complete baseline reads and writes")
	}
	session := ledgerNow()
	t.Logf("MON_LOSS_CLIENT_SESSION run=%s retained_instance=%d fresh_baseline_instance=%d retained_pid=%d", run, session.Instance, baselineProbe.Stage("connect").Instance, session.PID)
	if session.Instance == baselineProbe.Stage("connect").Instance {
		t.Fatal("the fresh probe and the retained client share a librados identity")
	}

	// Control: two MONs are isolated and the other three keep the quorum.
	majority := baseline.Members[2:]
	quorumWait := majorityWait{Window: 4 * time.Second, Interval: 2 * time.Second, Bound: 2 * time.Minute, MinRounds: 2}
	outageWait := majorityWait{Window: 20 * time.Second, Interval: 2 * time.Second, Bound: 2 * time.Minute, MinRounds: 5}
	token(monLossPhaseApplying)
	cut(baseline.Members[0])
	cut(baseline.Members[1])
	wait("MINORITY_CUT", majority, quorumWait)
	token(monLossPhaseMinority)
	if _, ok := waitCalls("calls at 3/5", 3*time.Minute, steady(monLossPhaseMinority)); !ok {
		t.Fatal("the retained client did not complete reads and writes at 3/5")
	}
	probe("fresh-minority", true)

	// The third MON is isolated alone. The outage token follows the local
	// oracle's stable window, so a call that began under it started in a lost
	// majority, and one that returned under it stayed within the window.
	token(monLossPhaseApplying)
	cut(baseline.Members[2])
	rounds := wait("MAJORITY_LOSS", nil, outageWait)
	token(monLossPhaseOutage)
	windowStart := time.Now()
	management := make(chan majorityManagement, 1)
	go func() { management <- probeMajorityManagement(ctx, withSockets(t, cluster), 30*time.Second) }()
	outageProbe := probe("fresh-outage", false)
	inWindow, settled := waitCalls("retained calls during the loss", 60*time.Second, func(l *monLossLedger) bool {
		returned := func(kind string) int {
			n := 0
			for _, call := range l.CallsUnder(kind, monLossPhaseOutage) {
				if call.Returned {
					n++
				}
			}
			return n
		}
		return returned("read") >= 2 && returned("write") >= 2
	})
	mgmt := <-management
	t.Logf("MON_LOSS_CLIENT_MANAGEMENT quorum_query_duration=%v quorum=%v error=%q (evidence only)", mgmt.Duration.Round(time.Millisecond), mgmt.Quorum, mgmt.Error)
	// The outage must have held for the whole window, by the local oracle.
	if after := observeMajorityRound(ctx, rec, withSockets(t, cluster), baseline); len(evaluateMajorityRound(baseline, after, nil)) > 0 {
		t.Fatalf("the outage did not persist through the client calls: %s", after.summary())
	}
	windowEnd := time.Now()
	for _, mon := range monitors {
		if !mon.IsRunning() {
			t.Fatalf("mon.%s stopped during the network cut", mon.DaemonName)
		}
	}
	token(monLossPhaseRestoring)
	t.Logf("MON_LOSS_CLIENT_LOSS rounds=%d window=%v settled=%t fresh=[%s] retained_tally=[%s]", len(rounds), windowEnd.Sub(windowStart).Round(time.Millisecond), settled, outageProbe.Verdict(), monLossTallyString(inWindow.Tally()))
	for _, kind := range []string{"read", "write"} {
		calls := inWindow.CallsUnder(kind, monLossPhaseOutage)
		t.Logf("MON_LOSS_CLIENT_LOSS retained %s calls begun under the outage token: %s", kind, monLossCallsShort(calls))
		if len(calls) == 0 {
			t.Fatalf("the retained client attempted no %s during the verified loss", kind)
		}
	}
	if stage := outageProbe.Stage("connect"); stage == nil {
		t.Fatal("the fresh probe never attempted a connection during the loss")
	}

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
		last = observeMajorityRound(ctx, rec, withSockets(t, cluster), baseline)
		if reasons := evaluateMajorityRound(baseline, last, baseline.Members); len(reasons) > 0 {
			t.Fatalf("the recovered quorum changed while continuity was re-observed: %v", reasons)
		}
	}
	status, err := cluster.QuorumStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(sortedClone(status.QuorumNames), baseline.Members) || status.MonMap.FSID != baseline.FSID {
		t.Fatalf("control quorum after recovery: %v fsid %s", status.QuorumNames, status.MonMap.FSID)
	}
	if problems := checkMajorityAddresses(baseline, status); len(problems) > 0 {
		t.Fatalf("monmap members or addresses changed: %v", problems)
	}
	t.Logf("MON_LOSS_CLIENT_RECOVERED quorum=%v election_epoch=%d monmap_epoch=%d uptime_continuity=ok addresses=unchanged", status.QuorumNames, last.Samples[0].ElectionEpoch, last.Samples[0].MonMapEpoch)

	// Recovery of the clients: the same retained session, and a new connection.
	token(monLossPhaseRecovered)
	if _, ok := waitCalls("calls after recovery", 3*time.Minute, steady(monLossPhaseRecovered)); !ok {
		t.Fatalf("the retained client did not complete reads and writes after recovery: %s", monLossTallyString(ledgerNow().Tally()))
	}
	probe("fresh-recovered", true)
	token(monLossPhaseStop)
	select {
	case outcome := <-actor.done:
		finished = true
		if outcome.err != nil || outcome.result.ExitCode != 0 {
			t.Fatalf("the retained client did not end cleanly: err=%v exit=%d stderr=%s", outcome.err, outcome.result.ExitCode, outcome.result.Stderr)
		}
	case <-time.After(3 * time.Minute):
		t.Fatal("the retained client did not end after the stop token")
	}

	// Readback with a fresh connection, against bytes derived by the controller.
	ledger := ledgerNow()
	if !ledger.Stopped || ledger.Instance != session.Instance {
		t.Fatalf("the retained client's end or identity changed: stopped=%t instance=%d, was %d", ledger.Stopped, ledger.Instance, session.Instance)
	}
	var names []string
	for _, op := range ledger.Writes() {
		names = append(names, op.Object)
	}
	readback, err := runner.readback(ctx, names)
	if err != nil {
		t.Fatal(err)
	}
	unconfirmed, err := monLossVerifyReadback(ledger, readback)
	if err != nil {
		t.Fatal(err)
	}
	acked := 0
	for _, op := range ledger.Writes() {
		if op.Outcome() == "acked" {
			acked++
		}
	}
	t.Logf("MON_LOSS_CLIENT_LEDGER run=%s retained_instance=%d reads=%d writes=%d tally=[%s]", run, ledger.Instance, len(ledger.Calls)-len(ledger.Writes()), len(ledger.Writes()), monLossTallyString(ledger.Tally()))
	t.Logf("MON_LOSS_CLIENT_READBACK acknowledged_writes_exact=%d unconfirmed_writes=%d unconfirmed_present=%d seed_exact=true listing=%d objects, no object outside the intents", acked, len(ledger.Writes())-acked, unconfirmed, len(readback.Listing))
}
