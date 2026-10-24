//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// twoDomainFakeManifest builds a manifest of three logical hosts with one MON,
// one OSD and, on the first two hosts, one MGR each.
func twoDomainFakeManifest(t *testing.T, log *[]string) outageManifest {
	t.Helper()
	hosts := []string{"host-a", "host-b", "host-c"}
	var members []outageMember
	owned := map[string]bool{}
	add := func(role, name, host string) {
		d := &outageFakeDaemon{name: role + "." + name, log: log, running: true}
		id := "container-" + role + "-" + name
		owned[id] = true
		members = append(members, outageMember{Role: role, Name: name, Host: host, Container: id, daemon: d, current: func() string { return id }})
	}
	// Deliberately not in stop order.
	add("mon", "a", "host-a")
	add("mon", "b", "host-b")
	add("mon", "c", "host-c")
	add("mgr", "x", "host-a")
	add("mgr", "y", "host-b")
	add("osd", "0", "host-a")
	add("osd", "1", "host-b")
	add("osd", "2", "host-c")
	manifest, err := newOutageManifest(hosts, members, owned)
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func twoDomainNames(members []outageMember) string {
	names := make([]string, 0, len(members))
	for _, m := range members {
		names = append(names, m.Role+"."+m.Name)
	}
	return strings.Join(names, ",")
}

func TestTwoDomainPlanStopsMonsLastAndRestoresThePrimaryFirst(t *testing.T) {
	var log []string
	manifest := twoDomainFakeManifest(t, &log)
	plan, err := newTwoDomainPlan(manifest, []string{"host-a", "host-b"}, "a")
	if err != nil {
		t.Fatal(err)
	}
	if got := twoDomainNames(plan.Members); got != "osd.0,mgr.x,osd.1,mgr.y,mon.b,mon.a" {
		t.Fatalf("stop order %s", got)
	}
	if !slices.Equal(plan.LostMONs, []string{"a", "b"}) || !slices.Equal(plan.SurvivingMONs, []string{"c"}) {
		t.Fatalf("lost %v surviving %v", plan.LostMONs, plan.SurvivingMONs)
	}
	for _, m := range plan.Members {
		if m.Host == "host-c" {
			t.Fatalf("the surviving host is in the plan: %+v", m)
		}
	}
	clock := new(time.Duration)
	run := newOutagePlan(func() time.Duration { *clock += 10 * time.Millisecond; return *clock }, plan.Members)
	if err := run.Apply(t.Context(), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := run.Restore(t.Context(), "primary"); err != nil {
		t.Fatal(err)
	}
	// The primary MON is the first container to start again, before any
	// other container could be handed its released address.
	want := "stop osd.0,stop mgr.x,stop osd.1,stop mgr.y,stop mon.b,stop mon.a,start mon.a,start mon.b,start mgr.y,start osd.1,start mgr.x,start osd.0"
	if got := strings.Join(log, ","); got != want {
		t.Fatalf("calls %s", got)
	}
}

// The order of the lost hosts does not decide where the primary MON goes, and
// an even number of MONs loses its quorum when half of them stop.
func TestTwoDomainPlanOrderAndEvenMonCount(t *testing.T) {
	var log []string
	manifest := twoDomainFakeManifest(t, &log)
	plan, err := newTwoDomainPlan(manifest, []string{"host-b", "host-a"}, "a")
	if err != nil {
		t.Fatal(err)
	}
	if got := twoDomainNames(plan.Members); got != "osd.1,mgr.y,osd.0,mgr.x,mon.b,mon.a" {
		t.Fatalf("stop order %s", got)
	}
	// A fourth MON outside the lost hosts: two of four is not a majority.
	id := "container-mon-d"
	four := manifest
	four.Members = append(slices.Clone(manifest.Members), outageMember{Role: "mon", Name: "d", Host: "host-c", Container: id,
		daemon: &outageFakeDaemon{name: "mon.d", log: &log, running: true}, current: func() string { return id }})
	if plan, err := newTwoDomainPlan(four, []string{"host-a", "host-b"}, "a"); err != nil || !slices.Equal(plan.SurvivingMONs, []string{"c", "d"}) {
		t.Fatalf("two of four MONs lost: %+v %v", plan, err)
	}
}

func TestTwoDomainPlanRefusals(t *testing.T) {
	var log []string
	manifest := twoDomainFakeManifest(t, &log)
	for name, tc := range map[string]struct {
		lost    []string
		primary string
		want    string
	}{
		"one host keeps the quorum":       {[]string{"host-a"}, "a", "do not cross the quorum boundary"},
		"unknown host":                    {[]string{"host-a", "host-z"}, "a", "not in the manifest"},
		"repeated host":                   {[]string{"host-a", "host-a"}, "a", "listed twice"},
		"no survivor to observe":          {[]string{"host-a", "host-b", "host-c"}, "a", "no MON stays outside"},
		"the primary MON is not stopped":  {[]string{"host-b", "host-c"}, "a", "primary MON"},
		"empty selection":                 {nil, "a", "do not cross the quorum boundary"},
		"the primary is not a MON at all": {[]string{"host-a", "host-b"}, "0", "primary MON"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := newTwoDomainPlan(manifest, tc.lost, tc.primary); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want %q", err, tc.want)
			}
		})
	}
	// Two lost hosts that hold only one of three MONs keep the quorum.
	mixed := manifest
	mixed.Members = slices.Clone(manifest.Members)
	for i, m := range mixed.Members {
		if m.Role == "mon" && m.Name == "b" {
			mixed.Members[i].Host = "host-c"
		}
	}
	if _, err := newTwoDomainPlan(mixed, []string{"host-a", "host-b"}, "a"); err == nil || !strings.Contains(err.Error(), "keeps a quorum") {
		t.Fatalf("a plan that loses one of three MONs was accepted: %v", err)
	}
}

func survivorSample(name, verdict string) majoritySample {
	return majoritySample{Name: name, Verdict: verdict, Reason: "scripted", UptimeMS: -1}
}

func TestTwoDomainSurvivorRound(t *testing.T) {
	round := func(samples ...majoritySample) majorityRound { return majorityRound{At: time.Now(), Samples: samples} }
	survivors, stopped := []string{"c"}, []string{"a", "b"}
	good := round(survivorSample("a", majorityInconclusive), survivorSample("b", majorityInconclusive), survivorSample("c", majorityOutage))
	if reasons := evaluateSurvivorRound(good, survivors, stopped); len(reasons) != 0 {
		t.Fatalf("a stable lost majority was refused: %v", reasons)
	}
	for name, tc := range map[string]struct {
		round majorityRound
		want  string
	}{
		"survivor in service":           {round(survivorSample("a", majorityInconclusive), survivorSample("b", majorityInconclusive), survivorSample("c", majorityService)), "surviving mon.c"},
		"survivor inconclusive":         {round(survivorSample("a", majorityInconclusive), survivorSample("b", majorityInconclusive), survivorSample("c", majorityInconclusive)), "surviving mon.c"},
		"survivor missing":              {round(survivorSample("a", majorityInconclusive), survivorSample("b", majorityInconclusive)), "no sample for mon.c"},
		"stopped MON claims service":    {round(survivorSample("a", majorityService), survivorSample("b", majorityInconclusive), survivorSample("c", majorityOutage)), "stopped mon.a reports service"},
		"stopped MON missing":           {round(survivorSample("a", majorityInconclusive), survivorSample("c", majorityOutage)), "no sample for mon.b"},
		"unexpected member":             {round(survivorSample("a", majorityInconclusive), survivorSample("b", majorityInconclusive), survivorSample("c", majorityOutage), survivorSample("d", majorityOutage)), "unexpected sample for mon.d"},
		"empty round":                   {round(), "no sample for mon.c"},
		"stopped MON alone is no proof": {round(survivorSample("a", majorityInconclusive), survivorSample("b", majorityInconclusive)), "no sample for mon.c"},
	} {
		t.Run(name, func(t *testing.T) {
			reasons := evaluateSurvivorRound(tc.round, survivors, stopped)
			if len(reasons) == 0 || !strings.Contains(strings.Join(reasons, "; "), tc.want) {
				t.Fatalf("reasons %v, want %q", reasons, tc.want)
			}
		})
	}
}

func TestTwoDomainWaitNeedsAStableWindow(t *testing.T) {
	w := majorityWait{Window: 30 * time.Millisecond, Interval: 5 * time.Millisecond, Bound: 500 * time.Millisecond, MinRounds: 3}
	clock := func(n int) majorityRound {
		return majorityRound{At: time.Now(), Samples: []majoritySample{survivorSample("c", map[bool]string{true: majorityOutage, false: majorityService}[n%2 == 1])}}
	}
	evaluate := func(r majorityRound) []string { return evaluateSurvivorRound(r, []string{"c"}, nil) }

	// An outage that flaps never reaches three consecutive rounds.
	n := 0
	if _, err := waitStableRounds(w, func() majorityRound { n++; return clock(n) }, evaluate); err == nil {
		t.Fatal("a flapping state was accepted")
	}
	// A state that settles after a few contrary rounds is accepted, and the
	// contrary rounds are part of the returned history.
	n = 0
	rounds, err := waitStableRounds(w, func() majorityRound {
		n++
		return clock(map[bool]int{true: 2, false: 1}[n <= 3])
	}, evaluate)
	if err != nil || len(rounds) < 6 {
		t.Fatalf("%d rounds, error %v", len(rounds), err)
	}
	// The window must be spanned, not only the round count.
	w.Window = 10 * time.Second
	w.Bound = 100 * time.Millisecond
	if _, err := waitStableRounds(w, func() majorityRound { return clock(1) }, evaluate); err == nil {
		t.Fatal("a window that was never spanned was accepted")
	}
}

func twoDomainOp(seq int, phase, after string, returned bool, errText string) outageOp {
	return outageOp{Op: fmt.Sprintf("r-%05d", seq), Object: fmt.Sprintf("op-r-%05d", seq), Seq: seq, Phase: phase, PhaseAfter: after, Returned: returned, Error: errText}
}

func TestTwoDomainLedgerOutcomes(t *testing.T) {
	ops := []outageOp{
		twoDomainOp(0, outagePhaseBaseline, outagePhaseBaseline, true, ""),
		twoDomainOp(1, outagePhaseApplying, outagePhaseOutage, true, ""), // overlapped the injection
		twoDomainOp(2, outagePhaseApplying, outagePhaseOutage, true, "TimedOut('timed out')"),
		twoDomainOp(3, outagePhaseOutage, outagePhaseOutage, true, "TimedOut('timed out')"),
		twoDomainOp(4, outagePhaseOutage, outagePhaseOutage, false, ""), // no return record
		twoDomainOp(5, outagePhaseRecovered, outagePhaseRecovered, true, ""),
	}
	out, err := twoDomainLedgerOutcomes(ops)
	if err != nil {
		t.Fatal(err)
	}
	if want := (twoDomainOutcomes{Begun: 2, Failed: 1, Inflight: 1, OverlapAcked: 1}); out != want {
		t.Fatalf("outcomes %+v, want %+v", out, want)
	}

	// An acknowledgement that returned under the outage token is an error,
	// but is still counted.
	broken := append(slices.Clone(ops), twoDomainOp(6, outagePhaseOutage, outagePhaseOutage, true, ""))
	out, err = twoDomainLedgerOutcomes(broken)
	if err == nil || !strings.Contains(err.Error(), "r-00006") || out.Acked != 1 {
		t.Fatalf("acknowledged outage write: %+v %v", out, err)
	}
	// One that returned only after the restoring token was issued was pending
	// when the restoration began: reported, not refused, not credited.
	for _, after := range []string{outagePhaseRestoring, outagePhaseRecovered} {
		late := append(slices.Clone(ops), twoDomainOp(6, outagePhaseOutage, after, true, ""))
		out, err := twoDomainLedgerOutcomes(late)
		if err != nil || out.LateAcked != 1 || out.Acked != 0 || out.Begun != 3 {
			t.Fatalf("late acknowledgement under %q: %+v %v", after, out, err)
		}
	}
	// No operation began under the outage token: nothing to attribute yet.
	out, err = twoDomainLedgerOutcomes(ops[:3])
	if err != nil || out.Begun != 0 {
		t.Fatalf("%+v %v", out, err)
	}
}

// TestTwoDomainLedgerWithTheRealActor drives the real actor script, whose
// store acknowledges every write, under the outage token. That is exactly what
// a replica set below min_size must never do, so the outcomes check refuses it,
// and a hanging operation is counted as an unknown outcome, not a success.
func TestTwoDomainLedgerWithTheRealActor(t *testing.T) {
	acked := newOutageFake(t)
	acked.token(outagePhaseOutage)
	_, done := acked.start()
	acked.waitFor("acknowledgements", acknowledged("outage/acked", 2))
	acked.token(outagePhaseStop)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := twoDomainLedgerOutcomes(acked.ops()); err == nil {
		t.Fatal("acknowledged writes under the outage token were accepted")
	}

	hung := newOutageFake(t, "FAKE_SEQ=1", "FAKE_MODE=before")
	hung.token(outagePhaseOutage)
	_, done = hung.start()
	hung.waitFor("a failed operation", func(ops []outageOp) bool { return len(ops) >= 2 && ops[1].Returned })
	hung.token(outagePhaseStop)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	ops := hung.ops()
	out, err := twoDomainLedgerOutcomes(ops[1:2])
	if err != nil || out.Failed != 1 || out.Acked != 0 {
		t.Fatalf("a failed operation under the outage: %+v %v", out, err)
	}
	if unconfirmed, err := outageVerify(ops, hung.readback(ops)); err != nil || unconfirmed != 0 {
		t.Fatalf("readback: %d %v", unconfirmed, err)
	}
}

// TestTwoDomainPendingWriteAcknowledgedAfterRestoration drives the real actor
// script through the transition the first native run showed: one call fails,
// the next begins under the outage token and stays pending, the restoring
// token is issued, and only then does the pending call succeed. It began under
// the outage token but is not wholly observed under it, so it is reported as
// late-acknowledged, must still be exact in the readback, and is not refused.
func TestTwoDomainPendingWriteAcknowledgedAfterRestoration(t *testing.T) {
	fake := newOutageFake(t, "FAKE_MODE=gated")
	gated := strings.Replace(outageFakeRados, `if seq == FAIL and MODE == "hang": time.sleep(120)`,
		`if MODE == "gated" and seq == 0: raise RuntimeError("timed out")
        if MODE == "gated" and seq == 1:
            while not os.path.exists(os.path.join(STORE, "gate")): time.sleep(.02)`, 1)
	if gated == outageFakeRados {
		t.Fatal("the stand-in has no hang hook to replace")
	}
	if err := os.WriteFile(filepath.Join(fake.dir, "rados.py"), []byte(gated), 0o600); err != nil {
		t.Fatal(err)
	}
	fake.token(outagePhaseOutage)
	_, done := fake.start()
	fake.waitFor("a failed call and a pending one", func(ops []outageOp) bool {
		return len(ops) == 2 && ops[0].Outcome() == "failed" && ops[1].Outcome() == "inflight"
	})
	fake.token(outagePhaseRestoring)
	if err := os.WriteFile(filepath.Join(fake.dir, "store", "gate"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	fake.waitFor("the pending call to return", func(ops []outageOp) bool { return len(ops) >= 2 && ops[1].Returned })
	fake.token(outagePhaseStop)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	ops := fake.ops()
	out, err := twoDomainLedgerOutcomes(ops)
	if err != nil {
		t.Fatalf("a pending write acknowledged after the restoration began was refused: %v", err)
	}
	if out.LateAcked != 1 || out.Failed != 1 || out.Acked != 0 || ops[1].PhaseAfter != outagePhaseRestoring {
		t.Fatalf("outcomes %+v, second operation %+v", out, ops[1])
	}
	if got := outageTally(ops)["outage-overlapping-acked"]; got != 1 {
		t.Fatalf("the shared credit classes disagree: %v", outageTally(ops))
	}
	if unconfirmed, err := outageVerify(ops, fake.readback(ops)); err != nil || unconfirmed != 0 {
		t.Fatalf("readback: %d %v", unconfirmed, err)
	}
}
