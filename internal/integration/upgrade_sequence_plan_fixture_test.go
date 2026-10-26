//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"strings"
	"testing"
)

// These are Docker-free controls of the upgrade sequence planner. They use
// synthetic observations and executed no Ceph or Docker, so they say nothing
// about a native transition.

func seqPlan() []upgradeDaemonID {
	var plan []upgradeDaemonID
	for _, id := range []string{"a", "b", "c"} {
		plan = append(plan, ledgerDaemon("mon", id))
	}
	for _, id := range []string{"x", "y"} {
		plan = append(plan, ledgerDaemon("mgr", id))
	}
	for _, id := range []string{"0", "1", "2"} {
		plan = append(plan, ledgerDaemon("osd", id))
	}
	return plan
}

func goodFacts(t *testing.T) upgradeClusterFacts {
	t.Helper()
	base, err := upgradeCheckpointAfter(seqPlan(), upgradePair, 0)
	if err != nil {
		t.Fatal(err)
	}
	inv := upgradeInventory{}
	for r, c := range base {
		inv[r] = c
	}
	return upgradeClusterFacts{
		SourceImage: "sha256:src", DestImage: "sha256:dst", Baseline: inv,
		PoolSize: intFact(3), PoolMinSize: intFact(2),
		AutoscaleOff: upgradeYes, NooutOwned: upgradeYes, HealthSettled: upgradeYes,
	}
}

func newSeq(t *testing.T, facts upgradeClusterFacts) *upgradeSequence {
	t.Helper()
	s, err := newUpgradeSequence(newTestLedger(t), upgradePair, seqPlan(), facts)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// settledFor builds good evidence for the n-th (1-based) planned transition.
func settledFor(t *testing.T, n int) upgradeSettled {
	t.Helper()
	plan := seqPlan()
	cp, err := upgradeCheckpointAfter(plan, upgradePair, n)
	if err != nil {
		t.Fatal(err)
	}
	inv := upgradeInventory{}
	for r, c := range cp {
		inv[r] = c
	}
	d := plan[n-1]
	ev := upgradeSettled{Daemon: d.key(), Inventory: inv, PGsSettled: upgradeYes}
	switch d.Role {
	case "mon":
		ev.Quorum = []string{"a", "b", "c"}
		if n == 3 {
			ev.MinMonRelease = intFact(20)
		}
	case "mgr":
		ev.MgrActive, ev.MgrStandbys = "x", []string{"y"}
	case "osd":
		ev.OSDsUpIn = intFact(3)
	}
	return ev
}

// driveTo runs the planned transitions through the ledger up to and including n.
func driveTo(t *testing.T, s *upgradeSequence, n int) {
	t.Helper()
	for i := s.began + 1; i <= n; i++ {
		id, d, err := s.admit()
		if err != nil {
			t.Fatal(err)
		}
		ledgerOK(t, s.ledger.fence(id, d))
		ledgerOK(t, s.ledger.oldAbsent(id, false, nil))
		ledgerOK(t, s.ledger.startAttempted(id, "dst-"+id))
		ledgerOK(t, s.ledger.confirm(id, upgradeNativeEvidence{Instance: "dst-" + id, Version: "20.2.4", FSID: d.FSID, StoreID: d.StoreID}))
		ledgerOK(t, s.settle(settledFor(t, i)))
	}
}

func wantErr(t *testing.T, err error, sub string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), sub) {
		t.Fatalf("error = %v, want it to contain %q", err, sub)
	}
}

func TestUpgradeSequenceRunsTheWholePlanInOrder(t *testing.T) {
	s := newSeq(t, goodFacts(t))
	driveTo(t, s, 8)
	if _, err := s.requireOSDRelease("20.2.4"); err == nil {
		t.Fatal("release requirement accepted before finalization")
	}
	ledgerOK(t, s.finalize())
	name, err := s.requireOSDRelease("20.2.4")
	if err != nil || name != "tentacle" {
		t.Fatalf("requireOSDRelease = %q, %v", name, err)
	}
	if _, err := s.requireOSDRelease("20.2.4"); err == nil {
		t.Fatal("release requirement issued twice")
	}
	if _, _, err := s.admit(); err == nil {
		t.Fatal("admit succeeded after finalization")
	}
}

func TestUpgradeSequencePreflightJudgement(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*upgradeClusterFacts)
		want   upgradePreflight
		reason string
	}{
		{"good", func(*upgradeClusterFacts) {}, upgradePreflightPassed, ""},
		{"no source image", func(f *upgradeClusterFacts) { f.SourceImage = "" }, upgradePreflightUnknown, "image identity"},
		{"no dest image", func(f *upgradeClusterFacts) { f.DestImage = "" }, upgradePreflightUnknown, "image identity"},
		{"same image", func(f *upgradeClusterFacts) { f.DestImage = f.SourceImage }, upgradePreflightFailed, "same"},
		{"no baseline", func(f *upgradeClusterFacts) { f.Baseline = nil }, upgradePreflightUnknown, "baseline inventory"},
		{"baseline already mixed", func(f *upgradeClusterFacts) {
			f.Baseline = upgradeInventory{"mon": {"19.2.5": 2, "20.2.4": 1}, "mgr": {"19.2.5": 2}, "osd": {"19.2.5": 3}}
		}, upgradePreflightFailed, "baseline"},
		{"baseline missing role", func(f *upgradeClusterFacts) { f.Baseline = upgradeInventory{"mon": {"19.2.5": 3}} }, upgradePreflightFailed, "baseline"},
		{"pool unobserved", func(f *upgradeClusterFacts) { f.PoolSize = nil }, upgradePreflightUnknown, "pool size"},
		{"pool min unobserved", func(f *upgradeClusterFacts) { f.PoolMinSize = nil }, upgradePreflightUnknown, "pool size"},
		{"pool 2/1", func(f *upgradeClusterFacts) { f.PoolSize, f.PoolMinSize = intFact(2), intFact(1) }, upgradePreflightFailed, "2/1"},
		{"autoscale unknown", func(f *upgradeClusterFacts) { f.AutoscaleOff = upgradeUnknown }, upgradePreflightUnknown, "autoscale"},
		{"autoscale on", func(f *upgradeClusterFacts) { f.AutoscaleOff = upgradeNo }, upgradePreflightFailed, "autoscale"},
		{"noout unknown", func(f *upgradeClusterFacts) { f.NooutOwned = upgradeUnknown }, upgradePreflightUnknown, "noout"},
		{"noout not owned", func(f *upgradeClusterFacts) { f.NooutOwned = upgradeNo }, upgradePreflightFailed, "noout"},
		{"health unknown", func(f *upgradeClusterFacts) { f.HealthSettled = upgradeUnknown }, upgradePreflightUnknown, "health"},
		{"health unsettled", func(f *upgradeClusterFacts) { f.HealthSettled = upgradeNo }, upgradePreflightFailed, "health"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := goodFacts(t)
			tc.mutate(&f)
			got, err := f.judge(seqPlan(), upgradePair)
			if got != tc.want {
				t.Fatalf("judge = %v (%v), want %v", got, err, tc.want)
			}
			if tc.reason == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			wantErr(t, err, tc.reason)
			// A refused preflight stops admission before any ledger state.
			s := newSeq(t, f)
			_, _, err = s.admit()
			wantErr(t, err, "preflight")
			if len(s.ledger.transitions) != 0 {
				t.Fatal("a refused preflight left a transition")
			}
		})
	}
}

func TestUpgradeSequenceRejectsUnapprovedPairAndWrongPlans(t *testing.T) {
	l := newTestLedger(t)
	other := upgradeReleasePair{Source: "19.2.5", Dest: "20.2.3"}
	s, err := newUpgradeSequence(l, other, seqPlan(), goodFacts(t))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.admit()
	wantErr(t, err, "not approved")

	f := goodFacts(t)
	unnamed := upgradeReleasePair{Source: "19.2.5", Dest: "99.1.0"}
	_, err = newUpgradeSequence(l, unnamed, seqPlan(), f)
	wantErr(t, err, "no known release name")

	mon, mgr, osd := ledgerDaemon("mon", "a"), ledgerDaemon("mgr", "x"), ledgerDaemon("osd", "0")
	for name, tc := range map[string]struct {
		plan []upgradeDaemonID
		want string
	}{
		"empty":      {nil, "no daemon"},
		"mgr first":  {[]upgradeDaemonID{mgr, mon, osd}, "out of the MON"},
		"osd before": {[]upgradeDaemonID{mon, osd, mgr}, "out of the MON"},
		"no mon":     {[]upgradeDaemonID{mgr, osd}, "does not start with a MON"},
		"duplicate":  {[]upgradeDaemonID{mon, mon}, "planned twice"},
		"mds":        {[]upgradeDaemonID{mon, ledgerDaemon("mds", "f")}, "outside the core"},
		"missing ID": {[]upgradeDaemonID{{Role: "mon"}}, "without an ID"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := newUpgradeSequence(newTestLedger(t), upgradePair, tc.plan, f)
			wantErr(t, err, tc.want)
		})
	}
}

func TestUpgradeSequenceAdmitsOneDaemonAtATime(t *testing.T) {
	s := newSeq(t, goodFacts(t))
	id, _, err := s.admit()
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.admit()
	wantErr(t, err, "not settled")
	// Confirmed but not settled still blocks the next daemon.
	d := seqPlan()[0]
	ledgerOK(t, s.ledger.fence(id, d))
	ledgerOK(t, s.ledger.oldAbsent(id, false, nil))
	ledgerOK(t, s.ledger.startAttempted(id, "dst-1"))
	_, _, err = s.admit()
	wantErr(t, err, "not settled")
	wantErr(t, s.settle(settledFor(t, 1)), "not native-confirmed")
	ledgerOK(t, s.ledger.confirm(id, upgradeNativeEvidence{Instance: "dst-1", Version: "20.2.4", FSID: d.FSID, StoreID: d.StoreID}))
	ledgerOK(t, s.settle(settledFor(t, 1)))
	if _, _, err := s.admit(); err != nil {
		t.Fatal(err)
	}
}

func TestUpgradeSequenceSettledEvidenceGuards(t *testing.T) {
	cases := []struct {
		name   string
		n      int
		mutate func(*upgradeSettled)
		want   string
	}{
		{"observation error", 1, func(e *upgradeSettled) { e.ObservationErr = errProbe }, "observation failed"},
		{"wrong daemon", 1, func(e *upgradeSettled) { e.Daemon = "mon.b" }, "describes"},
		{"no inventory", 1, func(e *upgradeSettled) { e.Inventory = nil }, "no version inventory"},
		{"stale inventory", 1, func(e *upgradeSettled) {
			e.Inventory = settledFor(t, 0+1).Inventory
			e.Inventory["mon"] = map[string]int{"19.2.5": 3}
		}, "differs from the checkpoint"},
		{"ahead inventory", 1, func(e *upgradeSettled) { e.Inventory = settledFor(t, 2).Inventory }, "differs from the checkpoint"},
		{"pgs unknown", 1, func(e *upgradeSettled) { e.PGsSettled = upgradeUnknown }, "PGs"},
		{"pgs unsettled", 1, func(e *upgradeSettled) { e.PGsSettled = upgradeNo }, "PGs"},
		{"no quorum", 1, func(e *upgradeSettled) { e.Quorum = nil }, "quorum"},
		{"partial quorum", 1, func(e *upgradeSettled) { e.Quorum = []string{"a", "b"} }, "quorum"},
		{"foreign quorum member", 1, func(e *upgradeSettled) { e.Quorum = []string{"a", "b", "z"} }, "quorum"},
		{"min_mon_release unobserved", 3, func(e *upgradeSettled) { e.MinMonRelease = nil }, "not observed"},
		{"min_mon_release old", 3, func(e *upgradeSettled) { e.MinMonRelease = intFact(19) }, "min_mon_release 19"},
		{"no active mgr", 4, func(e *upgradeSettled) { e.MgrActive = "" }, "MGR"},
		{"missing standby", 4, func(e *upgradeSettled) { e.MgrStandbys = nil }, "MGR"},
		{"active also standby", 4, func(e *upgradeSettled) { e.MgrStandbys = []string{"x"} }, "MGR"},
		{"osds unobserved", 6, func(e *upgradeSettled) { e.OSDsUpIn = nil }, "OSDs"},
		{"osd missing", 6, func(e *upgradeSettled) { e.OSDsUpIn = intFact(2) }, "OSDs"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newSeq(t, goodFacts(t))
			driveTo(t, s, tc.n-1)
			id, d, err := s.admit()
			if err != nil {
				t.Fatal(err)
			}
			ledgerOK(t, s.ledger.fence(id, d))
			ledgerOK(t, s.ledger.oldAbsent(id, false, nil))
			ledgerOK(t, s.ledger.startAttempted(id, "dst-"+id))
			ledgerOK(t, s.ledger.confirm(id, upgradeNativeEvidence{Instance: "dst-" + id, Version: "20.2.4", FSID: d.FSID, StoreID: d.StoreID}))
			ev := settledFor(t, tc.n)
			tc.mutate(&ev)
			wantErr(t, s.settle(ev), tc.want)
			if _, _, err := s.admit(); err == nil {
				t.Fatal("rejected evidence still admitted the next daemon")
			}
			// Good evidence afterwards is still accepted: rejection changed nothing.
			ledgerOK(t, s.settle(settledFor(t, tc.n)))
		})
	}
}

type probeErr struct{}

func (probeErr) Error() string { return "probe unavailable" }

var errProbe error = probeErr{}

func TestUpgradeSequenceHaltStopsAdmissionAndKeepsOwnership(t *testing.T) {
	s := newSeq(t, goodFacts(t))
	driveTo(t, s, 1)
	id, d, err := s.admit()
	if err != nil {
		t.Fatal(err)
	}
	ledgerOK(t, s.ledger.fence(id, d))
	ledgerOK(t, s.ledger.oldAbsent(id, false, nil))
	ledgerOK(t, s.ledger.startAttempted(id, "dst-"+id))
	s.halt(errProbe)
	s.halt(nil)
	_, _, err = s.admit()
	wantErr(t, err, "halted")
	wantErr(t, s.settle(settledFor(t, 2)), "halted")
	wantErr(t, s.finalize(), "halted")
	if got := s.ledger.owned(); len(got) != 1 || got[0] != "dst-"+id {
		t.Fatalf("owned = %v, want the attempted instance", got)
	}
}

func TestUpgradeSequenceFinalizeGuards(t *testing.T) {
	s := newSeq(t, goodFacts(t))
	wantErr(t, s.finalize(), "0 of 8")
	driveTo(t, s, 7)
	wantErr(t, s.finalize(), "7 of 8")
	if _, err := s.requireOSDRelease("20.2.4"); err == nil {
		t.Fatal("release requirement accepted before finalization")
	}
	driveTo(t, s, 8)
	ledgerOK(t, s.finalize())
	wantErr(t, s.finalize(), "already finalized")
	_, err := s.requireOSDRelease("19.2.5")
	wantErr(t, err, "destination is")
}
