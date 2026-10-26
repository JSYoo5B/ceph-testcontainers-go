//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

// These are Docker-free controls of the per-daemon upgrade transition ledger
// model. They use synthetic identities and executed no Ceph or Docker, so
// they say nothing about a native transition.

var upgradePair = upgradeReleasePair{Source: "19.2.5", Dest: "20.2.4"}

func ledgerDaemon(role, id string) upgradeDaemonID {
	return upgradeDaemonID{Role: role, ID: id, FSID: "fsid-1", StoreID: "store-" + role + id, Arch: "amd64"}
}

func ledgerBegin(tr string, d upgradeDaemonID) upgradeBegin {
	return upgradeBegin{Transition: tr, Daemon: d, Pair: upgradePair, Preflight: upgradePreflightPassed}
}

func newTestLedger(t *testing.T) *upgradeLedger {
	t.Helper()
	l, err := newUpgradeLedger("amd64", upgradePair)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func ledgerOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// walk advances a transition through the steps up to and including target.
func ledgerWalk(t *testing.T, l *upgradeLedger, tr string, d upgradeDaemonID, target upgradeStep) {
	t.Helper()
	ledgerOK(t, l.begin(ledgerBegin(tr, d)))
	if target >= upgradeFenced {
		ledgerOK(t, l.fence(tr, d))
	}
	if target >= upgradeOldAbsent {
		ledgerOK(t, l.oldAbsent(tr, false, nil))
	}
	if target >= upgradeStartAttempted {
		ledgerOK(t, l.startAttempted(tr, "dst-"+tr))
	}
	if target >= upgradeNativeConfirmed {
		ledgerOK(t, l.confirm(tr, upgradeNativeEvidence{Instance: "dst-" + tr, Version: "20.2.4", FSID: d.FSID, StoreID: d.StoreID}))
	}
}

func TestUpgradeLedgerAdvancesInOrder(t *testing.T) {
	l := newTestLedger(t)
	d := ledgerDaemon("mon", "a")
	ledgerOK(t, l.begin(ledgerBegin("t1", d)))
	for _, want := range []struct {
		act  upgradeAction
		step upgradeStep
		do   func() error
	}{
		{upgradeActFence, upgradeNotAttempted, func() error { return l.fence("t1", d) }},
		{upgradeActVerifyOldAbsent, upgradeFenced, func() error { return l.oldAbsent("t1", false, nil) }},
		{upgradeActStart, upgradeOldAbsent, func() error { return l.startAttempted("t1", "dst-t1") }},
		{upgradeActObserveAttempt, upgradeStartAttempted, func() error {
			return l.confirm("t1", upgradeNativeEvidence{Instance: "dst-t1", Version: "20.2.4", FSID: d.FSID, StoreID: d.StoreID})
		}},
		{upgradeActNone, upgradeNativeConfirmed, nil},
	} {
		if act, err := l.next("t1"); err != nil || act != want.act {
			t.Fatalf("next at %s = %v, %v; want %v", want.step, act, err, want.act)
		}
		if got, _ := l.stepOf("t1"); got != want.step {
			t.Fatalf("step = %s, want %s", got, want.step)
		}
		if want.do != nil {
			ledgerOK(t, want.do())
		}
	}
	ledgerOK(t, l.finalize("mon.a"))
}

func TestUpgradeLedgerRejectsStepSkipsAndRepeats(t *testing.T) {
	d := ledgerDaemon("mon", "a")
	ev := upgradeNativeEvidence{Instance: "dst-t1", Version: "20.2.4", FSID: d.FSID, StoreID: d.StoreID}
	steps := map[string]func(l *upgradeLedger) error{
		"fence":        func(l *upgradeLedger) error { return l.fence("t1", d) },
		"oldAbsent":    func(l *upgradeLedger) error { return l.oldAbsent("t1", false, nil) },
		"startAttempt": func(l *upgradeLedger) error { return l.startAttempted("t1", "dst-t1") },
		"confirm":      func(l *upgradeLedger) error { return l.confirm("t1", ev) },
	}
	// Each step is valid in exactly one state.
	valid := map[string]upgradeStep{"fence": upgradeNotAttempted, "oldAbsent": upgradeFenced, "startAttempt": upgradeOldAbsent, "confirm": upgradeStartAttempted}
	for name, call := range steps {
		for state := upgradeNotAttempted; state <= upgradeNativeConfirmed; state++ {
			l := newTestLedger(t)
			ledgerWalk(t, l, "t1", d, state)
			err := call(l)
			if state == valid[name] {
				if err != nil {
					t.Errorf("%s at %s: %v", name, state, err)
				}
				continue
			}
			if err == nil {
				t.Errorf("%s at %s was accepted", name, state)
			}
			if got, _ := l.stepOf("t1"); got != state {
				t.Errorf("%s at %s moved the step to %s", name, state, got)
			}
		}
	}
	if _, err := newTestLedger(t).next("missing"); err == nil {
		t.Error("next of an unknown transition succeeded")
	}
}

func TestUpgradeLedgerBeginGuards(t *testing.T) {
	d := ledgerDaemon("osd", "0")
	for name, tc := range map[string]struct {
		mutate func(*upgradeBegin)
		want   string
	}{
		"unapproved pair":   {func(b *upgradeBegin) { b.Pair = upgradeReleasePair{Source: "19.2.5", Dest: "21.0.0"} }, "not approved"},
		"reversed pair":     {func(b *upgradeBegin) { b.Pair = upgradeReleasePair{Source: "20.2.4", Dest: "19.2.5"} }, "not approved"},
		"architecture":      {func(b *upgradeBegin) { b.Daemon.Arch = "arm64" }, "architecture"},
		"missing FSID":      {func(b *upgradeBegin) { b.Daemon.FSID = "" }, "incomplete"},
		"missing store":     {func(b *upgradeBegin) { b.Daemon.StoreID = "" }, "incomplete"},
		"no transition id":  {func(b *upgradeBegin) { b.Transition = "" }, "no identity"},
		"unknown preflight": {func(b *upgradeBegin) { b.Preflight = upgradePreflightUnknown }, "preflight"},
		"failed preflight":  {func(b *upgradeBegin) { b.Preflight = upgradePreflightFailed }, "preflight"},
	} {
		t.Run(name, func(t *testing.T) {
			l := newTestLedger(t)
			req := ledgerBegin("t1", d)
			tc.mutate(&req)
			err := l.begin(req)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("begin error = %v, want %q", err, tc.want)
			}
			if _, err := l.get("t1"); err == nil {
				t.Fatal("a rejected begin left a transition")
			}
			// The same ledger still admits a correct request: nothing leaked.
			ledgerOK(t, l.begin(ledgerBegin("t1", d)))
		})
	}
	t.Run("duplicates", func(t *testing.T) {
		l := newTestLedger(t)
		ledgerOK(t, l.begin(ledgerBegin("t1", d)))
		if err := l.begin(ledgerBegin("t1", ledgerDaemon("osd", "1"))); err == nil {
			t.Error("reused transition identity was accepted")
		}
		if err := l.begin(ledgerBegin("t2", d)); err == nil {
			t.Error("second transition for one daemon was accepted")
		}
		other := ledgerDaemon("osd", "1")
		other.StoreID = d.StoreID
		if err := l.begin(ledgerBegin("t3", other)); err == nil {
			t.Error("a store owned by another daemon was accepted")
		}
	})
	t.Run("ledger construction", func(t *testing.T) {
		if _, err := newUpgradeLedger("", upgradePair); err == nil {
			t.Error("no architecture accepted")
		}
		if _, err := newUpgradeLedger("amd64"); err == nil {
			t.Error("no approved pair accepted")
		}
		if _, err := newUpgradeLedger("amd64", upgradeReleasePair{Source: "20.2.4", Dest: "20.2.4"}); err == nil {
			t.Error("same-release pair accepted")
		}
	})
}

func TestUpgradeLedgerFenceAndOldAbsentGuards(t *testing.T) {
	d := ledgerDaemon("mon", "a")
	t.Run("wrong identity at fence", func(t *testing.T) {
		for name, mutate := range map[string]func(*upgradeDaemonID){
			"fsid":  func(x *upgradeDaemonID) { x.FSID = "other" },
			"store": func(x *upgradeDaemonID) { x.StoreID = "other" },
			"id":    func(x *upgradeDaemonID) { x.ID = "b" },
		} {
			l := newTestLedger(t)
			ledgerOK(t, l.begin(ledgerBegin("t1", d)))
			wrong := d
			mutate(&wrong)
			if err := l.fence("t1", wrong); err == nil {
				t.Errorf("%s: fence of a different daemon was accepted", name)
			}
			if got, _ := l.stepOf("t1"); got != upgradeNotAttempted {
				t.Errorf("%s: step = %s", name, got)
			}
		}
	})
	t.Run("old container", func(t *testing.T) {
		l := newTestLedger(t)
		ledgerWalk(t, l, "t1", d, upgradeFenced)
		if err := l.oldAbsent("t1", true, nil); err == nil {
			t.Error("a present source container counted as absent")
		}
		if err := l.oldAbsent("t1", false, errors.New("inspect failed")); err == nil {
			t.Error("a failed observation counted as absent")
		}
		// A failed observation with present=false must not be read as absence.
		if got, _ := l.stepOf("t1"); got != upgradeFenced {
			t.Fatalf("step = %s", got)
		}
		ledgerOK(t, l.oldAbsent("t1", false, nil))
	})
}

func TestUpgradeLedgerConfirmNeedsMatchingNativeEvidence(t *testing.T) {
	d := ledgerDaemon("osd", "0")
	good := upgradeNativeEvidence{Instance: "dst-t1", Version: "20.2.4", FSID: d.FSID, StoreID: d.StoreID}
	for name, mutate := range map[string]func(*upgradeNativeEvidence){
		"other instance": func(e *upgradeNativeEvidence) { e.Instance = "dst-other" },
		"source version": func(e *upgradeNativeEvidence) { e.Version = "19.2.5" },
		"empty version":  func(e *upgradeNativeEvidence) { e.Version = "" },
		"other fsid":     func(e *upgradeNativeEvidence) { e.FSID = "other" },
		"other store":    func(e *upgradeNativeEvidence) { e.StoreID = "other" },
		"zero evidence":  func(e *upgradeNativeEvidence) { *e = upgradeNativeEvidence{} },
	} {
		t.Run(name, func(t *testing.T) {
			l := newTestLedger(t)
			ledgerWalk(t, l, "t1", d, upgradeStartAttempted)
			ev := good
			mutate(&ev)
			if err := l.confirm("t1", ev); err == nil {
				t.Fatal("mismatched evidence confirmed the attempt")
			}
			if got, _ := l.stepOf("t1"); got != upgradeStartAttempted {
				t.Fatalf("step = %s", got)
			}
			// The attempt keeps its owner and the matching evidence still works.
			if !slices.Equal(l.owned(), []string{"dst-t1"}) {
				t.Fatalf("owned = %v", l.owned())
			}
			ledgerOK(t, l.confirm("t1", good))
		})
	}
}

func TestUpgradeLedgerResumeObservesAttemptWithoutSecondWriter(t *testing.T) {
	l := newTestLedger(t)
	d := ledgerDaemon("osd", "0")
	ledgerWalk(t, l, "t1", d, upgradeStartAttempted)
	// An interrupted or failed creation call leaves start-attempted. The
	// driver is told to observe, and a second attempt is refused.
	if act, _ := l.next("t1"); act != upgradeActObserveAttempt {
		t.Fatalf("next = %v, want observe-attempt", act)
	}
	if err := l.startAttempted("t1", "dst-second"); err == nil {
		t.Fatal("a second start attempt was accepted")
	}
	if !slices.Equal(l.owned(), []string{"dst-t1"}) {
		t.Fatalf("owned = %v, want only the first attempt", l.owned())
	}
	// A confirmed transition owns nothing for disposal.
	ledgerOK(t, l.confirm("t1", upgradeNativeEvidence{Instance: "dst-t1", Version: "20.2.4", FSID: d.FSID, StoreID: d.StoreID}))
	if len(l.owned()) != 0 {
		t.Fatalf("owned after confirmation = %v", l.owned())
	}
}

func TestUpgradeLedgerFinalizeGuards(t *testing.T) {
	a, b := ledgerDaemon("mon", "a"), ledgerDaemon("mon", "b")
	t.Run("unconfirmed daemon", func(t *testing.T) {
		for state := upgradeNotAttempted; state < upgradeNativeConfirmed; state++ {
			l := newTestLedger(t)
			ledgerWalk(t, l, "t1", a, upgradeNativeConfirmed)
			ledgerWalk(t, l, "t2", b, state)
			if err := l.finalize("mon.a", "mon.b"); err == nil {
				t.Errorf("finalized with a daemon at %s", state)
			}
		}
	})
	t.Run("missing and unexpected daemons", func(t *testing.T) {
		l := newTestLedger(t)
		ledgerWalk(t, l, "t1", a, upgradeNativeConfirmed)
		if err := l.finalize("mon.a", "mon.b"); err == nil {
			t.Error("finalized without a transition for an expected daemon")
		}
		if err := l.finalize(); err == nil {
			t.Error("finalized with an empty expected set")
		}
		ledgerWalk(t, l, "t2", b, upgradeNativeConfirmed)
		if err := l.finalize("mon.a"); err == nil {
			t.Error("finalized with a transition outside the expected set")
		}
		ledgerOK(t, l.finalize("mon.a", "mon.b"))
	})
	t.Run("once, then closed", func(t *testing.T) {
		l := newTestLedger(t)
		ledgerWalk(t, l, "t1", a, upgradeNativeConfirmed)
		ledgerOK(t, l.finalize("mon.a"))
		if err := l.finalize("mon.a"); err == nil {
			t.Error("second finalization accepted")
		}
		if err := l.begin(ledgerBegin("t9", b)); err == nil {
			t.Error("begin after finalization accepted")
		}
	})
}

func TestUpgradeLedgerReservesInstanceForOneTransition(t *testing.T) {
	a, b := ledgerDaemon("mon", "a"), ledgerDaemon("mon", "b")
	for name, confirmFirst := range map[string]bool{"pending and pending": false, "confirmed and pending": true} {
		t.Run(name, func(t *testing.T) {
			l := newTestLedger(t)
			ledgerWalk(t, l, "t1", a, upgradeStartAttempted)
			if confirmFirst {
				ledgerOK(t, l.confirm("t1", upgradeNativeEvidence{Instance: "dst-t1", Version: "20.2.4", FSID: a.FSID, StoreID: a.StoreID}))
			}
			ledgerWalk(t, l, "t2", b, upgradeOldAbsent)
			if err := l.startAttempted("t2", "dst-t1"); err == nil {
				t.Fatal("a second transition reserved the same destination instance")
			}
			if got, _ := l.stepOf("t2"); got != upgradeOldAbsent {
				t.Fatalf("rejected reservation moved t2 to %s", got)
			}
			want := []string{"dst-t1"}
			if confirmFirst {
				want = nil
			}
			if !slices.Equal(l.owned(), want) {
				t.Fatalf("owned = %v, want %v", l.owned(), want)
			}
			// A distinct instance is still accepted afterwards.
			ledgerOK(t, l.startAttempted("t2", "dst-t2"))
		})
	}
}
