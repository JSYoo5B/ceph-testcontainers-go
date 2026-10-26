//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"strings"
	"testing"
)

const (
	clockLedOff = int64(30e9)
	clockLedSec = int64(1e9)
)

var clockLedTarget = clockOffsetTarget{ID: "target-a", PID: 4242, StartTicks: 9001}

func clockLedIntPtr(v int) *int { return &v }

func clockLedFile(text string) clockObservedFile {
	return clockObservedFile{KnownPresence: true, Present: true, KnownContent: true, Text: text,
		Identity: clockFileIdentity{HasIdentity: true, Device: 7, Inode: 11}}
}

func clockLedMeasured(center int64) clockOffsetInterval {
	return clockOffsetInterval{Known: true, Lo: center - 40e6, Hi: center + 40e6}
}

func clockLedStep(label string, off int64, ack int64) clockOffsetStep {
	text, _ := clockOffsetText(off)
	return clockOffsetStep{
		Label: label, IntendedNs: off, Write: clockWriteAcked, AckKnown: true, AckedMono: ack,
		File: clockLedFile(text), Target: clockLedTarget, Measured: clockLedMeasured(off),
		SampleKnown: true, SampleLoMono: ack + 12*clockLedSec, TargetSampleOK: true,
	}
}

func clockLedgerOK() clockOffsetLedger {
	zero := "+0"
	owned := clockOwnedFile{Path: cfgSelected, ExpectedText: zero,
		Identity: clockFileIdentity{HasIdentity: true, Device: 7, Inode: 11}}
	restore := clockLedStep("restore", 0, 300*clockLedSec)
	return clockOffsetLedger{
		Target: clockLedTarget, Owned: owned,
		Env:      clockTargetEnvironment{Known: true, TimestampFile: strPtr(cfgSelected)},
		Initial:  clockLedFile("+0"),
		CacheSec: clockLedIntPtr(10), Baseline: clockLedMeasured(0), BaselineTarget: clockLedTarget,
		Steps:   []clockOffsetStep{clockLedStep("plus", clockLedOff, 100*clockLedSec), clockLedStep("minus", -clockLedOff, 200*clockLedSec)},
		Restore: &restore,
		Cleanup: clockOffsetCleanup{Process: clockProcessExited, ExitKnown: true, ExitMono: 400 * clockLedSec,
			File: clockFileRemoved, RemovedKnown: true, RemovedMono: 401 * clockLedSec},
	}
}

func wantClockLedger(t *testing.T, name string, j clockLedgerJudgement, restore clockRestoreState, step0 clockStepState, firstFailure string) {
	t.Helper()
	if j.Restore != restore {
		t.Errorf("%s: restore %s (%s), want %s", name, j.Restore, j.RestoreReason, restore)
	}
	if step0 != "" && (len(j.Steps) == 0 || j.Steps[0].State != step0) {
		t.Errorf("%s: step0 %+v, want %s", name, j.Steps, step0)
	}
	if (firstFailure == "") != (j.FirstFailure == "") || !strings.Contains(j.FirstFailure, firstFailure) {
		t.Errorf("%s: first failure %q, want containing %q", name, j.FirstFailure, firstFailure)
	}
}

func TestClockOffsetLedgerHappyPath(t *testing.T) {
	j := judgeClockOffsetLedger(clockLedgerOK())
	wantClockLedger(t, "ok", j, clockRestoreVerified, clockStepAdmitted, "")
	if j.Steps[1].State != clockStepAdmitted || j.CleanupVerdict != clockProvenanceValid {
		t.Fatalf("unexpected: %s", j)
	}
}

func TestClockOffsetLedgerAckIsNotAdmission(t *testing.T) {
	cases := map[string]func(*clockOffsetStep){
		"no sample":        func(s *clockOffsetStep) { s.SampleKnown = false },
		"not from target":  func(s *clockOffsetStep) { s.TargetSampleOK = false },
		"unknown interval": func(s *clockOffsetStep) { s.Measured = clockOffsetInterval{} },
	}
	for n, mut := range cases {
		l := clockLedgerOK()
		mut(&l.Steps[0])
		j := judgeClockOffsetLedger(l)
		wantClockLedger(t, n, j, clockRestoreVerified, clockStepAckOnly, "step plus")
	}
}

func TestClockOffsetLedgerStepControls(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*clockOffsetStep)
		want clockStepState
	}{
		{"write failed", func(s *clockOffsetStep) { s.Write = clockWriteFailed }, clockStepUnknown},
		{"write canceled", func(s *clockOffsetStep) { s.Write = clockWriteCanceled }, clockStepUnknown},
		{"write status missing", func(s *clockOffsetStep) { s.Write = "" }, clockStepUnknown},
		{"ack time missing", func(s *clockOffsetStep) { s.AckKnown = false }, clockStepInconclusive},
		{"fractional second", func(s *clockOffsetStep) { s.IntendedNs = clockLedOff + 5 }, clockStepInconclusive},
		{"file text drift", func(s *clockOffsetStep) { s.File = clockLedFile("+31") }, clockStepRejected},
		{"file removed", func(s *clockOffsetStep) { s.File = clockObservedFile{KnownPresence: true} }, clockStepRejected},
		{"file replaced", func(s *clockOffsetStep) { s.File.Identity.Inode = 12 }, clockStepRejected},
		{"file unread", func(s *clockOffsetStep) { s.File.KnownContent = false }, clockStepInconclusive},
		{"file presence unknown", func(s *clockOffsetStep) { s.File = clockObservedFile{} }, clockStepInconclusive},
		{"replacement process", func(s *clockOffsetStep) { s.Target.StartTicks++ }, clockStepRejected},
		{"incomplete target", func(s *clockOffsetStep) { s.Target = clockOffsetTarget{} }, clockStepRejected},
		{"sample before ack", func(s *clockOffsetStep) { s.SampleLoMono = s.AckedMono - 1 }, clockStepInconclusive},
		{"interval spans zero", func(s *clockOffsetStep) {
			s.Measured = clockOffsetInterval{Known: true, Lo: -clockLedSec, Hi: clockLedOff + clockLedSec}
		}, clockStepInconclusive},
		{"inside cache window", func(s *clockOffsetStep) {
			s.Measured = clockLedMeasured(0)
			s.SampleLoMono = s.AckedMono + 5*clockLedSec
		}, clockStepPending},
		{"stale after window", func(s *clockOffsetStep) {
			s.Measured = clockLedMeasured(0)
			s.SampleLoMono = s.AckedMono + 12*clockLedSec
		}, clockStepRejected},
		{"wrong sign", func(s *clockOffsetStep) { s.Measured = clockLedMeasured(-clockLedOff) }, clockStepRejected},
	}
	for _, c := range cases {
		l := clockLedgerOK()
		c.mut(&l.Steps[0])
		j := judgeClockOffsetLedger(l)
		if j.Steps[0].State != c.want {
			t.Errorf("%s: %s (%s), want %s", c.name, j.Steps[0].State, j.Steps[0].Reason, c.want)
		}
		if j.FirstFailure == "" {
			t.Errorf("%s: non-admitted step left no first failure", c.name)
		}
	}
}

func TestClockOffsetLedgerCacheWindowBoundary(t *testing.T) {
	// The pinned comparison is a whole-second difference greater than the
	// duration, so the guaranteed refetch is duration+1 seconds after the ack.
	l := clockLedgerOK()
	s := &l.Steps[0]
	s.Measured = clockLedMeasured(0)
	s.SampleLoMono = s.AckedMono + 11*clockLedSec - 1
	if st, _ := judgeClockStep(l, *s); st != clockStepPending {
		t.Fatalf("just before window end: %s", st)
	}
	s.SampleLoMono = s.AckedMono + 11*clockLedSec
	if st, _ := judgeClockStep(l, *s); st != clockStepRejected {
		t.Fatalf("at window end: %s", st)
	}
	l.CacheSec = nil
	if st, _ := judgeClockStep(l, *s); st != clockStepInconclusive {
		t.Fatalf("unknown cache duration: %s", st)
	}
	l.CacheSec = clockLedIntPtr(-1)
	if st, _ := judgeClockStep(l, *s); st != clockStepInconclusive {
		t.Fatalf("negative cache duration: %s", st)
	}
}

func TestClockOffsetLedgerAdmissionControls(t *testing.T) {
	cases := map[string]func(*clockOffsetLedger){
		"FAKETIME present":        func(l *clockOffsetLedger) { l.Env.Faketime = strPtr("+0") },
		"FAKETIME empty":          func(l *clockOffsetLedger) { l.Env.Faketime = strPtr("") },
		"env unknown":             func(l *clockOffsetLedger) { l.Env.Known = false },
		"file not at zero":        func(l *clockOffsetLedger) { l.Owned.ExpectedText = "+5" },
		"no identity":             func(l *clockOffsetLedger) { l.Owned.Identity = clockFileIdentity{} },
		"baseline other instance": func(l *clockOffsetLedger) { l.BaselineTarget.PID++ },
		"baseline unknown":        func(l *clockOffsetLedger) { l.Baseline = clockOffsetInterval{} },
		"baseline not zero":       func(l *clockOffsetLedger) { l.Baseline = clockLedMeasured(clockLedOff) },
		"target incomplete":       func(l *clockOffsetLedger) { l.Target = clockOffsetTarget{ID: "x"}; l.BaselineTarget = l.Target },
	}
	for n, mut := range cases {
		l := clockLedgerOK()
		mut(&l)
		j := judgeClockOffsetLedger(l)
		if j.Admission.Verdict == clockProvenanceValid {
			t.Errorf("%s: admitted", n)
		}
		for _, s := range j.Steps {
			if s.State == clockStepAdmitted {
				t.Errorf("%s: step %s admitted without admission", n, s.Label)
			}
		}
		if j.Restore == clockRestoreVerified {
			t.Errorf("%s: restore verified without admission", n)
		}
		if j.FirstFailure == "" || len(j.Steps) != 2 {
			t.Errorf("%s: receipt incomplete: %s", n, j)
		}
	}
}

func TestClockOffsetLedgerRestoreControls(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*clockOffsetStep)
		want clockRestoreState
	}{
		{"write failed", func(s *clockOffsetStep) { s.Write = clockWriteFailed }, clockRestoreUnknown},
		{"ack time missing", func(s *clockOffsetStep) { s.AckKnown = false }, clockRestoreUnknown},
		{"not a zero restore", func(s *clockOffsetStep) { s.IntendedNs = clockLedSec }, clockRestoreUnknown},
		{"ack only", func(s *clockOffsetStep) { s.SampleKnown = false }, clockRestoreAckOnly},
		{"deleted is not restore", func(s *clockOffsetStep) { s.File = clockObservedFile{KnownPresence: true} }, clockRestoreNotRestored},
		{"file replaced", func(s *clockOffsetStep) { s.File.Identity.Device = 8 }, clockRestoreNotRestored},
		{"file still offset", func(s *clockOffsetStep) { s.File = clockLedFile("-30") }, clockRestoreNotRestored},
		{"file unread", func(s *clockOffsetStep) { s.File.KnownContent = false }, clockRestoreUnknown},
		{"replacement process", func(s *clockOffsetStep) { s.Target.PID++ }, clockRestoreNotRestored},
		{"sample before ack", func(s *clockOffsetStep) { s.SampleLoMono = s.AckedMono - 1 }, clockRestoreUnknown},
		{"still shifted late", func(s *clockOffsetStep) { s.Measured = clockLedMeasured(-clockLedOff) }, clockRestoreNotRestored},
		{"still shifted early", func(s *clockOffsetStep) {
			s.Measured = clockLedMeasured(-clockLedOff)
			s.SampleLoMono = s.AckedMono + 3*clockLedSec
		}, clockRestoreUnknown},
		{"interval spans offset", func(s *clockOffsetStep) {
			s.Measured = clockOffsetInterval{Known: true, Lo: -clockLedOff - clockLedSec, Hi: clockLedSec}
		}, clockRestoreUnknown},
	}
	for _, c := range cases {
		l := clockLedgerOK()
		c.mut(l.Restore)
		j := judgeClockOffsetLedger(l)
		if j.Restore != c.want {
			t.Errorf("%s: %s (%s), want %s", c.name, j.Restore, j.RestoreReason, c.want)
		}
		if !strings.Contains(j.FirstFailure, "restore") {
			t.Errorf("%s: first failure %q does not name restore", c.name, j.FirstFailure)
		}
	}
}

func TestClockOffsetLedgerDeletionLooksLikeZero(t *testing.T) {
	// After unlinking, a fallback source may yield a measurement inside the
	// reference interval. That must not be reported as a restoration.
	l := clockLedgerOK()
	l.Restore.File = clockObservedFile{KnownPresence: true}
	l.Restore.Measured = clockLedMeasured(0)
	j := judgeClockOffsetLedger(l)
	if j.Restore != clockRestoreNotRestored {
		t.Fatalf("deletion counted as restore: %s", j)
	}
}

func TestClockOffsetLedgerNoRestoreAttempt(t *testing.T) {
	l := clockLedgerOK()
	l.Restore = nil
	j := judgeClockOffsetLedger(l)
	if j.Restore != clockRestoreNotAttempted || j.FirstFailure != "" {
		t.Fatalf("%s", j)
	}
	// A disposed target is valid cleanup but never reported as restored.
	if j.CleanupVerdict != clockProvenanceValid {
		t.Fatalf("%s", j)
	}
}

func TestClockOffsetLedgerCleanupOwnership(t *testing.T) {
	exited := clockOffsetCleanup{Process: clockProcessExited, ExitKnown: true, ExitMono: 10, File: clockFileRemoved, RemovedKnown: true, RemovedMono: 11}
	cases := []struct {
		name     string
		c        clockOffsetCleanup
		restored bool
		want     clockProvenanceVerdict
	}{
		{"exit then remove", exited, false, clockProvenanceValid},
		{"remove before exit", func() clockOffsetCleanup { c := exited; c.RemovedMono = 9; return c }(), false, clockProvenanceInvalid},
		{"file left behind", clockOffsetCleanup{Process: clockProcessExited, ExitKnown: true, File: clockFilePresent}, false, clockProvenanceInvalid},
		{"exit time missing", func() clockOffsetCleanup { c := exited; c.ExitKnown = false; return c }(), false, clockProvenanceInconclusive},
		{"removal time missing", func() clockOffsetCleanup { c := exited; c.RemovedKnown = false; return c }(), false, clockProvenanceInconclusive},
		{"live target unlinked", clockOffsetCleanup{Process: clockProcessRunning, File: clockFileRemoved}, true, clockProvenanceInvalid},
		{"live target restored", clockOffsetCleanup{Process: clockProcessRunning, File: clockFilePresent}, true, clockProvenanceValid},
		{"live target unrestored", clockOffsetCleanup{Process: clockProcessRunning, File: clockFilePresent}, false, clockProvenanceInvalid},
		{"nothing recorded", clockOffsetCleanup{}, true, clockProvenanceInconclusive},
		{"process unrecorded", clockOffsetCleanup{File: clockFileRemoved}, true, clockProvenanceInconclusive},
	}
	for _, c := range cases {
		if v, r := judgeClockCleanup(c.c, c.restored); v != c.want {
			t.Errorf("%s: %s (%s), want %s", c.name, v, r, c.want)
		}
	}
}

func TestClockOffsetLedgerKeepsFirstFailureAndCleanupSeparate(t *testing.T) {
	l := clockLedgerOK()
	l.Steps[0].Write = clockWriteFailed
	l.Steps[1].File = clockObservedFile{KnownPresence: true}
	l.Cleanup = clockOffsetCleanup{Process: clockProcessRunning, File: clockFileRemoved, Errors: []string{"remove failed"}}
	j := judgeClockOffsetLedger(l)
	if !strings.HasPrefix(j.FirstFailure, "step plus unknown_outcome") {
		t.Fatalf("first failure replaced: %q", j.FirstFailure)
	}
	if j.CleanupVerdict != clockProvenanceInvalid || len(j.CleanupErrors) != 1 {
		t.Fatalf("cleanup merged into scenario: %s", j)
	}
	if len(j.Steps) != 2 {
		t.Fatalf("partial ledger not emitted in full: %s", j)
	}
	// The judgement owns its error list.
	l.Cleanup.Errors[0] = "changed"
	if j.CleanupErrors[0] != "remove failed" {
		t.Fatalf("judgement aliases caller memory")
	}
	if strings.Contains(j.String(), "+30") {
		t.Fatalf("receipt carries file text: %s", j)
	}
}

func TestClockOffsetLedgerFleet(t *testing.T) {
	a, b := clockLedgerOK(), clockLedgerOK()
	b.Target = clockOffsetTarget{ID: "target-b", PID: 77, StartTicks: 5}
	b.BaselineTarget = b.Target
	for i := range b.Steps {
		b.Steps[i].Target = b.Target
	}
	b.Restore.Target = b.Target
	b.Owned.Path = "owned-b.txt"
	b.Env.TimestampFile = strPtr("owned-b.txt")

	res := judgeClockFleet([]clockFleetEntry{{"a", &a}, {"b", &b}})
	for n, j := range res {
		if j.Restore != clockRestoreVerified || j.FirstFailure != "" {
			t.Fatalf("%s: %s", n, j)
		}
	}

	// Restore evidence of one target never counts for another.
	b2 := b
	r := *b.Restore
	r.Target = a.Target
	b2.Restore = &r
	res = judgeClockFleet([]clockFleetEntry{{"a", &a}, {"b", &b2}})
	if res["b"].Restore != clockRestoreNotRestored || res["a"].Restore != clockRestoreVerified {
		t.Fatalf("cross-target restore counted: %s / %s", res["a"], res["b"])
	}

	// A shared owned path or instance invalidates both ledgers.
	c := b
	c.Owned.Path = a.Owned.Path
	c.Env.TimestampFile = strPtr(a.Owned.Path)
	res = judgeClockFleet([]clockFleetEntry{{"a", &a}, {"c", &c}})
	for n, j := range res {
		if j.Admission.Verdict != clockProvenanceInvalid || j.Restore == clockRestoreVerified {
			t.Fatalf("%s shares a path: %s", n, j)
		}
	}
	d := b
	d.Target = a.Target
	res = judgeClockFleet([]clockFleetEntry{{"a", &a}, {"d", &d}})
	if res["a"].Admission.Verdict != clockProvenanceInvalid || res["d"].Admission.Verdict != clockProvenanceInvalid {
		t.Fatalf("shared instance not rejected")
	}

	// A typed-nil ledger is inconclusive, not read.
	res = judgeClockFleet([]clockFleetEntry{{"n", nil}})
	if res["n"].Admission.Verdict != clockProvenanceInconclusive || res["n"].Restore == clockRestoreVerified {
		t.Fatalf("nil ledger: %s", res["n"])
	}
}
