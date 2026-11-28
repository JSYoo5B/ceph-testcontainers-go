package fault

import (
	"testing"
)

func strPtr(s string) *string { return &s }

const cfgSelected = "owned-faketime.txt"

func cfgOwned() clockOwnedFile {
	return clockOwnedFile{Path: cfgSelected, Identity: clockFileIdentity{HasIdentity: true, Device: 7, Inode: 42}, ExpectedText: "+0"}
}

func cfgObserved(text string) clockObservedFile {
	return clockObservedFile{KnownPresence: true, Present: true, Identity: cfgOwned().Identity, KnownContent: true, Text: text}
}

func cfgEnv() clockTargetEnvironment {
	return clockTargetEnvironment{Known: true, TimestampFile: strPtr(cfgSelected)}
}

func wantVerdict(t *testing.T, name string, got clockProvenanceResult, want clockProvenanceVerdict) {
	t.Helper()
	if got.Verdict != want {
		t.Errorf("%s: verdict %q (%s), want %q", name, got.Verdict, got.Reason, want)
	}
}

func TestClockLiveFileAdmission(t *testing.T) {
	wantVerdict(t, "baseline", admitClockLiveFileMode(cfgEnv(), cfgOwned(), cfgObserved("+0")), clockProvenanceValid)
	wantVerdict(t, "trailing newline", admitClockLiveFileMode(cfgEnv(), cfgOwned(), cfgObserved("+0\n")), clockProvenanceValid)

	// Environment mode, including the empty value, is invalid.
	for _, v := range []string{"+0", ""} {
		env := cfgEnv()
		env.Faketime = strPtr(v)
		wantVerdict(t, "FAKETIME="+v, admitClockLiveFileMode(env, cfgOwned(), cfgObserved("+0")), clockProvenanceInvalid)
	}

	env := cfgEnv()
	env.TimestampFile = nil
	wantVerdict(t, "selector absent", admitClockLiveFileMode(env, cfgOwned(), cfgObserved("+0")), clockProvenanceInvalid)
	env.TimestampFile = strPtr("other-file.txt")
	wantVerdict(t, "selector names another file", admitClockLiveFileMode(env, cfgOwned(), cfgObserved("+0")), clockProvenanceInvalid)

	// Initial text must be exactly +0: empty, nonzero and wrong ledger fail.
	wantVerdict(t, "empty initial", admitClockLiveFileMode(cfgEnv(), cfgOwned(), cfgObserved("")), clockProvenanceInvalid)
	wantVerdict(t, "nonzero initial", admitClockLiveFileMode(cfgEnv(), cfgOwned(), cfgObserved("+30")), clockProvenanceInvalid)
	nz := cfgOwned()
	nz.ExpectedText = "+30"
	wantVerdict(t, "ledger not at zero", admitClockLiveFileMode(cfgEnv(), nz, cfgObserved("+30")), clockProvenanceInvalid)

	// Missing evidence is inconclusive and never decodes into a pass.
	wantVerdict(t, "environment unread", admitClockLiveFileMode(clockTargetEnvironment{}, cfgOwned(), cfgObserved("+0")), clockProvenanceInconclusive)
	wantVerdict(t, "ledger without identity", admitClockLiveFileMode(cfgEnv(), clockOwnedFile{Path: cfgSelected, ExpectedText: "+0"}, cfgObserved("+0")), clockProvenanceInconclusive)
	wantVerdict(t, "ledger without path", admitClockLiveFileMode(cfgEnv(), clockOwnedFile{Identity: cfgOwned().Identity, ExpectedText: "+0"}, cfgObserved("+0")), clockProvenanceInconclusive)
	wantVerdict(t, "presence unobserved", admitClockLiveFileMode(cfgEnv(), cfgOwned(), clockObservedFile{}), clockProvenanceInconclusive)
	noContent := cfgObserved("")
	noContent.KnownContent = false
	wantVerdict(t, "content unread", admitClockLiveFileMode(cfgEnv(), cfgOwned(), noContent), clockProvenanceInconclusive)
	noID := cfgObserved("+0")
	noID.Identity = clockFileIdentity{}
	wantVerdict(t, "identity unobserved", admitClockLiveFileMode(cfgEnv(), cfgOwned(), noID), clockProvenanceInconclusive)
}

func TestClockLiveFileVerification(t *testing.T) {
	wantVerdict(t, "unchanged", verifyClockLiveFileMode(cfgOwned(), cfgObserved("+0")), clockProvenanceValid)

	// A controller update moves the ledger with the file and stays valid.
	upd := cfgOwned()
	upd.ExpectedText = "+30"
	wantVerdict(t, "ledger updated", verifyClockLiveFileMode(upd, cfgObserved("+30")), clockProvenanceValid)
	// A file that did not take the update is drift, not success.
	wantVerdict(t, "update not applied", verifyClockLiveFileMode(upd, cfgObserved("+0")), clockProvenanceInvalid)

	gone := verifyClockLiveFileMode(cfgOwned(), clockObservedFile{KnownPresence: true, Present: false})
	wantVerdict(t, "unlinked", gone, clockProvenanceInvalid)
	if !gone.FallbackInEffect {
		t.Error("unlinked file must report that a non-owned source may apply")
	}
	if r := verifyClockLiveFileMode(cfgOwned(), cfgObserved("+0")); r.FallbackInEffect {
		t.Error("present owned file must not report a fallback")
	}

	swapped := cfgObserved("+0")
	swapped.Identity = clockFileIdentity{HasIdentity: true, Device: 7, Inode: 43}
	wantVerdict(t, "replaced with same text", verifyClockLiveFileMode(cfgOwned(), swapped), clockProvenanceInvalid)

	wantVerdict(t, "presence unobserved", verifyClockLiveFileMode(cfgOwned(), clockObservedFile{}), clockProvenanceInconclusive)
	noContent := cfgObserved("")
	noContent.KnownContent = false
	wantVerdict(t, "content unread", verifyClockLiveFileMode(cfgOwned(), noContent), clockProvenanceInconclusive)
	wantVerdict(t, "ledger without identity", verifyClockLiveFileMode(clockOwnedFile{Path: cfgSelected, ExpectedText: "+0"}, cfgObserved("+0")), clockProvenanceInconclusive)
}

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

func clockLedStep(off int64, ack int64) clockOffsetStep {
	text, _ := clockOffsetText(off)
	return clockOffsetStep{
		IntendedNs: off, Write: clockWriteAcked, AckKnown: true, AckedMono: ack,
		File: clockLedFile(text), Target: clockLedTarget, Measured: clockLedMeasured(off),
		SampleKnown: true, SampleLoMono: ack + 12*clockLedSec, TargetSampleOK: true,
	}
}

func clockLedgerOK() clockOffsetLedger {
	owned := clockOwnedFile{Path: cfgSelected, ExpectedText: "+0",
		Identity: clockFileIdentity{HasIdentity: true, Device: 7, Inode: 11}}
	return clockOffsetLedger{Target: clockLedTarget, Owned: owned, CacheSec: clockLedIntPtr(10), Baseline: clockLedMeasured(0)}
}

// clockLedLease is a lease that applied step, as Restore judges it.
func clockLedLease(step clockOffsetStep) *ClockOffsetLease {
	return &ClockOffsetLease{ledger: clockLedgerOK(), step: step}
}

func clockLedRestore() clockOffsetStep { return clockLedStep(0, 300*clockLedSec) }

func TestClockOffsetLedgerHappyPath(t *testing.T) {
	step := clockLedStep(clockLedOff, 100*clockLedSec)
	if st, why := judgeClockStep(clockLedgerOK(), step); st != clockStepAdmitted {
		t.Fatalf("step %s (%s)", st, why)
	}
	if st, why := clockLedLease(step).judgeRestore(clockLedRestore()); st != clockRestoreVerified {
		t.Fatalf("restore %s (%s)", st, why)
	}
}

func TestClockOffsetLedgerAckIsNotAdmission(t *testing.T) {
	cases := map[string]func(*clockOffsetStep){
		"no sample":        func(s *clockOffsetStep) { s.SampleKnown = false },
		"not from target":  func(s *clockOffsetStep) { s.TargetSampleOK = false },
		"unknown interval": func(s *clockOffsetStep) { s.Measured = clockOffsetInterval{} },
	}
	for n, mut := range cases {
		step := clockLedStep(clockLedOff, 100*clockLedSec)
		mut(&step)
		if st, why := judgeClockStep(clockLedgerOK(), step); st != clockStepAckOnly {
			t.Errorf("%s: step %s (%s), want %s", n, st, why, clockStepAckOnly)
		}
		// The restore still discriminates the reference from the intended offset.
		if st, why := clockLedLease(step).judgeRestore(clockLedRestore()); st != clockRestoreVerified {
			t.Errorf("%s: restore %s (%s), want %s", n, st, why, clockRestoreVerified)
		}
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
		step := clockLedStep(clockLedOff, 100*clockLedSec)
		c.mut(&step)
		if st, why := judgeClockStep(clockLedgerOK(), step); st != c.want {
			t.Errorf("%s: %s (%s), want %s", c.name, st, why, c.want)
		}
	}
}

func TestClockOffsetLedgerCacheWindowBoundary(t *testing.T) {
	// The pinned comparison is a whole-second difference greater than the
	// duration, so the guaranteed refetch is duration+1 seconds after the ack.
	l := clockLedgerOK()
	step := clockLedStep(clockLedOff, 100*clockLedSec)
	s := &step
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
	env := func() clockTargetEnvironment {
		return clockTargetEnvironment{Known: true, TimestampFile: strPtr(cfgSelected)}
	}
	files := map[string]func(*clockTargetEnvironment, *clockOwnedFile){
		"FAKETIME present": func(e *clockTargetEnvironment, _ *clockOwnedFile) { e.Faketime = strPtr("+0") },
		"FAKETIME empty":   func(e *clockTargetEnvironment, _ *clockOwnedFile) { e.Faketime = strPtr("") },
		"env unknown":      func(e *clockTargetEnvironment, _ *clockOwnedFile) { e.Known = false },
		"file not at zero": func(_ *clockTargetEnvironment, o *clockOwnedFile) { o.ExpectedText = "+5" },
		"no identity":      func(_ *clockTargetEnvironment, o *clockOwnedFile) { o.Identity = clockFileIdentity{} },
	}
	for n, mut := range files {
		e, owned := env(), clockLedgerOK().Owned
		mut(&e, &owned)
		if r := admitClockLiveFileMode(e, owned, clockLedFile("+0")); r.Verdict == clockProvenanceValid {
			t.Errorf("%s: admitted", n)
		}
	}
	if r := admitClockLiveFileMode(env(), clockLedgerOK().Owned, clockLedFile("+0")); r.Verdict != clockProvenanceValid {
		t.Fatalf("valid file refused: %s", r.Reason)
	}

	other := clockLedTarget
	other.PID++
	baselines := map[string]struct {
		target, measured clockOffsetTarget
		baseline         clockOffsetInterval
	}{
		"baseline other instance": {clockLedTarget, other, clockLedMeasured(0)},
		"baseline unknown":        {clockLedTarget, clockLedTarget, clockOffsetInterval{}},
		"baseline not zero":       {clockLedTarget, clockLedTarget, clockLedMeasured(clockLedOff)},
		"target incomplete":       {clockOffsetTarget{ID: "x"}, clockOffsetTarget{ID: "x"}, clockLedMeasured(0)},
	}
	for n, c := range baselines {
		if r := admitClockBaseline(c.target, c.measured, c.baseline); r.Verdict == clockProvenanceValid {
			t.Errorf("%s: admitted", n)
		}
	}
	if r := admitClockBaseline(clockLedTarget, clockLedTarget, clockLedMeasured(0)); r.Verdict != clockProvenanceValid {
		t.Fatalf("valid baseline refused: %s", r.Reason)
	}
}

func TestClockOffsetLedgerRestoreControls(t *testing.T) {
	shift := clockLedMeasured(-clockLedOff)
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
		{"in order", func(*clockOffsetStep) {}, clockRestoreVerified},
	}
	for _, c := range cases {
		restore := clockLedRestore()
		c.mut(&restore)
		if st, why := judgeClockRestore(clockLedgerOK(), restore, shift); st != c.want {
			t.Errorf("%s: %s (%s), want %s", c.name, st, why, c.want)
		}
	}
}

func TestClockOffsetLedgerDeletionLooksLikeZero(t *testing.T) {
	// After unlinking, a fallback source may yield a measurement inside the
	// reference interval. That must not be reported as a restoration.
	restore := clockLedRestore()
	restore.File = clockObservedFile{KnownPresence: true}
	restore.Measured = clockLedMeasured(0)
	if st, why := clockLedLease(clockLedStep(clockLedOff, 100*clockLedSec)).judgeRestore(restore); st != clockRestoreNotRestored {
		t.Fatalf("deletion counted as restore: %s (%s)", st, why)
	}
}

func TestClockOffsetLeaseRestoreDiscriminatesTheStep(t *testing.T) {
	// An admitted step is discriminated by its measured interval, any other
	// step by the intended offset.
	wide := clockLedStep(clockLedOff, 100*clockLedSec)
	wide.Measured = clockOffsetInterval{Known: true, Lo: clockLedSec, Hi: clockLedOff + clockLedSec}
	restore := clockLedRestore()
	restore.Measured = clockOffsetInterval{Known: true, Lo: -clockLedSec / 2, Hi: 2 * clockLedSec}
	if st, why := clockLedLease(wide).judgeRestore(restore); st != clockRestoreUnknown {
		t.Errorf("admitted step overlapping the restore: %s (%s)", st, why)
	}
	failed := clockOffsetStep{IntendedNs: clockLedOff, Write: clockWriteFailed}
	if st, why := clockLedLease(failed).judgeRestore(restore); st != clockRestoreVerified {
		t.Errorf("unacknowledged step: %s (%s)", st, why)
	}
	restore.Measured = clockOffsetInterval{Known: true, Lo: -clockLedSec, Hi: clockLedOff}
	if st, why := clockLedLease(failed).judgeRestore(restore); st != clockRestoreUnknown {
		t.Errorf("unacknowledged step inside the restore interval: %s (%s)", st, why)
	}

	// A restarted daemon is a fresh instance: +0 file and reference measurement.
	restarted := clockLedRestore()
	restarted.Target.StartTicks++
	step := clockLedStep(clockLedOff, 100*clockLedSec)
	if st, why := clockLedLease(step).judgeRestore(restarted); st != clockRestoreVerified {
		t.Errorf("restarted at the reference: %s (%s)", st, why)
	}
	restarted.Measured = clockLedMeasured(clockLedOff)
	if st, why := clockLedLease(step).judgeRestore(restarted); st != clockRestoreNotRestored {
		t.Errorf("restarted with the offset: %s (%s)", st, why)
	}
	restarted.Measured = clockLedMeasured(0)
	restarted.File = clockLedFile("+30")
	if st, why := clockLedLease(step).judgeRestore(restarted); st != clockRestoreNotRestored {
		t.Errorf("restarted with the offset file: %s (%s)", st, why)
	}
}
