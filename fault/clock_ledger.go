package fault

// This file holds the verdict rules of ApplyClockOffset for the libfaketime
// v0.9.12 interposer of the clock helper
// (3ccdd344aadf1e317156fa5fb7c881f2c4404778): when a daemon reads the owned
// timestamp file as its live control channel, and how the lease's offset step
// and its restore are judged.

import (
	"fmt"
	"strings"
)

// clockOffsetInterval is a closed interval of possible offsets in nanoseconds.
// Known false keeps an unmeasured interval from reading as [0,0].
type clockOffsetInterval struct {
	Known  bool
	Lo, Hi int64
}

func (a clockOffsetInterval) overlaps(b clockOffsetInterval) bool {
	return a.Known && b.Known && a.Lo <= b.Hi && b.Lo <= a.Hi
}

// clockFileIdentity is the device and inode of the controller's owned file as
// captured at creation. HasIdentity false means the capture failed.
type clockFileIdentity struct {
	HasIdentity bool
	Device      uint64
	Inode       uint64
}

// clockOwnedFile is the controller's record of the file it created for one
// target. ExpectedText is the text the controller last wrote. Namespace names
// the mount namespace that Path resolves in, such as the container that holds
// the file and runs the target; empty is the controller's own. A path alone
// does not name a file once targets run in separate containers.
type clockOwnedFile struct {
	Namespace    string
	Path         string
	Identity     clockFileIdentity
	ExpectedText string
}

// clockTargetEnvironment is the environment of the target process itself, read
// from the target and not from the launcher's intent. Known false means it
// could not be read.
type clockTargetEnvironment struct {
	Known         bool
	Faketime      *string
	TimestampFile *string
}

// clockObservedFile is the owned path as observed now. KnownPresence false and
// KnownContent false keep an unreadable observation out of the evidence.
type clockObservedFile struct {
	KnownPresence bool
	Present       bool
	Identity      clockFileIdentity
	KnownContent  bool
	Text          string
}

type clockProvenanceVerdict string

const (
	clockProvenanceValid        clockProvenanceVerdict = "valid"
	clockProvenanceInvalid      clockProvenanceVerdict = "invalid"
	clockProvenanceInconclusive clockProvenanceVerdict = "inconclusive"
)

// clockProvenanceResult is a configuration-provenance verdict. It carries no
// claim about the clock: even a valid result at "+0" says only that the target
// reads the owned file and that file holds the text the controller wrote.
type clockProvenanceResult struct {
	Verdict clockProvenanceVerdict
	Reason  string
	// FallbackInEffect is set when the owned file is gone and the target's
	// next read would use a non-owned source.
	FallbackInEffect bool
}

func provenanceResult(v clockProvenanceVerdict, format string, args ...any) clockProvenanceResult {
	return clockProvenanceResult{Verdict: v, Reason: fmt.Sprintf(format, args...)}
}

// admitClockLiveFileMode decides whether a target may be used as a live-file
// clock fixture. The target environment must be read from the target, must not
// contain FAKETIME (even empty), must name exactly the owned file, and the
// owned file must start with exactly "+0" and a captured identity.
func admitClockLiveFileMode(env clockTargetEnvironment, owned clockOwnedFile, initial clockObservedFile) clockProvenanceResult {
	if owned.Path == "" || !owned.Identity.HasIdentity {
		return provenanceResult(clockProvenanceInconclusive, "owned file path or identity was not captured")
	}
	if !env.Known {
		return provenanceResult(clockProvenanceInconclusive, "target environment was not read")
	}
	if env.Faketime != nil {
		return provenanceResult(clockProvenanceInvalid, "FAKETIME is present in the target environment, which disables file parsing")
	}
	if env.TimestampFile == nil {
		return provenanceResult(clockProvenanceInvalid, "FAKETIME_TIMESTAMP_FILE is absent from the target environment")
	}
	if *env.TimestampFile != owned.Path {
		return provenanceResult(clockProvenanceInvalid, "FAKETIME_TIMESTAMP_FILE does not name the owned file")
	}
	if owned.ExpectedText != "+0" {
		return provenanceResult(clockProvenanceInvalid, "controller ledger does not start at +0")
	}
	if r, done := checkOwnedFileState(owned, initial); done {
		return r
	}
	return provenanceResult(clockProvenanceValid, "target reads the owned file, which holds %q", owned.ExpectedText)
}

// verifyClockLiveFileMode re-checks a previously admitted target. The expected
// text comes from the controller's ledger, so a legitimate update is not
// reported as drift. An unlinked or replaced file invalidates the fixture and
// is never described as restoration.
func verifyClockLiveFileMode(owned clockOwnedFile, now clockObservedFile) clockProvenanceResult {
	if owned.Path == "" || !owned.Identity.HasIdentity {
		return provenanceResult(clockProvenanceInconclusive, "owned file path or identity was not captured")
	}
	if r, done := checkOwnedFileState(owned, now); done {
		return r
	}
	return provenanceResult(clockProvenanceValid, "owned file is present with the ledger text %q", owned.ExpectedText)
}

// checkOwnedFileState returns a terminal result when the observation is
// missing, shows removal, replacement or drift.
func checkOwnedFileState(owned clockOwnedFile, o clockObservedFile) (clockProvenanceResult, bool) {
	if !o.KnownPresence {
		return provenanceResult(clockProvenanceInconclusive, "owned file presence was not observed"), true
	}
	if !o.Present {
		r := provenanceResult(clockProvenanceInvalid, "owned file is gone; unlinking is not restoration and the target may read a non-owned source")
		r.FallbackInEffect = true
		return r, true
	}
	if !o.Identity.HasIdentity {
		return provenanceResult(clockProvenanceInconclusive, "owned file identity was not observed"), true
	}
	if o.Identity != owned.Identity {
		return provenanceResult(clockProvenanceInvalid, "owned file was replaced (device or inode changed)"), true
	}
	if !o.KnownContent {
		return provenanceResult(clockProvenanceInconclusive, "owned file content was not read"), true
	}
	if strings.TrimSuffix(o.Text, "\n") != owned.ExpectedText {
		return provenanceResult(clockProvenanceInvalid, "owned file text %q differs from ledger text %q", o.Text, owned.ExpectedText), true
	}
	return clockProvenanceResult{}, false
}

// The offset ledger judges, without a clock, a process or Docker, what one
// clock offset lease recorded: the write and measurement of its one offset
// step and the restore of that step. The lease builds the ledger from its
// observations and acts on the verdicts; the functions only classify.
//
// Source facts the judgement follows (src/libfaketime.c at the pinned commit):
//
//   - The offset text of an owned file such as "+30" is parsed by
//     parse_ft_string as seconds (atof) unless it contains one of the m, h, d
//     or y multipliers. The ledger only writes whole seconds.
//   - The target does not read the file on every clock call. fake_clock_gettime
//     keeps a cache: it refetches only when the whole-second difference of the
//     real clock since the last fetch exceeds cache_duration (10 by default,
//     FAKETIME_CACHE_DURATION), when caching is off or when a refetch is
//     forced. A write acknowledged by the controller therefore is not an
//     applied offset. The earliest guaranteed refetch after an acknowledged
//     write is cache_duration+1 whole seconds after it, and only for a clock
//     call made by the target.
//   - An unavailable owned file falls back to a non-owned source
//     ($HOME/.faketimerc, then /etc/faketimerc), so a measurement that looks like zero
//     after the owned file was removed is not a restoration.
//
// The ledger distinguishes four facts that must not be merged:
//
//  1. the controller's write was acknowledged,
//  2. the owned file still holds the ledger text with its original identity,
//  3. the same target instance showed the offset in a bracketed measurement
//     taken after the acknowledgement,
//  4. the target returned to the reference interval, measured the same way.
//
// Fact 1 alone never admits an offset. Fact 4 requires facts 2 and 3 for the
// same target instance. Deleting the file never counts as fact 4.

// clockOffsetTarget identifies one running instance of the target process. All
// three parts are needed so that a replacement process with the same name or
// recycled PID is a different instance.
type clockOffsetTarget struct {
	ID         string
	PID        int
	StartTicks uint64
}

func (t clockOffsetTarget) complete() bool {
	return t.ID != "" && t.PID > 0 && t.StartTicks > 0
}

// clockWriteStatus is the outcome of the controller's write of one step.
// The zero value is an unknown outcome, never a success.
type clockWriteStatus string

const (
	clockWriteAcked    clockWriteStatus = "acked"
	clockWriteFailed   clockWriteStatus = "failed"
	clockWriteCanceled clockWriteStatus = "canceled"
)

// clockOffsetStep is the record of one intended offset. Mono values are
// nanoseconds on an uninjected controller monotonic clock, with the *Known
// flag keeping an unrecorded value out of the evidence.
type clockOffsetStep struct {
	// IntendedNs is the offset the controller meant to apply; whole seconds.
	IntendedNs int64
	Write      clockWriteStatus
	AckKnown   bool
	AckedMono  int64
	// File is the owned file as re-read after the write.
	File clockObservedFile
	// Target is the instance the measurement came from.
	Target clockOffsetTarget
	// Measured is the bracketed offset interval of the target and SampleLoMono
	// is the monotonic controller time at the start of that bracket.
	Measured       clockOffsetInterval
	SampleKnown    bool
	SampleLoMono   int64
	TargetSampleOK bool // the sample was taken from the target by a clock call
}

// clockOffsetText renders whole-second offsets the way the parser reads them.
func clockOffsetText(ns int64) (string, bool) {
	const sec = int64(1e9)
	if ns%sec != 0 {
		return "", false
	}
	return fmt.Sprintf("%+d", ns/sec), true
}

// clockOffsetLedger is what the lease admitted for its one target: the
// instance, the owned file, the cache duration and the reference interval.
type clockOffsetLedger struct {
	Target clockOffsetTarget
	Owned  clockOwnedFile
	// CacheSec is cache_duration read from the target environment; nil means
	// unknown and the settle window is then unknown.
	CacheSec *int
	// Baseline is the reference interval measured from the same instance
	// before any offset.
	Baseline clockOffsetInterval
}

type clockStepState string

const (
	// clockStepAdmitted: ack, file state and a discriminating measurement of
	// the same instance agree.
	clockStepAdmitted clockStepState = "admitted"
	// clockStepAckOnly: the write was acknowledged but nothing else is known.
	clockStepAckOnly clockStepState = "ack_only"
	// clockStepPending: a mismatch inside the cache window is not a verdict.
	clockStepPending clockStepState = "pending_cache_window"
	// clockStepRejected: evidence shows the offset was not applied as intended.
	clockStepRejected clockStepState = "rejected"
	// clockStepUnknown: the write did not complete as acknowledged; the file
	// may hold the old text, the new text or neither.
	clockStepUnknown      clockStepState = "unknown_outcome"
	clockStepInconclusive clockStepState = "inconclusive"
)

type clockRestoreState string

const (
	clockRestoreVerified    clockRestoreState = "verified"
	clockRestoreAckOnly     clockRestoreState = "acknowledged_only"
	clockRestoreNotRestored clockRestoreState = "not_restored"
	clockRestoreUnknown     clockRestoreState = "unknown"
)

// settleWindowNs is the time after an acknowledged write after which a clock
// call by the target is guaranteed to have refetched the file: the whole-second
// comparison in the pinned source needs cache_duration+1 seconds.
func settleWindowNs(cacheSec *int) (int64, bool) {
	if cacheSec == nil || *cacheSec < 0 {
		return 0, false
	}
	return int64(*cacheSec+1) * 1e9, true
}

func sameInstance(a, b clockOffsetTarget) bool {
	return a.complete() && a == b
}

// admitClockBaseline requires the reference interval to come from the
// ledger's target instance and to contain zero.
func admitClockBaseline(target, measured clockOffsetTarget, baseline clockOffsetInterval) clockProvenanceResult {
	if !sameInstance(target, measured) {
		return provenanceResult(clockProvenanceInconclusive, "baseline was not measured from the ledger's target instance")
	}
	if !baseline.overlaps(clockOffsetInterval{Known: true}) {
		return provenanceResult(clockProvenanceInconclusive, "baseline interval is unknown or does not contain zero")
	}
	return provenanceResult(clockProvenanceValid, "baseline of the target instance contains zero")
}

// judgeClockStep judges the write of step s and its measurement against the
// target and owned file of ledger l. It returns the step state and a reason.
func judgeClockStep(l clockOffsetLedger, s clockOffsetStep) (clockStepState, string) {
	text, ok := clockOffsetText(s.IntendedNs)
	if !ok {
		return clockStepInconclusive, "intended offset is not a whole number of seconds"
	}
	if s.Write != clockWriteAcked {
		return clockStepUnknown, fmt.Sprintf("write outcome %q is not an acknowledgement; a later readback does not promote it", string(s.Write))
	}
	if !s.AckKnown {
		return clockStepInconclusive, "acknowledgement time was not recorded"
	}
	owned := l.Owned
	owned.ExpectedText = text
	if r := verifyClockLiveFileMode(owned, s.File); r.Verdict != clockProvenanceValid {
		state := clockStepRejected
		if r.Verdict == clockProvenanceInconclusive {
			state = clockStepInconclusive
		}
		return state, "owned file: " + r.Reason
	}
	if !s.SampleKnown || !s.TargetSampleOK || !s.Measured.Known {
		return clockStepAckOnly, "write acknowledged and file holds the ledger text, but no usable measurement"
	}
	if !sameInstance(l.Target, s.Target) {
		return clockStepRejected, "measurement came from a different process instance"
	}
	if s.SampleLoMono < s.AckedMono {
		return clockStepInconclusive, "measurement bracket starts before the acknowledgement"
	}
	want := clockOffsetInterval{Known: true, Lo: s.IntendedNs, Hi: s.IntendedNs}
	zero := clockOffsetInterval{Known: true}
	if s.IntendedNs != 0 && s.Measured.overlaps(zero) && s.Measured.overlaps(want) {
		return clockStepInconclusive, "measured interval contains both zero and the intended offset and does not discriminate"
	}
	if s.Measured.overlaps(want) {
		return clockStepAdmitted, "measured interval contains the intended offset after the acknowledgement"
	}
	win, known := settleWindowNs(l.CacheSec)
	if !known {
		return clockStepInconclusive, "cache duration unknown, so a mismatch cannot be attributed"
	}
	if s.SampleLoMono < s.AckedMono+win {
		return clockStepPending, "mismatch inside the cache window that follows the acknowledgement"
	}
	return clockStepRejected, "measurement after the cache window does not contain the intended offset"
}

// judgeClockRestore requires the acknowledged +0 write, the owned file with its
// original identity, and a measurement of the same instance that overlaps the
// baseline without also containing shift, the offset the daemon may still
// show.
func judgeClockRestore(l clockOffsetLedger, s clockOffsetStep, shift clockOffsetInterval) (clockRestoreState, string) {
	if s.IntendedNs != 0 {
		return clockRestoreUnknown, "restore step does not intend +0"
	}
	if s.Write != clockWriteAcked {
		return clockRestoreUnknown, "restore write outcome is not an acknowledgement"
	}
	if !s.AckKnown {
		return clockRestoreUnknown, "restore acknowledgement time was not recorded"
	}
	owned := l.Owned
	owned.ExpectedText = "+0"
	if r := verifyClockLiveFileMode(owned, s.File); r.Verdict != clockProvenanceValid {
		if r.Verdict == clockProvenanceInconclusive {
			return clockRestoreUnknown, "owned file: " + r.Reason
		}
		return clockRestoreNotRestored, "owned file: " + r.Reason
	}
	if !s.SampleKnown || !s.TargetSampleOK || !s.Measured.Known {
		return clockRestoreAckOnly, "restore write acknowledged and file reread, but the target was not measured"
	}
	if !sameInstance(l.Target, s.Target) {
		return clockRestoreNotRestored, "measurement came from a different process instance; a replacement is never restored by name"
	}
	if s.SampleLoMono < s.AckedMono {
		return clockRestoreUnknown, "measurement bracket starts before the restore acknowledgement"
	}
	if s.Measured.overlaps(l.Baseline) {
		if s.Measured.overlaps(shift) {
			return clockRestoreUnknown, "measured interval contains both the reference and the offset and does not discriminate"
		}
		return clockRestoreVerified, "same instance measured inside the reference interval after the +0 acknowledgement"
	}
	win, known := settleWindowNs(l.CacheSec)
	if !known {
		return clockRestoreUnknown, "cache duration unknown, so a mismatch cannot be attributed"
	}
	if s.SampleLoMono < s.AckedMono+win {
		return clockRestoreUnknown, "mismatch inside the cache window that follows the restore acknowledgement"
	}
	return clockRestoreNotRestored, "measurement after the cache window is outside the reference interval"
}
