//go:build all || integration

package integration_test

import (
	"fmt"
	"strings"
)

// This file models, without a clock, a process or Docker, how a per-target
// offset and restore ledger for a libfaketime v0.9.12 live-file fixture
// (3ccdd344aadf1e317156fa5fb7c881f2c4404778) is judged. It classifies recorded
// observations only. It never writes a file, never samples a clock and is not
// evidence that any interposer reaches a Ceph daemon or that a clock fault
// backend works.
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
//   - An unavailable owned file falls back to a non-owned source (see the
//     config-source provenance model), so a measurement that looks like zero
//     after the owned file was removed is not a restoration.
//
// The ledger distinguishes five facts that must not be merged:
//
//  1. the controller's write was acknowledged,
//  2. the owned file still holds the ledger text with its original identity,
//  3. the same target instance showed the offset in a bracketed measurement
//     taken after the acknowledgement,
//  4. the target returned to the reference interval, measured the same way,
//  5. the owned resources were disposed of in an order that does not move a
//     live target to a fallback source.
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
	Label string
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

type clockProcessState string

const (
	clockProcessRunning clockProcessState = "running"
	clockProcessExited  clockProcessState = "exited"
)

type clockFileDisposition string

const (
	clockFilePresent clockFileDisposition = "present"
	clockFileRemoved clockFileDisposition = "removed"
)

// clockOffsetCleanup records what happened to the owned resources. Errors are
// kept apart from the scenario's first failure.
type clockOffsetCleanup struct {
	Process      clockProcessState // zero value: not observed
	ExitMono     int64
	ExitKnown    bool
	File         clockFileDisposition // zero value: not observed
	RemovedMono  int64
	RemovedKnown bool
	Errors       []string
}

// clockOffsetLedger is everything one target contributes. It is emitted on
// every outcome, including a partial one, before cleanup is judged.
type clockOffsetLedger struct {
	Target clockOffsetTarget
	Owned  clockOwnedFile
	Env    clockTargetEnvironment
	// Initial is the owned file as observed at admission.
	Initial clockObservedFile
	// CacheSec is cache_duration read from the target environment; nil means
	// unknown and the settle window is then unknown.
	CacheSec *int
	// Baseline is the reference interval measured from the same instance
	// before any offset.
	Baseline       clockOffsetInterval
	BaselineTarget clockOffsetTarget
	Steps          []clockOffsetStep
	// Restore is the final return to +0, judged against Baseline.
	Restore *clockOffsetStep
	Cleanup clockOffsetCleanup
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
	clockRestoreVerified     clockRestoreState = "verified"
	clockRestoreAckOnly      clockRestoreState = "acknowledged_only"
	clockRestoreNotRestored  clockRestoreState = "not_restored"
	clockRestoreUnknown      clockRestoreState = "unknown"
	clockRestoreNotAttempted clockRestoreState = "not_attempted"
)

type clockStepReport struct {
	Label  string
	State  clockStepState
	Reason string
}

// clockLedgerJudgement keeps the first scenario failure and the cleanup
// failures separate. It holds only strings and enums, which are copies.
type clockLedgerJudgement struct {
	Admission      clockProvenanceResult
	Steps          []clockStepReport
	Restore        clockRestoreState
	RestoreReason  string
	FirstFailure   string
	CleanupVerdict clockProvenanceVerdict
	CleanupReason  string
	CleanupErrors  []string
}

func (j *clockLedgerJudgement) fail(format string, args ...any) {
	if j.FirstFailure == "" {
		j.FirstFailure = fmt.Sprintf(format, args...)
	}
}

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

// judgeClockStep judges one write and its measurement. expected is the target
// the ledger owns. It returns the step state and a reason.
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

// judgeClockOffsetLedger judges one target. Admission failure keeps every step
// out of the evidence, but the receipt still lists them.
func judgeClockOffsetLedger(l clockOffsetLedger) clockLedgerJudgement {
	j := clockLedgerJudgement{Restore: clockRestoreNotAttempted}
	j.Admission = admitClockLiveFileMode(l.Env, l.Owned, l.Initial)
	if j.Admission.Verdict == clockProvenanceValid {
		zero := clockOffsetInterval{Known: true}
		switch {
		case !sameInstance(l.Target, l.BaselineTarget):
			j.Admission = provenanceResult(clockProvenanceInconclusive, "baseline was not measured from the ledger's target instance")
		case !l.Baseline.overlaps(zero):
			j.Admission = provenanceResult(clockProvenanceInconclusive, "baseline interval is unknown or does not contain zero")
		}
	}
	admitted := j.Admission.Verdict == clockProvenanceValid
	if !admitted {
		j.fail("admission %s: %s", j.Admission.Verdict, j.Admission.Reason)
	}

	var lastShift clockOffsetInterval
	prevAck, prevSample, prevKnown := int64(0), int64(0), true
	for _, s := range l.Steps {
		state, reason := judgeClockStep(l, s)
		if state == clockStepAdmitted || state == clockStepAckOnly {
			if !prevKnown || s.AckedMono <= prevAck || s.AckedMono < prevSample {
				state, reason = clockStepInconclusive, "acknowledgement does not follow the previous step's acknowledgement and measurement"
			}
		}
		if s.AckKnown {
			prevAck = max(prevAck, s.AckedMono)
		} else {
			prevKnown = false
		}
		if s.SampleKnown {
			prevSample = max(prevSample, s.SampleLoMono)
		}
		if !admitted {
			state, reason = clockStepInconclusive, "target was not admitted: "+j.Admission.Reason
		}
		if state == clockStepAdmitted && s.IntendedNs != 0 {
			lastShift = s.Measured
		}
		if state != clockStepAdmitted {
			j.fail("step %s %s: %s", s.Label, state, reason)
		}
		j.Steps = append(j.Steps, clockStepReport{Label: s.Label, State: state, Reason: reason})
	}
	if l.Restore != nil && admitted {
		j.Restore, j.RestoreReason = judgeClockRestore(l, *l.Restore, lastShift)
		if j.Restore == clockRestoreVerified {
			j.Restore, j.RestoreReason = demoteRestoreByChronology(l)
		}
	} else if l.Restore != nil {
		j.Restore, j.RestoreReason = clockRestoreUnknown, "target was not admitted"
	}
	if j.Restore != clockRestoreVerified && j.Restore != clockRestoreNotAttempted {
		j.fail("restore %s: %s", j.Restore, j.RestoreReason)
	}
	j.CleanupVerdict, j.CleanupReason = judgeClockCleanup(l.Cleanup, j.Restore == clockRestoreVerified)
	j.CleanupErrors = append([]string(nil), l.Cleanup.Errors...)
	return j
}

// demoteRestoreByChronology keeps a verified restore only when its
// acknowledgement and measurement follow every earlier step. An earlier attempt
// whose acknowledgement time was never recorded (a failed or canceled write)
// could have landed after the restore, so the order cannot be established.
func demoteRestoreByChronology(l clockOffsetLedger) (clockRestoreState, string) {
	r := *l.Restore
	for _, s := range l.Steps {
		if !s.AckKnown {
			return clockRestoreUnknown, "an earlier write attempt has no recorded time, so it may follow the restore"
		}
		if r.AckedMono <= s.AckedMono || (s.SampleKnown && r.AckedMono < s.SampleLoMono) {
			return clockRestoreUnknown, "restore acknowledgement does not follow the offset sequence"
		}
		if r.SampleLoMono <= s.AckedMono {
			return clockRestoreUnknown, "restore measurement does not follow the offset sequence"
		}
	}
	return clockRestoreVerified, "same instance measured inside the reference interval after the +0 acknowledgement"
}

// judgeClockRestore requires the acknowledged +0 write, the owned file with its
// original identity, and a measurement of the same instance that overlaps the
// baseline without also containing the last admitted offset.
func judgeClockRestore(l clockOffsetLedger, s clockOffsetStep, lastShift clockOffsetInterval) (clockRestoreState, string) {
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
		if lastShift.Known && s.Measured.overlaps(lastShift) {
			return clockRestoreUnknown, "measured interval contains both the reference and the last admitted offset and does not discriminate"
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

// judgeClockCleanup judges the ownership of the target and file. Removing the
// owned file is allowed only after the target is observed gone; a live target
// either stays owned and verified restored, or is a cleanup failure.
func judgeClockCleanup(c clockOffsetCleanup, restored bool) (clockProvenanceVerdict, string) {
	if c.Process == "" || c.File == "" {
		return clockProvenanceInconclusive, "cleanup outcome was not recorded for the process and the file"
	}
	switch c.Process {
	case clockProcessExited:
		if !c.ExitKnown {
			return clockProvenanceInconclusive, "exit time was not recorded"
		}
		switch c.File {
		case clockFileRemoved:
			if !c.RemovedKnown {
				return clockProvenanceInconclusive, "removal time was not recorded"
			}
			if c.RemovedMono < c.ExitMono {
				return clockProvenanceInvalid, "owned file was removed before the target exit was observed"
			}
			return clockProvenanceValid, "target exited, then the owned file was removed"
		case clockFilePresent:
			return clockProvenanceInvalid, "owned file was left behind after the target exited"
		}
	case clockProcessRunning:
		if c.File == clockFileRemoved {
			return clockProvenanceInvalid, "owned file was removed while the target still runs; unlinking is not restoration"
		}
		if c.File == clockFilePresent {
			if restored {
				return clockProvenanceValid, "target is still owned and was verified restored"
			}
			return clockProvenanceInvalid, "live target left owned and not verified restored"
		}
	}
	return clockProvenanceInconclusive, "unrecognised cleanup record"
}

// String is the receipt: states and reasons only, with no raw file contents.
func (j clockLedgerJudgement) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "admission=%s", j.Admission.Verdict)
	for _, s := range j.Steps {
		fmt.Fprintf(&b, " %s=%s", s.Label, s.State)
	}
	fmt.Fprintf(&b, " restore=%s cleanup=%s", j.Restore, j.CleanupVerdict)
	if j.FirstFailure != "" {
		fmt.Fprintf(&b, " first_failure=%q", j.FirstFailure)
	}
	return b.String()
}

// clockFleetEntry names a ledger supplied to the fleet judge. A nil pointer is
// rejected rather than read.
type clockFleetEntry struct {
	Name   string
	Ledger *clockOffsetLedger
}

// judgeClockFleet judges targets independently and rejects sharing: one owned
// path or one target instance in two ledgers means that one target's evidence
// could be counted for another.
func judgeClockFleet(entries []clockFleetEntry) map[string]clockLedgerJudgement {
	out := map[string]clockLedgerJudgement{}
	paths := map[string][]string{}
	insts := map[clockOffsetTarget][]string{}
	for _, e := range entries {
		if e.Ledger == nil {
			continue
		}
		paths[e.Ledger.Owned.Path] = append(paths[e.Ledger.Owned.Path], e.Name)
		insts[e.Ledger.Target] = append(insts[e.Ledger.Target], e.Name)
	}
	for _, e := range entries {
		if e.Ledger == nil {
			j := clockLedgerJudgement{Restore: clockRestoreUnknown, CleanupVerdict: clockProvenanceInconclusive}
			j.Admission = provenanceResult(clockProvenanceInconclusive, "ledger is nil")
			j.fail("admission inconclusive: ledger is nil")
			out[e.Name] = j
			continue
		}
		j := judgeClockOffsetLedger(*e.Ledger)
		if len(paths[e.Ledger.Owned.Path]) > 1 || len(insts[e.Ledger.Target]) > 1 {
			j.Admission = provenanceResult(clockProvenanceInvalid, "owned file or target instance is shared between ledgers")
			j.fail("admission invalid: %s", j.Admission.Reason)
			for i := range j.Steps {
				if j.Steps[i].State == clockStepAdmitted {
					j.Steps[i].State, j.Steps[i].Reason = clockStepInconclusive, "ledger shares an owned resource"
				}
			}
			if j.Restore == clockRestoreVerified {
				j.Restore, j.RestoreReason = clockRestoreUnknown, "ledger shares an owned resource"
			}
			// Live cleanup depends on a verified restore; rejudge it.
			j.CleanupVerdict, j.CleanupReason = judgeClockCleanup(e.Ledger.Cleanup, j.Restore == clockRestoreVerified)
		}
		out[e.Name] = j
	}
	return out
}
