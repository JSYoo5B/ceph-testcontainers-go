//go:build all || integration

package integration_test

import (
	"errors"
	"fmt"
	"time"
)

// This file holds the pure judges of the live-client fullness scenario: one
// retained writer's operation ledger across the backfillfull policy barrier.
//
// What the pinned Tentacle source (7f793731f1b3) says about client writes
// under this policy, and so what the scenario expects and may claim:
//
//   - backfillfull_ratio is consulted for backfill reservations and scans
//     (OSDService::tentative_backfill_full and check_backfill_full). Nothing on
//     the client write path reads it.
//   - A client write is refused or held by other conditions only: the cluster
//     CEPH_OSDMAP_FULL flag and the pool's FLAG_FULL, both derived by the
//     monitor from the full ratio (Objecter::_osdmap_full_flag and
//     _osdmap_pool_full pause the op in the client; PrimaryLogPG::do_op
//     discards or fails it on the OSD), and the OSD's own fail-safe ratio
//     (check_failsafe_full, which never lies below the full ratio).
//   - The monitor marks a pool FLAG_BACKFILLFULL when an OSD under it is
//     backfillfull. The client library does not read that flag.
//
// The behavior promised by this policy is therefore that client writes are
// served. The scenario does not rely on that: it records the outcome it saw
// (served, blocked, failed or unknown) and compares it with the promise as a
// separate step, so a different outcome is reported as it came.

// fullnessLiveClass is the reading of the writer's ledger over the credited
// barrier window.
type fullnessLiveClass string

const (
	// liveServed: the writer was acknowledged at least the required number of
	// times inside the window, and no operation was refused or failed.
	liveServed fullnessLiveClass = "served"
	// liveBlocked: no acknowledgment, no new submission, and one and the same
	// submission waited for its reply in every sample.
	liveBlocked fullnessLiveClass = "blocked"
	// liveFailed: at least one operation returned a refusal or an error inside
	// the window.
	liveFailed fullnessLiveClass = "failed"
	// liveUnknown: the ledger moved, but not enough to be one of the above.
	liveUnknown fullnessLiveClass = "unknown"
)

// fullnessLiveSample is one reading of the primary's own PG report against the
// held baseline, together with the writer's ledger read at the same time.
// Err is set when either read failed or was malformed; such a sample is
// inconclusive evidence and is kept in the window.
type fullnessLiveSample struct {
	At          time.Time
	Verdict     barrierVerdict
	Err         error
	Interval    uint64
	Mark        secondFaultMark
	Outstanding string
}

// fullnessLiveSampleOf builds a sample. A failed or undecodable PG read, a
// failed or refused ledger, and a PG report without its epoch, interval,
// acting set or recovery progress are inconclusive; none of them decodes into
// an empty value that would count as evidence.
func fullnessLiveSampleOf(base backfillBaseline, at time.Time, raw []byte, rawErr error, events []secondFaultEvent, eventsErr error) fullnessLiveSample {
	s := fullnessLiveSample{At: at, Verdict: barrierInconclusive}
	ledger := minSizeSampleOf(at, raw, rawErr, events, eventsErr)
	if ledger.Err != nil {
		s.Err = ledger.Err
		return s
	}
	p, err := parsePGProgress(raw)
	if err != nil {
		s.Err = err
		return s
	}
	verdict, why := fullnessBarrierSample(base, p)
	s.Verdict, s.Interval, s.Mark, s.Outstanding = verdict, ledger.Interval, ledger.Mark, ledger.Outstanding
	if verdict == barrierInconclusive {
		s.Err = errors.New(why)
	}
	return s
}

// fullnessLiveJudgement is what an accepted window showed.
type fullnessLiveJudgement struct {
	Class   fullnessLiveClass `json:"class"`
	Why     string            `json:"why"`
	Samples int               `json:"samples"`
	Span    time.Duration     `json:"span_ns"`
	Start   secondFaultMark   `json:"writer_at_first_toofull"`
	End     secondFaultMark   `json:"writer_at_end_of_window"`
	Blocked string            `json:"blocked_submission,omitempty"`
}

// judgeFullnessLive classifies the writer over the credited window, which
// starts at the first sample in which the primary reported backfill_toofull.
// Every sample from there on must be a conclusive backfill_toofull sample in
// the first sample's interval, no two may lie more than maxGap apart, and the
// writer's counters may not go backwards: a failed read, a gap or a regressed
// ledger returns an error and no classification, so that the time on either
// side cannot be credited as one uninterrupted window. Operations are counted
// where the ledger recorded them (acknowledgment, error), never replayed and
// never reclassified by a later read.
func judgeFullnessLive(samples []fullnessLiveSample, minAcks int, maxGap time.Duration) (fullnessLiveJudgement, error) {
	j := fullnessLiveJudgement{}
	first := -1
	for i, s := range samples {
		if s.Err == nil && s.Verdict == barrierHolds {
			first = i
			break
		}
	}
	if first < 0 {
		return j, fmt.Errorf("no conclusive backfill_toofull sample in %d samples", len(samples))
	}
	window := samples[first:]
	j.Samples, j.Start, j.End = len(window), window[0].Mark, window[len(window)-1].Mark
	j.Span = window[len(window)-1].At.Sub(window[0].At)
	prev := window[0]
	for i, s := range window {
		switch {
		case s.Err != nil:
			return j, fmt.Errorf("window sample %d is inconclusive: %w", i, s.Err)
		case s.Verdict != barrierHolds:
			return j, fmt.Errorf("window sample %d is %s, not backfill_toofull", i, s.Verdict)
		case s.Interval != window[0].Interval:
			return j, fmt.Errorf("window sample %d is in interval %d, the first is in %d", i, s.Interval, window[0].Interval)
		case i > 0 && (s.At.Before(prev.At) || s.At.Sub(prev.At) > maxGap):
			return j, fmt.Errorf("window sample %d is %s after the previous one, want within %s", i, s.At.Sub(prev.At).Round(time.Millisecond), maxGap)
		case s.Mark.Submitted < prev.Mark.Submitted || s.Mark.Acked < prev.Mark.Acked || s.Mark.Refused < prev.Mark.Refused || s.Mark.Unknown < prev.Mark.Unknown:
			return j, fmt.Errorf("window sample %d: writer counters went from %+v to %+v", i, prev.Mark, s.Mark)
		}
		prev = s
	}
	acked := j.End.Acked - j.Start.Acked
	failed := (j.End.Refused - j.Start.Refused) + (j.End.Unknown - j.Start.Unknown)
	submitted := j.End.Submitted - j.Start.Submitted
	sameWaiting := window[0].Outstanding != ""
	for _, s := range window {
		sameWaiting = sameWaiting && s.Outstanding == window[0].Outstanding && s.Mark.Outstanding == 1
	}
	switch {
	case failed > 0:
		j.Class, j.Why = liveFailed, fmt.Sprintf("%d operations returned a refusal or an error inside the window", failed)
	case acked >= minAcks:
		j.Class, j.Why = liveServed, fmt.Sprintf("%d acknowledgments inside the window, no refusal or error", acked)
	case acked == 0 && submitted == 0 && sameWaiting:
		j.Class, j.Blocked, j.Why = liveBlocked, window[0].Outstanding, "one submission waited for its reply in every sample, with no acknowledgment and no new submission"
	default:
		j.Class, j.Why = liveUnknown, fmt.Sprintf("%d acknowledgments and %d new submissions, fewer than the %d required to call it served and not one waiting submission", acked, submitted, minAcks)
	}
	return j, nil
}

// fullnessLiveExpect compares the observed class with what the selected policy
// promises for client writes, which is that they are served. It is applied
// after the observation was recorded and judged, never in its place.
func fullnessLiveExpect(j fullnessLiveJudgement) error {
	if j.Class != liveServed {
		return fmt.Errorf("client writes under backfillfull alone were %s, not served: %s", j.Class, j.Why)
	}
	return nil
}

// fullnessLiveLate is an operation that had not returned at the end of the
// window and what the ledger recorded for it at the final read.
type fullnessLiveLate struct {
	Name  string `json:"name"`
	Final string `json:"final_outcome"`
}

// fullnessLiveSettle compares the operations at the end of the window with the
// final ledger. The final ledger must extend the earlier one in order. An
// operation that had already returned keeps its outcome; only one that was
// outstanding may change, and when it still has no terminal record at the
// stop it is classified unknown (secondFaultOps with final set). Nothing is resubmitted and a later read of the stored
// object does not promote or demote an outcome.
func fullnessLiveSettle(atWindowEnd, final []secondFaultOp) ([]fullnessLiveLate, error) {
	if len(final) < len(atWindowEnd) {
		return nil, fmt.Errorf("the final ledger has %d operations, %d were recorded at the end of the window", len(final), len(atWindowEnd))
	}
	var late []fullnessLiveLate
	for i, before := range atWindowEnd {
		after := final[i]
		if after.Name != before.Name || after.Seq != before.Seq || after.SHA256 != before.SHA256 || after.Bytes != before.Bytes {
			return nil, fmt.Errorf("operation %d is %+v in the final ledger, was %+v", i, after, before)
		}
		switch {
		case before.Outcome != opOutstanding && after.Outcome != before.Outcome:
			return nil, fmt.Errorf("%s returned %s, then the final ledger says %s", before.Name, before.Outcome, after.Outcome)
		case before.Outcome == opOutstanding:
			late = append(late, fullnessLiveLate{Name: before.Name, Final: after.Outcome})
		}
	}
	return late, nil
}

// fullnessLiveReadback is what the independent readback showed for the
// writer's operations.
type fullnessLiveReadback struct {
	AckedVerified  int `json:"acknowledged_read_back_exact"`
	UnknownPresent int `json:"unknown_present_whole"`
	UnknownAbsent  int `json:"unknown_absent"`
}

// fullnessLiveVerify requires every acknowledged operation to read back with
// exactly its submitted bytes (secondFaultVerify) and counts the unknown ones
// without promoting them. At least one acknowledged write is required, so that
// an empty ledger cannot pass.
func fullnessLiveVerify(ops []secondFaultOp, read map[string]secondFaultObject) (fullnessLiveReadback, error) {
	var out fullnessLiveReadback
	if err := secondFaultVerify(ops, read); err != nil {
		return out, err
	}
	for _, op := range ops {
		switch {
		case op.Outcome == opAcked:
			out.AckedVerified++
		case op.Outcome == opUnknown && read[op.Name].Present:
			out.UnknownPresent++
		case op.Outcome == opUnknown:
			out.UnknownAbsent++
		}
	}
	if out.AckedVerified == 0 {
		return out, errors.New("the writer has no acknowledged operation to read back")
	}
	return out, nil
}
