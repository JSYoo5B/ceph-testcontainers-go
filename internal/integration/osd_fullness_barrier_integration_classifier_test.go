//go:build all || integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
)

// This file holds the decoders and judges of the fullness-policy barrier
// scenario. They are pure, so that the evidence rules can be checked without a
// cluster.
//
// What the pinned Tentacle source (7f793731f1b3) does, and so what the
// scenario may claim:
//
//   - A replica answers a backfill reservation request (RepNotRecovering, on
//     RequestBackfillPrio) by PG::try_reserve_recovery_space, which asks
//     OSDService::tentative_backfill_full: the replica's own statfs against the
//     backfillfull ratio of its OSDMap. A refusal returns REJECT_TOOFULL, and
//     the primary (WaitRemoteBackfillReserved) sets PG_STATE_BACKFILL_TOOFULL
//     and schedules RequestBackfill again after osd_backfill_retry_interval.
//   - A backfill whose reservation was already granted is checked again at
//     every scan request: PrimaryLogPG::do_scan, on OP_SCAN_GET_DIGEST, asks
//     OSDService::check_backfill_full (the replica's current full state) and,
//     if it is backfillfull, queues BackfillTooFull. The replica then sends
//     REVOKE_TOOFULL, and the primary (Backfilling) sets BACKFILL_TOOFULL,
//     clears BACKFILLING and retries after the same interval.
//   - The flag is cleared only when the primary enters Backfilling again, or
//     leaves Active. A PG that keeps being refused therefore keeps reporting
//     backfill_toofull.
//   - The nobackfill flag does not stop the reservation. It defers the work in
//     PrimaryLogPG::start_recovery_ops once the PG is already in Backfilling,
//     so a PG held by it is reserved and has not scanned yet.
//   - recovery_toofull is a different check. It needs CEPH_OSD_FULL (the full
//     ratio, not backfillfull) on a member of acting_recovery_backfill, in
//     WaitLocalRecoveryReserved and WaitRemoteRecoveryReserved. Lowering only
//     backfillfull_ratio never produces it, and this scenario does not try.
//
// Neither path is told apart by the PG state alone, which reads
// backfill_toofull in both. The scenario tells them apart by order: the
// policy is applied after the reservation was seen granted (revocation) or
// before the destination first returns (admission).

// pgStateHas reports whether state, a "+" separated list of PG state names,
// contains name as a whole name.
func pgStateHas(state, name string) bool { return slices.Contains(strings.Split(state, "+"), name) }

// barrierVerdict is the reading of one PG sample against the held baseline.
type barrierVerdict string

const (
	// barrierHolds: the destination is still at MIN, nothing is running and the
	// primary reports backfill_toofull.
	barrierHolds barrierVerdict = "toofull"
	// barrierWaiting: at MIN, nothing running, but not (yet) backfill_toofull.
	barrierWaiting barrierVerdict = "waiting"
	// barrierRunning: the primary reports a running backfill.
	barrierRunning barrierVerdict = "running"
	// barrierAdvanced: the destination's cursor left MIN, so transfer happened.
	barrierAdvanced barrierVerdict = "advanced"
	// barrierInvalid: another interval or placement; the baseline no longer applies.
	barrierInvalid barrierVerdict = "invalid"
	// barrierInconclusive: a required field is missing, so the sample is no evidence.
	barrierInconclusive barrierVerdict = "inconclusive"
)

// fullnessBarrierSample reads one primary sample against a baseline taken by
// backfillHeld. Missing or empty fields are inconclusive, never evidence.
func fullnessBarrierSample(base backfillBaseline, p pgProgress) (barrierVerdict, string) {
	if p.Epoch == nil || p.Info.History.SameIntervalSince == nil || p.Info.History.SamePrimarySince == nil {
		return barrierInconclusive, "the sample has no epoch or interval"
	}
	if *p.Info.History.SameIntervalSince != base.SameIntervalSince || *p.Info.History.SamePrimarySince != base.SamePrimarySince ||
		!slices.Equal(p.Up, base.Up) || !slices.Equal(p.Acting, base.Acting) {
		return barrierInvalid, fmt.Sprintf("interval since %d (primary since %d), up %v, acting %v differ from the baseline", *p.Info.History.SameIntervalSince, *p.Info.History.SamePrimarySince, p.Up, p.Acting)
	}
	if *p.Epoch < base.Epoch {
		return barrierInvalid, fmt.Sprintf("map epoch %d is before the baseline's %d", *p.Epoch, base.Epoch)
	}
	cursor, found := p.cursorOf(base.Target)
	if !found {
		return barrierInconclusive, fmt.Sprintf("osd.%d has no cursor", base.Target)
	}
	if cursor != "MIN" {
		return barrierAdvanced, fmt.Sprintf("osd.%d last_backfill %s left MIN", base.Target, cursor)
	}
	progress, ok := p.progress()
	if !ok {
		return barrierInconclusive, "the PG reports no recovery progress: state " + p.State
	}
	if len(progress.BackfillTargets) != 1 || osdOf(progress.BackfillTargets[0]) != base.Target {
		return barrierInconclusive, fmt.Sprintf("backfill targets %v, want only osd.%d", progress.BackfillTargets, base.Target)
	}
	switch {
	case pgStateHas(p.State, "backfilling"):
		return barrierRunning, "state " + p.State
	case pgStateHas(p.State, "backfill_toofull"):
		return barrierHolds, "state " + p.State
	}
	return barrierWaiting, "state " + p.State
}

// barrierSample is one verdict with the time it was taken.
type barrierSample struct {
	Verdict barrierVerdict
	At      time.Time
}

// barrierJudgement is what a window of samples showed.
type barrierJudgement struct {
	// Holds is the number of backfill_toofull samples; Before counts the samples
	// before the first one; FirstHold and LastHold bound them in time.
	Holds, Before        int
	FirstHold, LastHold  time.Time
	BeforeRunning        int
	HeldFor              time.Duration
	BeforeVerdictsByName map[barrierVerdict]int
}

// judgeBarrier accepts a window of samples only when the primary reported
// backfill_toofull at least once, kept reporting it in every later sample for
// at least minSpan (so that at least one retry of the reservation was refused
// too), and never showed a running backfill or an advanced cursor after the
// first report. Samples before the first report may be waiting; they may show a
// running backfill only when allowRunningBefore is set, which is the revocation
// path (a granted reservation is taken back). In the admission path a running
// backfill before the first report means the reservation was granted, so the
// path is not the one claimed.
func judgeBarrier(samples []barrierSample, minSpan time.Duration, allowRunningBefore bool) (barrierJudgement, error) {
	j := barrierJudgement{BeforeVerdictsByName: map[barrierVerdict]int{}}
	first := slices.IndexFunc(samples, func(s barrierSample) bool { return s.Verdict == barrierHolds })
	if first < 0 {
		return j, fmt.Errorf("backfill_toofull was never reported in %d samples", len(samples))
	}
	for i, sample := range samples {
		if i < first {
			j.Before++
			j.BeforeVerdictsByName[sample.Verdict]++
			switch sample.Verdict {
			case barrierWaiting:
			case barrierRunning:
				j.BeforeRunning++
				if !allowRunningBefore {
					return j, fmt.Errorf("a backfill was running before backfill_toofull: the reservation was not refused at admission")
				}
			default:
				return j, fmt.Errorf("sample %d before the first backfill_toofull is %s", i, sample.Verdict)
			}
			continue
		}
		if sample.Verdict != barrierHolds {
			return j, fmt.Errorf("sample %d after the first backfill_toofull is %s", i, sample.Verdict)
		}
		j.Holds++
		j.LastHold = sample.At
	}
	j.FirstHold = samples[first].At
	j.HeldFor = j.LastHold.Sub(j.FirstHold)
	if j.HeldFor < minSpan {
		return j, fmt.Errorf("backfill_toofull was observed for %s, want at least %s", j.HeldFor.Round(time.Millisecond), minSpan)
	}
	return j, nil
}

// recordFailedRead is how a failed or malformed PG read enters the window. Once
// backfill_toofull has been reported, the window is being credited, so a read
// that failed inside it is an inconclusive sample that judgeBarrier refuses:
// the time on either side must not be credited as one uninterrupted barrier.
// Before the first report nothing is credited yet, so the failure is only
// counted by the caller and the sample list is unchanged.
func recordFailedRead(samples []barrierSample, at time.Time, credited bool) []barrierSample {
	if !credited {
		return samples
	}
	return append(samples, barrierSample{Verdict: barrierInconclusive, At: at})
}

// osdStates are the state names of every OSD in an "osd dump": exists, up,
// nearfull, backfillfull, full and others. An OSD without a state array, with a
// null one or without an id is an error, not an empty set.
func osdStates(data []byte) (map[int][]string, error) {
	var dump struct {
		OSDs *[]struct {
			OSD   *int      `json:"osd"`
			State *[]string `json:"state"`
		} `json:"osds"`
	}
	if err := json.Unmarshal(data, &dump); err != nil {
		return nil, fmt.Errorf("osd dump: %w", err)
	}
	if dump.OSDs == nil || len(*dump.OSDs) == 0 {
		return nil, fmt.Errorf("osd dump without its osds: %.256s", data)
	}
	states := map[int][]string{}
	for _, osd := range *dump.OSDs {
		if osd.OSD == nil || osd.State == nil {
			return nil, fmt.Errorf("osd dump entry without an id or a state array: %+v", osd)
		}
		if _, seen := states[*osd.OSD]; seen {
			return nil, fmt.Errorf("osd.%d appears twice in the osd dump", *osd.OSD)
		}
		states[*osd.OSD] = slices.Clone(*osd.State)
	}
	return states, nil
}

// backfillFullIs reports whether every listed OSD is in the dump, is up, has
// the backfillfull state and not the full state (which would block writes and
// belongs to recovery_toofull, not to this scenario) when want is set, and has
// neither state when it is not. The second result says why not.
func backfillFullIs(states map[int][]string, want bool, osds ...int) (bool, string) {
	for _, osd := range osds {
		list, ok := states[osd]
		switch {
		case !ok:
			return false, fmt.Sprintf("osd.%d is not in the osd dump", osd)
		case !slices.Contains(list, "up"):
			return false, fmt.Sprintf("osd.%d is not up: %v", osd, list)
		case slices.Contains(list, "full"):
			return false, fmt.Sprintf("osd.%d is full: %v", osd, list)
		case slices.Contains(list, "backfillfull") != want:
			return false, fmt.Sprintf("osd.%d states %v, backfillfull want %t", osd, list, want)
		}
	}
	return true, ""
}
