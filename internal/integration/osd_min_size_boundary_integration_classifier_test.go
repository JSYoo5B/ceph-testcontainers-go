//go:build all || integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"
)

// This file holds the decoders and judges of the min_size boundary scenario.
// They are pure, so that the evidence rules can be checked without a cluster.
//
// What the pinned Tentacle source (7f793731f1b3) does, and so what the
// scenario may claim:
//
//   - A PG goes active only when its acting set holds at least min_size OSDs
//     (PeeringState::acting_set_writeable). With fewer, and with the remaining
//     copy recoverable (osd_allow_recovery_below_min_size defaults to true),
//     the primary reports the peered state instead of active
//     (PeeringState::Active::react(AllReplicasActivated)).
//   - A backfill destination belongs to the up set and to acting_recovery_backfill,
//     not to the acting set, so it does not count toward min_size.
//   - PrimaryLogPG::do_request queues a client CEPH_MSG_OSD_OP on a peered PG that
//     is not active in waiting_for_active, and requeues it when the PG goes
//     active again. A client request is therefore delayed, not answered with an
//     error, while the PG is below min_size. A client that honors backoffs may be
//     told to wait instead (add_pg_backoff); either way no reply is sent.
//
// The scenario's observation of this is the retained writer's ledger: one
// submission has no terminal record for the whole window. That shows the
// request was not acknowledged. It does not show where the request waited.

// minSizeSample is one reading of the primary's own view of the PG together
// with the writer's ledger. Err is set when either read failed or was
// malformed; such a sample is no evidence and is kept in the window.
type minSizeSample struct {
	At          time.Time
	Err         error
	State       string
	Epoch       uint64
	Interval    uint64
	Acting      []int
	Mark        secondFaultMark
	Outstanding string // name of the single outstanding submission, empty when none or several
}

// noOSD is the placeholder Ceph prints for an empty slot of an acting set.
const noOSD = 2147483647

// minSizeActing counts the real members of an acting set.
func minSizeActing(acting []int) int {
	n := 0
	for _, osd := range acting {
		if osd != noOSD && osd >= 0 {
			n++
		}
	}
	return n
}

// minSizeSampleOf builds a sample from a pg query and the writer's ledger.
// Missing, null or wrong-typed fields and unreadable ledgers become Err.
func minSizeSampleOf(at time.Time, raw []byte, rawErr error, events []secondFaultEvent, eventsErr error) minSizeSample {
	s := minSizeSample{At: at}
	if rawErr != nil {
		s.Err = fmt.Errorf("pg query: %w", rawErr)
		return s
	}
	p, err := parsePGProgress(raw)
	switch {
	case err != nil:
		s.Err = err
		return s
	case p.Epoch == nil || p.Info.History.SameIntervalSince == nil:
		s.Err = fmt.Errorf("pg query without its epoch or interval: %.256s", raw)
		return s
	case p.Acting == nil:
		s.Err = fmt.Errorf("pg query without an acting set: %.256s", raw)
		return s
	}
	if eventsErr != nil {
		s.Err = fmt.Errorf("writer ledger: %w", eventsErr)
		return s
	}
	ops, err := secondFaultOps(events, false)
	if err != nil {
		s.Err = fmt.Errorf("writer ledger refused: %w", err)
		return s
	}
	s.State, s.Epoch, s.Interval, s.Acting = p.State, *p.Epoch, *p.Info.History.SameIntervalSince, slices.Clone(p.Acting)
	s.Mark = secondFaultCount(ops)
	if s.Mark.Outstanding == 1 {
		for _, op := range ops {
			if op.Outcome == opOutstanding {
				s.Outstanding = op.Name
			}
		}
	}
	return s
}

// minSizeBaseline is the state after the fault was confirmed, from which the
// window is judged: the interval the PG is in, the writer's acknowledgments so
// far, and the one submission that has not returned.
type minSizeBaseline struct {
	At       time.Time
	Interval uint64
	Acked    int
	Blocked  string
}

// minSizeBelow reports why a sample does not show a peered PG with fewer than
// minSize acting members, or "" when it does.
func minSizeBelow(s minSizeSample, minSize int) string {
	switch {
	case s.Err != nil:
		return "the sample is inconclusive: " + s.Err.Error()
	case s.State == "":
		return "the sample has no state"
	case pgStateHas(s.State, "active") || !pgStateHas(s.State, "peered"):
		return "state " + s.State + " is not peered"
	case minSizeActing(s.Acting) < 1 || minSizeActing(s.Acting) >= minSize:
		return fmt.Sprintf("acting %v does not hold between one and %d members", s.Acting, minSize-1)
	}
	return ""
}

// minSizeBaselineOf accepts a sample as the baseline: below min_size, and with
// exactly one submission waiting for its reply.
func minSizeBaselineOf(s minSizeSample, minSize int) (minSizeBaseline, error) {
	if why := minSizeBelow(s, minSize); why != "" {
		return minSizeBaseline{}, errors.New(why)
	}
	if s.Outstanding == "" || s.Mark.Refused != 0 || s.Mark.Unknown != 0 {
		return minSizeBaseline{}, fmt.Errorf("the writer has no single waiting submission without a failure: %+v", s.Mark)
	}
	return minSizeBaseline{At: s.At, Interval: s.Interval, Acked: s.Mark.Acked, Blocked: s.Outstanding}, nil
}

// minSizeJudgement is what an accepted window showed.
type minSizeJudgement struct {
	Samples int            `json:"samples"`
	Span    time.Duration  `json:"span_ns"`
	Acked   int            `json:"acked"`
	Blocked string         `json:"blocked"`
	States  map[string]int `json:"states"`
}

// judgeMinSizeBlock accepts a window only when every sample, with no gap longer
// than maxGap from the baseline or from the previous sample, shows the PG
// peered below min_size in the baseline's interval, with the writer's
// acknowledgment count unchanged and the same submission still waiting, and the
// window lasts at least minSpan over at least minSamples samples. A failed,
// missing or malformed read is a sample that fails the window; it is never
// skipped, so the time on either side cannot be credited as one uninterrupted
// barrier.
func judgeMinSizeBlock(base minSizeBaseline, samples []minSizeSample, minSize int, minSpan, maxGap time.Duration, minSamples int) (minSizeJudgement, error) {
	j := minSizeJudgement{Samples: len(samples), Acked: base.Acked, Blocked: base.Blocked, States: map[string]int{}}
	if base.Blocked == "" || base.At.IsZero() {
		return j, errors.New("no baseline")
	}
	if len(samples) < minSamples {
		return j, fmt.Errorf("%d samples, want at least %d", len(samples), minSamples)
	}
	prev := base.At
	for i, s := range samples {
		if why := minSizeBelow(s, minSize); why != "" {
			return j, fmt.Errorf("sample %d: %s", i, why)
		}
		switch {
		case s.At.Before(prev) || s.At.Sub(prev) > maxGap:
			return j, fmt.Errorf("sample %d is %s after the previous observation, want within %s", i, s.At.Sub(prev).Round(time.Millisecond), maxGap)
		case s.Interval != base.Interval:
			return j, fmt.Errorf("sample %d is in interval %d, the baseline's is %d", i, s.Interval, base.Interval)
		case s.Mark.Acked != base.Acked:
			return j, fmt.Errorf("sample %d: the writer was acknowledged %d times, baseline %d", i, s.Mark.Acked, base.Acked)
		case s.Mark.Refused != 0 || s.Mark.Unknown != 0 || s.Mark.Outstanding != 1:
			return j, fmt.Errorf("sample %d: writer outcomes %+v, want one submission waiting and no failure", i, s.Mark)
		case s.Outstanding != base.Blocked:
			return j, fmt.Errorf("sample %d: waiting submission %q, baseline %q", i, s.Outstanding, base.Blocked)
		}
		j.States[s.State]++
		prev = s.At
	}
	j.Span = samples[len(samples)-1].At.Sub(base.At)
	if j.Span < minSpan {
		return j, fmt.Errorf("the barrier was observed for %s, want at least %s", j.Span.Round(time.Millisecond), minSpan)
	}
	return j, nil
}

// collectMinSizeWindow takes samples every interval until the last one is at
// least span after start, and keeps every sample, failed ones included. sample
// and sleep are injected so that the collector can be checked offline.
func collectMinSizeWindow(ctx context.Context, start time.Time, span time.Duration, sample func() minSizeSample, sleep func(context.Context) error) ([]minSizeSample, error) {
	var samples []minSizeSample
	for {
		s := sample()
		samples = append(samples, s)
		if !s.At.Before(start.Add(span)) {
			return samples, nil
		}
		if err := sleep(ctx); err != nil {
			return samples, err
		}
	}
}
