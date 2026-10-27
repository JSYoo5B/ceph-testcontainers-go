//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"strings"
	"testing"
	"time"
)

// These controls judge synthetic clone timeline receipts. They test the
// receipt shape and verdicts only; they do not run a clone or show any native
// timing.

var (
	timelineBase  = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	timelineFirst = cloneTimelineIdentity{Filesystem: "fs", Group: "grp", Name: "clone", Incarnation: 1}
	timelineNext  = cloneTimelineIdentity{Filesystem: "fs", Group: "grp", Name: "clone", Incarnation: 2}
)

func timelineSpan(seq int, clone cloneTimelineIdentity, phase, outcome string, startMS, endMS int64) cloneTimelineSpan {
	span := cloneTimelineSpan{Seq: seq, Clone: clone, Phase: phase, Outcome: outcome, Finished: endMS > 0,
		StartMono: startMS * int64(time.Millisecond), StartWall: timelineBase.Add(time.Duration(startMS) * time.Millisecond)}
	if endMS > 0 {
		span.EndMono, span.EndWall = endMS*int64(time.Millisecond), timelineBase.Add(time.Duration(endMS)*time.Millisecond)
	}
	return span
}

func validTimeline() cloneTimelineReceipt {
	return cloneTimelineReceipt{
		Scenario: "synthetic",
		Clones: []cloneTimelineClone{
			{Identity: timelineFirst, Expect: []string{cloneTimelineSubmit, cloneTimelinePending, cloneTimelineTerminal}},
			{Identity: timelineNext, Expect: []string{cloneTimelineSubmit, cloneTimelineTerminal}},
		},
		Spans: []cloneTimelineSpan{
			timelineSpan(1, timelineFirst, cloneTimelineSubmit, "ok", 10, 20),
			timelineSpan(2, timelineFirst, cloneTimelinePending, "ok", 30, 40),
			timelineSpan(3, timelineFirst, cloneTimelineTerminal, "ok", 50, 60),
			timelineSpan(4, timelineNext, cloneTimelineSubmit, "ok", 70, 80),
			timelineSpan(5, timelineNext, cloneTimelineTerminal, "ok", 90, 100),
		},
	}
}

func requireProblem(t *testing.T, receipt cloneTimelineReceipt, want string) []cloneTimelineVerdict {
	t.Helper()
	verdicts, problems := judgeCloneTimeline(receipt)
	for _, problem := range problems {
		if strings.Contains(problem, want) {
			return verdicts
		}
	}
	t.Fatalf("no problem containing %q: %v", want, problems)
	return nil
}

func TestCloneTimelineAcceptsCompleteReceipt(t *testing.T) {
	verdicts, problems := judgeCloneTimeline(validTimeline())
	if len(problems) != 0 {
		t.Fatalf("problems = %v", problems)
	}
	for _, verdict := range verdicts {
		if verdict.Verdict != cloneTimelineVerdictComplete || len(verdict.Missing) != 0 {
			t.Fatalf("verdict = %+v", verdict)
		}
	}
}

func TestCloneTimelineRejectsZeroTimestamps(t *testing.T) {
	for name, edit := range map[string]func(*cloneTimelineSpan){
		"mono": func(s *cloneTimelineSpan) { s.StartMono = 0 },
		"wall": func(s *cloneTimelineSpan) { s.StartWall = time.Time{} },
		"end":  func(s *cloneTimelineSpan) { s.EndWall = time.Time{} },
	} {
		t.Run(name, func(t *testing.T) {
			receipt := validTimeline()
			edit(&receipt.Spans[0])
			if _, problems := judgeCloneTimeline(receipt); len(problems) == 0 {
				t.Fatal("zero timestamp accepted")
			}
		})
	}
}

func TestCloneTimelineRejectsDisorder(t *testing.T) {
	receipt := validTimeline()
	receipt.Spans[2].StartMono = receipt.Spans[1].StartMono // equal start is not strict
	requireProblem(t, receipt, "strictly after")

	receipt = validTimeline()
	receipt.Spans[1].StartMono = receipt.Spans[0].EndMono - 1 // overlaps the previous span
	requireProblem(t, receipt, "starts before span 1 ended")

	receipt = validTimeline()
	receipt.Spans[0].EndMono = receipt.Spans[0].StartMono - 1
	requireProblem(t, receipt, "ends before it starts")

	receipt = validTimeline()
	receipt.Spans[1], receipt.Spans[2] = receipt.Spans[2], receipt.Spans[1]
	receipt.Spans[1].Seq, receipt.Spans[2].Seq = 2, 3
	receipt.Spans[1].StartMono, receipt.Spans[2].StartMono = receipt.Spans[2].StartMono, receipt.Spans[1].StartMono
	receipt.Spans[1].StartWall, receipt.Spans[2].StartWall = receipt.Spans[2].StartWall, receipt.Spans[1].StartWall
	receipt.Spans[1].EndMono, receipt.Spans[2].EndMono = receipt.Spans[2].EndMono, receipt.Spans[1].EndMono
	receipt.Spans[1].EndWall, receipt.Spans[2].EndWall = receipt.Spans[2].EndWall, receipt.Spans[1].EndWall
	requireProblem(t, receipt, "out of profile order")
}

func TestCloneTimelineJoinsOnWholeIdentity(t *testing.T) {
	// A same-name replacement differs only in incarnation. Each field alone
	// must break the join to the declared clone.
	for name, edit := range map[string]func(*cloneTimelineIdentity){
		"incarnation": func(i *cloneTimelineIdentity) { i.Incarnation = 3 },
		"name":        func(i *cloneTimelineIdentity) { i.Name = "other" },
		"group":       func(i *cloneTimelineIdentity) { i.Group = "other" },
		"filesystem":  func(i *cloneTimelineIdentity) { i.Filesystem = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			receipt := validTimeline()
			edit(&receipt.Spans[0].Clone)
			verdicts := requireProblem(t, receipt, "undeclared clone")
			if verdicts[0].Verdict == cloneTimelineVerdictComplete || len(verdicts[0].Missing) == 0 {
				t.Fatalf("misjoined span counted for the declared clone: %+v", verdicts[0])
			}
		})
	}
	receipt := validTimeline()
	receipt.Clones[1].Identity = timelineFirst
	requireProblem(t, receipt, "declared twice")
}

func TestCloneTimelineKeepsFailedAndInconclusivePhases(t *testing.T) {
	receipt := validTimeline()
	receipt.Spans[1].Outcome = cloneTimelineOutcomeFailed
	receipt.Spans = receipt.Spans[:3] // later phases of the second clone were never reached
	verdicts, problems := judgeCloneTimeline(receipt)
	if len(problems) != 0 {
		t.Fatalf("problems = %v", problems)
	}
	if verdicts[0].Verdict != cloneTimelineOutcomeFailed {
		t.Fatalf("failed phase lost: %+v", verdicts[0])
	}
	if verdicts[1].Verdict != cloneTimelineOutcomeUnsure || len(verdicts[1].Missing) != 2 {
		t.Fatalf("unreached clone not inconclusive: %+v", verdicts[1])
	}

	receipt = validTimeline()
	receipt.Spans[2].Outcome = cloneTimelineOutcomeUnsure
	if verdicts, _ := judgeCloneTimeline(receipt); verdicts[0].Verdict != cloneTimelineOutcomeUnsure {
		t.Fatalf("inconclusive phase promoted: %+v", verdicts[0])
	}
}

func TestCloneTimelineNeverInfersAnUnknownPhase(t *testing.T) {
	receipt := validTimeline()
	receipt.Spans = append(receipt.Spans[:1:1], receipt.Spans[2:]...) // pending was never recorded
	for index := range receipt.Spans {
		receipt.Spans[index].Seq = index + 1
	}
	verdicts, problems := judgeCloneTimeline(receipt)
	if len(problems) != 0 {
		t.Fatalf("problems = %v", problems)
	}
	if verdicts[0].Verdict == cloneTimelineVerdictComplete || len(verdicts[0].Missing) != 1 || verdicts[0].Missing[0] != cloneTimelinePending {
		t.Fatalf("missing phase inferred from later phases: %+v", verdicts[0])
	}

	receipt = validTimeline()
	receipt.Spans[1].Phase = "invented"
	requireProblem(t, receipt, "unknown phase")

	receipt = validTimeline()
	receipt.Spans[1].Phase = cloneTimelineCleanup // known but outside this clone's profile
	requireProblem(t, receipt, "not in the profile")
}

func TestCloneTimelineUnfinishedSpanKeepsOutcome(t *testing.T) {
	receipt := validTimeline()
	receipt.Spans[2] = timelineSpan(3, timelineFirst, cloneTimelineTerminal, cloneTimelineOutcomeFailed, 50, 0)
	verdicts, problems := judgeCloneTimeline(receipt)
	if len(problems) != 0 {
		t.Fatalf("problems = %v", problems)
	}
	if verdicts[0].Verdict != cloneTimelineOutcomeFailed {
		t.Fatalf("unfinished failed span lost: %+v", verdicts[0])
	}
	receipt.Spans[2].Outcome = "ok"
	requireProblem(t, receipt, "unfinished but ok")
}

func TestCloneTimelineRecorderClosesUnfinishedSpans(t *testing.T) {
	for _, failed := range []bool{true, false} {
		timeline := newCloneTimeline("synthetic", cloneTimelineClone{Identity: timelineFirst, Expect: []string{cloneTimelineSubmit, cloneTimelinePending}})
		timeline.Begin(timelineFirst, cloneTimelineSubmit, "").Ok()
		timeline.Begin(timelineFirst, cloneTimelinePending, "") // never finished, as after a fatal check
		receipt := timeline.Receipt(failed)
		verdicts, problems := judgeCloneTimeline(receipt)
		if len(problems) != 0 {
			t.Fatalf("failed=%v problems = %v", failed, problems)
		}
		want := cloneTimelineOutcomeUnsure
		if failed {
			want = cloneTimelineOutcomeFailed
		}
		if receipt.Spans[1].Finished || receipt.Spans[1].Outcome != want || verdicts[0].Verdict != want {
			t.Fatalf("failed=%v span=%+v verdict=%+v", failed, receipt.Spans[1], verdicts[0])
		}
	}
}
