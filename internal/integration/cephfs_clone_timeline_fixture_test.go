//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// These controls judge synthetic clone timeline receipts. They test the
// receipt shape and verdicts only; they do not run a clone or show any native
// timing.

var (
	timelineBase  = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	timelineFirst = harness.CloneTimelineIdentity{Filesystem: "fs", Group: "grp", Name: "clone", Incarnation: 1}
	timelineNext  = harness.CloneTimelineIdentity{Filesystem: "fs", Group: "grp", Name: "clone", Incarnation: 2}
)

func timelineSpan(seq int, clone harness.CloneTimelineIdentity, phase, outcome string, startMS, endMS int64) harness.CloneTimelineSpan {
	span := harness.CloneTimelineSpan{Seq: seq, Clone: clone, Phase: phase, Outcome: outcome, Finished: endMS > 0,
		StartMono: startMS * int64(time.Millisecond), StartWall: timelineBase.Add(time.Duration(startMS) * time.Millisecond)}
	if endMS > 0 {
		span.EndMono, span.EndWall = endMS*int64(time.Millisecond), timelineBase.Add(time.Duration(endMS)*time.Millisecond)
	}
	return span
}

func validTimeline() harness.CloneTimelineReceipt {
	return harness.CloneTimelineReceipt{
		Scenario: "synthetic",
		Clones: []harness.CloneTimelineClone{
			{Identity: timelineFirst, Expect: []string{harness.CloneTimelineSubmit, harness.CloneTimelinePending, harness.CloneTimelineTerminal}},
			{Identity: timelineNext, Expect: []string{harness.CloneTimelineSubmit, harness.CloneTimelineTerminal}},
		},
		Spans: []harness.CloneTimelineSpan{
			timelineSpan(1, timelineFirst, harness.CloneTimelineSubmit, "ok", 10, 20),
			timelineSpan(2, timelineFirst, harness.CloneTimelinePending, "ok", 30, 40),
			timelineSpan(3, timelineFirst, harness.CloneTimelineTerminal, "ok", 50, 60),
			timelineSpan(4, timelineNext, harness.CloneTimelineSubmit, "ok", 70, 80),
			timelineSpan(5, timelineNext, harness.CloneTimelineTerminal, "ok", 90, 100),
		},
	}
}

func requireProblem(t *testing.T, receipt harness.CloneTimelineReceipt, want string) []harness.CloneTimelineVerdict {
	t.Helper()
	verdicts, problems := harness.JudgeCloneTimeline(receipt)
	for _, problem := range problems {
		if strings.Contains(problem, want) {
			return verdicts
		}
	}
	t.Fatalf("no problem containing %q: %v", want, problems)
	return nil
}

func TestCloneTimelineAcceptsCompleteReceipt(t *testing.T) {
	verdicts, problems := harness.JudgeCloneTimeline(validTimeline())
	if len(problems) != 0 {
		t.Fatalf("problems = %v", problems)
	}
	for _, verdict := range verdicts {
		if verdict.Verdict != harness.CloneTimelineVerdictComplete || len(verdict.Missing) != 0 {
			t.Fatalf("verdict = %+v", verdict)
		}
	}
}

func TestCloneTimelineRejectsZeroTimestamps(t *testing.T) {
	for name, edit := range map[string]func(*harness.CloneTimelineSpan){
		"mono": func(s *harness.CloneTimelineSpan) { s.StartMono = 0 },
		"wall": func(s *harness.CloneTimelineSpan) { s.StartWall = time.Time{} },
		"end":  func(s *harness.CloneTimelineSpan) { s.EndWall = time.Time{} },
	} {
		t.Run(name, func(t *testing.T) {
			receipt := validTimeline()
			edit(&receipt.Spans[0])
			if _, problems := harness.JudgeCloneTimeline(receipt); len(problems) == 0 {
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
	for name, edit := range map[string]func(*harness.CloneTimelineIdentity){
		"incarnation": func(i *harness.CloneTimelineIdentity) { i.Incarnation = 3 },
		"name":        func(i *harness.CloneTimelineIdentity) { i.Name = "other" },
		"group":       func(i *harness.CloneTimelineIdentity) { i.Group = "other" },
		"filesystem":  func(i *harness.CloneTimelineIdentity) { i.Filesystem = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			receipt := validTimeline()
			edit(&receipt.Spans[0].Clone)
			verdicts := requireProblem(t, receipt, "undeclared clone")
			if verdicts[0].Verdict == harness.CloneTimelineVerdictComplete || len(verdicts[0].Missing) == 0 {
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
	receipt.Spans[1].Outcome = harness.CloneTimelineOutcomeFailed
	receipt.Spans = receipt.Spans[:3] // later phases of the second clone were never reached
	verdicts, problems := harness.JudgeCloneTimeline(receipt)
	if len(problems) != 0 {
		t.Fatalf("problems = %v", problems)
	}
	if verdicts[0].Verdict != harness.CloneTimelineOutcomeFailed {
		t.Fatalf("failed phase lost: %+v", verdicts[0])
	}
	if verdicts[1].Verdict != harness.CloneTimelineOutcomeUnsure || len(verdicts[1].Missing) != 2 {
		t.Fatalf("unreached clone not inconclusive: %+v", verdicts[1])
	}

	receipt = validTimeline()
	receipt.Spans[2].Outcome = harness.CloneTimelineOutcomeUnsure
	if verdicts, _ := harness.JudgeCloneTimeline(receipt); verdicts[0].Verdict != harness.CloneTimelineOutcomeUnsure {
		t.Fatalf("inconclusive phase promoted: %+v", verdicts[0])
	}
}

func TestCloneTimelineNeverInfersAnUnknownPhase(t *testing.T) {
	receipt := validTimeline()
	receipt.Spans = append(receipt.Spans[:1:1], receipt.Spans[2:]...) // pending was never recorded
	for index := range receipt.Spans {
		receipt.Spans[index].Seq = index + 1
	}
	verdicts, problems := harness.JudgeCloneTimeline(receipt)
	if len(problems) != 0 {
		t.Fatalf("problems = %v", problems)
	}
	if verdicts[0].Verdict == harness.CloneTimelineVerdictComplete || len(verdicts[0].Missing) != 1 || verdicts[0].Missing[0] != harness.CloneTimelinePending {
		t.Fatalf("missing phase inferred from later phases: %+v", verdicts[0])
	}

	receipt = validTimeline()
	receipt.Spans[1].Phase = "invented"
	requireProblem(t, receipt, "unknown phase")

	receipt = validTimeline()
	receipt.Spans[1].Phase = harness.CloneTimelineCleanup // known but outside this clone's profile
	requireProblem(t, receipt, "not in the profile")
}

func TestCloneTimelineUnfinishedSpanKeepsOutcome(t *testing.T) {
	receipt := validTimeline()
	receipt.Spans[2] = timelineSpan(3, timelineFirst, harness.CloneTimelineTerminal, harness.CloneTimelineOutcomeFailed, 50, 0)
	verdicts, problems := harness.JudgeCloneTimeline(receipt)
	if len(problems) != 0 {
		t.Fatalf("problems = %v", problems)
	}
	if verdicts[0].Verdict != harness.CloneTimelineOutcomeFailed {
		t.Fatalf("unfinished failed span lost: %+v", verdicts[0])
	}
	receipt.Spans[2].Outcome = "ok"
	requireProblem(t, receipt, "unfinished but ok")
}

func TestCloneTimelineRecorderClosesUnfinishedSpans(t *testing.T) {
	for _, failed := range []bool{true, false} {
		timeline := harness.NewCloneTimeline("synthetic", harness.CloneTimelineClone{Identity: timelineFirst, Expect: []string{harness.CloneTimelineSubmit, harness.CloneTimelinePending}})
		timeline.Begin(timelineFirst, harness.CloneTimelineSubmit, "").Ok()
		timeline.Begin(timelineFirst, harness.CloneTimelinePending, "") // never finished, as after a fatal check
		receipt := timeline.Receipt(failed)
		verdicts, problems := harness.JudgeCloneTimeline(receipt)
		if len(problems) != 0 {
			t.Fatalf("failed=%v problems = %v", failed, problems)
		}
		want := harness.CloneTimelineOutcomeUnsure
		if failed {
			want = harness.CloneTimelineOutcomeFailed
		}
		if receipt.Spans[1].Finished || receipt.Spans[1].Outcome != want || verdicts[0].Verdict != want {
			t.Fatalf("failed=%v span=%+v verdict=%+v", failed, receipt.Spans[1], verdicts[0])
		}
	}
}
