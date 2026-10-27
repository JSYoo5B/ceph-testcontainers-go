//go:build all || integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// The clone timeline receipt records when each phase of a CephFS clone scenario
// started and ended, for the two scenarios that hold a native clone worker
// delay. It only observes: it never changes a setting, an assertion or the
// order of operations, and the judge below runs on the receipt after the
// scenario, so a malformed receipt is logged and never fails a scenario.
//
// A phase is a span of the scenario for one clone incarnation. The receipt does
// not infer a phase that was not recorded: a missing phase stays missing, an
// unfinished span keeps its failed or inconclusive outcome, and a phase name
// outside the vocabulary is a shape problem.
//
// The configured worker delay is captured by a pending worker when it starts.
// Restoring the setting after a collision or a cancellation therefore does not
// start a new, shorter worker wait; the delay-restore span only measures how
// long the restore itself took.

const (
	cloneTimelineSubmit          = "submit"
	cloneTimelinePending         = "pending-protection"
	cloneTimelineCancelSetup     = "cancel-setup"
	cloneTimelineCollisionSetup  = "collision-setup"
	cloneTimelineDelayRestore    = "delay-restore"
	cloneTimelineTerminal        = "terminal-state"
	cloneTimelineSourceRelease   = "source-release"
	cloneTimelineDataCheck       = "data-check"
	cloneTimelineFinalDataCheck  = "final-data-check"
	cloneTimelineCleanup         = "cleanup"
	cloneTimelineOutcomeOK       = "ok"
	cloneTimelineOutcomeFailed   = "failed"
	cloneTimelineOutcomeUnsure   = "inconclusive"
	cloneTimelineVerdictComplete = "complete"
)

var cloneTimelineVocabulary = []string{
	cloneTimelineSubmit, cloneTimelinePending, cloneTimelineCancelSetup, cloneTimelineCollisionSetup,
	cloneTimelineDelayRestore, cloneTimelineTerminal, cloneTimelineSourceRelease, cloneTimelineDataCheck,
	cloneTimelineFinalDataCheck, cloneTimelineCleanup,
}

// cloneTimelineIdentity names one clone incarnation. A same-name replacement
// differs only in Incarnation, so every join compares the whole record.
type cloneTimelineIdentity struct {
	Filesystem  string `json:"filesystem"`
	Group       string `json:"group"`
	Name        string `json:"name"`
	Incarnation int    `json:"incarnation"`
}

// cloneTimelineClone declares one clone and the phases its profile expects, in
// the order they must occur.
type cloneTimelineClone struct {
	Identity cloneTimelineIdentity `json:"identity"`
	Expect   []string              `json:"expect"`
}

type cloneTimelineSpan struct {
	Seq       int                   `json:"seq"`
	Clone     cloneTimelineIdentity `json:"clone"`
	Phase     string                `json:"phase"`
	Outcome   string                `json:"outcome"`
	Detail    string                `json:"detail,omitempty"`
	Finished  bool                  `json:"finished"`
	StartMono int64                 `json:"start_mono_ns"`
	EndMono   int64                 `json:"end_mono_ns"`
	StartWall time.Time             `json:"start_wall"`
	EndWall   time.Time             `json:"end_wall"`
}

type cloneTimelineReceipt struct {
	Scenario string               `json:"scenario"`
	Clones   []cloneTimelineClone `json:"clones"`
	Spans    []cloneTimelineSpan  `json:"spans"`
}

type cloneTimelineVerdict struct {
	Clone   cloneTimelineIdentity `json:"clone"`
	Verdict string                `json:"verdict"`
	Missing []string              `json:"missing,omitempty"`
}

// cloneTimeline collects spans. Monotonic times are nanoseconds since the
// recorder was created, taken from the process monotonic clock.
type cloneTimeline struct {
	mu      sync.Mutex
	base    time.Time
	receipt cloneTimelineReceipt
	open    map[int]bool
}

type cloneTimelineHandle struct {
	timeline *cloneTimeline
	index    int
}

func newCloneTimeline(scenario string, clones ...cloneTimelineClone) *cloneTimeline {
	return &cloneTimeline{base: time.Now(), receipt: cloneTimelineReceipt{Scenario: scenario, Clones: clones}, open: map[int]bool{}}
}

// Begin starts a span. The span stays unfinished until End or Ok is called.
func (c *cloneTimeline) Begin(clone cloneTimelineIdentity, phase, detail string) *cloneTimelineHandle {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	c.receipt.Spans = append(c.receipt.Spans, cloneTimelineSpan{
		Seq: len(c.receipt.Spans) + 1, Clone: clone, Phase: phase, Detail: detail,
		StartMono: int64(now.Sub(c.base)), StartWall: now.UTC(),
	})
	index := len(c.receipt.Spans) - 1
	c.open[index] = true
	return &cloneTimelineHandle{timeline: c, index: index}
}

// End finishes a span with an outcome.
func (h *cloneTimelineHandle) End(outcome string) {
	c := h.timeline
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	span := &c.receipt.Spans[h.index]
	span.Outcome, span.Finished = outcome, true
	span.EndMono, span.EndWall = int64(now.Sub(c.base)), now.UTC()
	delete(c.open, h.index)
}

// Ok finishes a span whose checks all passed.
func (h *cloneTimelineHandle) Ok() { h.End(cloneTimelineOutcomeOK) }

// Receipt closes every unfinished span and returns a copy. An unfinished span
// has no end time. It is failed when the scenario failed, because a fatal check
// ended it, and inconclusive otherwise.
func (c *cloneTimeline) Receipt(scenarioFailed bool) cloneTimelineReceipt {
	c.mu.Lock()
	defer c.mu.Unlock()
	outcome := cloneTimelineOutcomeUnsure
	if scenarioFailed {
		outcome = cloneTimelineOutcomeFailed
	}
	for index := range c.open {
		c.receipt.Spans[index].Outcome = outcome
	}
	c.open = map[int]bool{}
	out := c.receipt
	out.Clones = slices.Clone(c.receipt.Clones)
	out.Spans = slices.Clone(c.receipt.Spans)
	return out
}

// Emit logs the receipt, the verdicts and any shape problem. It is meant for
// defer, so it runs on every outcome, before the test's own cleanup functions
// destroy the cluster. It never fails the test.
func (c *cloneTimeline) Emit(t *testing.T) {
	t.Helper()
	receipt := c.Receipt(t.Failed())
	encoded, err := json.Marshal(receipt)
	if err != nil {
		t.Logf("clone timeline receipt could not be encoded: %v", err)
		return
	}
	t.Logf("clone timeline receipt: %s", encoded)
	verdicts, problems := judgeCloneTimeline(receipt)
	for _, verdict := range verdicts {
		t.Logf("clone timeline verdict: %+v", verdict)
	}
	for _, problem := range problems {
		t.Logf("clone timeline receipt problem: %s", problem)
	}
}

// judgeCloneTimeline checks the receipt shape and returns one verdict per
// declared clone. Problems describe a malformed receipt; a verdict describes
// the scenario outcome the receipt records.
func judgeCloneTimeline(receipt cloneTimelineReceipt) ([]cloneTimelineVerdict, []string) {
	var problems []string
	problem := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	if receipt.Scenario == "" {
		problem("scenario is empty")
	}
	if len(receipt.Clones) == 0 {
		problem("no clone is declared")
	}
	declared := map[cloneTimelineIdentity]cloneTimelineClone{}
	for _, clone := range receipt.Clones {
		identity := clone.Identity
		if identity.Filesystem == "" || identity.Name == "" || identity.Incarnation < 1 {
			problem("clone identity %+v lacks filesystem, name or a positive incarnation", identity)
		}
		if _, dup := declared[identity]; dup {
			problem("clone identity %+v is declared twice", identity)
		}
		declared[identity] = clone
		seen := map[string]bool{}
		for _, phase := range clone.Expect {
			if !slices.Contains(cloneTimelineVocabulary, phase) {
				problem("clone %+v expects unknown phase %q", identity, phase)
			}
			if seen[phase] {
				problem("clone %+v expects phase %q twice", identity, phase)
			}
			seen[phase] = true
		}
	}

	spansOf := map[cloneTimelineIdentity][]cloneTimelineSpan{}
	var previous *cloneTimelineSpan
	for index := range receipt.Spans {
		span := receipt.Spans[index]
		if span.Seq != index+1 {
			problem("span %d has sequence %d", index+1, span.Seq)
		}
		clone, known := declared[span.Clone]
		switch {
		case !known:
			problem("span %d names undeclared clone %+v", span.Seq, span.Clone)
		case !slices.Contains(cloneTimelineVocabulary, span.Phase):
			problem("span %d has unknown phase %q", span.Seq, span.Phase)
		case !slices.Contains(clone.Expect, span.Phase):
			problem("span %d phase %q is not in the profile of clone %+v", span.Seq, span.Phase, span.Clone)
		default:
			spansOf[span.Clone] = append(spansOf[span.Clone], span)
		}
		if !slices.Contains([]string{cloneTimelineOutcomeOK, cloneTimelineOutcomeFailed, cloneTimelineOutcomeUnsure}, span.Outcome) {
			problem("span %d has outcome %q", span.Seq, span.Outcome)
		}
		if span.StartMono <= 0 || span.StartWall.IsZero() {
			problem("span %d has a zero start time", span.Seq)
		}
		if span.Finished {
			if span.EndMono <= 0 || span.EndWall.IsZero() {
				problem("span %d is finished with a zero end time", span.Seq)
			}
			if span.EndMono < span.StartMono || span.EndWall.Before(span.StartWall) {
				problem("span %d ends before it starts", span.Seq)
			}
		} else {
			if span.Outcome == cloneTimelineOutcomeOK {
				problem("span %d is unfinished but ok", span.Seq)
			}
			if span.EndMono != 0 || !span.EndWall.IsZero() {
				problem("span %d is unfinished but has an end time", span.Seq)
			}
		}
		if previous != nil {
			if span.StartMono <= previous.StartMono || span.StartWall.Before(previous.StartWall) {
				problem("span %d does not start strictly after span %d", span.Seq, previous.Seq)
			}
			if previous.Finished && span.StartMono < previous.EndMono {
				problem("span %d starts before span %d ended", span.Seq, previous.Seq)
			}
		}
		previous = &receipt.Spans[index]
	}

	var verdicts []cloneTimelineVerdict
	for _, clone := range receipt.Clones {
		spans := spansOf[clone.Identity]
		verdict := cloneTimelineVerdict{Clone: clone.Identity, Verdict: cloneTimelineVerdictComplete}
		position, failed := 0, false
		for _, span := range spans {
			next := slices.Index(clone.Expect, span.Phase)
			if next < position {
				problem("clone %+v phase %q is recorded out of profile order or twice", clone.Identity, span.Phase)
			}
			position = max(position, next+1)
			if span.Outcome == cloneTimelineOutcomeFailed {
				failed = true
			}
		}
		for _, phase := range clone.Expect {
			recorded := slices.ContainsFunc(spans, func(s cloneTimelineSpan) bool { return s.Phase == phase })
			if !recorded {
				verdict.Missing = append(verdict.Missing, phase)
			}
		}
		switch {
		case failed:
			verdict.Verdict = cloneTimelineOutcomeFailed
		case len(verdict.Missing) > 0 || slices.ContainsFunc(spans, func(s cloneTimelineSpan) bool { return s.Outcome != cloneTimelineOutcomeOK }):
			verdict.Verdict = cloneTimelineOutcomeUnsure
		}
		verdicts = append(verdicts, verdict)
	}
	if len(problems) > 0 {
		slices.Sort(problems)
	}
	return verdicts, problems
}

func (v cloneTimelineVerdict) String() string {
	return fmt.Sprintf("%s/%s/%s#%d=%s missing=[%s]", v.Clone.Filesystem, v.Clone.Group, v.Clone.Name, v.Clone.Incarnation, v.Verdict, strings.Join(v.Missing, ","))
}
