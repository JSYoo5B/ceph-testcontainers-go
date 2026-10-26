package ceph

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

// This file records, for one verification over the fake engine, which source
// generation each controlled event belongs to and what the verifier does when
// that generation restarts, is removed, is replaced by another container or
// logs through a rotating driver. It characterizes the current behavior and
// does not change it: the verifier covers only the generation it selected at
// activation, and reports every other case as a gap instead of reading it.
//
// The fake engine is a code-path fixture. It keeps one log and one crash
// directory per container id, as Docker does across a restart of the same
// container, and tags each emitted event with the generation that a real
// daemon would have produced it in. Nothing here shows what a real restart,
// removal or rotation leaves readable, or the timing of the engine's own
// bookkeeping.

// genState is the identity of one container generation as the engine
// reports it.
type genState struct {
	ContainerID  string
	StartedAt    string
	RestartCount int
	// Present is false when the engine no longer knows the container.
	Present bool
}

// genEvent is a controlled event. Generation names the generation in which
// the fixture produced it: "first" is the one selected at activation,
// "second" is the same container after a restart, and "replacement" is a
// different container.
type genEvent struct {
	ID         string
	Kind       string // "log" or "crash"
	Container  string
	Generation string
}

// genLedger is the per-scenario record. Received lists are the event ids
// that the report carries; gaps are kept per check.
type genLedger struct {
	Scenario          string
	Baseline          genState
	Evaluated         genState
	Events            []genEvent
	LogsReceived      []string
	CrashesReceived   []string
	LogGaps           []string
	CrashGaps         []string
	LogsOutcome       VerificationOutcome
	CrashesOutcome    VerificationOutcome
	ReportOutcome     VerificationOutcome
	SourceLogBytes    int64
	SourceStartedAt   string
	SourceRestarts    int
	SourceContainerID string
}

// judgeGenerationFullCoverage accepts only a ledger that supports a full
// interval claim for the activation's generation: the same identified
// container generation at both ends, no gap in either check, a clean report
// and every first-generation event received exactly once with no event of
// any other generation received. Missing identity or evidence is an error.
func judgeGenerationFullCoverage(l genLedger) error {
	var problems []string
	bad := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	b, e := l.Baseline, l.Evaluated
	if b.ContainerID == "" || b.StartedAt == "" || !b.Present {
		bad("the activation generation is not identified: %+v", b)
	}
	if !e.Present || e.ContainerID != b.ContainerID || e.StartedAt != b.StartedAt || e.RestartCount != b.RestartCount {
		bad("the evaluated generation %+v is not the activation's %+v", e, b)
	}
	if l.SourceContainerID != b.ContainerID || l.SourceStartedAt != b.StartedAt || l.SourceRestarts != b.RestartCount {
		bad("the report's source (%q, %q, %d) is not the activation generation", l.SourceContainerID, l.SourceStartedAt, l.SourceRestarts)
	}
	if len(l.LogGaps) > 0 || len(l.CrashGaps) > 0 {
		bad("gaps: logs %v, crashes %v", l.LogGaps, l.CrashGaps)
	}
	if l.LogsOutcome == VerificationIncomplete || l.CrashesOutcome == VerificationIncomplete || l.ReportOutcome == VerificationIncomplete {
		bad("an incomplete check: %s/%s/%s", l.LogsOutcome, l.CrashesOutcome, l.ReportOutcome)
	}
	if l.SourceLogBytes <= 0 {
		bad("no log bytes were read")
	}
	first := 0
	for _, ev := range l.Events {
		if ev.ID == "" || ev.Container == "" || (ev.Kind != "log" && ev.Kind != "crash") || ev.Generation == "" {
			bad("incomplete event %+v", ev)
			continue
		}
		received := l.LogsReceived
		if ev.Kind == "crash" {
			received = l.CrashesReceived
		}
		n := countEqual(received, ev.ID)
		switch {
		case ev.Generation == "first" && n != 1:
			bad("first-generation event %s was received %d times", ev.ID, n)
		case ev.Generation != "first" && n > 0:
			bad("event %s of the %s generation was received", ev.ID, ev.Generation)
		}
		if ev.Generation == "first" {
			first++
		}
	}
	if first == 0 {
		bad("no first-generation event")
	}
	for kind, ids := range map[string][]string{"log": l.LogsReceived, "crash": l.CrashesReceived} {
		for _, id := range ids {
			if !slices.ContainsFunc(l.Events, func(ev genEvent) bool { return ev.ID == id && ev.Kind == kind }) {
				bad("%s %q was received but never emitted", kind, id)
			}
		}
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

// judgeNoSilentLoss accepts a ledger only when every emitted event that was
// not received is accounted for by a gap in the same check and that check is
// not clean. This is the invariant the verifier must keep for every
// generation case: lost coverage is incomplete, never a pass.
func judgeNoSilentLoss(l genLedger) error {
	var problems []string
	bad := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	for _, ev := range l.Events {
		received, gaps, outcome := l.LogsReceived, l.LogGaps, l.LogsOutcome
		if ev.Kind == "crash" {
			received, gaps, outcome = l.CrashesReceived, l.CrashGaps, l.CrashesOutcome
		}
		if countEqual(received, ev.ID) > 0 {
			continue
		}
		if len(gaps) == 0 {
			bad("%s event %s (%s generation) is missing without a gap", ev.Kind, ev.ID, ev.Generation)
		}
		if outcome != VerificationIncomplete && outcome != VerificationUnexpected {
			bad("%s check is %q although event %s (%s generation) is missing", ev.Kind, outcome, ev.ID, ev.Generation)
		}
		if l.ReportOutcome == VerificationClean {
			bad("the report is clean although event %s is missing", ev.ID)
		}
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

func genStateOf(docker *fakeVerifyDocker, id string) genState {
	ctr := docker.containers[id]
	if ctr == nil || ctr.removed {
		return genState{ContainerID: id}
	}
	return genState{ContainerID: id, StartedAt: ctr.startedAt, RestartCount: ctr.restarts, Present: true}
}

// genScenario changes the fixture after the activation. emitFirst events
// exist before it; the scenario adds the later events through the helpers.
type genScenario struct {
	name string
	// mutate runs between the first-generation events and the evaluation.
	mutate func(fx *verifyFixture, mon *fakeVerifyContainer, emit *genEmitter)
}

// genEmitter adds controlled events to a container of the fixture.
type genEmitter struct {
	fx     *verifyFixture
	events []genEvent
}

func (g *genEmitter) log(container, id, generation string, at time.Duration) {
	ctr := g.fx.docker.containers[container]
	ctr.lines = append(ctr.lines, fakeLogLine{verifyStart.Add(at), daemonLogLine("cluster", "WRN", "ledger-id="+id)})
	g.events = append(g.events, genEvent{ID: id, Kind: "log", Container: container, Generation: generation})
}

func (g *genEmitter) crash(container, id, generation string) {
	ctr := g.fx.docker.containers[container]
	ctr.crashes = append(ctr.crashes, id)
	g.events = append(g.events, genEvent{ID: id, Kind: "crash", Container: container, Generation: generation})
}

const genMonCrash = "mon"

// runGenerationScenario activates a verification over the fixture, emits one
// log line and one crash in the first generation of the mon and of osd1,
// applies the scenario and evaluates.
func runGenerationScenario(t *testing.T, sc genScenario) genLedger {
	t.Helper()
	fx := newVerifyFixture()
	fx.config.Logs = []LogExpectation{{Name: "ledger-sentinel", Daemon: "mon.a", Event: LogClusterWarning, Pattern: `ledger-id=`}}
	fx.config.Crashes = []CrashExpectation{{Name: "ledger-crash", Daemon: "mon.a"}}
	v := fx.activate(t)
	mon := fx.docker.containers[genMonCrash]
	baseline := genStateOf(fx.docker, genMonCrash)

	emit := &genEmitter{fx: fx}
	emit.log(genMonCrash, "log-first", "first", time.Minute)
	emit.crash(genMonCrash, "2026-10-23T01:02:00Z_first", "first")
	if sc.mutate != nil {
		sc.mutate(fx, mon, emit)
	}
	evaluated := genStateOf(fx.docker, genMonCrash)
	report := fx.evaluate(t, v)

	l := genLedger{Scenario: sc.name, Baseline: baseline, Evaluated: evaluated, Events: emit.events,
		LogsOutcome: report.Logs.Outcome, CrashesOutcome: report.Crashes.Outcome, ReportOutcome: report.Outcome}
	for _, finding := range report.Logs.Findings {
		if m := ledgerIDText.FindStringSubmatch(finding.Text); m != nil {
			l.LogsReceived = append(l.LogsReceived, m[1])
		}
	}
	for _, finding := range report.Crashes.Findings {
		l.CrashesReceived = append(l.CrashesReceived, finding.CrashID)
	}
	for _, gap := range report.Logs.Gaps {
		l.LogGaps = append(l.LogGaps, gap.Source+": "+gap.Reason)
	}
	for _, gap := range report.Crashes.Gaps {
		l.CrashGaps = append(l.CrashGaps, gap.Source+": "+gap.Reason)
	}
	for _, source := range report.Sources {
		if source.ContainerID == genMonCrash {
			l.SourceContainerID, l.SourceStartedAt, l.SourceRestarts, l.SourceLogBytes = source.ContainerID, source.StartedAt, source.RestartCount, source.LogBytes
		}
	}
	return l
}

func restart(mon *fakeVerifyContainer, byStart, byCount bool) {
	if byStart {
		mon.startedAt = verifyStart.Add(2 * time.Minute).Format(time.RFC3339Nano)
	}
	if byCount {
		mon.restarts++
	}
}

func hasGap(gaps []string, text string) bool {
	return slices.ContainsFunc(gaps, func(g string) bool { return strings.Contains(g, text) })
}

// The control is one unchanged generation: both first-generation events are
// received once, nothing else, and the ledger supports a full-interval
// claim for that generation only.
func TestVerificationGenerationLedgerContinuousControl(t *testing.T) {
	l := runGenerationScenario(t, genScenario{name: "continuous"})
	if err := judgeGenerationFullCoverage(l); err != nil {
		t.Fatalf("%v\nledger %+v", err, l)
	}
	if err := judgeNoSilentLoss(l); err != nil {
		t.Fatal(err)
	}
	if l.ReportOutcome == VerificationIncomplete || !slices.Equal(l.LogsReceived, []string{"log-first"}) {
		t.Fatalf("ledger %+v", l)
	}
}

// A restart of the same container id (detected by a new start time, by a
// new restart count, or by both) stops the log read: the first-generation
// line is no longer received either, and later lines are not read. Crashes
// stay in the container's writable layer, so both generations' crashes are
// received, but the finding carries no generation and a restart gap marks
// the check incomplete.
func TestVerificationGenerationLedgerRestart(t *testing.T) {
	for _, tc := range []struct {
		name          string
		byStart, byCt bool
	}{{"new start time", true, false}, {"new restart count", false, true}, {"both", true, true}} {
		t.Run(tc.name, func(t *testing.T) {
			l := runGenerationScenario(t, genScenario{name: "restart", mutate: func(_ *verifyFixture, mon *fakeVerifyContainer, emit *genEmitter) {
				restart(mon, tc.byStart, tc.byCt)
				emit.log(genMonCrash, "log-second", "second", 3*time.Minute)
				emit.crash(genMonCrash, "2026-10-23T01:04:00Z_second", "second")
			}})
			if err := judgeNoSilentLoss(l); err != nil {
				t.Fatalf("%v\nledger %+v", err, l)
			}
			if judgeGenerationFullCoverage(l) == nil {
				t.Fatalf("a restarted generation supported a full-interval claim: %+v", l)
			}
			if len(l.LogsReceived) != 0 || l.SourceLogBytes != 0 || !hasGap(l.LogGaps, "restarted since the activation") || l.LogsOutcome != VerificationIncomplete {
				t.Fatalf("logs: received %v, bytes %d, gaps %v, %s", l.LogsReceived, l.SourceLogBytes, l.LogGaps, l.LogsOutcome)
			}
			if !hasGap(l.CrashGaps, "restarted since the activation") || l.CrashesOutcome != VerificationIncomplete ||
				countEqual(l.CrashesReceived, "2026-10-23T01:02:00Z_first") != 1 || countEqual(l.CrashesReceived, "2026-10-23T01:04:00Z_second") != 1 {
				t.Fatalf("crashes: received %v, gaps %v, %s", l.CrashesReceived, l.CrashGaps, l.CrashesOutcome)
			}
			// The source keeps the activation's generation, not the new one.
			if l.SourceStartedAt != l.Baseline.StartedAt || l.SourceRestarts != l.Baseline.RestartCount || l.Evaluated == l.Baseline {
				t.Fatalf("source %q/%d, baseline %+v, evaluated %+v", l.SourceStartedAt, l.SourceRestarts, l.Baseline, l.Evaluated)
			}
		})
	}
}

// A removed container leaves a gap in both checks and nothing is received
// from it, including the events that were readable before the removal.
func TestVerificationGenerationLedgerRemoval(t *testing.T) {
	l := runGenerationScenario(t, genScenario{name: "removal", mutate: func(_ *verifyFixture, mon *fakeVerifyContainer, _ *genEmitter) {
		mon.removed = true
	}})
	if err := judgeNoSilentLoss(l); err != nil {
		t.Fatalf("%v\nledger %+v", err, l)
	}
	if judgeGenerationFullCoverage(l) == nil || l.Evaluated.Present {
		t.Fatalf("a removed generation supported a full-interval claim: %+v", l)
	}
	if len(l.LogsReceived) != 0 || len(l.CrashesReceived) != 0 || !hasGap(l.LogGaps, "the container was removed") || !hasGap(l.CrashGaps, "the container was removed") {
		t.Fatalf("ledger %+v", l)
	}
}

// A replacement container under a new id is not the selected generation:
// the old id is a removal gap, the new one is reported as added after the
// activation, and neither its log nor its crash is read.
func TestVerificationGenerationLedgerReplacement(t *testing.T) {
	l := runGenerationScenario(t, genScenario{name: "replacement", mutate: func(fx *verifyFixture, mon *fakeVerifyContainer, emit *genEmitter) {
		mon.removed = true
		fx.docker.containers["mon2"] = &fakeVerifyContainer{startedAt: verifyStart.Add(2 * time.Minute).Format(time.RFC3339Nano), process: "ceph-mon"}
		for i, c := range fx.snapshot.containers {
			if c.Role == "mon" {
				fx.snapshot.containers[i] = crashContainer("mon", "a", "mon2")
			}
		}
		emit.log("mon2", "log-replacement", "replacement", 3*time.Minute)
		emit.crash("mon2", "2026-10-23T01:04:00Z_replacement", "replacement")
	}})
	if err := judgeNoSilentLoss(l); err != nil {
		t.Fatalf("%v\nledger %+v", err, l)
	}
	if judgeGenerationFullCoverage(l) == nil {
		t.Fatalf("a replaced generation supported a full-interval claim: %+v", l)
	}
	if len(l.LogsReceived) != 0 || len(l.CrashesReceived) != 0 || !hasGap(l.LogGaps, "the container was removed") || !hasGap(l.LogGaps, "added after the activation") ||
		!hasGap(l.CrashGaps, "added after the activation") {
		t.Fatalf("ledger %+v", l)
	}
}

// A rotating json-file configuration is a refusal: no line of the window is
// read, so the first-generation line is missing with a log gap. The crash
// check is independent and still receives the crash.
func TestVerificationGenerationLedgerRotation(t *testing.T) {
	l := runGenerationScenario(t, genScenario{name: "rotation", mutate: func(_ *verifyFixture, mon *fakeVerifyContainer, _ *genEmitter) {
		mon.maxSize = "10m"
	}})
	if err := judgeNoSilentLoss(l); err != nil {
		t.Fatalf("%v\nledger %+v", err, l)
	}
	if judgeGenerationFullCoverage(l) == nil || len(l.LogsReceived) != 0 || l.SourceLogBytes != 0 ||
		!hasGap(l.LogGaps, "rotates at max-size 10m") || countEqual(l.CrashesReceived, "2026-10-23T01:02:00Z_first") != 1 {
		t.Fatalf("ledger %+v", l)
	}
}

// Each weakened ledger must be refused: the judges never turn missing,
// empty or contradicting evidence into coverage or into an accounted loss.
func TestVerificationGenerationLedgerJudgeControls(t *testing.T) {
	base := runGenerationScenario(t, genScenario{name: "continuous"})
	if err := judgeGenerationFullCoverage(base); err != nil {
		t.Fatal(err)
	}
	removed := runGenerationScenario(t, genScenario{name: "removal", mutate: func(_ *verifyFixture, mon *fakeVerifyContainer, emit *genEmitter) {
		mon.removed = true
		emit.log(genMonCrash, "log-second", "second", 3*time.Minute)
	}})
	if err := judgeNoSilentLoss(removed); err != nil {
		t.Fatal(err)
	}
	clone := func(l genLedger, mutate func(*genLedger)) genLedger {
		l.Events = slices.Clone(l.Events)
		l.LogsReceived, l.CrashesReceived = slices.Clone(l.LogsReceived), slices.Clone(l.CrashesReceived)
		l.LogGaps, l.CrashGaps = slices.Clone(l.LogGaps), slices.Clone(l.CrashGaps)
		mutate(&l)
		return l
	}
	full := map[string]func(*genLedger){
		"no baseline id":           func(l *genLedger) { l.Baseline.ContainerID = "" },
		"no baseline start":        func(l *genLedger) { l.Baseline.StartedAt = "" },
		"baseline absent":          func(l *genLedger) { l.Baseline.Present = false },
		"evaluated absent":         func(l *genLedger) { l.Evaluated.Present = false },
		"evaluated restarted":      func(l *genLedger) { l.Evaluated.RestartCount++ },
		"evaluated started later":  func(l *genLedger) { l.Evaluated.StartedAt += "x" },
		"evaluated other id":       func(l *genLedger) { l.Evaluated.ContainerID = "other" },
		"source other id":          func(l *genLedger) { l.SourceContainerID = "other" },
		"source other start":       func(l *genLedger) { l.SourceStartedAt = "" },
		"source other restarts":    func(l *genLedger) { l.SourceRestarts++ },
		"log gap":                  func(l *genLedger) { l.LogGaps = []string{"mon.a: gap"} },
		"crash gap":                func(l *genLedger) { l.CrashGaps = []string{"mon.a: gap"} },
		"incomplete logs":          func(l *genLedger) { l.LogsOutcome = VerificationIncomplete },
		"incomplete crashes":       func(l *genLedger) { l.CrashesOutcome = VerificationIncomplete },
		"incomplete report":        func(l *genLedger) { l.ReportOutcome = VerificationIncomplete },
		"no log bytes":             func(l *genLedger) { l.SourceLogBytes = 0 },
		"no events":                func(l *genLedger) { l.Events = nil },
		"event without id":         func(l *genLedger) { l.Events[0].ID = "" },
		"event without container":  func(l *genLedger) { l.Events[0].Container = "" },
		"event without generation": func(l *genLedger) { l.Events[0].Generation = "" },
		"event of unknown kind":    func(l *genLedger) { l.Events[0].Kind = "" },
		"first log lost":           func(l *genLedger) { l.LogsReceived = nil },
		"first crash lost":         func(l *genLedger) { l.CrashesReceived = nil },
		"duplicate log":            func(l *genLedger) { l.LogsReceived = repeatInPlace(l.LogsReceived, "log-first") },
		"duplicate crash":          func(l *genLedger) { l.CrashesReceived = repeatInPlace(l.CrashesReceived, "2026-10-23T01:02:00Z_first") },
		"unemitted log":            func(l *genLedger) { l.LogsReceived = append(l.LogsReceived, "log-ghost") },
		"unemitted crash":          func(l *genLedger) { l.CrashesReceived = append(l.CrashesReceived, "2026-10-23T01:09:00Z_ghost") },
		"later generation event received": func(l *genLedger) {
			l.Events = append(l.Events, genEvent{ID: "log-later", Kind: "log", Container: "mon", Generation: "second"})
			l.LogsReceived = append(l.LogsReceived, "log-later")
		},
		"only later-generation events": func(l *genLedger) {
			for i := range l.Events {
				l.Events[i].Generation = "second"
			}
			l.LogsReceived, l.CrashesReceived = nil, nil
		},
	}
	for name, mutate := range full {
		t.Run("full/"+name, func(t *testing.T) {
			if judgeGenerationFullCoverage(clone(base, mutate)) == nil {
				t.Fatal("a weakened ledger supported a full-interval claim")
			}
		})
	}
	loss := map[string]func(*genLedger){
		"log gap removed":     func(l *genLedger) { l.LogGaps = nil },
		"crash gap removed":   func(l *genLedger) { l.CrashGaps = nil },
		"log check clean":     func(l *genLedger) { l.LogsOutcome = VerificationClean },
		"crash check clean":   func(l *genLedger) { l.CrashesOutcome = VerificationClean },
		"log outcome empty":   func(l *genLedger) { l.LogsOutcome = "" },
		"crash outcome empty": func(l *genLedger) { l.CrashesOutcome = "" },
		"report clean":        func(l *genLedger) { l.ReportOutcome = VerificationClean },
		"no gaps and all clean": func(l *genLedger) {
			l.LogGaps, l.CrashGaps = nil, nil
			l.LogsOutcome, l.CrashesOutcome, l.ReportOutcome = VerificationClean, VerificationClean, VerificationClean
		},
	}
	for name, mutate := range loss {
		t.Run("loss/"+name, func(t *testing.T) {
			if judgeNoSilentLoss(clone(removed, mutate)) == nil {
				t.Fatal("a silent loss was accepted")
			}
		})
	}
}
