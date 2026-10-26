package ceph

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	mobycl "github.com/moby/moby/client"
)

// This file records, for one verification over the fake engine, which rule
// selected each event, the engine interval, the order in which the engine
// was asked and which controlled events were emitted, received or excluded.
// It characterizes the current selection behavior (logs by the engine-clock
// window, crashes by new raw identity without a metadata timestamp filter)
// and does not change it. The fake engine is a code-path fixture: its stamps
// and metadata timestamps are synthetic, so nothing here shows when a real
// daemon wrote an event or that the engine's and a daemon's clocks agree.

// ledgerCall is one request that verification made of the fake engine.
type ledgerCall struct {
	Seq    int
	Op     string // "info", "logs" or "crash"
	Target string
	// Clock is the time the engine reported (info only).
	Clock        time.Time
	Since, Until string // as requested (logs only)
}

// tracedDocker numbers the engine calls and runs afterInfo after each clock
// reading, which is how a controlled event is released at an exact point of
// the evaluation.
type tracedDocker struct {
	*fakeVerifyDocker
	mu        sync.Mutex
	calls     []ledgerCall
	infos     int
	afterInfo func(infoCount, seq int)
}

func (d *tracedDocker) record(call ledgerCall) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	call.Seq = len(d.calls) + 1
	d.calls = append(d.calls, call)
	return call.Seq
}

func (d *tracedDocker) Info(ctx context.Context, options mobycl.InfoOptions) (mobycl.SystemInfoResult, error) {
	result, err := d.fakeVerifyDocker.Info(ctx, options)
	if err != nil {
		return result, err
	}
	clock, _ := time.Parse(time.RFC3339Nano, result.Info.SystemTime)
	seq := d.record(ledgerCall{Op: "info", Clock: clock})
	d.mu.Lock()
	d.infos++
	n := d.infos
	d.mu.Unlock()
	if d.afterInfo != nil {
		d.afterInfo(n, seq)
	}
	return result, nil
}

func (d *tracedDocker) ContainerLogs(ctx context.Context, id string, options mobycl.ContainerLogsOptions) (mobycl.ContainerLogsResult, error) {
	d.record(ledgerCall{Op: "logs", Target: id, Since: options.Since, Until: options.Until})
	return d.fakeVerifyDocker.ContainerLogs(ctx, id, options)
}

func (d *tracedDocker) CopyFromContainer(ctx context.Context, id string, options mobycl.CopyFromContainerOptions) (mobycl.CopyFromContainerResult, error) {
	d.record(ledgerCall{Op: "crash", Target: id})
	return d.fakeVerifyDocker.CopyFromContainer(ctx, id, options)
}

// ledgerEvent is a controlled event: what the fixture emitted and which
// outcome the current rule predicts for it.
type ledgerEvent struct {
	ID   string
	Kind string // "log" or "crash"
	// Stamp is the synthetic log stamp or crash metadata timestamp. It is
	// recorded apart from ReleasedAfter, the sequence number of the engine
	// call after which the event became readable; 0 is before the
	// evaluation.
	Stamp         time.Time
	ReleasedAfter int
	Want          bool
}

// ledgerCheck is the per-check part of the ledger.
type ledgerCheck struct {
	Rule string
	// Start and End are the engine interval the rule used; zero when the
	// rule has none.
	Start, End time.Time
	// ReadSeq lists the sequence numbers of the engine reads of this check
	// in the evaluation.
	ReadSeq  []int
	Received []string
	Gaps     []string
}

type selectionLedger struct {
	// StartSample and EndSample are the sequence numbers of the engine
	// clock readings at activation and at evaluation.
	StartSample, EndSample int
	Events                 []ledgerEvent
	Logs, Crashes          ledgerCheck
	// The selected log source: its identity, driver and bytes.
	Source VerificationSource
	// LogsDisposition is "observed" when the source's log was read, or
	// "rejected" when the verifier refused the source with a gap.
	LogsDisposition string
}

var (
	ledgerIDText = regexp.MustCompile(`ledger-id=(\S+)`)
)

// buildSelectionLedger joins the engine trace to the report. logTarget and
// crashTarget are the engine ids of the log and crash sources.
func buildSelectionLedger(calls []ledgerCall, events []ledgerEvent, report *VerificationReport, logTarget string) selectionLedger {
	l := selectionLedger{Events: events}
	infos := 0
	for _, call := range calls {
		switch call.Op {
		case "info":
			infos++
			switch infos {
			case 1:
				l.StartSample = call.Seq
				l.Crashes.Start, l.Logs.Start = call.Clock, call.Clock
			case 2:
				l.EndSample = call.Seq
				l.Crashes.End, l.Logs.End = call.Clock, call.Clock
			}
		case "logs":
			if call.Target == logTarget && infos >= 2 {
				l.Logs.ReadSeq = append(l.Logs.ReadSeq, call.Seq)
				l.Logs.Start, l.Logs.End = parseDockerTimestamp(call.Since), parseDockerTimestamp(call.Until)
			}
		case "crash":
			if infos >= 2 {
				l.Crashes.ReadSeq = append(l.Crashes.ReadSeq, call.Seq)
			}
		}
	}
	l.Logs.Rule = "engine-clock Since/Until, and each line's own stamp inside the window"
	l.Crashes.Rule = "new raw crash identity since activation; the metadata timestamp is not filtered"
	// A crash read spans the window from the start sample to now, not an
	// interval that the rule filters on.
	l.Crashes.Start, l.Crashes.End = time.Time{}, time.Time{}
	for _, finding := range report.Logs.Findings {
		if m := ledgerIDText.FindStringSubmatch(finding.Text); m != nil {
			l.Logs.Received = append(l.Logs.Received, m[1])
		}
	}
	for _, finding := range report.Crashes.Findings {
		l.Crashes.Received = append(l.Crashes.Received, finding.CrashID)
	}
	for _, gap := range report.Logs.Gaps {
		l.Logs.Gaps = append(l.Logs.Gaps, gap.Source+": "+gap.Reason)
	}
	for _, gap := range report.Crashes.Gaps {
		l.Crashes.Gaps = append(l.Crashes.Gaps, gap.Source+": "+gap.Reason)
	}
	for _, source := range report.Sources {
		if source.ContainerID == logTarget {
			l.Source = source
		}
	}
	l.LogsDisposition = "observed"
	for _, gap := range report.Logs.Gaps {
		if gap.Source == l.Source.Source && strings.Contains(gap.Reason, "is not covered") {
			l.LogsDisposition = "rejected"
		}
	}
	return l
}

// judgeSelectionLedger accepts only a ledger that shows the log-positive
// control: a selected generation on the non-rotating json-file driver with
// bytes read and no log gap, controlled events released after the end-clock
// sample and before the reads, and exactly the predicted events received,
// each once. Missing or empty evidence is an error, never a pass.
func judgeSelectionLedger(l selectionLedger) error {
	var problems []string
	bad := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	if l.StartSample <= 0 || l.EndSample <= l.StartSample {
		bad("the clock samples are %d then %d", l.StartSample, l.EndSample)
	}
	if l.Logs.Start.IsZero() || l.Logs.End.IsZero() || !l.Logs.End.After(l.Logs.Start) {
		bad("the log interval %v..%v is missing or empty", l.Logs.Start, l.Logs.End)
	}
	if len(l.Logs.ReadSeq) != 1 || len(l.Crashes.ReadSeq) == 0 {
		bad("log reads %v, crash reads %v", l.Logs.ReadSeq, l.Crashes.ReadSeq)
	}
	for _, seq := range append(slices.Clone(l.Logs.ReadSeq), l.Crashes.ReadSeq...) {
		if seq <= l.EndSample {
			bad("a read (call %d) does not follow the end-clock sample (call %d)", seq, l.EndSample)
		}
	}
	s := l.Source
	if s.Source == "" || s.ContainerID == "" || s.ImageID == "" || s.StartedAt == "" {
		bad("the selected generation is not identified: %+v", s)
	}
	if s.LogDriver != "json-file" {
		bad("the log driver is %q", s.LogDriver)
	}
	if s.LogBytes <= 0 {
		bad("no log bytes were read")
	}
	if l.LogsDisposition != "observed" {
		bad("the log source was %q, not observed", l.LogsDisposition)
	}
	if len(l.Logs.Gaps) > 0 {
		bad("log gaps: %v", l.Logs.Gaps)
	}
	if len(l.Events) == 0 {
		bad("no controlled events")
	}
	late := 0
	for _, e := range l.Events {
		if e.ID == "" || (e.Kind != "log" && e.Kind != "crash") || e.Stamp.IsZero() {
			bad("incomplete event %+v", e)
			continue
		}
		if e.ReleasedAfter == l.EndSample {
			late++
		} else if e.ReleasedAfter != 0 {
			bad("event %s was released after call %d, not at the end-clock sample %d", e.ID, e.ReleasedAfter, l.EndSample)
		}
		received := l.Logs.Received
		if e.Kind == "crash" {
			received = l.Crashes.Received
		}
		switch n := countEqual(received, e.ID); {
		case n > 1:
			bad("event %s was received %d times", e.ID, n)
		case n == 1 && !e.Want:
			bad("event %s was received but the rule excludes it", e.ID)
		case n == 0 && e.Want:
			bad("event %s was not received", e.ID)
		}
	}
	if late == 0 {
		bad("no event was released after the end-clock sample")
	}
	for kind, check := range map[string]ledgerCheck{"log": l.Logs, "crash": l.Crashes} {
		for _, id := range check.Received {
			if !slices.ContainsFunc(l.Events, func(e ledgerEvent) bool { return e.ID == id && e.Kind == kind }) {
				bad("%s %q was received but never emitted", kind, id)
			}
		}
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

// selectionRun is the result of one traced evaluation.
type selectionRun struct {
	ledger selectionLedger
	report *VerificationReport
}

// runSelectionLedger activates a verification, moves the engine's clock to
// start+end, evaluates it and releases the late events right after the
// end-clock sample. The mon container holds log lines at the given offsets
// from the end; osd1 holds one crash before the sample and one released
// after it.
func runSelectionLedger(t *testing.T, end time.Duration, driver string, doneLate bool) selectionRun {
	t.Helper()
	fx := newVerifyFixture()
	fx.config.Logs = []LogExpectation{{Name: "ledger-sentinel", Daemon: "mon.a", Event: LogClusterWarning, Pattern: `ledger-id=`, Required: true}}
	fx.config.Crashes = []CrashExpectation{{Name: "ledger-crash", Daemon: "osd.1"}}
	fx.docker.containers["mon"].driver = driver
	traced := &tracedDocker{fakeVerifyDocker: fx.docker}
	v, err := newVerification(fx.config)
	if err != nil {
		t.Fatal(err)
	}
	fx.wire(v)
	v.docker = func(context.Context) (verificationDocker, func(), error) { return traced, func() {}, nil }
	if err := v.activate(t.Context()); err != nil {
		t.Fatal(err)
	}
	endClock := verifyStart.Add(end)
	fx.docker.clock = endClock

	mon, osd1 := fx.docker.containers["mon"], fx.docker.containers["osd1"]
	var events []ledgerEvent
	logLine := func(id string, stamp time.Time, want bool, after int) {
		mon.lines = append(mon.lines, fakeLogLine{stamp, daemonLogLine("cluster", "WRN", "ledger-id="+id)})
		events = append(events, ledgerEvent{ID: id, Kind: "log", Stamp: stamp, ReleasedAfter: after, Want: want})
	}
	// Before the evaluation: inside, at the boundary and before the window.
	logLine("log-inside", verifyStart.Add(10*time.Second), true, 0)
	logLine("log-at-end", endClock, true, 0)
	logLine("log-at-start", verifyStart, true, 0)
	logLine("log-before-start", verifyStart.Add(-time.Nanosecond), false, 0)
	osd1.crashes = append(osd1.crashes, "2026-10-23T01:00:10Z_early")
	events = append(events, ledgerEvent{ID: "2026-10-23T01:00:10Z_early", Kind: "crash", Stamp: verifyStart.Add(time.Minute), Want: true})

	lateCrash := "2026-10-23T01:00:20Z_late"
	traced.afterInfo = func(n, seq int) {
		if n != 2 {
			return
		}
		// Released after the end-clock sample and before any read.
		logLine("log-after-end", endClock.Add(time.Nanosecond), false, seq)
		osd1.crashes = append(osd1.crashes, lateCrash)
		if !doneLate {
			osd1.notDone = map[string]bool{lateCrash: true}
		}
		events = append(events, ledgerEvent{ID: lateCrash, Kind: "crash", Stamp: verifyStart.Add(time.Minute), ReleasedAfter: seq, Want: true})
	}
	report := fx.evaluate(t, v)
	return selectionRun{ledger: buildSelectionLedger(traced.calls, events, report, "mon"), report: report}
}

// With the engine clock at 30s the late crash's metadata timestamp (one
// minute) lies after the end; with two minutes it lies before. The crash is
// received either way, since crashes are selected by identity, while a log
// line stamped after the end sample is excluded.
func TestVerificationSelectionLedgerPositiveControl(t *testing.T) {
	for _, end := range []time.Duration{30 * time.Second, 2 * time.Minute} {
		t.Run(end.String(), func(t *testing.T) {
			run := runSelectionLedger(t, end, "", true)
			if err := judgeSelectionLedger(run.ledger); err != nil {
				t.Fatalf("%v\nledger %+v", err, run.ledger)
			}
			// Three lines are inside the window and permitted by the
			// required sentinel expectation; the line after the end
			// is not counted.
			if exp := run.report.Logs.Expectations; run.report.Logs.Outcome != VerificationClean || len(exp) != 1 || exp[0].Matched != 3 || !exp[0].Satisfied {
				t.Fatalf("logs %+v", run.report.Logs)
			}
			if run.report.Crashes.Outcome != VerificationClean || len(run.ledger.Crashes.Gaps) != 0 {
				t.Fatalf("crashes %+v", run.report.Crashes)
			}
			if !run.ledger.Logs.End.Equal(verifyStart.Add(end)) || !run.ledger.Logs.Start.Equal(verifyStart) {
				t.Fatalf("log interval %v..%v", run.ledger.Logs.Start, run.ledger.Logs.End)
			}
		})
	}
}

// A late crash without a done marker keeps its finding and adds the
// completion gap; the log ledger stays as before.
func TestVerificationSelectionLedgerCrashWithoutDone(t *testing.T) {
	run := runSelectionLedger(t, 30*time.Second, "", false)
	if err := judgeSelectionLedger(run.ledger); err != nil {
		t.Fatalf("%v\nledger %+v", err, run.ledger)
	}
	if run.report.Crashes.Outcome != VerificationIncomplete {
		t.Fatalf("crashes %+v", run.report.Crashes)
	}
	if len(run.ledger.Crashes.Gaps) != 1 || !strings.Contains(run.ledger.Crashes.Gaps[0], "has no done marker yet") ||
		!slices.Contains(run.ledger.Crashes.Received, "2026-10-23T01:00:20Z_late") {
		t.Fatalf("crash ledger %+v", run.ledger.Crashes)
	}
}

// An unsupported log driver is a refusal, not an observation: the ledger
// says "rejected", the judge refuses it as log evidence, and no event of the
// refused source is received.
func TestVerificationSelectionLedgerUnsupportedDriver(t *testing.T) {
	run := runSelectionLedger(t, 30*time.Second, "local", true)
	l := run.ledger
	if l.LogsDisposition != "rejected" || len(l.Logs.Received) != 0 || len(l.Logs.ReadSeq) != 0 || l.Source.LogBytes != 0 || l.Source.LogDriver != "local" {
		t.Fatalf("ledger %+v", l)
	}
	if run.report.Logs.Outcome == VerificationClean {
		t.Fatalf("an unsupported driver became clean: %+v", run.report.Logs)
	}
	if judgeSelectionLedger(l) == nil {
		t.Fatal("a rejected source was accepted as log evidence")
	}
	// The crash check is independent of the refused log source.
	if !slices.Contains(l.Crashes.Received, "2026-10-23T01:00:20Z_late") {
		t.Fatalf("crash ledger %+v", l.Crashes)
	}
}

// Each weakened ledger must be refused: the judge never turns missing,
// empty or contradicting evidence into a pass.
func TestVerificationSelectionLedgerJudgeControls(t *testing.T) {
	base := runSelectionLedger(t, 30*time.Second, "", true).ledger
	if err := judgeSelectionLedger(base); err != nil {
		t.Fatal(err)
	}
	clone := func(mutate func(*selectionLedger)) selectionLedger {
		l := base
		l.Events = slices.Clone(base.Events)
		l.Logs.ReadSeq, l.Crashes.ReadSeq = slices.Clone(base.Logs.ReadSeq), slices.Clone(base.Crashes.ReadSeq)
		l.Logs.Received, l.Crashes.Received = slices.Clone(base.Logs.Received), slices.Clone(base.Crashes.Received)
		mutate(&l)
		return l
	}
	cases := map[string]func(*selectionLedger){
		"no end sample":                func(l *selectionLedger) { l.EndSample = 0 },
		"no start sample":              func(l *selectionLedger) { l.StartSample = 0 },
		"empty interval":               func(l *selectionLedger) { l.Logs.End = l.Logs.Start },
		"missing interval":             func(l *selectionLedger) { l.Logs.Start, l.Logs.End = time.Time{}, time.Time{} },
		"no log read":                  func(l *selectionLedger) { l.Logs.ReadSeq = nil },
		"no crash read":                func(l *selectionLedger) { l.Crashes.ReadSeq = nil },
		"log read before the sample":   func(l *selectionLedger) { l.Logs.ReadSeq = []int{l.EndSample} },
		"crash read before the sample": func(l *selectionLedger) { l.Crashes.ReadSeq = []int{1} },
		"no container id":              func(l *selectionLedger) { l.Source.ContainerID = "" },
		"no image id":                  func(l *selectionLedger) { l.Source.ImageID = "" },
		"no start time":                func(l *selectionLedger) { l.Source.StartedAt = "" },
		"wrong driver":                 func(l *selectionLedger) { l.Source.LogDriver = "local" },
		"empty driver":                 func(l *selectionLedger) { l.Source.LogDriver = "" },
		"no bytes":                     func(l *selectionLedger) { l.Source.LogBytes = 0 },
		"rejected source":              func(l *selectionLedger) { l.LogsDisposition = "rejected" },
		"unknown disposition":          func(l *selectionLedger) { l.LogsDisposition = "" },
		"log gap":                      func(l *selectionLedger) { l.Logs.Gaps = []string{"mon.a: gap"} },
		"no events":                    func(l *selectionLedger) { l.Events = nil },
		"no late event": func(l *selectionLedger) {
			for i := range l.Events {
				l.Events[i].ReleasedAfter = 0
			}
		},
		"event released elsewhere": func(l *selectionLedger) { l.Events[len(l.Events)-1].ReleasedAfter = l.EndSample + 1 },
		"event without id":         func(l *selectionLedger) { l.Events[0].ID = "" },
		"event without stamp":      func(l *selectionLedger) { l.Events[0].Stamp = time.Time{} },
		"event of unknown kind":    func(l *selectionLedger) { l.Events[0].Kind = "" },
		"late log received":        func(l *selectionLedger) { l.Logs.Received = append(l.Logs.Received, "log-after-end") },
		"pre-window log received":  func(l *selectionLedger) { l.Logs.Received = append(l.Logs.Received, "log-before-start") },
		"inside log lost": func(l *selectionLedger) {
			l.Logs.Received = slices.DeleteFunc(l.Logs.Received, func(id string) bool { return id == "log-inside" })
		},
		"late crash lost": func(l *selectionLedger) {
			l.Crashes.Received = slices.DeleteFunc(l.Crashes.Received, func(id string) bool { return strings.HasSuffix(id, "_late") })
		},
		"duplicate sighting":     func(l *selectionLedger) { l.Logs.Received = append(l.Logs.Received, "log-inside") },
		"adjacent duplicate log": func(l *selectionLedger) { l.Logs.Received = repeatInPlace(l.Logs.Received, "log-inside") },
		"adjacent duplicate crash": func(l *selectionLedger) {
			l.Crashes.Received = repeatInPlace(l.Crashes.Received, "2026-10-23T01:00:20Z_late")
		},
		"unemitted crash": func(l *selectionLedger) {
			l.Crashes.Received = append(l.Crashes.Received, "2026-10-23T01:09:00Z_ghost")
		},
		"nothing received": func(l *selectionLedger) { l.Logs.Received, l.Crashes.Received = nil, nil },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			if judgeSelectionLedger(clone(mutate)) == nil {
				t.Fatal("a weakened ledger was accepted")
			}
		})
	}
}

// countEqual counts the elements equal to id, one by one, so that adjacent
// repeats are all counted.
func countEqual(ids []string, id string) int {
	n := 0
	for _, got := range ids {
		if got == id {
			n++
		}
	}
	return n
}

// repeatInPlace repeats id right after its first occurrence.
func repeatInPlace(ids []string, id string) []string {
	i := slices.Index(ids, id)
	if i < 0 {
		panic("id not received: " + id)
	}
	return slices.Insert(slices.Clone(ids), i, id)
}
