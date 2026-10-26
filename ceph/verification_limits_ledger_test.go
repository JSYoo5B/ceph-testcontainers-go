package ceph

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

// This file records, for one verification over the fake engine, what the
// collectors retain, truncate or drop when a collection limit is reached, and
// whether the report accounts for every event that is not in its findings.
// It characterizes the current behavior and does not change it. The claim it
// protects is narrow: a limit never makes an event look absent. Every emitted
// event is either a finding, counted in OmittedFindings, or covered by a gap
// of its check, and the check is never clean in the last case.
//
// The fake engine is a code-path fixture. It honors no Docker log retention,
// so nothing here shows what a real engine leaves readable, and the limits
// are the verifier's own (MaxLogBytes, the line and text bounds, MaxFindings,
// the crash record limits and the container limit), not Ceph's.

// limEvent is a controlled event. SourceLen is the length of the emitted
// message for a log event; Class is the classification the fixture's
// expectations give it.
type limEvent struct {
	ID        string
	Kind      string // "log" or "crash"
	Container string
	Class     string // "permitted" or "unexpected"
	SourceLen int
}

// limFinding is one retained finding as the report carries it. ID is empty
// when the retained text no longer holds the fixture's id.
type limFinding struct {
	ID          string
	ContainerID string
	TextLen     int
	Truncated   bool
}

// limCheck is the accounting of one check.
type limCheck struct {
	Outcome    VerificationOutcome
	Permitted  int
	Unexpected int
	Omitted    int
	Findings   []limFinding
	Gaps       []string
}

type limLedger struct {
	Scenario      string
	Events        []limEvent
	Logs, Crashes limCheck
}

func (l limLedger) check(kind string) limCheck {
	if kind == "crash" {
		return l.Crashes
	}
	return l.Logs
}

func limCheckOf(check VerificationCheck, id func(VerificationFinding) string) limCheck {
	c := limCheck{Outcome: check.Outcome, Permitted: check.Permitted, Unexpected: check.Unexpected, Omitted: check.OmittedFindings}
	for _, f := range check.Findings {
		c.Findings = append(c.Findings, limFinding{ID: id(f), ContainerID: f.ContainerID, TextLen: len(f.Text), Truncated: f.Truncated})
	}
	for _, gap := range check.Gaps {
		c.Gaps = append(c.Gaps, gap.Source+": "+gap.Reason)
	}
	return c
}

func (c limCheck) received(id, container string) int {
	n := 0
	for _, f := range c.Findings {
		if f.ID == id && (container == "" || f.ContainerID == container) {
			n++
		}
	}
	return n
}

// judgeLimitAccounting accepts a ledger only when the report accounts for
// every emitted event of both checks. Missing or unknown outcomes are errors.
func judgeLimitAccounting(l limLedger) error {
	var problems []string
	bad := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	for _, kind := range []string{"log", "crash"} {
		c := l.check(kind)
		if !recognizedOutcome(c.Outcome) {
			bad("the %s outcome %q is missing or unknown", kind, c.Outcome)
			continue
		}
		emitted, missing := 0, 0
		for _, ev := range l.Events {
			if ev.Kind != kind {
				continue
			}
			if ev.ID == "" || ev.Container == "" || (ev.Class != "permitted" && ev.Class != "unexpected") {
				bad("incomplete event %+v", ev)
				continue
			}
			emitted++
			switch n := c.received(ev.ID, ev.Container); {
			case n > 1:
				bad("%s event %s was received %d times", kind, ev.ID, n)
			case n == 0:
				missing++
			default:
				for _, f := range c.Findings {
					if f.ID != ev.ID || f.ContainerID != ev.Container {
						continue
					}
					if f.TextLen > maxFindingText {
						bad("%s finding %s keeps %d bytes of text, more than %d", kind, ev.ID, f.TextLen, maxFindingText)
					}
					// A finding is truncated exactly when its source is
					// longer than the bound the report keeps.
					if kind == "log" && f.Truncated != (ev.SourceLen > maxFindingText) {
						bad("log finding %s: truncated flag %v for a source of %d bytes", ev.ID, f.Truncated, ev.SourceLen)
					}
				}
			}
		}
		for _, f := range c.Findings {
			if f.ID != "" && !slices.ContainsFunc(l.Events, func(ev limEvent) bool { return ev.Kind == kind && ev.ID == f.ID && ev.Container == f.ContainerID }) {
				bad("%s finding %q from %q was never emitted there", kind, f.ID, f.ContainerID)
			}
		}
		// The report's own counts cover its findings and its omission
		// count, whatever was dropped.
		if c.Permitted+c.Unexpected != len(c.Findings)+c.Omitted {
			bad("%s check counts %d permitted and %d unexpected, but retains %d findings and omits %d", kind, c.Permitted, c.Unexpected, len(c.Findings), c.Omitted)
		}
		switch {
		case missing == 0:
		case len(c.Gaps) > 0:
			if c.Outcome == VerificationClean {
				bad("%s check is clean with gaps %v", kind, c.Gaps)
			}
		case missing <= c.Omitted:
			// Retained in the counts, dropped from the findings.
		default:
			bad("%d of %d %s events are missing, but %d are counted as omitted and no gap exists", missing, emitted, kind, c.Omitted)
		}
		if len(c.Gaps) > 0 && c.Outcome == VerificationClean {
			bad("%s check is clean with gaps %v", kind, c.Gaps)
		}
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

// judgeAbsence accepts a claim that no event of the kind occurred (beyond
// the findings) only for a check that read everything and kept everything:
// no gap, no omitted finding, no truncated finding, a recognized outcome.
func judgeAbsence(l limLedger, kind string) error {
	c := l.check(kind)
	var problems []string
	if !recognizedOutcome(c.Outcome) {
		problems = append(problems, fmt.Sprintf("the %s outcome %q is missing or unknown", kind, c.Outcome))
	}
	if len(c.Gaps) > 0 {
		problems = append(problems, fmt.Sprintf("gaps %v", c.Gaps))
	}
	if c.Omitted > 0 {
		problems = append(problems, fmt.Sprintf("%d findings were omitted", c.Omitted))
	}
	if slices.ContainsFunc(c.Findings, func(f limFinding) bool { return f.Truncated }) {
		problems = append(problems, "a finding keeps only part of its text")
	}
	// A finding whose source is longer than the text it keeps is clipped
	// evidence, whatever its flag says.
	for _, f := range c.Findings {
		for _, ev := range l.Events {
			if ev.Kind == "log" && kind == "log" && ev.ID == f.ID && ev.Container == f.ContainerID && ev.SourceLen > f.TextLen && !f.Truncated {
				problems = append(problems, fmt.Sprintf("finding %s keeps %d of %d bytes without a truncation flag", f.ID, f.TextLen, ev.SourceLen))
			}
		}
	}
	if c.Outcome == VerificationIncomplete {
		problems = append(problems, "the check is incomplete")
	}
	if len(problems) > 0 {
		return fmt.Errorf("%s absence is not supported: %s", kind, strings.Join(problems, "; "))
	}
	return nil
}

type limEmitter struct {
	fx     *verifyFixture
	events []limEvent
	next   time.Duration
}

// log adds a WRN cluster log line whose message is "ledger-id=<id>" followed
// by tail, to a container of the fixture.
func (g *limEmitter) log(container, id, class, tail string) {
	g.next += time.Millisecond
	message := "ledger-id=" + id + tail
	ctr := g.fx.docker.containers[container]
	// The verifier bounds the whole daemon line (envelope and message), so
	// SourceLen is that line's length.
	line := daemonLogLine("cluster", "WRN", message)
	ctr.lines = append(ctr.lines, fakeLogLine{verifyStart.Add(time.Minute + g.next), line})
	g.events = append(g.events, limEvent{ID: id, Kind: "log", Container: container, Class: class, SourceLen: len(line)})
}

func (g *limEmitter) crash(container, id, class string) {
	ctr := g.fx.docker.containers[container]
	ctr.crashes = append(ctr.crashes, id)
	g.events = append(g.events, limEvent{ID: id, Kind: "crash", Container: container, Class: class})
}

// limScenario configures the fixture before the activation and emits events
// after it.
type limScenario struct {
	name  string
	setup func(fx *verifyFixture)
	emit  func(fx *verifyFixture, g *limEmitter)
}

func runLimitScenario(t *testing.T, sc limScenario) limLedger {
	t.Helper()
	fx := newVerifyFixture()
	// mon.a's ledger lines and crashes are permitted; any other source's are
	// unexpected.
	fx.config.Logs = []LogExpectation{{Name: "ledger-sentinel", Daemon: "mon.a", Event: LogClusterWarning, Pattern: `ledger-id=`}}
	fx.config.Crashes = []CrashExpectation{{Name: "ledger-crash", Daemon: "mon.a"}}
	if sc.setup != nil {
		sc.setup(fx)
	}
	v := fx.activate(t)
	g := &limEmitter{fx: fx}
	sc.emit(fx, g)
	report := fx.evaluate(t, v)
	l := limLedger{Scenario: sc.name, Events: g.events}
	l.Logs = limCheckOf(report.Logs, func(f VerificationFinding) string {
		if m := ledgerIDText.FindStringSubmatch(f.Text); m != nil {
			return m[1]
		}
		return ""
	})
	l.Crashes = limCheckOf(report.Crashes, func(f VerificationFinding) string { return f.CrashID })
	return l
}

func limCrashID(i int) string { return fmt.Sprintf("2026-10-23T01:%02d:00Z_c%02d", i%60, i) }

// limScenarios are the limits the verifier applies. Each one hits exactly
// one of them; the control hits none.
func limScenarios() map[string]limScenario {
	return map[string]limScenario{
		"control": {name: "control", emit: func(_ *verifyFixture, g *limEmitter) {
			g.log("mon", "log-1", "permitted", "")
			g.log("mon", "log-2", "permitted", "")
			g.crash("mon", limCrashID(1), "permitted")
		}},
		"log byte limit": {name: "log byte limit", setup: func(fx *verifyFixture) { fx.config.MaxLogBytes = 1024 },
			emit: func(_ *verifyFixture, g *limEmitter) {
				for i := range 30 {
					g.log("mon", fmt.Sprintf("log-%02d", i), "permitted", "")
				}
			}},
		"line bound": {name: "line bound", emit: func(_ *verifyFixture, g *limEmitter) {
			g.log("mon", "log-before", "permitted", "")
			g.log("mon", "log-long", "permitted", " "+strings.Repeat("x", maxVerificationLine))
			g.log("mon", "log-after", "permitted", "")
		}},
		"finding text bound": {name: "finding text bound", emit: func(_ *verifyFixture, g *limEmitter) {
			g.log("mon", "log-short", "permitted", "")
			g.log("mon", "log-wide", "permitted", " "+strings.Repeat("y", 3*maxFindingText))
		}},
		"finding limit": {name: "finding limit", setup: func(fx *verifyFixture) { fx.config.MaxFindings = 3 },
			emit: func(_ *verifyFixture, g *limEmitter) {
				for i := range 6 {
					g.log("mon", fmt.Sprintf("log-p%d", i), "permitted", "")
				}
				for i := range 2 {
					g.log("osd1", fmt.Sprintf("log-u%d", i), "unexpected", "")
				}
			}},
		"crash record limit": {name: "crash record limit", emit: func(_ *verifyFixture, g *limEmitter) {
			for i := range 70 {
				g.crash("osd1", limCrashID(i), "unexpected")
			}
		}},
		"crash record size": {name: "crash record size", emit: func(fx *verifyFixture, g *limEmitter) {
			g.crash("osd1", limCrashID(1), "unexpected")
			g.crash("osd1", limCrashID(2), "unexpected")
			fx.docker.containers["osd1"].messages = map[string]string{limCrashID(2): strings.Repeat("z", 70<<10)}
		}},
		"container limit": {name: "container limit", setup: func(fx *verifyFixture) { fx.snapshot.omitted = 2 },
			emit: func(_ *verifyFixture, g *limEmitter) {
				g.log("mon", "log-1", "permitted", "")
			}},
	}
}

// The control reads everything it is asked to: both of its events are
// findings, nothing is omitted or truncated, and absence is supported.
func TestVerificationLimitLedgerControl(t *testing.T) {
	l := runLimitScenario(t, limScenarios()["control"])
	if err := judgeLimitAccounting(l); err != nil {
		t.Fatalf("%v\nledger %+v", err, l)
	}
	if l.Logs.received("log-1", "mon") != 1 || l.Logs.received("log-2", "mon") != 1 || l.Crashes.received(limCrashID(1), "mon") != 1 ||
		l.Logs.Omitted != 0 || l.Crashes.Omitted != 0 || len(l.Logs.Gaps)+len(l.Crashes.Gaps) != 0 {
		t.Fatalf("ledger %+v", l)
	}
	for _, kind := range []string{"log", "crash"} {
		if err := judgeAbsence(l, kind); err != nil {
			t.Fatal(err)
		}
	}
}

// Every limit that drops events leaves them accounted for, and none of them
// supports an absence claim for the check it hit.
func TestVerificationLimitLedgerDropsAreAccounted(t *testing.T) {
	cases := []struct {
		name  string
		check string
		// gap is the gap text the check carries, or "" when the drop is
		// counted as omitted findings.
		gap     string
		omitted int
		// lost is how many events of the check are not findings.
		lost func(limCheck) bool
	}{
		{name: "log byte limit", check: "log", gap: "the log stream of the window exceeds 1024 bytes"},
		{name: "line bound", check: "log", gap: "longer than 65536 bytes"},
		{name: "finding limit", check: "log", omitted: 5},
		{name: "crash record limit", check: "crash", gap: "metadata files were not accepted"},
		{name: "crash record size", check: "crash", gap: "larger than 65536 bytes"},
		{name: "container limit", check: "log", gap: "2 containers exceed the container limit"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l := runLimitScenario(t, limScenarios()[tc.name])
			if err := judgeLimitAccounting(l); err != nil {
				t.Fatalf("%v\nledger %+v", err, l)
			}
			c := l.check(tc.check)
			if tc.gap != "" && !hasGap(c.Gaps, tc.gap) {
				t.Fatalf("gaps %v do not hold %q", c.Gaps, tc.gap)
			}
			if tc.gap == "" && (len(c.Gaps) != 0 || c.Omitted != tc.omitted) {
				t.Fatalf("omitted %d (want %d), gaps %v", c.Omitted, tc.omitted, c.Gaps)
			}
			if err := judgeAbsence(l, tc.check); err == nil {
				t.Fatalf("a limited check supported an absence claim: %+v", c)
			}
			if tc.gap != "" && c.Outcome == VerificationClean {
				t.Fatalf("the %s check is clean with gaps %v", tc.check, c.Gaps)
			}
		})
	}
}

// The log byte limit keeps a prefix of the window: lines before the limit
// are findings, the later ones are not, and the gap says why. Nothing past
// the limit is read, so the retained findings are the first emitted ones.
func TestVerificationLimitLedgerLogByteLimitKeepsAPrefix(t *testing.T) {
	l := runLimitScenario(t, limScenarios()["log byte limit"])
	if err := judgeLimitAccounting(l); err != nil {
		t.Fatalf("%v\nledger %+v", err, l)
	}
	kept := 0
	for i := range 30 {
		if l.Logs.received(fmt.Sprintf("log-%02d", i), "mon") == 1 {
			if kept != i {
				t.Fatalf("log-%02d was kept after a dropped line: %+v", i, l.Logs)
			}
			kept++
		}
	}
	if kept == 0 || kept == 30 {
		t.Fatalf("%d of 30 lines kept: %+v", kept, l.Logs)
	}
}

// A line cut at the line bound keeps its id at the head, so the finding is
// received, truncated, and the check still carries the cut gap. The events
// around it are untouched.
func TestVerificationLimitLedgerLineBound(t *testing.T) {
	l := runLimitScenario(t, limScenarios()["line bound"])
	if err := judgeLimitAccounting(l); err != nil {
		t.Fatalf("%v\nledger %+v", err, l)
	}
	for _, id := range []string{"log-before", "log-after"} {
		if l.Logs.received(id, "mon") != 1 {
			t.Fatalf("%s was not received: %+v", id, l.Logs)
		}
	}
	if l.Logs.received("log-long", "mon") != 1 || !hasGap(l.Logs.Gaps, "1 log lines are longer than 65536 bytes") {
		t.Fatalf("ledger %+v", l.Logs)
	}
	for _, f := range l.Logs.Findings {
		if f.ID == "log-long" && !f.Truncated {
			t.Fatalf("the cut line's finding is not marked truncated: %+v", f)
		}
	}
}

// The finding keeps at most 1 KiB of text and says so, but its expectation
// was matched against the whole redacted line, and truncated text alone does
// not make the check incomplete. A tail-only pattern shows the match.
func TestVerificationLimitLedgerFindingTextBound(t *testing.T) {
	l := runLimitScenario(t, limScenario{name: "text", setup: func(fx *verifyFixture) {
		fx.config.Logs = []LogExpectation{{Name: "tail", Daemon: "mon.a", Event: LogClusterWarning, Pattern: `ledger-id=log-wide`}}
	}, emit: func(_ *verifyFixture, g *limEmitter) {
		g.log("mon", "log-wide", "permitted", " "+strings.Repeat("y", 3*maxFindingText))
	}})
	if err := judgeLimitAccounting(l); err != nil {
		t.Fatalf("%v\nledger %+v", err, l)
	}
	if l.Logs.Permitted != 1 || l.Logs.Unexpected != 0 || len(l.Logs.Findings) != 1 || !l.Logs.Findings[0].Truncated || l.Logs.Findings[0].TextLen != maxFindingText {
		t.Fatalf("ledger %+v", l.Logs)
	}
	if len(l.Logs.Gaps) != 0 || l.Logs.Outcome != VerificationClean {
		t.Fatalf("truncated text changed the check: %+v", l.Logs)
	}
	// The retained text is partial evidence: it supports no absence claim.
	if judgeAbsence(l, "log") == nil {
		t.Fatal("a truncated finding supported an absence claim")
	}
	// The tail of a wide line matches an expectation that the kept text
	// cannot show.
	tail := runLimitScenario(t, limScenario{name: "tail", setup: func(fx *verifyFixture) {
		fx.config.Logs = []LogExpectation{{Name: "tail", Daemon: "mon.a", Event: LogClusterWarning, Pattern: `tail-marker$`}}
	}, emit: func(_ *verifyFixture, g *limEmitter) {
		g.log("mon", "log-wide", "permitted", " "+strings.Repeat("y", 3*maxFindingText)+" tail-marker")
	}})
	if tail.Logs.Permitted != 1 || tail.Logs.Unexpected != 0 {
		t.Fatalf("the whole line was not matched: %+v", tail.Logs)
	}
}

// limBoundaryMessageTail pads a message so that the whole daemon line,
// envelope included, is exactly total bytes long.
func limBoundaryMessageTail(id string, total int) string {
	return " " + strings.Repeat("b", total-len(daemonLogLine("cluster", "WRN", "ledger-id="+id+" ")))
}

// The text bound applies to the whole daemon line, not to its message: a
// line of exactly maxFindingText bytes is kept whole and unflagged, one byte
// more is cut and flagged, and a message of maxFindingText bytes is already
// over the bound. Losing a correct flag is refused by both judges.
func TestVerificationLimitLedgerTextBoundIsTheWholeLine(t *testing.T) {
	run := func(total int) limLedger {
		return runLimitScenario(t, limScenario{name: "boundary", emit: func(_ *verifyFixture, g *limEmitter) {
			g.log("mon", "log-edge", "permitted", limBoundaryMessageTail("log-edge", total))
		}})
	}
	exact, over := run(maxFindingText), run(maxFindingText+1)
	for name, tc := range map[string]struct {
		l         limLedger
		truncated bool
	}{"exact": {exact, false}, "one over": {over, true}} {
		if err := judgeLimitAccounting(tc.l); err != nil {
			t.Fatalf("%s: %v\nledger %+v", name, err, tc.l)
		}
		f := tc.l.Logs.Findings[0]
		if f.Truncated != tc.truncated || f.TextLen != maxFindingText || tc.l.Events[0].SourceLen != maxFindingText+btoi(tc.truncated) {
			t.Fatalf("%s: finding %+v, event %+v", name, f, tc.l.Events[0])
		}
		if (judgeAbsence(tc.l, "log") == nil) == tc.truncated {
			t.Fatalf("%s: absence verdict %v", name, judgeAbsence(tc.l, "log"))
		}
	}
	// A message of maxFindingText bytes alone is over the bound.
	msg := runLimitScenario(t, limScenario{name: "message", emit: func(_ *verifyFixture, g *limEmitter) {
		g.log("mon", "log-msg", "permitted", " "+strings.Repeat("m", maxFindingText-len("ledger-id=log-msg ")))
	}})
	if err := judgeLimitAccounting(msg); err != nil || !msg.Logs.Findings[0].Truncated {
		t.Fatalf("%v\nledger %+v", err, msg.Logs)
	}
	for name, l := range map[string]limLedger{"exact": exact, "one over": over, "message": msg} {
		lost := l
		lost.Logs.Findings = slices.Clone(l.Logs.Findings)
		lost.Logs.Findings[0].Truncated = false
		if name == "exact" {
			if judgeLimitAccounting(lost) != nil || judgeAbsence(lost, "log") != nil {
				t.Fatalf("%s: an unflagged whole line was refused", name)
			}
			continue
		}
		if judgeLimitAccounting(lost) == nil || judgeAbsence(lost, "log") == nil {
			t.Fatalf("%s: a lost truncation flag was accepted", name)
		}
	}
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// The finding limit keeps unexpected findings first and counts the rest as
// omitted, without a gap: the counts still cover every event.
func TestVerificationLimitLedgerFindingLimitCounts(t *testing.T) {
	l := runLimitScenario(t, limScenarios()["finding limit"])
	if err := judgeLimitAccounting(l); err != nil {
		t.Fatalf("%v\nledger %+v", err, l)
	}
	if l.Logs.Permitted != 6 || l.Logs.Unexpected != 2 || l.Logs.Omitted != 5 || len(l.Logs.Findings) != 3 ||
		l.Logs.received("log-u0", "osd1") != 1 || l.Logs.received("log-u1", "osd1") != 1 {
		t.Fatalf("ledger %+v", l.Logs)
	}
	if l.Logs.Outcome != VerificationUnexpected {
		t.Fatalf("outcome %s", l.Logs.Outcome)
	}
}

// Past the crash record limit the omitted crashes are a gap, and the crashes
// that were read are only a prefix: the check cannot say the rest did not
// happen, and the report is not clean. Because the activation read of the
// same directory was complete, the 64 kept records are new.
func TestVerificationLimitLedgerCrashRecordLimit(t *testing.T) {
	l := runLimitScenario(t, limScenarios()["crash record limit"])
	if err := judgeLimitAccounting(l); err != nil {
		t.Fatalf("%v\nledger %+v", err, l)
	}
	if l.Crashes.Unexpected != 64 || len(l.Crashes.Findings) != 64 || l.Crashes.Outcome != VerificationUnexpected || !hasGap(l.Crashes.Gaps, "6 metadata files were not accepted") {
		t.Fatalf("ledger %+v", l.Crashes)
	}
}

// Each weakened ledger must be refused: the judges never turn missing,
// empty or contradicting evidence into an accounted drop or an absence.
func TestVerificationLimitLedgerJudgeControls(t *testing.T) {
	control := runLimitScenario(t, limScenarios()["control"])
	dropped := runLimitScenario(t, limScenarios()["log byte limit"])
	omitted := runLimitScenario(t, limScenarios()["finding limit"])
	cut := runLimitScenario(t, limScenarios()["line bound"])
	for name, l := range map[string]limLedger{"control": control, "dropped": dropped, "omitted": omitted, "cut": cut} {
		if err := judgeLimitAccounting(l); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	clone := func(l limLedger, mutate func(*limLedger)) limLedger {
		l.Events = slices.Clone(l.Events)
		for _, c := range []*limCheck{&l.Logs, &l.Crashes} {
			c.Findings, c.Gaps = slices.Clone(c.Findings), slices.Clone(c.Gaps)
		}
		mutate(&l)
		return l
	}
	accounting := map[string]struct {
		base   limLedger
		mutate func(*limLedger)
	}{
		"dropped: gap removed":            {dropped, func(l *limLedger) { l.Logs.Gaps = nil }},
		"dropped: check clean":            {dropped, func(l *limLedger) { l.Logs.Outcome = VerificationClean }},
		"dropped: empty outcome":          {dropped, func(l *limLedger) { l.Logs.Outcome = "" }},
		"dropped: unknown outcome":        {dropped, func(l *limLedger) { l.Logs.Outcome = "passed" }},
		"dropped: outcome deleted":        {dropped, func(l *limLedger) { l.Logs.Outcome, l.Crashes.Outcome = "", "" }},
		"dropped: omission hides nothing": {dropped, func(l *limLedger) { l.Logs.Gaps, l.Logs.Omitted = nil, 1 }},
		"dropped: counts shrink":          {dropped, func(l *limLedger) { l.Logs.Permitted-- }},
		"omitted: omission count removed": {omitted, func(l *limLedger) { l.Logs.Omitted = 0 }},
		"omitted: too few omitted":        {omitted, func(l *limLedger) { l.Logs.Omitted--; l.Logs.Permitted-- }},
		"omitted: counts lose a finding":  {omitted, func(l *limLedger) { l.Logs.Permitted-- }},
		"control: duplicate finding": {control, func(l *limLedger) {
			l.Logs.Findings = append(l.Logs.Findings, l.Logs.Findings[0])
			l.Logs.Permitted++
		}},
		"control: event lost": {control, func(l *limLedger) {
			l.Logs.Findings, l.Logs.Permitted = l.Logs.Findings[1:], l.Logs.Permitted-1
		}},
		"control: finding from another container": {control, func(l *limLedger) { l.Logs.Findings[0].ContainerID = "osd1" }},
		"control: unemitted finding": {control, func(l *limLedger) {
			l.Logs.Findings = append(l.Logs.Findings, limFinding{ID: "log-ghost", ContainerID: "mon"})
			l.Logs.Permitted++
		}},
		"control: gaps with a clean check": {control, func(l *limLedger) { l.Logs.Gaps = []string{"mon.a: gap"} }},
		"control: crash outcome missing":   {control, func(l *limLedger) { l.Crashes.Outcome = "" }},
		"control: event without id":        {control, func(l *limLedger) { l.Events[0].ID = "" }},
		"control: event without container": {control, func(l *limLedger) { l.Events[0].Container = "" }},
		"control: event without class":     {control, func(l *limLedger) { l.Events[0].Class = "" }},
		"control: oversized finding text":  {control, func(l *limLedger) { l.Logs.Findings[0].TextLen = maxFindingText + 1 }},
		"control: truncated without cause": {control, func(l *limLedger) { l.Logs.Findings[0].Truncated = true }},
		"cut: truncated flag lost": {cut, func(l *limLedger) {
			for i := range l.Logs.Findings {
				l.Logs.Findings[i].Truncated = false
			}
		}},
	}
	for name, tc := range accounting {
		t.Run("accounting/"+name, func(t *testing.T) {
			if judgeLimitAccounting(clone(tc.base, tc.mutate)) == nil {
				t.Fatal("a weakened ledger was accepted")
			}
		})
	}
	absence := map[string]struct {
		base   limLedger
		kind   string
		mutate func(*limLedger)
	}{
		"gap":              {control, "log", func(l *limLedger) { l.Logs.Gaps = []string{"mon.a: gap"} }},
		"omitted findings": {control, "log", func(l *limLedger) { l.Logs.Omitted = 1 }},
		"truncated finding": {control, "log", func(l *limLedger) {
			l.Logs.Findings[0].Truncated = true
		}},
		"incomplete":      {control, "log", func(l *limLedger) { l.Logs.Outcome = VerificationIncomplete }},
		"empty outcome":   {control, "log", func(l *limLedger) { l.Logs.Outcome = "" }},
		"unknown outcome": {control, "crash", func(l *limLedger) { l.Crashes.Outcome = "ok" }},
		"crash gap":       {control, "crash", func(l *limLedger) { l.Crashes.Gaps = []string{"osd.1: gap"} }},
	}
	for name, tc := range absence {
		t.Run("absence/"+name, func(t *testing.T) {
			if judgeAbsence(clone(tc.base, tc.mutate), tc.kind) == nil {
				t.Fatal("a weakened ledger supported an absence claim")
			}
		})
	}
}
