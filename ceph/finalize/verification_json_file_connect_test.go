package finalize

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// This file runs the event shape of the native json-file observation (an
// early structural WRN, an INF entry that only quotes a WRN marker, a run of
// INF noise and a tail WRN) through the unchanged verifier over the fake
// engine, and judges the result with the existing selection, generation and
// limits ledgers. It shows that the native receipt asks of the verifier what
// the ledgers already characterize; it does not show what a real engine
// retains. The fake engine honors the requested window, so nothing here says
// anything about the diagnostic tail or real log retention.

type jsonFileConnectRun struct {
	report    *VerificationReport
	selection selectionLedger
	generated genLedger
	limits    limLedger
	lines     map[string]string
}

func runJSONFileConnect(t *testing.T, skipEmission string) jsonFileConnectRun {
	t.Helper()
	fx := newVerifyFixture()
	fx.config.Logs = []LogExpectation{
		{Name: "early-sentinel", Required: true, Daemon: "mon.a", Event: LogClusterWarning, Pattern: `.*ledger-id=jfr-early-x.*`},
		{Name: "tail-sentinel", Required: true, Daemon: "mon.a", Event: LogClusterWarning, Pattern: `.*ledger-id=jfr-tail-x.*`},
		{Name: "quoted-sentinel", Required: true, Daemon: "mon.a", Event: LogClusterWarning, Pattern: `.*ledger-id=jfr-quote-x.*`},
	}
	fx.docker.containers["mon"].driver = "json-file"
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
	end := verifyStart.Add(30 * time.Second)
	fx.docker.clock = end
	baseline := genStateOf(fx.docker, "mon")

	mon := fx.docker.containers["mon"]
	lines := map[string]string{}
	emit := func(id string, at time.Duration, priority, tail string) {
		line := daemonLogLine("cluster", priority, "ledger-id="+id+tail)
		lines[id] = line
		if id != skipEmission {
			mon.lines = append(mon.lines, fakeLogLine{verifyStart.Add(at), line})
		}
	}
	emit("jfr-early-x", 5*time.Second, "WRN", "")
	emit("jfr-quote-x", 6*time.Second, "INF", " quotes [WRN]")
	for i := range 10 {
		emit(fmt.Sprintf("jfr-noise-x-%02d", i), 7*time.Second+time.Duration(i)*time.Millisecond, "INF", "")
	}
	emit("jfr-tail-x", 20*time.Second, "WRN", "")

	report := fx.evaluate(t, v)
	run := jsonFileConnectRun{report: report, lines: lines}
	run.selection = buildSelectionLedger(traced.calls, nil, report, "mon")

	run.generated = genLedger{Scenario: "json-file", Baseline: baseline, Evaluated: genStateOf(fx.docker, "mon"),
		Events: []genEvent{
			{ID: "jfr-early-x", Kind: "log", Container: "mon", Generation: "first"},
			{ID: "jfr-tail-x", Kind: "log", Container: "mon", Generation: "first"},
		},
		LogsOutcome: report.Logs.Outcome, CrashesOutcome: report.Crashes.Outcome, ReportOutcome: report.Outcome}
	idOf := func(f VerificationFinding) string {
		if m := ledgerIDText.FindStringSubmatch(f.Text); m != nil {
			return m[1]
		}
		return ""
	}
	for _, finding := range report.Logs.Findings {
		if id := idOf(finding); id != "" {
			run.generated.LogsReceived = append(run.generated.LogsReceived, genReceipt{ID: id, ContainerID: finding.ContainerID})
		}
	}
	for _, source := range report.Sources {
		if source.ContainerID == "mon" {
			run.generated.SourceContainerID, run.generated.SourceStartedAt, run.generated.SourceRestarts, run.generated.SourceLogBytes = source.ContainerID, source.StartedAt, source.RestartCount, source.LogBytes
		}
	}

	run.limits = limLedger{Scenario: "json-file", Events: []limEvent{
		{ID: "jfr-early-x", Kind: "log", Container: "mon", Class: "permitted", SourceLen: len(lines["jfr-early-x"])},
		{ID: "jfr-tail-x", Kind: "log", Container: "mon", Class: "permitted", SourceLen: len(lines["jfr-tail-x"])},
	}, Logs: limCheckOf(report.Logs, idOf), Crashes: limCheckOf(report.Crashes, func(f VerificationFinding) string { return f.CrashID })}
	return run
}

// The WRN sentinels are received once each, the quoting INF entry and the
// noise make no event, the log is read once from the engine-clock interval
// after the end sample, and the three ledgers' judges accept the result.
func TestVerificationJSONFileConnectsTheLedgers(t *testing.T) {
	run := runJSONFileConnect(t, "")
	report := run.report

	if err := judgeGenerationFullCoverage(run.generated); err != nil {
		t.Fatalf("%v\nledger %+v", err, run.generated)
	}
	if err := judgeNoSilentLoss(run.generated); err != nil {
		t.Fatal(err)
	}
	if err := judgeLimitAccounting(run.limits); err != nil {
		t.Fatalf("%v\nledger %+v", err, run.limits)
	}
	if err := judgeAbsence(run.limits, "log"); err != nil {
		t.Fatal(err)
	}

	got := receiptIDs(run.generated.LogsReceived)
	if len(got) != 2 || got[0] != "jfr-early-x" || got[1] != "jfr-tail-x" {
		t.Fatalf("received %v", got)
	}
	for _, text := range []string{"jfr-quote-x", "jfr-noise-x"} {
		for _, finding := range report.Logs.Findings {
			if strings.Contains(finding.Text, text) {
				t.Fatalf("%s became an event: %+v", text, finding)
			}
		}
	}
	// The quoted marker satisfies nothing, so the verification is
	// unexpected on that expectation alone.
	byName := map[string]ExpectationResult{}
	for _, result := range report.Logs.Expectations {
		byName[result.Name] = result
	}
	if !byName["early-sentinel"].Satisfied || !byName["tail-sentinel"].Satisfied || byName["quoted-sentinel"].Satisfied || byName["quoted-sentinel"].Matched != 0 || report.Logs.Outcome != VerificationUnexpected {
		t.Fatalf("expectations %+v outcome %s", byName, report.Logs.Outcome)
	}

	// Selection and read order: one log read of the source, after the
	// end-clock sample, over exactly the window.
	s := run.selection
	if s.LogsDisposition != "observed" || len(s.Logs.Gaps) != 0 || len(s.Logs.ReadSeq) != 1 || s.Logs.ReadSeq[0] <= s.EndSample ||
		!s.Logs.Start.Equal(verifyStart) || !s.Logs.End.Equal(verifyStart.Add(30*time.Second)) || s.Source.LogDriver != "json-file" || s.Source.LogBytes <= 0 {
		t.Fatalf("selection ledger %+v", s)
	}
}

// Negative control: an event that the ledger lists but that never reached the
// engine's log is a missing receipt for the generation judge, not a pass.
func TestVerificationJSONFileConnectRefusesAMissingSentinel(t *testing.T) {
	run := runJSONFileConnect(t, "jfr-tail-x")
	err := judgeGenerationFullCoverage(run.generated)
	if err == nil || !strings.Contains(err.Error(), "jfr-tail-x was received 0 times") {
		t.Fatalf("the generation judge did not refuse: %v", err)
	}
	if err := judgeNoSilentLoss(run.generated); err == nil {
		t.Fatal("a missing event without a gap passed the no-silent-loss judge")
	}
}
