//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// These fixtures are synthetic: they build the receipt of the native
// json-file observation by hand and exercise its judge without a cluster.
// They show what the judge accepts and refuses. They say nothing about what a
// running MON writes, what the engine retains, or what the verifier reads.

var jsonFileFixtureBase = time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC)

const jsonFileFixtureNonce = "0123456789ab"

func jsonFileFixtureID(role string, n int) string {
	switch role {
	case jsonFileRoleEarly:
		return "jfr-early-" + jsonFileFixtureNonce
	case jsonFileRoleQuote:
		return "jfr-quote-" + jsonFileFixtureNonce
	case jsonFileRoleTail:
		return "jfr-tail-" + jsonFileFixtureNonce
	}
	return fmt.Sprintf("jfr-noise-%s-%02d", jsonFileFixtureNonce, n)
}

func jsonFileFixtureFinding(id, expectation string) ceph.VerificationFinding {
	return ceph.VerificationFinding{
		Source: "mon.a", ContainerID: "container-mon-a", Event: string(ceph.LogClusterWarning), Time: jsonFileFixtureBase.Add(2 * time.Second).Format(time.RFC3339Nano),
		Text:        "cluster 2026-10-26T10:00:02.000000+0000 mon.a (mon.0) 7 : cluster [WRN] " + id,
		Expectation: expectation,
	}
}

// jsonFileFixtureReceipt is a receipt the judge accepts: one source
// generation on the non-rotating json-file driver, an early and a tail WRN
// received once each, a quoting INF entry and three noise entries that made
// no event, and a diagnostic tail that the noise overflowed.
func jsonFileFixtureReceipt() jsonFileReceipt {
	at := func(seconds float64) time.Time {
		return jsonFileFixtureBase.Add(time.Duration(seconds * float64(time.Second)))
	}
	ids := map[string]string{}
	var events []jsonFileEvent
	add := func(role string, n int, before, after float64) {
		id := jsonFileFixtureID(role, n)
		ids[role+fmt.Sprint(n)] = id
		events = append(events, jsonFileEvent{ID: id, Role: role, SampleBefore: at(before), SampleAfter: at(after)})
	}
	add(jsonFileRoleEarly, 0, 1, 2)
	add(jsonFileRoleQuote, 0, 3, 4)
	for n := range 3 {
		add(jsonFileRoleNoise, n, 5, 6)
	}
	add(jsonFileRoleTail, 0, 7, 8)

	raw := map[string]jsonFileRaw{}
	holds := map[string]bool{}
	for _, ev := range events {
		switch ev.Role {
		case jsonFileRoleEarly, jsonFileRoleTail:
			// The cluster entry (WRN) and the audit entry of the command
			// that carried it (INF).
			raw[ev.ID] = jsonFileRaw{Priorities: map[string]int{"WRN": 1, "INF": 1}, Stamps: map[string][]time.Time{"WRN": {ev.SampleAfter}, "INF": {ev.SampleBefore}}, Echo: 1}
		default:
			raw[ev.ID] = jsonFileRaw{Priorities: map[string]int{"INF": 2}, Stamps: map[string][]time.Time{"INF": {ev.SampleBefore, ev.SampleAfter}}, Echo: 1}
		}
		holds[ev.ID] = ev.Role != jsonFileRoleEarly && ev.Role != jsonFileRoleQuote
	}

	report := &ceph.VerificationReport{
		SchemaVersion: 1,
		Outcome:       ceph.VerificationUnexpected,
		Window:        ceph.VerificationWindow{Phase: "test", StartedAt: at(0), EndedAt: at(60), EvaluatedAt: at(60)},
		Sources: []ceph.VerificationSource{{Source: "mon.a", Role: "mon", Name: "a", ContainerID: "container-mon-a", ImageID: "sha256:image",
			StartedAt: "2026-10-26T09:50:00Z", RestartCount: 0, State: "running", LogDriver: "json-file", LogBytes: 4096}},
		Logs: ceph.VerificationCheck{
			Outcome: ceph.VerificationUnexpected, Permitted: 2,
			Findings: []ceph.VerificationFinding{
				jsonFileFixtureFinding(jsonFileFixtureID(jsonFileRoleEarly, 0), jsonFileEarlyName),
				jsonFileFixtureFinding(jsonFileFixtureID(jsonFileRoleTail, 0), jsonFileTailName),
			},
			Expectations: []ceph.ExpectationResult{
				{Name: jsonFileEarlyName, Required: true, MinCount: 1, Matched: 1, Satisfied: true},
				{Name: jsonFileTailName, Required: true, MinCount: 1, Matched: 1, Satisfied: true},
				{Name: jsonFileQuoteName, Required: true, MinCount: 1},
			},
			Gaps: []ceph.VerificationGap{},
		},
	}
	generation := jsonFileGeneration{ContainerID: "container-mon-a", StartedAt: "2026-10-26T09:50:00Z", LogDriver: "json-file", Present: true}
	return jsonFileReceipt{
		Source: "mon.a", Events: events, Baseline: generation, Evaluated: generation,
		ClockBefore: at(59), ClockAfter: at(61), Raw: raw, RawBytes: 9000, RawLines: 40,
		Tail:    jsonFileTail{Requested: 24, Lines: 24, Holds: holds},
		Elapsed: jsonFileElapsed{Emission: 5 * time.Second, Diagnostics: time.Second, Evaluation: 2 * time.Second},
		Report:  report,
	}
}

func jsonFileFixtureEvent(r *jsonFileReceipt, role string) *jsonFileEvent {
	for i := range r.Events {
		if r.Events[i].Role == role {
			return &r.Events[i]
		}
	}
	panic("no event of role " + role)
}

func TestJSONFileReceiptAcceptsTheControlledObservation(t *testing.T) {
	r := jsonFileFixtureReceipt()
	if err := judgeJSONFileReceipt(r); err != nil {
		t.Fatal(err)
	}
	if got := jsonFileDisposition(r.Report, r.Source); got != "observed" {
		t.Fatalf("disposition %q", got)
	}
	summary := r.summary()
	data, err := json.Marshal(summary)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{jsonFileFixtureID(jsonFileRoleEarly, 0), `"disposition":"observed"`, `"log_gaps":[]`, `"log_bytes":4096`, `"raw_bytes":9000`} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("summary %s lacks %s", data, want)
		}
	}
	if summary.Received[jsonFileFixtureID(jsonFileRoleEarly, 0)] != 1 || summary.Received[jsonFileFixtureID(jsonFileRoleQuote, 0)] != 0 {
		t.Fatalf("received %v", summary.Received)
	}
}

// Each case changes one piece of evidence of the accepted receipt. The judge
// must refuse it and name the reason, so that a missing, empty, null or
// wrongly typed piece never counts as evidence.
func TestJSONFileReceiptRefusesMissingOrBrokenEvidence(t *testing.T) {
	early, tail, quote := jsonFileFixtureID(jsonFileRoleEarly, 0), jsonFileFixtureID(jsonFileRoleTail, 0), jsonFileFixtureID(jsonFileRoleQuote, 0)
	noise := jsonFileFixtureID(jsonFileRoleNoise, 0)
	cases := []struct {
		name   string
		mutate func(*jsonFileReceipt)
		want   string
	}{
		{"no report", func(r *jsonFileReceipt) { r.Report = nil }, "no report"},
		{"no source", func(r *jsonFileReceipt) { r.Source = "" }, "no source"},
		{"source missing from the report", func(r *jsonFileReceipt) { r.Report.Sources = nil }, "disposition is \"absent\""},
		{"other source only", func(r *jsonFileReceipt) { r.Report.Sources[0].Source = "mon.b" }, "no mon.a source"},
		{"empty container ID", func(r *jsonFileReceipt) { r.Baseline.ContainerID = ""; r.Evaluated.ContainerID = "" }, "not identified"},
		{"empty start time", func(r *jsonFileReceipt) { r.Baseline.StartedAt = "" }, "not identified"},
		{"baseline not present", func(r *jsonFileReceipt) { r.Baseline.Present = false }, "not identified"},
		{"removed at evaluation", func(r *jsonFileReceipt) { r.Evaluated = jsonFileGeneration{ContainerID: r.Baseline.ContainerID} }, "is not the activation's"},
		{"restarted", func(r *jsonFileReceipt) { r.Evaluated.RestartCount = 1 }, "is not the activation's"},
		{"new start time", func(r *jsonFileReceipt) { r.Evaluated.StartedAt = "2026-10-26T09:59:00Z" }, "is not the activation's"},
		{"replaced container", func(r *jsonFileReceipt) { r.Evaluated.ContainerID = "container-other" }, "is not the activation's"},
		{"report source is another generation", func(r *jsonFileReceipt) { r.Report.Sources[0].StartedAt = "2026-10-26T09:51:00Z" }, "not the inspected generation"},
		{"report source without image", func(r *jsonFileReceipt) { r.Report.Sources[0].ImageID = "" }, "not the inspected generation"},
		{"engine reports another driver", func(r *jsonFileReceipt) { r.Baseline.LogDriver, r.Evaluated.LogDriver = "local", "local" }, "only json-file without rotation"},
		{"engine reports an empty driver", func(r *jsonFileReceipt) { r.Baseline.LogDriver = "" }, "only json-file without rotation"},
		{"engine reports rotation", func(r *jsonFileReceipt) { r.Evaluated.MaxSize = "10m" }, "only json-file without rotation"},
		{"report driver empty", func(r *jsonFileReceipt) { r.Report.Sources[0].LogDriver = "" }, "report's log driver"},
		{"no log bytes", func(r *jsonFileReceipt) { r.Report.Sources[0].LogBytes = 0 }, "no log bytes"},
		{"zero window start", func(r *jsonFileReceipt) { r.Report.Window.StartedAt = time.Time{} }, "missing or empty"},
		{"zero window end", func(r *jsonFileReceipt) { r.Report.Window.EndedAt = time.Time{} }, "missing or empty"},
		{"window end not after start", func(r *jsonFileReceipt) { r.Report.Window.EndedAt = r.Report.Window.StartedAt }, "missing or empty"},
		{"no clock sample before", func(r *jsonFileReceipt) { r.ClockBefore = time.Time{} }, "do not bracket"},
		{"no clock sample after", func(r *jsonFileReceipt) { r.ClockAfter = time.Time{} }, "do not bracket"},
		{"clock sample before is after the window end", func(r *jsonFileReceipt) { r.ClockBefore = r.Report.Window.EndedAt.Add(time.Second) }, "do not bracket"},
		{"clock sample after precedes the window end", func(r *jsonFileReceipt) { r.ClockAfter = r.Report.Window.EndedAt.Add(-time.Second) }, "do not bracket"},
		{"a log gap", func(r *jsonFileReceipt) {
			r.Report.Logs.Gaps = []ceph.VerificationGap{{Source: "osd.0", Reason: "log stream ends inside a frame"}}
		}, "log gaps"},
		{"a gap of the source", func(r *jsonFileReceipt) {
			r.Report.Logs.Gaps = []ceph.VerificationGap{{Source: "mon.a", Reason: "1 log lines have no valid timestamp"}}
		}, "disposition is \"gapped\""},
		{"omitted findings", func(r *jsonFileReceipt) { r.Report.Logs.OmittedFindings = 1; r.Report.Logs.Permitted++ }, "findings were omitted"},
		{"counts do not cover the findings", func(r *jsonFileReceipt) { r.Report.Logs.Permitted = 1 }, "retains 2 findings"},
		{"unknown outcome", func(r *jsonFileReceipt) { r.Report.Logs.Outcome = "" }, "missing or unknown"},
		{"finding text over the bound", func(r *jsonFileReceipt) { r.Report.Logs.Findings[0].Text += strings.Repeat("x", 1100) }, "more than 1024"},
		{"no events", func(r *jsonFileReceipt) { r.Events = nil }, "no early-wrn event"},
		{"event without ID", func(r *jsonFileReceipt) { r.Events[0].ID = "" }, "incomplete event"},
		{"event without sample", func(r *jsonFileReceipt) { r.Events[0].SampleBefore = time.Time{} }, "incomplete event"},
		{"event sample reversed", func(r *jsonFileReceipt) { r.Events[0].SampleAfter = r.Events[0].SampleBefore.Add(-time.Second) }, "incomplete event"},
		{"unknown role", func(r *jsonFileReceipt) { r.Events[0].Role = "bogus" }, "unknown role"},
		{"duplicate ID", func(r *jsonFileReceipt) { r.Events[2].ID = r.Events[3].ID }, "used twice"},
		{"no tail event", func(r *jsonFileReceipt) { r.Events = r.Events[:len(r.Events)-1] }, "no tail-wrn event"},
		{"no noise event", func(r *jsonFileReceipt) { r.Events = append(r.Events[:2:2], r.Events[len(r.Events)-1]) }, "no noise-inf event"},
		{"two early events", func(r *jsonFileReceipt) { r.Events[1].Role = jsonFileRoleEarly }, "one event each"},
		{"emitted before the window", func(r *jsonFileReceipt) { r.Events[0].SampleBefore = r.Report.Window.StartedAt.Add(-time.Second) }, "before the window start"},
		{"emitted after the window", func(r *jsonFileReceipt) {
			r.Events[len(r.Events)-1].SampleAfter = r.Report.Window.EndedAt.Add(time.Second)
		}, "after the window end"},
		{"roles out of order", func(r *jsonFileReceipt) { r.Events[0], r.Events[1] = r.Events[1], r.Events[0] }, "against the emission order"},
		{"overlapping emissions", func(r *jsonFileReceipt) {
			jsonFileFixtureEvent(r, jsonFileRoleQuote).SampleBefore = jsonFileFixtureBase.Add(1500 * time.Millisecond)
		}, "had finished"},
		{"no raw evidence", func(r *jsonFileReceipt) { delete(r.Raw, early) }, "no raw engine log evidence"},
		{"null raw priorities", func(r *jsonFileReceipt) { r.Raw[early] = jsonFileRaw{} }, "no raw engine log evidence"},
		{"raw WRN missing", func(r *jsonFileReceipt) {
			raw := r.Raw[early]
			raw.Priorities = map[string]int{"INF": 1}
			raw.Stamps = map[string][]time.Time{"INF": raw.Stamps["INF"]}
			r.Raw[early] = raw
		}, "holds no WRN line"},
		{"raw stamp count differs", func(r *jsonFileReceipt) {
			raw := r.Raw[tail]
			raw.Stamps = map[string][]time.Time{"INF": raw.Stamps["INF"]}
			r.Raw[tail] = raw
		}, "holds 0 stamps"},
		{"raw WRN outside the window", func(r *jsonFileReceipt) {
			raw := r.Raw[early]
			raw.Stamps = map[string][]time.Time{"WRN": {jsonFileFixtureBase.Add(-time.Second)}, "INF": raw.Stamps["INF"]}
			r.Raw[early] = raw
		}, "outside the window"},
		{"raw has two WRN lines but one finding", func(r *jsonFileReceipt) {
			raw := r.Raw[early]
			raw.Priorities = map[string]int{"WRN": 2, "INF": 1}
			raw.Stamps = map[string][]time.Time{"WRN": {raw.Stamps["WRN"][0], raw.Stamps["WRN"][0]}, "INF": raw.Stamps["INF"]}
			r.Raw[early] = raw
		}, "was received 1 times but the engine log holds 2"},
		{"early sentinel in an ERR line", func(r *jsonFileReceipt) {
			raw := r.Raw[early]
			raw.Priorities["ERR"] = 1
			raw.Stamps["ERR"] = []time.Time{jsonFileFixtureBase.Add(2 * time.Second)}
			r.Raw[early] = raw
		}, "appears in a ERR line"},
		{"early sentinel unenveloped with a marker", func(r *jsonFileReceipt) {
			raw := r.Raw[early]
			raw.Unrecognized = 1
			r.Raw[early] = raw
		}, "literal severity marker"},
		{"early sentinel not received", func(r *jsonFileReceipt) {
			r.Report.Logs.Findings = r.Report.Logs.Findings[1:]
			r.Report.Logs.Permitted = 1
		}, "was received 0 times"},
		{"early sentinel received twice", func(r *jsonFileReceipt) {
			r.Report.Logs.Findings = append(r.Report.Logs.Findings, r.Report.Logs.Findings[0])
			r.Report.Logs.Permitted++
		}, "was received 2 times"},
		{"early finding from another source", func(r *jsonFileReceipt) { r.Report.Logs.Findings[0].Source = "mon.b" }, "finding of " + early},
		{"early finding from another container", func(r *jsonFileReceipt) { r.Report.Logs.Findings[0].ContainerID = "container-other" }, "finding of " + early},
		{"early finding of another kind", func(r *jsonFileReceipt) { r.Report.Logs.Findings[0].Event = string(ceph.LogClusterError) }, "finding of " + early},
		{"early finding without expectation", func(r *jsonFileReceipt) { r.Report.Logs.Findings[0].Expectation = "" }, "finding of " + early},
		{"early finding truncated", func(r *jsonFileReceipt) { r.Report.Logs.Findings[0].Truncated = true }, "finding of " + early},
		{"early finding without time", func(r *jsonFileReceipt) { r.Report.Logs.Findings[0].Time = "" }, "finding of " + early},
		{"early expectation unsatisfied", func(r *jsonFileReceipt) { r.Report.Logs.Expectations[0].Satisfied = false }, jsonFileEarlyName},
		{"early expectation missing", func(r *jsonFileReceipt) { r.Report.Logs.Expectations = r.Report.Logs.Expectations[1:] }, "no expectation " + jsonFileEarlyName},
		{"tail expectation matched differently", func(r *jsonFileReceipt) { r.Report.Logs.Expectations[1].Matched = 2 }, jsonFileTailName},
		{"quoting entry became an event", func(r *jsonFileReceipt) {
			r.Report.Logs.Findings = append(r.Report.Logs.Findings, jsonFileFixtureFinding(quote, ""))
			r.Report.Logs.Unexpected++
		}, "quoting a WRN marker was received"},
		{"quoting entry satisfied the expectation", func(r *jsonFileReceipt) {
			r.Report.Logs.Expectations[2].Satisfied, r.Report.Logs.Expectations[2].Matched = true, 1
		}, "satisfied " + jsonFileQuoteName},
		{"quote expectation missing", func(r *jsonFileReceipt) { r.Report.Logs.Expectations = r.Report.Logs.Expectations[:2] }, "no expectation " + jsonFileQuoteName},
		{"quote token also in a WRN line", func(r *jsonFileReceipt) {
			r.Raw[quote] = jsonFileRaw{Priorities: map[string]int{"INF": 1, "WRN": 1}, Stamps: map[string][]time.Time{"INF": r.Raw[quote].Stamps["INF"][:1], "WRN": {jsonFileFixtureBase.Add(4 * time.Second)}}}
		}, "quoted-marker control void"},
		{"quote token has no INF line", func(r *jsonFileReceipt) {
			r.Raw[quote] = jsonFileRaw{Priorities: map[string]int{}, Stamps: map[string][]time.Time{}}
		}, "holds no INF line"},
		{"quote token with a literal marker and no envelope", func(r *jsonFileReceipt) {
			raw := r.Raw[quote]
			raw.Unrecognized = 1
			r.Raw[quote] = raw
		}, "reports those as a gap"},
		{"noise became an event", func(r *jsonFileReceipt) {
			r.Report.Logs.Findings = append(r.Report.Logs.Findings, jsonFileFixtureFinding(noise, ""))
			r.Report.Logs.Unexpected++
		}, "noise " + noise + " was received"},
		{"noise missing from the engine log", func(r *jsonFileReceipt) {
			r.Raw[noise] = jsonFileRaw{Priorities: map[string]int{}, Stamps: map[string][]time.Time{}}
		}, "noise is void"},
		{"noise in a WRN line", func(r *jsonFileReceipt) {
			raw := r.Raw[noise]
			raw.Priorities["WRN"] = 1
			raw.Stamps["WRN"] = []time.Time{jsonFileFixtureBase.Add(5 * time.Second)}
			r.Raw[noise] = raw
		}, "noise " + noise + " appears in 1 WRN"},
		{"stray sentinel in a finding", func(r *jsonFileReceipt) {
			r.Report.Logs.Findings = append(r.Report.Logs.Findings, jsonFileFixtureFinding("jfr-early-ffffffffffff", ""))
			r.Report.Logs.Unexpected++
		}, "was never emitted"},
		{"empty independent read", func(r *jsonFileReceipt) { r.RawBytes = 0 }, "independent read"},
		{"no raw lines", func(r *jsonFileReceipt) { r.RawLines = 0 }, "independent read"},
		{"no tail requested", func(r *jsonFileReceipt) { r.Tail.Requested = 0 }, "no diagnostic tail was requested"},
		{"tail failed", func(r *jsonFileReceipt) { r.Tail.Error = "container was not created" }, "diagnostic tail failed"},
		{"tail empty", func(r *jsonFileReceipt) { r.Tail.Lines = 0 }, "holds 0 lines"},
		{"tail longer than requested", func(r *jsonFileReceipt) { r.Tail.Lines = 25 }, "holds 25 lines"},
		{"tail holds the early sentinel", func(r *jsonFileReceipt) { r.Tail.Holds[early] = true }, "noise did not overflow"},
		{"tail lacks the tail sentinel", func(r *jsonFileReceipt) { r.Tail.Holds[tail] = false }, "missing from the diagnostic tail"},
		{"tail holds no noise", func(r *jsonFileReceipt) {
			for id := range r.Tail.Holds {
				if strings.HasPrefix(id, "jfr-noise-") {
					r.Tail.Holds[id] = false
				}
			}
		}, "holds none of the noise"},
		{"tail has no answer for an event", func(r *jsonFileReceipt) { delete(r.Tail.Holds, quote) }, "no answer for " + quote},
		{"null tail answers", func(r *jsonFileReceipt) { r.Tail.Holds = nil }, "no answer for"},
		{"no emission time", func(r *jsonFileReceipt) { r.Elapsed.Emission = 0 }, "elapsed times are missing"},
		{"no diagnostics time", func(r *jsonFileReceipt) { r.Elapsed.Diagnostics = 0 }, "elapsed times are missing"},
		{"no evaluation time", func(r *jsonFileReceipt) { r.Elapsed.Evaluation = 0 }, "elapsed times are missing"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := jsonFileFixtureReceipt()
			tc.mutate(&r)
			err := judgeJSONFileReceipt(r)
			if err == nil {
				t.Fatalf("the judge accepted: %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q lacks %q", err, tc.want)
			}
		})
	}
}

// A source that the verifier refused for its driver or rotation is a result
// of its own: it is reported as rejected, never as observed, and the
// log-positive judge refuses it with that reason.
func TestJSONFileReceiptRefusedSourceIsASeparateResult(t *testing.T) {
	for _, tc := range []struct{ name, reason string }{
		{"unsupported driver", `log driver "local" is not covered; only json-file without rotation is`},
		{"rotating json-file", "the json-file driver rotates at max-size 10m and can drop lines"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := jsonFileFixtureReceipt()
			r.Report.Logs.Gaps = []ceph.VerificationGap{{Source: "mon.a", Reason: tc.reason}}
			r.Report.Logs.Outcome = ceph.VerificationIncomplete
			if got := jsonFileDisposition(r.Report, r.Source); got != "rejected" {
				t.Fatalf("disposition %q", got)
			}
			err := judgeJSONFileReceipt(r)
			if err == nil || !strings.Contains(err.Error(), "was refused") {
				t.Fatalf("the judge did not name the refusal: %v", err)
			}
			if s := r.summary(); s.Disposition != "rejected" || len(s.LogGaps) != 1 {
				t.Fatalf("summary %+v", s)
			}
		})
	}
	// A gap of another source does not make this source rejected.
	r := jsonFileFixtureReceipt()
	r.Report.Logs.Gaps = []ceph.VerificationGap{{Source: "osd.0", Reason: `log driver "local" is not covered; only json-file without rotation is`}}
	if got := jsonFileDisposition(r.Report, r.Source); got != "observed" {
		t.Fatalf("disposition %q", got)
	}
	if jsonFileDisposition(nil, "mon.a") != "absent" {
		t.Fatal("a nil report is not absent")
	}
}

// The raw lines are classified by their structural envelope, like the
// verifier does, and each line is attributed to an ID as a whole token.
func TestJSONFileRawLinesUseTheStructuralPriority(t *testing.T) {
	early, other := "jfr-early-0123456789ab", "jfr-noise-0123456789ab-01"
	lines := []string{
		"2026-10-26T10:00:02.100000000Z cluster 2026-10-26T10:00:02.000000+0000 mon.a (mon.0) 7 : cluster [WRN] " + early,
		"2026-10-26T10:00:02.200000000Z audit 2026-10-26T10:00:02.000000+0000 mon.a (mon.0) 8 : audit [INF] from='client.1' cmd=[{\"logtext\": [\"" + early + "\"]}]: dispatch",
		"2026-10-26T10:00:02.300000000Z 2026-10-26T10:00:02.000+0000 7f0 0 log_channel(cluster) log [WRN] : " + early,
		"2026-10-26T10:00:02.400000000Z mon.a handle_command mon_command({\"logtext\": [\"" + early + "\"]} v 0)",
		"2026-10-26T10:00:02.500000000Z free text [WRN] " + early,
		"no stamp here " + early,
		"2026-10-26T10:00:02.600000000Z cluster 2026-10-26T10:00:02.000000+0000 mon.a (mon.0) 9 : cluster [INF] " + other,
		"2026-10-26T10:00:02.700000000Z cluster 2026-10-26T10:00:02.000000+0000 mon.a (mon.0) 10 : cluster [INF] " + other + "1",
	}
	raw, unstamped := jsonFileRawLines(lines, []string{early, other})
	if unstamped != 1 {
		t.Fatalf("unstamped %d", unstamped)
	}
	got := raw[early]
	if got.Priorities["WRN"] != 2 || got.Priorities["INF"] != 1 || got.Unrecognized != 1 || got.Echo != 1 || len(got.Stamps["WRN"]) != 2 {
		t.Fatalf("early %+v", got)
	}
	if o := raw[other]; o.Priorities["INF"] != 1 || len(o.Priorities) != 1 {
		t.Fatalf("other %+v: the longer ID must not count", o)
	}
}

func TestJSONFileTailFromAnswersPerWholeID(t *testing.T) {
	artifact := ceph.DiagnosticArtifact{Kind: "container-logs", Data: "line one\n\nline jfr-noise-aa-11 two\nlast jfr-tail-aa\n"}
	tail := jsonFileTailFrom(artifact, 8, []string{"jfr-noise-aa-1", "jfr-noise-aa-11", "jfr-tail-aa", "jfr-early-aa"})
	if tail.Lines != 3 || tail.Requested != 8 || tail.Holds["jfr-noise-aa-1"] || !tail.Holds["jfr-noise-aa-11"] || !tail.Holds["jfr-tail-aa"] || tail.Holds["jfr-early-aa"] {
		t.Fatalf("tail %+v", tail)
	}
	failed := jsonFileTailFrom(ceph.DiagnosticArtifact{Error: "boom", Truncated: true}, 8, []string{"x"})
	if failed.Error != "boom" || !failed.Truncated || failed.Lines != 0 || failed.Holds["x"] {
		t.Fatalf("failed tail %+v", failed)
	}
}

// A partial receipt can always be summarized, so that the scenario logs what
// it has on every outcome.
func TestJSONFileSummaryOfAPartialReceipt(t *testing.T) {
	s := jsonFileReceipt{Source: "mon.a"}.summary()
	if s.Disposition != "absent" || s.Received == nil || s.LogGaps == nil {
		t.Fatalf("summary %+v", s)
	}
	if _, err := json.Marshal(s); err != nil {
		t.Fatal(err)
	}
}

// The quote command must reach the MON without a literal severity marker (its
// echo is unframed and would be a gap), yet decode to a message that has one.
// The literal command is the negative control: it is what the first native
// attempt sent.
func TestJSONFileQuoteCommandHidesTheMarkerFromTheEcho(t *testing.T) {
	id := "jfr-quote-0123456789ab"
	command := jsonFileQuoteCommand(id)
	if rawSeverityMarker.MatchString(command) {
		t.Fatalf("the raw command carries a literal marker: %s", command)
	}
	var decoded struct {
		Prefix  string   `json:"prefix"`
		Logtext []string `json:"logtext"`
	}
	if err := json.Unmarshal([]byte(command), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Prefix != "log" || len(decoded.Logtext) != 1 || decoded.Logtext[0] != id+" quotes [WRN]" {
		t.Fatalf("decoded command %+v", decoded)
	}
	literal := `{"prefix": "log", "logtext": ["` + id + ` quotes [WRN]"]}`
	if !rawSeverityMarker.MatchString(literal) {
		t.Fatal("the negative control carries no literal marker")
	}
	// An echo of the literal command is an unenveloped marker line: the raw
	// read counts it, and the judge refuses a receipt that has one.
	echo := "2026-10-26T10:00:03.000000000Z mon.a@0(leader) handle_command mon_command(" + literal + " v 0)"
	raw, _ := jsonFileRawLines([]string{echo}, []string{id})
	if raw[id].Unrecognized != 1 {
		t.Fatalf("raw %+v", raw[id])
	}
	echo = "2026-10-26T10:00:03.000000000Z mon.a@0(leader) handle_command mon_command(" + command + " v 0)"
	if raw, _ = jsonFileRawLines([]string{echo}, []string{id}); raw[id].Unrecognized != 0 || raw[id].Echo != 1 {
		t.Fatalf("the escaped echo %+v", raw[id])
	}
}

// The receipt of the first native attempt: the quoted entry's literal marker
// echo is one unenveloped line, the verifier reports exactly one gap and the
// quoted expectation matches nothing. The judge refuses it as gapped and
// names the unenveloped line; the verifier's gap is not accepted.
func TestJSONFileReceiptRefusesTheFirstNativeShape(t *testing.T) {
	r := jsonFileFixtureReceipt()
	quote := jsonFileFixtureID(jsonFileRoleQuote, 0)
	r.Report.Logs.Gaps = []ceph.VerificationGap{{Source: "mon.a", Reason: "1 log lines carry an [ERR], [SEC] or [WRN] marker without a supported log envelope, so their severity was not classified"}}
	r.Report.Logs.Outcome = ceph.VerificationUnexpected
	raw := r.Raw[quote]
	raw.Unrecognized = 1
	r.Raw[quote] = raw
	if got := jsonFileDisposition(r.Report, r.Source); got != "gapped" {
		t.Fatalf("disposition %q", got)
	}
	err := judgeJSONFileReceipt(r)
	if err == nil || !strings.Contains(err.Error(), `disposition is "gapped"`) || !strings.Contains(err.Error(), "reports those as a gap") {
		t.Fatalf("the judge did not refuse the first native shape: %v", err)
	}
}
