//go:build all || integration

package integration_test

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/finalize"
)

// This file holds the receipt and the strict judge of the native
// same-generation json-file observation. It connects the maintained fake
// engine ledgers of package ceph to one controlled native observation, using
// only the exported report, because those ledgers are unexported test code:
//
//   - the selection ledger (log-positive control): a selected generation on
//     the non-rotating json-file driver, bytes read, no log gap, and an
//     engine interval whose clock samples bracket the evaluation;
//   - the generation ledger: the same container, start time and restart
//     count at both ends, every received event emitted there, none from
//     another source;
//   - the limits ledger: the check's own counts cover its findings and its
//     omission count, the retained text stays within its bound, a truncated
//     finding is flagged, and an absence claim needs no gap, no omission and
//     no truncation.
//
// The judge accepts only a complete receipt. A missing, empty or wrongly
// typed piece of evidence is an error, never a pass, and a refused log source
// (an unsupported driver or a rotating json-file) is a separate disposition
// that never satisfies the log-positive judge.

// The roles of the controlled events. The early WRN and the tail WRN are
// structural cluster log entries of level WRN; the quoted INF entry carries a
// WRN marker in its message only; the noise entries are INF and exist to push
// the early WRN out of the diagnostic tail.
const (
	jsonFileRoleEarly = "early-wrn"
	jsonFileRoleQuote = "quoted-inf"
	jsonFileRoleNoise = "noise-inf"
	jsonFileRoleTail  = "tail-wrn"
)

// jsonFileFindingText is the bound of the text that a report finding keeps
// (the verifier's own maxFindingText, which is not exported).
const jsonFileFindingText = 1024

// jsonFileEvent is one controlled event. SampleBefore and SampleAfter are the
// engine's clock read immediately before and after the command that emitted
// the event, so that the emission order is recorded independently of the
// stamps that the daemon wrote.
type jsonFileEvent struct {
	ID           string    `json:"id"`
	Role         string    `json:"role"`
	SampleBefore time.Time `json:"sample_before"`
	SampleAfter  time.Time `json:"sample_after"`
}

// jsonFileGeneration is a container generation as the engine reports it.
type jsonFileGeneration struct {
	ContainerID  string `json:"container_id"`
	StartedAt    string `json:"started_at"`
	RestartCount int    `json:"restart_count"`
	LogDriver    string `json:"log_driver"`
	// MaxSize is the json-file rotation size; empty when the driver does not
	// rotate.
	MaxSize string `json:"max_size"`
	Present bool   `json:"present"`
}

// jsonFileRaw is what the engine's own log of the source holds for one
// event ID, read independently of the verifier with the engine's stamps.
type jsonFileRaw struct {
	// Priorities counts the enveloped lines by structural priority, with
	// the stamps of those lines.
	Priorities map[string]int         `json:"priorities"`
	Stamps     map[string][]time.Time `json:"stamps"`
	// Unrecognized counts lines with the ID and a literal severity marker
	// but no supported envelope; Echo counts the other lines with the ID.
	Unrecognized int `json:"unrecognized"`
	Echo         int `json:"echo"`
}

// jsonFileTail is the diagnostic tail of the source: the last Requested log
// lines that the diagnostics collector kept.
type jsonFileTail struct {
	Requested int    `json:"requested"`
	Error     string `json:"error,omitempty"`
	Truncated bool   `json:"truncated"`
	Lines     int    `json:"lines"`
	// Holds says, for each event ID, whether the tail contains it.
	Holds map[string]bool `json:"holds"`
}

type jsonFileElapsed struct {
	Emission    time.Duration `json:"emission"`
	Diagnostics time.Duration `json:"diagnostics"`
	Evaluation  time.Duration `json:"evaluation"`
}

// jsonFileReceipt is everything the native scenario retains.
type jsonFileReceipt struct {
	Source string          `json:"source"`
	Events []jsonFileEvent `json:"events"`
	// Baseline is the source's generation before the activation and
	// Evaluated its generation after the evaluation, both inspected by the
	// test.
	Baseline  jsonFileGeneration `json:"baseline"`
	Evaluated jsonFileGeneration `json:"evaluated"`
	// ClockBefore was read before the evaluation started and ClockAfter
	// after it returned, on the engine's clock like the window.
	ClockBefore time.Time                    `json:"clock_before"`
	ClockAfter  time.Time                    `json:"clock_after"`
	Raw         map[string]jsonFileRaw       `json:"raw"`
	RawBytes    int64                        `json:"raw_bytes"`
	RawLines    int                          `json:"raw_lines"`
	Tail        jsonFileTail                 `json:"tail"`
	Elapsed     jsonFileElapsed              `json:"elapsed"`
	Report      *finalize.VerificationReport `json:"-"`
}

// The expectations of the scenario, one per structural WRN role and one that
// only a quoting INF entry could satisfy.
const (
	jsonFileEarlyName = "early-sentinel"
	jsonFileTailName  = "tail-sentinel"
	jsonFileQuoteName = "quoted-sentinel"
)

// jsonFileDisposition says what the verifier did with the source's log:
// "observed" when it read the log, "rejected" when it refused the source for
// its log driver or rotation, "gapped" for any other log gap, and "absent"
// when the report has no such source.
func jsonFileDisposition(report *finalize.VerificationReport, source string) string {
	if report == nil || !slices.ContainsFunc(report.Sources, func(s finalize.VerificationSource) bool { return s.Source == source }) {
		return "absent"
	}
	disposition := "observed"
	for _, gap := range report.Logs.Gaps {
		if gap.Source != source {
			continue
		}
		if strings.Contains(gap.Reason, "is not covered") || strings.Contains(gap.Reason, "rotates") {
			return "rejected"
		}
		disposition = "gapped"
	}
	return disposition
}

// jsonFileIDPattern matches an event ID as a whole token.
func jsonFileIDPattern(id string) *regexp.Regexp {
	return regexp.MustCompile(regexp.QuoteMeta(id) + `(?:[^A-Za-z0-9]|$)`)
}

// jsonFileReceived lists the findings of the report's log check that carry
// the event ID.
func jsonFileReceived(report *finalize.VerificationReport, id string) []finalize.VerificationFinding {
	pattern := jsonFileIDPattern(id)
	var found []finalize.VerificationFinding
	for _, finding := range report.Logs.Findings {
		if pattern.MatchString(finding.Text) {
			found = append(found, finding)
		}
	}
	return found
}

// judgeJSONFileReceipt accepts only a receipt that supports the controlled
// claim for one source generation: its log was read from the non-rotating
// json-file driver without any log gap; both structural WRN sentinels were
// received exactly as often as the engine's own log holds them in the
// window; the quoting INF entry and the noise made no event; the early
// sentinel, which the timestamp window holds, is absent from a diagnostic
// tail that the noise overflowed; and nothing was omitted or truncated.
// Every piece of evidence is checked for presence and shape first.
func judgeJSONFileReceipt(r jsonFileReceipt) error {
	var problems []string
	bad := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	report := r.Report
	if report == nil {
		return errors.New("no report")
	}
	if r.Source == "" {
		return errors.New("no source")
	}

	// Disposition: a refused source is its own result.
	switch d := jsonFileDisposition(report, r.Source); d {
	case "observed":
	case "rejected":
		bad("the log source was refused (%s), not observed: %+v", d, report.Logs.Gaps)
	default:
		bad("the log disposition is %q: %+v", d, report.Logs.Gaps)
	}
	if len(report.Logs.Gaps) != 0 {
		bad("log gaps %+v", report.Logs.Gaps)
	}

	// Generation: identified, non-rotating json-file, the same at both ends.
	b, e := r.Baseline, r.Evaluated
	if !b.Present || b.ContainerID == "" || b.StartedAt == "" {
		bad("the activation generation is not identified: %+v", b)
	}
	if !e.Present || e.ContainerID != b.ContainerID || e.StartedAt != b.StartedAt || e.RestartCount != b.RestartCount {
		bad("the evaluated generation %+v is not the activation's %+v", e, b)
	}
	if b.LogDriver != "json-file" || e.LogDriver != "json-file" || b.MaxSize != "" || e.MaxSize != "" {
		bad("the engine reports driver %q (max-size %q) then %q (%q); only json-file without rotation is covered", b.LogDriver, b.MaxSize, e.LogDriver, e.MaxSize)
	}
	var source *finalize.VerificationSource
	if i := slices.IndexFunc(report.Sources, func(s finalize.VerificationSource) bool { return s.Source == r.Source }); i < 0 {
		bad("no %s source", r.Source)
	} else {
		source = &report.Sources[i]
		if source.ContainerID != b.ContainerID || source.StartedAt != b.StartedAt || source.RestartCount != b.RestartCount || source.ImageID == "" {
			bad("the report's source %+v is not the inspected generation %+v", *source, b)
		}
		if source.LogDriver != "json-file" {
			bad("the report's log driver is %q", source.LogDriver)
		}
		if source.LogBytes <= 0 {
			bad("no log bytes were read")
		}
	}

	// Interval and order, on the engine's clock.
	w := report.Window
	if w.StartedAt.IsZero() || w.EndedAt.IsZero() || !w.EndedAt.After(w.StartedAt) {
		bad("the window %v..%v is missing or empty", w.StartedAt, w.EndedAt)
	}
	if r.ClockBefore.IsZero() || r.ClockAfter.IsZero() || r.ClockBefore.After(w.EndedAt) || w.EndedAt.After(r.ClockAfter) {
		bad("the engine clock samples %v and %v do not bracket the window end %v", r.ClockBefore, r.ClockAfter, w.EndedAt)
	}

	// The check's own accounting (limits ledger).
	switch report.Logs.Outcome {
	case finalize.VerificationClean, finalize.VerificationUnexpected, finalize.VerificationIncomplete:
	default:
		bad("the log outcome %q is missing or unknown", report.Logs.Outcome)
	}
	if report.Logs.OmittedFindings != 0 {
		bad("%d findings were omitted", report.Logs.OmittedFindings)
	}
	if report.Logs.Permitted+report.Logs.Unexpected != len(report.Logs.Findings)+report.Logs.OmittedFindings {
		bad("the check counts %d permitted and %d unexpected but retains %d findings and omits %d",
			report.Logs.Permitted, report.Logs.Unexpected, len(report.Logs.Findings), report.Logs.OmittedFindings)
	}
	for _, finding := range report.Logs.Findings {
		if len(finding.Text) > jsonFileFindingText {
			bad("a finding keeps %d bytes of text, more than %d", len(finding.Text), jsonFileFindingText)
		}
	}

	// Events: complete, unique, in emission order.
	roles := map[string]int{}
	ids := map[string]bool{}
	var previous *jsonFileEvent
	for i := range r.Events {
		ev := &r.Events[i]
		if ev.ID == "" || ev.SampleBefore.IsZero() || ev.SampleAfter.IsZero() || ev.SampleAfter.Before(ev.SampleBefore) {
			bad("incomplete event %+v", *ev)
			continue
		}
		if !slices.Contains([]string{jsonFileRoleEarly, jsonFileRoleQuote, jsonFileRoleNoise, jsonFileRoleTail}, ev.Role) {
			bad("event %s has the unknown role %q", ev.ID, ev.Role)
			continue
		}
		if ids[ev.ID] {
			bad("event ID %s is used twice", ev.ID)
		}
		ids[ev.ID] = true
		roles[ev.Role]++
		if !w.StartedAt.IsZero() && ev.SampleBefore.Before(w.StartedAt) {
			bad("event %s was emitted (clock %v) before the window start %v", ev.ID, ev.SampleBefore, w.StartedAt)
		}
		if !w.EndedAt.IsZero() && ev.SampleAfter.After(w.EndedAt) {
			bad("event %s was emitted (clock %v) after the window end %v", ev.ID, ev.SampleAfter, w.EndedAt)
		}
		if previous != nil && ev.SampleBefore.Before(previous.SampleAfter) && previous.Role != ev.Role {
			bad("event %s (%s) was emitted before event %s (%s) had finished", ev.ID, ev.Role, previous.ID, previous.Role)
		}
		previous = ev
	}
	for _, role := range []string{jsonFileRoleEarly, jsonFileRoleQuote, jsonFileRoleNoise, jsonFileRoleTail} {
		if roles[role] == 0 {
			bad("no %s event", role)
		}
	}
	if roles[jsonFileRoleEarly] != 1 || roles[jsonFileRoleQuote] != 1 || roles[jsonFileRoleTail] != 1 {
		bad("the early, quoted and tail roles must hold one event each, not %d, %d and %d", roles[jsonFileRoleEarly], roles[jsonFileRoleQuote], roles[jsonFileRoleTail])
	}
	if order := jsonFileRoleOrder(r.Events); order != "" {
		bad("%s", order)
	}

	expectation := func(name string) *finalize.ExpectationResult {
		i := slices.IndexFunc(report.Logs.Expectations, func(x finalize.ExpectationResult) bool { return x.Name == name })
		if i < 0 {
			bad("no expectation %s", name)
			return nil
		}
		return &report.Logs.Expectations[i]
	}
	wanted := map[string]string{jsonFileRoleEarly: jsonFileEarlyName, jsonFileRoleTail: jsonFileTailName, jsonFileRoleQuote: jsonFileQuoteName}

	for _, ev := range r.Events {
		raw, ok := r.Raw[ev.ID]
		if !ok || raw.Priorities == nil {
			bad("no raw engine log evidence for %s", ev.ID)
			continue
		}
		received := jsonFileReceived(report, ev.ID)
		inWindow := func(priority string) int {
			n := 0
			for _, stamp := range raw.Stamps[priority] {
				if !stamp.Before(w.StartedAt) && !stamp.After(w.EndedAt) {
					n++
				}
			}
			return n
		}
		for priority, count := range raw.Priorities {
			if len(raw.Stamps[priority]) != count {
				bad("raw evidence for %s counts %d %s lines but holds %d stamps", ev.ID, count, priority, len(raw.Stamps[priority]))
			}
		}
		switch ev.Role {
		case jsonFileRoleEarly, jsonFileRoleTail:
			if raw.Priorities["WRN"] < 1 {
				bad("the engine log holds no WRN line for %s (%+v), so the control is void", ev.ID, raw.Priorities)
			}
			if inWindow("WRN") != raw.Priorities["WRN"] {
				bad("%s: %d of %d raw WRN lines lie outside the window", ev.ID, raw.Priorities["WRN"]-inWindow("WRN"), raw.Priorities["WRN"])
			}
			for priority := range raw.Priorities {
				if priority != "WRN" && priority != "INF" {
					bad("%s appears in a %s line", ev.ID, priority)
				}
			}
			if raw.Unrecognized != 0 {
				bad("%s appears in %d lines with a literal severity marker and no envelope", ev.ID, raw.Unrecognized)
			}
			if len(received) != raw.Priorities["WRN"] || len(received) < 1 {
				bad("%s was received %d times but the engine log holds %d WRN lines for it in the window", ev.ID, len(received), raw.Priorities["WRN"])
			}
			for _, finding := range received {
				if finding.Source != r.Source || finding.ContainerID != b.ContainerID || finding.Event != string(finalize.LogClusterWarning) ||
					finding.Expectation != wanted[ev.Role] || finding.Truncated || finding.Time == "" {
					bad("finding of %s: %+v", ev.ID, finding)
				}
			}
			if x := expectation(wanted[ev.Role]); x != nil && (!x.Satisfied || x.Matched != len(received)) {
				bad("%s: %+v with %d findings", wanted[ev.Role], *x, len(received))
			}
		case jsonFileRoleQuote:
			if raw.Priorities["INF"] < 1 {
				bad("the engine log holds no INF line for %s, so the control is void", ev.ID)
			}
			for priority, count := range raw.Priorities {
				if priority != "INF" && count != 0 {
					bad("%s appears in %d %s lines, which makes the quoted-marker control void", ev.ID, count, priority)
				}
			}
			if raw.Unrecognized != 0 {
				bad("%s appears in %d lines with a literal severity marker and no envelope; the verifier reports those as a gap", ev.ID, raw.Unrecognized)
			}
			if len(received) != 0 {
				bad("the INF entry quoting a WRN marker was received as an event: %+v", received)
			}
			if x := expectation(jsonFileQuoteName); x != nil && (x.Satisfied || x.Matched != 0) {
				bad("a quoting entry satisfied %s: %+v", jsonFileQuoteName, *x)
			}
		case jsonFileRoleNoise:
			if raw.Priorities["INF"] < 1 {
				bad("the engine log holds no INF line for %s, so the noise is void", ev.ID)
			}
			for priority, count := range raw.Priorities {
				if priority != "INF" && count != 0 {
					bad("noise %s appears in %d %s lines", ev.ID, count, priority)
				}
			}
			if len(received) != 0 {
				bad("noise %s was received as an event: %+v", ev.ID, received)
			}
		}
	}

	// A finding that carries the sentinel prefix but belongs to no emitted
	// event is a stray sentinel.
	for _, finding := range report.Logs.Findings {
		if m := jsonFileAnyID.FindString(finding.Text); m != "" && !ids[m] {
			bad("finding %q carries the sentinel %s that was never emitted", finding.Text, m)
		}
	}

	if r.RawBytes <= 0 || r.RawLines <= 0 {
		bad("the independent read of the engine log is empty (%d bytes, %d lines)", r.RawBytes, r.RawLines)
	}

	// Window verification versus the diagnostic tail: the noise must have
	// pushed the early sentinel out of the tail, which still holds the tail
	// sentinel and some noise. Otherwise the pair shows nothing.
	t := r.Tail
	switch {
	case t.Requested < 1:
		bad("no diagnostic tail was requested")
	case t.Error != "":
		bad("the diagnostic tail failed: %s", t.Error)
	case t.Lines < 1 || t.Lines > t.Requested:
		bad("the diagnostic tail holds %d lines for a request of %d", t.Lines, t.Requested)
	}
	for _, ev := range r.Events {
		held, known := t.Holds[ev.ID]
		if !known {
			bad("the diagnostic tail has no answer for %s", ev.ID)
			continue
		}
		switch ev.Role {
		case jsonFileRoleEarly:
			if held {
				bad("the early sentinel %s is still in the diagnostic tail, so the noise did not overflow it", ev.ID)
			}
		case jsonFileRoleTail:
			if !held {
				bad("the tail sentinel %s is missing from the diagnostic tail", ev.ID)
			}
		}
	}
	noiseInTail := 0
	for _, ev := range r.Events {
		if ev.Role == jsonFileRoleNoise && t.Holds[ev.ID] {
			noiseInTail++
		}
	}
	if noiseInTail == 0 {
		bad("the diagnostic tail holds none of the noise")
	}

	if r.Elapsed.Emission <= 0 || r.Elapsed.Diagnostics <= 0 || r.Elapsed.Evaluation <= 0 {
		bad("elapsed times are missing: %+v", r.Elapsed)
	}
	if len(problems) == 0 {
		return nil
	}
	return errors.New(strings.Join(problems, "; "))
}

// jsonFileAnyID finds any sentinel of the scenario in a text; the IDs all
// start with "jfr-".
var jsonFileAnyID = regexp.MustCompile(`jfr-[a-z]+-[0-9a-f]+(?:-[0-9]+)?`)

// jsonFileRoleOrder checks the emission order of the roles: early, quoted,
// noise, tail, each by the clock samples of its commands.
func jsonFileRoleOrder(events []jsonFileEvent) string {
	rank := map[string]int{jsonFileRoleEarly: 0, jsonFileRoleQuote: 1, jsonFileRoleNoise: 2, jsonFileRoleTail: 3}
	for i := 1; i < len(events); i++ {
		a, b := events[i-1], events[i]
		ra, oka := rank[a.Role]
		rb, okb := rank[b.Role]
		if oka && okb && rb < ra {
			return fmt.Sprintf("event %s (%s) comes after %s (%s) in the receipt, against the emission order", b.ID, b.Role, a.ID, a.Role)
		}
	}
	return ""
}

// jsonFileRawLines reads engine log lines ("<stamp> <text>", as the engine
// prints them with timestamps) and returns, per ID, what they hold. Lines
// whose stamp does not parse are not evidence and are counted apart.
func jsonFileRawLines(lines []string, ids []string) (map[string]jsonFileRaw, int) {
	out := map[string]jsonFileRaw{}
	for _, id := range ids {
		out[id] = jsonFileRaw{Priorities: map[string]int{}, Stamps: map[string][]time.Time{}}
	}
	unstamped := 0
	for _, line := range lines {
		if line == "" {
			continue
		}
		stamp, text, _ := strings.Cut(line, " ")
		when, err := time.Parse(time.RFC3339Nano, stamp)
		if err != nil {
			unstamped++
			continue
		}
		for _, id := range ids {
			if !jsonFileIDPattern(id).MatchString(text) {
				continue
			}
			raw := out[id]
			match := rawDaemonEnvelope.FindStringSubmatch(text)
			if match == nil {
				match = rawClusterEnvelope.FindStringSubmatch(text)
			}
			switch {
			case match != nil:
				raw.Priorities[match[1]]++
				raw.Stamps[match[1]] = append(raw.Stamps[match[1]], when.UTC())
			case rawSeverityMarker.MatchString(text):
				raw.Unrecognized++
			default:
				raw.Echo++
			}
			out[id] = raw
		}
	}
	return out, unstamped
}

// jsonFileTailFrom summarizes one diagnostic log artifact for the event IDs.
func jsonFileTailFrom(artifact diagnostics.Artifact, requested int, ids []string) jsonFileTail {
	tail := jsonFileTail{Requested: requested, Error: artifact.Error, Truncated: artifact.Truncated, Holds: map[string]bool{}}
	for _, line := range strings.Split(artifact.Data, "\n") {
		if strings.TrimSpace(line) != "" {
			tail.Lines++
		}
	}
	for _, id := range ids {
		tail.Holds[id] = jsonFileIDPattern(id).MatchString(artifact.Data)
	}
	return tail
}

// jsonFileSummary is the compact record that the scenario logs on every
// outcome. It carries the identities, counts, bytes and times, not the
// report's findings.
type jsonFileSummary struct {
	Source      string             `json:"source"`
	Disposition string             `json:"disposition"`
	Baseline    jsonFileGeneration `json:"baseline"`
	Evaluated   jsonFileGeneration `json:"evaluated"`
	WindowStart time.Time          `json:"window_start"`
	WindowEnd   time.Time          `json:"window_end"`
	ClockBefore time.Time          `json:"clock_before"`
	ClockAfter  time.Time          `json:"clock_after"`
	Events      []jsonFileEvent    `json:"events"`
	// Received counts the findings per event ID; RawCounts the engine log
	// lines per ID and priority.
	Received  map[string]int            `json:"received"`
	RawCounts map[string]map[string]int `json:"raw_counts"`
	LogGaps   []string                  `json:"log_gaps"`
	Outcome   string                    `json:"outcome"`
	Findings  int                       `json:"findings"`
	Omitted   int                       `json:"omitted"`
	Truncated int                       `json:"truncated"`
	// LogBytes is what the verifier read in the window; RawBytes and
	// RawLines what the independent read of the whole engine log holds.
	LogBytes int64           `json:"log_bytes"`
	RawBytes int64           `json:"raw_bytes"`
	RawLines int             `json:"raw_lines"`
	Tail     jsonFileTail    `json:"tail"`
	Elapsed  jsonFileElapsed `json:"elapsed"`
}

// summary describes a receipt that may be partial: whatever is missing stays
// empty in the record.
func (r jsonFileReceipt) summary() jsonFileSummary {
	s := jsonFileSummary{Source: r.Source, Baseline: r.Baseline, Evaluated: r.Evaluated, ClockBefore: r.ClockBefore, ClockAfter: r.ClockAfter,
		Events: r.Events, Received: map[string]int{}, RawCounts: map[string]map[string]int{}, LogGaps: []string{}, RawBytes: r.RawBytes, RawLines: r.RawLines,
		Tail: r.Tail, Elapsed: r.Elapsed, Disposition: "absent"}
	for id, raw := range r.Raw {
		s.RawCounts[id] = raw.Priorities
	}
	if r.Report == nil {
		return s
	}
	report := r.Report
	s.Disposition = jsonFileDisposition(report, r.Source)
	s.WindowStart, s.WindowEnd = report.Window.StartedAt, report.Window.EndedAt
	s.Outcome = string(report.Logs.Outcome)
	s.Findings, s.Omitted = len(report.Logs.Findings), report.Logs.OmittedFindings
	for _, gap := range report.Logs.Gaps {
		s.LogGaps = append(s.LogGaps, gap.Source+": "+gap.Reason)
	}
	for _, finding := range report.Logs.Findings {
		if finding.Truncated {
			s.Truncated++
		}
	}
	for _, ev := range r.Events {
		s.Received[ev.ID] = len(jsonFileReceived(report, ev.ID))
	}
	for _, source := range report.Sources {
		if source.Source == r.Source {
			s.LogBytes = source.LogBytes
		}
	}
	return s
}

// jsonFileQuoteCommand is the raw MON command that logs, at INF, a message
// quoting a WRN marker. The MON echoes every command it handles to its own
// output outside any log envelope, and the verifier reports a line with a
// literal severity marker and no supported envelope as a gap. The marker is
// therefore sent as JSON escapes: the MON decodes them into a message with a
// literal marker, but the echo of the raw command never prints one. This is
// the construction of the envelope scenario (log_envelope_native_integration_test.go).
func jsonFileQuoteCommand(id string) string {
	return `{"prefix": "log", "logtext": ["` + id + ` quotes \u005bWRN\u005d"]}`
}
