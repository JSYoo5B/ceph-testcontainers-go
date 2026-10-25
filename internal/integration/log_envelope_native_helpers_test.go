//go:build all || integration

package integration_test

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The names of the two log expectations of the envelope scenario. The probe
// is a real WRN entry of the MON (a health check that failed because a
// global OSD flag was set); the quoted expectation requires a warning that
// only an INF entry quotes.
const (
	envelopeProbeName  = "warning-probe"
	envelopeQuotedName = "quoted-info"
	envelopeSource     = "mon.a"
)

// envelopeProbePattern matches the MON's cluster log entry of the failed
// OSDMAP_FLAGS health check (Monitor::do_health_to_clog) that a set noout flag
// raises.
const envelopeProbePattern = `.*Health check failed: noout flag\(s\) set \(OSDMAP_FLAGS\).*`

// envelopeConfig is the verification policy of the scenario for one quoted
// sentinel token.
func envelopeConfig(name, token string) ceph.VerificationConfig {
	return ceph.VerificationConfig{
		Name:        name,
		MaxFindings: 4096,
		Logs: []ceph.LogExpectation{
			{Name: envelopeProbeName, Required: true, Daemon: envelopeSource, Event: ceph.LogClusterWarning, Pattern: envelopeProbePattern},
			{Name: envelopeQuotedName, Required: true, Daemon: envelopeSource, Event: ceph.LogClusterWarning, Pattern: `.*` + regexp.QuoteMeta(token) + `.*`},
		},
	}
}

// The log envelopes of the pinned Ceph source, read independently of the
// library under test so that the raw container output is its own witness:
// a daemon's own log of a cluster log entry, and a cluster log entry as the
// MON writes it to its output.
var (
	rawDaemonEnvelope  = regexp.MustCompile(`^\S+ +[0-9a-f]+ +-?\d+ +log_channel\([A-Za-z0-9_.-]+\) log \[(DBG|INF|SEC|WRN|ERR)\] : `)
	rawClusterEnvelope = regexp.MustCompile(`^(?:[A-Za-z0-9_.-]+ )?\S+ \S+ \(\S+\) \d+ : [A-Za-z0-9_.-]+ \[(DBG|INF|SEC|WRN|ERR)\](?: |$)`)
)

// envelopeRaw counts the raw container lines that contain a token by the
// priority of their structural envelope. Unrecognized counts the lines that
// contain the token without a recognized envelope.
type envelopeRaw struct {
	Priorities   map[string]int
	Unrecognized int
}

// rawEnvelopeCounts classifies the lines that contain token.
func rawEnvelopeCounts(lines []string, token string) envelopeRaw {
	raw := envelopeRaw{Priorities: map[string]int{}}
	if token == "" {
		return raw
	}
	for _, line := range lines {
		if !strings.Contains(line, token) {
			continue
		}
		match := rawDaemonEnvelope.FindStringSubmatch(line)
		if match == nil {
			match = rawClusterEnvelope.FindStringSubmatch(line)
		}
		if match == nil {
			raw.Unrecognized++
			continue
		}
		raw.Priorities[match[1]]++
	}
	return raw
}

// judgeEnvelopeEvidence decides whether one report and the raw counts of the
// MON's output show the structural discrimination for the quoted token: the
// real WRN probe satisfied the required warning, the INF entries that quote
// a warning marker did not, nothing was left uninspected, and both sides
// agree. Missing, empty or wrongly shaped evidence is an error, never a pass.
func judgeEnvelopeEvidence(report *ceph.VerificationReport, token string, probe, quoted envelopeRaw) error {
	if report == nil {
		return errors.New("no report")
	}
	if token == "" {
		return errors.New("no quoted token")
	}
	var problems []string
	fail := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }

	index := slices.IndexFunc(report.Sources, func(source ceph.VerificationSource) bool { return source.Source == envelopeSource })
	if index < 0 {
		fail("no %s source", envelopeSource)
	} else {
		source := report.Sources[index]
		if source.ContainerID == "" || source.ImageID == "" || source.StartedAt == "" {
			fail("%s generation is not identified: %+v", envelopeSource, source)
		}
		if source.LogDriver != "json-file" {
			fail("%s log driver %q is not the non-rotating json-file", envelopeSource, source.LogDriver)
		}
		if source.LogBytes <= 0 {
			fail("%s log stream is empty", envelopeSource)
		}
	}
	if len(report.Logs.Gaps) != 0 {
		fail("log gaps %+v", report.Logs.Gaps)
	}
	if report.Logs.OmittedFindings != 0 {
		fail("%d findings were omitted", report.Logs.OmittedFindings)
	}
	find := func(name string) *ceph.ExpectationResult {
		i := slices.IndexFunc(report.Logs.Expectations, func(result ceph.ExpectationResult) bool { return result.Name == name })
		if i < 0 {
			fail("no expectation %s", name)
			return nil
		}
		return &report.Logs.Expectations[i]
	}
	if result := find(envelopeProbeName); result != nil && (!result.Satisfied || result.Matched < 1) {
		fail("the real WRN probe did not satisfy %s: %+v", envelopeProbeName, *result)
	}
	if result := find(envelopeQuotedName); result != nil && (result.Satisfied || result.Matched != 0) {
		fail("a quoting entry satisfied %s: %+v", envelopeQuotedName, *result)
	}
	for _, finding := range report.Logs.Findings {
		if strings.Contains(finding.Text, token) {
			fail("an event was derived from the quoted token: %+v", finding)
		}
		if finding.Expectation == envelopeProbeName && (finding.Source != envelopeSource || finding.Event != string(ceph.LogClusterWarning)) {
			fail("probe finding %+v", finding)
		}
	}
	// The raw output is the witness that the controls were not vacuous.
	if probe.Priorities["WRN"] < 1 {
		fail("the MON output holds no WRN probe line: %+v", probe)
	}
	if quoted.Priorities["INF"] < 1 {
		fail("the MON output holds no INF line that quotes the token: %+v", quoted)
	}
	for _, priority := range []string{"WRN", "ERR", "SEC", "DBG"} {
		if quoted.Priorities[priority] != 0 {
			fail("the token appears in %d %s lines, which makes the control void: %+v", quoted.Priorities[priority], priority, quoted)
		}
	}
	if len(problems) == 0 {
		return nil
	}
	return errors.New(strings.Join(problems, "; "))
}
