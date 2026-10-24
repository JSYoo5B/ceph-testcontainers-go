package ceph

import (
	"strings"
	"testing"
	"time"
)

// clusterLogLine is a cluster log entry as the MON writes it (operator<< of
// LogEntry), with the channel prefix of mon_cluster_log_to_stderr when
// prefixed is set.
func clusterLogLine(prefixed bool, channel, priority, message string) string {
	line := "2026-10-24T01:02:03.123456+0000 mon.a (mon.0) 17 : " + channel + " [" + priority + "] " + message
	if prefixed {
		return channel + " " + line
	}
	return line
}

// The classifier takes the severity from the structure of the supported log
// envelopes and never from a marker inside a message.
func TestClassifyVerificationLine(t *testing.T) {
	tests := []struct {
		name         string
		line         string
		kind         LogEvent
		channel      string
		priority     string
		unclassified bool
	}{
		{"daemon WRN", daemonLogLine("cluster", "WRN", "sentinel"), LogClusterWarning, "cluster", "WRN", false},
		{"daemon ERR", daemonLogLine("cluster", "ERR", "sentinel"), LogClusterError, "cluster", "ERR", false},
		{"daemon SEC", daemonLogLine("cluster", "SEC", "sentinel"), LogClusterSecurity, "cluster", "SEC", false},
		{"daemon negative level", "2026-10-24T01:02:03.123+0000 7f3a1c0e5640 -1 log_channel(cluster) log [ERR] : sentinel", LogClusterError, "cluster", "ERR", false},
		{"daemon INF is no event", daemonLogLine("cluster", "INF", "sentinel"), "", "cluster", "INF", false},
		{"daemon DBG is no event", daemonLogLine("cluster", "DBG", "sentinel"), "", "cluster", "DBG", false},
		{"cluster log WRN", clusterLogLine(false, "cluster", "WRN", "sentinel"), LogClusterWarning, "cluster", "WRN", false},
		{"prefixed cluster log WRN", clusterLogLine(true, "cluster", "WRN", "sentinel"), LogClusterWarning, "cluster", "WRN", false},
		{"cluster log with an empty message", "2026-10-24T01:02:03.123456+0000 mon.a (mon.0) 17 : cluster [WRN]", LogClusterWarning, "cluster", "WRN", false},
		// The cases of the issue: the message never decides the kind.
		{"INF quoting WRN", daemonLogLine("cluster", "INF", "sentinel quotes [WRN]"), "", "cluster", "INF", false},
		{"WRN quoting ERR", daemonLogLine("cluster", "WRN", "sentinel quotes [ERR]"), LogClusterWarning, "cluster", "WRN", false},
		{"cluster log INF quoting ERR", clusterLogLine(true, "cluster", "INF", "sentinel quotes [ERR]"), "", "cluster", "INF", false},
		{"cluster log WRN quoting SEC", clusterLogLine(false, "cluster", "WRN", "sentinel quotes [SEC]"), LogClusterWarning, "cluster", "WRN", false},
		// Other channels keep their own envelope.
		{"audit INF quoting WRN", daemonLogLine("audit", "INF", `from='client.1' cmd=[{"prefix": "config set", "value": "[WRN]"}]`), "", "audit", "INF", false},
		{"audit cluster log INF quoting ERR", clusterLogLine(true, "audit", "INF", "cmd=[{\"prefix\": \"x\"}] [ERR]"), "", "audit", "INF", false},
		{"audit WRN", clusterLogLine(false, "audit", "WRN", "denied"), LogClusterWarning, "audit", "WRN", false},
		{"cephadm channel ERR", daemonLogLine("cephadm", "ERR", "sentinel"), LogClusterError, "cephadm", "ERR", false},
		// Assertion and fatal signatures keep their own, whole line, path.
		{"bare assertion", "/build/src/osd/OSD.cc: 1234: FAILED ceph_assert(r == 0)", LogAssertion, "", "", false},
		{"bare abort", "ceph_abort_msg(\"x\")", LogAssertion, "", "", false},
		{"bare signal", "*** Caught signal (Segmentation fault) **", LogFatalSignal, "", "", false},
		{"INF quoting an assertion", daemonLogLine("cluster", "INF", "report: FAILED ceph_assert(x)"), LogAssertion, "cluster", "INF", false},
		{"WRN quoting a signal", daemonLogLine("cluster", "WRN", "report: Caught signal"), LogFatalSignal, "cluster", "WRN", false},
		{"audit quoting an assertion", clusterLogLine(true, "audit", "INF", "FAILED assert(y)"), LogAssertion, "audit", "INF", false},
		// Unknown or ambiguous forms are never guessed from a marker.
		{"bare marker", "[ERR] sentinel", "", "", "", true},
		{"marker after a prefix", "bootstrap log_channel(cluster) log [WRN] : sentinel", "", "", "", true},
		{"missing stamp and thread", "0 log_channel(cluster) log [WRN] : sentinel", "", "", "", true},
		{"daemon envelope not at the start", "x " + daemonLogLine("cluster", "WRN", "sentinel"), "", "", "", true},
		{"unknown priority", daemonLogLine("cluster", "FOO", "sentinel [WRN]"), "", "", "", true},
		{"lowercase priority", daemonLogLine("cluster", "wrn", "sentinel [SEC]"), "", "", "", true},
		{"channel prefix disagrees", "audit " + clusterLogLine(false, "cluster", "INF", "sentinel [ERR]"), "", "", "", true},
		{"missing channel", "2026-10-24T01:02:03.123456+0000 mon.a (mon.0) 17 : [WRN] sentinel", "", "", "", true},
		{"missing priority", "2026-10-24T01:02:03.123456+0000 mon.a (mon.0) 17 : cluster sentinel [WRN]", "", "", "", true},
		// A malformed producer stamp is no envelope.
		{"daemon lone digit stamp", "1 deadbeef 0 log_channel(cluster) log [INF] : quotes [ERR] sentinel", "", "", "", true},
		{"daemon impossible stamp", "2026-99-99T99:99:99.123+0000 7f3a  0 log_channel(cluster) log [WRN] : sentinel", "", "", "", true},
		{"cluster impossible stamp", "2026-02-30T01:02:03.123456+0000 mon.a (mon.0) 17 : cluster [WRN] sentinel", "", "", "", true},
		{"stamp without fraction", "2026-10-24T01:02:03+0000 7f3a  0 log_channel(cluster) log [WRN] : sentinel", "", "", "", true},
		{"stamp without timezone", "2026-10-24T01:02:03.123 7f3a  0 log_channel(cluster) log [WRN] : sentinel", "", "", "", true},
		{"stamp with two fractional digits", "2026-10-24T01:02:03.12+0000 7f3a  0 log_channel(cluster) log [WRN] : sentinel", "", "", "", true},
		{"offset minutes 60", "2026-10-24T01:02:03.123+1260 7f3a  0 log_channel(cluster) log [WRN] : sentinel", "", "", "", true},
		{"offset minutes 60 from zero hours", "2026-10-24T01:02:03.123456+0060 mon.a (mon.0) 17 : cluster [WRN] sentinel", "", "", "", true},
		{"offset hours beyond +14", "2026-10-24T01:02:03.123+1500 7f3a  0 log_channel(cluster) log [WRN] : sentinel", "", "", "", true},
		{"offset hours beyond -12", "2026-10-24T01:02:03.123-1300 7f3a  0 log_channel(cluster) log [WRN] : sentinel", "", "", "", true},
		{"offset minutes 99", "2026-10-24T01:02:03.123+0099 7f3a  0 log_channel(cluster) log [WRN] : sentinel", "", "", "", true},
		{"offset with a colon", "2026-10-24T01:02:03.123+05:30 7f3a  0 log_channel(cluster) log [WRN] : sentinel", "", "", "", true},
		{"offset Z", "2026-10-24T01:02:03.123Z 7f3a  0 log_channel(cluster) log [WRN] : sentinel", "", "", "", true},
		{"leap day", "2024-02-29T01:02:03.123+1400 7f3a  0 log_channel(cluster) log [WRN] : sentinel", LogClusterWarning, "cluster", "WRN", false},
		{"offset -1200", "2026-10-24T01:02:03.123456-1200 mon.a (mon.0) 17 : cluster [WRN] sentinel", LogClusterWarning, "cluster", "WRN", false},
		{"non-leap Feb 29", "2026-02-29T01:02:03.123+0000 7f3a  0 log_channel(cluster) log [WRN] : sentinel", "", "", "", true},
		{"coarse and fine stamps", "2026-10-24T01:02:03.123456-0530 7f3a  0 log_channel(cluster) log [WRN] : sentinel", LogClusterWarning, "cluster", "WRN", false},
		{"no marker", "plain daemon output", "", "", "", false},
		{"DBG marker only", "[DBG] bare", "", "", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			kind, envelope, _, unclassified := classifyVerificationLine(tc.line)
			if kind != tc.kind || envelope.channel != tc.channel || envelope.priority != tc.priority || unclassified != tc.unclassified {
				t.Fatalf("classified %q as kind %q channel %q priority %q unclassified %v", tc.line, kind, envelope.channel, envelope.priority, unclassified)
			}
		})
	}
}

// The four controls of the issue through the whole verification: a required
// warning is satisfied by a warning entry only.
func TestVerificationRequiredWarningUsesTheStructuralSeverity(t *testing.T) {
	required := []LogExpectation{{Name: "sentinel-warning", Required: true, Event: LogClusterWarning, Pattern: `sentinel`}}
	tests := []struct {
		name     string
		line     string
		outcome  VerificationOutcome
		matched  int
		findings int
		event    string
	}{
		{"WRN", daemonLogLine("cluster", "WRN", "sentinel"), VerificationClean, 1, 1, string(LogClusterWarning)},
		{"WRN quoting ERR", daemonLogLine("cluster", "WRN", "sentinel quotes [ERR]"), VerificationClean, 1, 1, string(LogClusterWarning)},
		{"INF quoting WRN", daemonLogLine("cluster", "INF", "sentinel quotes [WRN]"), VerificationUnexpected, 0, 0, ""},
		{"INF", daemonLogLine("cluster", "INF", "sentinel"), VerificationUnexpected, 0, 0, ""},
		{"cluster log INF quoting WRN", clusterLogLine(true, "cluster", "INF", "sentinel quotes [WRN]"), VerificationUnexpected, 0, 0, ""},
		{"cluster log WRN", clusterLogLine(true, "cluster", "WRN", "sentinel"), VerificationClean, 1, 1, string(LogClusterWarning)},
		{"audit INF quoting WRN", clusterLogLine(true, "audit", "INF", "sentinel quotes [WRN]"), VerificationUnexpected, 0, 0, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fx := newVerifyFixture()
			fx.config.Logs = required
			v := fx.activate(t)
			fx.line("mon", 2*time.Minute, tc.line)
			report := fx.evaluate(t, v)
			if report.Outcome != tc.outcome || report.Logs.Expectations[0].Matched != tc.matched || len(report.Logs.Findings) != tc.findings || len(report.Logs.Gaps) != 0 {
				t.Fatalf("outcome %s logs %+v", report.Outcome, report.Logs)
			}
			if tc.findings == 1 && (report.Logs.Findings[0].Event != tc.event || report.Logs.Findings[0].Expectation != "sentinel-warning") {
				t.Fatalf("finding %+v", report.Logs.Findings[0])
			}
		})
	}
}

// A marker quoted by a lower level entry makes no event, and a quoted lower
// marker does not turn a higher entry into a lower one.
func TestVerificationQuotedMarkersMakeNoEvent(t *testing.T) {
	fx := newVerifyFixture()
	v := fx.activate(t)
	fx.line("mon", 2*time.Minute, daemonLogLine("cluster", "INF", "sentinel quotes [WRN] [ERR] [SEC]"))
	fx.line("mon", 3*time.Minute, clusterLogLine(true, "audit", "INF", `cmd=[{"prefix": "osd pool create"}] [ERR]`))
	fx.line("osd1", 4*time.Minute, daemonLogLine("cluster", "DBG", "pgmap [WRN]"))
	report := fx.evaluate(t, v)
	if report.Outcome != VerificationClean || len(report.Logs.Findings) != 0 || len(report.Logs.Gaps) != 0 {
		t.Fatalf("outcome %s logs %+v", report.Outcome, report.Logs)
	}
}

// The reported kind of an entry is its structural one, whatever channel it
// is on; a quoted marker is retained in the message only.
func TestVerificationFindingKindIsStructural(t *testing.T) {
	fx := newVerifyFixture()
	v := fx.activate(t)
	fx.line("mon", 2*time.Minute, daemonLogLine("cluster", "WRN", "sentinel quotes [ERR]"))
	fx.line("mon", 3*time.Minute, clusterLogLine(true, "audit", "ERR", "denied"))
	report := fx.evaluate(t, v)
	if report.Outcome != VerificationUnexpected || len(report.Logs.Findings) != 2 {
		t.Fatalf("outcome %s logs %+v", report.Outcome, report.Logs)
	}
	first, second := report.Logs.Findings[0], report.Logs.Findings[1]
	if first.Event != string(LogClusterWarning) || !strings.Contains(first.Text, "sentinel quotes [ERR]") || second.Event != string(LogClusterError) {
		t.Fatalf("findings %+v", report.Logs.Findings)
	}
}

// Quoted assertion and fatal signatures stay on their own path: they are
// reported as such whatever the envelope says, and they are not mistaken for
// a warning that satisfies a required warning.
func TestVerificationQuotedAssertionsStayAssertions(t *testing.T) {
	fx := newVerifyFixture()
	fx.config.Logs = []LogExpectation{{Name: "warning", Required: true, Event: LogClusterWarning, Pattern: `.`}}
	v := fx.activate(t)
	fx.line("mon", 2*time.Minute, daemonLogLine("cluster", "INF", "report FAILED ceph_assert(x) [WRN]"))
	fx.line("osd1", 3*time.Minute, clusterLogLine(true, "audit", "INF", "Caught signal [WRN]"))
	report := fx.evaluate(t, v)
	if report.Outcome != VerificationUnexpected || report.Logs.Unexpected != 2 || report.Logs.Expectations[0].Matched != 0 {
		t.Fatalf("outcome %s logs %+v", report.Outcome, report.Logs)
	}
	if report.Logs.Findings[0].Event != string(LogAssertion) || report.Logs.Findings[1].Event != string(LogFatalSignal) {
		t.Fatalf("findings %+v", report.Logs.Findings)
	}
}

// A line with a marker but without a supported envelope is not classified
// and is not clean: it is a gap that names the count, without a guess.
func TestVerificationUnsupportedEnvelopeIsAGap(t *testing.T) {
	for _, line := range []string{
		"[ERR] sentinel",
		"sentinel quotes [WRN]",
		"0 log_channel(cluster) log [WRN] : no stamp",
		"audit " + clusterLogLine(false, "cluster", "INF", "channel prefix disagrees [ERR]"),
	} {
		t.Run(line, func(t *testing.T) {
			fx := newVerifyFixture()
			fx.config.Logs = []LogExpectation{{Name: "sentinel-warning", Required: true, Event: LogClusterWarning, Pattern: `sentinel`}}
			v := fx.activate(t)
			fx.line("mon", 2*time.Minute, line)
			fx.line("mon", 3*time.Minute, "[SEC] sentinel")
			report := fx.evaluate(t, v)
			if report.Logs.Expectations[0].Matched != 0 || len(report.Logs.Findings) != 0 || report.Logs.Outcome != VerificationUnexpected ||
				!strings.Contains(gapReasons(report.Logs), "2 log lines carry an [ERR], [SEC] or [WRN] marker without a supported log envelope") {
				t.Fatalf("outcome %s logs %+v", report.Logs.Outcome, report.Logs)
			}
		})
	}
	t.Run("alone it is incomplete", func(t *testing.T) {
		fx := newVerifyFixture()
		v := fx.activate(t)
		fx.line("mon", 2*time.Minute, "[ERR] sentinel")
		report := fx.evaluate(t, v)
		if report.Outcome != VerificationIncomplete || len(report.Logs.Findings) != 0 || len(report.Logs.Gaps) != 1 || report.Logs.Gaps[0].Source != "mon.a" {
			t.Fatalf("outcome %s logs %+v", report.Outcome, report.Logs)
		}
	})
}

// A malformed producer stamp makes the line an unclassified marker line at
// the collector: an INF line is not silently clean, and a WRN line does not
// satisfy a required warning.
func TestVerificationMalformedStampIsAGap(t *testing.T) {
	forms := map[string]func(priority string) string{
		"daemon offset 1260": func(p string) string {
			return "2026-10-24T01:02:03.123+1260 7f3a  0 log_channel(cluster) log [" + p + "] : quotes [ERR] sentinel"
		},
		"daemon offset 0060": func(p string) string {
			return "2026-10-24T01:02:03.123456+0060 7f3a  0 log_channel(cluster) log [" + p + "] : quotes [ERR] sentinel"
		},
		"cluster offset 1260": func(p string) string {
			return "2026-10-24T01:02:03.123456+1260 mon.a (mon.0) 17 : cluster [" + p + "] quotes [ERR] sentinel"
		},
		"cluster offset 0060": func(p string) string {
			return "cluster 2026-10-24T01:02:03.123456+0060 mon.a (mon.0) 17 : cluster [" + p + "] quotes [ERR] sentinel"
		},
		"cluster offset colon": func(p string) string {
			return "2026-10-24T01:02:03.123456+05:30 mon.a (mon.0) 17 : cluster [" + p + "] quotes [ERR] sentinel"
		},
		"daemon lone digit": func(p string) string {
			return "1 deadbeef 0 log_channel(cluster) log [" + p + "] : quotes [ERR] sentinel"
		},
		"daemon impossible": func(p string) string {
			return "2026-99-99T99:99:99.123+0000 7f3a  0 log_channel(cluster) log [" + p + "] : quotes [ERR] sentinel"
		},
		"cluster lone digit": func(p string) string { return "1 mon.a (mon.0) 17 : cluster [" + p + "] quotes [ERR] sentinel" },
		"cluster impossible": func(p string) string {
			return "cluster 2026-13-01T25:61:61.123456+0000 mon.a (mon.0) 17 : cluster [" + p + "] quotes [ERR] sentinel"
		},
	}
	for name, form := range forms {
		t.Run(name+" INF", func(t *testing.T) {
			fx := newVerifyFixture()
			v := fx.activate(t)
			fx.line("mon", 2*time.Minute, form("INF"))
			report := fx.evaluate(t, v)
			if report.Outcome != VerificationIncomplete || len(report.Logs.Findings) != 0 || len(report.Logs.Gaps) != 1 {
				t.Fatalf("outcome %s logs %+v", report.Outcome, report.Logs)
			}
		})
		t.Run(name+" required WRN", func(t *testing.T) {
			fx := newVerifyFixture()
			fx.config.Logs = []LogExpectation{{Name: "sentinel-warning", Required: true, Event: LogClusterWarning, Pattern: `sentinel`}}
			v := fx.activate(t)
			fx.line("mon", 2*time.Minute, form("WRN"))
			report := fx.evaluate(t, v)
			if report.Outcome != VerificationUnexpected || report.Logs.Expectations[0].Matched != 0 || len(report.Logs.Findings) != 0 || len(report.Logs.Gaps) != 1 {
				t.Fatalf("outcome %s logs %+v", report.Outcome, report.Logs)
			}
		})
	}
}
