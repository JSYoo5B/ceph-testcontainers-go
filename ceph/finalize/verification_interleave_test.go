package finalize

import (
	"slices"
	"strings"
	"testing"
	"time"
)

// The splits below are as a Tentacle 20.2.4 MON wrote them under a burst of
// `log` commands: its daemon lines landed between the pieces of the header
// of a cluster log entry written with `cerr << channel << " " << entry`.
const (
	splitDaemonAudit = "2026-11-10T00:33:14.339+0000 7d9025c0d640  0 log_channel(audit) log [INF] : from='client.? 172.18.0.2:0/1025132452' entity='client.admin' cmd='[{\"prefix\": \"log\", \"logtext\": [\"sentinel quotes [WRN]\"]}]': finished"
	splitMessage     = "from='client.? 172.18.0.2:0/1025132452' entity='client.admin' cmd='[{\"prefix\": \"log\", \"logtext\": [\"sentinel quotes [WRN]\"]}]': finished"
)

// splitEntries are entries split at every observed kind of point: after the
// priority, inside the stamp, and inside the name and rank.
var splitEntries = map[string][]string{
	"after the priority": {
		"audit 2026-11-10T00:33:13.319590+0000 mon.a (mon.0) 1026 : audit [INF] " + splitDaemonAudit,
		splitMessage,
	},
	"inside the stamp, name and rank": {
		"audit 2026-11" + splitDaemonAudit,
		"-10T00:" + splitDaemonAudit,
		"33:13." + splitDaemonAudit,
		"319605+0000 mon.a (" + splitDaemonAudit,
		"mon.0) 1027 : audit [INF] " + splitMessage,
	},
}

// A split cluster log entry is restored: its daemon lines and the entry are
// classified on their own, and the marker quoted in the messages is no gap.
func TestVerificationRestoresEntriesSplitByDaemonLines(t *testing.T) {
	for name, lines := range splitEntries {
		t.Run(name, func(t *testing.T) {
			fx := newVerifyFixture()
			fx.config.Logs = []LogExpectation{{Name: "quoted", Required: true, Event: LogClusterWarning, Pattern: `sentinel`}}
			fx.config.ArchiveLogs = true
			v := fx.activate(t)
			for i, line := range lines {
				fx.line("mon", 2*time.Minute+time.Duration(i)*time.Millisecond, line)
			}
			report := fx.evaluate(t, v)
			if len(report.Logs.Gaps) != 0 || len(report.Logs.Findings) != 0 || report.Logs.Expectations[0].Matched != 0 {
				t.Fatalf("outcome %s logs %+v", report.Outcome, report.Logs)
			}
			// The window's first line, then one line per daemon line and the
			// restored entry, all with the first fragment's position.
			var got []string
			for _, line := range report.logLines {
				if line.Source == "mon.a" {
					got = append(got, line.Text)
				}
			}
			want := []string{daemonLogLine("cluster", "INF", "osd.0 boot")}
			entry := ""
			for i, line := range lines {
				if i < len(lines)-1 {
					at := strings.Index(line, splitDaemonAudit)
					want, entry = append(want, splitDaemonAudit), entry+line[:at]
					continue
				}
				entry += line
			}
			want = append(want, entry)
			if !slices.Equal(got, want) {
				t.Fatalf("mon.a lines\n%q\nwant\n%q", got, want)
			}
			if _, ok := parseVerificationEnvelope(entry); !ok || !strings.HasSuffix(entry, "audit [INF] "+splitMessage) {
				t.Fatalf("restored entry %q", entry)
			}
		})
	}
}

// A split entry's own priority and a daemon line's own priority both count:
// a WRN entry split by an INF daemon line is a warning, and a WRN daemon line
// inside an INF entry's header is one too.
func TestVerificationSplitEntriesKeepTheirPriorities(t *testing.T) {
	fx := newVerifyFixture()
	v := fx.activate(t)
	fx.line("mon", 2*time.Minute, "cluster 2026-11-10T00:33:13.319590+0000 mon.a"+daemonLogLine("cluster", "INF", "sentinel quotes [ERR]"))
	fx.line("mon", 2*time.Minute+time.Millisecond, " (mon.0) 1026 : cluster [WRN] Health check failed: noout flag(s) set (OSDMAP_FLAGS)")
	fx.line("mon", 3*time.Minute, "cluster 2026-11-10T00:33:14.319590+0000 mon.a (mon.0) 1027 : cluster [INF] "+daemonLogLine("cluster", "WRN", "slow ops"))
	fx.line("mon", 3*time.Minute+time.Millisecond, "sentinel quotes [WRN]")
	report := fx.evaluate(t, v)
	if len(report.Logs.Gaps) != 0 || len(report.Logs.Findings) != 2 {
		t.Fatalf("outcome %s logs %+v", report.Outcome, report.Logs)
	}
	first, second := report.Logs.Findings[0], report.Logs.Findings[1]
	if first.Event != string(LogClusterWarning) || !strings.Contains(first.Text, "(mon.0) 1026 : cluster [WRN] Health check failed") ||
		second.Event != string(LogClusterWarning) || !strings.Contains(second.Text, "log [WRN] : slow ops") {
		t.Fatalf("findings %+v", report.Logs.Findings)
	}
}

// A line is restored only as a split header: a message that quotes a daemon
// line is not split, so the quoted line makes no event, and a head that no
// line completes stays as read.
func TestVerificationDoesNotSplitMessages(t *testing.T) {
	t.Run("a message quoting a daemon line", func(t *testing.T) {
		fx := newVerifyFixture()
		v := fx.activate(t)
		fx.line("mon", 2*time.Minute, clusterLogLine(true, "cluster", "INF", "quoted "+daemonLogLine("cluster", "WRN", "sentinel")))
		fx.line("mon", 2*time.Minute+time.Millisecond, "(mon.0) 18 : cluster [INF] not the rest of that entry")
		report := fx.evaluate(t, v)
		if report.Outcome != VerificationClean || len(report.Logs.Findings) != 0 || len(report.Logs.Gaps) != 0 {
			t.Fatalf("outcome %s logs %+v", report.Outcome, report.Logs)
		}
	})
	// A record whose whole message is a quoted daemon line ends its header
	// where the daemon line starts, like a fragment. The record after it is
	// a record of its own and keeps its event.
	for name, next := range map[string]string{
		"a cluster record after a record quoting a daemon line": clusterLogLine(true, "cluster", "WRN", "Health check failed: noout flag(s) set (OSDMAP_FLAGS)"),
		"a daemon record after a record quoting a daemon line":  daemonLogLine("cluster", "WRN", "Health check failed: noout flag(s) set (OSDMAP_FLAGS)"),
	} {
		t.Run(name, func(t *testing.T) {
			fx := newVerifyFixture()
			fx.config.ArchiveLogs = true
			v := fx.activate(t)
			quoting := clusterLogLine(true, "cluster", "INF", daemonLogLine("cluster", "INF", "sentinel"))
			fx.line("mon", 2*time.Minute, quoting)
			fx.line("mon", 2*time.Minute+time.Millisecond, next)
			report := fx.evaluate(t, v)
			if len(report.Logs.Gaps) != 0 || len(report.Logs.Findings) != 1 || report.Logs.Findings[0].Event != string(LogClusterWarning) ||
				!strings.Contains(report.Logs.Findings[0].Text, "Health check failed") {
				t.Fatalf("outcome %s logs %+v", report.Outcome, report.Logs)
			}
			var got []string
			for _, line := range report.logLines {
				if line.Source == "mon.a" {
					got = append(got, line.Text)
				}
			}
			if want := []string{daemonLogLine("cluster", "INF", "osd.0 boot"), quoting, next}; !slices.Equal(got, want) {
				t.Fatalf("mon.a lines\n%q\nwant\n%q", got, want)
			}
		})
	}
	t.Run("a head without its rest", func(t *testing.T) {
		fx := newVerifyFixture()
		v := fx.activate(t)
		fx.line("mon", 2*time.Minute, "audit 2026-11"+daemonLogLine("cluster", "WRN", "sentinel"))
		fx.line("mon", 2*time.Minute+time.Millisecond, "sentinel quotes [WRN]")
		report := fx.evaluate(t, v)
		if len(report.Logs.Findings) != 0 || !strings.Contains(gapReasons(report.Logs), "2 log lines carry an [ERR], [SEC] or [WRN] marker without a supported log envelope") {
			t.Fatalf("outcome %s logs %+v", report.Outcome, report.Logs)
		}
	})
	t.Run("a head at the end of the read", func(t *testing.T) {
		fx := newVerifyFixture()
		v := fx.activate(t)
		fx.line("mon", 2*time.Minute, "audit 2026-11"+daemonLogLine("cluster", "INF", "sentinel quotes [WRN]"))
		report := fx.evaluate(t, v)
		if len(report.Logs.Findings) != 0 || !strings.Contains(gapReasons(report.Logs), "1 log lines carry") {
			t.Fatalf("outcome %s logs %+v", report.Outcome, report.Logs)
		}
	})
}
