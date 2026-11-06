package finalize

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// With ArchiveLogs, an evaluation keeps every line of the window, events or
// not, from both streams in the order received, with its source and
// Docker's timestamp and without the cluster's secrets. Lines outside the
// window and lines without a timestamp are not kept. Without ArchiveLogs,
// nothing is kept, but the lines are still counted.
func TestVerificationArchiveLogsKeepsTheWindow(t *testing.T) {
	stamp := func(after time.Duration) string { return verifyStart.Add(after).Format(time.RFC3339Nano) }
	osd0 := slices.Concat(
		diagnosticsFrame(2, stamp(-time.Minute)+" before the window\n"),
		diagnosticsFrame(2, stamp(time.Minute)+" "+daemonLogLine("cluster", "WRN", "Monitor daemon marked osd.0 down, but it is still running")+"\n"),
		diagnosticsFrame(1, stamp(2*time.Minute)+" ordinary stdout line key AQBverifysecret==\n"),
		diagnosticsFrame(2, stamp(3*time.Minute)+" map e13 wrongly marked me down at e12\n"),
		diagnosticsFrame(2, "no timestamp\n"),
	)
	for _, archive := range []bool{true, false} {
		fx := newVerifyFixture()
		fx.config.ArchiveLogs = archive
		fx.config.Logs = []LogExpectation{{Name: "down", Daemon: "osd.0", Event: LogClusterWarning, Pattern: "marked osd.0 down"}}
		fx.docker.containers["osd0"].rawLogs = osd0
		report := fx.evaluate(t, fx.activate(t))
		if report.Logs.Permitted != 1 || report.Logs.Unexpected != 0 || !strings.Contains(gapReasons(report.Logs), "osd.0: 1 log lines have no valid timestamp") {
			t.Fatalf("archive %v: log check %+v", archive, report.Logs)
		}
		// mon.a and osd.1 each have one line in the window, osd.0 three.
		lines := map[string]int{}
		for _, source := range report.Sources {
			lines[source.Source] = source.LogLines
		}
		if lines["mon.a"] != 1 || lines["osd.0"] != 3 || lines["osd.1"] != 1 {
			t.Fatalf("archive %v: lines per source %v", archive, lines)
		}
		hasNote := slices.Contains(report.Notes, archivedLogsNote)
		if !archive {
			if report.logLines != nil || report.archiveLogs || hasNote {
				t.Fatalf("lines kept without ArchiveLogs: %+v", report.logLines)
			}
			continue
		}
		if !report.archiveLogs || !hasNote || len(report.logLines) != 5 {
			t.Fatalf("kept lines %+v", report.logLines)
		}
		var got []string
		for _, line := range report.logLines {
			if line.Source == "osd.0" {
				if line.ContainerID != "osd0" {
					t.Fatalf("line %+v", line)
				}
				got = append(got, line.Stream+" "+line.Time+" "+line.Text)
			}
		}
		want := []string{
			"stderr " + stamp(time.Minute) + " " + daemonLogLine("cluster", "WRN", "Monitor daemon marked osd.0 down, but it is still running"),
			"stdout " + stamp(2*time.Minute) + " ordinary stdout line key [REDACTED]",
			"stderr " + stamp(3*time.Minute) + " map e13 wrongly marked me down at e12",
		}
		if !slices.Equal(got, want) {
			t.Fatalf("osd.0 lines\n%q\nwant\n%q", got, want)
		}
	}
}

// The kept lines have the log check's coverage: a source over MaxLogBytes
// keeps the prefix that was read, with the limit's gap, a cut line keeps its
// read part marked truncated, and a restarted or rotating source keeps
// nothing, with its gap.
func TestVerificationArchiveLogsFollowTheLogCoverage(t *testing.T) {
	fx := newVerifyFixture()
	fx.config.ArchiveLogs, fx.config.MaxLogBytes = true, 1024
	v := fx.activate(t)
	fx.line("osd0", 2*time.Minute, "first line in the window")
	fx.line("osd0", 3*time.Minute, strings.Repeat("y", 2048))
	fx.docker.containers["osd1"].restarts = 1
	fx.docker.containers["mon"].maxSize = "10m"
	report := fx.evaluate(t, v)
	reasons := gapReasons(report.Logs)
	for _, want := range []string{"osd.0: the log stream of the window exceeds 1024 bytes", "osd.1: restarted since the activation", "mon.a: the json-file driver rotates"} {
		if !strings.Contains(reasons, want) {
			t.Fatalf("gaps %s, want %q", reasons, want)
		}
	}
	var sources []string
	for _, line := range report.logLines {
		sources = append(sources, line.Source)
	}
	if !slices.Equal(sources, []string{"osd.0", "osd.0", "osd.0"}) {
		t.Fatalf("kept lines %+v", report.logLines)
	}
	last := report.logLines[2]
	if report.logLines[1].Text != "first line in the window" || !strings.HasPrefix(last.Text, "yyy") || len(last.Text) >= 2048 || !last.Truncated {
		t.Fatalf("kept prefix %+v", report.logLines)
	}
	if report.Sources[1].LogBytes != 1024 || report.Sources[1].LogLines != 3 {
		t.Fatalf("osd.0 source %+v", report.Sources[1])
	}
}

// The failure archive writes the kept lines of a verification with
// ArchiveLogs next to its report and lists the file in the manifest; one
// without ArchiveLogs gets no such file.
func TestFinalizationArchivesVerificationLogs(t *testing.T) {
	fx, vfx := verifyFinalization(t, Config{Archive: true})
	if _, err := fx.Verify(t.Context(), VerificationConfig{Name: "with logs", ArchiveLogs: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := fx.Verify(t.Context(), VerificationConfig{Name: "without"}); err != nil {
		t.Fatal(err)
	}
	vfx.line("osd1", 2*time.Minute, daemonLogLine("cluster", "ERR", "osd.1 failed"))
	fx.run()
	archives := fx.archives(t)
	if len(archives) != 1 {
		t.Fatalf("archives %v, errors %v", archives, fx.tb.errors)
	}
	manifest := readManifest(t, archives[0])
	var files []string
	for _, file := range manifest.Files {
		if file.Error != "" {
			t.Fatalf("file %+v", file)
		}
		files = append(files, file.Name)
	}
	if !slices.Contains(files, "verification-1-with_logs-logs.jsonl") || slices.Contains(files, "verification-2-without-logs.jsonl") {
		t.Fatalf("files %v", files)
	}
	data, err := os.ReadFile(filepath.Join(archives[0], "verification-1-with_logs-logs.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if size := manifestFileBytes(manifest, "verification-1-with_logs-logs.jsonl"); size != len(data) {
		t.Fatalf("manifest bytes %d, file %d", size, len(data))
	}
	var got []string
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		var line verificationLogLine
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			t.Fatalf("%v: %s", err, scanner.Bytes())
		}
		got = append(got, line.Source+" "+line.Stream+" "+line.Text)
	}
	want := []string{
		"mon.a stderr " + daemonLogLine("cluster", "INF", "osd.0 boot"),
		"osd.0 stderr " + daemonLogLine("cluster", "INF", "osd.0 boot"),
		"osd.1 stderr " + daemonLogLine("cluster", "INF", "osd.0 boot"),
		"osd.1 stderr " + daemonLogLine("cluster", "ERR", "osd.1 failed"),
	}
	if !slices.Equal(got, want) {
		t.Fatalf("archived lines\n%q\nwant\n%q", got, want)
	}
	if _, err := os.Stat(filepath.Join(archives[0], "verification-2-without-logs.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("lines archived without ArchiveLogs: %v", err)
	}
}

func manifestFileBytes(manifest finalizationManifest, name string) int {
	for _, file := range manifest.Files {
		if file.Name == name {
			return file.Bytes
		}
	}
	return -1
}
