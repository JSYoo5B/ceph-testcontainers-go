package finalize

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
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

// archivedVerificationLines runs a failing finalization with one verification
// that archives its lines and returns them, with the whole text of every
// file of the archive.
func archivedVerificationLines(t *testing.T, config VerificationConfig, mutate func(*verifyFixture)) ([]verificationLogLine, *VerificationReport, string) {
	t.Helper()
	fx, vfx := verifyFinalization(t, Config{Archive: true})
	config.Name, config.ArchiveLogs = "archived", true
	if _, err := fx.Verify(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	mutate(vfx)
	fx.run()
	archives := fx.archives(t)
	if len(archives) != 1 {
		t.Fatalf("archives %v, errors %v", archives, fx.tb.errors)
	}
	entries, err := os.ReadDir(archives[0])
	if err != nil {
		t.Fatal(err)
	}
	var all strings.Builder
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(archives[0], entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		all.Write(data)
	}
	data, err := os.ReadFile(filepath.Join(archives[0], "verification-1-archived-logs.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var lines []verificationLogLine
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(nil, 1<<20)
	for scanner.Scan() {
		var line verificationLogLine
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			t.Fatalf("%v: %s", err, scanner.Bytes())
		}
		lines = append(lines, line)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	reportData, err := os.ReadFile(filepath.Join(archives[0], "verification-1-archived.json"))
	if err != nil {
		t.Fatal(err)
	}
	var report VerificationReport
	if err := json.Unmarshal(reportData, &report); err != nil {
		t.Fatal(err)
	}
	return lines, &report, all.String()
}

func linesOf(lines []verificationLogLine, source string) []string {
	var got []string
	for _, line := range lines {
		if line.Source == source {
			got = append(got, line.Stream+" "+line.Text)
		}
	}
	return got
}

// A private key block whose delimiters are on different lines is withheld
// from the archived lines and the findings, per source and stream: a
// complete block, one that begins on an event line, one that the read ends
// inside, and one whose beginning the read did not see (cut at 64 KiB),
// whose end withholds the stream's lines back to the read's start. Lines of
// the other stream between them stay.
func TestVerificationArchiveLogsWithholdPrivateKeyBlocks(t *testing.T) {
	stamp := func(after time.Duration) string { return verifyStart.Add(after).Format(time.RFC3339Nano) }
	payloads := []string{"MIIEvQIBADANBgkqhkiG9w0payloadOne", "payloadTwoOnTheErrLine", "payloadThreeInsideTheErrBlock", "payloadFourNeverClosed", "payloadFiveOnAnEventLine"}
	lines, report, archive := archivedVerificationLines(t, VerificationConfig{}, func(fx *verifyFixture) {
		fx.docker.containers["osd0"].rawLogs = slices.Concat(
			diagnosticsFrame(1, stamp(time.Minute)+" key -----BEGIN PRIVATE KEY-----\n"),
			diagnosticsFrame(2, stamp(time.Minute+time.Second)+" between the key lines\n"),
			diagnosticsFrame(1, stamp(time.Minute+2*time.Second)+" "+payloads[0]+"\n"),
			diagnosticsFrame(1, stamp(time.Minute+3*time.Second)+" -----END PRIVATE KEY----- after the key\n"),
			diagnosticsFrame(2, stamp(2*time.Minute)+" "+daemonLogLine("cluster", "ERR", "dump -----BEGIN RSA PRIVATE KEY-----"+payloads[1])+"\n"),
			diagnosticsFrame(2, stamp(2*time.Minute+time.Second)+" "+payloads[2]+"\n"),
			diagnosticsFrame(2, stamp(2*time.Minute+2*time.Second)+" -----END RSA PRIVATE KEY-----\n"),
		)
		fx.docker.containers["osd1"].rawLogs = slices.Concat(
			diagnosticsFrame(1, stamp(time.Minute)+" -----BEGIN EC PRIVATE KEY-----\n"),
			diagnosticsFrame(2, stamp(time.Minute)+" stderr of osd.1\n"),
			diagnosticsFrame(1, stamp(time.Minute+time.Second)+" "+payloads[3]+"\n"),
		)
		fx.docker.containers["mon"].rawLogs = slices.Concat(
			diagnosticsFrame(1, stamp(time.Minute)+" "+strings.Repeat("x", maxVerificationLine)+"-----BEGIN PRIVATE KEY-----\n"),
			diagnosticsFrame(1, stamp(time.Minute+time.Second)+" "+daemonLogLine("cluster", "ERR", payloads[4])+"\n"),
			diagnosticsFrame(2, stamp(time.Minute+time.Second)+" stderr of mon.a\n"),
			diagnosticsFrame(1, stamp(time.Minute+2*time.Second)+" -----END PRIVATE KEY-----\n"),
			diagnosticsFrame(1, stamp(time.Minute+3*time.Second)+" after the orphan end\n"),
		)
	})
	for _, payload := range payloads {
		if strings.Contains(archive, payload) {
			t.Errorf("the archive holds %q", payload)
		}
	}
	const key = verificationKeyMarker
	for source, want := range map[string][]string{
		"osd.0": {
			"stdout key " + key, "stderr between the key lines", "stdout " + key, "stdout " + key + " after the key",
			"stderr " + daemonLogLine("cluster", "ERR", "dump "+key), "stderr " + key, "stderr " + key,
		},
		"osd.1": {"stdout " + key, "stderr stderr of osd.1", "stdout " + key},
		"mon.a": {"stdout " + key, "stdout " + key, "stderr stderr of mon.a", "stdout " + key, "stdout after the orphan end"},
	} {
		if got := linesOf(lines, source); !slices.Equal(got, want) {
			t.Errorf("%s lines\n%q\nwant\n%q", source, got, want)
		}
	}
	var findings []string
	for _, finding := range report.Logs.Findings {
		findings = append(findings, finding.Source+" "+finding.Text)
	}
	if want := []string{"mon.a " + key, "osd.0 " + daemonLogLine("cluster", "ERR", "dump "+key)}; !slices.Equal(findings, want) {
		t.Errorf("findings\n%q\nwant\n%q", findings, want)
	}
}

// A truncated line whose tail is the start of a configured secret keeps its
// safe prefix, marked truncated, and withholds that tail, in the archived
// lines and the findings: the last line of a cut stream and a line cut at 64
// KiB. A whole secret on an ordinary line is redacted as before.
func TestVerificationArchiveLogsWithholdACutSecret(t *testing.T) {
	stamp := func(after time.Duration) string { return verifyStart.Add(after).Format(time.RFC3339Nano) }
	const secret = "opaqueTokenValue123"
	partial := secret[:len(secret)-1]
	cutEvent := stamp(2*time.Minute) + " " + daemonLogLine("cluster", "ERR", "presented "+secret+" again")
	cutEvent = cutEvent[:strings.Index(cutEvent, partial)+len(partial)]
	long := stamp(time.Minute) + " long "
	long += strings.Repeat("y", maxVerificationLine-len(long)-len(partial)) + secret + " rest\n"
	lines, report, archive := archivedVerificationLines(t, VerificationConfig{RedactValues: []string{secret}}, func(fx *verifyFixture) {
		frame := diagnosticsFrame(2, cutEvent+" again\n")
		fx.docker.containers["osd0"].rawLogs = slices.Concat(
			diagnosticsFrame(2, stamp(time.Minute)+" whole "+secret+" line\n"),
			// The frame is cut right after the partial secret.
			frame[:8+len(cutEvent)],
		)
		fx.docker.containers["osd1"].rawLogs = diagnosticsFrame(1, long)
	})
	if strings.Contains(archive, partial[:len(partial)-4]) {
		t.Errorf("the archive holds a part of the secret")
	}
	find := func(source, prefix string) verificationLogLine {
		for _, line := range lines {
			if line.Source == source && strings.HasPrefix(line.Text, prefix) {
				return line
			}
		}
		t.Fatalf("no %s line %q in %+v", source, prefix, lines)
		return verificationLogLine{}
	}
	whole, event, cut := find("osd.0", "whole"), find("osd.0", daemonLogLine("cluster", "ERR", "")), find("osd.1", "long")
	if whole.Text != "whole [REDACTED] line" || whole.Truncated {
		t.Errorf("whole line %+v", whole)
	}
	if event.Text != daemonLogLine("cluster", "ERR", "presented [REDACTED]") || !event.Truncated {
		t.Errorf("cut stream line %+v", event)
	}
	if !strings.HasSuffix(cut.Text, "y[REDACTED]") || !cut.Truncated {
		t.Errorf("long line ending %q, truncated %v", cut.Text[max(len(cut.Text)-20, 0):], cut.Truncated)
	}
	if len(report.Logs.Findings) != 1 || report.Logs.Findings[0].Text != event.Text || !report.Logs.Findings[0].Truncated {
		t.Errorf("findings %+v", report.Logs.Findings)
	}
}

// A stream cut right after a whole secret that starts with its own last
// character: that character is also a possible start of the secret, and
// withholding it alone would split the whole secret and keep the rest.
func TestVerificationArchiveLogsWithholdASecretEndingACutLine(t *testing.T) {
	stamp := verifyStart.Add(time.Minute).Format(time.RFC3339Nano)
	const secret = "opaque-token-o"
	cutEvent := stamp + " " + daemonLogLine("cluster", "ERR", "value "+secret)
	lines, report, archive := archivedVerificationLines(t, VerificationConfig{RedactValues: []string{secret}}, func(fx *verifyFixture) {
		frame := diagnosticsFrame(2, cutEvent+" more\n")
		fx.docker.containers["osd0"].rawLogs = frame[:8+len(cutEvent)]
	})
	if strings.Contains(archive, "opaque-token") {
		t.Errorf("the archive holds a part of the secret")
	}
	want := daemonLogLine("cluster", "ERR", "value [REDACTED]")
	var event *verificationLogLine
	for i := range lines {
		if lines[i].Source == "osd.0" {
			event = &lines[i]
		}
	}
	if event == nil || event.Text != want || !event.Truncated {
		t.Fatalf("cut line %+v, want %q truncated", event, want)
	}
	if len(report.Logs.Findings) != 1 || report.Logs.Findings[0].Text != want {
		t.Errorf("findings %+v", report.Logs.Findings)
	}
}

// A redaction value with a line break is refused before the verification
// is activated: log lines are masked one at a time, so its parts on
// consecutive lines would each stay readable in the archived lines. The
// failure archive then holds no lines of that verification and no part of
// the value.
func TestVerificationArchiveLogsRefuseAMultilineSecret(t *testing.T) {
	stamp := func(after time.Duration) string { return verifyStart.Add(after).Format(time.RFC3339Nano) }
	const first, second = "opaqueAlphaPart", "opaqueBetaPart"
	fx, vfx := verifyFinalization(t, Config{Archive: true})
	config := VerificationConfig{Name: "archived", ArchiveLogs: true, RedactValues: []string{first + "\n" + second}}
	_, err := fx.Verify(t.Context(), config)
	if !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "redaction value must be one line") {
		t.Errorf("Verify err = %v, want a refusal of the multiline value", err)
	}
	vfx.docker.containers["osd0"].rawLogs = slices.Concat(
		diagnosticsFrame(2, stamp(time.Minute)+" "+first+"\n"),
		diagnosticsFrame(2, stamp(2*time.Minute)+" "+second+"\n"),
	)
	fx.tb.Errorf("the test failed")
	fx.run()
	archives := fx.archives(t)
	if len(archives) != 1 {
		t.Fatalf("archives %v, errors %v", archives, fx.tb.errors)
	}
	entries, err := os.ReadDir(archives[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(archives[0], entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(entry.Name(), "verification-") {
			t.Errorf("the archive holds %s", entry.Name())
		}
		if strings.Contains(string(data), first) || strings.Contains(string(data), second) {
			t.Errorf("%s holds a part of the multiline value", entry.Name())
		}
	}
}

func TestVerificationLineRedactionMasksWholeAndCutSecrets(t *testing.T) {
	r := verificationLineRedaction{redact: func(s string) string { return s }, secrets: []string{"opaque-token-o", "abcd"}}
	for _, c := range []struct {
		text      string
		truncated bool
		want      string
	}{
		{"value opaque-token-o", true, "value [REDACTED]"},
		{"value opaque-token-", true, "value [REDACTED]"},
		{"opaque-token-o x ab", true, "[REDACTED] x [REDACTED]"},
		{"x abcd abc", true, "x [REDACTED] [REDACTED]"},
		{"x abcdabc", true, "x [REDACTED]"},
		{"plain text", true, "plain text"},
		{"value opaque-token-o", false, "value opaque-token-o"},
	} {
		if got := r.line(c.text, c.truncated); got != c.want {
			t.Errorf("line(%q, %v) = %q, want %q", c.text, c.truncated, got, c.want)
		}
	}
}

func TestSecretPrefixSuffix(t *testing.T) {
	for _, c := range []struct {
		text, secret string
		want         int
	}{
		{"line abc", "abcd", 3},
		{"line abcd", "abcd", 0},
		{"line ab", "abcd", 2},
		{"line", "abcd", 0},
		{"aaaa", "aaab", 3},
		{"x abab", "ababc", 4},
		{"xa", "a", 0},
		{"", "abc", 0},
	} {
		if got := secretPrefixSuffix(c.text, c.secret); got != c.want {
			t.Errorf("secretPrefixSuffix(%q, %q) = %d, want %d", c.text, c.secret, got, c.want)
		}
	}
}
