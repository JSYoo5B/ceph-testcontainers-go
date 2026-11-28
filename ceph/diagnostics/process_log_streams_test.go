package diagnostics

import (
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/internal/evidence"
)

// readStreamsLog reads entries as the log of an RGW whose process started
// at one second, redacting secrets.
func readStreamsLog(t *testing.T, secrets []string, entries ...fakeLogEntry) *ProcessLog {
	t.Helper()
	ctr := &fakeLogContainer{started: []time.Duration{0}, entries: append([]fakeLogEntry{startEntry(time.Second, "ceph-rgw", 3)}, entries...)}
	log, err := readProcessLog(t.Context(), newProcessLogDocker(map[string]*fakeLogContainer{"r": ctr}), logMember("rgw", "rgw", "r"), LogConfig{MaxBytes: 1 << 20}, logRedactor(secrets), nil)
	if err != nil {
		t.Fatal(err)
	}
	return log
}

func TestProcessLogFollowsKeyBlocksPerStream(t *testing.T) {
	// stderr carries a key block; stdout carries a block of its own that
	// closes while the one on stderr is still open, and ordinary lines.
	log := readStreamsLog(t, nil,
		fakeLogEntry{at: 2 * time.Second, stream: 1, text: "stdout before"},
		fakeLogEntry{at: 3 * time.Second, stream: 2, text: "-----BEGIN PRIVATE KEY-----"},
		fakeLogEntry{at: 4 * time.Second, stream: 1, text: "-----BEGIN PRIVATE KEY-----"},
		fakeLogEntry{at: 5 * time.Second, stream: 2, text: "PAYLOADONE"},
		fakeLogEntry{at: 6 * time.Second, stream: 1, text: "-----END PRIVATE KEY-----"},
		fakeLogEntry{at: 7 * time.Second, stream: 2, text: "PAYLOADTWO"},
		fakeLogEntry{at: 8 * time.Second, stream: 1, text: "stdout after"},
		fakeLogEntry{at: 9 * time.Second, stream: 2, text: "-----END PRIVATE KEY-----"},
	)
	joined := strings.Join(lineTexts(log.Lines), "\n")
	if strings.Contains(joined, "PAYLOAD") || !strings.Contains(joined, "stdout before") || !strings.Contains(joined, "stdout after") {
		t.Fatalf("lines:\n%s", joined)
	}
}

func TestProcessLogWithholdsBeforeAnOrphanEnd(t *testing.T) {
	// The block began before the window: its payload and END are read, its
	// BEGIN is not.
	log := readStreamsLog(t, nil,
		fakeLogEntry{at: 2 * time.Second, stream: 2, text: "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQ"},
		fakeLogEntry{at: 3 * time.Second, stream: 1, text: "stdout line"},
		fakeLogEntry{at: 4 * time.Second, stream: 2, text: "AoIBAQC7VJTUt9Us8cKj"},
		fakeLogEntry{at: 5 * time.Second, stream: 2, text: "-----END PRIVATE KEY----- after"},
	)
	joined := strings.Join(lineTexts(log.Lines), "\n")
	if strings.Contains(joined, "MIIEvQ") || strings.Contains(joined, "AoIBAQC7") || !strings.Contains(joined, "stdout line") || !strings.Contains(joined, " after") {
		t.Fatalf("lines:\n%s", joined)
	}
}

func TestProcessLogRedactsASecretSplitByTheCut(t *testing.T) {
	const secret = "AQBsplitsecretsplitsecretsplit=="
	stamp := processLogEpoch.Add(2*time.Second).Format(time.RFC3339Nano) + " "
	// The cut at 64 KiB falls inside the secret.
	text := strings.Repeat("y", evidence.MaxLogLine-len(stamp)-len(secret)/2) + secret + " rest"
	log := readStreamsLog(t, []string{secret}, fakeLogEntry{at: 2 * time.Second, stream: 2, text: text})
	last := log.Lines[len(log.Lines)-1].Text
	if strings.Contains(last, secret[:len(secret)/2]) || strings.Contains(last, secret[:4]) || log.Status != LogTruncated {
		t.Fatalf("status %s, cut line ends %q", log.Status, last[max(len(last)-64, 0):])
	}
}

func TestProcessLogSplitsStreamsApart(t *testing.T) {
	log := readStreamsLog(t, nil,
		fakeLogEntry{at: 2 * time.Second, stream: 1, text: "stdout part", partial: true},
		fakeLogEntry{at: 3 * time.Second, stream: 2, text: "stderr line"},
	)
	// The stdout line has no newline, so it ends with its stream, after the
	// stderr line.
	texts := lineTexts(log.Lines[1:])
	if len(texts) != 2 || texts[0] != "stderr line" || texts[1] != "stdout part" {
		t.Fatalf("lines %q", texts)
	}
}

func TestProcessLogCountsALineWithoutTimestampAsALoss(t *testing.T) {
	log := readStreamsLog(t, nil, fakeLogEntry{stream: 2, text: "no timestamp", bare: true})
	if log.Status == LogComplete {
		t.Fatalf("status %s with lines %q", log.Status, lineTexts(log.Lines))
	}
}
