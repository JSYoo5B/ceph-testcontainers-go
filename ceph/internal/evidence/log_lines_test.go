package evidence

import (
	"bytes"
	"slices"
	"strings"
	"testing"
	"time"
)

func readTestLines(t *testing.T, stream []byte, tty bool, limit int64) ([]LogLine, LogRead, error) {
	t.Helper()
	var lines []LogLine
	read, err := ReadLogLines(bytes.NewReader(stream), tty, limit, func(line LogLine) bool {
		lines = append(lines, line)
		return true
	})
	return lines, read, err
}

func TestReadLogLinesSplitsEachStream(t *testing.T) {
	stamp := "2026-11-27T04:00:00.5Z "
	var stream []byte
	stream = append(stream, logFrame(1, stamp+"stdout part")...)
	stream = append(stream, logFrame(2, stamp+"stderr line\r\r\n")...)
	stream = append(stream, logFrame(1, " rest\nno timestamp\n")...)
	lines, read, err := readTestLines(t, stream, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []LogLine{
		{Stream: "stderr", Time: time.Date(2026, 11, 27, 4, 0, 0, 5e8, time.UTC), Text: "stderr line"},
		{Stream: "stdout", Time: time.Date(2026, 11, 27, 4, 0, 0, 5e8, time.UTC), Text: "stdout part rest"},
		{Stream: "stdout", Text: "no timestamp", Malformed: true},
	}
	if !slices.Equal(lines, want) || read.Lines != 3 || read.Malformed != 1 || read.Bytes != int64(len(stream)) {
		t.Fatalf("lines %+v, read %+v", lines, read)
	}
}

func TestReadLogLinesCutsAndBounds(t *testing.T) {
	stamp := "2026-11-27T04:00:00Z "
	long := stamp + strings.Repeat("x", MaxLogLine) + "\n"
	lines, read, err := readTestLines(t, []byte(long+stamp+"next\n"), true, 0)
	if err != nil || len(lines) != 2 || !lines[0].Cut || len(stamp)+len(lines[0].Text) != MaxLogLine || lines[1].Text != "next" || read.Cut != 1 {
		t.Fatalf("lines %d, read %+v, %v", len(lines), read, err)
	}
	// The limit ends the stream inside a line, which may lack its end.
	stream := []byte(stamp + "first\n" + stamp + "second\n")
	lines, read, err = readTestLines(t, stream, true, int64(len(stamp)+8))
	if err != nil || !read.Exceeded || read.Bytes != int64(len(stamp)+8) || len(lines) != 2 || lines[0].Ended || !lines[1].Ended {
		t.Fatalf("lines %+v, read %+v, %v", lines, read, err)
	}
	// A stream cut inside a frame is an error, and its pending line ended.
	frame := logFrame(1, stamp+"whole\n"+stamp+"partial")
	lines, _, err = readTestLines(t, frame[:len(frame)-2], false, 0)
	if err != ErrLogFrameCut || len(lines) != 2 || !lines[1].Ended {
		t.Fatalf("lines %+v, %v", lines, err)
	}
}

func TestReadLogLinesStopsWhenTheVisitorIsDone(t *testing.T) {
	stamp := "2026-11-27T04:00:00Z "
	var seen []string
	read, err := ReadLogLines(strings.NewReader(stamp+"a\n"+stamp+"b\n"+stamp+"c"), true, 0, func(line LogLine) bool {
		seen = append(seen, line.Text)
		return line.Text != "b"
	})
	if err != nil || !slices.Equal(seen, []string{"a", "b"}) || read.Lines != 2 {
		t.Fatalf("seen %q, read %+v, %v", seen, read, err)
	}
}

func TestKeyBlockFollowerKeepsOneStatePerStream(t *testing.T) {
	var f KeyBlockFollower
	type step struct {
		stream, text, safe string
		withhold           int
	}
	for i, s := range []step{
		{"stderr", "-----BEGIN PRIVATE KEY-----", PrivateKeyMarker, -1},
		{"stdout", "-----BEGIN PRIVATE KEY-----", PrivateKeyMarker, -1},
		{"stderr", "PAYLOAD", PrivateKeyMarker, -1},
		{"stdout", "-----END PRIVATE KEY----- out", PrivateKeyMarker + " out", -1},
		{"stderr", "PAYLOAD", PrivateKeyMarker, -1},
		{"stderr", "-----END PRIVATE KEY----- err", PrivateKeyMarker + " err", -1},
		// An end without a beginning withholds back to the stream's last end.
		{"stdout", "plain", "plain", -1},
		{"stderr", "PAYLOAD", "PAYLOAD", -1},
		{"stderr", "tail -----END RSA PRIVATE KEY----- x", PrivateKeyMarker + " x", 6},
		{"stdout", "a -----BEGIN PRIVATE KEY----- b -----END PRIVATE KEY----- c", "a " + PrivateKeyMarker + " c", -1},
	} {
		safe, line, withhold := f.Follow(s.stream, s.text)
		if safe != s.safe || line != i || withhold != s.withhold {
			t.Errorf("line %d (%s %q) = %q, %d, %d; want %q, %d", i, s.stream, s.text, safe, line, withhold, s.safe, s.withhold)
		}
	}
}

func TestLogLineRedactorMasksWholeAndCutSecrets(t *testing.T) {
	redact := LogLineRedactor([]string{"opaque-token-o", "abcd"}, func(s string) string { return s })
	for _, c := range []struct {
		text       string
		missingEnd bool
		want       string
	}{
		{"value opaque-token-o", true, "value [REDACTED]"},
		{"value opaque-token-", true, "value [REDACTED]"},
		{"opaque-token-o x ab", true, "[REDACTED] x [REDACTED]"},
		{"x abcd abc", true, "x [REDACTED] [REDACTED]"},
		{"x abcdabc", true, "x [REDACTED]"},
		{"plain text", true, "plain text"},
		{"value opaque-token-o", false, "value opaque-token-o"},
	} {
		if got := redact(c.text, c.missingEnd); got != c.want {
			t.Errorf("redact(%q, %v) = %q, want %q", c.text, c.missingEnd, got, c.want)
		}
	}
}
