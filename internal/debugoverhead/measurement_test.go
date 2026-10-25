package debugoverhead_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestDebugOverheadAccountingRejectsUnmatchedSamples(t *testing.T) {
	gen := containerGeneration{"osd-container", "2026-01-01T00:00:00Z"}
	body := `{"id":"osd-container","read":"2026-01-01T00:00:01Z","cpu_stats":{"cpu_usage":{"total_usage":1000000000}},"memory_stats":{"usage":1048576}}`
	first, err := parseDebugOverheadSample(gen, []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	zero := strings.Replace(strings.Replace(body, `1000000000`, `0`, 1), `1048576`, `0`, 1)
	if sample, err := parseDebugOverheadSample(gen, []byte(zero)); err != nil || sample.CPUNanos != 0 || sample.Memory != 0 {
		t.Fatalf("observed zero counters treated as unavailable: %+v %v", sample, err)
	}
	for name, data := range map[string]string{
		"other container": strings.Replace(body, `"osd-container"`, `"other"`, 1),
		"no read time":    strings.Replace(body, `"read":`, `"unknown":`, 1),
		"no CPU":          strings.Replace(body, `"total_usage":`, `"unknown":`, 1),
		"no memory":       strings.Replace(body, `"usage":1048576`, `"unknown":1048576`, 1),
		"null CPU":        strings.Replace(body, `1000000000`, `null`, 1),
		"fractional CPU":  strings.Replace(body, `1000000000`, `1.5`, 1),
		"huge memory":     strings.Replace(body, `1048576`, `18446744073709551615`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseDebugOverheadSample(gen, []byte(data)); err == nil {
				t.Fatal("unavailable accounting accepted")
			}
		})
	}
	last := first
	last.Read = first.Read.Add(time.Second)
	last.CPUNanos += 123
	last.Memory -= 256
	if cpu, memory, err := debugOverheadDelta(first, last); err != nil || cpu != 123 || memory != -256 {
		t.Fatalf("wrong CPU nanoseconds or signed memory delta: %d %d %v", cpu, memory, err)
	}
	for name, edit := range map[string]func(*debugOverheadSample){
		"restart":       func(s *debugOverheadSample) { s.Generation.StartedAt = "another-start" },
		"replacement":   func(s *debugOverheadSample) { s.Generation.ID = "another-container" },
		"CPU reset":     func(s *debugOverheadSample) { s.CPUNanos = 0 },
		"same time":     func(s *debugOverheadSample) { s.Read = first.Read },
		"backward time": func(s *debugOverheadSample) { s.Read = first.Read.Add(-time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			changed := last
			edit(&changed)
			if _, _, err := debugOverheadDelta(first, changed); err == nil {
				t.Fatal("incomparable accounting accepted")
			}
		})
	}
}

func TestDebugOverheadLogPayloadAccounting(t *testing.T) {
	frame := func(kind byte, payload string) []byte {
		header := make([]byte, 8)
		header[0] = kind
		binary.BigEndian.PutUint32(header[4:], uint32(len(payload)))
		return append(header, []byte(payload)...)
	}
	stream := append(frame(1, "out\n"), frame(2, "error\n")...)
	if n, err := debugOverheadLogBytes(bytes.NewReader(stream), false, 10); err != nil || n != 10 {
		t.Fatalf("frame headers were counted as output, or streams lost: %d %v", n, err)
	}
	for _, tc := range []struct {
		name  string
		data  []byte
		limit int64
	}{
		{"too much output", stream, 9},
		{"short header", []byte{1, 0}, 10},
		{"short payload", frame(1, "payload")[:10], 10},
		{"non-output frame", frame(3, "no"), 10},
		{"huge advertised frame", []byte{1, 0, 0, 0, 255, 255, 255, 255}, 10},
		{"no bound", nil, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if n, err := debugOverheadLogBytes(bytes.NewReader(tc.data), false, tc.limit); err == nil {
				t.Fatalf("incomplete stream became a count %d", n)
			}
		})
	}
	if n, err := debugOverheadLogBytes(strings.NewReader("tty\n"), true, 4); err != nil || n != 4 {
		t.Fatal(fmt.Sprint(n, err))
	}
	if _, err := debugOverheadLogBytes(strings.NewReader("too long"), true, 4); err == nil {
		t.Fatal("unbounded TTY output accepted")
	}
}
