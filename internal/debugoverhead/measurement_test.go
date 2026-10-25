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
		"other container":   strings.Replace(body, `"osd-container"`, `"other"`, 1),
		"null ID":           strings.Replace(body, `"id":"osd-container"`, `"id":null`, 1),
		"empty ID":          strings.Replace(body, `"id":"osd-container"`, `"id":""`, 1),
		"mistyped ID":       strings.Replace(body, `"id":"osd-container"`, `"id":123`, 1),
		"no read time":      strings.Replace(body, `"read":`, `"unknown":`, 1),
		"no CPU":            strings.Replace(body, `"total_usage":`, `"unknown":`, 1),
		"no memory":         strings.Replace(body, `"usage":1048576`, `"unknown":1048576`, 1),
		"null CPU":          strings.Replace(body, `1000000000`, `null`, 1),
		"fractional CPU":    strings.Replace(body, `1000000000`, `1.5`, 1),
		"huge memory":       strings.Replace(body, `1048576`, `18446744073709551615`, 1),
		"wrong CPU nesting": strings.Replace(body, `"cpu_stats":{"cpu_usage":{"total_usage":1000000000}}`, `"cpu_stats":{},"cpu_usage":{"total_usage":1000000000}`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseDebugOverheadSample(gen, []byte(data)); err == nil {
				t.Fatal("unavailable accounting accepted")
			}
		})
	}
	withoutID := strings.Replace(body, `"id":"osd-container",`, ``, 1)
	without, err := parseDebugOverheadSample(gen, []byte(withoutID))
	if err != nil || without.StatsIdentity != "endpoint-generation" || without.Generation != gen || without.CPUNanos != first.CPUNanos || without.Memory != first.Memory || !without.Read.Equal(first.Read) {
		t.Fatalf("valid one-shot response without optional ID was refused or changed counters: %+v %v", without, err)
	}
	if first.StatsIdentity != "response-and-endpoint-generation" {
		t.Fatalf("response identity evidence lost: %+v", first)
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

func TestDebugOverheadRefusalEvidenceIsBoundedAndAllowlisted(t *testing.T) {
	gen := containerGeneration{"osd-container", "2026-01-01T00:00:00Z"}
	secret := strings.Repeat("sensitive-value-", 1000)
	body := fmt.Sprintf(`{"id":%q,"read":%q,"cpu_stats":{"cpu_usage":{"total_usage":null}},"memory_stats":{},"unrelated":%q}`, secret, secret, secret)
	_, err := parseDebugOverheadSample(gen, []byte(body))
	if err == nil {
		t.Fatal("malformed selected fields accepted")
	}
	text := err.Error()
	for _, expected := range []string{"json=valid", "generation=complete", "response_id=contradictory", "read=invalid", "cpu_total_usage=null", "memory_usage=missing"} {
		if !strings.Contains(text, expected) {
			t.Fatalf("selected field evidence %q missing from %q", expected, text)
		}
	}
	if len(text) > 384 || strings.Contains(text, "sensitive-value") || strings.Contains(text, "unrelated") || strings.Contains(text, gen.ID) {
		t.Fatalf("raw response leaked or refusal was unbounded: %q", text)
	}
	_, err = parseDebugOverheadSample(gen, []byte(`{"unrelated":"sensitive-value",`))
	if err == nil || !strings.Contains(err.Error(), "json=invalid") || strings.Contains(err.Error(), "sensitive-value") {
		t.Fatalf("invalid JSON diagnostic leaked input or guessed fields: %v", err)
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

func TestDebugOperationIntervalRefusesUnboundedOrUnorderedEvidence(t *testing.T) {
	begin := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	valid := debugOperationInterval{StartedAt: begin, FinishedAt: begin.Add(time.Second), BudgetNanos: int64(2 * time.Second)}
	if err := valid.validate(); err != nil {
		t.Fatal(err)
	}
	for _, interval := range []debugOperationInterval{
		{FinishedAt: valid.FinishedAt, BudgetNanos: valid.BudgetNanos},
		{StartedAt: begin, BudgetNanos: valid.BudgetNanos},
		{StartedAt: begin, FinishedAt: begin, BudgetNanos: valid.BudgetNanos},
		{StartedAt: valid.FinishedAt, FinishedAt: begin, BudgetNanos: valid.BudgetNanos},
		{StartedAt: begin, FinishedAt: valid.FinishedAt},
		{StartedAt: begin, FinishedAt: valid.FinishedAt, BudgetNanos: int64(time.Millisecond)},
	} {
		if err := interval.validate(); err == nil {
			t.Fatalf("invalid interval admitted %+v", interval)
		}
	}
}
