//go:build all || integration

package integration_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"
	"unicode/utf8"
)

// LineBytes is aligned with Lines. Original counts bytes before UTF-8 repair
// and redaction; Redacted is the safe full line; Captured includes any marker.
type monReplacementTraceBytes struct {
	Original int `json:"original"`
	Redacted int `json:"redacted"`
	Captured int `json:"captured"`
}

const monReplacementTraceInputBytes = 8 << 20

var (
	monTracePrivateKeyStart = regexp.MustCompile(`-----BEGIN (?:[A-Z ]+)?PRIVATE KEY-----`)
	monTracePrivateKeyEnd   = regexp.MustCompile(`-----END (?:[A-Z ]+)?PRIVATE KEY-----`)
)

// Redact a private-key block before selecting or clipping individual lines.
// A block may span lines, including lines with unrelated log prefixes.
func redactMonReplacementTraceLine(raw string, inKey *bool) string {
	line := strings.ToValidUTF8(raw, "�")
	var out strings.Builder
	for {
		if *inKey {
			end := monTracePrivateKeyEnd.FindStringIndex(line)
			if end == nil {
				return monReplacementRedact(out.String() + "[REDACTED PRIVATE KEY]")
			}
			out.WriteString("[REDACTED PRIVATE KEY]")
			line, *inKey = line[end[1]:], false
		}
		start := monTracePrivateKeyStart.FindStringIndex(line)
		if start == nil {
			out.WriteString(line)
			return monReplacementRedact(out.String())
		}
		out.WriteString(line[:start[0]])
		line, *inKey = line[start[1]:], true
	}
}

// clipMonReplacementLine retains both ends of a redacted line under the old
// 512-byte budget, with an explicit omission. Every cut is a UTF-8 boundary.
func clipMonReplacementLine(line string) (string, bool) {
	if len(line) <= monReplacementTraceLineSize {
		return line, false
	}
	const omitted = " [...omitted...] "
	budget := monReplacementTraceLineSize - len(omitted)
	prefix := budget / 2
	for prefix > 0 && !utf8.RuneStart(line[prefix]) {
		prefix--
	}
	tail := len(line) - (budget - budget/2)
	for tail < len(line) && !utf8.RuneStart(line[tail]) {
		tail++
	}
	return line[:prefix] + omitted + line[tail:], true
}

// The caller owns logs and closes it after this function. Closing on expiry
// releases a blocked stream read; the callback is joined before returning, so
// neither the collector nor a cancellation goroutine outlives its phase.
func scanMonReplacementTrace(ctx context.Context, logs io.ReadCloser, trace *monReplacementTrace) error {
	closed := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = logs.Close()
		close(closed)
	})
	defer func() {
		if !stop() {
			<-closed
		}
	}()
	abort := func(err error) error {
		trace.StreamAborted, trace.Truncated = true, true
		return fmt.Errorf("logs: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return abort(err)
	}
	input := &io.LimitedReader{R: logs, N: monReplacementTraceInputBytes + 1}
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	inKey := false
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return abort(err)
		}
		// The extra byte is a sentinel: never accept a partial final line
		// when the bounded stream may contain more evidence.
		if input.N <= 0 {
			return abort(fmt.Errorf("input exceeds %d bytes", monReplacementTraceInputBytes))
		}
		raw := scanner.Text()
		matched := monReplacementTracePattern.MatchString(raw)
		if !matched && !inKey && !strings.Contains(raw, "PRIVATE KEY-----") {
			continue
		}
		line := redactMonReplacementTraceLine(raw, &inKey)
		if !matched {
			continue
		}
		trace.Matched++
		if !utf8.ValidString(raw) {
			trace.InvalidUTF8Lines++
			trace.Truncated = true
		}
		trace.MatchedBytes += len(raw)
		captured, clipped := clipMonReplacementLine(line)
		if clipped {
			trace.ClippedLines++
			trace.Truncated = true
		}
		if len(trace.Lines) == monReplacementTraceLines {
			trace.RetainedBytes -= len(trace.Lines[0])
			trace.Lines, trace.LineBytes = trace.Lines[1:], trace.LineBytes[1:]
			trace.DroppedLines++
			trace.Truncated = true
		}
		trace.Lines = append(trace.Lines, captured)
		trace.LineBytes = append(trace.LineBytes, monReplacementTraceBytes{Original: len(raw), Redacted: len(line), Captured: len(captured)})
		trace.RetainedBytes += len(captured)
	}
	if err := ctx.Err(); err != nil {
		return abort(err)
	}
	if err := scanner.Err(); err != nil {
		return abort(err)
	}
	if input.N <= 0 {
		return abort(fmt.Errorf("input exceeds %d bytes", monReplacementTraceInputBytes))
	}
	trace.StreamComplete = true
	return nil
}
