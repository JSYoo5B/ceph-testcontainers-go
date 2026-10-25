//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/testcontainers/testcontainers-go"
)

type monTraceContainer struct {
	testcontainers.Container
	open func(context.Context) (io.ReadCloser, error)
}

func (c monTraceContainer) Logs(ctx context.Context) (io.ReadCloser, error) { return c.open(ctx) }

func monTraceText(text string) monTraceContainer {
	return monTraceContainer{open: func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(text)), nil
	}}
}

func TestMonReplacementTraceEvidence(t *testing.T) {
	const cause = "handle_auth_bad_method result=-13 available=cephx"
	for _, tc := range []struct {
		name, input                        string
		matched, clipped, dropped, invalid int
		aborted                            bool
		cause                              bool
	}{
		{name: "short", input: "monclient(hunting): " + cause + "\n", matched: 1, cause: true},
		{name: "auth tail beyond prefix", input: "monclient(hunting): " + strings.Repeat("x", 540) + cause + "\n", matched: 1, clipped: 1, cause: true},
		{name: "oversized unrelated line aborts stream", input: strings.Repeat("x", 1<<20+1) + "\nmonclient: " + cause, aborted: true},
		{name: "repaired invalid UTF8", input: "monclient: " + string([]byte{0xff, 0xc3}) + cause, matched: 1, invalid: 1, cause: true},
		{name: "unicode clipping", input: "monclient: " + strings.Repeat("界", 190) + cause, matched: 1, clipped: 1, cause: true},
		{name: "line rolling budget", input: strings.Repeat("monclient: normal observation\n", 302), matched: 302, dropped: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			trace := monReplacementTrace{Daemon: "mds.synthetic"}
			err := readMonReplacementTrace(t.Context(), monTraceText(tc.input), time.Second, &trace)
			if (err != nil) != tc.aborted || trace.StreamAborted != tc.aborted || trace.StreamComplete == tc.aborted || trace.Matched != tc.matched || trace.ClippedLines != tc.clipped || trace.DroppedLines != tc.dropped || trace.InvalidUTF8Lines != tc.invalid {
				t.Fatalf("trace=%+v err=%v", trace, err)
			}
			if trace.Truncated != (tc.aborted || tc.clipped > 0 || tc.dropped > 0 || tc.invalid > 0) {
				t.Fatalf("silent evidence loss: %+v", trace)
			}
			if len(trace.Lines) != min(tc.matched, monReplacementTraceLines) || len(trace.LineBytes) != len(trace.Lines) || trace.RetainedBytes > monReplacementTraceLines*monReplacementTraceLineSize {
				t.Fatalf("budgets: %+v", trace)
			}
			size := 0
			for i, line := range trace.Lines {
				size += len(line)
				if !utf8.ValidString(line) || len(line) > monReplacementTraceLineSize || trace.LineBytes[i].Captured != len(line) || trace.LineBytes[i].Original <= 0 || trace.LineBytes[i].Redacted < len(line) {
					t.Fatalf("line %d %q metadata=%+v", i, line, trace.LineBytes[i])
				}
			}
			if size != trace.RetainedBytes || strings.Contains(strings.Join(trace.Lines, "\n"), cause) != tc.cause {
				t.Fatalf("captured bytes/cause: %+v", trace)
			}
			if tc.clipped > 0 && !strings.Contains(trace.Lines[0], "[...omitted...]") {
				t.Fatal("clipping marker absent")
			}
		})
	}
}

func TestMonReplacementTraceRedactsBeforeClipping(t *testing.T) {
	input := "monclient: password=synthetic-password " + strings.Repeat("x", 520) + " authorization=Bearer-synthetic\n" +
		"monclient: -----BEGIN PRIVATE KEY-----\nmonclient: synthetic-private-content\n-----END PRIVATE KEY-----\n" +
		"auth token=synthetic-token " + strings.Repeat("界", 180)
	var trace monReplacementTrace
	if err := readMonReplacementTrace(t.Context(), monTraceText(input), time.Second, &trace); err != nil {
		t.Fatal(err)
	}
	out := strings.Join(trace.Lines, "\n")
	for _, secret := range []string{"synthetic-password", "Bearer-synthetic", "synthetic-private-content", "synthetic-token"} {
		if strings.Contains(out, secret) {
			t.Fatalf("secret survived: %s", secret)
		}
	}
	if !strings.Contains(out, "[REDACTED") || trace.ClippedLines != 2 {
		t.Fatalf("redaction/clipping: %+v", trace)
	}
}

type monTraceBlockedReader struct {
	closed chan struct{}
	once   sync.Once
}

func (r *monTraceBlockedReader) Read([]byte) (int, error) { <-r.closed; return 0, io.EOF }
func (r *monTraceBlockedReader) Close() error             { r.once.Do(func() { close(r.closed) }); return nil }

func TestMonReplacementTraceBounds(t *testing.T) {
	t.Run("expired context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		var trace monReplacementTrace
		err := readMonReplacementTrace(ctx, monTraceText("auth result=-13"), time.Second, &trace)
		if err == nil || !trace.StreamAborted || trace.Matched != 0 {
			t.Fatalf("expired sample admitted: %+v %v", trace, err)
		}
	})
	t.Run("blocked reader closed on deadline", func(t *testing.T) {
		r := &monTraceBlockedReader{closed: make(chan struct{})}
		ctr := monTraceContainer{open: func(context.Context) (io.ReadCloser, error) { return r, nil }}
		var trace monReplacementTrace
		started := time.Now()
		err := readMonReplacementTrace(t.Context(), ctr, 25*time.Millisecond, &trace)
		if err == nil || !trace.StreamAborted || time.Since(started) > time.Second {
			t.Fatalf("blocked stream: %+v %v", trace, err)
		}
		select {
		case <-r.closed:
		default:
			t.Fatal("owned stream left open")
		}
	})
	t.Run("total input budget", func(t *testing.T) {
		// Every individual line fits the scanner, but the stream exceeds its
		// independent finite byte budget before a later authentication result.
		input := strings.Repeat(strings.Repeat("x", 1023)+"\n", monReplacementTraceInputBytes/1024+1) + "auth result=-13\n"
		var trace monReplacementTrace
		err := readMonReplacementTrace(t.Context(), monTraceText(input), 5*time.Second, &trace)
		if err == nil || !trace.StreamAborted || trace.Matched != 0 || !strings.Contains(fmt.Sprint(err), "input exceeds") {
			t.Fatalf("input budget: %+v %v", trace, err)
		}
	})
}
