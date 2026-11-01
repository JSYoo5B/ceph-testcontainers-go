//go:build all || integration

package rgw_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"
)

// rgwDiagnosticGateway is what a failure collection reads from one gateway.
type rgwDiagnosticGateway interface {
	State(context.Context) (*container.State, error)
	Logs(context.Context) (io.ReadCloser, error)
	Admin(context.Context, ...string) ([]byte, error)
}

// rgwDiagnosticSite is one gateway of a failed test, with the redaction its
// admin output and log need.
type rgwDiagnosticSite struct {
	Name     string
	Gateway  rgwDiagnosticGateway
	Commands [][]string
	Redact   func([]byte) string
	// RedactLog redacts the log window; it defaults to Redact.
	RedactLog func([]byte) string
	// CrashExcerpt adds rgwCrashExcerpt of the log window, bound to the
	// container generation that the collection inspected.
	CrashExcerpt bool
}

// rgwDiagnosticBounds bound a failure collection: the whole collection, one
// container state or admin read, and one log read of at most LogBytes.
type rgwDiagnosticBounds struct {
	Total, Step, Log time.Duration
	LogBytes         int64
}

var rgwDefaultDiagnosticBounds = rgwDiagnosticBounds{Total: 45 * time.Second, Step: 8 * time.Second, Log: 10 * time.Second, LogBytes: 2 << 20}

// rgwFailureDiagnostics collects the evidence of a failed multisite test
// under a fresh bound, independent of the test's own expired context. It reads
// every site's container state first, so that a slow admin query cannot hide
// an exited gateway, then each site's admin commands and log tail. Each read
// has its own bound inside the total, and a read that fails, times out or is
// truncated is reported as such: no field is dropped silently. The returned
// lines are already redacted.
func rgwFailureDiagnostics(parent context.Context, bounds rgwDiagnosticBounds, sites []rgwDiagnosticSite) []string {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), bounds.Total)
	defer cancel()
	var lines []string
	states := make(map[string]*container.State)
	step := func(bound time.Duration) (context.Context, context.CancelFunc) {
		return context.WithTimeout(ctx, bound)
	}
	for _, site := range sites {
		stateCtx, done := step(bounds.Step)
		state, err := site.Gateway.State(stateCtx)
		done()
		switch {
		case err != nil:
			lines = append(lines, fmt.Sprintf("%s gateway container: state unavailable: %v", site.Name, err))
		case state == nil:
			lines = append(lines, site.Name+" gateway container: state unavailable: no state")
		default:
			states[site.Name] = state
			lines = append(lines, fmt.Sprintf("%s gateway container: status=%s running=%t exit=%d oom=%t started=%s finished=%s",
				site.Name, state.Status, state.Running, state.ExitCode, state.OOMKilled, state.StartedAt, state.FinishedAt))
		}
	}
	for _, site := range sites {
		for _, command := range site.Commands {
			label := strings.Join(command[:min(3, len(command))], " ")
			adminCtx, done := step(bounds.Step)
			data, err := site.Gateway.Admin(adminCtx, command...)
			done()
			if err != nil {
				lines = append(lines, fmt.Sprintf("%s native %s diagnostic: unavailable: %v", site.Name, label, err))
				continue
			}
			lines = append(lines, fmt.Sprintf("%s native %s diagnostic: %s", site.Name, label, site.Redact(data)))
		}
		lines = append(lines, rgwLogDiagnostic(ctx, site, bounds, states[site.Name])...)
	}
	return lines
}

func rgwLogDiagnostic(parent context.Context, site rgwDiagnosticSite, bounds rgwDiagnosticBounds, state *container.State) []string {
	ctx, cancel := context.WithTimeout(parent, bounds.Log)
	defer cancel()
	logs, err := site.Gateway.Logs(ctx)
	if err != nil {
		return []string{fmt.Sprintf("%s recent RGW diagnostics: unavailable: %v", site.Name, err)}
	}
	data, omitted, readErr := rgwLogWindow(logs, bounds.LogBytes)
	_ = logs.Close()
	redact := site.RedactLog
	if redact == nil {
		redact = site.Redact
	}
	var notes []string
	if omitted > 0 {
		notes = append(notes, fmt.Sprintf("truncated: kept the first and last %d bytes, omitted %d bytes between them", bounds.LogBytes/2, omitted))
	}
	if readErr != nil {
		notes = append(notes, "read stopped: "+readErr.Error())
	}
	text := redact(data)
	if len(notes) > 0 {
		text += " [" + strings.Join(notes, "; ") + "]"
	}
	lines := []string{site.Name + " recent RGW diagnostics: " + text}
	if site.CrashExcerpt {
		started, finished := "", ""
		if state != nil {
			started, finished = state.StartedAt, state.FinishedAt
		}
		lines = append(lines, site.Name+" "+rgwCrashExcerpt(data, omitted > 0 || readErr != nil, started, finished))
	}
	return lines
}

// rgwCrashRendering is one rendering of a fatal-signal report in a daemon
// log. Ceph prints the report twice: once to standard error, without a
// timestamp, and once in its dump of recent events, with the daemon's own
// time. Both renderings describe one crash.
type rgwCrashRendering struct {
	Signal, Thread string
	// NativeTime is the daemon's timestamp of the report, or zero when the
	// rendering has none. It is never a collection time.
	NativeTime time.Time
	Frames     []string
	// Complete is false when the backtrace was cut: by the frame limit, or
	// because the captured text ended or skipped inside it.
	Complete bool
}

// rgwCrashFrameLimit bounds the frames kept from one rendering.
const rgwCrashFrameLimit = 16

var (
	rgwCrashSignal = regexp.MustCompile(`\*\*\* Caught signal \(([^)]*)\) \*\*`)
	rgwCrashThread = regexp.MustCompile(`^\s*in thread \S+ thread_name:(\S+)`)
	rgwCrashFrame  = regexp.MustCompile(`^\s*\d+: (.*)$`)
	rgwCrashTime   = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+[+-]\d{4}) `)
	rgwCrashAddr   = regexp.MustCompile(` ?\[0x[0-9a-f]+\]`)
)

// rgwCrashRenderings finds every fatal-signal rendering in a captured log.
func rgwCrashRenderings(data []byte) []rgwCrashRendering {
	lines := strings.Split(string(data), "\n")
	var found []rgwCrashRendering
	for i := 0; i < len(lines); i++ {
		match := rgwCrashSignal.FindStringSubmatch(lines[i])
		if match == nil {
			continue
		}
		rendering := rgwCrashRendering{Signal: match[1]}
		if stamp := rgwCrashTime.FindStringSubmatch(lines[i]); stamp != nil {
			rendering.NativeTime, _ = time.Parse("2006-01-02T15:04:05.999999999-0700", stamp[1])
		}
		j := i + 1
		if j < len(lines) {
			if thread := rgwCrashThread.FindStringSubmatch(lines[j]); thread != nil {
				rendering.Thread = thread[1]
				j++
			}
		}
		for ; j < len(lines); j++ {
			line := lines[j]
			if strings.Contains(line, "ceph version") {
				continue
			}
			frame := rgwCrashFrame.FindStringSubmatch(line)
			if frame == nil {
				// Ceph ends a whole backtrace with its NOTE line. Any other
				// text, the end of the capture or rgwLogWindow's marker means
				// the backtrace was cut.
				rendering.Complete = len(rendering.Frames) > 0 && strings.Contains(line, "NOTE:")
				break
			}
			if len(rendering.Frames) == rgwCrashFrameLimit {
				rendering.Complete = false
				for j < len(lines) && rgwCrashFrame.MatchString(lines[j]) {
					j++
				}
				break
			}
			rendering.Frames = append(rendering.Frames, strings.TrimSpace(rgwCrashAddr.ReplaceAllString(frame[1], "")))
		}
		found = append(found, rendering)
		i = j - 1
	}
	return found
}

// fingerprint identifies a rendering by signal, thread and frames, without
// the load addresses that differ between processes.
func (r rgwCrashRendering) fingerprint() string {
	sum := sha256.Sum256([]byte(r.Signal + "\x00" + r.Thread + "\x00" + strings.Join(r.Frames, "\x00")))
	return hex.EncodeToString(sum[:6])
}

// rgwCrashExcerpt summarizes the fatal-signal renderings of a captured log
// against the gateway generation that a test bound: the container's process
// start and, when known, Docker's finish time. It keeps the daemon's clock and
// Docker's clock apart, counts renderings rather than crashes, and says when
// the capture was truncated. No rendering in the capture does not prove that
// no crash occurred.
func rgwCrashExcerpt(data []byte, truncated bool, startedAt, finishedAt string) string {
	renderings := rgwCrashRenderings(data)
	note := ""
	if truncated {
		note = "; the capture was truncated"
	}
	if len(renderings) == 0 {
		return "crash excerpt: no fatal-signal rendering in the captured log" + note
	}
	started, startErr := time.Parse(time.RFC3339Nano, startedAt)
	finished, finishErr := time.Parse(time.RFC3339Nano, finishedAt)
	parts := []string{fmt.Sprintf("crash excerpt: %d fatal-signal renderings in the captured log (one crash prints two; renderings are not a crash count)%s", len(renderings), note)}
	dataSync := false
	for i, r := range renderings {
		when := "no native timestamp"
		if !r.NativeTime.IsZero() {
			when = "native time " + r.NativeTime.UTC().Format(time.RFC3339Nano)
			switch {
			case startErr != nil:
				when += ", generation unknown"
			case r.NativeTime.Before(started):
				when += ", before the bound generation started"
			default:
				when += ", in the bound generation"
			}
			if finishErr == nil && !finished.IsZero() && finished.Year() > 1 {
				when += fmt.Sprintf(", %s before Docker's finish time", finished.Sub(r.NativeTime).Round(time.Millisecond))
			}
		}
		frames := strings.Join(r.Frames, " | ")
		if !r.Complete {
			frames += " [backtrace cut]"
		}
		parts = append(parts, fmt.Sprintf("[%d] signal=%s thread=%s %s fingerprint=%s frames: %s", i+1, r.Signal, r.Thread, when, r.fingerprint(), frames))
		for _, frame := range r.Frames {
			if strings.Contains(frame, "RGWRemoteDataLog::run_sync") {
				dataSync = true
			}
		}
	}
	if dataSync {
		parts = append(parts, "classification: native RGW data-sync crash, a documented limitation of RGW multisite (rgw/multisite README); the fixture does not restart the gateway or replay requests")
	} else {
		parts = append(parts, "classification: native RGW crash of unclassified cause")
	}
	return strings.Join(parts, "; ")
}

// rgwLogOmitted marks where rgwLogWindow dropped the middle of a log.
const rgwLogOmitted = "\n[log bytes omitted here]\n"

// rgwLogWindow reads a whole log stream, keeping at most limit bytes: the
// first and the last limit/2 of it. A crash prints its signal and backtrace
// at the end of the process's output, so the tail keeps them, and the head
// keeps the start of the generation. omitted counts the dropped bytes; a read
// error ends the window where it occurred.
func rgwLogWindow(r io.Reader, limit int64) ([]byte, int64, error) {
	head, err := io.ReadAll(io.LimitReader(r, limit-limit/2))
	if err != nil || int64(len(head)) < limit-limit/2 {
		return head, 0, err
	}
	tailLimit := int(limit / 2)
	var tail []byte
	var omitted int64
	chunk := make([]byte, 32<<10)
	for {
		n, readErr := r.Read(chunk)
		tail = append(tail, chunk[:n]...)
		if extra := len(tail) - tailLimit; extra > 0 {
			omitted += int64(extra)
			tail = append(tail[:0], tail[extra:]...)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			err = readErr
			break
		}
	}
	if omitted == 0 {
		return append(head, tail...), 0, err
	}
	return append(append(head, rgwLogOmitted...), tail...), omitted, err
}
