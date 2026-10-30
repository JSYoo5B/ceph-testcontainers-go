//go:build all || integration

package rgw_test

import (
	"context"
	"fmt"
	"io"
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
	// RedactLog redacts the log tail; it defaults to Redact.
	RedactLog func([]byte) string
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
		lines = append(lines, rgwLogDiagnostic(ctx, site, bounds))
	}
	return lines
}

func rgwLogDiagnostic(parent context.Context, site rgwDiagnosticSite, bounds rgwDiagnosticBounds) string {
	ctx, cancel := context.WithTimeout(parent, bounds.Log)
	defer cancel()
	logs, err := site.Gateway.Logs(ctx)
	if err != nil {
		return fmt.Sprintf("%s recent RGW diagnostics: unavailable: %v", site.Name, err)
	}
	data, readErr := io.ReadAll(io.LimitReader(logs, bounds.LogBytes+1))
	_ = logs.Close()
	redact := site.RedactLog
	if redact == nil {
		redact = site.Redact
	}
	var notes []string
	if int64(len(data)) > bounds.LogBytes {
		data = data[:bounds.LogBytes]
		notes = append(notes, fmt.Sprintf("truncated at %d bytes", bounds.LogBytes))
	}
	if readErr != nil {
		notes = append(notes, "read stopped: "+readErr.Error())
	}
	text := redact(data)
	if len(notes) > 0 {
		text += " [" + strings.Join(notes, "; ") + "]"
	}
	return site.Name + " recent RGW diagnostics: " + text
}
