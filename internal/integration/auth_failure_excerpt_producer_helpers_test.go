//go:build all || integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// authCauseUnavailable is the cause of a refusal that no MON log line or
// client methods line establishes: an excerpt gives where a refusal was
// printed, never why it happened.
const authCauseUnavailable = "unavailable"

// authExcerptRedactionBudget bounds each redaction of a first-failure excerpt.
// The adapter derives its context from the caller's without its cancellation,
// since the test's own context may have ended with the failure.
const authExcerptRedactionBudget = 30 * time.Second

// authRedactionSource is what the excerpt redactor needs; *ceph.Container
// provides it.
type authRedactionSource interface {
	RedactSecrets(ctx context.Context, data []byte, values ...string) ([]byte, error)
}

// newAuthExcerptRedactor adapts the context-bounded RedactSecrets to the
// redaction step of buildAuthFailureExcerpt. A nil source, including a typed
// nil pointer, yields a nil redactor, which withholds the text. A redaction
// that fails, or that finishes after its budget, withholds the text too: its
// result is not evidence. The returned string is a copy.
func newAuthExcerptRedactor(parent context.Context, source authRedactionSource, budget time.Duration, values ...string) func(string) (string, error) {
	if source == nil {
		return nil
	}
	if v := reflect.ValueOf(source); v.Kind() == reflect.Pointer && v.IsNil() {
		return nil
	}
	return func(text string) (string, error) {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), budget)
		defer cancel()
		redacted, err := source.RedactSecrets(ctx, []byte(text), values...)
		if err != nil {
			return "", err
		}
		if err := ctx.Err(); err != nil {
			return "", err
		}
		return string(redacted), nil
	}
}

// authFailedOperation is the original rejected native operation as its
// producer saw it: the stderr capture of that one invocation, the window
// from before it started until after it returned, and the binding of the
// client process capture that printed it. Binding.Daemon names the
// producing client, not the MGR that the client addressed.
type authFailedOperation struct {
	Operation      string
	Binding        authExcerptBinding
	Start, End     time.Time
	Stderr         []byte
	StderrComplete bool
}

// authRejectionOperation is the identity of the rejected attempt. A control
// has an identity of its own, derived from it.
func authRejectionOperation(r authTestedRejection) string {
	return fmt.Sprintf("%s/cycle-%d/attempt-%d", r.Run, r.Cycle, r.Attempt)
}

// authControlOperation is the identity of the control attempt against target.
func authControlOperation(r authTestedRejection, target string) string {
	return authRejectionOperation(r) + "/control/" + target
}

// authFirstExcerptLine is the non-text provenance of one retained line.
type authFirstExcerptLine struct {
	At    time.Time `json:"at"`
	Stage string    `json:"stage"`
}

// authFirstExcerptRecord is what a bundle retains of the original operation's
// stderr. CommandStderrComplete reports whether the command's stderr was read
// completely; Status reports separately whether the retained excerpt is
// complete, clipped, empty, withheld or unavailable. The record holds the
// redacted, bounded Text and per-line provenance, and neither the raw lines
// nor the lines that selection rejected (only their count).
type authFirstExcerptRecord struct {
	Operation             string                 `json:"operation"`
	Daemon                string                 `json:"daemon,omitempty"`
	Generation            string                 `json:"generation,omitempty"`
	Capture               string                 `json:"capture,omitempty"`
	WindowStart           time.Time              `json:"window_start"`
	WindowEnd             time.Time              `json:"window_end"`
	CommandStderrComplete bool                   `json:"command_stderr_complete"`
	Status                string                 `json:"status"`
	Stage                 string                 `json:"stage"`
	Text                  string                 `json:"text,omitempty"`
	RetainedBytes         int                    `json:"retained_bytes"`
	Lines                 []authFirstExcerptLine `json:"lines,omitempty"`
	RejectedLines         int                    `json:"rejected_lines"`
	Reason                string                 `json:"reason,omitempty"`
}

var authExcerptStatuses = []authExcerptStatus{authExcerptComplete, authExcerptClipped, authExcerptNoLines, authExcerptWithheld, authExcerptUnavailable}

// produceAuthFirstExcerpt builds the first-failure excerpt of op: it selects
// the client-side authentication lines of the operation's own stderr inside
// its window, redacts them and only then bounds them. A selection that cannot
// be made (an unbound capture, a missing or inverted window) gives an
// unavailable excerpt with the reason; it is never an error that could mask
// the refusal. The returned excerpt is what the ledger records and the record
// is what a bundle retains.
func produceAuthFirstExcerpt(op authFailedOperation, redact func(string) (string, error), limit int) (authFailureExcerpt, authFirstExcerptRecord) {
	record := authFirstExcerptRecord{Operation: op.Operation, WindowStart: op.Start, WindowEnd: op.End, CommandStderrComplete: op.StderrComplete}
	if op.Binding.complete() {
		record.Daemon, record.Generation, record.Capture = op.Binding.Daemon, op.Binding.Generation, op.Binding.Capture
	}
	var lines []string
	for _, line := range strings.Split(string(op.Stderr), "\n") {
		lines = append(lines, strings.TrimRight(line, "\r"))
	}
	selection, err := selectAuthFailureLines(op.Binding, lines, op.Start, op.End)
	var excerpt authFailureExcerpt
	if err != nil {
		excerpt = authFailureExcerpt{Operation: op.Operation, Start: op.Start, End: op.End, Status: authExcerptUnavailable, Stage: authStageUnknown}
		record.Reason = err.Error()
	} else {
		excerpt = buildAuthFailureExcerpt(op.Operation, selection, op.Start, op.End, redact, limit)
		record.RejectedLines = len(selection.Rejected)
	}
	record.Status, record.Stage = string(excerpt.Status), string(excerpt.Stage)
	record.Text, record.RetainedBytes = excerpt.Text, excerpt.RetainedBytes
	for _, line := range excerpt.Lines {
		record.Lines = append(record.Lines, authFirstExcerptLine{At: line.At, Stage: string(line.Stage)})
	}
	return excerpt, record
}

// checkAuthFirstExcerpt returns what makes record unusable as the original
// operation's excerpt: another operation's identity, a binding that names the
// intended MGR target instead of the producing client, or a status that
// disagrees with its text.
func checkAuthFirstExcerpt(record authFirstExcerptRecord, rejection authTestedRejection) error {
	var problems []string
	if want := authRejectionOperation(rejection); record.Operation != want {
		problems = append(problems, fmt.Sprintf("excerpt operation %q, not %q", record.Operation, want))
	}
	if record.Daemon != "" && record.Daemon == rejection.Target {
		problems = append(problems, "excerpt is bound to the intended target, not the producing client")
	}
	known := false
	for _, status := range authExcerptStatuses {
		known = known || record.Status == string(status)
	}
	switch {
	case !known:
		problems = append(problems, fmt.Sprintf("excerpt status %q", record.Status))
	case (record.Status == string(authExcerptComplete) || record.Status == string(authExcerptClipped)) &&
		(record.RetainedBytes != len(record.Text) || record.Text == "" || record.Daemon == ""):
		problems = append(problems, "retained excerpt disagrees with its text or lacks its binding")
	case record.Status != string(authExcerptComplete) && record.Status != string(authExcerptClipped) && (record.Text != "" || record.RetainedBytes != 0):
		problems = append(problems, "excerpt without retained status holds text")
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}
