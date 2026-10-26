//go:build all || integration

package integration_test

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

// authFailureExcerptLimit caps the redacted first-failure excerpt. It is the
// excerpt's own bound, so a clipped excerpt is visible and does not share the
// string-only cap of the diagnostic query helpers.
const authFailureExcerptLimit = 1024

// authFailureStage says which authentication step a daemon log line belongs
// to. It is evidence about where a refusal happened, not about why.
type authFailureStage string

const (
	// authStageMon is a client-side MonClient authentication-method refusal.
	authStageMon authFailureStage = "mon-authentication"
	// authStageAuthorizer is a service daemon's ticket or authorizer
	// verification failure (for example a later MGR authorizer refusal).
	authStageAuthorizer authFailureStage = "service-authorizer"
	// authStageUnknown is any stage that the lines do not establish.
	authStageUnknown authFailureStage = "unknown"
)

// authExcerptBinding names the daemon, process generation and capture that a
// log line came from. A line alone does not say which process printed it, so
// any empty field leaves the line unbound.
type authExcerptBinding struct {
	Daemon     string
	Generation string
	Capture    string
}

func (b authExcerptBinding) complete() bool {
	return b.Daemon != "" && b.Generation != "" && b.Capture != ""
}

// authFailureLine is one selected daemon log line with the binding of the
// capture it was read from. Text is the original line before redaction.
type authFailureLine struct {
	Binding authExcerptBinding
	Text    string
	At      time.Time
	Stage   authFailureStage
}

// authFailureLinePatterns are the pinned Ceph log statements (Tentacle 20.2.4)
// that a refused authentication can leave:
//   - MonConnection::handle_auth_bad_method (both the "server allowed_methods"
//     and the "failed to auth with my available methods" form),
//   - cephx_decode_ticket's "could not decrypt ticket info" and
//     "could not get ... service secret",
//   - cephx_verify_authorizer's "could not get ... service secret".
var authFailureLinePatterns = []struct {
	re    *regexp.Regexp
	stage authFailureStage
}{
	{regexp.MustCompile(`handle_auth_bad_method server allowed_methods \[[0-9, ]*\] and I support \[[0-9, ]*\]`), authStageMon},
	{regexp.MustCompile(`handle_auth_bad_method failed to auth with my available methods: `), authStageMon},
	{regexp.MustCompile(`ceph_decode_ticket could not decrypt ticket info\. error:`), authStageAuthorizer},
	{regexp.MustCompile(`ceph_decode_ticket could not get (?:general )?service secret for service_id=`), authStageAuthorizer},
	{regexp.MustCompile(`verify_authorizer could not get (?:general )?service secret for service `), authStageAuthorizer},
}

// authLogTimestamp matches the leading ISO timestamp of a Ceph log line.
var authLogTimestamp = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3,6}[+-]\d{4}) `)

// authFailureSelection is the result of selecting lines for one failed
// operation window.
type authFailureSelection struct {
	Lines    []authFailureLine
	Rejected []string
}

// selectAuthFailureLines returns the authentication-failure lines of capture
// whose own timestamp lies inside [start, end]. A matching line without a
// parsable timestamp, or outside the window, is rejected and never retained;
// an unbound capture selects nothing, since its lines cannot be attributed.
func selectAuthFailureLines(binding authExcerptBinding, capture []string, start, end time.Time) (authFailureSelection, error) {
	if !binding.complete() {
		return authFailureSelection{}, errors.New("capture has no daemon, generation and capture binding")
	}
	if start.IsZero() || end.IsZero() || end.Before(start) {
		return authFailureSelection{}, errors.New("operation window is missing or inverted")
	}
	var selection authFailureSelection
	for _, line := range capture {
		stage := authStageUnknown
		for _, p := range authFailureLinePatterns {
			if p.re.MatchString(line) {
				stage = p.stage
				break
			}
		}
		if stage == authStageUnknown {
			continue
		}
		match := authLogTimestamp.FindStringSubmatch(line)
		if match == nil {
			selection.Rejected = append(selection.Rejected, "no timestamp: "+line)
			continue
		}
		at, err := time.Parse("2006-01-02T15:04:05.999999-0700", match[1])
		if err != nil {
			selection.Rejected = append(selection.Rejected, "unparsable timestamp: "+line)
			continue
		}
		if at.Before(start) || at.After(end) {
			selection.Rejected = append(selection.Rejected, "outside operation window: "+line)
			continue
		}
		selection.Lines = append(selection.Lines, authFailureLine{Binding: binding, Text: line, At: at, Stage: stage})
	}
	return selection, nil
}

// authExcerptStatus is the completeness record of one retained excerpt. It is
// separate from whether the original command's stderr was read completely.
type authExcerptStatus string

const (
	authExcerptComplete    authExcerptStatus = "complete"
	authExcerptClipped     authExcerptStatus = "clipped"
	authExcerptNoLines     authExcerptStatus = "no-lines"
	authExcerptWithheld    authExcerptStatus = "redaction-unavailable"
	authExcerptUnavailable authExcerptStatus = "unavailable"
)

// authFailureExcerpt is the redacted, bounded excerpt of one failed
// operation. Text is empty unless Status is complete or clipped.
type authFailureExcerpt struct {
	Operation     string
	Binding       authExcerptBinding
	Start, End    time.Time
	Status        authExcerptStatus
	Text          string
	RetainedBytes int
	Lines         []authFailureLine
	Stage         authFailureStage
}

// authBoundText clips text to at most limit bytes at a UTF-8 boundary and
// reports whether it clipped.
func authBoundText(text string, limit int) (string, bool) {
	if len(text) <= limit {
		return text, false
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut], true
}

// buildAuthFailureExcerpt joins the selected lines, redacts the joined text
// and only then clips it, so a secret spanning the boundary cannot survive as
// an unmatched fragment. A nil redactor or a redaction error withholds the
// text. Zero selected lines is "no-lines", never an empty complete excerpt.
func buildAuthFailureExcerpt(operation string, selection authFailureSelection, start, end time.Time, redact func(string) (string, error), limit int) authFailureExcerpt {
	excerpt := authFailureExcerpt{Operation: operation, Start: start, End: end, Stage: authStageUnknown}
	if operation == "" || limit <= 0 {
		excerpt.Status = authExcerptUnavailable
		return excerpt
	}
	if len(selection.Lines) == 0 {
		excerpt.Status = authExcerptNoLines
		return excerpt
	}
	excerpt.Binding = selection.Lines[0].Binding
	excerpt.Stage = selection.Lines[0].Stage
	texts := make([]string, 0, len(selection.Lines))
	for _, line := range selection.Lines {
		if line.Binding != excerpt.Binding {
			excerpt.Binding = authExcerptBinding{}
			excerpt.Status = authExcerptUnavailable
			excerpt.Stage = authStageUnknown
			return excerpt
		}
		if line.Stage != excerpt.Stage {
			excerpt.Stage = authStageUnknown
		}
		texts = append(texts, line.Text)
	}
	excerpt.Lines = slices.Clone(selection.Lines)
	if redact == nil {
		excerpt.Status = authExcerptWithheld
		return excerpt
	}
	redacted, err := redact(strings.Join(texts, "\n"))
	if err != nil {
		excerpt.Status = authExcerptWithheld
		return excerpt
	}
	text, clipped := authBoundText(redacted, limit)
	excerpt.Text, excerpt.RetainedBytes = text, len(text)
	excerpt.Status = authExcerptComplete
	if clipped {
		excerpt.Status = authExcerptClipped
	}
	return excerpt
}

// supportsStage reports whether the excerpt can supply stage evidence for
// the expected daemon and generation. Withheld, empty, clipped or
// differently bound excerpts supply none, and the stage is then unknown. The
// cause is never derived here: equal allowed_methods lists, a stage and a
// configured fault do not establish a mechanism mismatch or a stale ticket.
func (e authFailureExcerpt) supportsStage(expected authExcerptBinding) (authFailureStage, error) {
	switch {
	case e.Status != authExcerptComplete:
		return authStageUnknown, fmt.Errorf("excerpt is %s", e.Status)
	case !expected.complete() || e.Binding != expected:
		return authStageUnknown, errors.New("excerpt is not bound to the expected daemon, generation and capture")
	case e.Stage == authStageUnknown:
		return authStageUnknown, errors.New("lines do not agree on a stage")
	}
	return e.Stage, nil
}

// authExcerptLedger keeps the first excerpt recorded for each operation
// immutable. Later excerpts for the same operation (a retry, a control) are
// kept apart and never overwrite or promote the first.
type authExcerptLedger struct {
	first map[string]authFailureExcerpt
	later map[string][]authFailureExcerpt
}

func newAuthExcerptLedger() *authExcerptLedger {
	return &authExcerptLedger{first: map[string]authFailureExcerpt{}, later: map[string][]authFailureExcerpt{}}
}

func cloneAuthExcerpt(e authFailureExcerpt) authFailureExcerpt {
	e.Lines = slices.Clone(e.Lines)
	return e
}

// record stores e as the first excerpt of its operation, or as a later one.
// It reports whether e became the first. An empty operation is rejected.
func (l *authExcerptLedger) record(e authFailureExcerpt) (bool, error) {
	if e.Operation == "" {
		return false, errors.New("excerpt has no operation identity")
	}
	if _, ok := l.first[e.Operation]; ok {
		l.later[e.Operation] = append(l.later[e.Operation], cloneAuthExcerpt(e))
		return false, nil
	}
	l.first[e.Operation] = cloneAuthExcerpt(e)
	return true, nil
}

// First returns a copy of the first excerpt of operation.
func (l *authExcerptLedger) First(operation string) (authFailureExcerpt, bool) {
	e, ok := l.first[operation]
	return cloneAuthExcerpt(e), ok
}

// Later returns the number of excerpts recorded after the first.
func (l *authExcerptLedger) Later(operation string) int { return len(l.later[operation]) }
