//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// The lines below are synthetic. Their statements are the pinned Ceph source
// log statements (Tentacle 20.2.4: MonConnection::handle_auth_bad_method,
// cephx_decode_ticket, cephx_verify_authorizer); none is a saved native
// observation, and the identifiers are placeholders.
const (
	excerptMonRefusal   = "2026-10-15T13:12:01.600+0000 7f1b8c0a1640 -1 monclient(hunting): handle_auth_bad_method server allowed_methods [2] and I support [2]"
	excerptMonPermLine  = "2026-10-15T13:12:01.610+0000 7f1b8c0a1640 -1 monclient(hunting): handle_auth_bad_method failed to auth with my available methods: (13) Permission denied"
	excerptTicketLine   = "2026-10-15T13:12:01.620+0000 7f1b8c0a1640  0 cephx: ceph_decode_ticket could not decrypt ticket info. error:bad magic"
	excerptSecretLine   = "2026-10-15T13:12:01.625+0000 7f1b8c0a1640  0 cephx: verify_authorizer could not get service secret for service mgr secret_id=2"
	excerptNoise        = "2026-10-15T13:12:01.615+0000 7f1b8c0a1640  1 monclient: unrelated line"
	excerptRetryLine    = "2026-10-15T13:12:09.000+0000 7f1b8c0a1640 -1 monclient(hunting): handle_auth_bad_method server allowed_methods [2] and I support [2]"
	excerptNoTimestamp  = "monclient(hunting): handle_auth_bad_method server allowed_methods [2] and I support [2]"
	excerptBadTimestamp = "2026-13-45T99:12:01.600+0000 7f1b8c0a1640 -1 monclient(hunting): handle_auth_bad_method server allowed_methods [2] and I support [2]"
)

var (
	excerptStart   = time.Date(2026, 10, 15, 13, 12, 1, 500_000_000, time.UTC)
	excerptEnd     = time.Date(2026, 10, 15, 13, 12, 1, 700_000_000, time.UTC)
	excerptBinding = authExcerptBinding{Daemon: "client.admin", Generation: "gen-1", Capture: "capture-1"}
)

func excerptIdentity(text string) (string, error) { return text, nil }

func excerptFor(t *testing.T, binding authExcerptBinding, lines []string, redact func(string) (string, error), limit int) authFailureExcerpt {
	t.Helper()
	selection, err := selectAuthFailureLines(binding, lines, excerptStart, excerptEnd)
	if err != nil {
		t.Fatal(err)
	}
	return buildAuthFailureExcerpt("op-1", selection, excerptStart, excerptEnd, redact, limit)
}

func testAuthFailureExcerptSelection(t *testing.T) {
	capture := []string{excerptNoise, excerptMonRefusal, excerptRetryLine, excerptNoTimestamp, excerptBadTimestamp, excerptMonPermLine}
	selection, err := selectAuthFailureLines(excerptBinding, capture, excerptStart, excerptEnd)
	if err != nil {
		t.Fatal(err)
	}
	if len(selection.Lines) != 2 || selection.Lines[0].Text != excerptMonRefusal || selection.Lines[1].Text != excerptMonPermLine {
		t.Fatalf("selected %+v", selection.Lines)
	}
	for _, line := range selection.Lines {
		if line.Binding != excerptBinding || line.Stage != authStageMon {
			t.Fatalf("line not bound or staged: %+v", line)
		}
	}
	if len(selection.Rejected) != 3 {
		t.Fatalf("rejected %q", selection.Rejected)
	}
	// Boundaries are inclusive on both ends.
	for _, w := range []struct{ start, end time.Time }{
		{excerptStart, time.Date(2026, 10, 15, 13, 12, 1, 600_000_000, time.UTC)},
		{time.Date(2026, 10, 15, 13, 12, 1, 600_000_000, time.UTC), excerptEnd},
	} {
		s, err := selectAuthFailureLines(excerptBinding, []string{excerptMonRefusal}, w.start, w.end)
		if err != nil || len(s.Lines) != 1 {
			t.Fatalf("boundary line lost: %+v %v", s, err)
		}
	}
	// The end alone is not the window: a line before the start is rejected.
	s, err := selectAuthFailureLines(excerptBinding, []string{excerptMonRefusal}, time.Date(2026, 10, 15, 13, 12, 1, 601_000_000, time.UTC), excerptEnd)
	if err != nil || len(s.Lines) != 0 || len(s.Rejected) != 1 {
		t.Fatalf("early line retained: %+v %v", s, err)
	}
	// The ordinary stages are kept apart.
	s, err = selectAuthFailureLines(excerptBinding, []string{excerptTicketLine, excerptSecretLine}, excerptStart, excerptEnd)
	if err != nil || len(s.Lines) != 2 || s.Lines[0].Stage != authStageAuthorizer || s.Lines[1].Stage != authStageAuthorizer {
		t.Fatalf("authorizer lines: %+v %v", s, err)
	}
	// Missing binding fields and a bad window select nothing.
	for _, b := range []authExcerptBinding{{Generation: "g", Capture: "c"}, {Daemon: "d", Capture: "c"}, {Daemon: "d", Generation: "g"}, {}} {
		if _, err := selectAuthFailureLines(b, capture, excerptStart, excerptEnd); err == nil {
			t.Fatalf("unbound capture %+v selected lines", b)
		}
	}
	for _, w := range []struct{ start, end time.Time }{{time.Time{}, excerptEnd}, {excerptStart, time.Time{}}, {excerptEnd, excerptStart}} {
		if _, err := selectAuthFailureLines(excerptBinding, capture, w.start, w.end); err == nil {
			t.Fatalf("bad window %+v accepted", w)
		}
	}
	// Empty and missing captures are no lines, not an excerpt.
	for _, empty := range [][]string{nil, {}, {excerptNoise}} {
		if e := excerptFor(t, excerptBinding, empty, excerptIdentity, authFailureExcerptLimit); e.Status != authExcerptNoLines || e.Text != "" {
			t.Fatalf("empty capture gave %+v", e)
		}
	}
}

func testAuthFailureExcerptRedactionAndClipping(t *testing.T) {
	e := excerptFor(t, excerptBinding, []string{excerptMonRefusal}, excerptIdentity, authFailureExcerptLimit)
	if e.Status != authExcerptComplete || e.Text != excerptMonRefusal || e.RetainedBytes != len(excerptMonRefusal) {
		t.Fatalf("short excerpt: %+v", e)
	}
	// Exactly at the limit is complete; one byte over is clipped.
	exact := len(excerptMonRefusal)
	if e := excerptFor(t, excerptBinding, []string{excerptMonRefusal}, excerptIdentity, exact); e.Status != authExcerptComplete {
		t.Fatalf("exact limit: %+v", e)
	}
	if e := excerptFor(t, excerptBinding, []string{excerptMonRefusal}, excerptIdentity, exact-1); e.Status != authExcerptClipped || e.RetainedBytes != exact-1 {
		t.Fatalf("over limit: %+v", e)
	}
	// Clipping lands on a UTF-8 boundary.
	if text, clipped := authBoundText("abé", 3); text != "ab" || !clipped {
		t.Fatalf("multibyte clip: %q %v", text, clipped)
	}
	if text, clipped := authBoundText("abé", 4); text != "abé" || clipped {
		t.Fatalf("multibyte fit: %q %v", text, clipped)
	}
	// Redaction runs before clipping and may expand the text, which can push
	// the tail past the limit; the expansion is visible as clipping.
	expand := func(text string) (string, error) {
		return strings.ReplaceAll(text, "allowed_methods", "allowed_methods<expanded-redaction-marker>"), nil
	}
	if e := excerptFor(t, excerptBinding, []string{excerptMonRefusal}, expand, exact+5); e.Status != authExcerptClipped {
		t.Fatalf("expansion not visible: %+v", e)
	}
	// A secret value is removed before the clip, so no fragment of it survives
	// at the boundary.
	secretLine := excerptMonRefusal + " token=SYNTHETICSECRETVALUE"
	mask := func(text string) (string, error) {
		return strings.ReplaceAll(text, "SYNTHETICSECRETVALUE", "<redacted>"), nil
	}
	cut := strings.Index(secretLine, "SYNTHETICSECRETVALUE") + 5
	if e := excerptFor(t, excerptBinding, []string{secretLine}, mask, cut); strings.Contains(e.Text, "SYNTH") || e.Status != authExcerptClipped {
		t.Fatalf("secret fragment survived: %+v", e)
	}
	// Redaction unavailable withholds the text; it never falls back to raw.
	for name, redact := range map[string]func(string) (string, error){
		"nil":   nil,
		"error": func(string) (string, error) { return "", errors.New("redaction expired") },
	} {
		e := excerptFor(t, excerptBinding, []string{excerptMonRefusal}, redact, authFailureExcerptLimit)
		if e.Status != authExcerptWithheld || e.Text != "" || e.RetainedBytes != 0 {
			t.Fatalf("%s redactor leaked: %+v", name, e)
		}
		if _, err := e.supportsStage(excerptBinding); err == nil {
			t.Fatalf("%s: withheld excerpt supported a stage", name)
		}
	}
	// A missing operation identity or limit is unavailable.
	selection, _ := selectAuthFailureLines(excerptBinding, []string{excerptMonRefusal}, excerptStart, excerptEnd)
	if e := buildAuthFailureExcerpt("", selection, excerptStart, excerptEnd, excerptIdentity, 10); e.Status != authExcerptUnavailable {
		t.Fatalf("no operation: %+v", e)
	}
	if e := buildAuthFailureExcerpt("op", selection, excerptStart, excerptEnd, excerptIdentity, 0); e.Status != authExcerptUnavailable {
		t.Fatalf("no limit: %+v", e)
	}
	// Lines of two bindings never merge into one excerpt.
	other := authExcerptBinding{Daemon: "client.admin", Generation: "gen-2", Capture: "capture-1"}
	mixed := authFailureSelection{Lines: append(selection.Lines, authFailureLine{Binding: other, Text: excerptMonPermLine, Stage: authStageMon})}
	if e := buildAuthFailureExcerpt("op", mixed, excerptStart, excerptEnd, excerptIdentity, authFailureExcerptLimit); e.Status != authExcerptUnavailable || e.Text != "" {
		t.Fatalf("mixed bindings merged: %+v", e)
	}
}

func testAuthFailureExcerptStage(t *testing.T) {
	mon := excerptFor(t, excerptBinding, []string{excerptMonRefusal}, excerptIdentity, authFailureExcerptLimit)
	if stage, err := mon.supportsStage(excerptBinding); err != nil || stage != authStageMon {
		t.Fatalf("mon stage: %q %v", stage, err)
	}
	auth := excerptFor(t, excerptBinding, []string{excerptTicketLine}, excerptIdentity, authFailureExcerptLimit)
	if stage, err := auth.supportsStage(excerptBinding); err != nil || stage != authStageAuthorizer {
		t.Fatalf("authorizer stage: %q %v", stage, err)
	}
	// Lines of different stages do not agree on one.
	both := excerptFor(t, excerptBinding, []string{excerptMonRefusal, excerptTicketLine}, excerptIdentity, authFailureExcerptLimit)
	if stage, err := both.supportsStage(excerptBinding); err == nil || stage != authStageUnknown {
		t.Fatalf("mixed stages gave %q %v", stage, err)
	}
	// A different daemon, generation or capture, or an unbound expectation,
	// supplies no stage.
	for _, expected := range []authExcerptBinding{
		{Daemon: "mgr.a", Generation: "gen-1", Capture: "capture-1"},
		{Daemon: "client.admin", Generation: "gen-2", Capture: "capture-1"},
		{Daemon: "client.admin", Generation: "gen-1", Capture: "capture-2"},
		{},
	} {
		if stage, err := mon.supportsStage(expected); err == nil || stage != authStageUnknown {
			t.Fatalf("expected %+v: %q %v", expected, stage, err)
		}
	}
	// A clipped excerpt is visibly incomplete and supplies no stage.
	clipped := excerptFor(t, excerptBinding, []string{excerptMonRefusal}, excerptIdentity, 10)
	if _, err := clipped.supportsStage(excerptBinding); err == nil {
		t.Fatal("clipped excerpt supported a stage")
	}
	// The same final refusal text with different prefixes stays distinguishable
	// in the retained excerpt: the lines before it are not lost.
	first := excerptFor(t, excerptBinding, []string{excerptMonRefusal, excerptMonPermLine}, excerptIdentity, authFailureExcerptLimit)
	second := excerptFor(t, excerptBinding, []string{excerptMonPermLine}, excerptIdentity, authFailureExcerptLimit)
	if first.Text == second.Text {
		t.Fatal("distinct prefixes collapsed to one retained excerpt")
	}
}

func testAuthFailureExcerptLedger(t *testing.T) {
	ledger := newAuthExcerptLedger()
	first := excerptFor(t, excerptBinding, []string{excerptMonRefusal}, excerptIdentity, authFailureExcerptLimit)
	if ok, err := ledger.record(first); err != nil || !ok {
		t.Fatalf("first record: %v %v", ok, err)
	}
	// A retry whose output differs and an identical duplicate both stay later.
	retrySel, _ := selectAuthFailureLines(excerptBinding, []string{excerptRetryLine}, excerptStart, time.Date(2026, 10, 15, 13, 12, 10, 0, time.UTC))
	retry := buildAuthFailureExcerpt("op-1", retrySel, excerptStart, excerptEnd, excerptIdentity, authFailureExcerptLimit)
	for _, e := range []authFailureExcerpt{retry, first} {
		if ok, err := ledger.record(e); err != nil || ok {
			t.Fatalf("later record became first: %v %v", ok, err)
		}
	}
	got, ok := ledger.First("op-1")
	if !ok || got.Text != excerptMonRefusal || ledger.Later("op-1") != 2 {
		t.Fatalf("first changed: %+v later=%d", got, ledger.Later("op-1"))
	}
	// Returned and stored values are copies.
	got.Lines[0].Text = "mutated"
	first.Lines[0].Text = "mutated"
	again, _ := ledger.First("op-1")
	if again.Lines[0].Text != excerptMonRefusal {
		t.Fatalf("ledger aliases caller data: %+v", again.Lines[0])
	}
	// A different operation has its own first; a missing one is absent.
	other := first
	other.Operation = "op-2"
	if ok, err := ledger.record(other); err != nil || !ok {
		t.Fatalf("second operation: %v %v", ok, err)
	}
	if _, ok := ledger.First("op-3"); ok {
		t.Fatal("unrecorded operation present")
	}
	// An empty operation identity is rejected.
	if _, err := ledger.record(authFailureExcerpt{}); err == nil {
		t.Fatal("excerpt without operation accepted")
	}
	// An unavailable first excerpt is still the first: a later complete one
	// does not replace it.
	l2 := newAuthExcerptLedger()
	empty := excerptFor(t, excerptBinding, nil, excerptIdentity, authFailureExcerptLimit)
	_, _ = l2.record(empty)
	_, _ = l2.record(excerptFor(t, excerptBinding, []string{excerptMonRefusal}, excerptIdentity, authFailureExcerptLimit))
	if f, _ := l2.First("op-1"); f.Status != authExcerptNoLines {
		t.Fatalf("later excerpt rewrote an empty first: %+v", f)
	}
}

func TestAuthFailureExcerptSelection(t *testing.T) { testAuthFailureExcerptSelection(t) }
func TestAuthFailureExcerptRedaction(t *testing.T) { testAuthFailureExcerptRedactionAndClipping(t) }
func TestAuthFailureExcerptStage(t *testing.T)     { testAuthFailureExcerptStage(t) }
func TestAuthFailureExcerptLedger(t *testing.T)    { testAuthFailureExcerptLedger(t) }
