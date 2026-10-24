package ceph

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

var ownershipWarning = daemonLogLine("cluster", "WRN", "Health check failed: 1 osds down (OSD_DOWN)")

func ownershipFixture() *verifyFixture {
	fx := newVerifyFixture()
	fx.config.Logs = []LogExpectation{{Name: "only-known-warning", Required: true, Event: LogClusterWarning, Pattern: `OSD_DOWN`}}
	fx.config.Crashes = []CrashExpectation{{Name: "known-crash", Daemon: "osd.0"}}
	return fx
}

// Editing a previously returned report's policy must not change the policy
// that a later evaluation of the same verification reports.
func TestVerificationReportOwnsPolicy(t *testing.T) {
	fx := ownershipFixture()
	v := fx.activate(t)
	fx.line("mon", 2*time.Minute, ownershipWarning)
	first := fx.evaluate(t, v)
	if first.Outcome != VerificationClean || first.Logs.Findings[0].Expectation != "only-known-warning" {
		t.Fatalf("control: %s %+v", first.Outcome, first.Logs)
	}
	first.LogExpectations[0].Name, first.LogExpectations[0].Pattern = "caller-edited-name", `cannot-match`
	first.CrashExpectations[0].Name, first.CrashExpectations[0].Daemon = "caller-edited-crash", "mon.a"
	first.Logs.Findings[0].Expectation = "edited"

	second := fx.evaluate(t, v)
	if second.Outcome != VerificationClean || second.Logs.Findings[0].Expectation != "only-known-warning" {
		t.Fatalf("matching changed: %s %+v", second.Outcome, second.Logs)
	}
	if got := second.LogExpectations[0]; got.Name != "only-known-warning" || got.Pattern != `OSD_DOWN` {
		t.Fatalf("effective log policy %+v", got)
	}
	if got := second.CrashExpectations[0]; got.Name != "known-crash" || got.Daemon != "osd.0" {
		t.Fatalf("effective crash policy %+v", got)
	}
	// The reports are separate values and the policy stays what was passed.
	if &first.LogExpectations[0] == &second.LogExpectations[0] || &first.CrashExpectations[0] == &second.CrashExpectations[0] {
		t.Fatal("two reports share expectation storage")
	}
}

// Editing the configuration slices after activation is not a way to change
// the policy either: the verification keeps its own copy.
func TestVerificationOwnsConfiguredPolicy(t *testing.T) {
	fx := ownershipFixture()
	logs, crashes := fx.config.Logs, fx.config.Crashes
	v := fx.activate(t)
	logs[0].Name, logs[0].Pattern = "config-edited", `cannot-match`
	crashes[0].Name = "config-edited-crash"
	fx.line("mon", 2*time.Minute, ownershipWarning)
	report := fx.evaluate(t, v)
	if report.Outcome != VerificationClean || report.LogExpectations[0].Name != "only-known-warning" ||
		report.LogExpectations[0].Pattern != `OSD_DOWN` || report.CrashExpectations[0].Name != "known-crash" {
		t.Fatalf("policy followed the caller's slices: %s %+v %+v", report.Outcome, report.LogExpectations, report.CrashExpectations)
	}
}

// Editing a report's notes must reach neither a later report of the same
// verification nor a separately created verifier.
func TestVerificationReportOwnsNotes(t *testing.T) {
	fx := ownershipFixture()
	v := fx.activate(t)
	first := fx.evaluate(t, v)
	original := slices.Clone(first.Notes)
	if len(original) == 0 {
		t.Fatal("no notes")
	}
	first.Notes[0] = "caller-supplied scope note"
	first.Notes = append(first.Notes[:1], "appended")

	other := ownershipFixture()
	second := other.evaluate(t, other.activate(t))
	if !slices.Equal(second.Notes, original) {
		t.Fatalf("a separate verifier inherited edited notes: %q", second.Notes)
	}
	if again := fx.evaluate(t, v); !slices.Equal(again.Notes, original) {
		t.Fatalf("a later report inherited edited notes: %q", again.Notes)
	}
	if !slices.Equal(verificationNotes, original) {
		t.Fatalf("the shared notes changed: %q", verificationNotes)
	}
}

// The serialized report keeps its shape: empty policy is still an empty
// array and the policy and notes are what was configured.
func TestVerificationReportJSONShapeUnchangedByOwnership(t *testing.T) {
	fx := newVerifyFixture()
	report := fx.evaluate(t, fx.activate(t))
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Logs   []json.RawMessage `json:"log_expectations"`
		Crash  []json.RawMessage `json:"crash_expectations"`
		Notes  []string          `json:"notes"`
		Schema int               `json:"schema_version"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Logs == nil || decoded.Crash == nil || len(decoded.Logs) != 0 || len(decoded.Crash) != 0 ||
		!slices.Equal(decoded.Notes, verificationNotes) || decoded.Schema != 1 {
		t.Fatalf("shape %s", data)
	}
}

// A report that the caller took with Evaluate and edited does not reach the
// report that the finalization check archives.
func TestFinalizationArchivedVerificationKeepsPolicyAndNotes(t *testing.T) {
	fx, _ := verifyFinalization(t, FinalizationConfig{Archive: true})
	v, err := fx.Verify(t.Context(), VerificationConfig{
		Logs: []LogExpectation{{Name: "never-seen", Required: true, Event: LogClusterWarning, Pattern: `OSD_DOWN`}}})
	if err != nil {
		t.Fatal(err)
	}
	early, err := v.Evaluate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	early.LogExpectations[0].Name, early.LogExpectations[0].Pattern = "caller-edited-name", `cannot-match`
	early.Notes[0] = "caller-supplied scope note"
	fx.run()
	archives := fx.archives(t)
	if len(archives) != 1 {
		t.Fatalf("archives %v errors %v", archives, fx.tb.errors)
	}
	data, err := os.ReadFile(filepath.Join(archives[0], "verification-1-post-test.json"))
	if err != nil {
		t.Fatal(err)
	}
	var report VerificationReport
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if report.Outcome != VerificationUnexpected || report.LogExpectations[0].Name != "never-seen" ||
		report.LogExpectations[0].Pattern != `OSD_DOWN` || !slices.Equal(report.Notes, verificationNotes) {
		t.Fatalf("archived report %s", data)
	}
}
