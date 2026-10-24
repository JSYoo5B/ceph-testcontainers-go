package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeSettler is a capture whose settlement records when it ran.
type fakeSettler struct {
	fx        *finalizationFixture
	name      string
	allowance time.Duration
	delay     time.Duration
	report    any
	err       error
	panicWith any
	calls     int
	ctxLeft   time.Duration
	ctxErr    error
}

func (s *fakeSettler) SettlementAllowance() time.Duration { return s.allowance }

func (s *fakeSettler) Settle(ctx context.Context) (any, error) {
	s.fx.event("settle-start-" + s.name)
	s.calls++
	if deadline, ok := ctx.Deadline(); ok {
		s.ctxLeft = time.Until(deadline)
	}
	s.ctxErr = ctx.Err()
	if s.panicWith != nil {
		panic(s.panicWith)
	}
	// A settlement ignores its context's end, as capture.Session does.
	time.Sleep(s.delay)
	s.fx.event("settle-end-" + s.name)
	return s.report, s.err
}

func (fx *finalizationFixture) settler(name string, report any, err error) *fakeSettler {
	return &fakeSettler{fx: fx, name: name, allowance: 90 * time.Second, report: report, err: err}
}

func indexOf(events []string, name string) int {
	for i, event := range events {
		if event == name {
			return i
		}
	}
	return -1
}

func TestFinalizationSettlesCaptureBeforeArchiveAndCallerOwnedRemoval(t *testing.T) {
	fx := newFinalizationFixture(t, FinalizationConfig{Archive: true, CheckTimeout: time.Millisecond})
	s := fx.settler("traffic", map[string]any{"end": "stopped", "output": "traffic.pcap"}, nil)
	s.delay = 50 * time.Millisecond
	fx.Capture("traffic", s)
	// The test body failed and never called Stop. The check budget is
	// already spent when the capture is settled.
	fx.tb.Errorf("workload failed")
	fx.Check("slow", func(ctx context.Context) error { <-ctx.Done(); return nil })
	fx.Cleanup("remove target", func(context.Context) error { fx.event("remove-target"); return nil })
	fx.run()
	events := fx.events
	settled, diagnostics, removal := indexOf(events, "settle-end-traffic"), indexOf(events, "diagnostics"), indexOf(events, "remove-target")
	if settled < 0 || settled > diagnostics || diagnostics > removal {
		t.Fatalf("order = %v, want the settlement finished before the archive and the target removal", events)
	}
	if s.calls != 1 {
		t.Errorf("settled %d times", s.calls)
	}
	// The expired check budget is not the capture's: it got a fresh context
	// carrying its own admitted allowance.
	if s.ctxErr != nil || s.ctxLeft < 80*time.Second || s.ctxLeft > 90*time.Second {
		t.Errorf("settlement context: err %v, %v left; want a live context bounded by the 90s allowance", s.ctxErr, s.ctxLeft)
	}
	archives := fx.archives(t)
	if len(archives) != 1 {
		t.Fatalf("archives = %v", archives)
	}
	data, err := os.ReadFile(filepath.Join(archives[0], "capture-1-traffic.json"))
	if err != nil {
		t.Fatal(err)
	}
	var kept map[string]any
	if err := json.Unmarshal(data, &kept); err != nil || kept["end"] != "stopped" {
		t.Fatalf("kept report = %s: %v", data, err)
	}
	manifest := readManifest(t, archives[0])
	if len(manifest.Captures) != 1 || !manifest.Captures[0].Reported || manifest.Captures[0].Allowance != "1m30s" || manifest.Captures[0].Error != "" || manifest.Trigger.CaptureFailed {
		t.Errorf("captures = %+v, trigger = %+v", manifest.Captures, manifest.Trigger)
	}
	if !manifest.Trigger.TestFailed || !manifest.Archive.Complete {
		t.Errorf("trigger %+v, archive %+v", manifest.Trigger, manifest.Archive)
	}
	found := false
	for _, file := range manifest.Files {
		found = found || file.Name == "capture-1-traffic.json" && file.Error == "" && file.Bytes == len(data)
	}
	if !found {
		t.Errorf("files = %+v", manifest.Files)
	}
}

func TestFinalizationCaptureSettlementErrorKeepsOriginalFailureAndReport(t *testing.T) {
	fx := newFinalizationFixture(t, FinalizationConfig{Archive: true})
	s := fx.settler("traffic", map[string]any{"end": "killed"}, errors.New("capture failed: killed"))
	fx.Capture("traffic", s)
	fx.tb.Errorf("workload assertion failed")
	fx.run()
	if len(fx.tb.errors) != 2 || fx.tb.errors[0] != "workload assertion failed" || !strings.Contains(fx.tb.errors[1], "capture traffic: capture failed: killed") {
		t.Fatalf("errors = %q, want the original failure and the settlement error", fx.tb.errors)
	}
	archives := fx.archives(t)
	if len(archives) != 1 {
		t.Fatalf("archives = %v", archives)
	}
	if _, err := os.Stat(filepath.Join(archives[0], "capture-1-traffic.json")); err != nil {
		t.Errorf("the report of a failed settlement was not kept: %v", err)
	}
	m := readManifest(t, archives[0])
	if !m.Trigger.CaptureFailed || !m.Trigger.TestFailed || len(m.Captures) != 1 || !strings.Contains(m.Captures[0].Error, "killed") || !m.Captures[0].Reported {
		t.Errorf("manifest = %+v", m)
	}
}

func TestFinalizationCaptureOnlyFailureTriggersArchive(t *testing.T) {
	fx := newFinalizationFixture(t, FinalizationConfig{Archive: true})
	fx.Capture("traffic", fx.settler("traffic", map[string]any{"end": "failed"}, errors.New("target removed")))
	fx.run()
	if archives := fx.archives(t); len(archives) != 1 || !fx.tb.failed {
		t.Fatalf("archives = %v, failed = %v", archives, fx.tb.failed)
	}
}

func TestFinalizationPassingRunSettlesCaptureWithoutArchive(t *testing.T) {
	fx := newFinalizationFixture(t, FinalizationConfig{Archive: true})
	s := fx.settler("traffic", map[string]any{"end": "stopped"}, nil)
	fx.Capture("traffic", s)
	fx.run()
	if s.calls != 1 || len(fx.archives(t)) != 0 || fx.tb.failed {
		t.Errorf("calls %d, archives %v, failed %v", s.calls, fx.archives(t), fx.tb.failed)
	}
	if indexOf(fx.events, "diagnostics") >= 0 {
		t.Errorf("events = %v", fx.events)
	}
}

func TestFinalizationWaitsForSlowSettlementAndSettlesTheOthers(t *testing.T) {
	fx := newFinalizationFixture(t, FinalizationConfig{Archive: true})
	// The allowance is far shorter than the settlement: the finalization
	// still waits for it instead of abandoning it before the target removal.
	slow := fx.settler("slow", map[string]any{"end": "stopped"}, nil)
	slow.allowance, slow.delay = time.Millisecond, 60*time.Millisecond
	boom := fx.settler("boom", nil, nil)
	boom.panicWith = "settle exploded"
	last := fx.settler("last", map[string]any{"end": "stopped"}, nil)
	fx.Capture("slow", slow)
	fx.Capture("boom", boom)
	fx.Capture("last", last)
	fx.Cleanup("remove target", func(context.Context) error { fx.event("remove-target"); return nil })
	fx.run()
	e := fx.events
	if !(indexOf(e, "settle-end-slow") >= 0 && indexOf(e, "settle-end-slow") < indexOf(e, "settle-start-boom") &&
		indexOf(e, "settle-start-last") > indexOf(e, "settle-start-boom") && indexOf(e, "settle-end-last") < indexOf(e, "remove-target")) {
		t.Fatalf("order = %v", e)
	}
	if len(fx.tb.errors) != 1 || !strings.Contains(fx.tb.errors[0], "capture boom: panic: settle exploded") {
		t.Errorf("errors = %q", fx.tb.errors)
	}
	archives := fx.archives(t)
	if len(archives) != 1 {
		t.Fatalf("archives = %v", archives)
	}
	m := readManifest(t, archives[0])
	if len(m.Captures) != 3 || m.Captures[1].Reported || !m.Captures[0].Reported || !m.Captures[2].Reported {
		t.Errorf("captures = %+v", m.Captures)
	}
	for _, name := range []string{"capture-1-slow.json", "capture-2-boom.json", "capture-3-last.json"} {
		_, err := os.Stat(filepath.Join(archives[0], name))
		if (name == "capture-2-boom.json") == (err == nil) {
			t.Errorf("%s: stat err %v", name, err)
		}
	}
}

func TestFinalizationCaptureRegistrationErrors(t *testing.T) {
	fx := newFinalizationFixture(t, FinalizationConfig{})
	fx.Capture("none", nil)
	if len(fx.tb.errors) != 1 || !strings.Contains(fx.tb.errors[0], "has no session") {
		t.Errorf("errors = %q", fx.tb.errors)
	}
	fx.run()
	fx.Capture("late", fx.settler("late", nil, nil))
	if len(fx.tb.errors) != 2 || !strings.Contains(fx.tb.errors[1], "added after finalization started") {
		t.Errorf("errors = %q", fx.tb.errors)
	}
	var none *Finalization
	none.Capture("nil finalization", nil)
}

// pointerSettler dereferences its receiver, as capture.Session does.
type pointerSettler struct{ allowance time.Duration }

func (s *pointerSettler) SettlementAllowance() time.Duration { return s.allowance }
func (s *pointerSettler) Settle(context.Context) (any, error) {
	return nil, nil
}

func TestFinalizationRejectsTypedNilCaptureAndSettlesTheNext(t *testing.T) {
	fx := newFinalizationFixture(t, FinalizationConfig{Archive: true})
	var typedNil *pointerSettler
	fx.Capture("typed nil", typedNil)
	if len(fx.tb.errors) != 1 || !strings.Contains(fx.tb.errors[0], "has no session") {
		t.Fatalf("errors = %q, want the typed nil refused at registration", fx.tb.errors)
	}
	good := fx.settler("good", map[string]any{"end": "stopped"}, nil)
	fx.Capture("good", good)
	fx.Cleanup("remove target", func(context.Context) error { fx.event("remove-target"); return nil })
	fx.run()
	if good.calls != 1 || indexOf(fx.events, "settle-end-good") > indexOf(fx.events, "remove-target") {
		t.Errorf("events = %v: a bad registration must not bypass the later capture", fx.events)
	}
	archives := fx.archives(t)
	if len(archives) != 1 {
		t.Fatalf("archives = %v", archives)
	}
	if _, err := os.Stat(filepath.Join(archives[0], "capture-1-good.json")); err != nil {
		t.Error(err)
	}
}

type panickingAllowance struct{ pointerSettler }

func (*panickingAllowance) SettlementAllowance() time.Duration { panic("no allowance") }

func TestFinalizationAllowancePanicDoesNotSkipLaterCaptures(t *testing.T) {
	fx := newFinalizationFixture(t, FinalizationConfig{Archive: true})
	fx.Capture("bad", &panickingAllowance{})
	good := fx.settler("good", map[string]any{"end": "stopped"}, nil)
	fx.Capture("good", good)
	fx.Cleanup("remove target", func(context.Context) error { fx.event("remove-target"); return nil })
	fx.run()
	if good.calls != 1 || indexOf(fx.events, "settle-end-good") > indexOf(fx.events, "remove-target") {
		t.Errorf("events = %v", fx.events)
	}
	if len(fx.tb.errors) != 1 || !strings.Contains(fx.tb.errors[0], "capture bad: panic: no allowance") {
		t.Errorf("errors = %q", fx.tb.errors)
	}
	archives := fx.archives(t)
	if len(archives) != 1 {
		t.Fatalf("archives = %v", archives)
	}
	if m := readManifest(t, archives[0]); len(m.Captures) != 2 || m.Captures[0].Reported || !m.Captures[1].Reported {
		t.Errorf("captures = %+v", m.Captures)
	}
}

func TestFinalizationRedactsArchivedCaptureMetadata(t *testing.T) {
	const opaque, token = "opaque-configured-value-7f3a", "tok3n-abcdef123456"
	bearer := "Authorization: Bearer " + token
	fx := newFinalizationFixture(t, FinalizationConfig{Archive: true,
		Diagnostics: DiagnosticsConfig{RedactValues: []string{opaque}}})
	report := map[string]any{
		"stderr": "tcpdump: warning " + opaque + "\nGET / HTTP/1.1\nHost: example.invalid\n" + bearer,
		"nested": map[string]any{"list": []any{"x " + opaque, 7}},
		"counts": map[string]any{"captured": 12345678901234567},
		"end":    "stopped",
	}
	fx.Capture("traffic", fx.settler("traffic", report, errors.New("capture failed: "+opaque+" "+bearer)))
	fx.run()
	archives := fx.archives(t)
	if len(archives) != 1 {
		t.Fatalf("archives = %v", archives)
	}
	for _, name := range []string{"capture-1-traffic.json", "manifest.json"} {
		data, err := os.ReadFile(filepath.Join(archives[0], name))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), opaque) || strings.Contains(string(data), token) {
			t.Errorf("%s keeps a secret:\n%s", name, data)
		}
		if name != "manifest.json" && (!strings.Contains(string(data), "[REDACTED]") || !strings.Contains(string(data), "12345678901234567") || !strings.Contains(string(data), `"stopped"`)) {
			t.Errorf("%s lost its redaction marker or its other content:\n%s", name, data)
		}
	}
	// The caller's own report value is not rewritten.
	if !strings.Contains(report["stderr"].(string), opaque) {
		t.Error("the settled report was modified in place")
	}
}
