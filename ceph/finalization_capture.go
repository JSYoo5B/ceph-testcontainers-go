package ceph

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"
)

// CaptureSettler is a running packet capture that a Finalization settles. The
// capture package's Session implements it; the interface lives here because
// that package imports this one.
type CaptureSettler interface {
	// SettlementAllowance is the longest Settle runs, independent of the
	// context it is given. The finalization admits exactly this much for the
	// capture; it is not taken from the checks' budget.
	SettlementAllowance() time.Duration
	// Settle ends the capture, delivers its file and removes its helper. It
	// must not abandon work that is still running when its context ends, and
	// it returns the report, which must be JSON-marshalable and hold no host
	// path, also with an error. Settling twice returns the first report.
	Settle(ctx context.Context) (report any, err error)
}

type finalizationCapture struct {
	name    string
	session CaptureSettler
}

// finalizationCaptureReport is a settled capture's report, kept for the
// archive.
type finalizationCaptureReport struct {
	// sequence is the capture's registration order, from one.
	sequence int
	name     string
	report   any
}

// Capture registers a packet capture that the finalization settles after the
// checks and before the failure archive, so that a check or a failed test
// body that never called Stop still ends the capture before the archive and
// before any Cleanup removes a caller-owned target.
//
// The settlement is a phase of its own. It does not use CheckTimeout or the
// expired test context: each capture gets a fresh context bounded by its
// SettlementAllowance, and the finalization waits for the settlement to
// return instead of abandoning it, because the capture's target must not be
// removed underneath a running settlement. An explicit Stop that already ran
// is not repeated; its report is the one kept. Settlement errors fail the
// test, and the report is kept for the archive even then. Nil sessions and
// registration after finalization started fail the test instead.
func (f *Finalization) Capture(name string, session CaptureSettler) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if isNilSession(session) {
		f.t.Errorf("ceph finalization: Capture %q has no session", name)
		return
	}
	if f.started {
		f.t.Errorf("ceph finalization: Capture %q was added after finalization started", name)
		return
	}
	f.captures = append(f.captures, finalizationCapture{name: name, session: session})
}

// settleCaptures runs after the checks. Every capture is settled even when a
// check failed or an earlier settlement did.
func (f *Finalization) settleCaptures(run *finalizationRun, captures []finalizationCapture) {
	for i, capture := range captures {
		allowance, allowanceErr := safeAllowance(capture.session)
		ctx, cancel := context.WithTimeout(context.Background(), allowance)
		var (
			mu       sync.Mutex
			report   any
			reported bool
		)
		outcome := f.runStep(ctx, finalizationStep{name: capture.name, run: func(ctx context.Context) error {
			if allowanceErr != nil {
				return allowanceErr
			}
			r, err := capture.session.Settle(ctx)
			mu.Lock()
			report, reported = f.redactReport(r), r != nil
			mu.Unlock()
			return err
		}})
		cancel()
		mu.Lock()
		if reported {
			f.mu.Lock()
			f.captureReports = append(f.captureReports, finalizationCaptureReport{sequence: i + 1, name: capture.name, report: report})
			f.mu.Unlock()
		}
		run.manifest.Captures = append(run.manifest.Captures, finalizationCaptureOutcome{
			finalizationOutcome: outcome, Allowance: allowance.String(), Reported: reported})
		mu.Unlock()
		if outcome.Error != "" {
			run.manifest.Trigger.CaptureFailed = true
			f.t.Errorf("ceph finalization capture %s: %s", capture.name, outcome.Error)
		}
	}
}

// finalizationCaptureOutcome is one capture settlement.
type finalizationCaptureOutcome struct {
	finalizationOutcome
	// Allowance is the settlement's admitted bound.
	Allowance string `json:"allowance"`
	// Reported means that the settlement returned a report, kept as
	// capture-<n>-<name>.json in the archive.
	Reported bool `json:"reported"`
}

func captureReportFile(sequence int, name string) string {
	return fmt.Sprintf("capture-%d-%s.json", sequence, artifactName(name))
}

// isNilSession reports a nil interface and an interface that holds a nil
// pointer, map, slice, function or channel, which would panic on first use.
func isNilSession(session CaptureSettler) bool {
	if session == nil {
		return true
	}
	switch v := reflect.ValueOf(session); v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface:
		return v.IsNil()
	}
	return false
}

// safeAllowance reads the allowance without letting a misbehaving session end
// the settlement of the captures after it.
func safeAllowance(session CaptureSettler) (allowance time.Duration, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic: %v", recovered)
		}
	}()
	return session.SettlementAllowance(), nil
}

// redactReport applies the finalization's redaction policy to every string
// value of a settled report before the archive keeps it. It changes only the
// archived metadata, never a capture file. A report that cannot be
// round-tripped through JSON is returned unchanged, and the archive's write
// then records the marshaling error.
func (f *Finalization) redactReport(report any) any {
	if report == nil {
		return nil
	}
	data, err := json.Marshal(report)
	if err != nil {
		return report
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var generic any
	if err := decoder.Decode(&generic); err != nil {
		return report
	}
	redactor := f.redactor()
	var walk func(any) any
	walk = func(value any) any {
		switch v := value.(type) {
		case string:
			return redactor(v)
		case []any:
			for i := range v {
				v[i] = walk(v[i])
			}
		case map[string]any:
			for key, item := range v {
				v[key] = walk(item)
			}
		}
		return value
	}
	return walk(generic)
}
