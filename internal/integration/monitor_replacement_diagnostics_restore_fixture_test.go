//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

// This adapter calls the maintained fake producer by default. Faults are
// injected only at the selected daemon's real config get/set boundary; the
// recorder, ownership check, readback and finish reporting remain unchanged.
type monRestoreProbe struct {
	*fakeMonCluster
	calls []string
	get   func(context.Context) ([]byte, error, bool)
	set   func(context.Context, string) ([]byte, error, bool)
}

func (p *monRestoreProbe) AdminSocket(ctx context.Context, daemon string, args ...string) ([]byte, error) {
	if daemon == "osd.0" && len(args) >= 3 && args[0] == "config" && args[2] == "debug_monc" {
		p.calls = append(p.calls, strings.Join(args, " "))
		if args[1] == "get" && p.get != nil {
			if data, err, handled := p.get(ctx); handled {
				return data, err
			}
		}
		if args[1] == "set" && len(args) == 4 && p.set != nil {
			if data, err, handled := p.set(ctx, args[3]); handled {
				return data, err
			}
		}
	}
	return p.fakeMonCluster.AdminSocket(ctx, daemon, args...)
}
func monRestoreWindow(t *testing.T, r *monReplacementRecorder) monReplacementDebug {
	t.Helper()
	for _, window := range r.Debug {
		if window.Daemon == "osd.0" {
			return window
		}
	}
	t.Fatal("missing owned OSD debug window")
	return monReplacementDebug{}
}

func TestMonReplacementDebugRestoreOwnership(t *testing.T) {
	for _, tc := range []struct {
		name, outside, wantAction, wantResult string
		wantWrite                             bool
	}{
		{name: "owned window", wantAction: "restored and read back", wantResult: "ok", wantWrite: true},
		{name: "already prior", outside: "0/10", wantAction: "already prior; no write", wantResult: "ok"},
		{name: "outside edit", outside: "20/20", wantResult: "conflict: current debug_monc differs from window"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeMonCluster()
			p := &monRestoreProbe{fakeMonCluster: f}
			r := newMonReplacementRecorder("synthetic", p, fakeMonReplacementBounds)
			r.begin(t.Context())
			p.calls = nil
			if tc.outside != "" {
				f.debug["osd.0"] = tc.outside
			}
			r.restoreDebug(t.Context())
			window := monRestoreWindow(t, r)
			if window.Restored != tc.wantResult || window.RestoreAction != tc.wantAction || window.RestoreAttempted != tc.wantWrite || window.Requested != monReplacementDebugLevel {
				t.Fatalf("window %+v", window)
			}
			wantCalls := []string{"config get debug_monc"}
			if tc.wantWrite {
				wantCalls = append(wantCalls, "config set debug_monc 0/10", "config get debug_monc")
			}
			if !slices.Equal(p.calls, wantCalls) {
				t.Fatalf("calls %v want %v", p.calls, wantCalls)
			}
			want := tc.outside
			if want == "" {
				want = "0/10"
			}
			if f.debug["osd.0"] != want {
				t.Fatalf("outside setting overwritten: %v", f.debug)
			}
			before := slices.Clone(p.calls)
			reporter := &fakeReporter{}
			finishMonReplacement(reporter, r)
			if !slices.Equal(before, p.calls) {
				t.Fatal("cleanup replayed a completed restore decision")
			}
			if tc.outside == "20/20" && (len(reporter.entries) != 1 || !strings.Contains(reporter.entries[0], "debug_monc_restored=1/2") || !strings.Contains(reporter.entries[0], "osd.0:conflict:")) {
				t.Fatalf("conflict hidden by finish: %v", reporter.entries)
			}
		})
	}
}

func TestMonReplacementDebugRestoreFaults(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		prepare                    func(*fakeMonCluster, *monRestoreProbe)
		prefix                     string
		wantWrite, stopUnconfirmed bool
	}{
		{name: "read unavailable", prepare: func(_ *fakeMonCluster, p *monRestoreProbe) {
			p.get = func(context.Context) ([]byte, error, bool) { return nil, errors.New("socket unavailable"), true }
		}, prefix: "unconfirmed before restore:"},
		{name: "paused current read", prepare: func(f *fakeMonCluster, _ *monRestoreProbe) { f.paused["osd.0"] = true }, prefix: "unconfirmed before restore:"},
		{name: "restore refused", prepare: func(_ *fakeMonCluster, p *monRestoreProbe) {
			p.set = func(context.Context, string) ([]byte, error, bool) { return nil, errors.New("restore refused"), true }
		}, prefix: "failed:", wantWrite: true},
		{name: "restore stop unconfirmed", prepare: func(_ *fakeMonCluster, p *monRestoreProbe) {
			p.set = func(context.Context, string) ([]byte, error, bool) {
				return nil, &supervised.NotStoppedError{Marker: "synthetic-command", Err: errors.New("cancel failed")}, true
			}
		}, prefix: "failed:", wantWrite: true, stopUnconfirmed: true},
		{name: "current read stop unconfirmed", prepare: func(_ *fakeMonCluster, p *monRestoreProbe) {
			p.get = func(context.Context) ([]byte, error, bool) {
				return nil, &supervised.NotStoppedError{Marker: "synthetic-read", Err: errors.New("cancel failed")}, true
			}
		}, prefix: "unconfirmed before restore:", stopUnconfirmed: true},
		{name: "readback unavailable", prepare: func(_ *fakeMonCluster, p *monRestoreProbe) {
			n := 0
			p.get = func(context.Context) ([]byte, error, bool) {
				n++
				if n == 2 {
					return nil, errors.New("readback unavailable"), true
				}
				return nil, nil, false
			}
		}, prefix: "unconfirmed:", wantWrite: true},
		{name: "readback mismatch", prepare: func(_ *fakeMonCluster, p *monRestoreProbe) {
			n := 0
			p.get = func(context.Context) ([]byte, error, bool) {
				n++
				if n == 2 {
					return []byte(`{"debug_monc":"20/20"}`), nil, true
				}
				return nil, nil, false
			}
		}, prefix: "mismatch:", wantWrite: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeMonCluster()
			p := &monRestoreProbe{fakeMonCluster: f}
			r := newMonReplacementRecorder("synthetic", p, fakeMonReplacementBounds)
			r.begin(t.Context())
			p.calls = nil
			tc.prepare(f, p)
			expired, cancel := context.WithCancel(t.Context())
			cancel()
			started := time.Now()
			r.restoreDebug(expired)
			window := monRestoreWindow(t, r)
			if !strings.HasPrefix(window.Restored, tc.prefix) || window.RestoreAttempted != tc.wantWrite || window.RestoreStopUnconfirmed != tc.stopUnconfirmed || window.RestoreAction != "" || time.Since(started) > time.Second {
				t.Fatalf("fault window %+v calls=%v", window, p.calls)
			}
			sets := 0
			for _, call := range p.calls {
				if strings.HasPrefix(call, "config set ") {
					sets++
				}
			}
			if sets != map[bool]int{false: 0, true: 1}[tc.wantWrite] {
				t.Fatalf("mutation count %v", p.calls)
			}
			before := slices.Clone(p.calls)
			r.restoreDebug(t.Context())
			if !slices.Equal(before, p.calls) {
				t.Fatal("uncertain restore was retried")
			}
		})
	}
}

func TestMonReplacementDebugUncertainApply(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(map[bool]string{false: "not applied", true: "applied without acknowledgement"}[applied], func(t *testing.T) {
			f := newFakeMonCluster()
			p := &monRestoreProbe{fakeMonCluster: f}
			p.set = func(ctx context.Context, value string) ([]byte, error, bool) {
				if value != monReplacementDebugLevel {
					return nil, nil, false
				}
				if applied {
					if _, err := f.AdminSocket(ctx, "osd.0", "config", "set", "debug_monc", value); err != nil {
						t.Fatal(err)
					}
				}
				return nil, errors.New("apply acknowledgement unavailable"), true
			}
			r := newMonReplacementRecorder("synthetic", p, fakeMonReplacementBounds)
			r.begin(t.Context())
			before := monRestoreWindow(t, r)
			if !before.Set || before.Prior != "0/10" || before.Error == "" || before.ApplyStopUnconfirmed {
				t.Fatalf("uncertain application discarded %+v", before)
			}
			p.calls = nil
			r.restoreDebug(t.Context())
			after := monRestoreWindow(t, r)
			if after.Restored != "ok" || after.ApplyStopUnconfirmed || after.Error != before.Error || after.RestoreAttempted != applied || f.debug["osd.0"] != "0/10" {
				t.Fatalf("uncertain apply lost: %+v", after)
			}
			want := []string{"config get debug_monc"}
			if applied {
				want = append(want, "config set debug_monc 0/10", "config get debug_monc")
			}
			if !slices.Equal(p.calls, want) {
				t.Fatalf("apply/restore replay: %v", p.calls)
			}
		})
	}
}

// A command whose stop is unconfirmed is different from a lost result after
// a stopped command: it may still apply after any read or attempted restore.
func TestMonReplacementDebugApplyStopUnconfirmed(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(map[bool]string{false: "prior observed", true: "requested observed"}[applied], func(t *testing.T) {
			f := newFakeMonCluster()
			p := &monRestoreProbe{fakeMonCluster: f}
			p.set = func(ctx context.Context, value string) ([]byte, error, bool) {
				if value != monReplacementDebugLevel {
					return nil, nil, false
				}
				if applied {
					if _, err := f.AdminSocket(ctx, "osd.0", "config", "set", "debug_monc", value); err != nil {
						t.Fatal(err)
					}
				}
				return nil, &supervised.NotStoppedError{Marker: "synthetic-apply", Err: errors.New("apply stop unconfirmed")}, true
			}
			r := newMonReplacementRecorder("synthetic", p, fakeMonReplacementBounds)
			r.begin(t.Context())
			p.calls = nil
			r.restoreDebug(t.Context())
			w := monRestoreWindow(t, r)
			if !w.Set || !w.ApplyStopUnconfirmed || w.RestoreAttempted || w.RestoreAction != "" || w.Restored != "unconfirmed: apply command stop not confirmed" || !slices.Equal(p.calls, []string{"config get debug_monc"}) {
				t.Fatalf("unconfirmed apply treated as stopped: %+v calls=%v", w, p.calls)
			}
		})
	}
}
