package override

import (
	"context"
	"errors"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

func newConfigFixture(ctr *configFixture) *ceph.Container {
	return configCluster(ctr, ceph.MessengerDefault)
}

func TestSetConfigRestoresAbsenceAndKeepsMaskedInheritedValues(t *testing.T) {
	ctr := &configFixture{entries: []ceph.ConfigEntry{{Section: "global", Name: "osd_max_backfills", Value: "3"}, {Section: "osd", Mask: "class:ssd", Name: "osd_max_backfills", Value: "4"}}}
	c := newConfigFixture(ctr)
	change, err := SetConfig(t.Context(), c, ceph.ConfigSetting{Section: "osd", Name: "osd-max-backfills", Value: "2"})
	if err != nil {
		t.Fatal(err)
	}
	copy := *change
	if again, err := SetConfig(t.Context(), c, ceph.ConfigSetting{Section: "osd", Name: "osd_max_backfills", Value: "5"}); err == nil || again == nil || again.state.refused == nil {
		t.Fatal("overlapping override accepted")
	}
	if err := copy.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(ctr.entries) != 2 || ctr.entries[0].Value != "3" || ctr.entries[1].Value != "4" || len(heldConfig(c)) != 0 {
		t.Fatal("restoration changed inherited/masked state")
	}
}

func TestSetConfigUsesNativeNormalizedValueAndExistingEntry(t *testing.T) {
	ctr := &configFixture{entries: []ceph.ConfigEntry{{Section: "osd", Mask: "class:ssd", Name: "osd_recovery_sleep", Value: "0"}}, normalized: "1"}
	c := newConfigFixture(ctr)
	change, err := SetConfig(t.Context(), c, ceph.ConfigSetting{Section: "osd", Mask: "class:ssd", Name: "osd_recovery_sleep", Value: "1.000"})
	if err != nil {
		t.Fatal(err)
	}
	ctr.normalized = ""
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(ctr.entries) != 1 || ctr.entries[0].Value != "0" {
		t.Fatal("native canonical value wasn't restored")
	}
}

func TestSetConfigPreservesModuleOptionsAndStoredStrings(t *testing.T) {
	module := ceph.ConfigEntry{Section: "mgr", Name: "mgr/volumes/pause_cloning", Value: "false"}
	foreign := ceph.ConfigEntry{Section: "client.example", Name: "key", Value: "stored\nstring"}
	ctr := &configFixture{entries: []ceph.ConfigEntry{module, foreign}}
	c := newConfigFixture(ctr)
	change, err := SetConfig(t.Context(), c, ceph.ConfigSetting{Section: "mgr", Name: module.Name, Value: "true"})
	if err != nil {
		t.Fatal(err)
	}
	other, err := SetConfig(t.Context(), c, ceph.ConfigSetting{Section: "osd", Name: "debug_osd", Value: "2"})
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	servicehook.Lock(c)
	entries, err := overridehook.ConfigEntries(t.Context(), c)
	servicehook.Unlock(c)
	if err != nil || len(entries) != 2 || !slices.Contains(entries, module) || !slices.Contains(entries, foreign) {
		t.Fatal("module setting or arbitrary foreign stored string lost")
	}
}

func TestSetConfigRefusesOutsideEditsAndSanitizesValues(t *testing.T) {
	ctr := &configFixture{}
	c := newConfigFixture(ctr)
	change, err := SetConfig(t.Context(), c, ceph.ConfigSetting{Section: "client.example", Name: "debug_ms", Value: "PRIVATE-VALUE"})
	if err != nil {
		t.Fatal(err)
	}
	for _, format := range []string{"%v", "%+v", "%#v"} {
		for _, v := range []any{change, *change} {
			if strings.Contains(fmt.Sprintf(format, v), "PRIVATE-VALUE") {
				t.Fatal("override formatting leaked value")
			}
		}
	}
	ctr.entries[0].Value = "external"
	before := len(ctr.calls)
	if err := change.Restore(t.Context()); err == nil || len(ctr.calls) != before+1 || ctr.entries[0].Value != "external" {
		t.Fatal("outside edit overwritten")
	}
	ctr.entries[0].Value = "PRIVATE-VALUE"
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestSetConfigLostApplyAndRestoreRepliesConverge(t *testing.T) {
	ctr := &configFixture{failAfter: "set"}
	c := newConfigFixture(ctr)
	change, err := SetConfig(t.Context(), c, ceph.ConfigSetting{Section: "osd", Name: "debug_osd", Value: "2"})
	if change.state.refused != nil || err == nil || strings.Contains(err.Error(), "PRIVATE-ERROR") {
		t.Fatal("lost reply lost handle or leaked values")
	}
	ctr.failAfter = "rm"
	if err := change.Restore(t.Context()); err == nil {
		t.Fatal("lost restore reply hidden")
	}
	ctr.failAfter = ""
	before := len(ctr.calls)
	if err := change.Restore(t.Context()); err != nil || len(ctr.calls) != before+1 || len(heldConfig(c)) != 0 {
		t.Fatal("applied restoration was not reconciled")
	}
}

func TestSetConfigLostNormalizedReadbackRequiresInspection(t *testing.T) {
	ctr := &configFixture{normalized: "7200", failDumpAt: 2}
	c := newConfigFixture(ctr)
	change, err := SetConfig(t.Context(), c, ceph.ConfigSetting{Section: "osd", Name: "osd_scrub_min_interval", Value: "7200.0000"})
	if err == nil || change.state.refused != nil || change.state.known {
		t.Fatal("lost readback didn't preserve an uncertain handle")
	}
	if err := change.Restore(t.Context()); err == nil || !strings.Contains(err.Error(), "readback was unavailable") || ctr.entries[0].Value != "7200" {
		t.Fatal("unknown normalized value adopted for mutation")
	}
	// Explicit outside cleanup can resolve an uncertain lease without another set.
	ctr.entries = nil
	if err := change.Restore(t.Context()); err != nil || len(heldConfig(c)) != 0 {
		t.Fatal("previous native state was not reconciled")
	}
}

func TestSetConfigValidationAndMaskCanonicalization(t *testing.T) {
	for _, setting := range []ceph.ConfigSetting{{Section: "global|mon", Name: "debug_ms"}, {Section: "global.a", Name: "debug_ms"}, {Section: "osd", Name: "--option"}, {Section: "osd", Name: "debug_ms", Value: "bad\nvalue"}, {Section: "osd", Name: "debug_ms", Mask: "host:a/rack:b"}, {Section: "osd", Name: "debug_ms", Mask: "class:ssd/class:hdd"}} {
		ctr := &configFixture{}
		change, err := SetConfig(t.Context(), newConfigFixture(ctr), setting)
		if err == nil || change.state.refused == nil || len(ctr.calls) != 0 {
			t.Fatal("invalid setting reached Ceph")
		}
	}
	setting, err := overridehook.NormalizeConfigSetting(ceph.ConfigSetting{Section: "osd.0", Mask: "class:ssd/host:a", Name: "debug-ms"})
	if err != nil || overridehook.ConfigWho(setting) != "osd.0/host:a/class:ssd" || setting.Name != "debug_ms" {
		t.Fatal("mask/name canonicalization failed")
	}
}

// A refused call returns a handle that owns nothing: Restore returns nil
// without a lock or a native command, and the registry holds no key.
func TestSetConfigRefusalsReturnNotAppliedHandles(t *testing.T) {
	setting := ceph.ConfigSetting{Section: "osd", Name: "debug_osd", Value: "2"}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	ctr := &configFixture{}
	own := newConfigFixture(ctr)
	terminated := testhelper.TestCluster(ctr, testhelper.TestConfig{StartupTimeout: time.Second, Closed: true})
	shared := testhelper.TestCluster(ctr, testhelper.TestConfig{StartupTimeout: time.Second, Shared: true})
	for name, tc := range map[string]struct {
		c       *ceph.Container
		ctx     context.Context
		setting ceph.ConfigSetting
	}{
		"canceled context":   {own, canceled, setting},
		"invalid section":    {own, t.Context(), ceph.ConfigSetting{Section: "bad", Name: "debug_ms"}},
		"terminated cluster": {terminated, t.Context(), setting},
		"shared cluster":     {shared, t.Context(), setting},
	} {
		change, err := SetConfig(tc.ctx, tc.c, tc.setting)
		if err == nil || change == nil || change.state == nil || change.state.refused == nil || len(ctr.calls) != 0 {
			t.Fatalf("%s: handle %v, error %v, calls %v", name, change, err, ctr.calls)
		}
		// Hold the cluster lock: a not-applied Restore must not take it.
		servicehook.Lock(tc.c)
		done := make(chan error, 1)
		go func() { done <- change.Restore(t.Context()) }()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("%s: Restore of a not-applied handle = %v", name, err)
			}
		case <-time.After(time.Second):
			t.Errorf("%s: Restore of a not-applied handle waited for a lock", name)
		}
		servicehook.Unlock(tc.c)
		if len(heldConfig(tc.c)) != 0 {
			t.Errorf("%s: a refused call holds a key", name)
		}
	}
}

func TestSetConfigNilClusterIsInvalidRequestWithOp(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	// A nil cluster is reported before the context and the input.
	change, err := SetConfig(canceled, nil, ceph.ConfigSetting{Section: "bad"})
	var op *ceph.OperationError
	if change == nil || !errors.Is(err, ceph.ErrInvalidRequest) || !errors.As(err, &op) || op.Op != "override.SetConfig" || errors.Is(err, context.Canceled) {
		t.Fatalf("nil cluster: %v %v", change, err)
	}
	if err := change.Restore(t.Context()); err != nil {
		t.Fatalf("Restore of the not-applied handle: %v", err)
	}
	policy, err := SetMessengerPolicy(canceled, nil, ceph.MessengerPolicy{})
	if policy == nil || !errors.Is(err, ceph.ErrInvalidRequest) || !errors.As(err, &op) || op.Op != "override.SetMessengerPolicy" {
		t.Fatalf("nil cluster policy: %v %v", policy, err)
	}
}

// The reservation of a call that fails before the first write is settled: no
// key stays held and the key is free for a retry.
func TestSetConfigEarlyReturnsLeaveNoReservation(t *testing.T) {
	setting := ceph.ConfigSetting{Section: "osd", Name: "osd_max_backfills", Value: "2"}
	ctr := &configFixture{failDumpAt: 1}
	c := newConfigFixture(ctr)
	change, err := SetConfig(t.Context(), c, setting)
	if err == nil || change.state.refused == nil || len(heldConfig(c)) != 0 {
		t.Fatalf("failed read: %v %v", change, err)
	}
	change, err = SetConfig(t.Context(), c, setting)
	if err != nil || len(heldConfig(c)) != 1 {
		t.Fatalf("a freed key stayed refused: %v", err)
	}
	if again, err := SetConfig(t.Context(), c, setting); err == nil || err.Error() != "restore the existing configuration override first" || again.state.refused == nil {
		t.Fatalf("overlap: %v", err)
	}
	if err := change.Restore(t.Context()); err != nil || len(heldConfig(c)) != 0 {
		t.Fatalf("restore: %v", err)
	}
}

// A handle whose state is a copy of a restored predecessor's is stale: its
// key now maps to the successor, and Restore must refuse it without a native
// call or a change to the successor.
func TestSetConfigRestoreRefusesStaleTokens(t *testing.T) {
	ctr := &configFixture{}
	c := newConfigFixture(ctr)
	setting := ceph.ConfigSetting{Section: "osd", Name: "osd_max_backfills", Value: "2"}
	first, err := SetConfig(t.Context(), c, setting)
	if err != nil {
		t.Fatal(err)
	}
	stale := &ConfigHandle{owner: c, setting: first.setting, state: &configState{previous: first.state.previous, applied: "2", known: true, token: first.state.token}}
	if err := first.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	second, err := SetConfig(t.Context(), c, setting)
	if err != nil {
		t.Fatal(err)
	}
	before := len(ctr.calls)
	key := overridehook.ConfigKey(first.setting)
	if err := stale.Restore(t.Context()); err == nil || err.Error() != "configuration override is not tracked by this cluster" || len(ctr.calls) != before || heldConfig(c)[key] != second {
		t.Fatalf("a stale handle was not refused: %v", err)
	}
}

func TestSetConfigRestoreAfterTerminateIsRefusedAndKeepsTheEntry(t *testing.T) {
	ctr := &configFixture{}
	c := newConfigFixture(ctr)
	change, err := SetConfig(t.Context(), c, ceph.ConfigSetting{Section: "osd", Name: "debug_osd", Value: "2"})
	if err != nil {
		t.Fatal(err)
	}
	servicehook.Lock(c)
	testhelper.TestSetClosed(c, true)
	servicehook.Unlock(c)
	before := len(ctr.calls)
	if err := change.Restore(t.Context()); err == nil || len(ctr.calls) != before || len(heldConfig(c)) != 1 {
		t.Fatalf("restore after Terminate: %v", err)
	}
}

func TestSetConfigWaitsForTopologyOnlyUntilTheContextEnds(t *testing.T) {
	ctr := &configFixture{}
	c := newConfigFixture(ctr)
	servicehook.Lock(c)
	short, stop := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer stop()
	started := time.Now()
	change, err := SetConfig(short, c, ceph.ConfigSetting{Section: "global", Name: "debug_ms", Value: "1"})
	servicehook.Unlock(c)
	if !errors.Is(err, context.DeadlineExceeded) || change.state.refused == nil || len(ctr.calls) != 0 || time.Since(started) > time.Second {
		t.Fatalf("configuration admission ignored the caller deadline: %v", err)
	}
}

func TestSetConfigConcurrentOverlapAdmitsOne(t *testing.T) {
	ctr := &configFixture{}
	c := newConfigFixture(ctr)
	var wg sync.WaitGroup
	results := make([]*ConfigHandle, 8)
	errs := make([]error, len(results))
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = SetConfig(t.Context(), c, ceph.ConfigSetting{Section: "osd", Name: "debug_osd", Value: "2"})
		}()
	}
	wg.Wait()
	applied := 0
	for i, change := range results {
		if change == nil {
			t.Fatal("a call returned a nil handle")
		}
		if errs[i] == nil {
			applied++
		}
	}
	if applied != 1 || len(heldConfig(c)) != 1 {
		t.Fatalf("%d overlapping calls applied, %d held", applied, len(heldConfig(c)))
	}
}
