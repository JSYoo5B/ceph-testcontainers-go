package override

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// A real handle feeds Finalization.TrackDebugLevel through the narrow
// interface, so ceph never names the override type.
var _ ceph.DebugLevelReceipter = (*DebugLevelHandle)(nil)

// TestSetDebugLevelRefusalsReturnNotAppliedHandles is the table of the refusals
// that happen before a window is opened. Every row returns a non-nil handle, a
// receipt that copies the request as given with the refusal as its reason, no
// command, and a Restore that is a no-op.
func TestSetDebugLevelRefusalsReturnNotAppliedHandles(t *testing.T) {
	good := ceph.DebugLevelSetting{Daemon: "osd.0", Name: "debug_osd", Level: "1"}
	for name, test := range map[string]struct {
		config  *servicehook.TestConfig
		nilC    bool
		ctx     func() context.Context
		setting ceph.DebugLevelSetting
		kind    error
		want    string
	}{
		"nil cluster":    {nilC: true, setting: good, kind: ceph.ErrInvalidRequest, want: "the cluster is nil"},
		"shared cluster": {config: &servicehook.TestConfig{Shared: true}, setting: good, kind: ceph.ErrSharedCluster},
		"ended context": {setting: good, want: "context canceled", ctx: func() context.Context {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx
		}},
		"MON":            {setting: ceph.DebugLevelSetting{Daemon: "mon.a", Name: "debug_ms", Level: "1"}, kind: ceph.ErrInvalidRequest, want: "must be an OSD"},
		"MGR":            {setting: ceph.DebugLevelSetting{Daemon: "mgr.a", Name: "debug_ms", Level: "1"}, kind: ceph.ErrInvalidRequest, want: "must be an OSD"},
		"malformed":      {setting: ceph.DebugLevelSetting{Daemon: "osd.01", Name: "debug_ms", Level: "1"}, kind: ceph.ErrInvalidRequest, want: "must be an OSD"},
		"option":         {setting: ceph.DebugLevelSetting{Daemon: "osd.0", Name: "debug_mon", Level: "1"}, kind: ceph.ErrInvalidRequest, want: "not supported"},
		"level":          {setting: ceph.DebugLevelSetting{Daemon: "osd.0", Name: "debug_osd", Level: "100"}, kind: ceph.ErrInvalidRequest, want: "N or N/M"},
		"unknown OSD":    {setting: ceph.DebugLevelSetting{Daemon: "osd.7", Name: "debug_osd", Level: "1"}, kind: ceph.ErrInvalidRequest, want: "not a running-capable OSD"},
		"closed cluster": {config: &servicehook.TestConfig{Closed: true, OSDs: 1}, setting: good, want: "terminated"},
	} {
		t.Run(name, func(t *testing.T) {
			native := &blocklistFixture{}
			config := servicehook.TestConfig{StartupTimeout: time.Second}
			if test.config != nil {
				config = *test.config
				config.StartupTimeout = time.Second
			}
			var c *ceph.Container
			if !test.nilC {
				c = servicehook.TestCluster(native, config)
			}
			ctx := t.Context()
			if test.ctx != nil {
				ctx = test.ctx()
			}
			h, err := SetDebugLevel(ctx, c, test.setting)
			if h == nil || err == nil {
				t.Fatalf("handle %v err %v", h, err)
			}
			if test.kind != nil && !errors.Is(err, test.kind) {
				t.Errorf("error %v is not %v", err, test.kind)
			}
			if test.want != "" && !strings.Contains(err.Error(), test.want) {
				t.Errorf("error %q lacks %q", err, test.want)
			}
			if test.nilC || errors.Is(test.kind, ceph.ErrSharedCluster) {
				var op *ceph.OperationError
				if !errors.As(err, &op) || op.Op != "override.SetDebugLevel" {
					t.Errorf("error %v does not carry the operation", err)
				}
			}
			receipt := h.Receipt()
			if receipt.State != ceph.DebugLevelNotApplied || receipt.Reason != err.Error() || receipt.Daemon != test.setting.Daemon ||
				receipt.Name != test.setting.Name || receipt.Requested != test.setting.Level || receipt.Applied != "" || receipt.Restored != "" ||
				receipt.Generation != "" || receipt.Prior != "" || len(receipt.Transitions) != 1 {
				t.Errorf("receipt %+v for %v", receipt, err)
			}
			if native.commands != 0 {
				t.Errorf("a refused call ran %d commands", native.commands)
			}
			ended, cancel := context.WithCancel(t.Context())
			cancel()
			if err := restoreWithin(ended, t, h); err != nil {
				t.Errorf("Restore of a not-applied window: %v", err)
			}
			if h.Receipt().State != ceph.DebugLevelNotApplied || len(h.Receipt().Transitions) != 1 {
				t.Errorf("Restore changed the receipt: %+v", h.Receipt())
			}
			if err := restoreWithin(t.Context(), t, h); err != nil {
				t.Errorf("second Restore: %v", err)
			}
		})
	}
}

// TestSetDebugLevelRefusalLeavesNoSlot shows that a refusal at the cluster
// holds no registry slot: the same window can be opened right after it.
func TestSetDebugLevelRefusalLeavesNoSlot(t *testing.T) {
	c := servicehook.TestCluster(nil, servicehook.TestConfig{StartupTimeout: time.Second})
	if _, err := SetDebugLevel(t.Context(), c, ceph.DebugLevelSetting{Daemon: "osd.7", Name: "debug_osd", Level: "1"}); err == nil {
		t.Fatal("unknown OSD accepted")
	}
	registry := overridehook.DebugWindowRegistry(c)
	if slotHeld(registry) {
		t.Fatal("a refused call kept its slot")
	}
}

// TestDebugLevelZeroHandleIsUnavailable pins what a handle without a window
// does: Receipt has no state, which Finalization records as absent, and
// Restore is an error, not a restoration.
func TestDebugLevelZeroHandleIsUnavailable(t *testing.T) {
	for name, h := range map[string]*DebugLevelHandle{"nil": nil, "zero value": {}} {
		receipt := h.Receipt()
		if receipt.State != "" || receipt.Transitions == nil || len(receipt.Transitions) != 0 {
			t.Errorf("%s: receipt %+v", name, receipt)
		}
		if err := h.Restore(t.Context()); err == nil {
			t.Errorf("%s: Restore reported success", name)
		}
	}
	if got := (DebugLevelHandle{}).String(); got != "Ceph debug level override" {
		t.Errorf("String of the zero value: %q", got)
	}
}

// TestDebugLevelRestoreNeedsNoClusterMutex shows that a window restores and
// releases its slot while a long topology operation holds the cluster mutex:
// the registry has a lock of its own and the fake target takes no cluster lock.
func TestDebugLevelRestoreNeedsNoClusterMutex(t *testing.T) {
	c := servicehook.TestCluster(nil, servicehook.TestConfig{StartupTimeout: time.Second})
	registry := overridehook.DebugWindowRegistry(c)
	fake := newDebugLevelFake("1/5")
	h, err := openDebugLevelFake(t.Context(), registry, fake, "20")
	if err != nil {
		t.Fatal(err)
	}
	servicehook.Lock(c)
	done := make(chan error, 1)
	go func() { done <- h.Restore(t.Context()) }()
	select {
	case err := <-done:
		servicehook.Unlock(c)
		if err != nil || slotHeld(registry) {
			t.Fatalf("restore: %v slot held %v", err, slotHeld(registry))
		}
	case <-time.After(5 * time.Second):
		servicehook.Unlock(c)
		t.Fatal("Restore waited for the cluster mutex")
	}
}

// TestDebugLevelAfterTerminate pins the result of a window whose cluster has
// terminated: the target no longer reports the daemon as owned, so the window
// ends as target-changed with the terminal error, nothing is restored and the
// slot is released.
func TestDebugLevelAfterTerminate(t *testing.T) {
	registry := newDebugWindows(t)
	fake := newDebugLevelFake("1/5")
	h, err := openDebugLevelFake(t.Context(), registry, fake, "20")
	if err != nil {
		t.Fatal(err)
	}
	fake.edit(func(f *debugLevelFake) { f.gen.owned = false })
	err = h.Restore(t.Context())
	if err == nil || !strings.Contains(err.Error(), "no longer owned by this cluster") || h.Receipt().State != ceph.DebugLevelTargetChanged ||
		h.Receipt().Restored != "" || len(fake.setCalls()) != 1 || slotHeld(registry) {
		t.Fatalf("restore: %v receipt %+v sets %v", err, h.Receipt(), fake.setCalls())
	}
	if again := h.Restore(t.Context()); again == nil || again.Error() != err.Error() {
		t.Fatalf("second restore: %v", again)
	}
}
