package overridehook_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// TestRegistryThroughHooks calls every method of the seam that needs no
// native command. The blocklist commands themselves run against a fake control
// container in the tests of package override.
func TestRegistryThroughHooks(t *testing.T) {
	c := servicehook.TestCluster(nil, servicehook.TestConfig{StartupTimeout: 3 * time.Second})
	ctx := t.Context()
	if err := overridehook.Admit(c, "override.AddBlocklist"); err != nil {
		t.Fatalf("a cluster of its own refused an override: %v", err)
	}
	if err := overridehook.Admit(c, "override.Unclassified"); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("an unclassified override was admitted: %v", err)
	}
	if got := overridehook.StartupTimeout(c); got != 3*time.Second {
		t.Fatalf("startup timeout %v", got)
	}
	if overridehook.Terminated() == nil || !errors.Is(overridehook.Terminated(), ceph.ErrInvalidRequest) {
		t.Fatal("the terminated error is missing or unclassified")
	}
	cause := errors.New("cause")
	if err := overridehook.Invalid(cause); !errors.Is(err, ceph.ErrInvalidRequest) || !errors.Is(err, cause) || err.Error() != cause.Error() {
		t.Fatalf("Invalid changed the text or lost the class: %v", err)
	}
	if got, err := overridehook.NormalizeBlocklistSession("v2:192.0.2.7:0/1234"); err != nil || got != "192.0.2.7:0/1234" {
		t.Fatalf("normalize: %q %v", got, err)
	}
	if _, err := overridehook.NormalizeBlocklistSession("192.0.2.7"); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a bare address was accepted: %v", err)
	}
	type handle struct{}
	var first overridehook.Token
	held := &handle{}
	err := overridehook.WithTopology(ctx, c, func() error {
		if err := overridehook.Ready(c); err == nil {
			t.Error("a cluster without a control container was ready")
		}
		token, err := overridehook.Reserve(ctx, c, overridehook.Blocklist, "192.0.2.7:0/1")
		if err != nil {
			return err
		}
		first = token
		if _, err := overridehook.Reserve(ctx, c, overridehook.Blocklist, "192.0.2.7:0/1"); err == nil {
			t.Error("a reserved key was reserved twice")
		}
		if closed, err := overridehook.Settle(c, token, held); err != nil || closed {
			t.Errorf("settle: closed=%v err=%v", closed, err)
		}
		if _, err := overridehook.Reserve(ctx, c, overridehook.Blocklist, "192.0.2.7:0/1"); err == nil {
			t.Error("a held key was reserved twice")
		}
		if _, err := overridehook.Reserve(ctx, c, overridehook.Kind("Other"), "x"); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("an unknown kind was reserved: %v", err)
		}
		if _, err := overridehook.Settle(c, overridehook.Token{}, held); err == nil {
			t.Error("a zero token settled")
		}
		if got := overridehook.TestHeld(c, overridehook.Blocklist); got["192.0.2.7:0/1"] != held || len(got) != 1 {
			t.Errorf("the held handle is %v", got)
		}
		// The OSD flag kind has its own keys and its own overlap text.
		flagToken, err := overridehook.Reserve(ctx, c, overridehook.OSDFlag, "noout")
		if err != nil {
			t.Errorf("an OSD flag key was refused next to a blocklist key: %v", err)
			return nil
		}
		if _, err := overridehook.Reserve(ctx, c, overridehook.OSDFlag, "noout"); err == nil || err.Error() != "restore the existing OSD flag override first" {
			t.Errorf("a reserved OSD flag was reserved twice: %v", err)
		}
		if _, err := overridehook.Settle(c, flagToken, held); err != nil {
			t.Errorf("settle of an OSD flag: %v", err)
		}
		if got := overridehook.TestHeld(c, overridehook.OSDFlag); got["noout"] != held || len(got) != 1 {
			t.Errorf("the held OSD flag handle is %v", got)
		}
		if !overridehook.Release(c, flagToken) {
			t.Error("the OSD flag was not released")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	overridehook.WithTopologyNoContext(c, func() {
		if !overridehook.Tracks(c, first) || !overridehook.Release(c, first) || overridehook.Tracks(c, first) || overridehook.Release(c, first) {
			t.Error("release through the hooks did not forget the token exactly once")
		}
		// Without a control container every native operation fails and none panics.
		if _, err := overridehook.BlocklistEntries(ctx, c); err == nil {
			t.Error("blocklist read without a control container")
		}
		if err := overridehook.BlocklistAdd(ctx, c, "192.0.2.7:0/1", time.Minute); err == nil {
			t.Error("blocklist add without a control container")
		}
		if err := overridehook.BlocklistRemove(ctx, c, "192.0.2.7:0/1"); err == nil {
			t.Error("blocklist removal without a control container")
		}
		if _, err := overridehook.OSDMapFlags(ctx, c); err == nil {
			t.Error("OSDMap flag read without a control container")
		}
		if err := overridehook.SetOSDFlag(ctx, c, ceph.OSDFlagNoOut, true); err == nil {
			t.Error("OSD flag write without a control container")
		}
	})
	// A context that has ended stops the wait for a held mutex.
	servicehook.Lock(c)
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := overridehook.WithTopology(short, c, func() error { t.Error("ran without the mutex"); return nil }); err == nil {
		t.Error("WithTopology ignored its context")
	}
	servicehook.Unlock(c)
	if err := c.Terminate(ctx); err != nil {
		t.Fatalf("Terminate undid a released override: %v", err)
	}
}
