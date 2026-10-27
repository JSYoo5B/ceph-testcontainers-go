package overridehook_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/testcontainers/testcontainers-go"
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

// inertControl stands for a control container that no test command reaches.
type inertControl struct{ testcontainers.Container }

// TestDebugHooks calls the debug level operations of the seam that need no
// native command. The window logic runs against a fake target in the tests of
// package override.
func TestDebugHooks(t *testing.T) {
	ctx := t.Context()
	c := servicehook.TestCluster(inertControl{}, servicehook.TestConfig{StartupTimeout: time.Second, OSDs: 1})
	if err := overridehook.Admit(c, "override.SetDebugLevel"); err != nil {
		t.Fatalf("a cluster of its own refused SetDebugLevel: %v", err)
	}
	shared := servicehook.TestCluster(nil, servicehook.TestConfig{Shared: true})
	if err := overridehook.Admit(shared, "override.SetDebugLevel"); !errors.Is(err, ceph.ErrSharedCluster) {
		t.Fatalf("a shared cluster admitted SetDebugLevel: %v", err)
	}
	if target, err := overridehook.OpenDebugTarget(ctx, c, "osd.0"); err != nil || target == nil {
		t.Fatalf("an owned OSD: %v %v", target, err)
	}
	for _, daemon := range []string{"osd.5", "mon.a", "osd.01"} {
		if target, err := overridehook.OpenDebugTarget(ctx, c, daemon); target != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("%s: target %v err %v", daemon, target, err)
		}
	}
	closed := servicehook.TestCluster(inertControl{}, servicehook.TestConfig{Closed: true, OSDs: 1})
	if _, err := overridehook.OpenDebugTarget(ctx, closed, "osd.0"); !errors.Is(err, overridehook.Terminated()) {
		t.Errorf("a terminated cluster: %v", err)
	}
	// The lock is released again: the cluster mutex can be taken.
	overridehook.WithTopologyNoContext(c, func() {})
	registry := overridehook.DebugWindowRegistry(c)
	first, second := new(int), new(int)
	if err := registry.Reserve("osd.0/debug_osd", first); err != nil {
		t.Fatal(err)
	}
	if err := registry.Reserve("osd.0/debug_osd", second); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Errorf("a second window was admitted: %v", err)
	}
	registry.Release("osd.0/debug_osd", second)
	if err := registry.Reserve("osd.0/debug_osd", second); err == nil {
		t.Error("a release by a non-owner freed the slot")
	}
	registry.Release("osd.0/debug_osd", first)
	if err := registry.Reserve("osd.0/debug_osd", second); err != nil {
		t.Errorf("the slot was not freed: %v", err)
	}
	// The registry has a lock of its own: it works while the cluster mutex is
	// held by a long topology operation.
	servicehook.Lock(c)
	locked := make(chan struct{})
	go func() {
		defer close(locked)
		owner := new(int)
		if err := registry.Reserve("osd.0/debug_ms", owner); err != nil {
			t.Errorf("reserve under the cluster mutex: %v", err)
		}
		registry.Release("osd.0/debug_ms", owner)
	}()
	select {
	case <-locked:
	case <-time.After(5 * time.Second):
		t.Error("the registry waited for the cluster mutex")
	}
	servicehook.Unlock(c)
	if overridehook.DebugWindowRegistry(c) == overridehook.DebugWindowRegistry(shared) {
		t.Error("two clusters share one registry")
	}
	for name, test := range map[string]struct {
		owned, running, paused bool
		identity               string
		err                    error
		changed                bool
		want                   string
	}{
		"same":      {true, true, false, "id", nil, false, ""},
		"inspect":   {true, true, false, "id", errors.New("boom"), false, "inspect osd.0"},
		"not owned": {false, true, false, "id", nil, true, "no longer owned"},
		"other":     {true, true, false, "id2", nil, true, "another container generation"},
		"stopped":   {true, false, false, "id", nil, true, "is stopped"},
		"paused":    {true, true, true, "id", nil, false, "is paused"},
	} {
		changed, err := overridehook.CheckDebugGeneration(test.owned, test.identity, test.running, test.paused, test.err, "id", "osd.0")
		if changed != test.changed || (test.want == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), test.want)) {
			t.Errorf("%s: changed %v err %v", name, changed, err)
		}
	}
}
