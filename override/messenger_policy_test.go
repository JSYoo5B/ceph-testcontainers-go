package override

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

var (
	modeCRC    = []ceph.MessengerConnectionMode{ceph.MessengerCRC}
	modeSecure = []ceph.MessengerConnectionMode{ceph.MessengerSecure}
)

// writes returns the set and rm commands that the fixture received, in order.
func (ctr *configFixture) writes() []string {
	var writes []string
	for _, call := range ctr.calls {
		if len(call) >= 4 && call[0] == "config" && (call[1] == "set" || call[1] == "rm") {
			writes = append(writes, strings.Join(call[1:4], " "))
		}
	}
	return writes
}

func TestSetMessengerPolicyRefusesBeforeAnyChange(t *testing.T) {
	control := &configFixture{}
	c := configCluster(control, ceph.MessengerV2Secure)
	policy, err := SetMessengerPolicy(t.Context(), c, ceph.MessengerPolicy{OSDs: modeCRC})
	if !errors.Is(err, ceph.ErrInvalidRequest) || policy == nil || len(control.calls) != 0 {
		t.Fatalf("temporary policy under MessengerV2Secure: %v, calls %v", err, control.calls)
	}
	if err := policy.Restore(t.Context()); err != nil {
		t.Fatalf("Restore of the not-applied handle: %v", err)
	}
	c = configCluster(control, ceph.MessengerV2Only)
	for name, policy := range map[string]ceph.MessengerPolicy{
		"empty":        {},
		"unknown mode": {OSDs: []ceph.MessengerConnectionMode{"plain"}},
	} {
		handle, err := SetMessengerPolicy(t.Context(), c, policy)
		if !errors.Is(err, ceph.ErrInvalidRequest) || handle == nil || len(control.calls) != 0 {
			t.Fatalf("%s temporary policy: %v, calls %v", name, err, control.calls)
		}
		if err := handle.Restore(t.Context()); err != nil {
			t.Fatalf("%s: Restore of the not-applied handle: %v", name, err)
		}
	}
}

// The policy is one SetConfig per non-nil field, and Restore returns them in
// reverse order.
func TestSetMessengerPolicyAppliesInOrderAndRestoresInReverse(t *testing.T) {
	control := &configFixture{}
	c := newConfigFixture(control)
	handle, err := SetMessengerPolicy(t.Context(), c, ceph.MessengerPolicy{Monitors: modeCRC, Managers: modeSecure, OSDs: modeCRC})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"set mon ms_mon_service_mode", "set mgr ms_mon_service_mode", "set osd ms_service_mode"}
	if got := control.writes(); !slices.Equal(got, want) || len(heldConfig(c)) != 3 {
		t.Fatalf("apply wrote %v, held %d", got, len(heldConfig(c)))
	}
	if err := handle.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	want = append(want, "rm osd ms_service_mode", "rm mgr ms_mon_service_mode", "rm mon ms_mon_service_mode")
	if got := control.writes(); !slices.Equal(got, want) || len(heldConfig(c)) != 0 || len(control.entries) != 0 {
		t.Fatalf("restore wrote %v, held %d", got[3:], len(heldConfig(c)))
	}
	// Restore is idempotent.
	before := len(control.calls)
	if err := handle.Restore(t.Context()); err != nil || len(control.calls) != before {
		t.Fatalf("second Restore: %v", err)
	}
}

// A later option that is refused leaves the earlier ones in the handle and
// does not append the refused one.
func TestSetMessengerPolicyLaterRefusalKeepsEarlierOptions(t *testing.T) {
	control := &configFixture{}
	c := newConfigFixture(control)
	blocker, err := SetConfig(t.Context(), c, ceph.ConfigSetting{Section: "mgr", Name: "ms_mon_service_mode", Value: "secure"})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := SetMessengerPolicy(t.Context(), c, ceph.MessengerPolicy{Monitors: modeCRC, Managers: modeCRC, OSDs: modeCRC})
	if err == nil || handle == nil || len(handle.state.children) != 1 || handle.state.refused != nil {
		t.Fatalf("later refusal: handle %v, error %v", handle, err)
	}
	if err := handle.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Only the first option was undone; the blocker still owns the manager key.
	if len(heldConfig(c)) != 1 || len(control.entries) != 1 || control.entries[0].Section != "mgr" || control.entries[0].Value != "secure" {
		t.Fatalf("restore touched the blocker: %v", control.entries)
	}
	if err := blocker.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// A first option that is refused leaves a not-applied handle with no children.
func TestSetMessengerPolicyFirstRefusalIsNotApplied(t *testing.T) {
	control := &configFixture{}
	c := newConfigFixture(control)
	blocker, err := SetConfig(t.Context(), c, ceph.ConfigSetting{Section: "mon", Name: "ms_mon_service_mode", Value: "secure"})
	if err != nil {
		t.Fatal(err)
	}
	before := len(control.writes())
	handle, err := SetMessengerPolicy(t.Context(), c, ceph.MessengerPolicy{Monitors: modeCRC, OSDs: modeCRC})
	if err == nil || handle == nil || handle.state.refused == nil || len(handle.state.children) != 0 || len(control.writes()) != before {
		t.Fatalf("first refusal: handle %v, error %v", handle, err)
	}
	if err := handle.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := blocker.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// An uncertain option (a lost reply) stays a real, restorable handle.
func TestSetMessengerPolicyUncertainOptionStaysRestorable(t *testing.T) {
	control := &configFixture{failAfter: "set"}
	c := newConfigFixture(control)
	handle, err := SetMessengerPolicy(t.Context(), c, ceph.MessengerPolicy{Monitors: modeCRC, OSDs: modeCRC})
	if err == nil || handle == nil || handle.state.refused != nil || len(handle.state.children) != 1 {
		t.Fatalf("lost reply: handle %v, error %v", handle, err)
	}
	control.failAfter = ""
	if err := handle.Restore(t.Context()); err != nil || len(heldConfig(c)) != 0 || len(control.entries) != 0 {
		t.Fatalf("uncertain option was not restored: %v", err)
	}
}
