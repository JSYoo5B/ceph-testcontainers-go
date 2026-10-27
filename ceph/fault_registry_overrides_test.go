package ceph

import (
	"testing"
)

var overrideKinds = []struct {
	name    string
	kind    faultKind
	overlap string
}{
	{"config", faultConfigOverride, "restore the existing configuration override first"},
	{"OSD flag", faultOSDFlag, "restore the existing OSD flag override first"},
	{"MGR module", faultMGRModule, "restore the existing MGR module override first"},
}

func TestOverrideKindsAreTrackedWithTodaysRefusals(t *testing.T) {
	for _, tc := range overrideKinds {
		t.Run(tc.name, func(t *testing.T) {
			if faultKinds[tc.kind].phase != faultTracked || faultKinds[tc.kind].undone != "" {
				t.Fatalf("kind %v is not a plain tracked kind: %+v", tc.kind, faultKinds[tc.kind])
			}
			for _, reserved := range []bool{false, true} {
				if err := faultKinds[tc.kind].refuse(reserved); err == nil || err.Error() != tc.overlap {
					t.Fatalf("refusal (reserved=%v) = %v, want %q", reserved, err, tc.overlap)
				}
			}
			c := &Container{}
			var log []string
			hold(t, c, tc.kind, "key", "", &log)
			if _, handle, err := c.reserveFault(t.Context(), tc.kind, "key"); err == nil || handle != nil || err.Error() != tc.overlap {
				t.Fatalf("overlap reply %v %v, want the text %q and no handle", handle, err, tc.overlap)
			}
			c.mu.Lock()
			for _, phase := range append([]faultPhase{faultTracked}, faultPhases...) {
				if len(c.heldFaults(phase)) != 0 {
					t.Errorf("a tracked override is visited by phase %v", phase)
				}
			}
			c.mu.Unlock()
		})
	}
}

func TestTerminateLeavesTrackedOverrideKinds(t *testing.T) {
	c := &Container{}
	var log []string
	for _, tc := range overrideKinds {
		hold(t, c, tc.kind, "key", "", &log)
	}
	hold(t, c, faultNetworkDelay, "delay", "", &log)
	if err := c.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(log) != 1 || log[0] != "delay:unlocked" {
		t.Fatalf("Terminate undid %v; want the delay alone", log)
	}
	for _, tc := range overrideKinds {
		if len(c.faultsOf(tc.kind)) != 1 {
			t.Errorf("Terminate dropped the %s override", tc.name)
		}
	}
}

// settled fails the test if a reservation (a key without a handle) remains
// for kind, and returns the number of held keys.
func settled(t *testing.T, c *Container, kind faultKind, what string) int {
	t.Helper()
	held := 0
	for key, handle := range c.faultsOf(kind) {
		if handle == nil {
			t.Errorf("%s left a reservation for %q", what, key)
			continue
		}
		held++
	}
	return held
}

func TestOverrideConfigEarlyReturnsLeaveNoReservation(t *testing.T) {
	setting := ConfigSetting{Section: "osd", Name: "osd_max_backfills", Value: "2"}
	ctr := &configFixture{failDumpAt: 1}
	c := configCluster(ctr)
	if change, err := c.OverrideConfig(t.Context(), setting); err == nil || change != nil || settled(t, c, faultConfigOverride, "a failed configuration read") != 0 {
		t.Fatalf("failed read: %v %v", change, err)
	}
	change, err := c.OverrideConfig(t.Context(), setting)
	if err != nil || settled(t, c, faultConfigOverride, "a retry") != 1 {
		t.Fatalf("a freed key stayed refused: %v", err)
	}
	if _, err := c.OverrideConfig(t.Context(), setting); err == nil || err.Error() != overrideKinds[0].overlap {
		t.Fatalf("overlap: %v", err)
	}
	if err := change.Restore(t.Context()); err != nil || len(c.faultsOf(faultConfigOverride)) != 0 {
		t.Fatalf("restore: %v", err)
	}
}

func TestOverrideOSDFlagEarlyReturnsLeaveNoReservation(t *testing.T) {
	ctr := &osdPolicyFixture{raw: "not json"}
	c := osdPolicyCluster(ctr)
	if change, err := c.OverrideOSDFlag(t.Context(), OSDFlagNoOut, true); err == nil || change != nil || settled(t, c, faultOSDFlag, "a failed dump") != 0 {
		t.Fatalf("failed dump: %v %v", change, err)
	}
	ctr.raw = ""
	change, err := c.OverrideOSDFlag(t.Context(), OSDFlagNoOut, true)
	if err != nil || settled(t, c, faultOSDFlag, "a retry") != 1 {
		t.Fatalf("a freed key stayed refused: %v", err)
	}
	if _, err := c.OverrideOSDFlag(t.Context(), OSDFlagNoOut, false); err == nil || err.Error() != overrideKinds[1].overlap {
		t.Fatalf("overlap: %v", err)
	}
	if err := change.Restore(t.Context()); err != nil || len(c.faultsOf(faultOSDFlag)) != 0 {
		t.Fatalf("restore: %v", err)
	}
}

// A handle whose state is a copy of a restored predecessor's is stale: its
// key now maps to the successor, and Restore must refuse it without a native
// call or a change to the successor.
func TestOverrideRestoreRefusesStaleTokens(t *testing.T) {
	t.Run("config", func(t *testing.T) {
		ctr := &configFixture{}
		c := configCluster(ctr)
		setting := ConfigSetting{Section: "osd", Name: "osd_max_backfills", Value: "2"}
		first, err := c.OverrideConfig(t.Context(), setting)
		if err != nil {
			t.Fatal(err)
		}
		stale := &ConfigOverrideHandle{owner: c, setting: first.setting, state: &configOverrideState{previous: first.state.previous, applied: "2", known: true, token: first.state.token}}
		if err := first.Restore(t.Context()); err != nil {
			t.Fatal(err)
		}
		second, err := c.OverrideConfig(t.Context(), setting)
		if err != nil {
			t.Fatal(err)
		}
		before := len(ctr.calls)
		if err := stale.Restore(t.Context()); err == nil || err.Error() != "configuration override is not tracked by this cluster" || len(ctr.calls) != before || c.heldFault(faultConfigOverride, configOverrideKey(setting)) != second {
			t.Fatalf("a stale handle was not refused: %v", err)
		}
	})
	t.Run("OSD flag", func(t *testing.T) {
		ctr := &osdPolicyFixture{}
		c := osdPolicyCluster(ctr)
		first, err := c.OverrideOSDFlag(t.Context(), OSDFlagNoOut, true)
		if err != nil {
			t.Fatal(err)
		}
		stale := &OSDFlagOverrideHandle{owner: c, flag: first.flag, state: &osdFlagOverrideState{applied: true, token: first.state.token}}
		if err := first.Restore(t.Context()); err != nil {
			t.Fatal(err)
		}
		second, err := c.OverrideOSDFlag(t.Context(), OSDFlagNoOut, true)
		if err != nil {
			t.Fatal(err)
		}
		before := len(ctr.calls)
		if err := stale.Restore(t.Context()); err == nil || err.Error() != "OSD flag override is not tracked by this cluster" || len(ctr.calls) != before || c.heldFault(faultOSDFlag, string(OSDFlagNoOut)) != second {
			t.Fatalf("a stale handle was not refused: %v", err)
		}
	})
}

func TestAllowPoolDeleteHeldFollowsTheRegistry(t *testing.T) {
	ctr := &configFixture{}
	c := configCluster(ctr)
	if c.allowPoolDeleteHeld() {
		t.Fatal("an empty registry holds the key")
	}
	_, err := c.OverrideConfig(t.Context(), ConfigSetting{Section: "osd", Name: "osd_max_backfills", Value: "2"})
	if err != nil || c.allowPoolDeleteHeld() {
		t.Fatalf("an unrelated key counted as held: %v", err)
	}
	change, err := c.OverrideConfig(t.Context(), allowPoolDeleteSetting)
	if err != nil || !c.allowPoolDeleteHeld() {
		t.Fatalf("the held key was not seen: %v", err)
	}
	if err := change.Restore(t.Context()); err != nil || c.allowPoolDeleteHeld() {
		t.Fatalf("a restored key stayed held: %v", err)
	}

}
