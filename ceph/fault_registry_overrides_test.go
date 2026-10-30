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
			if _, handle, err := c.faults.reserve(t.Context(), tc.kind, "key"); err == nil || handle != nil || err.Error() != tc.overlap {
				t.Fatalf("overlap reply %v %v, want the text %q and no handle", handle, err, tc.overlap)
			}
			c.mu.Lock()
			for _, phase := range append([]faultPhase{faultTracked}, faultPhases...) {
				if len(c.faults.held(phase)) != 0 {
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

func TestAllowPoolDeleteHeldFollowsTheRegistry(t *testing.T) {
	c := &Container{}
	var log []string
	if c.faults.poolDeleteOverrideHeld() {
		t.Fatal("an empty registry holds the key")
	}
	hold(t, c, faultConfigOverride, configOverrideKey(ConfigSetting{Section: "osd", Name: "osd_max_backfills"}), "", &log)
	if c.faults.poolDeleteOverrideHeld() {
		t.Fatal("an unrelated key counted as held")
	}
	hold(t, c, faultConfigOverride, configOverrideKey(allowPoolDeleteSetting), "", &log)
	if !c.faults.poolDeleteOverrideHeld() {
		t.Fatal("the held key was not seen")
	}
	c.mu.Lock()
	for id, entry := range c.faults.entries {
		if id.key == configOverrideKey(allowPoolDeleteSetting) {
			c.faults.release(entry)
		}
	}
	c.mu.Unlock()
	if c.faults.poolDeleteOverrideHeld() {
		t.Fatal("a released key stayed held")
	}
}
