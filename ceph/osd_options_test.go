package ceph

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func mustComposeOSD(t *testing.T, configs ...OSDOptions) osdPlan {
	t.Helper()
	plan, err := composeOSDOptions(configs)
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	return plan
}

func mustRefuseOSD(t *testing.T, configs ...OSDOptions) error {
	t.Helper()
	plan, err := composeOSDOptions(configs)
	if err == nil || !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("composition was not refused as an invalid request: %+v %v", plan, err)
	}
	return err
}

// Zero, one and several configuration values.
func TestOSDOptionsZeroOneAndMultipleValues(t *testing.T) {
	for name, configs := range map[string][]OSDOptions{"no value": nil, "zero value": {{}}, "two zero values": {{}, {}}} {
		if plan := mustComposeOSD(t, configs...); plan != (osdPlan{}) {
			t.Fatalf("%s: defaults changed: %+v", name, plan)
		}
	}
	placement := OSDConfig{Host: "host-a", Rack: "rack-a", Root: "default", DeviceClass: "fast"}
	if plan := mustComposeOSD(t, OSDOptions{}.WithPlacement(placement)); plan.placement != placement || plan.imageSet {
		t.Fatalf("one placement: %+v", plan)
	}
	if plan := mustComposeOSD(t, OSDOptions{}.WithImage("example.invalid/ceph:new")); plan.image != "example.invalid/ceph:new" || !plan.imageSet || plan.placement != (OSDConfig{}) {
		t.Fatalf("one image: %+v", plan)
	}
	// Fields set in different values compose, and an unset value erases nothing.
	plan := mustComposeOSD(t, OSDOptions{}.WithPlacement(placement), OSDOptions{}, OSDOptions{}.WithImage("example.invalid/ceph:new"))
	if plan.placement != placement || plan.image != "example.invalid/ceph:new" || !plan.imageSet {
		t.Fatalf("two values: %+v", plan)
	}
	// One chained value is the same configuration.
	if chained := mustComposeOSD(t, OSDOptions{}.WithPlacement(placement).WithImage("example.invalid/ceph:new")); chained != plan {
		t.Fatalf("chained %+v != composed %+v", chained, plan)
	}
}

// Explicit empty settings differ from unspecified ones.
func TestOSDOptionsExplicitEmptyIsNotUnspecified(t *testing.T) {
	// An empty placement is an explicit default placement: it composes, and it
	// conflicts with another placement.
	if plan := mustComposeOSD(t, OSDOptions{}.WithPlacement(OSDConfig{})); plan.placement != (OSDConfig{}) {
		t.Fatalf("explicit default placement: %+v", plan)
	}
	_ = mustRefuseOSD(t, OSDOptions{}.WithPlacement(OSDConfig{}), OSDOptions{}.WithPlacement(OSDConfig{Host: "host-a"}))
	// An empty image is an explicit invalid selection, not "no override".
	for name, image := range map[string]string{"empty": "", "space": " ", "padded": " a:1", "newline": "a:1\n", "NUL": "a:\x00"} {
		t.Run(name, func(t *testing.T) { _ = mustRefuseOSD(t, OSDOptions{}.WithImage(image)) })
	}
	_ = mustRefuseOSD(t, OSDOptions{}, OSDOptions{}.WithImage(""))
	_ = mustRefuseOSD(t, OSDOptions{}.WithImage("a:1").WithImage(""))
}

// Repeated settings across values are refused, also when they agree.
func TestOSDOptionsRefuseRepeatedSettings(t *testing.T) {
	a, b := OSDConfig{Host: "host-a"}, OSDConfig{Host: "host-b"}
	for name, test := range map[string]struct {
		configs []OSDOptions
		want    string
	}{
		"different placements": {[]OSDOptions{OSDOptions{}.WithPlacement(a), OSDOptions{}.WithPlacement(b)}, "placement"},
		"equal placements":     {[]OSDOptions{OSDOptions{}.WithPlacement(a), OSDOptions{}.WithPlacement(a)}, "placement"},
		"different images":     {[]OSDOptions{OSDOptions{}.WithImage("a:1"), OSDOptions{}.WithImage("b:2")}, "image"},
		"equal images":         {[]OSDOptions{OSDOptions{}.WithImage("a:1"), OSDOptions{}.WithImage("a:1")}, "image"},
		"separated by a gap":   {[]OSDOptions{OSDOptions{}.WithImage("a:1"), {}, OSDOptions{}.WithImage("a:1")}, "image"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := mustRefuseOSD(t, test.configs...); !strings.Contains(err.Error(), test.want) {
				t.Fatalf("refusal does not name %s: %v", test.want, err)
			}
		})
	}
}

// Within one value the later call replaces the earlier one.
func TestOSDOptionsLaterCallOfOneValueReplacesTheEarlier(t *testing.T) {
	plan := mustComposeOSD(t, OSDOptions{}.WithPlacement(OSDConfig{Host: "a", Rack: "r"}).WithPlacement(OSDConfig{Host: "b"}).WithImage("a:1").WithImage("b:2"))
	if plan.placement != (OSDConfig{Host: "b"}) || plan.image != "b:2" {
		t.Fatalf("later call did not replace: %+v", plan)
	}
}

// Derived values leave their base and each other unchanged.
func TestOSDOptionsDerivedValuesAreIndependent(t *testing.T) {
	base := OSDOptions{}.WithPlacement(OSDConfig{Rack: "rack-a"})
	first, second := base.WithImage("a:1"), base.WithImage("b:2")
	moved := base.WithPlacement(OSDConfig{Rack: "rack-b"})
	if p := mustComposeOSD(t, first); p.image != "a:1" || p.placement.Rack != "rack-a" {
		t.Fatalf("first: %+v", p)
	}
	if p := mustComposeOSD(t, second); p.image != "b:2" || p.placement.Rack != "rack-a" {
		t.Fatalf("second: %+v", p)
	}
	if p := mustComposeOSD(t, base); p.imageSet || p.placement.Rack != "rack-a" {
		t.Fatalf("base changed by its derived values: %+v", p)
	}
	if p := mustComposeOSD(t, moved); p.placement.Rack != "rack-b" || p.imageSet {
		t.Fatalf("moved: %+v", p)
	}
}

// A refused configuration is decided before the topology lock and before any
// registration, after checking the context.
func TestAddOSDRefusesBeforeLockAndRegistration(t *testing.T) {
	conflicting := []OSDOptions{OSDOptions{}.WithImage("a:1"), OSDOptions{}.WithImage("b:2")}
	newCluster := func() *Container {
		return &Container{settings: options{essentialImage: "example.invalid/ceph:cluster", startupTimeout: time.Second}}
	}
	for name, configs := range map[string][]OSDOptions{
		"conflicting images": conflicting,
		"invalid image":      {OSDOptions{}.WithImage(" ")},
		"invalid placement":  {OSDOptions{}.WithPlacement(OSDConfig{Host: "bad host"})},
	} {
		t.Run(name, func(t *testing.T) {
			c := newCluster()
			c.mu.Lock() // a call that waited for the lock would time out
			defer c.mu.Unlock()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			started := time.Now()
			osd, err := c.AddOSD(ctx, configs...)
			if osd != nil || !errors.Is(err, ErrInvalidRequest) || ctx.Err() != nil || time.Since(started) > 2*time.Second || c.osdHandles.count() != 0 || c.closed {
				t.Fatalf("osd=%v err=%v after %v owned=%d", osd, err, time.Since(started), c.osdHandles.count())
			}
		})
	}
	// The context is checked before the configuration.
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if osd, err := newCluster().AddOSD(canceled, conflicting...); osd != nil || !errors.Is(err, context.Canceled) || errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("canceled context: %v %v", osd, err)
	}

}

// No argument and an explicit default placement resolve to the same location.
func TestAddOSDDefaultAndExplicitDefaultPlacementResolveAlike(t *testing.T) {
	settings := options{defaultCRUSHRoot: "custom"}
	implicit, err := normalizeOSDConfig(resolveOSDDefaults(settings, mustComposeOSD(t).placement))
	if err != nil {
		t.Fatal(err)
	}
	explicit, err := normalizeOSDConfig(resolveOSDDefaults(settings, mustComposeOSD(t, OSDOptions{}.WithPlacement(OSDConfig{})).placement))
	if err != nil || implicit != explicit {
		t.Fatalf("implicit %+v explicit %+v %v", implicit, explicit, err)
	}
}
