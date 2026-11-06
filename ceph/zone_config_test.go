package ceph

import (
	"errors"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

func regionalZones() ZoneConfig {
	return ZoneConfig{}.WithZones("a", "b").WithQuorumZone("a").WithHelperImage("helper")
}

func regionalLayout(zones ...string) ClusterConfig {
	var layout []OSDConfig
	for _, zone := range zones {
		layout = append(layout, OSDConfig{Zone: zone})
	}
	return ClusterConfig{}.WithInitialOSDs(layout...)
}

func TestZoneConfigZeroValueChangesNothing(t *testing.T) {
	settings, err := resolveOptions("img", []testcontainers.ContainerCustomizer{WithZoneConfig(ZoneConfig{})})
	if err != nil || settings.zones.enabled() || settings.zones.set != 0 {
		t.Fatalf("the zero value changed the settings: %+v %v", settings.zones, err)
	}
}

func TestZoneConfigComposesARegionalCluster(t *testing.T) {
	settings, err := resolveOptions("img", []testcontainers.ContainerCustomizer{
		WithZoneConfig(regionalZones()), WithClusterConfig(regionalLayout("a", "a", "b")),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !settings.zones.enabled() || settings.zones.quorum != "a" || settings.zones.helperImage != "helper" || !settings.zones.declares("b") {
		t.Fatalf("zones %+v", settings.zones)
	}
	if got := settings.initialOSDs[2].Zone; got != "b" {
		t.Fatalf("initial OSD zone %q", got)
	}
	// The fields compose from separate values.
	split := []testcontainers.ContainerCustomizer{
		WithZoneConfig(ZoneConfig{}.WithHelperImage("helper")), WithZoneConfig(ZoneConfig{}.WithZones("a", "b")),
		WithZoneConfig(ZoneConfig{}.WithQuorumZone("b")), WithClusterConfig(regionalLayout("a", "b")),
	}
	if settings, err := resolveOptions("img", split); err != nil || settings.zones.quorum != "b" {
		t.Fatalf("split values: %+v %v", settings.zones, err)
	}
}

func TestZoneConfigValuesOwnTheirZones(t *testing.T) {
	names := []string{"a", "b"}
	zones := ZoneConfig{}.WithZones(names...)
	names[0] = "c"
	if zones.zones[0] != "a" {
		t.Fatalf("the value shares the caller's slice: %v", zones.zones)
	}
	if replaced := zones.WithZones("x", "y"); replaced.zones[0] != "x" || zones.zones[0] != "a" {
		t.Fatalf("a later call does not replace within its own value: %v %v", replaced.zones, zones.zones)
	}
}

func TestZoneConfigRefusals(t *testing.T) {
	for name, opts := range map[string][]testcontainers.ContainerCustomizer{
		"one zone":             {WithZoneConfig(ZoneConfig{}.WithZones("a").WithQuorumZone("a").WithHelperImage("h")), WithClusterConfig(regionalLayout("a"))},
		"duplicate zone":       {WithZoneConfig(ZoneConfig{}.WithZones("a", "a").WithQuorumZone("a").WithHelperImage("h")), WithClusterConfig(regionalLayout("a"))},
		"invalid zone name":    {WithZoneConfig(ZoneConfig{}.WithZones("a", "b c").WithQuorumZone("a").WithHelperImage("h")), WithClusterConfig(regionalLayout("a"))},
		"OSD-like zone name":   {WithZoneConfig(ZoneConfig{}.WithZones("a", "osd.1").WithQuorumZone("a").WithHelperImage("h")), WithClusterConfig(regionalLayout("a"))},
		"no quorum zone":       {WithZoneConfig(ZoneConfig{}.WithZones("a", "b").WithHelperImage("h")), WithClusterConfig(regionalLayout("a", "b"))},
		"undeclared quorum":    {WithZoneConfig(ZoneConfig{}.WithZones("a", "b").WithQuorumZone("c").WithHelperImage("h")), WithClusterConfig(regionalLayout("a", "b"))},
		"no helper image":      {WithZoneConfig(ZoneConfig{}.WithZones("a", "b").WithQuorumZone("a")), WithClusterConfig(regionalLayout("a", "b"))},
		"empty helper image":   {WithZoneConfig(ZoneConfig{}.WithZones("a", "b").WithQuorumZone("a").WithHelperImage(" ")), WithClusterConfig(regionalLayout("a", "b"))},
		"quorum without zones": {WithZoneConfig(ZoneConfig{}.WithQuorumZone("a"))},
		"host network":         {WithZoneConfig(regionalZones()), WithClusterConfig(regionalLayout("a", "b")), WithHostNetwork()},
		"IPv6":                 {WithZoneConfig(regionalZones()), WithClusterConfig(regionalLayout("a", "b")), WithNetworkConfig(NetworkConfig{}.WithAddressFamily(AddressIPv6))},
		"default OSD layout":   {WithZoneConfig(regionalZones()), WithClusterConfig(ClusterConfig{}.WithOSDCount(2))},
		"implicit OSD layout":  {WithZoneConfig(regionalZones())},
		"OSD without zone":     {WithZoneConfig(regionalZones()), WithClusterConfig(regionalLayout("a", ""))},
		"OSD in unknown zone":  {WithZoneConfig(regionalZones()), WithClusterConfig(regionalLayout("a", "c"))},
		"zone without zones":   {WithClusterConfig(regionalLayout("a", "b"))},
		"field set twice":      {WithZoneConfig(regionalZones()), WithZoneConfig(ZoneConfig{}.WithQuorumZone("a")), WithClusterConfig(regionalLayout("a", "b"))},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := resolveOptions("img", opts); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("got %v, want ErrInvalidRequest", err)
			}
		})
	}
	// Without initial OSDs, every later OSD names its zone.
	if _, err := resolveOptions("img", []testcontainers.ContainerCustomizer{WithZoneConfig(regionalZones()), WithClusterConfig(ClusterConfig{}.WithNoInitialOSDs())}); err != nil {
		t.Fatalf("no initial OSDs: %v", err)
	}
}

func TestOSDZoneOfALaterOSD(t *testing.T) {
	zoned := zoneSettings{names: []string{"a", "b"}, quorum: "a"}
	for _, tc := range []struct {
		settings zoneSettings
		zone     string
		ok       bool
	}{
		{zoned, "a", true}, {zoned, "", false}, {zoned, "c", false},
		{zoneSettings{}, "", true}, {zoneSettings{}, "a", false},
	} {
		if err := tc.settings.checkOSDZone(tc.zone); (err == nil) != tc.ok || (err != nil && !errors.Is(err, ErrInvalidRequest)) {
			t.Errorf("zones %v, OSD zone %q: %v", tc.settings.names, tc.zone, err)
		}
	}
}

func TestCRUSHHierarchyPlacesZonesBelowTheRoot(t *testing.T) {
	ok := []OSDConfig{
		{Root: "region", Zone: "a", Rack: "a-1", Host: "a-1-0"},
		{Root: "region", Zone: "a", Rack: "a-1", Host: "a-1-1"},
		{Root: "region", Zone: "b", Host: "b-0"},
	}
	if err := validateCRUSHHierarchy("region", ok); err != nil {
		t.Fatal(err)
	}
	for name, layout := range map[string][]OSDConfig{
		"zone under two roots":   {{Root: "r1", Zone: "a", Host: "h1"}, {Root: "r2", Zone: "a", Host: "h2"}},
		"rack under two zones":   {{Root: "r", Zone: "a", Rack: "k", Host: "h1"}, {Root: "r", Zone: "b", Rack: "k", Host: "h2"}},
		"host under two zones":   {{Root: "r", Zone: "a", Host: "h"}, {Root: "r", Zone: "b", Host: "h"}},
		"zone named like a host": {{Root: "r", Zone: "h", Host: "x"}, {Root: "r", Zone: "a", Host: "h"}},
	} {
		if err := validateCRUSHHierarchy("r", layout); !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("%s: got %v, want ErrInvalidRequest", name, err)
		}
	}
	if _, err := normalizeInitialOSDs([]OSDConfig{{Zone: "a", Host: "h"}, {Zone: "b", Host: "h"}}); err == nil || !strings.Contains(err.Error(), "conflicting") {
		t.Fatalf("a host in two zones of one layout: %v", err)
	}
	if _, err := normalizeOSDConfig(OSDConfig{Zone: "bad zone"}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid zone bucket name: %v", err)
	}
}

func TestPoolFailureDomainZone(t *testing.T) {
	config, err := normalizePoolConfig(PoolConfig{Name: "p", FailureDomain: "zone"})
	if err != nil || config.FailureDomain != "zone" {
		t.Fatalf("zone failure domain: %+v %v", config, err)
	}
	if _, err := normalizePoolConfig(PoolConfig{Name: "p", FailureDomain: "region"}); err == nil {
		t.Fatal("an unknown failure domain was accepted")
	}
	pool := PoolConfig{Name: "p", Replicas: 2, MinSize: 1, FailureDomain: "zone"}
	spans := func(zones ...string) error {
		_, err := resolveOptions("img", []testcontainers.ContainerCustomizer{WithZoneConfig(regionalZones()),
			WithClusterConfig(regionalLayout(zones...).WithPools(pool))})
		return err
	}
	if err := spans("a", "b"); err != nil {
		t.Fatalf("two zones hold two zone replicas: %v", err)
	}
	if err := spans("a", "a"); err == nil || !strings.Contains(err.Error(), "zone domains") {
		t.Fatalf("one zone holds two zone replicas: %v", err)
	}
}

// Without zones the cluster's own members are unchanged and a caller's zone
// is refused; with zones an undeclared zone is refused, and a placed
// container carries its zone label and the enrollment hooks.
func TestZoneCustomizers(t *testing.T) {
	plain := &Container{}
	req := testcontainers.GenericContainerRequest{}
	if err := plain.zoneMember("a", "osd.0")(&req); err != nil || len(req.Labels) != 0 || len(req.LifecycleHooks) != 0 {
		t.Fatalf("a cluster without zones changed its member: %+v %v", req.ContainerRequest, err)
	}
	if err := plain.zoneAttachment()(&req); err != nil || len(req.LifecycleHooks) != 0 {
		t.Fatalf("a cluster without zones changed a client: %v", err)
	}
	if err := plain.WithZone("a")(&req); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("WithZone without zones: %v", err)
	}
	zoned := &Container{settings: options{zones: zoneSettings{names: []string{"a", "b"}, quorum: "a"}}, boundary: &zoneBoundary{}}
	if err := zoned.WithZone("c")(&req); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("an undeclared zone: %v", err)
	}
	placed := testcontainers.GenericContainerRequest{}
	if err := zoned.WithZone("b")(&placed); err != nil {
		t.Fatal(err)
	}
	if placed.Labels[zoneLabel] != "b" || len(placed.LifecycleHooks) != 1 ||
		len(placed.LifecycleHooks[0].PostStarts) != 1 || len(placed.LifecycleHooks[0].PreStops) != 1 || len(placed.LifecycleHooks[0].PreTerminates) != 1 {
		t.Fatalf("WithZone placed %+v with hooks %+v", placed.Labels, placed.LifecycleHooks)
	}
	member := testcontainers.GenericContainerRequest{}
	if err := zoned.quorumMember("mgr.a")(&member); err != nil || member.Labels[zoneLabel] != "a" || member.Labels[zoneMemberLabel] != "mgr.a" {
		t.Fatalf("a quorum member: %+v %v", member.Labels, err)
	}
	// A caller's WithZone after the quorum default places a service daemon.
	if err := zoned.WithZone("b")(&member); err != nil || member.Labels[zoneLabel] != "b" {
		t.Fatalf("WithZone did not override the quorum zone: %+v %v", member.Labels, err)
	}
}
