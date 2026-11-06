package ceph

import (
	"fmt"
	"slices"
	"strings"
)

// zoneField names one setting of ZoneConfig.
type zoneField uint8

const (
	zoneNames zoneField = 1 << iota
	zoneQuorum
	zoneHelperImage
)

// ZoneConfig is the logical availability zone group of the Ceph
// configuration: which zones one regional cluster spans, which zone holds the
// MON quorum, and the image of the boundary's routers and route agents. It is
// a plain value with chainable, value-returning methods, not a customizer;
// pass it to Run through its one grouped option function, WithZoneConfig. The
// zero value changes nothing, and each field records whether it was set.
//
//	zones := ceph.ZoneConfig{}.WithZones("a", "b").WithQuorumZone("a").WithHelperImage(fault.NetemHelperImage)
//	layout := ceph.ClusterConfig{}.WithInitialOSDs(ceph.OSDConfig{Zone: "a"}, ceph.OSDConfig{Zone: "b"})
//	cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithZoneConfig(zones), ceph.WithClusterConfig(layout))
//
// A zone is at once a CRUSH bucket of type zone below the OSDs' root and a
// set of endpoints on the cluster's bridges. The cluster keeps its one public
// bridge (and its replication bridge with NetworkConfig.WithSeparateClusterNetwork),
// so every zone shares each plane's subnet and Ceph's public_network and
// cluster_network are unchanged. The boundary between the zones is owned by
// the cluster: one router per zone, which forwards the packets that leave its
// zone on every plane, and in the network namespace of every member one /32
// route per address of every member of another zone, through the router of
// the member's own zone. Every cross-zone packet therefore crosses exactly one
// router, the one of the zone it leaves, and a packet within a zone goes
// directly. fault.ApplyZoneProfile shapes or cuts what one router forwards.
// See docs/guides/regional-zones.md for the membership rules, the route
// lifecycle and the limits of a single Docker engine.
//
// All three settings are required together, and Run refuses zones with host
// networking, with an address family other than AddressIPv4, with OSDs that
// name no zone or an undeclared one, and with ClusterConfig.WithOSDCount
// (use WithInitialOSDs or WithNoInitialOSDs). Within one value a later call of
// the same method replaces the earlier one; Run refuses a field that two
// WithZoneConfig options both set.
type ZoneConfig struct {
	set         zoneField
	zones       []string
	quorum      string
	helperImage string
}

// WithZones declares the logical availability zones, at least two distinct
// CRUSH bucket names (letters, digits, underscores, dots or dashes). Each name
// becomes the CRUSH zone bucket of the OSDs that name it and the zone of the
// containers that Container.WithZone places there. The value keeps its own
// copy of names.
//
//purpose:construction
func (z ZoneConfig) WithZones(names ...string) ZoneConfig {
	z.set |= zoneNames
	z.zones = slices.Clone(names)
	return z
}

// WithQuorumZone selects the zone of every MON, and so of the quorum, of
// every MGR, of the control CLI and of the service daemons that are started
// without Container.WithZone. A partition then leaves the quorum on this side:
// the other zones' clients cannot authenticate, and their OSDs are marked down
// once enough of this zone's OSDs report them (see ZoneLayout.DownReporters).
//
//purpose:construction
func (z ZoneConfig) WithQuorumZone(name string) ZoneConfig {
	z.set |= zoneQuorum
	z.quorum = name
	return z
}

// WithHelperImage selects the image of the zone routers and of the route
// agents that join each member's network namespace with NET_ADMIN. It needs
// sleep, sh, cat and iproute2's ip; fault.NetemHelperImage qualifies. The
// helper is never selected implicitly, and the daemon images stay unchanged.
//
//purpose:construction
func (z ZoneConfig) WithHelperImage(image string) ZoneConfig {
	z.set |= zoneHelperImage
	z.helperImage = image
	return z
}

// WithZoneConfig is the grouped option function that carries a ZoneConfig to
// Run. Like every Option it changes the cluster settings only and never
// reaches a container request.
//
//purpose:construction
func WithZoneConfig(zones ZoneConfig) Option {
	return func(o *options) error { return zones.apply(o) }
}

// zoneSettings is the zone part of the cluster settings. A cluster without
// zones has the zero value.
type zoneSettings struct {
	names       []string
	quorum      string
	helperImage string
	set         zoneField
}

// enabled reports a cluster with zones.
func (z zoneSettings) enabled() bool { return len(z.names) > 0 }

// declares reports whether name is one of the zones.
func (z zoneSettings) declares(name string) bool { return slices.Contains(z.names, name) }

// apply validates the fields z set and, only if all are valid, writes them to
// the settings. It refuses a field that an earlier WithZoneConfig already set.
func (z ZoneConfig) apply(o *options) error {
	if clash := z.set & o.zones.set; clash != 0 {
		return fmt.Errorf("zone %s set by more than one WithZoneConfig", zoneFieldName(clash))
	}
	if z.set&zoneNames != 0 {
		if len(z.zones) < 2 {
			return fmt.Errorf("a regional cluster needs at least two zones, got %d", len(z.zones))
		}
		seen := map[string]bool{}
		for _, name := range z.zones {
			if err := validateZoneName(name); err != nil {
				return err
			}
			if seen[name] {
				return fmt.Errorf("zone %q is declared twice", name)
			}
			seen[name] = true
		}
	}
	if z.set&zoneQuorum != 0 {
		if err := validateZoneName(z.quorum); err != nil {
			return fmt.Errorf("quorum %w", err)
		}
	}
	if z.set&zoneHelperImage != 0 && strings.TrimSpace(z.helperImage) == "" {
		return fmt.Errorf("zone helper image must not be empty")
	}
	if z.set&zoneNames != 0 {
		o.zones.names = slices.Clone(z.zones)
	}
	if z.set&zoneQuorum != 0 {
		o.zones.quorum = z.quorum
	}
	if z.set&zoneHelperImage != 0 {
		o.zones.helperImage = z.helperImage
	}
	o.zones.set |= z.set
	return nil
}

func validateZoneName(name string) error {
	if name == "" || len(name) > 63 || !crushLocationName.MatchString(name) || crushDeviceName.MatchString(name) {
		return fmt.Errorf("zone name %q must be 1 to 63 letters, digits, underscores, dots or dashes and not an OSD name", name)
	}
	return nil
}

func zoneFieldName(f zoneField) string {
	switch {
	case f&zoneNames != 0:
		return "names"
	case f&zoneQuorum != 0:
		return "quorum zone"
	default:
		return "helper image"
	}
}

// checkOSDZone refuses a later OSD's zone that the cluster's zones do not
// admit: none in a cluster with zones, any in one without.
func (z zoneSettings) checkOSDZone(zone string) error {
	switch {
	case !z.enabled() && zone != "":
		return invalidRequest("OSD zone %q requires a cluster with WithZoneConfig", zone)
	case z.enabled() && !z.declares(zone):
		return invalidRequest("OSD zone %q is not one of the zones %v", zone, z.names)
	}
	return nil
}

// validateZones checks the zone settings against the rest of the assembled
// settings, before anything is created.
func validateZones(settings options) error {
	z := settings.zones
	if !z.enabled() {
		if z.set != 0 {
			return fmt.Errorf("ZoneConfig.WithQuorumZone and WithHelperImage require ZoneConfig.WithZones")
		}
		for i, osd := range settings.initialOSDs {
			if osd.Zone != "" {
				return fmt.Errorf("initial OSD %d names zone %q, which requires WithZoneConfig", i, osd.Zone)
			}
		}
		return nil
	}
	switch {
	case z.set&zoneQuorum == 0:
		return fmt.Errorf("a regional cluster needs ZoneConfig.WithQuorumZone")
	case !z.declares(z.quorum):
		return fmt.Errorf("quorum zone %q is not one of the zones %v", z.quorum, z.names)
	case z.set&zoneHelperImage == 0:
		return fmt.Errorf("a regional cluster needs ZoneConfig.WithHelperImage: the boundary helper is never selected implicitly")
	case settings.hostNetwork:
		return fmt.Errorf("zones require bridge mode: a host-network cluster has no endpoint to route")
	case settings.addressFamily != AddressIPv4:
		return fmt.Errorf("zones require NetworkConfig.WithAddressFamily(%s), got %s", AddressIPv4, settings.addressFamily)
	case !settings.noInitialOSDs && settings.osds > 0 && len(settings.initialOSDs) == 0:
		return fmt.Errorf("zones require ClusterConfig.WithInitialOSDs with OSDConfig.Zone (or WithNoInitialOSDs): every OSD belongs to a zone")
	}
	for i, osd := range settings.initialOSDs {
		if !z.declares(osd.Zone) {
			return fmt.Errorf("initial OSD %d is in zone %q, not one of the zones %v", i, osd.Zone, z.names)
		}
	}
	return nil
}
