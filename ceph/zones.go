package ceph

import (
	"context"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"strconv"
	"strings"

	"github.com/testcontainers/testcontainers-go"
)

// ZoneLayout is the zone topology of a cluster from WithZoneConfig, as the
// fixture holds it: the zones with their routers and members, the routes the
// boundary installed, the MON quorum's zone, Ceph's down-reporter policy and
// the containers on the cluster's bridges that belong to no zone.
type ZoneLayout struct {
	// QuorumZone holds every MON, MGR and the control CLI.
	QuorumZone string
	// Zones are sorted by name.
	Zones []Zone
	// Routes are the /32 routes in the members' network namespaces, sorted
	// by member and destination. A member and its peers in the same zone
	// have none between them.
	Routes []ZoneRoute
	// DownReporters is mon_osd_min_down_reporters and ReporterSubtree is
	// mon_osd_reporter_subtree_level: a MON marks an OSD down only after
	// failure reports from DownReporters distinct ReporterSubtree buckets.
	// When the boundary is cut, a zone's OSDs are marked down only if the
	// quorum zone's Zone.ReporterSubtrees reaches DownReporters.
	DownReporters   int
	ReporterSubtree string
	// Unzoned names the containers on the cluster's bridges that are neither
	// a router nor a member enrolled for its current start. They reach every
	// zone directly; fault.ApplyZoneProfile refuses to shape while there is one.
	Unzoned []string
}

// Zone is one logical availability zone of a ZoneLayout.
type Zone struct {
	Name string
	// Router forwards what leaves the zone.
	Router ZoneEndpoint
	// Members are sorted by name: mon.<name>, mgr.<name>, osd.<id>, control,
	// the service names of service daemons, and the container name of a
	// container placed with Container.WithZone.
	Members []ZoneEndpoint
	// ReporterSubtrees counts the distinct ReporterSubtree buckets (host by
	// default) of the zone's OSDs; zero when the level is not one of osd,
	// host, rack, zone or root.
	ReporterSubtrees int
}

// ZoneEndpoint is a router or a member on the cluster's bridges.
// ClusterAddress is set only for an endpoint on a separate replication
// bridge: an OSD or a router.
type ZoneEndpoint struct {
	Name, ContainerID             string
	PublicAddress, ClusterAddress netip.Addr
}

// ZoneRoute is one route the boundary installed: Member sends to Destination
// on Plane through Via, the address of its zone's router on that plane.
type ZoneRoute struct {
	Member           string
	Plane            NetworkPlane
	Destination, Via netip.Addr
}

// WithZone places a container in zone: Container.WithClient and
// ClientConfig.WithIdentity containers, which a cluster with zones refuses to
// start without it, and service daemons such as an rgw.Start gateway, which
// otherwise join the quorum zone. Pass it after the attachment option. On
// every start the container is enrolled as a member: a route agent with
// NET_ADMIN joins its network namespace and the boundary installs its routes
// and its peers' routes to it; when it stops they are removed. A start
// fails when its enrollment does. A cluster without zones refuses it, and so
// does an undeclared zone.
//
//purpose:construction,handoff
func (c *Container) WithZone(zone string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		if c.boundary == nil {
			return invalidRequest("Container.WithZone requires a cluster with WithZoneConfig")
		}
		if !c.settings.zones.declares(zone) {
			return invalidRequest("zone %q is not one of the zones %v", zone, c.settings.zones.names)
		}
		if err := testcontainers.WithLabels(map[string]string{zoneLabel: zone})(req); err != nil {
			return err
		}
		return c.zoneHooks()(req)
	}
}

// zoneMember labels a container that the cluster creates as member name of
// zone and enrolls it on every start. Without zones it changes nothing.
func (c *Container) zoneMember(zone, name string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		if c.boundary == nil {
			return nil
		}
		if err := testcontainers.WithLabels(map[string]string{zoneLabel: zone, zoneMemberLabel: name})(req); err != nil {
			return err
		}
		return c.zoneHooks()(req)
	}
}

// quorumMember is zoneMember in the quorum zone.
func (c *Container) quorumMember(name string) testcontainers.CustomizeRequestOption {
	return c.zoneMember(c.settings.zones.quorum, name)
}

// zoneAttachment enrolls a caller's client container on every start, whose
// zone WithZone labels; a start without the label fails. Without zones it
// changes nothing.
func (c *Container) zoneAttachment() testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		if c.boundary == nil {
			return nil
		}
		return c.zoneHooks()(req)
	}
}

// zoneHooks enrolls on start and withdraws before a stop and before the
// removal. A failed withdrawal does not fail the stop: the boundary keeps the
// error for Terminate, and the next start or close releases the rest.
func (c *Container) zoneHooks() testcontainers.CustomizeRequestOption {
	b := c.boundary
	withdraw := func(ctx context.Context, ctr testcontainers.Container) error {
		b.withdraw(ctx, ctr)
		return nil
	}
	return testcontainers.WithAdditionalLifecycleHooks(testcontainers.ContainerLifecycleHooks{
		PostStarts:    []testcontainers.ContainerHook{b.enroll},
		PreStops:      []testcontainers.ContainerHook{withdraw},
		PreTerminates: []testcontainers.ContainerHook{withdraw},
	})
}

// Zones returns the zone topology of a cluster from WithZoneConfig. It reads
// Ceph's down-reporter settings with the control CLI and inspects the
// cluster's bridges for containers without a zone. A cluster without zones
// is refused.
//
//purpose:observation,evidence
func (c *Container) Zones(ctx context.Context) (ZoneLayout, error) {
	if c.boundary == nil {
		return ZoneLayout{}, invalidRequest("the cluster has no zones: use WithZoneConfig")
	}
	if err := c.lockTopology(ctx); err != nil {
		return ZoneLayout{}, err
	}
	defer c.mu.Unlock()
	if c.closed {
		return ZoneLayout{}, errClusterTerminated
	}
	var placements []osdZonePlacement
	for _, osd := range c.fleet.osdHandles.each() {
		if !osd.purged {
			placements = append(placements, osdZonePlacement{id: osd.ID, placement: osd.Placement()})
		}
	}
	reporters, err := c.conn().ceph(ctx, "config", "get", "mon", "mon_osd_min_down_reporters")
	if err != nil {
		return ZoneLayout{}, err
	}
	subtree, err := c.conn().ceph(ctx, "config", "get", "mon", "mon_osd_reporter_subtree_level")
	if err != nil {
		return ZoneLayout{}, err
	}
	layout, err := c.boundary.layout(ctx)
	if err != nil {
		return ZoneLayout{}, err
	}
	layout.QuorumZone = c.settings.zones.quorum
	if layout.DownReporters, err = strconv.Atoi(strings.TrimSpace(string(reporters))); err != nil {
		return ZoneLayout{}, fmt.Errorf("parse mon_osd_min_down_reporters %q: %w", reporters, err)
	}
	layout.ReporterSubtree = strings.TrimSpace(string(subtree))
	for i := range layout.Zones {
		layout.Zones[i].ReporterSubtrees = reporterSubtrees(layout.Zones[i].Name, layout.ReporterSubtree, placements)
	}
	return layout, nil
}

type osdZonePlacement struct {
	id        int
	placement OSDConfig
}

// reporterSubtrees counts the distinct buckets of level among the OSDs of
// zone, or zero for a level the fixture does not place.
func reporterSubtrees(zone, level string, osds []osdZonePlacement) int {
	buckets := map[string]bool{}
	for _, osd := range osds {
		if osd.placement.Zone != zone {
			continue
		}
		switch level {
		case "osd":
			buckets[strconv.Itoa(osd.id)] = true
		case "host":
			buckets[osd.placement.Host] = true
		case "rack":
			buckets[osd.placement.Rack] = true
		case "zone":
			buckets[osd.placement.Zone] = true
		case "root":
			buckets[osd.placement.Root] = true
		default:
			return 0
		}
	}
	delete(buckets, "")
	return len(buckets)
}

// layout returns the zones, members and routes without the Ceph settings.
func (b *zoneBoundary) layout(ctx context.Context) (ZoneLayout, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ZoneLayout{}, errClusterTerminated
	}
	unzoned, err := b.unzonedLocked(ctx)
	if err != nil {
		return ZoneLayout{}, err
	}
	layout := ZoneLayout{Unzoned: unzoned}
	for _, name := range b.settings.names {
		zone := Zone{Name: name}
		if router := b.routers[name]; router != nil {
			zone.Router = ZoneEndpoint{Name: "router." + name, ContainerID: router.ctr.GetContainerID(),
				PublicAddress: router.addrs[PublicNetworkPlane], ClusterAddress: router.addrs[ClusterNetworkPlane]}
		}
		for _, member := range b.enrolled {
			if member.endpoint.zone == name && member.agent != nil {
				zone.Members = append(zone.Members, member.endpoint.view())
			}
		}
		slices.SortFunc(zone.Members, func(a, b ZoneEndpoint) int { return strings.Compare(a.Name, b.Name) })
		layout.Zones = append(layout.Zones, zone)
	}
	slices.SortFunc(layout.Zones, func(a, b Zone) int { return strings.Compare(a.Name, b.Name) })
	for _, member := range b.enrolled {
		for _, dest := range slices.SortedFunc(maps.Keys(member.routes), netip.Addr.Compare) {
			record := member.routes[dest]
			layout.Routes = append(layout.Routes, ZoneRoute{Member: member.endpoint.name, Plane: record.plane, Destination: dest, Via: record.via})
		}
	}
	slices.SortStableFunc(layout.Routes, func(a, b ZoneRoute) int { return strings.Compare(a.Member, b.Member) })
	return layout, nil
}

func (e zoneEndpoint) view() ZoneEndpoint {
	return ZoneEndpoint{Name: e.name, ContainerID: e.id, PublicAddress: e.addrs[PublicNetworkPlane], ClusterAddress: e.addrs[ClusterNetworkPlane]}
}
