//go:build integration && ci_optional && ci_batch_zone_boundary

//ci: timeout=40m job-timeout=55 image-roles=essential,client

package integration_test

import (
	"context"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	zoneRoot    = "tc-region"
	zonePool    = "tc-zone-boundary"
	zoneOneWay  = 20 * time.Millisecond
	zoneSamples = 20
	// zoneObjectSize is larger than any background traffic of the probe
	// phases, so the boundary's byte counters attribute the replication.
	zoneObjectSize = 4 << 20
)

// zonePath is one fixed probe: TCP connection setup from an endpoint to a
// Ceph-advertised address and port. Cross paths leave their zone.
type zonePath struct {
	Name, From string
	To         netip.AddrPort
	Cross      bool
}

// TestZoneBoundaryRegionalCluster places the daemons and clients of one
// cluster in two logical availability zones on one Docker engine and routes
// every cross-zone packet of both planes through a caller-owned boundary:
// one router per zone, attached to the public and the cluster bridge, and
// /32 routes in every endpoint's network namespace. Zone A holds the only MON
// (and so the quorum and the control CLI), the MGR, two OSDs and a client;
// zone B one OSD and a client. CRUSH places the pool's two replicas in
// distinct zone racks with min_size 1.
//
// It qualifies the boundary with fixed probes before shaping, with a one-way
// delay, with the delay in both directions, and after restoration; drives a
// write whose acting set spans the zones; partitions the boundary in both
// directions and observes reachability, PG state and both clients; restores it
// and reads the acknowledged data back; and releases the boundary, also after
// a partial setup with a failed route and a failed probe. The zones are labels
// on one engine: nothing here is physically independent.
func TestZoneBoundaryRegionalCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 32*time.Minute)
	defer cancel()
	layout := []ceph.OSDConfig{
		{Host: "zone-a-host-0", Rack: "zone-a", Root: zoneRoot},
		{Host: "zone-a-host-1", Rack: "zone-a", Root: zoneRoot},
		{Host: "zone-b-host-0", Rack: "zone-b", Root: zoneRoot},
	}
	cluster, clientA := harness.NewServiceCluster(t,
		ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithSeparateClusterNetwork()),
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithInitialOSDs(layout...).WithDefaultCRUSHRoot(zoneRoot).WithPoolDefaults(2, 1)),
		ceph.WithStartupTimeout(3*time.Minute))
	clientB, err := testcontainers.Run(ctx, harness.NativeClientImage(t, harness.EssentialImage(t)), cluster.WithClient(), ceph.WithIdleEntrypoint(),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"ceph", "--connect-timeout", "5", "status"})))
	if clientB != nil {
		testcontainers.CleanupContainer(t, clientB)
	}
	if err != nil {
		t.Fatal(err)
	}
	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: zonePool, PGNum: 1, Replicas: 2, MinSize: 1, Application: "rados", FailureDomain: "rack", CRUSHRoot: zoneRoot})
	if err != nil {
		t.Fatal(err)
	}
	observer := harness.ObserveCluster(cluster)
	if err := observer.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	facts, err := ceph.FactsOf(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if !facts.HasSeparateClusterNetwork || facts.UsesHostNetwork {
		t.Fatalf("cluster lacks separate public and cluster bridges: %+v", facts)
	}
	t.Logf("release %s, essential image %s, client image %s, public bridge %s, cluster bridge %s",
		facts.CephVersion, facts.Images.Essential, harness.NativeClientImage(t, harness.EssentialImage(t)), facts.NetworkName, facts.ClusterNetworkName)
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := docker.Close(); err != nil {
			t.Error(err)
		}
	})
	if version, err := docker.ServerVersion(ctx, mobycl.ServerVersionOptions{}); err == nil {
		t.Logf("engine: Docker %s, %s/%s", version.Version, version.Os, version.Arch)
	}
	planes := map[string]string{zonePublicPlane: facts.NetworkName, zoneClusterPlane: facts.ClusterNetworkName}
	helperImage := harness.NetemImage(t)
	b, err := newZoneBoundary(ctx, t, docker, helperImage, planes)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cleanupCancel()
		if err := b.close(cleanupCtx); err != nil {
			t.Errorf("release the zone boundary: %v", err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	// Step 1: membership, advertised addresses and routes.
	endpoints, zoneOfOSD := zoneMembership(t, ctx, cluster, b, clientA, clientB)
	byName := map[string]zoneEndpoint{}
	for _, ep := range endpoints {
		byName[ep.Name] = ep
		t.Logf("member %-10s zone %s container %.12s public %v cluster %v", ep.Name, ep.Zone, ep.ContainerID, ep.Addrs[zonePublicPlane], ep.Addrs[zoneClusterPlane])
	}
	control, err := cluster.ControlContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(endpoints, func(ep zoneEndpoint) bool { return ep.ContainerID == control.GetContainerID() && ep.Zone == "a" }) {
		t.Fatal("the control CLI does not run in a zone A endpoint")
	}
	checkZoneInventory(t, ctx, b, endpoints, "before the routers")
	advertised := checkZoneAdvertised(t, ctx, observer, byName)
	quorum, err := observer.QuorumStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	state, err := pool.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("quorum %v in zone A (single MON); pool %s size %d min_size %d, failure domain rack over zone-a and zone-b", quorum.QuorumNames, state.Name, state.Size, state.MinSize)
	if state.Size != 2 || state.MinSize != 1 {
		t.Fatalf("pool size/min_size are %d/%d, want 2/1", state.Size, state.MinSize)
	}

	egress := map[string]zoneRouterAddrs{}
	for _, zone := range []string{"a", "b"} {
		router, err := b.addRouter(ctx, "zone-"+zone+"-egress", zone)
		if err != nil {
			t.Fatal(err)
		}
		egress[zone] = zoneRouterAddrs{Name: router.Name, Addrs: router.Addrs}
		t.Logf("router %s carries what leaves zone %s: public %v on %s, cluster %v on %s, send_redirects 0, ip_forward 1",
			router.Name, zone, router.Addrs[zonePublicPlane], router.Ifaces[zonePublicPlane], router.Addrs[zoneClusterPlane], router.Ifaces[zoneClusterPlane])
	}
	checkZoneInventory(t, ctx, b, endpoints, "with the routers")
	for _, ep := range endpoints {
		agent, err := b.addAgent(ctx, ep)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("agent of %s: interfaces %v", ep.Name, agent.Ifaces)
	}
	routes, err := planZoneRoutes(endpoints, egress)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.installRoutes(ctx, routes); err != nil {
		t.Fatal(err)
	}
	t.Logf("installed %d routes", len(routes))
	checkZoneLookups(t, ctx, b, endpoints, egress)

	osdA0, osdA1, osdB := zoneOSDs(t, zoneOfOSD)
	paths := zoneProbePaths(t, advertised, osdA0, osdA1, osdB)

	// Step 2: baseline, one-way delay, both directions, restoration.
	base := zoneProbePhase(t, ctx, b, "baseline", paths, zoneSamples, 2*time.Second)
	for _, p := range paths {
		if err := zoneComplete(base[p.Name]); err != nil {
			t.Fatalf("baseline %s: %v", p.Name, err)
		}
	}
	profile := fault.NetworkProfile{HelperImage: helperImage, Delay: zoneOneWay}
	zoneShapeAll(t, ctx, b, []string{"a"}, profile)
	oneWay := zoneProbePhase(t, ctx, b, "zone A to B delayed", paths, zoneSamples, 2*time.Second)
	zoneLeaseStatus(t, ctx, b, "zone A to B delayed")
	zoneJudge(t, paths, base, oneWay, zoneOneWay)
	if err := b.restoreLeases(ctx); err != nil {
		t.Fatal(err)
	}
	zoneShapeAll(t, ctx, b, []string{"a", "b"}, profile)
	both := zoneProbePhase(t, ctx, b, "both directions delayed", paths, zoneSamples, 2*time.Second)
	zoneLeaseStatus(t, ctx, b, "both directions delayed")
	zoneJudge(t, paths, base, both, 2*zoneOneWay)

	// Step 3: a write across the zones while the boundary is shaped.
	first := zoneWriteObject(t, ctx, b, pool, clientA, "client-a", "zone-object-1", zoneObjectSize, zoneOfOSD)
	if err := b.restoreLeases(ctx); err != nil {
		t.Fatal(err)
	}
	restored := zoneProbePhase(t, ctx, b, "restored", paths, zoneSamples, 2*time.Second)
	for _, p := range paths {
		if err := checkZoneControl(base[p.Name], restored[p.Name]); err != nil {
			t.Errorf("restored %s: %v", p.Name, err)
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	// Step 3: a bidirectional partition, then recovery and read-back.
	second := zonePartition(t, ctx, b, observer, pool, clientA, clientB, paths, base, osdB, zoneOfOSD, helperImage)
	for _, obj := range []zoneObject{first, second} {
		for _, reader := range []struct {
			name string
			ctr  testcontainers.Container
		}{{"client-a", clientA}, {"client-b", clientB}} {
			got := zoneReadObject(t, ctx, reader.ctr, obj.Name)
			if got != obj.SHA256 {
				t.Fatalf("%s read %s with sha256 %s, want the acknowledged %s", reader.name, obj.Name, got, obj.SHA256)
			}
			t.Logf("%s read back %s: sha256 %s", reader.name, obj.Name, got)
		}
	}

	// Step 4: release, then a partial setup with failures and its release.
	if err := b.close(ctx); err != nil {
		t.Fatalf("release the zone boundary: %v", err)
	}
	t.Log("released the boundary: leases restored, routes removed, agents, routers and the management bridge removed, no labelled leftovers")
	zonePartialTeardown(t, ctx, docker, helperImage, planes, byName, osdA0, osdB, paths, base)
	if err := observer.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
}

// zoneMembership lists every Ceph endpoint of the cluster with its zone:
// MONs, MGRs and the clients by role, OSDs by their CRUSH rack.
func zoneMembership(t *testing.T, ctx context.Context, cluster *ceph.Container, b *zoneBoundary, clientA, clientB testcontainers.Container) ([]zoneEndpoint, map[int]string) {
	t.Helper()
	var endpoints []zoneEndpoint
	add := func(name, zone string, ctr testcontainers.Container) {
		t.Helper()
		addrs, err := b.endpointAddrs(ctx, ctr.GetContainerID())
		if err != nil {
			t.Fatal(err)
		}
		endpoints = append(endpoints, zoneEndpoint{Name: name, Zone: zone, ContainerID: ctr.GetContainerID(), Addrs: addrs})
	}
	mons, err := cluster.Monitors(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, mon := range mons {
		add("mon."+mon.DaemonName, "a", mon.Container)
	}
	mgrs, err := cluster.Managers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, mgr := range mgrs {
		add("mgr."+mgr.DaemonName, "a", mgr.Container)
	}
	osds, err := cluster.OSDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	zoneOfOSD := map[int]string{}
	for _, osd := range osds {
		zone, found := strings.CutPrefix(osd.Placement().Rack, "zone-")
		if !found {
			t.Fatalf("osd.%d is in rack %q, not a zone rack", osd.ID, osd.Placement().Rack)
		}
		zoneOfOSD[osd.ID] = zone
		add(fmt.Sprintf("osd.%d", osd.ID), zone, osd.Container)
	}
	add("client-a", "a", clientA)
	add("client-b", "b", clientB)
	return endpoints, zoneOfOSD
}

// checkZoneInventory fails when a plane has an endpoint outside the
// membership and the boundary's routers: such a container would reach the
// other zone directly over the shared bridge.
func checkZoneInventory(t *testing.T, ctx context.Context, b *zoneBoundary, endpoints []zoneEndpoint, stage string) {
	t.Helper()
	known := map[string]string{}
	for _, ep := range endpoints {
		known[ep.ContainerID] = ep.Name
	}
	for _, router := range b.routers {
		known[router.ctr.GetContainerID()] = "router " + router.Name
	}
	for _, plane := range zoneSortedKeys(b.planes) {
		inspection, err := b.docker.NetworkInspect(ctx, b.planes[plane], mobycl.NetworkInspectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for id, resource := range inspection.Network.Containers {
			name, found := known[id]
			if !found {
				t.Fatalf("%s: %s plane endpoint %s (%s, %v) belongs to no zone", stage, plane, resource.Name, id, resource.IPv4Address)
			}
			names = append(names, name)
		}
		slices.Sort(names)
		var subnets []string
		for _, config := range inspection.Network.IPAM.Config {
			subnets = append(subnets, config.Subnet.String())
		}
		t.Logf("%s: %s plane %s %v endpoints %v", stage, plane, inspection.Network.Name, subnets, names)
	}
}

// checkZoneAdvertised compares every address the MON, MGR and OSD maps
// advertise with the endpoint's address on its plane, and returns the maps.
func checkZoneAdvertised(t *testing.T, ctx context.Context, observer *ceph.Observer, byName map[string]zoneEndpoint) ceph.DaemonAddresses {
	t.Helper()
	native, err := observer.DaemonAddresses(ctx)
	if err != nil {
		t.Fatal(err)
	}
	same := func(what, name, plane string, vector []ceph.EntityAddress) {
		t.Helper()
		ep, found := byName[name]
		if !found || len(vector) == 0 {
			t.Fatalf("%s: no member %s or no advertised address %v", what, name, vector)
		}
		for _, entry := range vector {
			addr, err := netip.ParseAddrPort(entry.Addr)
			if err != nil {
				t.Fatal(err)
			}
			if addr.Addr() != ep.Addrs[plane] {
				t.Fatalf("%s advertises %s, not %s's %s address %v", what, entry.Addr, name, plane, ep.Addrs[plane])
			}
		}
		t.Logf("advertised %-28s zone %s %v", what, ep.Zone, vector)
	}
	for _, mon := range native.Monitors {
		same("mon."+mon.Name, "mon."+mon.Name, zonePublicPlane, mon.Addrs)
	}
	if native.ActiveManager == nil {
		t.Fatal("no active MGR")
	}
	same("mgr."+native.ActiveManager.Name, "mgr."+native.ActiveManager.Name, zonePublicPlane, native.ActiveManager.Addrs)
	for _, osd := range native.OSDs {
		name := fmt.Sprintf("osd.%d", osd.ID)
		same(name+" public", name, zonePublicPlane, osd.Public)
		same(name+" heartbeat front", name, zonePublicPlane, osd.HeartbeatFront)
		same(name+" cluster", name, zoneClusterPlane, osd.Cluster)
		same(name+" heartbeat back", name, zoneClusterPlane, osd.HeartbeatBack)
	}
	return native
}

// checkZoneLookups asks every endpoint's kernel how it reaches every other
// endpoint on each shared plane: through its zone's router across the
// boundary, directly within the zone.
func checkZoneLookups(t *testing.T, ctx context.Context, b *zoneBoundary, endpoints []zoneEndpoint, egress map[string]zoneRouterAddrs) {
	t.Helper()
	count := 0
	for _, ep := range endpoints {
		agent := b.agents[ep.Name]
		for _, peer := range endpoints {
			if peer.Name == ep.Name {
				continue
			}
			for _, plane := range zoneSortedPlanes(ep.Addrs) {
				dest, shared := peer.Addrs[plane]
				if !shared {
					continue
				}
				var via netip.Addr
				if peer.Zone != ep.Zone {
					via = egress[ep.Zone].Addrs[plane]
				}
				got, err := b.routeLookup(ctx, ep.Name, dest)
				if err == nil {
					err = checkRouteLookup(got, dest, via, ep.Addrs[plane], agent.Ifaces[plane])
				}
				if err != nil {
					t.Fatalf("%s to %s on the %s plane: %v", ep.Name, peer.Name, plane, err)
				}
				count++
			}
		}
	}
	t.Logf("checked %d route lookups: cross-zone through the egress router of the sender's zone, same-zone on-link", count)
}

func zoneOSDs(t *testing.T, zoneOfOSD map[int]string) (int, int, int) {
	t.Helper()
	var a, bz []int
	for id, zone := range zoneOfOSD {
		if zone == "a" {
			a = append(a, id)
		} else {
			bz = append(bz, id)
		}
	}
	slices.Sort(a)
	if len(a) != 2 || len(bz) != 1 {
		t.Fatalf("zones hold OSDs %v and %v, want two and one", a, bz)
	}
	return a[0], a[1], bz[0]
}

// zoneProbePaths fixes the probes: client and daemon paths across the
// boundary in both directions on both planes, and same-zone controls.
func zoneProbePaths(t *testing.T, native ceph.DaemonAddresses, osdA0, osdA1, osdB int) []zonePath {
	t.Helper()
	osd := func(id int) ceph.OSDAddress {
		for _, entry := range native.OSDs {
			if entry.ID == id {
				return entry
			}
		}
		t.Fatalf("osd.%d is not in the OSD map", id)
		return ceph.OSDAddress{}
	}
	port := func(vector []ceph.EntityAddress) netip.AddrPort {
		t.Helper()
		addr, err := entityAddrPort(vector, "v2")
		if err != nil {
			t.Fatal(err)
		}
		return addr
	}
	if len(native.Monitors) != 1 {
		t.Fatalf("%d MONs, want 1", len(native.Monitors))
	}
	mon := port(native.Monitors[0].Addrs)
	a0, a1, b := osd(osdA0), osd(osdA1), osd(osdB)
	name := func(id int) string { return fmt.Sprintf("osd.%d", id) }
	return []zonePath{
		{Name: "client-a to " + name(osdB) + " public", From: "client-a", To: port(b.Public), Cross: true},
		{Name: "client-b to " + name(osdA0) + " public", From: "client-b", To: port(a0.Public), Cross: true},
		{Name: name(osdB) + " to mon.a", From: name(osdB), To: mon, Cross: true},
		{Name: name(osdA0) + " to " + name(osdB) + " cluster", From: name(osdA0), To: port(b.Cluster), Cross: true},
		{Name: name(osdB) + " to " + name(osdA0) + " cluster", From: name(osdB), To: port(a0.Cluster), Cross: true},
		{Name: "client-a to " + name(osdA0) + " public", From: "client-a", To: port(a0.Public)},
		{Name: name(osdA0) + " to " + name(osdA1) + " cluster", From: name(osdA0), To: port(a1.Cluster)},
		{Name: "client-b to " + name(osdB) + " public", From: "client-b", To: port(b.Public)},
	}
}

// zoneProbePhase probes every path and logs the summaries and the routers'
// counter deltas over the phase.
func zoneProbePhase(t *testing.T, ctx context.Context, b *zoneBoundary, phase string, paths []zonePath, count int, timeout time.Duration) map[string]zoneRTT {
	t.Helper()
	before, err := b.allCounters(ctx)
	if err != nil {
		t.Fatal(err)
	}
	results := map[string]zoneRTT{}
	for _, p := range paths {
		r, err := b.probe(ctx, p.From, p.To, count, timeout)
		if err != nil {
			t.Fatalf("%s %s: %v", phase, p.Name, err)
		}
		s := summarizeZoneRTT(r)
		results[p.Name] = s
		kind := "same-zone"
		if p.Cross {
			kind = "cross-zone"
		}
		detail := ""
		if len(r.Errors) > 0 {
			detail = fmt.Sprintf(" errors %q", r.Errors[0])
		}
		t.Logf("RTT %-24s %-10s %-28s %s %s%s", phase, kind, p.Name, p.To, s, detail)
	}
	zoneLogCounters(t, ctx, b, phase, before)
	return results
}

func zoneLogCounters(t *testing.T, ctx context.Context, b *zoneBoundary, phase string, before map[string]zoneCounters) {
	t.Helper()
	after, err := b.allCounters(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range zoneSortedKeys(after) {
		zone, plane, _ := strings.Cut(key, "/")
		router := b.routers[zone]
		d := after[key].delta(before[key])
		t.Logf("counters %-24s %s %s (%s): rx %d packets, tx %d packets %d bytes", phase, router.Name, plane, router.Ifaces[plane], d.RxPackets, d.TxPackets, d.TxBytes)
	}
}

// zoneShapeAll applies profile on every plane of the routers of zones.
func zoneShapeAll(t *testing.T, ctx context.Context, b *zoneBoundary, zones []string, profile fault.NetworkProfile) {
	t.Helper()
	for _, zone := range zones {
		for _, plane := range zoneSortedKeys(b.planes) {
			lease, err := b.shape(ctx, zone, plane, profile)
			if err != nil {
				t.Fatalf("shape what leaves zone %s on the %s plane: %v", zone, plane, err)
			}
			t.Logf("profile on %s %s (%s): delay %v loss %v", b.routers[zone].Name, plane, lease.Interface, lease.Delay, lease.Loss)
		}
	}
}

// zoneLeaseStatus logs the kernel's view of every held lease: the netem
// qdisc's configuration and the packets it shaped, which are the packets that
// left one zone on one plane.
func zoneLeaseStatus(t *testing.T, ctx context.Context, b *zoneBoundary, phase string) {
	t.Helper()
	for _, lease := range b.leases {
		status, err := lease.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !status.Applied || status.Delay != lease.Delay || status.Loss != lease.Loss {
			t.Fatalf("%s: lease on %s is not applied as requested: %+v", phase, lease.ContainerID, status)
		}
		t.Logf("netem %-24s container %.12s %s: %s root %s delay %v loss %v, %d packets %d bytes %d drops",
			phase, lease.ContainerID, lease.NetworkName, status.Interface, status.RootKind, status.Delay, status.Loss, status.Packets, status.Bytes, status.Drops)
	}
}

// zoneJudge compares a shaped phase with the baseline: cross-zone paths gain
// added once, same-zone paths keep their baseline.
func zoneJudge(t *testing.T, paths []zonePath, base, shaped map[string]zoneRTT, added time.Duration) {
	t.Helper()
	for _, p := range paths {
		var err error
		if p.Cross {
			err = checkZoneShaped(base[p.Name], shaped[p.Name], added)
		} else {
			err = checkZoneControl(base[p.Name], shaped[p.Name])
		}
		if err != nil {
			t.Errorf("%s: %v", p.Name, err)
		}
	}
	if t.Failed() {
		t.FailNow()
	}
}

type zoneObject struct {
	Name, SHA256 string
}

// zoneWriteObject writes size random bytes from a client and checks that
// the object's PG spans the zones and that the replication crossed the
// boundary: the cluster-plane router of the primary's zone sent at least the
// object's size while the write ran.
func zoneWriteObject(t *testing.T, ctx context.Context, b *zoneBoundary, pool *ceph.Pool, client testcontainers.Container, clientName, name string, size int, zoneOfOSD map[int]string) zoneObject {
	t.Helper()
	pg := zoneSinglePG(t, ctx, pool)
	zones, err := actingZones(pg.Acting, zoneOfOSD)
	if err != nil {
		t.Fatal(err)
	}
	if len(zones["a"]) != 1 || len(zones["b"]) != 1 {
		t.Fatalf("PG %s acting set %v does not span the zones: %v", pg.PGID, pg.Acting, zones)
	}
	primaryZone := zoneOfOSD[pg.ActingPrimary]
	path := "/tmp/" + name
	sum := harness.ExecOutput(t, ctx, client, "sh", "-c", fmt.Sprintf("head -c %d /dev/urandom > %s && sha256sum %s", size, path, path))
	digest := strings.Fields(sum)[0]
	before, err := b.allCounters(ctx)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	code, out, err := zoneExec(ctx, client, "rados", "-p", zonePool, "put", name, path)
	elapsed := time.Since(start)
	if err != nil || code != 0 {
		t.Fatalf("%s write %s: exit %d %q %v", clientName, name, code, out, err)
	}
	after, err := b.allCounters(ctx)
	if err != nil {
		t.Fatal(err)
	}
	replication := after[primaryZone+"/"+zoneClusterPlane].delta(before[primaryZone+"/"+zoneClusterPlane])
	t.Logf("%s wrote %s (%d bytes, sha256 %s) in %v to PG %s acting %v primary osd.%d (zone %s); %s cluster plane sent %d bytes in %d packets",
		clientName, name, size, digest, elapsed.Round(time.Millisecond), pg.PGID, pg.Acting, pg.ActingPrimary, primaryZone,
		b.routers[primaryZone].Name, replication.TxBytes, replication.TxPackets)
	zoneLogCounters(t, ctx, b, "write "+name, before)
	if replication.TxBytes < uint64(size) {
		t.Fatalf("the replication of %s did not cross the boundary: %d bytes on %s's cluster plane", name, replication.TxBytes, b.routers[primaryZone].Name)
	}
	if primaryZone != "a" {
		ingress := after["a/"+zonePublicPlane].delta(before["a/"+zonePublicPlane])
		if ingress.TxBytes < uint64(size) {
			t.Fatalf("the client write to a zone B primary did not cross the public boundary: %d bytes", ingress.TxBytes)
		}
	}
	return zoneObject{Name: name, SHA256: digest}
}

func zoneSinglePG(t *testing.T, ctx context.Context, pool *ceph.Pool) ceph.PGState {
	t.Helper()
	snapshot, err := pool.PGs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.PGs) != 1 {
		t.Fatalf("pool has %d PGs, want 1", len(snapshot.PGs))
	}
	return snapshot.PGs[0]
}

func zoneReadObject(t *testing.T, ctx context.Context, client testcontainers.Container, name string) string {
	t.Helper()
	path := "/tmp/readback-" + name
	out := harness.ExecOutput(t, ctx, client, "sh", "-c", fmt.Sprintf("rados -p %s get %s %s && sha256sum %s && rm -f %s", zonePool, name, path, path, path))
	return strings.Fields(out)[0]
}

// zonePartition drops every packet that crosses the boundary, in both
// directions on both planes, and observes same-zone reachability, the zone B
// OSD being reported down by the zone A OSDs, the PG serving from zone A, a
// write from zone A and the zone B client cut off from the only MON. It then
// restores the boundary and waits for recovery. It returns the object
// written under the partition.
func zonePartition(t *testing.T, ctx context.Context, b *zoneBoundary, observer *ceph.Observer, pool *ceph.Pool, clientA, clientB testcontainers.Container,
	paths []zonePath, base map[string]zoneRTT, osdB int, zoneOfOSD map[int]string, helperImage string,
) zoneObject {
	t.Helper()
	requirePartitionStat(t, ctx, clientB, zonePool, "zone-object-1", zoneObjectSize, false)
	zoneShapeAll(t, ctx, b, []string{"a", "b"}, fault.NetworkProfile{HelperImage: helperImage, Loss: 100})
	cutAt := time.Now()
	zoneLeaseStatus(t, ctx, b, "partitioned")
	for _, p := range paths {
		count, timeout := zoneSamples, 2*time.Second
		if p.Cross {
			count, timeout = 5, time.Second
		}
		r, err := b.probe(ctx, p.From, p.To, count, timeout)
		if err != nil {
			t.Fatal(err)
		}
		s := summarizeZoneRTT(r)
		if p.Cross {
			err = checkZoneCut(s)
		} else {
			err = checkZoneControl(base[p.Name], s)
		}
		t.Logf("RTT %-24s %-28s %s", "partitioned", p.Name, s)
		if err != nil {
			t.Fatalf("partitioned %s: %v", p.Name, err)
		}
	}
	waitFor := func(what string, bound time.Duration, done func() (bool, string)) time.Duration {
		t.Helper()
		deadline := time.Now().Add(bound)
		for {
			ok, detail := done()
			if ok {
				elapsed := time.Since(cutAt)
				t.Logf("%s after %v: %s", what, elapsed.Round(time.Second), detail)
				return elapsed
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s not observed within %v: %s", what, bound, detail)
			}
			time.Sleep(2 * time.Second)
		}
	}
	osdUp := func(want bool) func() (bool, string) {
		return func() (bool, string) {
			states, err := observer.OSDStates(ctx)
			if err != nil {
				return false, err.Error()
			}
			for _, s := range states {
				if s.ID == osdB {
					return s.Up == want, fmt.Sprintf("%+v", states)
				}
			}
			return false, fmt.Sprintf("osd.%d missing: %+v", osdB, states)
		}
	}
	waitFor(fmt.Sprintf("osd.%d marked down", osdB), 3*time.Minute, osdUp(false))
	var served ceph.PGState
	waitFor("PG served by zone A alone", 3*time.Minute, func() (bool, string) {
		snapshot, err := pool.PGs(ctx)
		if err != nil || len(snapshot.PGs) != 1 {
			return false, fmt.Sprintf("%v %+v", err, snapshot.PGs)
		}
		served = snapshot.PGs[0]
		zones, err := actingZones(served.Acting, zoneOfOSD)
		ok := err == nil && len(zones["b"]) == 0 && len(zones["a"]) == 1 && strings.Contains(served.State, "active") && strings.Contains(served.State, "undersized")
		return ok, fmt.Sprintf("PG %s state %s acting %v primary osd.%d", served.PGID, served.State, served.Acting, served.ActingPrimary)
	})
	second := zoneWriteDuringPartition(t, ctx, pool, clientA)
	requirePartitionStat(t, ctx, clientB, zonePool, "zone-object-1", zoneObjectSize, true)
	if err := b.restoreLeases(ctx); err != nil {
		t.Fatal(err)
	}
	restoredAt := time.Now()
	t.Logf("restored the boundary %v after the cut", restoredAt.Sub(cutAt).Round(time.Second))
	waitFor(fmt.Sprintf("osd.%d up again", osdB), 5*time.Minute, osdUp(true))
	recoverCtx, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()
	if err := observer.WaitForClean(recoverCtx); err != nil {
		t.Fatalf("recovery after the partition: %v", err)
	}
	requirePartitionStat(t, ctx, clientB, zonePool, "zone-object-1", zoneObjectSize, false)
	pg := zoneSinglePG(t, ctx, pool)
	zones, err := actingZones(pg.Acting, zoneOfOSD)
	if err != nil || len(zones["a"]) != 1 || len(zones["b"]) != 1 {
		t.Fatalf("after recovery PG %s acting %v does not span the zones: %v %v", pg.PGID, pg.Acting, zones, err)
	}
	t.Logf("recovered %v after the restoration: PG %s state %s acting %v primary osd.%d", time.Since(restoredAt).Round(time.Second), pg.PGID, pg.State, pg.Acting, pg.ActingPrimary)
	return second
}

// zoneWriteDuringPartition writes an object from zone A while the boundary is
// cut; with min_size 1 the zone A replica acknowledges it.
func zoneWriteDuringPartition(t *testing.T, ctx context.Context, pool *ceph.Pool, clientA testcontainers.Container) zoneObject {
	t.Helper()
	name := "zone-object-2"
	path := "/tmp/" + name
	sum := harness.ExecOutput(t, ctx, clientA, "sh", "-c", fmt.Sprintf("head -c %d /dev/urandom > %s && sha256sum %s", 1<<20, path, path))
	start := time.Now()
	code, out, err := zoneExec(ctx, clientA, "rados", "--rados-osd-op-timeout", "90", "-p", zonePool, "put", name, path)
	if err != nil || code != 0 {
		t.Fatalf("client-a write during the partition: exit %d %q %v", code, out, err)
	}
	t.Logf("client-a wrote %s during the partition in %v: acknowledged", name, time.Since(start).Round(time.Millisecond))
	return zoneObject{Name: name, SHA256: strings.Fields(sum)[0]}
}

// zonePartialTeardown first checks that the released boundary left the
// direct paths, then builds a second boundary only partly: one router, three
// agents and the routes of one client; a route through an off-link gateway
// fails and a probe to an unused address fails. Releasing it must leave
// nothing behind either.
func zonePartialTeardown(t *testing.T, ctx context.Context, docker *testcontainers.DockerClient, helperImage string, planes map[string]string,
	byName map[string]zoneEndpoint, osdA0, osdB int, paths []zonePath, base map[string]zoneRTT,
) {
	t.Helper()
	partial, err := newZoneBoundary(ctx, t, docker, helperImage, planes)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cleanupCancel()
		if err := partial.close(cleanupCtx); err != nil {
			t.Errorf("release the partial boundary: %v", err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	router, err := partial.addRouter(ctx, "zone-a-egress", "a")
	if err != nil {
		t.Fatal(err)
	}
	a0, b0 := byName[fmt.Sprintf("osd.%d", osdA0)], byName[fmt.Sprintf("osd.%d", osdB)]
	for _, ep := range []zoneEndpoint{byName["client-a"], a0, b0} {
		if _, err := partial.addAgent(ctx, ep); err != nil {
			t.Fatal(err)
		}
	}
	for _, check := range []struct {
		from string
		peer zoneEndpoint
	}{{"client-a", b0}, {a0.Name, b0}, {b0.Name, a0}} {
		from := byName[check.from]
		for _, plane := range zoneSortedPlanes(from.Addrs) {
			dest, shared := check.peer.Addrs[plane]
			if !shared {
				continue
			}
			got, err := partial.routeLookup(ctx, from.Name, dest)
			if err == nil {
				err = checkRouteLookup(got, dest, netip.Addr{}, from.Addrs[plane], partial.agents[from.Name].Ifaces[plane])
			}
			if err != nil {
				t.Fatalf("after the release %s to %s on the %s plane is not direct: %v", from.Name, check.peer.Name, plane, err)
			}
		}
	}
	for _, p := range paths {
		if p.From != "client-a" || !p.Cross {
			continue
		}
		r, err := partial.probe(ctx, p.From, p.To, zoneSamples, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		s := summarizeZoneRTT(r)
		t.Logf("RTT %-24s %-28s %s", "direct after release", p.Name, s)
		if err := checkZoneControl(base[p.Name], s); err != nil {
			t.Fatalf("direct %s after the release: %v", p.Name, err)
		}
	}
	routes := []zoneRoute{{Endpoint: "client-a", Plane: zonePublicPlane, Router: router.Name, Dest: b0.Addrs[zonePublicPlane], Via: router.Addrs[zonePublicPlane]}}
	if err := partial.installRoutes(ctx, routes); err != nil {
		t.Fatal(err)
	}
	offLink := netip.MustParseAddr("192.0.2.254")
	err = partial.installRoutes(ctx, []zoneRoute{{Endpoint: a0.Name, Plane: zonePublicPlane, Router: "off-link", Dest: b0.Addrs[zonePublicPlane], Via: offLink}})
	t.Logf("partial setup: a route through an off-link gateway failed: %v", err)
	if err == nil {
		t.Fatal("a route through an off-link gateway was installed")
	}
	r, err := partial.probe(ctx, "client-a", netip.MustParseAddrPort("192.0.2.1:9"), 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("partial setup: a probe to an unused address failed: %v", r.Errors)
	if len(r.Samples) != 0 {
		t.Fatal("a probe to an unused address completed")
	}
	if err := partial.close(ctx); err != nil {
		t.Fatalf("release the partial boundary: %v", err)
	}
	t.Log("released the partial boundary: 2 recorded routes removed or absent, agents, router and management bridge removed, no labelled leftovers")
}

// TestZoneBoundaryTwoSubnetRouting records whether this engine forwards
// through a container router between two Docker bridges, which a zone
// topology with one subnet per zone needs. Each endpoint has an address on a
// shared bridge and on its zone bridge; the router is attached to both zone
// bridges. It probes the peer's zone address through the router, and the
// peer's shared-bridge address through the router (the path a Ceph address on
// the cluster's bridge would take), once for each zone bridge gateway mode.
// Docker 28 and later drop packets addressed to a container on another bridge
// in nat mode, before they reach the router when the host's bridge netfilter
// is enabled; the outcome is recorded, not asserted. The direct path and the
// router's own reachability are asserted as controls. A mode the engine
// refuses as unknown is recorded as unavailable and its subtest is skipped
// with no probe executed; any other failure to create a bridge fails.
func TestZoneBoundaryTwoSubnetRouting(t *testing.T) {
	image := harness.NetemImage(t)
	docker, err := testcontainers.NewDockerClientWithOpts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := docker.Close(); err != nil {
			t.Error(err)
		}
	})
	if version, err := docker.ServerVersion(t.Context(), mobycl.ServerVersionOptions{}); err == nil {
		t.Logf("engine: Docker %s, %s/%s", version.Version, version.Os, version.Arch)
	}
	for _, mode := range []string{"nat", "nat-unprotected", "routed"} {
		t.Run(mode, func(t *testing.T) {
			zoneTwoSubnets(t, docker, image, mode)
		})
	}
}

// zoneBridge is a bridge that the two-subnet probe created itself.
type zoneBridge struct{ ID, Name string }

func zoneTwoSubnets(t *testing.T, docker *testcontainers.DockerClient, image, mode string) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	labels := map[string]string{zoneBoundaryLabel: "two-subnet-" + strings.ReplaceAll(mode, "-", "") + "-" + fmt.Sprint(time.Now().UnixNano())}
	// testcontainers' network.New drops driver options, so the bridges are
	// created through the engine API with testcontainers' labels for Ryuk.
	newBridge := func(gatewayMode string) (*zoneBridge, error) {
		t.Helper()
		all := testcontainers.GenericLabels()
		for k, v := range labels {
			all[k] = v
		}
		options := map[string]string{}
		if gatewayMode != "" {
			options["com.docker.network.bridge.gateway_mode_ipv4"] = gatewayMode
		}
		name := "zone-two-subnet-" + uuid.NewString()
		created, err := docker.NetworkCreate(ctx, name, mobycl.NetworkCreateOptions{Driver: "bridge", Options: options, Labels: all})
		if err != nil {
			return nil, err
		}
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if _, err := docker.NetworkRemove(cleanupCtx, created.ID, mobycl.NetworkRemoveOptions{}); err != nil {
				t.Errorf("remove bridge %s: %v", name, err)
			}
		})
		inspection, err := docker.NetworkInspect(ctx, created.ID, mobycl.NetworkInspectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if got := inspection.Network.Options["com.docker.network.bridge.gateway_mode_ipv4"]; got != gatewayMode {
			t.Fatalf("bridge %s has gateway mode %q, want %q", name, got, gatewayMode)
		}
		return &zoneBridge{ID: created.ID, Name: name}, nil
	}
	shared, err := newBridge("")
	if err != nil {
		t.Fatalf("create the shared bridge: %v", err)
	}
	zoneBridgeFor := func(zone string) *zoneBridge {
		t.Helper()
		bridge, err := newBridge(mode)
		switch {
		case zoneGatewayModeUnavailable(mode, err):
			t.Skipf("two-subnet record: %s", zoneTwoSubnetOutcome{Mode: mode, Unavailable: err})
		case err != nil:
			t.Fatalf("create the zone %s bridge in gateway mode %s: %v", zone, mode, err)
		}
		return bridge
	}
	zoneA, zoneB := zoneBridgeFor("A"), zoneBridgeFor("B")
	start := func(name string, on *zoneBridge, host func(*container.HostConfig)) testcontainers.Container {
		t.Helper()
		ctr, err := testcontainers.Run(ctx, image, network.WithNetworkName([]string{name}, on.Name), ceph.WithIdleEntrypoint(),
			testcontainers.WithLabels(labels), testcontainers.WithConfigModifier(func(config *container.Config) { config.User = "0:0" }),
			testcontainers.WithHostConfigModifier(host))
		if ctr != nil {
			testcontainers.CleanupContainer(t, ctr, testcontainers.StopTimeout(0))
		}
		if err != nil {
			t.Fatal(err)
		}
		return ctr
	}
	endpointCaps := func(host *container.HostConfig) { host.CapDrop, host.CapAdd = []string{"ALL"}, []string{"NET_ADMIN"} }
	ea, eb := start("ea", shared, endpointCaps), start("eb", shared, endpointCaps)
	router := start("router", zoneA, func(host *container.HostConfig) {
		host.CapDrop = []string{"ALL"}
		host.Sysctls = map[string]string{"net.ipv4.ip_forward": "1"}
	})
	connect := func(bridge *zoneBridge, ctr testcontainers.Container) {
		t.Helper()
		if _, err := docker.NetworkConnect(ctx, bridge.ID, mobycl.NetworkConnectOptions{Container: ctr.GetContainerID()}); err != nil {
			t.Fatal(err)
		}
	}
	connect(zoneA, ea)
	connect(zoneB, eb)
	connect(zoneB, router)
	addr := func(ctr testcontainers.Container, bridge *zoneBridge) netip.Addr {
		t.Helper()
		inspection, err := docker.ContainerInspect(ctx, ctr.GetContainerID(), mobycl.ContainerInspectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		endpoint := inspection.Container.NetworkSettings.Networks[bridge.Name]
		if endpoint == nil || !endpoint.IPAddress.IsValid() {
			t.Fatalf("%s has no address on %s", ctr.GetContainerID(), bridge.Name)
		}
		return endpoint.IPAddress
	}
	subnet := func(bridge *zoneBridge) netip.Prefix {
		t.Helper()
		inspection, err := docker.NetworkInspect(ctx, bridge.ID, mobycl.NetworkInspectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(inspection.Network.IPAM.Config) == 0 {
			t.Fatalf("%s has no IPAM configuration", bridge.Name)
		}
		t.Logf("bridge %s options %v subnet %v", bridge.Name, inspection.Network.Options, inspection.Network.IPAM.Config[0].Subnet)
		return inspection.Network.IPAM.Config[0].Subnet
	}
	eaShared, ebShared, eaZone, ebZone := addr(ea, shared), addr(eb, shared), addr(ea, zoneA), addr(eb, zoneB)
	routerA, routerB := addr(router, zoneA), addr(router, zoneB)
	listen := `import socket
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); s.bind(("0.0.0.0", 7000)); s.listen(64)
while True:
    c, _ = s.accept(); c.close()
`
	for _, ctr := range []testcontainers.Container{eb, router} {
		harness.ExecOutput(t, ctx, ctr, "sh", "-c", `nohup python3 -c "$0" >/dev/null 2>&1 &`, listen)
	}
	probe := func(from testcontainers.Container, to netip.Addr) zoneRTT {
		t.Helper()
		out, err := zoneExecOK(ctx, from, "python3", "-c", zoneProbeScript, to.String(), "7000", "5", "1", "0.05")
		if err != nil {
			t.Fatal(err)
		}
		r, err := parseZoneProbe(5, out)
		if err != nil {
			t.Fatal(err)
		}
		return summarizeZoneRTT(r)
	}
	// control waits for a listener that was just started in the background.
	control := func(what string, from testcontainers.Container, to netip.Addr) {
		t.Helper()
		var s zoneRTT
		for range 10 {
			if s = probe(from, to); zoneComplete(s) == nil {
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatalf("%s control: %s", what, s)
	}
	control("direct shared-bridge", ea, ebShared)
	control("router on zone A", ea, routerA)
	harness.ExecOutput(t, ctx, ea, "ip", "route", "add", subnet(zoneB).String(), "via", routerA.String())
	harness.ExecOutput(t, ctx, eb, "ip", "route", "add", subnet(zoneA).String(), "via", routerB.String())
	harness.ExecOutput(t, ctx, ea, "ip", "route", "add", ebShared.String()+"/32", "via", routerA.String(), "src", eaShared.String())
	harness.ExecOutput(t, ctx, eb, "ip", "route", "add", eaShared.String()+"/32", "via", routerB.String(), "src", ebShared.String())
	rxBefore := harness.ExecOutput(t, ctx, router, "cat", "/sys/class/net/eth0/statistics/rx_packets")
	zoneAddressed := probe(ea, ebZone)
	rxMid := harness.ExecOutput(t, ctx, router, "cat", "/sys/class/net/eth0/statistics/rx_packets")
	sharedAddressed := probe(ea, ebShared)
	rxAfter := harness.ExecOutput(t, ctx, router, "cat", "/sys/class/net/eth0/statistics/rx_packets")
	t.Logf("gateway mode %s: endpoint A shared %s zone %s, endpoint B shared %s zone %s, router %s and %s", mode, eaShared, eaZone, ebShared, ebZone, routerA, routerB)
	t.Logf("gateway mode %s: zone-addressed destination %s, shared-addressed destination %s; router zone A rx packets %s -> %s -> %s",
		mode, ebZone, ebShared, rxBefore, rxMid, rxAfter)
	t.Logf("two-subnet record: %s", zoneTwoSubnetOutcome{Mode: mode, ZoneAddressed: zoneAddressed, SharedAddressed: sharedAddressed})
}
