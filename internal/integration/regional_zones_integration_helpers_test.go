//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/network"
)

const (
	regionalPool       = "tc-regional"
	regionalOneWay     = 20 * time.Millisecond
	regionalSamples    = 20
	regionalObjectSize = 4 << 20
	regionalBoundary   = "org.testcontainers.ceph.zone.boundary"
	regionalOwnerLabel = "org.testcontainers.ceph.cluster"
)

// regionalPath is one fixed probe: TCP connection setup from a member to a
// Ceph-advertised address and port. Cross paths leave their zone.
type regionalPath struct {
	Name, From string
	To         netip.AddrPort
	Cross      bool
}

// regionalTopology is what the scenario knows about one regional cluster.
type regionalTopology struct {
	cluster        *ceph.Container
	observer       *ceph.Observer
	pool           *ceph.Pool
	clientA        testcontainers.Container
	clientB        testcontainers.Container
	layout         ceph.ZoneLayout
	probes         map[string]*harness.ZoneProbe
	routerProbes   map[string]*harness.ZoneProbe
	helper         string
	osdA0, osdA1   int
	osdB           int
	clientAName    string
	clientBName    string
	zoneOf         map[string]string
	separatePlanes bool
}

// testRegionalZoneTopology starts one cluster across zones a and b through
// ceph.Run with WithZoneConfig: the MON, the MGR and two OSDs on separate
// CRUSH hosts in zone a, one OSD in zone b, a client in each zone, separate
// public and cluster bridges and a pool whose two replicas CRUSH places in
// distinct zones. It checks the membership, the CRUSH zone of every OSD
// against its network zone, every advertised address against its member and
// every kernel route decision; qualifies the boundary with fixed probes
// before shaping, with one direction delayed, with both, and after
// restoration; drives a write across the zones; cuts the boundary in both
// directions; restarts the zone b OSD; and terminates the cluster, checking
// that nothing with the boundary's label is left.
func testRegionalZoneTopology(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Minute)
	defer cancel()
	helper := harness.NetemImage(t)
	cluster := harness.NewRegionalCluster(t, ceph.Run,
		ceph.WithZoneConfig(harness.RegionalZoneConfig(t)),
		ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithSeparateClusterNetwork()),
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithInitialOSDs(
			ceph.OSDConfig{Zone: "a", Host: "zone-a-host-0"},
			ceph.OSDConfig{Zone: "a", Host: "zone-a-host-1"},
			ceph.OSDConfig{Zone: "b", Host: "zone-b-host-0"},
		).WithPoolDefaults(2, 1)),
		ceph.WithStartupTimeout(4*time.Minute))
	top := &regionalTopology{cluster: cluster, observer: harness.ObserveCluster(cluster), helper: helper, separatePlanes: true}
	logRegionalEnvironment(t, ctx, cluster)
	top.clientA = harness.RegionalClient(t, ctx, cluster, "a")
	top.clientB = harness.RegionalClient(t, ctx, cluster, "b")
	checkUnzonedClientRefused(t, ctx, cluster)
	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: regionalPool, PGNum: 1, Replicas: 2, MinSize: 1, Application: "rados", FailureDomain: "zone"})
	if err != nil {
		t.Fatal(err)
	}
	top.pool = pool
	if err := top.observer.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}

	// Membership, CRUSH zones, advertised addresses and routes.
	top.readLayout(t, ctx, "after startup")
	checkRegionalMembership(t, top)
	checkRegionalCRUSHZones(t, ctx, top)
	native := checkRegionalAdvertised(t, ctx, top)
	top.startProbes(t, ctx)
	checkRegionalRoutes(t, ctx, top)

	// Baseline, one direction delayed, both, restoration.
	paths := regionalProbePaths(t, top, native)
	base := regionalProbePhase(t, ctx, top, "baseline", paths, regionalSamples, 2*time.Second)
	for _, p := range paths {
		if err := harness.CheckZoneControl(base[p.Name], base[p.Name]); err != nil {
			t.Fatalf("baseline %s: %v", p.Name, err)
		}
	}
	profile := fault.NetworkProfile{HelperImage: helper, Delay: regionalOneWay}
	leases := regionalShape(t, ctx, top, []string{"a"}, profile)
	oneWay := regionalProbePhase(t, ctx, top, "zone a egress delayed", paths, regionalSamples, 2*time.Second)
	regionalLeaseStatus(t, ctx, "zone a egress delayed", leases)
	regionalJudge(t, paths, base, oneWay, regionalOneWay)
	regionalRestore(t, ctx, leases)
	leases = regionalShape(t, ctx, top, []string{"a", "b"}, profile)
	both := regionalProbePhase(t, ctx, top, "both directions delayed", paths, regionalSamples, 2*time.Second)
	regionalLeaseStatus(t, ctx, "both directions delayed", leases)
	regionalJudge(t, paths, base, both, 2*regionalOneWay)
	first := regionalWrite(t, ctx, top, top.clientA, "client-a", "regional-object-1", regionalObjectSize)
	regionalRestore(t, ctx, leases)
	restored := regionalProbePhase(t, ctx, top, "restored", paths, regionalSamples, 2*time.Second)
	for _, p := range paths {
		if err := harness.CheckZoneControl(base[p.Name], restored[p.Name]); err != nil {
			t.Errorf("restored %s: %v", p.Name, err)
		}
	}
	checkRoutersUnshaped(t, ctx, top)
	if t.Failed() {
		t.FailNow()
	}

	// A bidirectional partition, then recovery and read-back.
	second := regionalPartition(t, ctx, top, paths, base)
	for _, obj := range []regionalObject{first, second} {
		for _, reader := range []struct {
			name string
			ctr  testcontainers.Container
		}{{"client-a", top.clientA}, {"client-b", top.clientB}} {
			if got := regionalRead(t, ctx, reader.ctr, obj.Name); got != obj.SHA256 {
				t.Fatalf("%s read %s with sha256 %s, want the acknowledged %s", reader.name, obj.Name, got, obj.SHA256)
			}
			t.Logf("%s read back %s: sha256 %s", reader.name, obj.Name, obj.SHA256)
		}
	}

	// A restarted member gets a new namespace and is enrolled again.
	checkRegionalRestart(t, ctx, top)

	// Teardown releases the boundary and nothing else is left.
	checkRegionalTeardown(t, ctx, top)
}

func logRegionalEnvironment(t *testing.T, ctx context.Context, cluster *ceph.Container) {
	t.Helper()
	facts, err := ceph.FactsOf(cluster)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("release %s, images %+v, public bridge %s, cluster bridge %s", facts.CephVersion, facts.Images, facts.NetworkName, facts.ClusterNetworkName)
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = docker.Close() }()
	if version, err := docker.ServerVersion(ctx, mobycl.ServerVersionOptions{}); err == nil {
		t.Logf("engine: Docker %s, %s/%s", version.Version, version.Os, version.Arch)
	}
}

// checkUnzonedClientRefused starts a WithClient container without a zone: a
// cluster with zones refuses it, because it would reach both zones directly.
func checkUnzonedClientRefused(t *testing.T, ctx context.Context, cluster *ceph.Container) {
	t.Helper()
	image, _ := harness.IntegrationImages(t)
	ctr, err := testcontainers.Run(ctx, harness.NativeClientImage(t, image), cluster.WithClient(), ceph.WithIdleEntrypoint())
	if ctr != nil {
		if terminateErr := ctr.Terminate(ctx); terminateErr != nil {
			t.Fatalf("remove the refused client: %v", terminateErr)
		}
	}
	if err == nil || !strings.Contains(err.Error(), "Container.WithZone") {
		t.Fatalf("a client without a zone started: %v", err)
	}
	t.Logf("a WithClient container without WithZone was refused: %v", err)
	if _, err := testcontainers.Run(ctx, harness.NativeClientImage(t, image), cluster.WithClient(), cluster.WithZone("c")); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("an undeclared zone was accepted: %v", err)
	}
}

func (top *regionalTopology) readLayout(t *testing.T, ctx context.Context, stage string) {
	t.Helper()
	layout, err := top.cluster.Zones(ctx)
	if err != nil {
		t.Fatal(err)
	}
	top.layout = layout
	top.zoneOf = map[string]string{}
	for _, zone := range layout.Zones {
		var names []string
		for _, member := range zone.Members {
			top.zoneOf[member.Name] = zone.Name
			names = append(names, fmt.Sprintf("%s(%v/%v)", member.Name, member.PublicAddress, member.ClusterAddress))
		}
		t.Logf("%s: zone %s router %v/%v members %v reporter subtrees %d", stage, zone.Name, zone.Router.PublicAddress, zone.Router.ClusterAddress, names, zone.ReporterSubtrees)
	}
	t.Logf("%s: quorum zone %s, %d routes, mon_osd_min_down_reporters %d at %s level, unzoned %v",
		stage, layout.QuorumZone, len(layout.Routes), layout.DownReporters, layout.ReporterSubtree, layout.Unzoned)
}

// checkRegionalMembership checks who is where and that Ceph's reporter
// policy can mark the far zone down from the quorum zone.
func checkRegionalMembership(t *testing.T, top *regionalTopology) {
	t.Helper()
	layout := top.layout
	clientA, _ := harness.ZoneMemberByContainer(t, layout, top.clientA.GetContainerID())
	clientB, _ := harness.ZoneMemberByContainer(t, layout, top.clientB.GetContainerID())
	top.clientAName, top.clientBName = clientA.Name, clientB.Name
	osds, err := top.cluster.OSDs(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var zoneA []int
	for _, osd := range osds {
		name := fmt.Sprintf("osd.%d", osd.ID)
		if got := top.zoneOf[name]; got != osd.Placement().Zone {
			t.Fatalf("%s is a member of zone %q, its placement names %q", name, got, osd.Placement().Zone)
		}
		if osd.Placement().Zone == "a" {
			zoneA = append(zoneA, osd.ID)
		} else {
			top.osdB = osd.ID
		}
	}
	slices.Sort(zoneA)
	if len(zoneA) != 2 {
		t.Fatalf("zone a holds OSDs %v, want two", zoneA)
	}
	top.osdA0, top.osdA1 = zoneA[0], zoneA[1]
	want := map[string]string{"mon.a": "a", "mgr.a": "a", top.clientAName: "a", top.clientBName: "b"}
	for name, zone := range want {
		if top.zoneOf[name] != zone {
			t.Fatalf("%s is in zone %q, want %s", name, top.zoneOf[name], zone)
		}
	}
	if layout.QuorumZone != "a" || len(layout.Unzoned) != 0 {
		t.Fatalf("quorum zone %q, unzoned %v", layout.QuorumZone, layout.Unzoned)
	}
	quorum := layout.Zones[0]
	if quorum.Name != "a" || quorum.ReporterSubtrees < layout.DownReporters || layout.Zones[1].ReporterSubtrees != 1 {
		t.Fatalf("zone a has %d %s reporter subtrees for %d down reporters, zone b %d", quorum.ReporterSubtrees, layout.ReporterSubtree, layout.DownReporters, layout.Zones[1].ReporterSubtrees)
	}
	// Each ordered pair of members in different zones has one route per
	// plane both have.
	want2 := 0
	for _, za := range layout.Zones {
		for _, zb := range layout.Zones {
			if za.Name == zb.Name {
				continue
			}
			for _, a := range za.Members {
				for _, b := range zb.Members {
					want2++
					if a.ClusterAddress.IsValid() && b.ClusterAddress.IsValid() {
						want2++
					}
				}
			}
		}
	}
	if len(layout.Routes) != want2 {
		t.Fatalf("%d routes, want %d", len(layout.Routes), want2)
	}
}

// checkRegionalCRUSHZones compares the CRUSH zone bucket of every OSD with
// its network zone.
func checkRegionalCRUSHZones(t *testing.T, ctx context.Context, top *regionalTopology) {
	t.Helper()
	for name, zone := range top.zoneOf {
		id, isOSD := strings.CutPrefix(name, "osd.")
		if !isOSD {
			continue
		}
		out, err := top.cluster.Ceph(ctx, "osd", "find", id, "--format", "json")
		if err != nil {
			t.Fatal(err)
		}
		var found struct {
			Location map[string]string `json:"crush_location"`
		}
		if err := json.Unmarshal(out, &found); err != nil {
			t.Fatalf("osd find %s: %v", id, err)
		}
		if found.Location["zone"] != zone {
			t.Fatalf("%s is in CRUSH location %v, a member of network zone %s", name, found.Location, zone)
		}
		t.Logf("%s: CRUSH location %v, network zone %s", name, found.Location, zone)
	}
}

// checkRegionalAdvertised compares every address the MON, MGR and OSD maps
// advertise with the member's address on its plane.
func checkRegionalAdvertised(t *testing.T, ctx context.Context, top *regionalTopology) ceph.DaemonAddresses {
	t.Helper()
	native, err := top.observer.DaemonAddresses(ctx)
	if err != nil {
		t.Fatal(err)
	}
	same := func(what, name string, cluster bool, vector []ceph.EntityAddress) {
		t.Helper()
		member, _ := harness.ZoneMember(t, top.layout, name)
		want := member.PublicAddress
		if cluster {
			want = member.ClusterAddress
		}
		if len(vector) == 0 {
			t.Fatalf("%s advertises nothing", what)
		}
		for _, entry := range vector {
			addr, err := netip.ParseAddrPort(entry.Addr)
			if err != nil {
				t.Fatal(err)
			}
			if addr.Addr() != want {
				t.Fatalf("%s advertises %s, not %s's address %v", what, entry.Addr, name, want)
			}
		}
		t.Logf("advertised %-26s zone %s %v", what, top.zoneOf[name], vector)
	}
	for _, mon := range native.Monitors {
		same("mon."+mon.Name, "mon."+mon.Name, false, mon.Addrs)
	}
	if native.ActiveManager == nil {
		t.Fatal("no active MGR")
	}
	same("mgr."+native.ActiveManager.Name, "mgr."+native.ActiveManager.Name, false, native.ActiveManager.Addrs)
	for _, osd := range native.OSDs {
		name := fmt.Sprintf("osd.%d", osd.ID)
		same(name+" public", name, false, osd.Public)
		same(name+" heartbeat front", name, false, osd.HeartbeatFront)
		same(name+" cluster", name, true, osd.Cluster)
		same(name+" heartbeat back", name, true, osd.HeartbeatBack)
	}
	return native
}

func (top *regionalTopology) startProbes(t *testing.T, ctx context.Context) {
	t.Helper()
	top.probes, top.routerProbes = map[string]*harness.ZoneProbe{}, map[string]*harness.ZoneProbe{}
	for _, zone := range top.layout.Zones {
		top.routerProbes[zone.Name] = harness.NewZoneProbe(t, ctx, zone.Router.Name, zone.Router.ContainerID)
		for _, member := range zone.Members {
			top.probes[member.Name] = harness.NewZoneProbe(t, ctx, member.Name, member.ContainerID)
		}
	}
}

// checkRegionalRoutes asks every member's kernel how it reaches every other
// member on each plane both have: through its own zone's router across the
// boundary, directly within its zone.
func checkRegionalRoutes(t *testing.T, ctx context.Context, top *regionalTopology) {
	t.Helper()
	count := 0
	for _, from := range top.layout.Zones {
		router := from.Router
		for _, member := range from.Members {
			for _, to := range top.layout.Zones {
				for _, peer := range to.Members {
					if peer.Name == member.Name {
						continue
					}
					for _, plane := range []struct {
						name           string
						src, dest, via netip.Addr
					}{
						{"public", member.PublicAddress, peer.PublicAddress, router.PublicAddress},
						{"cluster", member.ClusterAddress, peer.ClusterAddress, router.ClusterAddress},
					} {
						if !plane.src.IsValid() || !plane.dest.IsValid() {
							continue
						}
						want := netip.Addr{}
						if to.Name != from.Name {
							want = plane.via
						}
						got, err := top.probes[member.Name].RouteGet(ctx, plane.dest)
						if err != nil {
							t.Fatal(err)
						}
						if got.Via != want || got.Src != plane.src {
							t.Fatalf("%s to %s on the %s plane goes via %v from %v, want via %v from %v", member.Name, peer.Name, plane.name, got.Via, got.Src, want, plane.src)
						}
						count++
					}
				}
			}
		}
	}
	t.Logf("checked %d kernel route lookups: cross-zone through the sender's zone router, same-zone on-link", count)
}

func regionalProbePaths(t *testing.T, top *regionalTopology, native ceph.DaemonAddresses) []regionalPath {
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
		addr, err := harness.EntityAddrPort(vector, "v2")
		if err != nil {
			t.Fatal(err)
		}
		return addr
	}
	mon := port(native.Monitors[0].Addrs)
	a0, a1, b := osd(top.osdA0), osd(top.osdA1), osd(top.osdB)
	n := func(id int) string { return fmt.Sprintf("osd.%d", id) }
	ca, cb := top.clientAName, top.clientBName
	return []regionalPath{
		{Name: "client-a to " + n(top.osdB) + " public", From: ca, To: port(b.Public), Cross: true},
		{Name: "client-b to " + n(top.osdA0) + " public", From: cb, To: port(a0.Public), Cross: true},
		{Name: n(top.osdB) + " to mon.a", From: n(top.osdB), To: mon, Cross: true},
		{Name: n(top.osdA0) + " to " + n(top.osdB) + " cluster", From: n(top.osdA0), To: port(b.Cluster), Cross: true},
		{Name: n(top.osdB) + " to " + n(top.osdA0) + " cluster", From: n(top.osdB), To: port(a0.Cluster), Cross: true},
		{Name: "client-a to " + n(top.osdA0) + " public", From: ca, To: port(a0.Public)},
		{Name: n(top.osdA0) + " to " + n(top.osdA1) + " cluster", From: n(top.osdA0), To: port(a1.Cluster)},
		{Name: "client-b to " + n(top.osdB) + " public", From: cb, To: port(b.Public)},
	}
}

// regionalCounters reads every router's transmitted bytes on every plane,
// keyed by "zone/plane".
func regionalCounters(t *testing.T, ctx context.Context, top *regionalTopology) map[string]uint64 {
	t.Helper()
	out := map[string]uint64{}
	for _, zone := range top.layout.Zones {
		for plane, addr := range map[string]netip.Addr{"public": zone.Router.PublicAddress, "cluster": zone.Router.ClusterAddress} {
			_, tx, err := top.routerProbes[zone.Name].InterfaceBytes(ctx, addr)
			if err != nil {
				t.Fatal(err)
			}
			out[zone.Name+"/"+plane] = tx
		}
	}
	return out
}

func regionalProbePhase(t *testing.T, ctx context.Context, top *regionalTopology, phase string, paths []regionalPath, count int, timeout time.Duration) map[string]harness.ZoneRTT {
	t.Helper()
	before := regionalCounters(t, ctx, top)
	results := map[string]harness.ZoneRTT{}
	for _, p := range paths {
		s, err := top.probes[p.From].ConnectRTT(ctx, p.To, count, timeout)
		if err != nil {
			t.Fatalf("%s %s: %v", phase, p.Name, err)
		}
		results[p.Name] = s
		kind := "same-zone"
		if p.Cross {
			kind = "cross-zone"
		}
		t.Logf("RTT %-24s %-10s %-26s %s %s", phase, kind, p.Name, p.To, s)
	}
	after := regionalCounters(t, ctx, top)
	for _, key := range slices.Sorted(func(yield func(string) bool) {
		for k := range after {
			if !yield(k) {
				return
			}
		}
	}) {
		t.Logf("router %-24s %s sent %d bytes", phase, key, after[key]-before[key])
	}
	return results
}

// regionalShape applies profile at the boundary to what leaves each of zones
// on both planes.
func regionalShape(t *testing.T, ctx context.Context, top *regionalTopology, zones []string, profile fault.NetworkProfile) []*fault.NetworkProfileLease {
	t.Helper()
	var leases []*fault.NetworkProfileLease
	for _, zone := range zones {
		for _, plane := range []ceph.NetworkPlane{ceph.PublicNetworkPlane, ceph.ClusterNetworkPlane} {
			p := profile
			p.Plane = plane
			lease, err := fault.ApplyZoneProfile(ctx, top.cluster, zone, p)
			if lease != nil {
				leases = append(leases, lease)
				t.Cleanup(func() {
					if err := lease.Restore(context.Background()); err != nil {
						t.Errorf("restore the zone %s %s profile: %v", zone, plane, err)
					}
				})
			}
			if err != nil {
				t.Fatalf("shape what leaves zone %s on the %s plane: %v", zone, plane, err)
			}
			t.Logf("zone %s %s egress profile on %.12s %s: delay %v loss %v", zone, plane, lease.ContainerID, lease.Interface, lease.Delay, lease.Loss)
		}
	}
	return leases
}

func regionalRestore(t *testing.T, ctx context.Context, leases []*fault.NetworkProfileLease) {
	t.Helper()
	for i := len(leases) - 1; i >= 0; i-- {
		if err := leases[i].Restore(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func regionalLeaseStatus(t *testing.T, ctx context.Context, phase string, leases []*fault.NetworkProfileLease) {
	t.Helper()
	for _, lease := range leases {
		status, err := lease.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !status.Applied || status.Delay != lease.Delay || status.Loss != lease.Loss {
			t.Fatalf("%s: the lease on %.12s is not applied as requested: %+v", phase, lease.ContainerID, status)
		}
		t.Logf("netem %-24s router %.12s %s: %s delay %v loss %v, %d packets %d bytes %d drops",
			phase, lease.ContainerID, lease.NetworkName, status.Interface, status.Delay, status.Loss, status.Packets, status.Bytes, status.Drops)
	}
}

// regionalJudge compares a shaped phase with the baseline: cross-zone paths
// gain added once, same-zone paths keep their baseline.
func regionalJudge(t *testing.T, paths []regionalPath, base, shaped map[string]harness.ZoneRTT, added time.Duration) {
	t.Helper()
	for _, p := range paths {
		var err error
		if p.Cross {
			err = harness.CheckZoneShaped(base[p.Name], shaped[p.Name], added)
		} else {
			err = harness.CheckZoneControl(base[p.Name], shaped[p.Name])
		}
		if err != nil {
			t.Errorf("%s: %v", p.Name, err)
		}
	}
	if t.Failed() {
		t.FailNow()
	}
}

// checkRoutersUnshaped checks that no netem or prio qdisc is left on a router.
func checkRoutersUnshaped(t *testing.T, ctx context.Context, top *regionalTopology) {
	t.Helper()
	for zone, probe := range top.routerProbes {
		out, err := probe.Exec(ctx, "tc", "qdisc", "show")
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(out, "netem") || strings.Contains(out, "prio") {
			t.Fatalf("the router of zone %s keeps a qdisc: %s", zone, out)
		}
	}
}

type regionalObject struct {
	Name, SHA256 string
}

func regionalSinglePG(t *testing.T, ctx context.Context, pool *ceph.Pool) ceph.PGState {
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

func (top *regionalTopology) actingZones(acting []int) map[string][]int {
	zones := map[string][]int{}
	for _, id := range acting {
		zone := top.zoneOf[fmt.Sprintf("osd.%d", id)]
		zones[zone] = append(zones[zone], id)
	}
	return zones
}

// regionalWrite writes size random bytes and checks that the PG spans the
// zones and that the replication crossed the boundary: the cluster-plane
// router of the primary's zone sent at least the object's size.
func regionalWrite(t *testing.T, ctx context.Context, top *regionalTopology, client testcontainers.Container, clientName, name string, size int) regionalObject {
	t.Helper()
	pg := regionalSinglePG(t, ctx, top.pool)
	zones := top.actingZones(pg.Acting)
	if len(zones["a"]) != 1 || len(zones["b"]) != 1 {
		t.Fatalf("PG %s acting set %v does not span the zones: %v", pg.PGID, pg.Acting, zones)
	}
	primaryZone := top.zoneOf[fmt.Sprintf("osd.%d", pg.ActingPrimary)]
	path := "/tmp/" + name
	digest := strings.Fields(harness.ExecOutput(t, ctx, client, "sh", "-c", fmt.Sprintf("head -c %d /dev/urandom > %s && sha256sum %s", size, path, path)))[0]
	before := regionalCounters(t, ctx, top)
	start := time.Now()
	harness.ExecOutput(t, ctx, client, "rados", "-p", regionalPool, "put", name, path)
	elapsed := time.Since(start)
	after := regionalCounters(t, ctx, top)
	key := primaryZone + "/cluster"
	t.Logf("%s wrote %s (%d bytes, sha256 %s) in %v to PG %s acting %v primary osd.%d (zone %s); the zone %s router sent %d bytes on the cluster plane",
		clientName, name, size, digest, elapsed.Round(time.Millisecond), pg.PGID, pg.Acting, pg.ActingPrimary, primaryZone, primaryZone, after[key]-before[key])
	if after[key]-before[key] < uint64(size) {
		t.Fatalf("the replication of %s did not cross the boundary: %d bytes", name, after[key]-before[key])
	}
	return regionalObject{Name: name, SHA256: digest}
}

func regionalRead(t *testing.T, ctx context.Context, client testcontainers.Container, name string) string {
	t.Helper()
	path := "/tmp/readback-" + name
	out := harness.ExecOutput(t, ctx, client, "sh", "-c", fmt.Sprintf("rados -p %s get %s %s && sha256sum %s && rm -f %s", regionalPool, name, path, path, path))
	return strings.Fields(out)[0]
}

func regionalExec(ctx context.Context, ctr testcontainers.Container, args ...string) (int, string, error) {
	execCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	code, r, err := ctr.Exec(execCtx, args, tcexec.Multiplexed())
	if err != nil {
		return code, "", err
	}
	out, err := io.ReadAll(r)
	return code, strings.TrimSpace(string(out)), err
}

// regionalPartition drops every packet that crosses the boundary, in both
// directions on both planes, and observes same-zone reachability, the zone b
// OSD marked down by the two zone a reporters, the PG served from zone a, a
// write from zone a and the zone b client cut off from the only MON. It then
// restores the boundary and waits for recovery.
func regionalPartition(t *testing.T, ctx context.Context, top *regionalTopology, paths []regionalPath, base map[string]harness.ZoneRTT) regionalObject {
	t.Helper()
	leases := regionalShape(t, ctx, top, []string{"a", "b"}, fault.NetworkProfile{HelperImage: top.helper, Loss: 100})
	cutAt := time.Now()
	regionalLeaseStatus(t, ctx, "partitioned", leases)
	for _, p := range paths {
		count, timeout := regionalSamples, 2*time.Second
		if p.Cross {
			count, timeout = 5, time.Second
		}
		s, err := top.probes[p.From].ConnectRTT(ctx, p.To, count, timeout)
		if err != nil {
			t.Fatal(err)
		}
		if p.Cross {
			err = harness.CheckZoneCut(s)
		} else {
			err = harness.CheckZoneControl(base[p.Name], s)
		}
		t.Logf("RTT %-24s %-26s %s", "partitioned", p.Name, s)
		if err != nil {
			t.Fatalf("partitioned %s: %v", p.Name, err)
		}
	}
	waitFor := func(what string, bound time.Duration, done func() (bool, string)) {
		t.Helper()
		deadline := time.Now().Add(bound)
		for {
			ok, detail := done()
			if ok {
				t.Logf("%s after %v: %s", what, time.Since(cutAt).Round(time.Second), detail)
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s not observed within %v: %s", what, bound, detail)
			}
			time.Sleep(2 * time.Second)
		}
	}
	osdUp := func(want bool) func() (bool, string) {
		return func() (bool, string) {
			states, err := top.observer.OSDStates(ctx)
			if err != nil {
				return false, err.Error()
			}
			for _, s := range states {
				if s.ID == top.osdB {
					return s.Up == want, fmt.Sprintf("%+v", states)
				}
			}
			return false, fmt.Sprintf("osd.%d missing: %+v", top.osdB, states)
		}
	}
	waitFor(fmt.Sprintf("osd.%d marked down", top.osdB), 3*time.Minute, osdUp(false))
	waitFor("PG served by zone a alone", 3*time.Minute, func() (bool, string) {
		snapshot, err := top.pool.PGs(ctx)
		if err != nil || len(snapshot.PGs) != 1 {
			return false, fmt.Sprintf("%v %+v", err, snapshot.PGs)
		}
		pg := snapshot.PGs[0]
		zones := top.actingZones(pg.Acting)
		ok := len(zones["b"]) == 0 && len(zones["a"]) == 1 && strings.Contains(pg.State, "active") && strings.Contains(pg.State, "undersized")
		return ok, fmt.Sprintf("PG %s state %s acting %v primary osd.%d", pg.PGID, pg.State, pg.Acting, pg.ActingPrimary)
	})
	name := "regional-object-2"
	path := "/tmp/" + name
	digest := strings.Fields(harness.ExecOutput(t, ctx, top.clientA, "sh", "-c", fmt.Sprintf("head -c %d /dev/urandom > %s && sha256sum %s", 1<<20, path, path)))[0]
	start := time.Now()
	if code, out, err := regionalExec(ctx, top.clientA, "rados", "--rados-osd-op-timeout", "90", "-p", regionalPool, "put", name, path); err != nil || code != 0 {
		t.Fatalf("client-a write during the partition: exit %d %q %v", code, out, err)
	}
	t.Logf("client-a wrote %s during the partition in %v: acknowledged", name, time.Since(start).Round(time.Millisecond))
	start = time.Now()
	code, out, err := regionalExec(ctx, top.clientB, "rados", "--client-mount-timeout", "15", "-p", regionalPool, "stat", "regional-object-1")
	t.Logf("client-b in zone b during the partition: rados stat exit %d after %v: %s (%v)", code, time.Since(start).Round(time.Second), out, err)
	if err == nil && code == 0 {
		t.Fatal("client-b reached the cluster across the partitioned boundary")
	}
	regionalRestore(t, ctx, leases)
	restoredAt := time.Now()
	waitFor(fmt.Sprintf("osd.%d up again", top.osdB), 5*time.Minute, osdUp(true))
	recoverCtx, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()
	if err := top.observer.WaitForClean(recoverCtx); err != nil {
		t.Fatalf("recovery after the partition: %v", err)
	}
	pg := regionalSinglePG(t, ctx, top.pool)
	if zones := top.actingZones(pg.Acting); len(zones["a"]) != 1 || len(zones["b"]) != 1 {
		t.Fatalf("after recovery PG %s acting %v does not span the zones", pg.PGID, pg.Acting)
	}
	t.Logf("recovered %v after the restoration: PG %s state %s acting %v", time.Since(restoredAt).Round(time.Second), pg.PGID, pg.State, pg.Acting)
	return regionalObject{Name: name, SHA256: digest}
}

// checkRegionalRestart stops and starts the zone b OSD: the new start's
// namespace has no routes until the boundary enrolls it again, which it does
// before the OSD is ready.
func checkRegionalRestart(t *testing.T, ctx context.Context, top *regionalTopology) {
	t.Helper()
	osds, err := top.cluster.OSDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var target *ceph.OSDContainer
	for _, osd := range osds {
		if osd.ID == top.osdB {
			target = osd
		}
	}
	timeout := 10 * time.Second
	if err := target.Stop(ctx, &timeout); err != nil {
		t.Fatal(err)
	}
	stopped, err := top.cluster.Zones(ctx)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("osd.%d", top.osdB)
	for _, route := range stopped.Routes {
		if route.Member == name {
			t.Fatalf("the stopped %s keeps a route record %+v", name, route)
		}
		for _, zone := range stopped.Zones {
			for _, member := range zone.Members {
				if member.Name == name {
					t.Fatalf("the stopped %s is still a member", name)
				}
			}
		}
	}
	if err := target.Start(ctx); err != nil {
		t.Fatal(err)
	}
	top.readLayout(t, ctx, "after the restart of "+name)
	if len(top.layout.Unzoned) != 0 {
		t.Fatalf("unzoned after the restart: %v", top.layout.Unzoned)
	}
	member, zone := harness.ZoneMember(t, top.layout, name)
	if zone != "b" {
		t.Fatalf("%s rejoined zone %s", name, zone)
	}
	probe := harness.NewZoneProbe(t, ctx, name+" restarted", member.ContainerID)
	mon, _ := harness.ZoneMember(t, top.layout, "mon.a")
	router := harness.ZoneRouter(t, top.layout, "b")
	got, err := probe.RouteGet(ctx, mon.PublicAddress)
	if err != nil {
		t.Fatal(err)
	}
	if got.Via != router.PublicAddress {
		t.Fatalf("the restarted %s reaches mon.a via %v, want the zone b router %v", name, got.Via, router.PublicAddress)
	}
	back, err := top.probes["mon.a"].RouteGet(ctx, member.PublicAddress)
	if err != nil {
		t.Fatal(err)
	}
	if want := harness.ZoneRouter(t, top.layout, "a").PublicAddress; back.Via != want {
		t.Fatalf("mon.a reaches the restarted %s via %v, want the zone a router %v", name, back.Via, want)
	}
	if err := top.observer.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("restarted %s: enrolled again, routes through router b out and router a back", name)
}

// checkRegionalTeardown terminates the clients and the cluster and checks
// that no container or network of the boundary or the cluster is left.
func checkRegionalTeardown(t *testing.T, ctx context.Context, top *regionalTopology) {
	t.Helper()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = docker.Close() }()
	inspection, err := docker.ContainerInspect(ctx, top.layout.Zones[0].Router.ContainerID, mobycl.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	owner := inspection.Container.Config.Labels[regionalOwnerLabel]
	if owner == "" {
		t.Fatal("the router has no cluster owner label")
	}
	for _, client := range []testcontainers.Container{top.clientA, top.clientB} {
		if err := client.Terminate(ctx); err != nil {
			t.Fatal(err)
		}
	}
	layout, err := top.cluster.Zones(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range layout.Routes {
		if route.Member == top.clientAName || route.Member == top.clientBName {
			t.Fatalf("a removed client keeps a route record %+v", route)
		}
	}
	if err := top.cluster.Terminate(ctx); err != nil {
		t.Fatalf("terminate the regional cluster: %v", err)
	}
	for _, label := range []string{regionalBoundary + "=" + owner, regionalOwnerLabel + "=" + owner} {
		filter := mobycl.Filters{}.Add("label", label)
		ctrs, err := docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true, Filters: filter})
		if err != nil {
			t.Fatal(err)
		}
		nets, err := docker.NetworkList(ctx, mobycl.NetworkListOptions{Filters: filter})
		if err != nil {
			t.Fatal(err)
		}
		if len(ctrs.Items) != 0 || len(nets.Items) != 0 {
			t.Fatalf("%d containers and %d networks with %s are left", len(ctrs.Items), len(nets.Items), label)
		}
	}
	t.Log("terminated: no container or network with the boundary's or the cluster's label is left")
}

// testRegionalZoneFailedSetup starts a cluster whose boundary helper lacks
// ip: the first router starts on its bridges and then fails its check, Run
// returns the error, and Terminate releases the router, the routers'
// bridge and the cluster's bridges without touching anything else.
func testRegionalZoneFailedSetup(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	image, base := harness.IntegrationImages(t)
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = docker.Close() }()
	bystander, err := testcontainers.Run(ctx, harness.NetemImage(t), ceph.WithIdleEntrypoint(), testcontainers.WithLabels(map[string]string{regionalBoundary: "bystander"}))
	if bystander != nil {
		testcontainers.CleanupContainer(t, bystander)
	}
	if err != nil {
		t.Fatal(err)
	}
	labelled := func() []string {
		filter := mobycl.Filters{}.Add("label", regionalBoundary)
		ctrs, err := docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true, Filters: filter})
		if err != nil {
			t.Fatal(err)
		}
		nets, err := docker.NetworkList(ctx, mobycl.NetworkListOptions{Filters: filter})
		if err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, c := range ctrs.Items {
			ids = append(ids, "container "+c.ID)
		}
		for _, n := range nets.Items {
			ids = append(ids, "network "+n.ID)
		}
		slices.Sort(ids)
		return ids
	}
	before := labelled()
	zones := ceph.ZoneConfig{}.WithZones("a", "b").WithQuorumZone("a").WithHelperImage(harness.EssentialImage(t))
	cluster, err := ceph.Run(ctx, image, append(base,
		ceph.WithZoneConfig(zones),
		ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithSeparateClusterNetwork()),
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithInitialOSDs(ceph.OSDConfig{Zone: "a"}, ceph.OSDConfig{Zone: "b"})))...)
	if err == nil {
		t.Fatal("a boundary helper without ip was accepted")
	}
	t.Logf("Run with a helper image without ip: %v", err)
	if cluster == nil {
		t.Fatal("the partial cluster was not returned")
	}
	facts, factsErr := ceph.FactsOf(cluster)
	if factsErr != nil {
		t.Fatal(factsErr)
	}
	during := labelled()
	if len(during) <= len(before) {
		t.Fatalf("the failed setup left no labelled router or bridge to release: %v", during)
	}
	if err := cluster.Terminate(ctx); err != nil {
		t.Fatalf("terminate the partial regional cluster: %v", err)
	}
	if after := labelled(); !slices.Equal(after, before) {
		t.Fatalf("labelled resources before %v, after the teardown %v", before, after)
	}
	for _, name := range []string{facts.NetworkName, facts.ClusterNetworkName} {
		if _, err := docker.NetworkInspect(ctx, name, mobycl.NetworkInspectOptions{}); err == nil {
			t.Fatalf("bridge %s is left", name)
		}
	}
	if state, err := bystander.State(ctx); err != nil || !state.Running {
		t.Fatalf("the bystander with the boundary label key was touched: %+v %v", state, err)
	}
	t.Logf("the failed setup created %d labelled resources; Terminate released them and the bridges, and left the bystander", len(during)-len(before))
}

// testRegionalZoneRouteOwnership checks that the boundary owns only its own
// routes and admits a member only once its enrollment completed. A plain
// container attached to the public bridge without WithZone, which the
// boundary never enrolls, is unzoned and ApplyZoneProfile refuses to shape
// until it is removed. A zone a client and a zone b client each get a caller /32 route to a new zone b
// client while it starts, before its enrollment: the zone b route is within
// the zone and stays, the zone a route conflicts with the boundary's route
// there, so the enrollment fails on the peer routes. The new client is then
// unzoned and ApplyZoneProfile refuses to shape. Once the caller removed the
// conflicting route, a retried Start completes the enrollment and installs
// the boundary's route. The caller then replaces that route, and the removal
// of the client leaves the caller's route in place.
func testRegionalZoneRouteOwnership(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	cluster := harness.NewRegionalCluster(t, ceph.Run,
		ceph.WithZoneConfig(harness.RegionalZoneConfig(t)),
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs()),
		ceph.WithStartupTimeout(4*time.Minute))
	clientA := harness.RegionalClient(t, ctx, cluster, "a")
	clientB := harness.RegionalClient(t, ctx, cluster, "b")
	editorA := regionalRouteEditor(t, ctx, "client-a", clientA.GetContainerID())
	editorB := regionalRouteEditor(t, ctx, "client-b", clientB.GetContainerID())
	layout, err := cluster.Zones(ctx)
	if err != nil {
		t.Fatal(err)
	}
	routerA := harness.ZoneRouter(t, layout, "a")
	publicA, _ := harness.ZoneMemberByContainer(t, layout, clientA.GetContainerID())
	checkNeverEnrolledContainerUnzoned(t, ctx, cluster, clientA, publicA.PublicAddress)

	// The caller's routes go in while the new client starts, before the
	// boundary's hook enrolls it; the hook runs again on a retried Start.
	var joining netip.Addr
	callerRoutes := func(ctx context.Context, ctr testcontainers.Container) error {
		if joining.IsValid() {
			return nil
		}
		addr, err := regionalAddressOnBridgeOf(ctx, ctr, clientA, publicA.PublicAddress)
		if err != nil {
			return err
		}
		joining = addr
		for _, editor := range []testcontainers.Container{editorA, editorB} {
			if err := regionalCallerRoute(ctx, editor, "add", joining); err != nil {
				return err
			}
		}
		return nil
	}
	image, _ := harness.IntegrationImages(t)
	joiner, err := testcontainers.Run(ctx, harness.NativeClientImage(t, image),
		testcontainers.WithAdditionalLifecycleHooks(testcontainers.ContainerLifecycleHooks{PostStarts: []testcontainers.ContainerHook{callerRoutes}}),
		cluster.WithClient(), cluster.WithZone("b"), ceph.WithIdleEntrypoint())
	if joiner != nil {
		testcontainers.CleanupContainer(t, joiner)
	}
	if err == nil {
		t.Fatal("a zone b client joined although a zone a member has a caller route to it")
	}
	if joiner == nil || !strings.Contains(err.Error(), "does not own") {
		t.Fatalf("the conflicting enrollment failed with %v, want the refusal of a route the boundary does not own", err)
	}
	t.Logf("enrollment refused on the peer routes: %v", err)
	name := regionalContainerName(t, ctx, joiner)

	// A partial enrollment is not a member and keeps the boundary closed to
	// faults; both caller routes are as the caller left them.
	partial, err := cluster.Zones(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(partial.Unzoned, func(u string) bool { return strings.HasPrefix(u, name+" ") }) {
		t.Fatalf("the partially enrolled %s is not unzoned: %v", name, partial.Unzoned)
	}
	for _, zone := range partial.Zones {
		for _, member := range zone.Members {
			if member.ContainerID == joiner.GetContainerID() {
				t.Fatalf("the partially enrolled %s is a member of zone %s", name, zone.Name)
			}
		}
	}
	profile := fault.NetworkProfile{HelperImage: harness.NetemImage(t), Delay: regionalOneWay, Plane: ceph.PublicNetworkPlane}
	lease, err := fault.ApplyZoneProfile(ctx, cluster, "a", profile)
	if lease != nil {
		if restoreErr := lease.Restore(ctx); restoreErr != nil {
			t.Errorf("restore the zone a profile: %v", restoreErr)
		}
	}
	if !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), name) {
		t.Fatalf("ApplyZoneProfile with the partially enrolled %s: %v, want a refusal naming it", name, err)
	}
	t.Logf("ApplyZoneProfile refused while %s is partially enrolled: %v", name, err)
	for _, editor := range []struct {
		name string
		ctr  testcontainers.Container
	}{{"client-a", editorA}, {"client-b", editorB}} {
		if got := regionalRoutesTo(t, ctx, editor.ctr, joining); len(got) != 1 || !strings.Contains(got[0], "proto static") || strings.Contains(got[0], " via ") {
			t.Fatalf("%s has routes %q to %v, want only the caller's route", editor.name, got, joining)
		}
	}

	// Without the conflict a retried Start completes the enrollment.
	if err := regionalCallerRoute(ctx, editorA, "del", joining); err != nil {
		t.Fatal(err)
	}
	if err := joiner.Start(ctx); err != nil {
		t.Fatalf("retry the enrollment of %s: %v", name, err)
	}
	complete, err := cluster.Zones(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(complete.Unzoned) != 0 {
		t.Fatalf("unzoned after the retried enrollment: %v", complete.Unzoned)
	}
	if _, zone := harness.ZoneMemberByContainer(t, complete, joiner.GetContainerID()); zone != "b" {
		t.Fatalf("%s joined zone %s, want b", name, zone)
	}
	got := regionalRoutesTo(t, ctx, editorA, joining)
	if len(got) != 1 || !strings.Contains(got[0], "via "+routerA.PublicAddress.String()) || !strings.Contains(got[0], "proto 157") {
		t.Fatalf("client-a has routes %q to %v, want only the boundary's route via the zone a router %v", got, joining, routerA.PublicAddress)
	}
	if got := regionalRoutesTo(t, ctx, editorB, joining); len(got) != 1 || !strings.Contains(got[0], "proto static") {
		t.Fatalf("client-b has routes %q to %v, want only the caller's route", got, joining)
	}
	lease, err = fault.ApplyZoneProfile(ctx, cluster, "a", profile)
	if err != nil {
		t.Fatalf("ApplyZoneProfile after the completed enrollment: %v", err)
	}
	if err := lease.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("retried Start enrolled %s: client-a routes it via the zone a router, client-b keeps its own route, and the boundary shapes again", name)

	// A route the caller replaced is no longer the boundary's: the removal of
	// the client leaves it.
	if err := regionalCallerRoute(ctx, editorA, "replace", joining); err != nil {
		t.Fatal(err)
	}
	if err := joiner.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	for _, editor := range []struct {
		name string
		ctr  testcontainers.Container
	}{{"client-a", editorA}, {"client-b", editorB}} {
		if got := regionalRoutesTo(t, ctx, editor.ctr, joining); len(got) != 1 || !strings.Contains(got[0], "proto static") || strings.Contains(got[0], " via ") {
			t.Fatalf("after the removal of %s, %s has routes %q to %v, want only the caller's route", name, editor.name, got, joining)
		}
		if err := regionalCallerRoute(ctx, editor.ctr, "del", joining); err != nil {
			t.Fatal(err)
		}
	}
	after, err := cluster.Zones(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range after.Routes {
		if route.Destination == joining || route.Member == name {
			t.Fatalf("the removed %s keeps a route record %+v", name, route)
		}
	}
	t.Logf("removing %s left the caller's replaced route in client-a and its own route in client-b", name)
}

// checkNeverEnrolledContainerUnzoned attaches an ordinary container to the
// public bridge of peer, whose address there is addr, without the cluster's
// options: the boundary never enrolls it, so the layout lists it as unzoned
// and ApplyZoneProfile refuses to shape until it is removed.
func checkNeverEnrolledContainerUnzoned(t *testing.T, ctx context.Context, cluster *ceph.Container, peer testcontainers.Container, addr netip.Addr) {
	t.Helper()
	info, err := peer.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bridge := ""
	for name, endpoint := range info.NetworkSettings.Networks {
		if endpoint.IPAddress.Unmap() == addr {
			bridge = name
		}
	}
	if bridge == "" {
		t.Fatalf("no bridge of %.12s has %v", peer.GetContainerID(), addr)
	}
	plain, err := testcontainers.Run(ctx, harness.NetemImage(t), ceph.WithIdleEntrypoint(), network.WithNetworkName(nil, bridge))
	if plain != nil {
		testcontainers.CleanupContainer(t, plain)
	}
	if err != nil {
		t.Fatalf("start a plain container on the public bridge: %v", err)
	}
	name := regionalContainerName(t, ctx, plain)
	layout, err := cluster.Zones(ctx)
	if err != nil {
		t.Fatalf("Zones with the never-enrolled %s on the bridge: %v", name, err)
	}
	if !slices.ContainsFunc(layout.Unzoned, func(u string) bool { return strings.HasPrefix(u, name+" ") }) {
		t.Fatalf("the never-enrolled %s is not unzoned: %v", name, layout.Unzoned)
	}
	profile := fault.NetworkProfile{HelperImage: harness.NetemImage(t), Delay: regionalOneWay, Plane: ceph.PublicNetworkPlane}
	lease, err := fault.ApplyZoneProfile(ctx, cluster, "a", profile)
	if lease != nil {
		if restoreErr := lease.Restore(ctx); restoreErr != nil {
			t.Errorf("restore the zone a profile: %v", restoreErr)
		}
	}
	if !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), name) {
		t.Fatalf("ApplyZoneProfile with the never-enrolled %s: %v, want a refusal naming it", name, err)
	}
	t.Logf("a plain container on the bridge without WithZone is unzoned %v and ApplyZoneProfile refuses: %v", layout.Unzoned, err)
	if err := plain.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := cluster.Zones(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Unzoned) != 0 {
		t.Fatalf("unzoned after removing %s: %v", name, after.Unzoned)
	}
}

// regionalRouteEditor starts a helper with NET_ADMIN in the network
// namespace of id, to play the caller that owns routes there.
func regionalRouteEditor(t *testing.T, ctx context.Context, name, id string) testcontainers.Container {
	t.Helper()
	ctr, err := testcontainers.Run(ctx, harness.NetemImage(t), ceph.WithIdleEntrypoint(),
		testcontainers.WithConfigModifier(func(config *container.Config) { config.User = "0:0" }),
		testcontainers.WithHostConfigModifier(func(host *container.HostConfig) {
			host.NetworkMode = container.NetworkMode("container:" + id)
			host.CapDrop = []string{"ALL"}
			host.CapAdd = []string{"NET_ADMIN"}
		}))
	if ctr != nil {
		testcontainers.CleanupContainer(t, ctr)
	}
	if err != nil {
		t.Fatalf("start the route editor of %s: %v", name, err)
	}
	return ctr
}

// regionalCallerRoute adds, replaces or deletes the caller's route to dest:
// a direct route on the device that reaches dest, with protocol static.
func regionalCallerRoute(ctx context.Context, editor testcontainers.Container, verb string, dest netip.Addr) error {
	script := `dev=$(ip -o -4 route get "$1" | sed -n 's/.* dev \([^ ]*\).*/\1/p'); ip route "$2" "$1/32" dev "$dev" proto static`
	code, out, err := regionalExec(ctx, editor, "sh", "-ec", script, "sh", dest.String(), verb)
	if err != nil || code != 0 {
		return fmt.Errorf("ip route %s %v/32 as the caller: exit %d %v: %s", verb, dest, code, err, out)
	}
	return nil
}

// regionalRoutesTo lists the main table's routes to exactly dest/32.
func regionalRoutesTo(t *testing.T, ctx context.Context, editor testcontainers.Container, dest netip.Addr) []string {
	t.Helper()
	code, out, err := regionalExec(ctx, editor, "ip", "-o", "-4", "route", "show", "exact", dest.String()+"/32")
	if err != nil || code != 0 {
		t.Fatalf("list the routes to %v: exit %d %v: %s", dest, code, err, out)
	}
	if out == "" {
		return nil
	}
	return strings.Split(out, "\n")
}

// regionalAddressOnBridgeOf returns ctr's address on the bridge where peer
// has addr.
func regionalAddressOnBridgeOf(ctx context.Context, ctr, peer testcontainers.Container, addr netip.Addr) (netip.Addr, error) {
	peerInfo, err := peer.Inspect(ctx)
	if err != nil {
		return netip.Addr{}, err
	}
	info, err := ctr.Inspect(ctx)
	if err != nil {
		return netip.Addr{}, err
	}
	for name, endpoint := range peerInfo.NetworkSettings.Networks {
		if endpoint.IPAddress.Unmap() != addr {
			continue
		}
		if own := info.NetworkSettings.Networks[name]; own != nil && own.IPAddress.IsValid() {
			return own.IPAddress.Unmap(), nil
		}
	}
	return netip.Addr{}, fmt.Errorf("container %.12s is not on the bridge of %v", ctr.GetContainerID(), addr)
}

func regionalContainerName(t *testing.T, ctx context.Context, ctr testcontainers.Container) string {
	t.Helper()
	info, err := ctr.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimPrefix(info.Name, "/")
}
