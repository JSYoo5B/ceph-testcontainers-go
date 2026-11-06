package ceph

import (
	"net/netip"
	"slices"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

func zoneAddrs(public, cluster string) map[NetworkPlane]netip.Addr {
	addrs := map[NetworkPlane]netip.Addr{PublicNetworkPlane: netip.MustParseAddr(public)}
	if cluster != "" {
		addrs[ClusterNetworkPlane] = netip.MustParseAddr(cluster)
	}
	return addrs
}

func zoneRouters() map[string]map[NetworkPlane]netip.Addr {
	return map[string]map[NetworkPlane]netip.Addr{
		"a": zoneAddrs("10.0.0.250", "10.1.0.250"),
		"b": zoneAddrs("10.0.0.251", "10.1.0.251"),
	}
}

func TestPlanZoneEnrollmentRoutesEveryCrossZonePacketThroughOneRouter(t *testing.T) {
	members := []zoneEndpoint{
		{id: "mon", name: "mon.a", zone: "a", addrs: zoneAddrs("10.0.0.2", "")},
		{id: "osd0", name: "osd.0", zone: "a", addrs: zoneAddrs("10.0.0.3", "10.1.0.3")},
		{id: "osd1", name: "osd.1", zone: "b", addrs: zoneAddrs("10.0.0.4", "10.1.0.4")},
	}
	joining := zoneEndpoint{id: "osd2", name: "osd.2", zone: "b", addrs: zoneAddrs("10.0.0.5", "10.1.0.5")}
	own, peers, err := planZoneEnrollment(joining, members, zoneRouters())
	if err != nil {
		t.Fatal(err)
	}
	got := func(ops []zoneRouteOp) []string {
		var out []string
		for _, op := range ops {
			if op.remove {
				t.Fatalf("enrollment planned a removal %+v", op)
			}
			out = append(out, op.holder+" "+string(op.plane)+" "+op.dest.String()+" via "+op.via.String())
		}
		return out
	}
	// The joining OSD reaches zone a through its own zone's router, on each
	// plane it shares with the peer; the MON has no cluster address.
	wantOwn := []string{
		"osd2 public 10.0.0.2 via 10.0.0.251",
		"osd2 cluster 10.1.0.3 via 10.1.0.251",
		"osd2 public 10.0.0.3 via 10.0.0.251",
	}
	if g := got(own); !slices.Equal(g, wantOwn) {
		t.Fatalf("own routes %v, want %v", g, wantOwn)
	}
	// Zone a reaches it through zone a's router. The same-zone peer gets
	// nothing, not even a removal: a route it has to the joining address is
	// not the boundary's.
	wantPeers := []string{
		"mon public 10.0.0.5 via 10.0.0.250",
		"osd0 cluster 10.1.0.5 via 10.1.0.250",
		"osd0 public 10.0.0.5 via 10.0.0.250",
	}
	if g := got(peers); !slices.Equal(g, wantPeers) {
		t.Fatalf("peer routes %v, want %v", g, wantPeers)
	}
}

func TestPlanZoneEnrollmentRefusesAmbiguousAddresses(t *testing.T) {
	routers := zoneRouters()
	member := zoneEndpoint{id: "x", name: "x", zone: "a", addrs: zoneAddrs("10.0.0.2", "")}
	for name, tc := range map[string]struct {
		joining zoneEndpoint
		routers map[string]map[NetworkPlane]netip.Addr
	}{
		"shared address":       {zoneEndpoint{id: "y", name: "y", zone: "b", addrs: zoneAddrs("10.0.0.2", "")}, routers},
		"router address":       {zoneEndpoint{id: "y", name: "y", zone: "b", addrs: zoneAddrs("10.0.0.250", "")}, routers},
		"zone without router":  {zoneEndpoint{id: "y", name: "y", zone: "c", addrs: zoneAddrs("10.0.0.3", "")}, routers},
		"router without plane": {zoneEndpoint{id: "y", name: "y", zone: "b", addrs: zoneAddrs("10.0.0.3", "")}, map[string]map[NetworkPlane]netip.Addr{"a": zoneAddrs("10.0.0.250", ""), "b": {}}},
		"IPv6 address":         {zoneEndpoint{id: "y", name: "y", zone: "b", addrs: map[NetworkPlane]netip.Addr{PublicNetworkPlane: netip.MustParseAddr("fd00::3")}}, routers},
	} {
		if _, _, err := planZoneEnrollment(tc.joining, []zoneEndpoint{member}, tc.routers); err == nil {
			t.Errorf("%s: planned without an error", name)
		}
	}
}

// TestZoneRouteCommands pins the ownership rules of the route commands: an
// installation adds with the boundary's protocol and refuses any other route
// to the destination instead of replacing it; a removal deletes only the
// recorded gateway, device and protocol. The kernel's matching of these
// selectors is exercised natively by TestRegionalZoneTopology.
func TestZoneRouteCommands(t *testing.T) {
	record := zoneRouteRecord{plane: PublicNetworkPlane, via: netip.MustParseAddr("10.0.0.250"), dev: "eth0"}
	dest := netip.MustParseAddr("10.0.0.5")
	owned := "10.0.0.5/32 via 10.0.0.250 dev eth0 proto 157"
	install := zoneRouteCommand(zoneRouteOp{dest: dest, via: record.via, dev: record.dev})
	for _, want := range []string{
		"n=$(ip -o -4 route show exact 10.0.0.5/32 | wc -l)",
		"o=$(ip -o -4 route show exact " + owned + " | wc -l)",
		"if [ $n -eq 0 ]; then ip route add " + owned + ";",
		"elif [ $n -ne $o ]; then",
		"exit 1; fi",
	} {
		if !strings.Contains(install, want) {
			t.Errorf("install %q lacks %q", install, want)
		}
	}
	if strings.Contains(install, "replace") {
		t.Errorf("install %q replaces a route it does not own", install)
	}
	removal := zoneRouteCommand(record.removal("x", dest))
	if want := "ip route del " + owned + " 2>/dev/null || true; test -z \"$(ip -o -4 route show exact " + owned + ")\""; removal != want {
		t.Errorf("removal %q, want %q", removal, want)
	}
}

func TestZoneMemberAdmittedOnlyWhenCompleteForItsStart(t *testing.T) {
	agent := &testcontainers.DockerContainer{}
	for _, tc := range []struct {
		name   string
		member *zoneMemberState
		want   bool
	}{
		{"untracked", nil, false},
		{"agent started, routes pending", &zoneMemberState{startedAt: "t1", agent: agent}, false},
		{"complete", &zoneMemberState{startedAt: "t1", agent: agent, complete: true}, true},
		{"complete for an earlier start", &zoneMemberState{startedAt: "t0", agent: agent, complete: true}, false},
		{"agent removed", &zoneMemberState{startedAt: "t1", complete: true}, false},
	} {
		if got := tc.member.enrolledFor("t1"); got != tc.want {
			t.Errorf("%s: admitted %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestZoneBoundaryLeavesOutContainersItNeverEnrolled covers a container on a
// plane's bridge that the boundary has no record of, such as one attached
// without Container.WithZone, and a partial enrollment: neither is admitted,
// and neither is inspected, so the boundary needs no Docker client here.
func TestZoneBoundaryLeavesOutContainersItNeverEnrolled(t *testing.T) {
	agent := &testcontainers.DockerContainer{}
	b := &zoneBoundary{enrolled: map[string]*zoneMemberState{
		"partial": {startedAt: "t1", agent: agent},
	}}
	for _, id := range []string{"never-enrolled", "partial"} {
		admitted, err := b.admittedLocked(t.Context(), id)
		if err != nil || admitted {
			t.Errorf("%s: admitted %v, %v; want not admitted without an error", id, admitted, err)
		}
	}
}

func TestZoneInterfaceParser(t *testing.T) {
	addrs := "1: lo    inet 127.0.0.1/8 scope host lo\n42: eth0@if43    inet 10.0.0.5/16 brd 10.0.255.255 scope global eth0\n44: eth1@if45    inet 10.1.0.5/16 scope global eth1"
	if iface, err := interfaceWithAddress(addrs, netip.MustParseAddr("10.1.0.5")); err != nil || iface != "eth1" {
		t.Fatalf("interface %q %v", iface, err)
	}
	if _, err := interfaceWithAddress(addrs, netip.MustParseAddr("10.2.0.5")); err == nil {
		t.Fatal("a missing address was found")
	}
	if _, err := interfaceWithAddress(addrs+"\n46: eth2    inet 10.1.0.5/16 scope global eth2", netip.MustParseAddr("10.1.0.5")); err == nil {
		t.Fatal("an address on two interfaces was accepted")
	}
}

func TestReporterSubtreesCountTheZonesReporters(t *testing.T) {
	osds := []osdZonePlacement{
		{0, OSDConfig{Zone: "a", Host: "a-0", Rack: "r", Root: "x"}},
		{1, OSDConfig{Zone: "a", Host: "a-1", Rack: "r", Root: "x"}},
		{2, OSDConfig{Zone: "b", Host: "b-0", Root: "x"}},
	}
	for _, tc := range []struct {
		zone, level string
		want        int
	}{
		{"a", "host", 2}, {"a", "osd", 2}, {"a", "rack", 1}, {"a", "zone", 1}, {"a", "root", 1},
		{"b", "host", 1}, {"b", "rack", 0}, {"a", "datacenter", 0}, {"c", "host", 0},
	} {
		if got := reporterSubtrees(tc.zone, tc.level, osds); got != tc.want {
			t.Errorf("zone %s level %s: %d, want %d", tc.zone, tc.level, got, tc.want)
		}
	}
}

// TestZonePeersLeaveOutMembersThatCannotHoldRoutes pins that a partial
// enrollment without an agent or an interface is not planned as a peer, so
// it does not fail another member's enrollment, while a partial enrollment
// that can hold routes is.
func TestZonePeersLeaveOutMembersThatCannotHoldRoutes(t *testing.T) {
	agent := &testcontainers.DockerContainer{}
	addrs := zoneAddrs("10.0.0.2", "10.1.0.2")
	both := map[NetworkPlane]string{PublicNetworkPlane: "eth0", ClusterNetworkPlane: "eth1"}
	b := &zoneBoundary{enrolled: map[string]*zoneMemberState{
		"self":      {endpoint: zoneEndpoint{id: "self", name: "self", addrs: addrs}, agent: agent, ifaces: both},
		"complete":  {endpoint: zoneEndpoint{id: "complete", name: "complete", addrs: addrs}, agent: agent, ifaces: both, complete: true},
		"routesdue": {endpoint: zoneEndpoint{id: "routesdue", name: "routesdue", addrs: addrs}, agent: agent, ifaces: both},
		"noagent":   {endpoint: zoneEndpoint{id: "noagent", name: "noagent", addrs: addrs}, ifaces: both},
		"noiface":   {endpoint: zoneEndpoint{id: "noiface", name: "noiface", addrs: addrs}, agent: agent, ifaces: map[NetworkPlane]string{PublicNetworkPlane: "eth0"}},
	}}
	var got []string
	for _, ep := range b.endpointsExcept("self") {
		got = append(got, ep.name)
	}
	if want := []string{"complete", "routesdue"}; !slices.Equal(got, want) {
		t.Fatalf("peers %v, want %v", got, want)
	}
}
