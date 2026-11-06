package ceph

import (
	"net/netip"
	"slices"
	"strings"
	"testing"
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
	// Zone a reaches it through zone a's router; the same-zone peer gets a
	// removal of any stale route.
	wantPeers := []string{
		"mon public 10.0.0.5 via 10.0.0.250",
		"osd0 cluster 10.1.0.5 via 10.1.0.250",
		"osd0 public 10.0.0.5 via 10.0.0.250",
		"osd1 cluster 10.1.0.5 via invalid IP",
		"osd1 public 10.0.0.5 via invalid IP",
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

func TestZoneRouteCommands(t *testing.T) {
	install := zoneRouteOp{dest: netip.MustParseAddr("10.0.0.5"), via: netip.MustParseAddr("10.0.0.250")}
	if got := zoneRouteCommand(install, "eth0"); got != "ip route replace 10.0.0.5/32 via 10.0.0.250 dev eth0" {
		t.Fatalf("install: %s", got)
	}
	removal := zoneRouteCommand(zoneRouteOp{dest: netip.MustParseAddr("10.0.0.5")}, "eth0")
	if !strings.HasPrefix(removal, "ip route del 10.0.0.5/32") || !strings.Contains(removal, `test -z "$(ip -o -4 route show 10.0.0.5/32)"`) {
		t.Fatalf("a removal must check that no route is left: %s", removal)
	}
}

func TestZoneInterfaceAndLeftoverRouteParsers(t *testing.T) {
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
	routes := "default via 10.0.0.1 dev eth0\n10.0.0.0/16 dev eth0 proto kernel scope link src 10.0.0.5\n10.0.0.4 via 10.0.0.250 dev eth0"
	left := routesThrough(routes, []netip.Addr{netip.MustParseAddr("10.0.0.250")})
	if len(left) != 1 || left[0] != "10.0.0.4 via 10.0.0.250 dev eth0" {
		t.Fatalf("leftover routes %v", left)
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
