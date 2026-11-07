//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	cerrdefs "github.com/containerd/errdefs"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func zoneAddr(s string) netip.Addr { return netip.MustParseAddr(s) }

func zoneTestEndpoints() ([]zoneEndpoint, map[string]zoneRouterAddrs) {
	endpoints := []zoneEndpoint{
		{Name: "mon.a", Zone: "a", Addrs: map[string]netip.Addr{zonePublicPlane: zoneAddr("172.18.0.2")}},
		{Name: "osd.0", Zone: "a", Addrs: map[string]netip.Addr{zonePublicPlane: zoneAddr("172.18.0.4"), zoneClusterPlane: zoneAddr("172.19.0.2")}},
		{Name: "osd.2", Zone: "b", Addrs: map[string]netip.Addr{zonePublicPlane: zoneAddr("172.18.0.6"), zoneClusterPlane: zoneAddr("172.19.0.4")}},
		{Name: "client-b", Zone: "b", Addrs: map[string]netip.Addr{zonePublicPlane: zoneAddr("172.18.0.8")}},
	}
	egress := map[string]zoneRouterAddrs{
		"a": {Name: "zone-a-egress", Addrs: map[string]netip.Addr{zonePublicPlane: zoneAddr("172.18.0.9"), zoneClusterPlane: zoneAddr("172.19.0.5")}},
		"b": {Name: "zone-b-egress", Addrs: map[string]netip.Addr{zonePublicPlane: zoneAddr("172.18.0.10"), zoneClusterPlane: zoneAddr("172.19.0.6")}},
	}
	return endpoints, egress
}

// Every cross-zone address of a shared plane gets one route through the
// egress router of the sender's zone; same-zone peers and planes that one
// side lacks get none.
func TestPlanZoneRoutes(t *testing.T) {
	endpoints, egress := zoneTestEndpoints()
	routes, err := planZoneRoutes(endpoints, egress)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, r := range routes {
		got = append(got, r.String())
	}
	want := []string{
		"mon.a: 172.18.0.6/32 via 172.18.0.9 (zone-a-egress, public plane)",
		"mon.a: 172.18.0.8/32 via 172.18.0.9 (zone-a-egress, public plane)",
		"osd.0: 172.19.0.4/32 via 172.19.0.5 (zone-a-egress, cluster plane)",
		"osd.0: 172.18.0.6/32 via 172.18.0.9 (zone-a-egress, public plane)",
		"osd.0: 172.18.0.8/32 via 172.18.0.9 (zone-a-egress, public plane)",
		"osd.2: 172.18.0.2/32 via 172.18.0.10 (zone-b-egress, public plane)",
		"osd.2: 172.19.0.2/32 via 172.19.0.6 (zone-b-egress, cluster plane)",
		"osd.2: 172.18.0.4/32 via 172.18.0.10 (zone-b-egress, public plane)",
		"client-b: 172.18.0.2/32 via 172.18.0.10 (zone-b-egress, public plane)",
		"client-b: 172.18.0.4/32 via 172.18.0.10 (zone-b-egress, public plane)",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("routes:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestPlanZoneRoutesRefusals(t *testing.T) {
	for name, mutate := range map[string]func([]zoneEndpoint, map[string]zoneRouterAddrs) ([]zoneEndpoint, map[string]zoneRouterAddrs){
		"zone without router": func(e []zoneEndpoint, r map[string]zoneRouterAddrs) ([]zoneEndpoint, map[string]zoneRouterAddrs) {
			delete(r, "b")
			return e, r
		},
		"router without a needed plane": func(e []zoneEndpoint, r map[string]zoneRouterAddrs) ([]zoneEndpoint, map[string]zoneRouterAddrs) {
			r["a"] = zoneRouterAddrs{Name: "zone-a-egress", Addrs: map[string]netip.Addr{zonePublicPlane: zoneAddr("172.18.0.9")}}
			return e, r
		},
		"shared address": func(e []zoneEndpoint, r map[string]zoneRouterAddrs) ([]zoneEndpoint, map[string]zoneRouterAddrs) {
			e[3].Addrs[zonePublicPlane] = zoneAddr("172.18.0.9")
			return e, r
		},
		"duplicate name": func(e []zoneEndpoint, r map[string]zoneRouterAddrs) ([]zoneEndpoint, map[string]zoneRouterAddrs) {
			e[3].Name = "osd.2"
			return e, r
		},
		"one zone": func(e []zoneEndpoint, r map[string]zoneRouterAddrs) ([]zoneEndpoint, map[string]zoneRouterAddrs) {
			return e[:2], r
		},
		"IPv6 address": func(e []zoneEndpoint, r map[string]zoneRouterAddrs) ([]zoneEndpoint, map[string]zoneRouterAddrs) {
			e[0].Addrs[zonePublicPlane] = zoneAddr("fd00::2")
			return e, r
		},
	} {
		t.Run(name, func(t *testing.T) {
			endpoints, egress := zoneTestEndpoints()
			if routes, err := planZoneRoutes(mutate(endpoints, egress)); err == nil {
				t.Fatalf("planned %v", routes)
			}
		})
	}
}

func TestParseRouteGet(t *testing.T) {
	routed, err := parseRouteGet("172.18.0.4 via 172.18.0.5 dev eth0 src 172.18.0.2 uid 0 \n    cache ")
	if err != nil {
		t.Fatal(err)
	}
	if err := checkRouteLookup(routed, zoneAddr("172.18.0.4"), zoneAddr("172.18.0.5"), zoneAddr("172.18.0.2"), "eth0"); err != nil {
		t.Fatal(err)
	}
	direct, err := parseRouteGet("172.18.0.3 dev eth0 src 172.18.0.2 uid 0 \n    cache ")
	if err != nil {
		t.Fatal(err)
	}
	if err := checkRouteLookup(direct, zoneAddr("172.18.0.3"), netip.Addr{}, zoneAddr("172.18.0.2"), "eth0"); err != nil {
		t.Fatal(err)
	}
	for name, check := range map[string]error{
		"direct taken for routed": checkRouteLookup(direct, zoneAddr("172.18.0.3"), zoneAddr("172.18.0.5"), zoneAddr("172.18.0.2"), "eth0"),
		"routed taken for direct": checkRouteLookup(routed, zoneAddr("172.18.0.4"), netip.Addr{}, zoneAddr("172.18.0.2"), "eth0"),
		"wrong device":            checkRouteLookup(routed, zoneAddr("172.18.0.4"), zoneAddr("172.18.0.5"), zoneAddr("172.18.0.2"), "eth1"),
		"wrong source":            checkRouteLookup(routed, zoneAddr("172.18.0.4"), zoneAddr("172.18.0.5"), zoneAddr("172.18.0.9"), "eth0"),
	} {
		if check == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	for _, bad := range []string{"", "RTNETLINK answers: Network is unreachable", "172.18.0.4 via 172.18.0.5"} {
		if _, err := parseRouteGet(bad); err == nil {
			t.Errorf("parsed %q", bad)
		}
	}
}

func TestRoutesThroughAndInterfaces(t *testing.T) {
	routes := "default via 172.18.0.1 dev eth0 \n172.18.0.0/16 dev eth0 proto kernel scope link src 172.18.0.2 \n172.18.0.4 via 172.18.0.5 dev eth0 \n172.19.0.3 via 172.19.0.4 dev eth1 "
	left := routesThrough(routes, []netip.Addr{zoneAddr("172.18.0.5"), zoneAddr("172.19.0.4")})
	if !slices.Equal(left, []string{"172.18.0.4 via 172.18.0.5 dev eth0", "172.19.0.3 via 172.19.0.4 dev eth1"}) {
		t.Fatalf("left %q", left)
	}
	if left := routesThrough(routes, []netip.Addr{zoneAddr("172.18.0.9")}); len(left) != 0 {
		t.Fatalf("left %q", left)
	}
	addrs := "1: lo    inet 127.0.0.1/8 scope host lo\\       valid_lft forever preferred_lft forever\n" +
		"2: eth0    inet 172.18.0.2/16 brd 172.18.255.255 scope global eth0\\       valid_lft forever preferred_lft forever\n" +
		"3: eth1    inet 172.19.0.2/16 brd 172.19.255.255 scope global eth1\\       valid_lft forever preferred_lft forever"
	if iface, err := interfaceWithAddress(addrs, zoneAddr("172.19.0.2")); err != nil || iface != "eth1" {
		t.Fatalf("interface %q %v", iface, err)
	}
	if _, err := interfaceWithAddress(addrs, zoneAddr("172.19.0.3")); err == nil {
		t.Fatal("found an interface for an absent address")
	}
}

func TestZoneProbeAndRTT(t *testing.T) {
	r, err := parseZoneProbe(4, `{"samples_ms": [0.21, 0.19, 0.25, 0.2], "errors": []}`)
	if err != nil {
		t.Fatal(err)
	}
	base := summarizeZoneRTT(r)
	if base.OK != 4 || math.Abs(base.Median-0.205) > 1e-9 || base.P10 != 0.19 || base.P90 != 0.25 {
		t.Fatalf("summary %+v", base)
	}
	for _, bad := range []string{`{"samples_ms": [0.2], "errors": []}`, `{"samples_ms": [-1, 1, 1, 1], "errors": []}`, `not json`} {
		if _, err := parseZoneProbe(4, bad); err == nil {
			t.Errorf("parsed %q", bad)
		}
	}
	summary := func(samples ...float64) zoneRTT {
		return summarizeZoneRTT(zoneProbeResult{Requested: len(samples), Samples: samples})
	}
	shaped := summary(40.9, 40.8, 41.0, 40.95)
	if err := checkZoneShaped(base, shaped, 40*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := checkZoneShaped(base, shaped, 20*time.Millisecond); err == nil {
		t.Fatal("accepted a path shaped twice")
	}
	if err := checkZoneShaped(base, summary(0.2, 0.21, 0.2, 0.22), 40*time.Millisecond); err == nil {
		t.Fatal("accepted a path that bypassed the boundary")
	}
	if err := checkZoneShaped(base, zoneRTT{Requested: 4, OK: 3, Median: 40.9}, 40*time.Millisecond); err == nil {
		t.Fatal("accepted a run with a failed probe")
	}
	if err := checkZoneControl(base, summary(0.3, 0.21, 0.19, 0.2)); err != nil {
		t.Fatal(err)
	}
	if err := checkZoneControl(base, summary(20.2, 20.21, 20.19, 20.2)); err == nil {
		t.Fatal("accepted a delayed control")
	}
	if err := checkZoneCut(zoneRTT{Requested: 5}); err != nil {
		t.Fatal(err)
	}
	if err := checkZoneCut(zoneRTT{Requested: 5, OK: 1}); err == nil {
		t.Fatal("accepted a completed probe across a partition")
	}
	if got := classifyTwoSubnetPath(zoneRTT{Requested: 5}); got != "dropped" {
		t.Fatal(got)
	}
	if got := classifyTwoSubnetPath(summary(1, 1, 1)); got != "forwarded" {
		t.Fatal(got)
	}
}

func TestZoneTwoSubnetGatewayModeAndRecord(t *testing.T) {
	// The bridge driver's refusal, as the engine client returns it for a 400.
	refusal := func(mode string) error {
		return fmt.Errorf("%w: Error response from daemon: failed to parse com.docker.network.bridge.gateway_mode_ipv4 value: %s (unknown gateway mode %s)",
			cerrdefs.ErrInvalidArgument, mode, mode)
	}
	if !zoneGatewayModeUnavailable("routed", refusal("routed")) {
		t.Fatal("did not recognize the engine refusing the requested gateway mode")
	}
	for name, err := range map[string]error{
		"no error":         nil,
		"another mode":     refusal("nat-unprotected"),
		"pool exhaustion":  fmt.Errorf("%w: Error response from daemon: all predefined address pools have been fully subnetted", cerrdefs.ErrInvalidArgument),
		"API timeout":      fmt.Errorf("create bridge: %w", context.DeadlineExceeded),
		"unclassified 500": errors.New("Error response from daemon: failed to parse com.docker.network.bridge.gateway_mode_ipv4 value: nat (unknown gateway mode nat)"),
	} {
		if zoneGatewayModeUnavailable("nat", err) {
			t.Errorf("%s: treated as an unavailable gateway mode: %v", name, err)
		}
	}
	if got := classifyTwoSubnetPath(zoneRTT{}); got != "not executed" {
		t.Fatalf("a probe that did not run is %q", got)
	}
	unavailable := zoneTwoSubnetOutcome{Mode: "routed", Unavailable: refusal("routed")}.String()
	if !strings.Contains(unavailable, "unavailable") || !strings.Contains(unavailable, "not executed") || strings.Contains(unavailable, "dropped") {
		t.Fatalf("unavailable record: %s", unavailable)
	}
	executed := zoneTwoSubnetOutcome{Mode: "nat", ZoneAddressed: zoneRTT{Requested: 5}, SharedAddressed: zoneRTT{Requested: 5}}.String()
	if !strings.Contains(executed, "probes executed") || !strings.Contains(executed, "dropped (0/5)") || strings.Contains(executed, "not executed") {
		t.Fatalf("executed record: %s", executed)
	}
}

func TestZoneEntityAddressesAndActingZones(t *testing.T) {
	vector := []ceph.EntityAddress{{Type: "v2", Addr: "172.18.0.4:6802", Nonce: 7}, {Type: "v1", Addr: "172.18.0.4:6803", Nonce: 7}}
	if got, err := entityAddrPort(vector, "v2"); err != nil || got != netip.MustParseAddrPort("172.18.0.4:6802") {
		t.Fatalf("%v %v", got, err)
	}
	if _, err := entityAddrPort(vector[1:], "v2"); err == nil {
		t.Fatal("found a v2 entry in a v1-only vector")
	}
	zones, err := actingZones([]int{2, 0}, map[int]string{0: "a", 1: "a", 2: "b"})
	if err != nil || !slices.Equal(zones["a"], []int{0}) || !slices.Equal(zones["b"], []int{2}) {
		t.Fatalf("%v %v", zones, err)
	}
	if _, err := actingZones([]int{3}, map[int]string{0: "a"}); err == nil {
		t.Fatal("accepted an OSD without a zone")
	}
}

func TestRegionalPartitionStatEvidence(t *testing.T) {
	for _, tc := range []struct {
		code      int
		out       string
		err       error
		cut, want bool
	}{
		{0, "stat size 4096", nil, false, true},
		{0, "stat size 4095", nil, false, false},
		{0, "stat size 4096", nil, true, false},
		{1, "connect errno 110", nil, true, true},
		{1, "connect errno 111", nil, true, true},
		{1, "connect errno 13", nil, true, false},
		{1, "stat: object not found", nil, true, false},
		{1, "connect errno 110", errors.New("Docker exec stream ended early"), true, false},
	} {
		if got := checkPartitionStat(tc.code, tc.out, tc.err, tc.cut, 4096) == nil; got != tc.want {
			t.Errorf("exit %d output %q err %v cut=%t accepted=%t, want %t", tc.code, tc.out, tc.err, tc.cut, got, tc.want)
		}
	}
}
