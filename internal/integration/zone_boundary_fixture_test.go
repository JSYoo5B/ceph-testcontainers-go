//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	cerrdefs "github.com/containerd/errdefs"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"

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
	base, err := harness.SummarizeZoneProbe(4, `{"samples_ms": [0.21, 0.19, 0.25, 0.2], "errors": []}`)
	if err != nil {
		t.Fatal(err)
	}
	if base.OK != 4 || math.Abs(base.Median-0.205) > 1e-9 || base.P10 != 0.19 || base.P90 != 0.25 {
		t.Fatalf("summary %+v", base)
	}
	for _, bad := range []string{`{"samples_ms": [0.2], "errors": []}`, `{"samples_ms": [-1, 1, 1, 1], "errors": []}`, `not json`} {
		if _, err := harness.SummarizeZoneProbe(4, bad); err == nil {
			t.Errorf("parsed %q", bad)
		}
	}
	summary := func(samples ...float64) zoneRTT {
		t.Helper()
		raw, err := json.Marshal(map[string]any{"samples_ms": samples, "errors": []string{}})
		if err != nil {
			t.Fatal(err)
		}
		s, err := harness.SummarizeZoneProbe(len(samples), string(raw))
		if err != nil {
			t.Fatal(err)
		}
		return s
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
	for name, check := range map[string]struct {
		errors  []string
		timeout bool
	}{
		"two timeouts":       {[]string{"TimeoutError: timed out", "TimeoutError: timed out"}, true},
		"missing evidence":   {[]string{"TimeoutError: timed out"}, false},
		"empty timeout":      {[]string{"TimeoutError: timed out", "TimeoutError: "}, false},
		"connection refused": {[]string{"TimeoutError: timed out", "ConnectionRefusedError: [Errno 111] Connection refused"}, false},
		"connection reset":   {[]string{"TimeoutError: timed out", "ConnectionResetError: [Errno 104] Connection reset by peer"}, false},
		"no route":           {[]string{"TimeoutError: timed out", "OSError: [Errno 101] Network is unreachable"}, false},
		"host unreachable":   {[]string{"TimeoutError: timed out", "OSError: [Errno 113] No route to host"}, false},
		"host down":          {[]string{"TimeoutError: timed out", "OSError: [Errno 112] Host is down"}, false},
	} {
		t.Run(name, func(t *testing.T) {
			got := zoneRTT{Requested: 2, Errors: check.errors}
			// Without the kernel state of the failed attempt, no gateway
			// makes a failure other than a timeout acceptable.
			for _, gateway := range []netip.Addr{{}, zoneAddr("172.19.0.3")} {
				if err := harness.CheckZoneCut(got, gateway); (err == nil) != check.timeout {
					t.Fatalf("partition verdict with the gateway %v: %v, want timeout=%t", gateway, err, check.timeout)
				}
			}
			if out := classifyTwoSubnetPath(got); (out == "timed out") != check.timeout {
				t.Fatalf("two-subnet outcome: %q, want timeout=%t", out, check.timeout)
			}
		})
	}
	if err := harness.CheckZoneCut(zoneRTT{Requested: 1, OK: 1}, zoneAddr("172.19.0.3")); err == nil {
		t.Fatal("accepted a completed probe across a partition")
	}
	if got := classifyTwoSubnetPath(summary(1, 1, 1)); got != "forwarded" {
		t.Fatal(got)
	}
}

// zoneCutProbe is the probe output of five attempts across a cut on the
// cluster plane, the third of which failed with errno and message while the
// kernel reported route for the target and neighbours as its neighbour table.
func zoneCutProbe(t *testing.T, errno, message, route, neighbours string) zoneRTT {
	t.Helper()
	timeout := "TimeoutError: timed out"
	raw, err := json.Marshal(map[string]any{
		"samples_ms": []float64{},
		"errors":     []string{timeout, timeout, message, timeout, timeout},
		"failures":   []map[string]any{{"error": 2, "errno": errno, "route": route, "neighbours": neighbours}},
	})
	if err != nil {
		t.Fatal(err)
	}
	s, err := harness.SummarizeZoneProbe(5, string(raw))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestZoneCutUnresolvedGateway covers the one failure besides a timeout that
// shows a dropping partition: EHOSTUNREACH raised by the sender's own kernel
// because the ARP replies of its zone router are dropped as well. The same
// errno with any other kernel state came over the wire or from a changed
// route, and stays rejected.
func TestZoneCutUnresolvedGateway(t *testing.T) {
	const (
		unreachable = "OSError: [Errno 113] No route to host"
		routed      = "172.19.0.5 via 172.19.0.3 dev eth1 src 172.19.0.6 uid 0 \n    cache \n"
		peers       = "172.19.0.4 dev eth1 lladdr 02:42:ac:13:00:04 REACHABLE \n172.18.0.3 dev eth0 FAILED \n"
	)
	gateway := zoneAddr("172.19.0.3")
	for name, check := range map[string]struct {
		errno, message, route, neighbours string
		gateway                           netip.Addr
		cut                               bool
	}{
		"gateway resolution failed":      {"EHOSTUNREACH", unreachable, routed, peers + "172.19.0.3 dev eth1 FAILED \n", gateway, true},
		"gateway resolution in progress": {"EHOSTUNREACH", unreachable, routed, peers + "172.19.0.3 dev eth1 INCOMPLETE \n", gateway, true},
		"flagged entry failed":           {"EHOSTUNREACH", unreachable, routed, "172.19.0.3 dev eth1 router FAILED \n", gateway, true},

		// A router that rejects with ICMP host-unreachable or
		// host-prohibited was reached through a resolved neighbour entry.
		"ICMP reject, gateway reachable": {"EHOSTUNREACH", unreachable, routed, peers + "172.19.0.3 dev eth1 lladdr 02:42:ac:13:00:03 REACHABLE \n", gateway, false},
		"ICMP reject, gateway stale":     {"EHOSTUNREACH", unreachable, routed, peers + "172.19.0.3 dev eth1 lladdr 02:42:ac:13:00:03 STALE \n", gateway, false},
		"ICMP reject, gateway delayed":   {"EHOSTUNREACH", unreachable, routed, peers + "172.19.0.3 dev eth1 lladdr 02:42:ac:13:00:03 DELAY \n", gateway, false},
		"ICMP reject, gateway probed":    {"EHOSTUNREACH", unreachable, routed, peers + "172.19.0.3 dev eth1 lladdr 02:42:ac:13:00:03 PROBE \n", gateway, false},
		"static gateway entry":           {"EHOSTUNREACH", unreachable, routed, "172.19.0.3 dev eth1 lladdr 02:42:ac:13:00:03 PERMANENT \n", gateway, false},
		"no gateway entry":               {"EHOSTUNREACH", unreachable, routed, peers, gateway, false},
		"failed entry of another host":   {"EHOSTUNREACH", unreachable, routed, peers + "172.19.0.33 dev eth1 FAILED \n", gateway, false},
		"failed entry on another device": {"EHOSTUNREACH", unreachable, routed, peers + "172.19.0.3 dev eth0 FAILED \n", gateway, false},
		"neighbour table unread":         {"EHOSTUNREACH", unreachable, routed, "ip did not run: [Errno 2] No such file or directory: 'ip'", gateway, false},

		// A changed route: the target is unreachable by route, on-link, or
		// behind another router whose own entry failed.
		"unreachable route":      {"EHOSTUNREACH", unreachable, "ip exited 2: RTNETLINK answers: No route to host", peers + "172.19.0.3 dev eth1 FAILED \n", gateway, false},
		"on-link route":          {"EHOSTUNREACH", unreachable, "172.19.0.5 dev eth1 src 172.19.0.6 uid 0 \n", peers + "172.19.0.5 dev eth1 FAILED \n172.19.0.3 dev eth1 FAILED \n", gateway, false},
		"another gateway":        {"EHOSTUNREACH", unreachable, "172.19.0.5 via 172.19.0.2 dev eth1 src 172.19.0.6 uid 0 \n", peers + "172.19.0.2 dev eth1 FAILED \n", gateway, false},
		"no gateway to resolve":  {"EHOSTUNREACH", unreachable, routed, peers + "172.19.0.3 dev eth1 FAILED \n", netip.Addr{}, false},
		"gateway of other plane": {"EHOSTUNREACH", unreachable, routed, peers + "172.19.0.3 dev eth1 FAILED \n", zoneAddr("172.18.0.3"), false},

		// Other errnos, even with a failed gateway entry recorded.
		"refused":             {"ECONNREFUSED", "ConnectionRefusedError: [Errno 111] Connection refused", routed, peers + "172.19.0.3 dev eth1 FAILED \n", gateway, false},
		"reset":               {"ECONNRESET", "ConnectionResetError: [Errno 104] Connection reset by peer", routed, peers + "172.19.0.3 dev eth1 FAILED \n", gateway, false},
		"network unreachable": {"ENETUNREACH", "OSError: [Errno 101] Network is unreachable", routed, peers + "172.19.0.3 dev eth1 FAILED \n", gateway, false},
		"host down":           {"EHOSTDOWN", "OSError: [Errno 112] Host is down", routed, peers + "172.19.0.3 dev eth1 FAILED \n", gateway, false},
		"unnamed errno":       {"", unreachable, routed, peers + "172.19.0.3 dev eth1 FAILED \n", gateway, false},
	} {
		t.Run(name, func(t *testing.T) {
			got := zoneCutProbe(t, check.errno, check.message, check.route, check.neighbours)
			err := harness.CheckZoneCut(got, check.gateway)
			if (err == nil) != check.cut {
				t.Fatalf("partition verdict %v for %s, want cut=%t", err, got, check.cut)
			}
			if classifyTwoSubnetPath(got) == "timed out" {
				t.Fatalf("a path without a recorded gateway classified %s as timed out", got)
			}
		})
	}

	// The record belongs to one attempt: it does not excuse another one.
	other := zoneCutProbe(t, "EHOSTUNREACH", unreachable, routed, "172.19.0.3 dev eth1 FAILED \n")
	other.Errors[3] = "ConnectionRefusedError: [Errno 111] Connection refused"
	if err := harness.CheckZoneCut(other, gateway); err == nil {
		t.Fatal("accepted a refusal next to an unresolved gateway")
	}
	other = zoneCutProbe(t, "EHOSTUNREACH", unreachable, routed, "172.19.0.3 dev eth1 FAILED \n")
	other.Errors[3] = unreachable
	if err := harness.CheckZoneCut(other, gateway); err == nil {
		t.Fatal("accepted a second EHOSTUNREACH without its own record")
	}
	for _, bad := range []string{
		`{"samples_ms": [], "errors": ["a"], "failures": [{"error": 1, "errno": "EHOSTUNREACH"}]}`,
		`{"samples_ms": [], "errors": ["a"], "failures": [{"error": -1, "errno": "EHOSTUNREACH"}]}`,
		`{"samples_ms": [], "errors": ["a"], "failures": [{"error": 0}, {"error": 0}]}`,
	} {
		if _, err := harness.SummarizeZoneProbe(1, bad); err == nil {
			t.Errorf("parsed %s", bad)
		}
	}
}

func TestParseNeighbour(t *testing.T) {
	table := "172.19.0.3 dev eth1 lladdr 02:42:ac:13:00:03 REACHABLE \n172.19.0.4 dev eth1 FAILED \n172.18.0.3 dev eth0 lladdr 02:42:ac:12:00:03 router STALE \nfe80::1 dev eth1 INCOMPLETE \n"
	for _, check := range []struct{ addr, dev, state string }{
		{"172.19.0.3", "eth1", "REACHABLE"}, {"172.19.0.4", "eth1", "FAILED"}, {"172.18.0.3", "eth0", "STALE"},
		{"172.19.0.4", "eth0", ""}, {"172.19.0.40", "eth1", ""}, {"172.19.0.5", "eth1", ""},
	} {
		if got := harness.ParseNeighbour(table, zoneAddr(check.addr), check.dev); got != check.state {
			t.Errorf("%s on %s: %q, want %q", check.addr, check.dev, got, check.state)
		}
	}
	if got := harness.ParseNeighbour("", zoneAddr("172.19.0.3"), "eth1"); got != "" {
		t.Errorf("empty table: %q", got)
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
	executed := zoneTwoSubnetOutcome{
		Mode:            "nat",
		ZoneAddressed:   zoneRTT{Requested: 5},
		SharedAddressed: zoneRTT{Requested: 5, Errors: slices.Repeat([]string{"TimeoutError: timed out"}, 5)},
	}.String()
	if !strings.Contains(executed, "probes executed") || strings.Contains(executed, "not executed") {
		t.Fatalf("executed record: %s", executed)
	}
	if !strings.Contains(executed, "zone-addressed through the router connect failed (0/5 [])") ||
		!strings.Contains(executed, "shared-addressed through the router timed out (0/5 [TimeoutError: timed out") {
		t.Fatalf("record must distinguish missing evidence from timeouts: %s", executed)
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
		{1, "connect errno 110\nmonclient(hunting): authenticate timed out after 15", nil, true, true},
		{1, "connect errno 110\nconnect errno 13", nil, true, false},
		{1, "connect errno 111", nil, true, false},
		{1, "connect errno 13", nil, true, false},
		{1, "stat: object not found", nil, true, false},
		{1, "connect errno 110", errors.New("Docker exec stream ended early"), true, false},
	} {
		if got := checkPartitionStat(tc.code, tc.out, tc.err, tc.cut, 4096) == nil; got != tc.want {
			t.Errorf("exit %d output %q err %v cut=%t accepted=%t, want %t", tc.code, tc.out, tc.err, tc.cut, got, tc.want)
		}
	}
}
