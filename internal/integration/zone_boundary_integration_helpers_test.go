//go:build all || integration

package integration_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// A logical availability zone on one Docker engine is a set of endpoints of
// the cluster's own bridges. The fixture keeps one public and one cluster
// bridge, so endpoints of two zones share each plane's subnet. The boundary
// between them is a pair of run-owned routers per cluster, one for the traffic
// that leaves each zone, attached to every plane, and one /32 route per
// cross-zone peer address in every endpoint's network namespace. The helpers
// below decide the routes and judge the observations; the Docker operations
// are in the zone boundary batch's own files.

const (
	zonePublicPlane  = "public"
	zoneClusterPlane = "cluster"
)

// zoneEndpoint is one container of the cluster's bridges and its zone. Addrs
// holds its IPv4 address on each plane it is attached to.
type zoneEndpoint struct {
	Name, Zone, ContainerID string
	Addrs                   map[string]netip.Addr
}

// zoneRouterAddrs is the router that carries what leaves its zone: its name and
// its address on each plane.
type zoneRouterAddrs struct {
	Name  string
	Addrs map[string]netip.Addr
}

// zoneRoute sends what Endpoint sends to Dest on Plane through Via, the
// address of Router on that plane.
type zoneRoute struct {
	Endpoint, Plane, Router string
	Dest, Via               netip.Addr
}

func (r zoneRoute) String() string {
	return fmt.Sprintf("%s: %s/32 via %s (%s, %s plane)", r.Endpoint, r.Dest, r.Via, r.Router, r.Plane)
}

// planZoneRoutes returns, for every endpoint, one route per address of every
// endpoint of another zone on a plane that both share, through the router
// that egress names for the endpoint's own zone. Every forwarded packet then
// crosses exactly one router: the one of the zone it leaves. It refuses an
// endpoint without a router for its zone, a router without an address on a
// plane it must serve, an invalid or non-IPv4 address, and any address that
// two endpoints or routers share, because a duplicate would make a route
// ambiguous or point it at a router.
func planZoneRoutes(endpoints []zoneEndpoint, egress map[string]zoneRouterAddrs) ([]zoneRoute, error) {
	owner := map[netip.Addr]string{}
	claim := func(who string, addr netip.Addr) error {
		if !addr.IsValid() || !addr.Is4() {
			return fmt.Errorf("%s has no IPv4 address (%v)", who, addr)
		}
		if other, found := owner[addr]; found {
			return fmt.Errorf("%s and %s share %s", other, who, addr)
		}
		owner[addr] = who
		return nil
	}
	names := map[string]bool{}
	zones := map[string]bool{}
	for _, ep := range endpoints {
		if ep.Name == "" || ep.Zone == "" {
			return nil, fmt.Errorf("endpoint %+v lacks a name or zone", ep)
		}
		if names[ep.Name] {
			return nil, fmt.Errorf("endpoint %s is listed twice", ep.Name)
		}
		names[ep.Name] = true
		zones[ep.Zone] = true
		if _, found := egress[ep.Zone]; !found {
			return nil, fmt.Errorf("zone %s of %s has no egress router", ep.Zone, ep.Name)
		}
		for _, plane := range zoneSortedPlanes(ep.Addrs) {
			if err := claim(ep.Name+" "+plane, ep.Addrs[plane]); err != nil {
				return nil, err
			}
		}
	}
	if len(zones) < 2 {
		return nil, fmt.Errorf("%d zone(s) have no boundary", len(zones))
	}
	for _, zone := range zoneSortedKeys(egress) {
		router := egress[zone]
		for _, plane := range zoneSortedPlanes(router.Addrs) {
			if err := claim("router "+router.Name+" "+plane, router.Addrs[plane]); err != nil {
				return nil, err
			}
		}
	}
	var routes []zoneRoute
	for _, ep := range endpoints {
		router := egress[ep.Zone]
		for _, peer := range endpoints {
			if peer.Zone == ep.Zone {
				continue
			}
			for _, plane := range zoneSortedPlanes(ep.Addrs) {
				dest, shared := peer.Addrs[plane]
				if !shared {
					continue
				}
				via, served := router.Addrs[plane]
				if !served {
					return nil, fmt.Errorf("router %s has no %s address for %s to %s", router.Name, plane, ep.Name, peer.Name)
				}
				routes = append(routes, zoneRoute{Endpoint: ep.Name, Plane: plane, Router: router.Name, Dest: dest, Via: via})
			}
		}
	}
	return routes, nil
}

func zoneSortedPlanes(addrs map[string]netip.Addr) []string {
	return zoneSortedKeys(addrs)
}

func zoneSortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// zoneRouteLookup is what ip route get reports for one destination: the
// gateway (invalid for an on-link destination), the device and the source.
type zoneRouteLookup struct {
	Dest, Via, Src netip.Addr
	Dev            string
}

// parseRouteGet reads the first line of `ip route get <dest>`.
func parseRouteGet(out string) (zoneRouteLookup, error) {
	line, _, _ := strings.Cut(strings.TrimSpace(out), "\n")
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return zoneRouteLookup{}, errors.New("ip route get printed nothing")
	}
	var lookup zoneRouteLookup
	dest, err := netip.ParseAddr(fields[0])
	if err != nil {
		return zoneRouteLookup{}, fmt.Errorf("ip route get line %q: %w", line, err)
	}
	lookup.Dest = dest
	for i := 1; i+1 < len(fields); i++ {
		switch fields[i] {
		case "via":
			if lookup.Via, err = netip.ParseAddr(fields[i+1]); err != nil {
				return zoneRouteLookup{}, fmt.Errorf("ip route get line %q: %w", line, err)
			}
		case "src":
			if lookup.Src, err = netip.ParseAddr(fields[i+1]); err != nil {
				return zoneRouteLookup{}, fmt.Errorf("ip route get line %q: %w", line, err)
			}
		case "dev":
			lookup.Dev = fields[i+1]
		}
	}
	if lookup.Dev == "" {
		return zoneRouteLookup{}, fmt.Errorf("ip route get line %q names no device", line)
	}
	return lookup, nil
}

// checkRouteLookup compares a lookup with the expected path: through via, or
// on-link when via is invalid, on dev and from src.
func checkRouteLookup(got zoneRouteLookup, dest, via, src netip.Addr, dev string) error {
	switch {
	case got.Dest != dest:
		return fmt.Errorf("lookup is for %s, not %s", got.Dest, dest)
	case got.Via != via:
		if !via.IsValid() {
			return fmt.Errorf("%s goes through %s, not directly", dest, got.Via)
		}
		return fmt.Errorf("%s goes through %v, not %s", dest, got.Via, via)
	case got.Dev != dev:
		return fmt.Errorf("%s leaves through %s, not %s", dest, got.Dev, dev)
	case got.Src != src:
		return fmt.Errorf("%s is sent from %s, not %s", dest, got.Src, src)
	}
	return nil
}

// routesThrough returns the lines of `ip -o -4 route show` whose gateway is
// one of vias: the routes a boundary left behind.
func routesThrough(out string, vias []netip.Addr) []string {
	var left []string
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		for i := 0; i+1 < len(fields); i++ {
			if fields[i] != "via" {
				continue
			}
			if addr, err := netip.ParseAddr(fields[i+1]); err == nil && slices.Contains(vias, addr) {
				left = append(left, strings.TrimSpace(line))
			}
		}
	}
	return left
}

// interfaceWithAddress returns the interface that `ip -o -4 addr show` lists
// with addr.
func interfaceWithAddress(out string, addr netip.Addr) (string, error) {
	var found []string
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		for i := 0; i+1 < len(fields); i++ {
			if fields[i] != "inet" {
				continue
			}
			prefix, err := netip.ParsePrefix(fields[i+1])
			if err == nil && prefix.Addr() == addr && len(fields) > 1 {
				name, _, _ := strings.Cut(fields[1], "@")
				found = append(found, name)
			}
		}
	}
	switch len(found) {
	case 1:
		return found[0], nil
	case 0:
		return "", fmt.Errorf("no interface has %s", addr)
	default:
		return "", fmt.Errorf("interfaces %v all have %s", found, addr)
	}
}

// zoneProbeResult is one probe run: the requested count, the round trips that
// completed and the attempts that failed.
type zoneProbeResult struct {
	Requested int
	Samples   []float64
	Errors    []string
}

func parseZoneProbe(requested int, out string) (zoneProbeResult, error) {
	var raw struct {
		Samples []float64 `json:"samples_ms"`
		Errors  []string  `json:"errors"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &raw); err != nil {
		return zoneProbeResult{}, fmt.Errorf("probe output %q: %w", out, err)
	}
	if len(raw.Samples)+len(raw.Errors) != requested {
		return zoneProbeResult{}, fmt.Errorf("probe reported %d samples and %d errors for %d attempts", len(raw.Samples), len(raw.Errors), requested)
	}
	for _, s := range raw.Samples {
		if s < 0 || math.IsNaN(s) || math.IsInf(s, 0) {
			return zoneProbeResult{}, fmt.Errorf("probe sample %v is not a duration", s)
		}
	}
	return zoneProbeResult{Requested: requested, Samples: raw.Samples, Errors: raw.Errors}, nil
}

// zoneRTT summarizes the completed samples of one probe run: count, median
// and the 10th and 90th percentiles (nearest rank), in milliseconds.
type zoneRTT struct {
	Requested, OK    int
	Median, P10, P90 float64
}

func (s zoneRTT) String() string {
	if s.OK == 0 {
		return fmt.Sprintf("0/%d", s.Requested)
	}
	return fmt.Sprintf("%d/%d median %.3f ms (p10 %.3f, p90 %.3f)", s.OK, s.Requested, s.Median, s.P10, s.P90)
}

func summarizeZoneRTT(r zoneProbeResult) zoneRTT {
	s := zoneRTT{Requested: r.Requested, OK: len(r.Samples)}
	if s.OK == 0 {
		return s
	}
	sorted := slices.Clone(r.Samples)
	slices.Sort(sorted)
	rank := func(p float64) float64 {
		i := int(math.Ceil(p*float64(len(sorted)))) - 1
		return sorted[max(0, min(i, len(sorted)-1))]
	}
	if n := len(sorted); n%2 == 1 {
		s.Median = sorted[n/2]
	} else {
		s.Median = (sorted[n/2-1] + sorted[n/2]) / 2
	}
	s.P10, s.P90 = rank(0.10), rank(0.90)
	return s
}

func zoneComplete(s zoneRTT) error {
	if s.Requested == 0 || s.OK != s.Requested {
		return fmt.Errorf("%d of %d probes completed", s.OK, s.Requested)
	}
	return nil
}

// checkZoneControl says whether a path kept its baseline round trip: every
// probe completed and the median moved by no more than three times the
// baseline's p10-p90 spread, or 1 ms when the baseline is tighter.
func checkZoneControl(base, observed zoneRTT) error {
	if err := zoneComplete(base); err != nil {
		return fmt.Errorf("baseline: %w", err)
	}
	if err := zoneComplete(observed); err != nil {
		return err
	}
	bound := max(1.0, 3*(base.P90-base.P10))
	if moved := math.Abs(observed.Median - base.Median); moved > bound {
		return fmt.Errorf("median moved %.3f ms from the baseline %.3f ms, more than %.3f ms", moved, base.Median, bound)
	}
	return nil
}

// checkZoneShaped says whether a cross-zone path gained the configured
// outbound plus return delay once: every probe completed and the median grew
// by at least added less 1 ms, and by less than added plus a quarter of it, or
// plus 5 ms when that is more. A packet shaped twice, or a path that bypasses
// the boundary, falls outside.
func checkZoneShaped(base, observed zoneRTT, added time.Duration) error {
	if err := zoneComplete(base); err != nil {
		return fmt.Errorf("baseline: %w", err)
	}
	if err := zoneComplete(observed); err != nil {
		return err
	}
	want := float64(added) / float64(time.Millisecond)
	low, high := want-1, want+max(5, want/4)
	if grew := observed.Median - base.Median; grew < low || grew > high {
		return fmt.Errorf("median grew %.3f ms over the baseline %.3f ms, want %.0f ms (%.3f to %.3f)", grew, base.Median, want, low, high)
	}
	return nil
}

// checkZoneCut says whether no probe on a path completed.
func checkZoneCut(observed zoneRTT) error {
	if observed.Requested == 0 || observed.OK != 0 {
		return fmt.Errorf("%d of %d probes completed across a partitioned boundary", observed.OK, observed.Requested)
	}
	return nil
}

// entityAddrPort returns the address and port of the first entry of vector
// with protocol typ ("v2" or "v1").
func entityAddrPort(vector []ceph.EntityAddress, typ string) (netip.AddrPort, error) {
	for _, entry := range vector {
		if entry.Type != typ {
			continue
		}
		addr, err := netip.ParseAddrPort(entry.Addr)
		if err != nil {
			return netip.AddrPort{}, fmt.Errorf("entity address %q: %w", entry.Addr, err)
		}
		return addr, nil
	}
	return netip.AddrPort{}, fmt.Errorf("no %s entry in %+v", typ, vector)
}

// actingZones groups an acting set by the zone of each OSD. An OSD whose zone
// is unknown is an error, because the expectation would not be interpretable.
func actingZones(acting []int, zoneOf map[int]string) (map[string][]int, error) {
	zones := map[string][]int{}
	for _, id := range acting {
		zone, found := zoneOf[id]
		if !found {
			return nil, fmt.Errorf("osd.%d of acting set %v has no zone", id, acting)
		}
		zones[zone] = append(zones[zone], id)
	}
	return zones, nil
}

// classifyTwoSubnetPath names what a routed probe across two Docker bridges
// observed.
func classifyTwoSubnetPath(r zoneRTT) string {
	switch {
	case r.Requested == 0:
		return "not probed"
	case r.OK == r.Requested:
		return "forwarded"
	case r.OK == 0:
		return "dropped"
	default:
		return "partly forwarded"
	}
}
