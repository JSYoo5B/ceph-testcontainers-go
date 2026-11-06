//go:build all || integration

package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
)

// RegionalZoneConfig is the zone set of the regional tests: zones a and b,
// the quorum in a, and the netem helper as the boundary's image.
func RegionalZoneConfig(t *testing.T) ceph.ZoneConfig {
	t.Helper()
	return ceph.ZoneConfig{}.WithZones("a", "b").WithQuorumZone("a").WithHelperImage(NetemImage(t))
}

// NewRegionalCluster starts a cluster with run, the integration images and
// opts, and terminates it when the test ends. It attaches no client: a
// cluster with zones needs each client's zone.
func NewRegionalCluster(t *testing.T, run ClusterRun, opts ...testcontainers.ContainerCustomizer) *ceph.Container {
	t.Helper()
	image, base := IntegrationImages(t)
	cluster, err := run(t.Context(), image, append(base, opts...)...)
	if cluster != nil {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			if err := cluster.Terminate(ctx); err != nil {
				t.Errorf("terminate the regional cluster: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	return cluster
}

// RegionalClient starts an idle native client in zone and removes it when
// the test ends, before the cluster.
func RegionalClient(t *testing.T, ctx context.Context, cluster *ceph.Container, zone string, opts ...ceph.ClientContainerOptions) testcontainers.Container {
	t.Helper()
	image, _ := IntegrationImages(t)
	client, err := testcontainers.Run(ctx, NativeClientImage(t, image), cluster.WithClient(opts...), cluster.WithZone(zone), ceph.WithIdleEntrypoint(),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"ceph", "--connect-timeout", "5", "status"})))
	if client != nil {
		testcontainers.CleanupContainer(t, client)
	}
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// ZoneMember returns the member of layout named name and its zone.
func ZoneMember(t *testing.T, layout ceph.ZoneLayout, name string) (ceph.ZoneEndpoint, string) {
	t.Helper()
	for _, zone := range layout.Zones {
		for _, member := range zone.Members {
			if member.Name == name {
				return member, zone.Name
			}
		}
	}
	t.Fatalf("no zone member %s in %+v", name, layout.Zones)
	return ceph.ZoneEndpoint{}, ""
}

// ZoneMemberByContainer returns the member whose container is id.
func ZoneMemberByContainer(t *testing.T, layout ceph.ZoneLayout, id string) (ceph.ZoneEndpoint, string) {
	t.Helper()
	for _, zone := range layout.Zones {
		for _, member := range zone.Members {
			if member.ContainerID == id {
				return member, zone.Name
			}
		}
	}
	t.Fatalf("container %.12s is no zone member", id)
	return ceph.ZoneEndpoint{}, ""
}

// ZoneRouter returns the router of zone.
func ZoneRouter(t *testing.T, layout ceph.ZoneLayout, zone string) ceph.ZoneEndpoint {
	t.Helper()
	for _, z := range layout.Zones {
		if z.Name == zone {
			return z.Router
		}
	}
	t.Fatalf("no zone %s", zone)
	return ceph.ZoneEndpoint{}
}

// zoneProbeScript measures TCP connection setup to a host and port: one SYN
// and its SYN-ACK, so each sample is one round trip of the same small packets.
const zoneProbeScript = `import json, socket, sys, time
host, port, count, timeout, gap = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4]), float(sys.argv[5])
samples, errors = [], []
for _ in range(count):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    start = time.perf_counter()
    try:
        s.connect((host, port))
        samples.append((time.perf_counter() - start) * 1000.0)
    except OSError as e:
        errors.append(type(e).__name__ + ": " + str(e))
    finally:
        s.close()
    time.sleep(gap)
print(json.dumps({"samples_ms": samples, "errors": errors}))
`

// ZoneProbe is a test helper without capabilities in the network namespace of
// a zone member or router: it reads the kernel's route decisions and
// interface counters there and measures round trips from there. It is not an
// endpoint of the cluster's bridges.
type ZoneProbe struct {
	Name string
	ctr  testcontainers.Container
}

// NewZoneProbe starts a probe in the namespace of the container id, from the
// netem helper image, and removes it when the test ends.
func NewZoneProbe(t *testing.T, ctx context.Context, name, id string) *ZoneProbe {
	t.Helper()
	ctr, err := testcontainers.Run(ctx, NetemImage(t), ceph.WithIdleEntrypoint(),
		testcontainers.WithHostConfigModifier(func(host *container.HostConfig) {
			host.NetworkMode = container.NetworkMode("container:" + id)
			host.CapDrop = []string{"ALL"}
		}))
	if ctr != nil {
		testcontainers.CleanupContainer(t, ctr)
	}
	if err != nil {
		t.Fatalf("start the probe of %s: %v", name, err)
	}
	return &ZoneProbe{Name: name, ctr: ctr}
}

// Exec runs args in the probe and returns its trimmed output.
func (p *ZoneProbe) Exec(ctx context.Context, args ...string) (string, error) {
	execCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	code, r, err := p.ctr.Exec(execCtx, args, tcexec.Multiplexed())
	if err != nil {
		return "", err
	}
	out, err := io.ReadAll(r)
	text := strings.TrimSpace(string(out))
	if err != nil {
		return text, err
	}
	if code != 0 {
		return text, fmt.Errorf("%s in the probe of %s exited %d: %s", strings.Join(args[:min(len(args), 3)], " "), p.Name, code, text)
	}
	return text, nil
}

// ZoneLookup is what `ip route get` reports for one destination: the gateway
// (invalid for an on-link destination), the device and the source.
type ZoneLookup struct {
	Dest, Via, Src netip.Addr
	Dev            string
}

// RouteGet asks the probe's kernel how it reaches dest.
func (p *ZoneProbe) RouteGet(ctx context.Context, dest netip.Addr) (ZoneLookup, error) {
	out, err := p.Exec(ctx, "ip", "route", "get", dest.String())
	if err != nil {
		return ZoneLookup{}, err
	}
	return ParseRouteGet(out)
}

// ParseRouteGet reads the first line of `ip route get <dest>`.
func ParseRouteGet(out string) (ZoneLookup, error) {
	line, _, _ := strings.Cut(strings.TrimSpace(out), "\n")
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return ZoneLookup{}, errors.New("ip route get printed nothing")
	}
	var lookup ZoneLookup
	var err error
	if lookup.Dest, err = netip.ParseAddr(fields[0]); err != nil {
		return ZoneLookup{}, fmt.Errorf("ip route get line %q: %w", line, err)
	}
	for i := 1; i+1 < len(fields); i++ {
		switch fields[i] {
		case "via":
			if lookup.Via, err = netip.ParseAddr(fields[i+1]); err != nil {
				return ZoneLookup{}, fmt.Errorf("ip route get line %q: %w", line, err)
			}
		case "src":
			if lookup.Src, err = netip.ParseAddr(fields[i+1]); err != nil {
				return ZoneLookup{}, fmt.Errorf("ip route get line %q: %w", line, err)
			}
		case "dev":
			lookup.Dev = fields[i+1]
		}
	}
	if lookup.Dev == "" {
		return ZoneLookup{}, fmt.Errorf("ip route get line %q names no device", line)
	}
	return lookup, nil
}

// InterfaceBytes returns the name and the transmitted bytes of the interface
// that has addr in the probe's namespace.
func (p *ZoneProbe) InterfaceBytes(ctx context.Context, addr netip.Addr) (string, uint64, error) {
	addrs, err := p.Exec(ctx, "ip", "-o", "-4", "addr", "show")
	if err != nil {
		return "", 0, err
	}
	iface := ""
	for _, line := range strings.Split(addrs, "\n") {
		fields := strings.Fields(line)
		for i := 0; i+1 < len(fields); i++ {
			if prefix, err := netip.ParsePrefix(fields[i+1]); fields[i] == "inet" && err == nil && prefix.Addr() == addr {
				iface, _, _ = strings.Cut(fields[1], "@")
			}
		}
	}
	if iface == "" {
		return "", 0, fmt.Errorf("the probe of %s has no interface with %s", p.Name, addr)
	}
	dev, err := p.Exec(ctx, "cat", "/proc/net/dev")
	if err != nil {
		return "", 0, err
	}
	for _, line := range strings.Split(dev, "\n") {
		name, counters, found := strings.Cut(line, ":")
		if fields := strings.Fields(counters); found && strings.TrimSpace(name) == iface && len(fields) >= 9 {
			tx, err := strconv.ParseUint(fields[8], 10, 64)
			return iface, tx, err
		}
	}
	return "", 0, fmt.Errorf("no counters for %s in /proc/net/dev", iface)
}

// ZoneRTT summarizes one probe run: requested and completed round trips,
// their median and 10th and 90th percentiles (nearest rank) in
// milliseconds, and the errors of the failed attempts.
type ZoneRTT struct {
	Requested, OK    int
	Median, P10, P90 float64
	Errors           []string
}

func (s ZoneRTT) String() string {
	if s.OK == 0 {
		return fmt.Sprintf("0/%d", s.Requested)
	}
	return fmt.Sprintf("%d/%d median %.3f ms (p10 %.3f, p90 %.3f)", s.OK, s.Requested, s.Median, s.P10, s.P90)
}

// ConnectRTT measures count TCP connection setups to target.
func (p *ZoneProbe) ConnectRTT(ctx context.Context, target netip.AddrPort, count int, timeout time.Duration) (ZoneRTT, error) {
	out, err := p.Exec(ctx, "python3", "-c", zoneProbeScript, target.Addr().String(), strconv.Itoa(int(target.Port())),
		strconv.Itoa(count), strconv.FormatFloat(timeout.Seconds(), 'f', 3, 64), "0.05")
	if err != nil {
		return ZoneRTT{}, err
	}
	return SummarizeZoneProbe(count, out)
}

// SummarizeZoneProbe reads the probe script's output for count attempts.
func SummarizeZoneProbe(count int, out string) (ZoneRTT, error) {
	var raw struct {
		Samples []float64 `json:"samples_ms"`
		Errors  []string  `json:"errors"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &raw); err != nil {
		return ZoneRTT{}, fmt.Errorf("probe output %q: %w", out, err)
	}
	if len(raw.Samples)+len(raw.Errors) != count {
		return ZoneRTT{}, fmt.Errorf("probe reported %d samples and %d errors for %d attempts", len(raw.Samples), len(raw.Errors), count)
	}
	s := ZoneRTT{Requested: count, OK: len(raw.Samples), Errors: raw.Errors}
	for _, sample := range raw.Samples {
		if sample < 0 || math.IsNaN(sample) || math.IsInf(sample, 0) {
			return ZoneRTT{}, fmt.Errorf("probe sample %v is not a duration", sample)
		}
	}
	if s.OK == 0 {
		return s, nil
	}
	sorted := slices.Sorted(slices.Values(raw.Samples))
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
	return s, nil
}

func zoneComplete(s ZoneRTT) error {
	if s.Requested == 0 || s.OK != s.Requested {
		return fmt.Errorf("%d of %d probes completed %v", s.OK, s.Requested, s.Errors)
	}
	return nil
}

// CheckZoneControl says whether a path kept its baseline round trip: every
// probe completed and the median moved by no more than three times the
// baseline's p10-p90 spread, or 1 ms when the baseline is tighter.
func CheckZoneControl(base, observed ZoneRTT) error {
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

// CheckZoneShaped says whether a path gained the configured outbound plus
// return delay once: every probe completed and the median grew by at least
// added less 1 ms and by less than added plus a quarter of it, or plus 5 ms
// when that is more. A packet shaped twice, or a path that bypasses the
// boundary, falls outside.
func CheckZoneShaped(base, observed ZoneRTT, added time.Duration) error {
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

// CheckZoneCut says whether no probe on a path completed.
func CheckZoneCut(observed ZoneRTT) error {
	if observed.Requested == 0 || observed.OK != 0 {
		return fmt.Errorf("%d of %d probes completed across a partitioned boundary", observed.OK, observed.Requested)
	}
	return nil
}

// EntityAddrPort returns the address and port of the first entry of vector
// with protocol typ ("v2" or "v1").
func EntityAddrPort(vector []ceph.EntityAddress, typ string) (netip.AddrPort, error) {
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
