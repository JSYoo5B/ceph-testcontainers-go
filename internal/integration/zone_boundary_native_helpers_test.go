//go:build integration && ci_optional && ci_batch_zone_boundary

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/network"
)

// zoneBoundaryLabel marks every container and network that one boundary
// creates with its run ID, so its teardown can prove that none is left.
const zoneBoundaryLabel = "org.testcontainers.ceph.zone-boundary"

// zoneProbeScript measures TCP connection setup to a host and port: one SYN
// and its SYN-ACK, so each sample is one round trip of the same small packets.
// It prints the samples in milliseconds and the errors as JSON.
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

// zoneBoundary is the caller-owned routed boundary between the logical zones
// of one cluster: a management bridge for its routers, one router per zone
// attached to every plane, one NET_ADMIN agent in each endpoint's network
// namespace, the routes it installed there and the netem leases on the
// routers. Everything it records is released by close, in reverse order,
// after partial setup too.
type zoneBoundary struct {
	t      *testing.T
	docker *testcontainers.DockerClient
	image  string
	runID  string
	// planes maps each plane to the cluster's bridge name.
	planes  map[string]string
	seed    *testcontainers.DockerNetwork
	routers map[string]*zoneRouter
	agents  map[string]*zoneAgent
	routes  []zoneRoute
	leases  []*fault.NetworkProfileLease
	closed  bool
}

// zoneRouter forwards what leaves Zone. Addrs and Ifaces are its address and
// interface on each plane.
type zoneRouter struct {
	Name, Zone string
	ctr        testcontainers.Container
	Addrs      map[string]netip.Addr
	Ifaces     map[string]string
}

// zoneAgent runs in the network namespace of one endpoint with NET_ADMIN, to
// install and remove its routes and to probe from it. Ifaces is the
// endpoint's interface on each plane.
type zoneAgent struct {
	Endpoint zoneEndpoint
	ctr      testcontainers.Container
	Ifaces   map[string]string
}

func newZoneBoundary(ctx context.Context, t *testing.T, docker *testcontainers.DockerClient, image string, planes map[string]string) (*zoneBoundary, error) {
	b := &zoneBoundary{t: t, docker: docker, image: image, runID: strconv.FormatInt(time.Now().UnixNano(), 36), planes: planes,
		routers: map[string]*zoneRouter{}, agents: map[string]*zoneAgent{}}
	seed, err := network.New(ctx, network.WithLabels(b.labels()), network.WithInternal())
	if seed != nil {
		b.seed = seed
	}
	if err != nil {
		return b, fmt.Errorf("create the routers' management bridge: %w", err)
	}
	return b, nil
}

func (b *zoneBoundary) labels() map[string]string {
	return map[string]string{zoneBoundaryLabel: b.runID}
}

// addRouter starts the router of zone on the management bridge and then
// attaches it to every plane. The plane interfaces are created after the
// sysctls are in place, so they inherit send_redirects 0 from default:
// a router that forwards back out of the interface a packet came in on must
// not tell the sender to go to the peer directly, which would bypass it.
func (b *zoneBoundary) addRouter(ctx context.Context, name, zone string) (*zoneRouter, error) {
	ctr, err := testcontainers.Run(ctx, b.image,
		network.WithNetwork([]string{name}, b.seed),
		ceph.WithIdleEntrypoint(),
		testcontainers.WithLabels(b.labels()),
		testcontainers.WithName(name+"-"+b.runID),
		testcontainers.WithHostConfigModifier(func(host *container.HostConfig) {
			host.CapDrop = []string{"ALL"}
			host.Sysctls = map[string]string{
				"net.ipv4.ip_forward":                  "1",
				"net.ipv4.conf.all.send_redirects":     "0",
				"net.ipv4.conf.default.send_redirects": "0",
			}
		}))
	if ctr == nil {
		return nil, fmt.Errorf("start router %s: %w", name, err)
	}
	router := &zoneRouter{Name: name, Zone: zone, ctr: ctr, Addrs: map[string]netip.Addr{}, Ifaces: map[string]string{}}
	b.routers[zone] = router
	if err != nil {
		return router, fmt.Errorf("start router %s: %w", name, err)
	}
	for _, plane := range zoneSortedKeys(b.planes) {
		if _, err := b.docker.NetworkConnect(ctx, b.planes[plane], mobycl.NetworkConnectOptions{Container: ctr.GetContainerID()}); err != nil {
			return router, fmt.Errorf("attach router %s to the %s plane: %w", name, plane, err)
		}
	}
	addrs, err := b.endpointAddrs(ctx, ctr.GetContainerID())
	if err != nil {
		return router, err
	}
	out, err := zoneExecOK(ctx, ctr, "ip", "-o", "-4", "addr", "show")
	if err != nil {
		return router, err
	}
	checks := []string{"/proc/sys/net/ipv4/ip_forward", "/proc/sys/net/ipv4/conf/all/send_redirects"}
	for _, plane := range zoneSortedKeys(b.planes) {
		addr, found := addrs[plane]
		if !found {
			return router, fmt.Errorf("router %s has no %s address", name, plane)
		}
		iface, err := interfaceWithAddress(out, addr)
		if err != nil {
			return router, fmt.Errorf("router %s: %w", name, err)
		}
		router.Addrs[plane], router.Ifaces[plane] = addr, iface
		checks = append(checks, "/proc/sys/net/ipv4/conf/"+iface+"/send_redirects")
	}
	values, err := zoneExecOK(ctx, ctr, append([]string{"cat"}, checks...)...)
	if err != nil {
		return router, err
	}
	if got := strings.Fields(values); len(got) != len(checks) || got[0] != "1" || slices.ContainsFunc(got[1:], func(v string) bool { return v != "0" }) {
		return router, fmt.Errorf("router %s forwarding sysctls %v are %v, want 1 and then 0", name, checks, got)
	}
	return router, nil
}

// endpointAddrs is the IPv4 address of a container on each plane it is
// attached to, from Docker.
func (b *zoneBoundary) endpointAddrs(ctx context.Context, id string) (map[string]netip.Addr, error) {
	inspection, err := b.docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
	if err != nil {
		return nil, err
	}
	if inspection.Container.NetworkSettings == nil {
		return nil, fmt.Errorf("container %s has no network settings", id)
	}
	addrs := map[string]netip.Addr{}
	for plane, name := range b.planes {
		if endpoint := inspection.Container.NetworkSettings.Networks[name]; endpoint != nil && endpoint.IPAddress.IsValid() {
			addrs[plane] = endpoint.IPAddress
		}
	}
	return addrs, nil
}

// addAgent starts a helper in the network namespace of ep's container with
// NET_ADMIN as its only capability. The endpoint's own image and capabilities
// stay unchanged.
func (b *zoneBoundary) addAgent(ctx context.Context, ep zoneEndpoint) (*zoneAgent, error) {
	ctr, err := testcontainers.Run(ctx, b.image,
		ceph.WithIdleEntrypoint(),
		testcontainers.WithLabels(b.labels()),
		testcontainers.WithConfigModifier(func(config *container.Config) { config.User = "0:0" }),
		testcontainers.WithHostConfigModifier(func(host *container.HostConfig) {
			host.NetworkMode = container.NetworkMode("container:" + ep.ContainerID)
			host.CapDrop = []string{"ALL"}
			host.CapAdd = []string{"NET_ADMIN"}
		}))
	if ctr == nil {
		return nil, fmt.Errorf("start the agent of %s: %w", ep.Name, err)
	}
	agent := &zoneAgent{Endpoint: ep, ctr: ctr, Ifaces: map[string]string{}}
	b.agents[ep.Name] = agent
	if err != nil {
		return agent, fmt.Errorf("start the agent of %s: %w", ep.Name, err)
	}
	out, err := zoneExecOK(ctx, ctr, "ip", "-o", "-4", "addr", "show")
	if err != nil {
		return agent, err
	}
	for _, plane := range zoneSortedPlanes(ep.Addrs) {
		iface, err := interfaceWithAddress(out, ep.Addrs[plane])
		if err != nil {
			return agent, fmt.Errorf("%s: %w", ep.Name, err)
		}
		agent.Ifaces[plane] = iface
	}
	return agent, nil
}

// installRoutes records each route before it adds it, so a failed or
// uncertain addition is still removed by close.
func (b *zoneBoundary) installRoutes(ctx context.Context, routes []zoneRoute) error {
	for _, r := range routes {
		agent := b.agents[r.Endpoint]
		if agent == nil {
			return fmt.Errorf("%s has no agent", r)
		}
		b.routes = append(b.routes, r)
		if _, err := zoneExecOK(ctx, agent.ctr, "ip", "route", "replace", r.Dest.String()+"/32", "via", r.Via.String(), "dev", agent.Ifaces[r.Plane]); err != nil {
			return fmt.Errorf("install %s: %w", r, err)
		}
	}
	return nil
}

func (b *zoneBoundary) routeLookup(ctx context.Context, endpoint string, dest netip.Addr) (zoneRouteLookup, error) {
	agent := b.agents[endpoint]
	if agent == nil {
		return zoneRouteLookup{}, fmt.Errorf("%s has no agent", endpoint)
	}
	out, err := zoneExecOK(ctx, agent.ctr, "ip", "route", "get", dest.String())
	if err != nil {
		return zoneRouteLookup{}, err
	}
	return parseRouteGet(out)
}

// probe measures count TCP connection setups from endpoint to target.
func (b *zoneBoundary) probe(ctx context.Context, endpoint string, target netip.AddrPort, count int, timeout time.Duration) (zoneProbeResult, error) {
	agent := b.agents[endpoint]
	if agent == nil {
		return zoneProbeResult{}, fmt.Errorf("%s has no agent", endpoint)
	}
	out, err := zoneExecOK(ctx, agent.ctr, "python3", "-c", zoneProbeScript, target.Addr().String(), strconv.Itoa(int(target.Port())),
		strconv.Itoa(count), strconv.FormatFloat(timeout.Seconds(), 'f', 3, 64), "0.05")
	if err != nil {
		return zoneProbeResult{}, err
	}
	return parseZoneProbe(count, out)
}

// zoneCounters are a router interface's kernel counters.
type zoneCounters struct {
	RxPackets, TxPackets, TxBytes uint64
}

func (c zoneCounters) delta(before zoneCounters) zoneCounters {
	return zoneCounters{RxPackets: c.RxPackets - before.RxPackets, TxPackets: c.TxPackets - before.TxPackets, TxBytes: c.TxBytes - before.TxBytes}
}

func (b *zoneBoundary) counters(ctx context.Context, zone, plane string) (zoneCounters, error) {
	router := b.routers[zone]
	if router == nil {
		return zoneCounters{}, fmt.Errorf("zone %s has no router", zone)
	}
	dir := "/sys/class/net/" + router.Ifaces[plane] + "/statistics/"
	out, err := zoneExecOK(ctx, router.ctr, "cat", dir+"rx_packets", dir+"tx_packets", dir+"tx_bytes")
	if err != nil {
		return zoneCounters{}, err
	}
	fields := strings.Fields(out)
	if len(fields) != 3 {
		return zoneCounters{}, fmt.Errorf("router %s counters %q", router.Name, out)
	}
	var values [3]uint64
	for i, f := range fields {
		if values[i], err = strconv.ParseUint(f, 10, 64); err != nil {
			return zoneCounters{}, err
		}
	}
	return zoneCounters{RxPackets: values[0], TxPackets: values[1], TxBytes: values[2]}, nil
}

// allCounters reads every router's counters on every plane, keyed by
// "zone/plane".
func (b *zoneBoundary) allCounters(ctx context.Context) (map[string]zoneCounters, error) {
	all := map[string]zoneCounters{}
	for _, zone := range zoneSortedKeys(b.routers) {
		for _, plane := range zoneSortedKeys(b.planes) {
			c, err := b.counters(ctx, zone, plane)
			if err != nil {
				return nil, err
			}
			all[zone+"/"+plane] = c
		}
	}
	return all, nil
}

// shape applies profile to what the router of zone sends on plane: the
// packets that leave zone through the boundary, and only those.
func (b *zoneBoundary) shape(ctx context.Context, zone, plane string, profile fault.NetworkProfile) (*fault.NetworkProfileLease, error) {
	router := b.routers[zone]
	if router == nil {
		return nil, fmt.Errorf("zone %s has no router", zone)
	}
	lease, err := fault.ApplyLinkProfile(ctx, router.ctr, b.planes[plane], profile)
	if lease != nil {
		b.leases = append(b.leases, lease)
	}
	return lease, err
}

// restoreLeases restores every held lease, newest first, and keeps the ones
// whose restoration failed.
func (b *zoneBoundary) restoreLeases(ctx context.Context) error {
	var errs []error
	var kept []*fault.NetworkProfileLease
	for i := len(b.leases) - 1; i >= 0; i-- {
		if err := b.leases[i].Restore(ctx); err != nil {
			errs = append(errs, fmt.Errorf("restore the profile on %s: %w", b.leases[i].ContainerID, err))
			kept = append([]*fault.NetworkProfileLease{b.leases[i]}, kept...)
		}
	}
	b.leases = kept
	return errors.Join(errs...)
}

// close releases what the boundary recorded, in reverse order of creation,
// and checks that nothing is left: no netem lease or qdisc on a router, no
// route through a router in an endpoint's namespace, and no container or
// network with the boundary's label. It runs after partial setup and is
// idempotent once it succeeded.
func (b *zoneBoundary) close(ctx context.Context) error {
	if b == nil || b.closed {
		return nil
	}
	var errs []error
	if err := b.restoreLeases(ctx); err != nil {
		errs = append(errs, err)
	}
	for _, zone := range zoneSortedKeys(b.routers) {
		router := b.routers[zone]
		for _, plane := range zoneSortedKeys(router.Ifaces) {
			out, err := zoneExecOK(ctx, router.ctr, "tc", "qdisc", "show", "dev", router.Ifaces[plane])
			if err != nil {
				errs = append(errs, err)
			} else if strings.Contains(out, "netem") || strings.Contains(out, "prio") {
				errs = append(errs, fmt.Errorf("router %s keeps a qdisc on %s: %s", router.Name, router.Ifaces[plane], out))
			}
		}
	}
	var vias []netip.Addr
	for _, router := range b.routers {
		for _, addr := range router.Addrs {
			vias = append(vias, addr)
		}
	}
	for i := len(b.routes) - 1; i >= 0; i-- {
		r := b.routes[i]
		agent := b.agents[r.Endpoint]
		if agent == nil {
			errs = append(errs, fmt.Errorf("cannot remove %s: no agent", r))
			continue
		}
		code, out, err := zoneExec(ctx, agent.ctr, "ip", "route", "del", r.Dest.String()+"/32", "via", r.Via.String())
		if err != nil || (code != 0 && !strings.Contains(out, "No such process")) {
			errs = append(errs, fmt.Errorf("remove %s: exit %d %q %v", r, code, out, err))
		}
	}
	for _, name := range zoneSortedKeys(b.agents) {
		agent := b.agents[name]
		out, err := zoneExecOK(ctx, agent.ctr, "ip", "-o", "-4", "route", "show")
		if err != nil {
			errs = append(errs, err)
		} else if left := routesThrough(out, vias); len(left) > 0 {
			errs = append(errs, fmt.Errorf("%s keeps routes through the boundary: %v", name, left))
		}
	}
	if len(errs) == 0 {
		b.routes = nil
	}
	for _, name := range zoneSortedKeys(b.agents) {
		if err := b.agents[name].ctr.Terminate(ctx, testcontainers.StopTimeout(0)); err != nil {
			errs = append(errs, fmt.Errorf("remove the agent of %s: %w", name, err))
		}
	}
	for _, zone := range zoneSortedKeys(b.routers) {
		if err := b.routers[zone].ctr.Terminate(ctx, testcontainers.StopTimeout(0)); err != nil {
			errs = append(errs, fmt.Errorf("remove router %s: %w", b.routers[zone].Name, err))
		}
	}
	if b.seed != nil {
		if err := b.seed.Remove(ctx); err != nil {
			errs = append(errs, fmt.Errorf("remove the management bridge: %w", err))
		}
	}
	filter := mobycl.Filters{}.Add("label", zoneBoundaryLabel+"="+b.runID)
	if ctrs, err := b.docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true, Filters: filter}); err != nil {
		errs = append(errs, err)
	} else if len(ctrs.Items) > 0 {
		errs = append(errs, fmt.Errorf("%d boundary containers are left", len(ctrs.Items)))
	}
	if nets, err := b.docker.NetworkList(ctx, mobycl.NetworkListOptions{Filters: filter}); err != nil {
		errs = append(errs, err)
	} else if len(nets.Items) > 0 {
		errs = append(errs, fmt.Errorf("%d boundary networks are left", len(nets.Items)))
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	b.agents, b.routers, b.seed, b.closed = map[string]*zoneAgent{}, map[string]*zoneRouter{}, nil, true
	return nil
}

func zoneExec(ctx context.Context, ctr testcontainers.Container, args ...string) (int, string, error) {
	execCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	code, r, err := ctr.Exec(execCtx, args, tcexec.Multiplexed())
	if err != nil {
		return code, "", err
	}
	out, err := io.ReadAll(r)
	return code, strings.TrimSpace(string(out)), err
}

func zoneExecOK(ctx context.Context, ctr testcontainers.Container, args ...string) (string, error) {
	code, out, err := zoneExec(ctx, ctr, args...)
	if err != nil {
		return out, fmt.Errorf("%s: %w", strings.Join(args[:min(len(args), 3)], " "), err)
	}
	if code != 0 {
		return out, fmt.Errorf("%s exited %d: %s", strings.Join(args[:min(len(args), 3)], " "), code, out)
	}
	return out, nil
}
