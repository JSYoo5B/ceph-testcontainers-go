package ceph

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
)

// Labels of the zone boundary. zoneLabel and zoneMemberLabel are on every
// zone member: its zone and its name. zoneBoundaryLabel, with the cluster's
// owner ID, is on every router, route agent and on the routers' management
// bridge, so a teardown can show that none is left.
const (
	zoneLabel         = "org.testcontainers.ceph.zone"
	zoneMemberLabel   = "org.testcontainers.ceph.zone.member"
	zoneBoundaryLabel = "org.testcontainers.ceph.zone.boundary"
)

// zoneExecTimeout bounds one command in a router or a route agent.
const zoneExecTimeout = time.Minute

// zoneBoundary is the routed boundary between the zones of one cluster: a
// management bridge for the routers, one router per zone attached to every
// plane, and for every member one route agent in its network namespace with
// the routes the boundary installed there. It has its own leaf mutex: it is
// taken with or without Container.mu and never takes it. Every container and
// route is recorded before it is created, so close releases what a failed or
// uncertain step left.
type zoneBoundary struct {
	mu       sync.Mutex
	settings zoneSettings
	owner    string
	// planes maps each plane to its bridge: public always, cluster only with a
	// separate replication bridge.
	planes   map[NetworkPlane]string
	docker   *testcontainers.DockerClient
	seed     *testcontainers.DockerNetwork
	routers  map[string]*zoneRouter
	enrolled map[string]*zoneMemberState
	closed   bool
}

// zoneRouter forwards what leaves its zone. addrs and ifaces are its address
// and interface on each plane.
type zoneRouter struct {
	zone   string
	ctr    testcontainers.Container
	addrs  map[NetworkPlane]netip.Addr
	ifaces map[NetworkPlane]string
}

// zoneMemberState is one tracked container: its identity, the start it is
// tracked for, its address and interface on each plane, its route agent and
// the routes installed in its namespace, by destination. A member is tracked
// from its first enrollment step, so close releases a partial enrollment, but
// it is a member of its zone only once complete: after its agent, its own
// routes and the other members' routes to it are all in place.
type zoneMemberState struct {
	endpoint  zoneEndpoint
	startedAt string
	ifaces    map[NetworkPlane]string
	agent     testcontainers.Container
	routes    map[netip.Addr]zoneRouteRecord
	complete  bool
}

// enrolledFor reports whether m is a complete enrollment of the start at
// startedAt. A partial enrollment or one of an earlier start is not.
func (m *zoneMemberState) enrolledFor(startedAt string) bool {
	return m != nil && m.complete && m.agent != nil && m.startedAt == startedAt
}

// zoneEndpoint is a container of the planes and its zone.
type zoneEndpoint struct {
	id, name, zone string
	addrs          map[NetworkPlane]netip.Addr
}

// zoneRouteRecord is one installed route: through via on plane.
type zoneRouteRecord struct {
	plane NetworkPlane
	via   netip.Addr
}

// zoneRouteOp changes the route to dest in holder's namespace: through via,
// or, with an invalid via, its removal.
type zoneRouteOp struct {
	holder string
	plane  NetworkPlane
	dest   netip.Addr
	via    netip.Addr
}

// newZoneBoundary starts the boundary of a cluster whose bridges exist: the
// management bridge and one router per zone. A returned boundary owns what
// was created, also with an error.
func newZoneBoundary(ctx context.Context, settings zoneSettings, owner string, planes map[NetworkPlane]string) (*zoneBoundary, error) {
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return nil, fmt.Errorf("zone boundary Docker client: %w", err)
	}
	b := &zoneBoundary{settings: settings, owner: owner, planes: maps.Clone(planes), docker: docker,
		routers: map[string]*zoneRouter{}, enrolled: map[string]*zoneMemberState{}}
	seed, err := network.New(ctx, network.WithLabels(b.labels()), network.WithInternal())
	if seed != nil {
		b.seed = seed
	}
	if err != nil {
		return b, fmt.Errorf("create the zone routers' management bridge: %w", err)
	}
	for _, zone := range settings.names {
		if err := b.addRouter(ctx, zone); err != nil {
			return b, err
		}
	}
	return b, nil
}

func (b *zoneBoundary) labels() map[string]string {
	return map[string]string{zoneBoundaryLabel: b.owner, clusterOwnerLabel: b.owner}
}

func (b *zoneBoundary) sortedPlanes() []NetworkPlane {
	return slices.Sorted(maps.Keys(b.planes))
}

// addRouter starts the router of zone on the management bridge and then
// attaches it to every plane, so that the plane interfaces are created after
// the sysctls and inherit send_redirects 0 from default: a router that
// forwards a packet back out of the interface it came in on must not tell the
// sender to reach the peer directly, which would bypass it.
func (b *zoneBoundary) addRouter(ctx context.Context, zone string) error {
	ctr, err := runContainer(ctx, b.settings.helperImage,
		network.WithNetwork([]string{"zone-" + zone}, b.seed),
		WithIdleEntrypoint(),
		testcontainers.WithLabels(b.labels()),
		testcontainers.WithLabels(map[string]string{zoneLabel: zone}),
		testcontainers.WithHostConfigModifier(func(host *container.HostConfig) {
			host.CapDrop = []string{"ALL"}
			host.Sysctls = map[string]string{
				"net.ipv4.ip_forward":                  "1",
				"net.ipv4.conf.all.send_redirects":     "0",
				"net.ipv4.conf.default.send_redirects": "0",
			}
		}))
	if ctr == nil {
		return fmt.Errorf("start the router of zone %s: %w", zone, err)
	}
	router := &zoneRouter{zone: zone, ctr: ctr, addrs: map[NetworkPlane]netip.Addr{}, ifaces: map[NetworkPlane]string{}}
	b.routers[zone] = router
	if err != nil {
		return fmt.Errorf("start the router of zone %s: %w", zone, err)
	}
	for _, plane := range b.sortedPlanes() {
		if _, err := b.docker.NetworkConnect(ctx, b.planes[plane], mobycl.NetworkConnectOptions{Container: ctr.GetContainerID()}); err != nil {
			return fmt.Errorf("attach the router of zone %s to the %s plane: %w", zone, plane, err)
		}
	}
	addrs, _, err := b.inspectEndpoint(ctx, ctr.GetContainerID())
	if err != nil {
		return err
	}
	out, err := b.exec(ctx, ctr, "ip", "-o", "-4", "addr", "show")
	if err != nil {
		return fmt.Errorf("router of zone %s: %w", zone, err)
	}
	checks := []string{"/proc/sys/net/ipv4/ip_forward", "/proc/sys/net/ipv4/conf/all/send_redirects"}
	for _, plane := range b.sortedPlanes() {
		addr, found := addrs[plane]
		if !found {
			return fmt.Errorf("the router of zone %s has no IPv4 address on the %s plane", zone, plane)
		}
		iface, err := interfaceWithAddress(out, addr)
		if err != nil {
			return fmt.Errorf("router of zone %s: %w", zone, err)
		}
		router.addrs[plane], router.ifaces[plane] = addr, iface
		checks = append(checks, "/proc/sys/net/ipv4/conf/"+iface+"/send_redirects")
	}
	values, err := b.exec(ctx, ctr, append([]string{"cat"}, checks...)...)
	if err != nil {
		return fmt.Errorf("router of zone %s: %w", zone, err)
	}
	if got := strings.Fields(values); len(got) != len(checks) || got[0] != "1" || slices.ContainsFunc(got[1:], func(v string) bool { return v != "0" }) {
		return fmt.Errorf("the router of zone %s has forwarding sysctls %v = %v, want 1 and then 0", zone, checks, got)
	}
	return nil
}

// inspectEndpoint returns a container's IPv4 address on each plane it is
// attached to, its labels and the time of its current start.
func (b *zoneBoundary) inspectEndpoint(ctx context.Context, id string) (map[NetworkPlane]netip.Addr, zoneInspection, error) {
	inspection, err := b.docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
	if err != nil {
		return nil, zoneInspection{}, fmt.Errorf("inspect zone endpoint %.12s: %w", id, err)
	}
	info := zoneInspection{name: strings.TrimPrefix(inspection.Container.Name, "/")}
	if inspection.Container.Config != nil {
		info.labels = inspection.Container.Config.Labels
	}
	if state := inspection.Container.State; state != nil {
		info.running, info.startedAt = state.Running, state.StartedAt
	}
	addrs := map[NetworkPlane]netip.Addr{}
	if settings := inspection.Container.NetworkSettings; settings != nil {
		for plane, name := range b.planes {
			if endpoint := settings.Networks[name]; endpoint != nil && endpoint.IPAddress.IsValid() {
				addrs[plane] = endpoint.IPAddress.Unmap()
			}
		}
	}
	return addrs, info, nil
}

// zoneInspection is what enrollment reads from a container besides its
// addresses.
type zoneInspection struct {
	name, startedAt string
	running         bool
	labels          map[string]string
}

// enroll makes a started container a member of the zone its labels name: a
// route agent in its network namespace, a route to every member of another
// zone through its own zone's router, and in every other member a route to
// it through that member's router, or, within the zone, no route. It runs on
// every start, because a start gives the container a new network namespace,
// and does nothing for a start it already enrolled completely. A failed step
// leaves the member tracked but not admitted (see unzonedLocked), and a retry
// for the same start resumes it.
func (b *zoneBoundary) enroll(ctx context.Context, ctr testcontainers.Container) error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return errClusterTerminated
	}
	id := ctr.GetContainerID()
	addrs, info, err := b.inspectEndpoint(ctx, id)
	if err != nil {
		return err
	}
	zone := info.labels[zoneLabel]
	if zone == "" {
		return invalidRequest("container %s joins a cluster with zones without a zone: add Container.WithZone", info.name)
	}
	if !b.settings.declares(zone) {
		return invalidRequest("container %s names zone %q, not one of the zones %v", info.name, zone, b.settings.names)
	}
	if len(addrs) == 0 {
		return invalidRequest("zone member %s has no IPv4 address on the cluster's bridges", info.name)
	}
	name := info.labels[zoneMemberLabel]
	if name == "" {
		name = info.name
	}
	member := b.enrolled[id]
	switch {
	case member.enrolledFor(info.startedAt):
		return nil
	case member != nil && member.startedAt == info.startedAt:
		// A retry of a partial enrollment of this start: the namespace keeps
		// the agent and the routes already installed, and installing them
		// again is a no-op.
	default:
		if member != nil {
			// The previous start's namespace and its routes are gone.
			if err := b.dropMember(ctx, id); err != nil {
				return err
			}
		}
		if err := b.dropStaleMembers(ctx, id, addrs); err != nil {
			return err
		}
		member = &zoneMemberState{endpoint: zoneEndpoint{id: id, name: name, zone: zone, addrs: addrs}, startedAt: info.startedAt,
			ifaces: map[NetworkPlane]string{}, routes: map[netip.Addr]zoneRouteRecord{}}
		b.enrolled[id] = member
	}
	if member.agent == nil {
		agent, err := runContainer(ctx, b.settings.helperImage,
			WithIdleEntrypoint(),
			testcontainers.WithLabels(b.labels()),
			testcontainers.WithConfigModifier(func(config *container.Config) { config.User = "0:0" }),
			testcontainers.WithHostConfigModifier(func(host *container.HostConfig) {
				host.NetworkMode = container.NetworkMode("container:" + id)
				host.CapDrop = []string{"ALL"}
				host.CapAdd = []string{"NET_ADMIN"}
			}))
		if agent != nil {
			member.agent = agent
		}
		if err != nil {
			return fmt.Errorf("start the route agent of %s: %w", name, err)
		}
	}
	out, err := b.exec(ctx, member.agent, "ip", "-o", "-4", "addr", "show")
	if err != nil {
		return fmt.Errorf("route agent of %s: %w", name, err)
	}
	for plane, addr := range addrs {
		iface, err := interfaceWithAddress(out, addr)
		if err != nil {
			return fmt.Errorf("route agent of %s: %w", name, err)
		}
		member.ifaces[plane] = iface
	}
	own, peers, err := planZoneEnrollment(member.endpoint, b.endpointsExcept(id), b.routerAddrs())
	if err != nil {
		return err
	}
	if err := b.applyRoutes(ctx, own); err != nil {
		return err
	}
	if err := b.applyRoutes(ctx, peers); err != nil {
		return err
	}
	member.complete = true
	return nil
}

// endpointsExcept lists the members other than id, sorted by name.
func (b *zoneBoundary) endpointsExcept(id string) []zoneEndpoint {
	var out []zoneEndpoint
	for other, member := range b.enrolled {
		if other != id {
			out = append(out, member.endpoint)
		}
	}
	slices.SortFunc(out, func(a, b zoneEndpoint) int { return strings.Compare(a.name, b.name) })
	return out
}

func (b *zoneBoundary) routerAddrs() map[string]map[NetworkPlane]netip.Addr {
	out := map[string]map[NetworkPlane]netip.Addr{}
	for zone, router := range b.routers {
		out[zone] = maps.Clone(router.addrs)
	}
	return out
}

// dropStaleMembers removes the records of members that share an address with
// a joining container: Docker gives a stopped or removed container's address
// to the next one, so such a member no longer has it.
func (b *zoneBoundary) dropStaleMembers(ctx context.Context, joining string, addrs map[NetworkPlane]netip.Addr) error {
	for _, id := range slices.Sorted(maps.Keys(b.enrolled)) {
		if id == joining {
			continue
		}
		member := b.enrolled[id]
		shares := false
		for _, addr := range member.endpoint.addrs {
			shares = shares || slices.Contains(slices.Collect(maps.Values(addrs)), addr)
		}
		if !shares {
			continue
		}
		current, info, err := b.inspectEndpoint(ctx, id)
		if err == nil && info.running && info.startedAt == member.startedAt && maps.Equal(current, member.endpoint.addrs) {
			return fmt.Errorf("zone member %s still has an address of the joining container", member.endpoint.name)
		}
		if err := b.dropMember(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

// dropMember forgets a member whose namespace is gone: it removes its agent
// and the other members' routes to its addresses.
func (b *zoneBoundary) dropMember(ctx context.Context, id string) error {
	member := b.enrolled[id]
	if member == nil {
		return nil
	}
	var ops []zoneRouteOp
	for other, peer := range b.enrolled {
		if other == id {
			continue
		}
		for _, addr := range member.endpoint.addrs {
			if record, found := peer.routes[addr]; found {
				ops = append(ops, zoneRouteOp{holder: other, plane: record.plane, dest: addr})
			}
		}
	}
	err := b.applyRoutes(ctx, ops)
	if member.agent != nil {
		if terminateErr := member.agent.Terminate(ctx, testcontainers.StopTimeout(0)); !onlyMissingHostResource(terminateErr) {
			err = errors.Join(err, fmt.Errorf("remove the route agent of %s: %w", member.endpoint.name, terminateErr))
		} else {
			member.agent = nil
		}
	}
	if err != nil {
		return err
	}
	delete(b.enrolled, id)
	return nil
}

// withdraw removes a member that is about to stop or be removed: the other
// members' routes to it and its agent. A member whose withdrawal fails stays
// recorded, and close releases what is left: its agent and the routes of the
// live members.
func (b *zoneBoundary) withdraw(ctx context.Context, ctr testcontainers.Container) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed {
		_ = b.dropMember(ctx, ctr.GetContainerID())
	}
}

// applyRoutes runs ops, grouped by holder in one command each. A route is
// recorded before it is installed, so close removes a failed or uncertain
// installation, and forgotten only after its removal succeeded.
func (b *zoneBoundary) applyRoutes(ctx context.Context, ops []zoneRouteOp) error {
	byHolder := map[string][]zoneRouteOp{}
	for _, op := range ops {
		byHolder[op.holder] = append(byHolder[op.holder], op)
	}
	var errs []error
	for _, holder := range slices.Sorted(maps.Keys(byHolder)) {
		member := b.enrolled[holder]
		if member == nil || member.agent == nil {
			errs = append(errs, fmt.Errorf("zone member %.12s has no route agent", holder))
			continue
		}
		var script []string
		for _, op := range byHolder[holder] {
			if op.via.IsValid() {
				member.routes[op.dest] = zoneRouteRecord{plane: op.plane, via: op.via}
			}
			script = append(script, zoneRouteCommand(op, member.ifaces[op.plane]))
		}
		if _, err := b.exec(ctx, member.agent, "sh", "-ec", strings.Join(script, "\n")); err != nil {
			errs = append(errs, fmt.Errorf("routes of %s: %w", member.endpoint.name, err))
			continue
		}
		for _, op := range byHolder[holder] {
			if !op.via.IsValid() {
				delete(member.routes, op.dest)
			}
		}
	}
	return errors.Join(errs...)
}

// zoneRouteCommand is the shell command of op on device dev. A removal
// succeeds when no route to the destination is left.
func zoneRouteCommand(op zoneRouteOp, dev string) string {
	dest := op.dest.String() + "/32"
	if op.via.IsValid() {
		return "ip route replace " + dest + " via " + op.via.String() + " dev " + dev
	}
	return "ip route del " + dest + " 2>/dev/null || true; test -z \"$(ip -o -4 route show " + dest + ")\""
}

// planZoneEnrollment returns the routes that enrolling joining needs: in its
// own namespace one per address of every member of another zone on a plane
// both share, through its zone's router; in every other member one to each of
// joining's shared addresses, through that member's router when the zones
// differ and a removal when they are the same. It refuses a zone without a
// router, a router without an address on a plane it serves and an address
// that two members or a member and a router share.
func planZoneEnrollment(joining zoneEndpoint, members []zoneEndpoint, routers map[string]map[NetworkPlane]netip.Addr) (own, peers []zoneRouteOp, err error) {
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
	for _, zone := range slices.Sorted(maps.Keys(routers)) {
		for _, plane := range slices.Sorted(maps.Keys(routers[zone])) {
			if err := claim("the router of zone "+zone, routers[zone][plane]); err != nil {
				return nil, nil, err
			}
		}
	}
	for _, ep := range append([]zoneEndpoint{joining}, members...) {
		if _, found := routers[ep.zone]; !found {
			return nil, nil, fmt.Errorf("zone %s of %s has no router", ep.zone, ep.name)
		}
		for _, plane := range slices.Sorted(maps.Keys(ep.addrs)) {
			if err := claim(ep.name, ep.addrs[plane]); err != nil {
				return nil, nil, err
			}
		}
	}
	via := func(zone string, plane NetworkPlane, holder string) (netip.Addr, error) {
		addr, found := routers[zone][plane]
		if !found {
			return netip.Addr{}, fmt.Errorf("the router of zone %s has no %s address for %s", zone, plane, holder)
		}
		return addr, nil
	}
	for _, peer := range members {
		for _, plane := range slices.Sorted(maps.Keys(joining.addrs)) {
			dest, shared := peer.addrs[plane]
			if !shared {
				continue
			}
			if peer.zone == joining.zone {
				peers = append(peers, zoneRouteOp{holder: peer.id, plane: plane, dest: joining.addrs[plane]})
				continue
			}
			ownVia, err := via(joining.zone, plane, joining.name)
			if err != nil {
				return nil, nil, err
			}
			peerVia, err := via(peer.zone, plane, peer.name)
			if err != nil {
				return nil, nil, err
			}
			own = append(own, zoneRouteOp{holder: joining.id, plane: plane, dest: dest, via: ownVia})
			peers = append(peers, zoneRouteOp{holder: peer.id, plane: plane, dest: joining.addrs[plane], via: peerVia})
		}
	}
	return own, peers, nil
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

func (b *zoneBoundary) exec(ctx context.Context, ctr testcontainers.Container, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, zoneExecTimeout)
	defer cancel()
	out, err := commandOnce(ctx, ctr, args...)
	return strings.TrimSpace(string(out)), err
}

// router returns the router of zone after checking that every container on
// the plane's bridge is an enrolled member of its current start or a router:
// any other container would reach the other zones directly.
func (b *zoneBoundary) router(ctx context.Context, zone string, plane NetworkPlane) (testcontainers.Container, string, error) {
	if b == nil {
		return nil, "", invalidRequest("the cluster has no zones: use WithZoneConfig")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, "", errClusterTerminated
	}
	router := b.routers[zone]
	if router == nil {
		return nil, "", invalidRequest("zone %q is not one of the zones %v", zone, b.settings.names)
	}
	name, found := b.planes[plane]
	if !found && plane == ClusterNetworkPlane {
		// With one bridge the cluster plane is the public one.
		name, found = b.planes[PublicNetworkPlane]
	}
	if !found {
		return nil, "", invalidRequest("invalid Ceph network plane %q", plane)
	}
	unzoned, err := b.unzonedLocked(ctx)
	if err != nil {
		return nil, "", err
	}
	if len(unzoned) > 0 {
		return nil, "", invalidRequest("containers %v are on the cluster's bridges without a zone and would bypass the boundary", unzoned)
	}
	return router.ctr, name, nil
}

// unzonedLocked lists the containers on the planes' bridges that are neither
// a router nor a member completely enrolled for its current start: a partial
// enrollment, whose missing routes would let it reach another zone directly,
// is listed. The caller holds mu.
func (b *zoneBoundary) unzonedLocked(ctx context.Context) ([]string, error) {
	known := map[string]bool{}
	for _, router := range b.routers {
		known[router.ctr.GetContainerID()] = true
	}
	var unzoned []string
	for _, plane := range b.sortedPlanes() {
		inspection, err := b.docker.NetworkInspect(ctx, b.planes[plane], mobycl.NetworkInspectOptions{})
		if err != nil {
			return nil, fmt.Errorf("inspect the %s plane: %w", plane, err)
		}
		for id, resource := range inspection.Network.Containers {
			if known[id] {
				continue
			}
			if member := b.enrolled[id]; member.enrolledFor(member.startedAt) {
				_, info, err := b.inspectEndpoint(ctx, id)
				if err != nil {
					return nil, err
				}
				if member.enrolledFor(info.startedAt) {
					known[id] = true
					continue
				}
			}
			unzoned = append(unzoned, fmt.Sprintf("%s (%s plane)", resource.Name, plane))
		}
	}
	slices.Sort(unzoned)
	return slices.Compact(unzoned), nil
}

// close releases the boundary before the cluster's containers are removed:
// the routes it installed, checking that no route through a router is left,
// the agents, the routers and the management bridge, and then checks that no
// container or network with the boundary's label is left. Network shaping on
// the routers is restored before, by the fault registry. A failed step keeps
// what it could not release for a retried Terminate.
func (b *zoneBoundary) close(ctx context.Context) []error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	if b.docker == nil {
		return nil // Released by an earlier Terminate.
	}
	var errs []error
	var vias []netip.Addr
	for _, router := range b.routers {
		vias = append(vias, slices.Collect(maps.Values(router.addrs))...)
	}
	for _, id := range slices.Sorted(maps.Keys(b.enrolled)) {
		member := b.enrolled[id]
		if member.agent == nil {
			delete(b.enrolled, id)
			continue
		}
		_, info, inspectErr := b.inspectEndpoint(ctx, id)
		live := inspectErr == nil && info.running && info.startedAt == member.startedAt
		if live && len(member.routes) > 0 {
			var ops []zoneRouteOp
			for dest, record := range member.routes {
				ops = append(ops, zoneRouteOp{holder: id, plane: record.plane, dest: dest})
			}
			slices.SortFunc(ops, func(a, b zoneRouteOp) int { return a.dest.Compare(b.dest) })
			if err := b.applyRoutes(ctx, ops); err != nil {
				errs = append(errs, err)
				continue
			}
			out, err := b.exec(ctx, member.agent, "ip", "-o", "-4", "route", "show")
			if err != nil {
				errs = append(errs, fmt.Errorf("routes of %s: %w", member.endpoint.name, err))
				continue
			}
			if left := routesThrough(out, vias); len(left) > 0 {
				errs = append(errs, fmt.Errorf("%s keeps routes through the boundary: %v", member.endpoint.name, left))
				continue
			}
		}
		// A member that is gone or restarted has lost its namespace and routes.
		if err := member.agent.Terminate(ctx, testcontainers.StopTimeout(0)); !onlyMissingHostResource(err) {
			errs = append(errs, fmt.Errorf("remove the route agent of %s: %w", member.endpoint.name, err))
			continue
		}
		delete(b.enrolled, id)
	}
	for _, zone := range slices.Sorted(maps.Keys(b.routers)) {
		if err := b.routers[zone].ctr.Terminate(ctx, testcontainers.StopTimeout(0)); !onlyMissingHostResource(err) {
			errs = append(errs, fmt.Errorf("remove the router of zone %s: %w", zone, err))
			continue
		}
		delete(b.routers, zone)
	}
	if b.seed != nil && len(b.routers) == 0 {
		if err := b.seed.Remove(ctx); !onlyMissingHostResource(err) {
			errs = append(errs, fmt.Errorf("remove the zone routers' management bridge: %w", err))
		} else {
			b.seed = nil
		}
	}
	if len(errs) > 0 {
		return errs
	}
	if err := b.checkReleasedLocked(ctx); err != nil {
		return []error{err}
	}
	if err := b.docker.Close(); err != nil {
		return []error{fmt.Errorf("close the zone boundary's Docker client: %w", err)}
	}
	b.docker = nil
	return nil
}

// checkReleasedLocked fails when a container or a network with the
// boundary's label is left. The caller holds mu.
func (b *zoneBoundary) checkReleasedLocked(ctx context.Context) error {
	filter := mobycl.Filters{}.Add("label", zoneBoundaryLabel+"="+b.owner)
	ctrs, err := b.docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true, Filters: filter})
	if err != nil {
		return fmt.Errorf("list the zone boundary's containers: %w", err)
	}
	nets, err := b.docker.NetworkList(ctx, mobycl.NetworkListOptions{Filters: filter})
	if err != nil {
		return fmt.Errorf("list the zone boundary's networks: %w", err)
	}
	if len(ctrs.Items) > 0 || len(nets.Items) > 0 {
		return fmt.Errorf("the zone boundary left %d containers and %d networks", len(ctrs.Items), len(nets.Items))
	}
	return nil
}
