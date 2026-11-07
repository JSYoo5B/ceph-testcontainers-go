# Regional zones

A regional cluster is one Ceph cluster whose daemons and clients sit in
logical availability zones on one Docker engine, with a boundary between the
zones that a test can delay or cut. It is for testing a service that uses
Ceph across zones: which replica a client reads, which gateway its routing
policy picks, what it does when the cross-zone path is slow or gone. It is
opt-in; a cluster without `WithZoneConfig` starts exactly as before.

```golang
zones := ceph.ZoneConfig{}.WithZones("a", "b").WithQuorumZone("a").WithHelperImage(fault.NetemHelperImage)
layout := ceph.ClusterConfig{}.WithInitialOSDs(
	ceph.OSDConfig{Zone: "a", Host: "a-0"}, ceph.OSDConfig{Zone: "a", Host: "a-1"},
	ceph.OSDConfig{Zone: "b", Host: "b-0"},
).WithPoolDefaults(2, 1)
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithZoneConfig(zones), ceph.WithClusterConfig(layout),
	ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithSeparateClusterNetwork()))
testcontainers.CleanupContainer(t, cluster)
// ...
client, err := testcontainers.Run(ctx, ceph.DefaultImage, cluster.WithClient(), cluster.WithZone("a"), ceph.WithIdleEntrypoint())
testcontainers.CleanupContainer(t, client)
pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "regional", Replicas: 2, MinSize: 1, FailureDomain: "zone"})
lease, err := fault.ApplyZoneProfile(ctx, cluster, "a", fault.NetworkProfile{
	HelperImage: fault.NetemHelperImage, Plane: ceph.PublicNetworkPlane, Delay: 20 * time.Millisecond})
defer lease.Restore(ctx)
```

## Topology

- **Zones.** `ZoneConfig.WithZones` declares two or more zones. Each name is
  a CRUSH bucket of type `zone` below the OSDs' root and a set of endpoints on
  the cluster's bridges.
- **Membership.** Every container on the cluster's bridges belongs to one
  zone. MONs, MGRs, the control CLI, the cluster's own client and service
  daemons started without `WithZone` are in the quorum zone. Each OSD is in its
  `OSDConfig.Zone`, which a regional cluster requires for every OSD, initial or
  added. A caller's `WithClient` or `ClientConfig.WithIdentity` container
  needs `Container.WithZone`; without it the start fails. Pass
  `Container.WithZone` to `rgw.Start` or another service start to place a
  service daemon in another zone.
- **CRUSH to network.** An OSD's CRUSH location is
  `root=<root> zone=<zone> [rack=<rack>] host=<host>`, and its container is a
  member of the network zone of the same name. A pool with
  `FailureDomain: "zone"` puts each replica in a distinct zone. A client's
  own `crush_location` (for example `zone=a` for replica reads) is the
  client's setting; the fixture places its container, not its Ceph
  configuration.
- **Addresses.** The cluster keeps its public bridge, and its replication
  bridge with `NetworkConfig.WithSeparateClusterNetwork`. Every zone shares
  each bridge's subnet, so `public_network` and `cluster_network` are
  unchanged and every daemon advertises the address of its container on the
  bridge, which any member can route to.
- **Boundary.** Run starts one router per zone from the helper image, first on
  a run-owned management bridge and then attached to every plane, with
  forwarding on and ICMP redirects off. On every start of a member, a route
  agent with `NET_ADMIN` joins the member's network namespace and installs a
  /32 route to every address of every member of another zone through the
  member's own zone's router; each other member gets a route to the new
  member through its own router. A cross-zone packet therefore crosses
  exactly one router, the one of the zone it leaves, on the plane it travels;
  within a zone packets go directly. Before a member stops or is removed, the
  other members' routes to it and its agent are removed. The daemon images
  stay unchanged.
- **Route ownership.** The boundary's routes carry routing protocol 157, and
  the boundary owns a route by its destination, gateway, device and that
  protocol. It never replaces or shadows another /32 route: enrollment fails
  when a member already has a route of its own to an address the boundary
  needs to route, and leaves the routes members have within their zone alone.
  It removes only a route that still matches its record, so a route that the
  caller replaced or removed in the meantime stays as the caller left it.
- **Partial enrollment.** A member is a member only once its agent, its own
  routes and the other members' routes to it are in place. Until then it is
  listed in `Unzoned`, and a retried start of the same container (for example
  `Start` after the cause of the failure is removed) completes the enrollment
  with what is already installed.
- **Observation.** `Container.Zones` returns the zones with their routers and
  members (name, container, address on each plane), the installed routes, the
  containers on the bridges without a zone (`Unzoned`), and Ceph's
  down-reporter policy.

## Faults at the boundary

`fault.ApplyZoneProfile(ctx, cluster, zone, profile)` applies a netem profile
to what the zone's router sends on `profile.Plane`: the packets that leave the
zone on that plane, and only those. To delay both directions of a path, apply
a profile to each zone; each packet is shaped once, by the router of the zone
it leaves, so 20 ms on each side adds about 40 ms to a round trip. A `Loss` of
100 on every zone and plane partitions the zones. `Peer` or `Peers` limit the
profile to some destinations, for example one client's packets to one remote
OSD. The lease and its restoration are those of `ApplyNetworkProfile`, and
Terminate restores a held lease before it releases the boundary.

`ApplyZoneProfile` refuses to shape while any container on the bridge of any
configured plane, not only the plane it shapes, is neither a router nor a
completely enrolled member: such a container reaches every zone directly, or
through the routes a partial enrollment lacks. With a separate cluster
network, an unzoned container attached only to the cluster bridge therefore
also blocks shaping the public plane; `ZoneLayout.Unzoned` lists the
containers to look for. A path within a zone is not at the boundary; use
`fault.ApplyNetworkProfile` with a `Peer` on the member's own endpoint for it.

## Expected Ceph behavior

The outcome of a partition depends on placement, so the quorum zone is an
explicit setting. Clients and OSDs of the other zones cannot reach any MON
while the boundary is cut: a client there fails to authenticate. A MON marks
an OSD down only after failure reports from `mon_osd_min_down_reporters`
distinct `mon_osd_reporter_subtree_level` buckets (2 and host by default), so
the far zone's OSDs are marked down only if the quorum zone has that many
reporting hosts. `ZoneLayout.DownReporters`, `ReporterSubtree` and
`Zone.ReporterSubtrees` report the three values. With two OSD hosts in the
quorum zone, one in the other and size 2/min_size 1, a cut marks the far OSD
down after about `osd_heartbeat_grace` and the quorum zone keeps acknowledging
writes.

## Teardown and failures

Every router, agent and the management bridge carry the cluster's owner label
and `org.testcontainers.ceph.zone.boundary`. Every route is recorded before it
is installed, so a failed or uncertain installation is still removed.
Terminate, after the fault registry and before any daemon, removes the
recorded routes, checks that no route with the boundary's protocol is left,
leaves the caller's routes, removes the agents, the routers and the management
bridge, and checks that no container or network with the boundary's label is
left. A step that fails keeps what it could not release for a retried
Terminate. A Run that fails while the boundary starts returns the partial
cluster, whose Terminate releases what was created.

## Limits of one Docker engine

- The zones share one kernel, CPU pool, disk and each bridge's subnet. They
  are not independent failure domains: a slow disk or a busy CPU affects every
  zone.
- The only controlled difference between the zones is the boundary. Without a
  profile, a cross-zone round trip is one router hop longer (about 0.04 ms on
  a Linux amd64 host) and otherwise the same as a same-zone one.
- Membership of MONs, MGRs and service daemons is an assignment, not
  something Ceph observes; only OSDs carry their zone in CRUSH.
- The topology needs IPv4 bridge networking. Host networking, IPv6 and
  dual-stack planes are refused. All MONs are in the quorum zone; MONs split
  across zones are not supported.
- A container that joins the bridges outside the fixture (for example with
  `docker network connect`) bypasses the boundary. `ZoneLayout.Unzoned` lists
  it and `ApplyZoneProfile` refuses to shape while it is there.
- Docker reuses the address of a stopped or removed container; enrollment
  drops the stale record of a member whose address a new one received.
