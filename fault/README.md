# Dedicated faults

Import `github.com/JSYoo5B/ceph-testcontainers-go/fault` alongside `ceph` to
put a running cluster into a failure condition that a test wants to observe
its SUT under: a frozen daemon, a cut or slowed network path, a damaged
replica. Faults reproduce conditions for integration tests; they are not a
chaos platform or a performance tool. See
[API purpose and purpose tags](../docs/guides/api-purpose.md).

The package has no fixture lifetime of its own: cluster-bound faults use the
cluster created by `ceph.Run` and are restored by its `Terminate`.

## Order of use

1. **Start a cluster** with `ceph.Run` (and any service fixture the SUT needs).
2. **Inject the fault** that the scenario needs:
   - `fault.Pause`: freeze every process of a daemon or client container.
   - `fault.InterruptNetwork`: disconnect one endpoint from the public or
     cluster bridge.
   - `fault.ApplyNetworkProfile`: delay, drop, rate-limit, reorder, duplicate
     or corrupt the packets a container sends, optionally only to selected
     peers (opt-in netem helper, `fault.NetemHelperImage`).
   - `fault.InjectObjectDataError`: make one replica of an object return a
     read error to scrub and recovery.
   - `fault.InterruptLink` / `fault.ApplyLinkProfile`: the same on a bridge the
     test owns, outside the cluster's planes.
3. **Run the SUT** under the fault.
4. **Observe** the effect with the cluster's observation APIs
   (`ceph.Observer`, `Pool.PGs`, `NetworkProfileLease.Status`, ...).
5. **Restore** with the handle's `Resume` / `Restore` when the scenario needs
   recovery; `Terminate` restores what is still held. Keep a non-nil handle
   returned with an error: restoring it stays retryable.

```golang
osds, err := cluster.OSDs(ctx)
if err != nil {
	t.Fatal(err)
}
slow, err := fault.ApplyNetworkProfile(ctx, cluster, osds[0], fault.NetworkProfile{
	HelperImage: fault.NetemHelperImage,
	Plane:       ceph.PublicNetworkPlane,
	Peer:        netip.MustParseAddr(clientAddress),
	Delay:       200 * time.Millisecond,
})
if slow != nil {
	defer slow.Restore(context.Background())
}
if err != nil {
	t.Fatal(err)
}
// osd.0's replies to this client are delayed by 200 ms; nothing else is.
```

## Concepts and limits

- Ownership: cluster-bound handles are restored by `Terminate` (shaping first,
  then endpoints, then paused containers). Standalone link faults and faults on
  caller-owned containers are the caller's: restore them before removing the
  container or bridge.
- Whole endpoint versus selected paths: `InterruptNetwork` cuts a whole
  endpoint; to cut one direction of selected paths use `ApplyNetworkProfile`
  with `Peer`/`Peers` and a `Loss` of 100. Shaping applies to what the target
  sends; delay the peer's endpoint for the other direction.
- Opt-in helpers: network profiles need an image that meets
  [netem contract 1](https://github.com/JSYoo5B/ceph-testcontainers-images/blob/main/docs/EXTENSIONS.md)
  (`fault.NetemHelperImage` is the published one) and an engine that grants it
  `NET_ADMIN`. The preflight refuses an unsuitable image, a missing capability
  or a kernel without the needed netem options before the target changes.
- Host networking: `InterruptNetwork` and network profiles refuse host-network
  clusters; `Pause` works there.
- Ceph reacts to faults on its own schedule: a frozen or cut OSD is marked down
  only after its peers report it (heartbeat grace), and TCP hides reorder,
  duplicate and corrupt faults from Ceph. Hold `nodown` with
  `override.SetOSDFlag` to keep a frozen OSD up.
- Policy and admission changes (full ratios, OSD flags, the session blocklist)
  live in [`override`](../override/README.md), even when a test uses them to
  provoke failures.

## Reference

The full contract of every function, field and handle is in go doc:
[pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/fault)
or `go doc -all github.com/JSYoo5B/ceph-testcontainers-go/fault`. Worked
scenarios, client-side effects and the engines they were checked on are in
[Fault injection](../docs/guides/fault-injection.md); error categories in
[Errors](../docs/guides/errors.md).
