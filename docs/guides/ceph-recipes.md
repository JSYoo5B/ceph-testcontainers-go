# Ceph recipes

Worked examples for the `ceph` package. They are collected here until the usage scenarios move to the project wiki. The [package README](../../ceph/README.md) gives the usual order of use; [configuration](ceph-configuration.md) and [operations](ceph-operations.md) explain each API in depth.


The fragments below run inside a test with `ctx := t.Context()` and a `cluster`
started as in the [usage example](../../ceph/README.md#usual-order-of-use), unless they show their own
`Run`. They use `waitForHealthCheck` from [HealthDetails](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#HealthDetails).

## Attach a client with a restricted identity

Create an identity limited to one pool and namespace, attach a container that
only has that identity's keyring, and verify the limits with a fresh connection.

```golang
reader, err := cluster.CreateClient(ctx, "reader", ceph.ClientCaps{
	Mon: "allow r",
	OSD: "allow r pool=app-data namespace=tenant-a",
})
if err != nil {
	t.Fatal(err)
}
facts, err := ceph.FactsOf(cluster)
client, err := testcontainers.Run(ctx, facts.Images.Client,
	reader.WithIdentity(),
	ceph.WithIdleEntrypoint(),
)
testcontainers.CleanupContainer(t, client)
if err != nil {
	t.Fatal(err)
}
// Inside the container: rados --id reader -p app-data -N tenant-a ls
// Writes, other namespaces and other pools are denied with EPERM.

// Widen the identity; the key stays the same. Omitted services lose their caps.
if err := reader.UpdateCaps(ctx, ceph.ClientCaps{
	Mon: "allow r",
	OSD: "allow rw pool=app-data namespace=tenant-a",
}); err != nil {
	t.Fatal(err)
}
// Use a new client process to observe the change; existing sessions may keep
// their old tickets for a while.
if err := reader.Delete(ctx); err != nil {
	t.Fatal(err)
}
```

## Give each test an independent fixture

Start `ceph.Run` in each test and register the returned cluster for cleanup
before checking the error. Use `WithPools` for bootstrap pools, or
`CreatePool` for an explicitly owned dynamic pool. Create restricted Cephx
identities with `CreateClient` and use `ClientConfig.WithIdentity` for client containers.
See the [usage example](../../ceph/README.md#usual-order-of-use).

## Create an erasure-coded data pool for RBD images

RBD keeps image metadata in a replicated pool and can store data in an EC pool
that allows overwrites.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(3)),
	rbd.WithPools(ceph.PoolConfig{Name: "rbd-meta"}),
	ceph.WithClusterConfig(ceph.ClusterConfig{}.WithPools(ceph.PoolConfig{
		Name: "rbd-data", Application: "rbd",
		ErasureCode: &ceph.ErasureCodeConfig{K: 2, M: 1, AllowOverwrites: true},
	})),
)
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
if err := cluster.WaitForClean(ctx); err != nil {
	t.Fatal(err)
}
// In a WithClient container:
// rbd create --size 64M --data-pool rbd-data rbd-meta/volume
```

`K+M` OSDs (three here) are needed because each shard goes to a distinct OSD.

## Simulate failure domains with CRUSH hosts and racks

Logical hosts, racks, roots and device classes let you test placement rules and
domain failures on one Docker engine.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithClusterConfig(ceph.ClusterConfig{}.WithInitialOSDs(
		ceph.OSDConfig{Host: "host-a", Rack: "rack-a", Root: "tc-storage", DeviceClass: "fast"},
		ceph.OSDConfig{Host: "host-b", Rack: "rack-b", Root: "tc-storage", DeviceClass: "fast"},
		ceph.OSDConfig{Host: "host-c", Rack: "rack-c", Root: "tc-storage", DeviceClass: "fast"},
	).WithPoolDefaults(3, 2).WithDefaultCRUSHRoot("tc-storage")),
)
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{
	Name: "by-host", Application: "rados",
	FailureDomain: "host", CRUSHRoot: "tc-storage", DeviceClass: "fast",
})
if err != nil {
	t.Fatal(err)
}
if err := cluster.WaitForClean(ctx); err != nil {
	t.Fatal(err)
}
// Each PG now has one replica per host. Stopping the only OSD of host-a takes
// that whole failure domain down; with min_size 2 the pool stays writable.
_ = pool
```

Verify the hierarchy with `cluster.Ceph(ctx, "osd", "tree", "--format", "json")`
or `"osd", "find", "<id>"`.

## Start without OSDs, then add storage

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs()))
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
// MON and MGR are up; the application can be tested before storage exists.
// Pool defaults stay size 2, min_size 1.
for range 2 {
	if _, err := cluster.AddOSD(ctx); err != nil {
		t.Fatal(err) // a non-nil partial OSD is still owned by the cluster
	}
}
if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "application", Application: "rados"}); err != nil {
	t.Fatal(err)
}
if err := cluster.WaitForClean(ctx); err != nil {
	t.Fatal(err)
}
```

`CreatePool` before any OSD exists is refused by the failure-domain check.

## Start without a manager

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialManagers().WithPools(ceph.PoolConfig{Name: "application", Application: "rados"})),
)
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
// MON quorum and both OSDs are up and in; the pool exists. Direct RADOS I/O
// works, MGR-backed commands and PG statistics do not.
if _, err := cluster.AddManager(ctx, "a"); err != nil {
	t.Fatal(err)
}
if err := cluster.WaitForClean(ctx); err != nil {
	t.Fatal(err)
}
```

Add `ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs())` for a MON-only phase; you can add the first
manager before or after the first OSD.

## Reproduce a minimal legacy placement setup

A client that implements only the oldest CRUSH and placement features can be
tested against one MemStore OSD, a legacy CRUSH map and a pool with one PG, one
replica and no `hashpspool` flag on any pool. The fixture has no option for a
hand-written CRUSH map; install it with the native tools, as below. `crushtool`
is in the official image and the published client and essential images.

```golang
// Ceph creates the MGR's .mgr pool with the MON's default flags, so turn
// the hashpspool default off before any pool exists.
legacyPools := filepath.Join(t.TempDir(), "legacy-pools.conf")
if err := os.WriteFile(legacyPools, []byte("[global]\nosd_pool_default_flag_hashpspool = false\n"), 0o644); err != nil {
	t.Fatal(err)
}
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1)),
	ceph.WithOSDStorage(ceph.OSDStorage{}.WithObjectStore(ceph.OSDMemStore)),
	ceph.WithOSDStorage(ceph.OSDStorage{}.WithBlockSize(256<<20)),
	ceph.WithConfigFile(legacyPools),
)
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}

// Keep OSDs from moving themselves into host buckets or setting a device
// class when they start, so the installed map stays as written.
for _, name := range []string{"osd_crush_update_on_start", "osd_class_update_on_start"} {
	if _, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "osd", Name: name, Value: "false"}); err != nil {
		t.Fatal(err)
	}
}
const legacyMap = `tunable choose_local_tries 2
tunable choose_local_fallback_tries 5
tunable choose_total_tries 19
tunable chooseleaf_descend_once 0
tunable chooseleaf_vary_r 0
tunable chooseleaf_stable 0
tunable straw_calc_version 0
device 0 osd.0
type 0 osd
type 10 root
root default {
    id -1
    alg straw
    hash 0
    item osd.0 weight 1.00000
}
rule replicated_rule {
    id 0
    type replicated
    step take default
    step choose firstn 0 type osd
    step emit
}
`
control, err := cluster.ControlContainer(ctx)
if err != nil {
	t.Fatal(err)
}
if err := control.CopyToContainer(ctx, []byte(legacyMap), "/tmp/crush.txt", 0o644); err != nil {
	t.Fatal(err)
}
if code, _, err := control.Exec(ctx, []string{"crushtool", "-c", "/tmp/crush.txt", "-o", "/tmp/crush.bin"}); err != nil || code != 0 {
	t.Fatalf("crushtool exited %d: %v", code, err)
}
if _, err := cluster.Ceph(ctx, "osd", "setcrushmap", "-i", "/tmp/crush.bin"); err != nil {
	t.Fatal(err)
}

pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{
	Name: "legacy", PGNum: 1, Replicas: 1, MinSize: 1, NoHashPSPool: true, Application: "rados",
})
```

- While any pool has `hashpspool`, the MON requires the `OSDHASHPSPOOL`
  feature of every client and refuses a client without it when it opens its
  MON session. `NoHashPSPool` clears the flag only on the pool it creates;
  the configuration file keeps it off `.mgr` and every later pool. Check
  every pool with `Observer.Pools` once `.mgr` exists.
- `CreatePool` adds its own rule, `tc-<pool>-replicated`, which with the
  default `osd` failure domain also chooses OSDs directly. To use the map's
  `replicated_rule`, create the pool with `cluster.Ceph(ctx, "osd", "pool",
  "create", ...)` instead; such a pool is not one that `Pool.Remove` removes.
- The legacy tunables lower the features a client needs to read the CRUSH map.
  Switching to modern tunables later (`ceph osd crush tunables optimal`) raises
  them again, which tests a client's refusal of unsupported features.
- Ceph reports `POOL_NO_REDUNDANCY` for a pool with one replica; `WaitForClean`
  still passes.

## Run small OSDs

Small logical files keep disk and tmpfs usage low. Skip mClock's startup
benchmark before the first OSD starts and keep the setting for the cluster's
whole lifetime, because restored defaults would run the benchmark at the next OSD
start.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs()),
	ceph.WithOSDStorage(ceph.OSDStorage{}.WithBlockSize(64<<20)),
	ceph.WithOSDStorage(ceph.OSDStorage{}.WithInMemory(256<<20)), // optional; omit for container-local files
)
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
// No Restore: Terminate disposes of the configuration database.
if _, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{
	Section: "osd", Name: "osd_mclock_skip_benchmark", Value: "true",
}); err != nil {
	t.Fatal(err)
}
for range 2 {
	if _, err := cluster.AddOSD(ctx); err != nil {
		t.Fatal(err)
	}
}
```

Known-good pairs for a workload of a few MiB, including OSD restarts, backfill to
a third OSD and the removal of an original OSD, are 64 MiB OSDs with a 256 MiB
shared cap, 128 MiB with 512 MiB, 256 MiB with 1 GiB, and 512 MiB with 2 GiB.
Size both bounds for your own workload with headroom for replicas, metadata and
recovery. The three limits are independent:

| Resource | Bounded by | Includes |
| --- | --- | --- |
| Logical storage per OSD | `OSDStorage.WithBlockSize` | BlueStore data, metadata and temporary writes inside each file |
| Shared RAM backing | `OSDStorage.WithInMemory` | Allocated tmpfs pages of all OSD files |
| Process memory | Docker host or VM limits and daemon settings | MON, MGR, OSD and client memory |

If you customize daemon arguments or config files, confirm the effective value
with `cluster.Ceph(ctx, "config", "show", "osd.0", "osd_mclock_skip_benchmark")`.

## Choose the OSD scheduler before adding OSDs

`osd_op_queue` is read when an OSD starts, so select it before the first OSD.
In a small comparison with HDD-class file-backed OSDs, default WPQ recovered and
backfilled more slowly than the default mClock scheduler, and setting the two WPQ
HDD recovery sleeps to 0 removed most of that difference. The module keeps
Ceph's default (mClock); measure your own scenario before changing it.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs()))
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
for _, setting := range []ceph.ConfigSetting{
	{Section: "osd", Name: "osd_op_queue", Value: "wpq"},
	{Section: "osd", Name: "osd_recovery_sleep_hdd", Value: "0"},
	{Section: "osd", Name: "osd_recovery_sleep_degraded_hdd", Value: "0"},
} {
	if _, err := override.SetConfig(ctx, cluster, setting); err != nil {
		t.Fatal(err)
	}
}
for range 2 {
	if _, err := cluster.AddOSD(ctx); err != nil {
		t.Fatal(err)
	}
}
// Confirm the running daemon's value: {"osd_op_queue": "wpq"}
out, err := cluster.Ceph(ctx, "tell", "osd.0", "config", "get", "osd_op_queue")
if err != nil {
	t.Fatal(err)
}
t.Log(string(out))
```

Restoring the override, or setting it on a running OSD, does not change the queue
until the OSD restarts.

## Replace every monitor and refresh clients

Rolling replacement adds a new monitor before removing an old one, so quorum is
never at risk. Owned daemons are updated automatically; your client containers
are not.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(3)))
// ... CleanupContainer, error check, client container `client` with WithClient()
original, err := cluster.Monitors(ctx) // a, b, c
if err != nil {
	t.Fatal(err)
}
for i, name := range []string{"d", "e", "f"} {
	if _, err := cluster.AddMonitor(ctx, name); err != nil {
		t.Fatal(err)
	}
	if err := original[i].Remove(ctx); err != nil {
		t.Fatal(err)
	}
}
if err := cluster.WaitForQuorum(ctx); err != nil {
	t.Fatal(err)
}
// Running sessions follow the monmap; a restarted client reads ceph.conf.
if err := cluster.RefreshClientMonitorConfig(ctx, client); err != nil {
	t.Fatal(err)
}
// For a client you configure yourself, write the new mon_host value.
observer, err := ceph.Observe(cluster)
if err != nil {
	t.Fatal(err)
}
addresses, err := observer.MonitorBootstrapAddresses(ctx)
if err != nil {
	t.Fatal(err)
}
t.Log("mon_host =", addresses)
```

For RBD and CephFS mirror links between clusters, refresh the link as described
in [rbd](../../rbd/README.md) and [cephfs](../../cephfs/README.md).

## Keep an OSD down without data movement

```golang
hold, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoOut, true)
// hold is never nil: after a refusal its Restore does nothing.
defer hold.Restore(context.Background())
if err != nil {
	t.Fatal(err)
}
osds, err := cluster.OSDs(ctx)
if err != nil {
	t.Fatal(err)
}
osd := osds[0]
timeout := 10 * time.Second
if err := osd.Stop(ctx, &timeout); err != nil {
	t.Fatal(err)
}
// Degraded reads and writes; the MON does not mark the OSD out, so no backfill
// starts. Expect OSD_DOWN, PG_DEGRADED and OSDMAP_FLAGS in HealthDetails.
if err := osd.Start(ctx); err != nil {
	t.Fatal(err)
}
if err := hold.Restore(ctx); err != nil {
	t.Fatal(err)
}
if err := cluster.WaitForClean(ctx); err != nil {
	t.Fatal(err)
}
```

To move data off a running OSD instead, use `OSDContainer.SetIn(ctx, false)` on its handle, wait with
`WaitForPGClean`, and mark it in again.

## Hold a pool relocation mid-flight

Create the state in which PGs have moved to new OSDs but objects are still being
recovered, and test client I/O against it.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithClusterConfig(ceph.ClusterConfig{}.WithInitialOSDs(
		ceph.OSDConfig{DeviceClass: "fast"},
		ceph.OSDConfig{DeviceClass: "slow"},
	)),
)
// ... CleanupContainer and error check
app, err := cluster.CreatePool(ctx, ceph.PoolConfig{
	Name: "app", Application: "rados", Replicas: 1, DeviceClass: "fast",
})
if err != nil {
	t.Fatal(err)
}
if err := cluster.WaitForClean(ctx); err != nil {
	t.Fatal(err)
}
// ... write objects with the client
for _, flag := range []ceph.OSDFlag{ceph.OSDFlagNoBackfill, ceph.OSDFlagNoRecover} {
	hold, err := override.SetOSDFlag(ctx, cluster, flag, true)
	defer hold.Restore(context.Background())
	if err != nil {
		t.Fatal(err)
	}
}
if err := app.SetPlacement(ctx, ceph.PoolPlacement{DeviceClass: "slow"}); err != nil {
	t.Fatal(err)
}
// Pool.PGs now reports up/acting on the slow OSD with most PGs in
// active+recovery_wait+degraded. Reads of old objects trigger on-demand
// recovery and succeed; new writes succeed.
// Restore both flags, then poll Pool.PGs until every PG is active+clean on the
// slow OSD (WaitForClean alone can pass before the new mapping is reported).
```

Moving the pool back with `app.SetPlacement(ctx, ceph.PoolPlacement{DeviceClass: "fast"})`
reuses its original rule, which also makes it removable with `Pool.Remove` again.

## Fill the cluster with low full ratios

```golang
change, err := override.SetFullRatios(ctx, cluster, ceph.FullRatios{
	NearFull: 0.0001, BackfillFull: 0.0002, Full: 0.0003,
})
// change is never nil: after a refusal its Restore does nothing.
defer change.Restore(context.Background())
if err != nil {
	t.Fatal(err)
}
if err := waitForHealthCheck(ctx, cluster, "OSD_FULL"); err != nil {
	t.Fatal(err)
}
// Ordinary RADOS writes now block. A librados write with the FULL_TRY
// operation flag fails with ENOSPC (errno 28) instead; reads still work.
if err := change.Restore(ctx); err != nil {
	t.Fatal(err)
}
```

The cluster needs a little existing data for the OSDs to cross the thresholds.
To test only `OSD_NEARFULL` or `OSD_BACKFILLFULL`, lower only those ratios (for
example `NearFull: 0.0001, BackfillFull: 0.90, Full: 0.95`); ordinary writes
still succeed in those states. Setting `FULL_TRY` is the client's job; the module
adds no client SDK.

## Reproduce a scrub inconsistency and repair it

```golang
// A replicated pool with one PG and two replicas; the object "victim" exists.
var mapping struct {
	PGID          ceph.PGID `json:"pgid"`
	Acting        []int     `json:"acting"`
	ActingPrimary int       `json:"acting_primary"`
}
out, err := cluster.Ceph(ctx, "osd", "map", "app", "victim", "--format", "json")
if err != nil {
	t.Fatal(err)
}
if err := json.Unmarshal(out, &mapping); err != nil {
	t.Fatal(err)
}
replica := mapping.Acting[0]
if replica == mapping.ActingPrimary {
	replica = mapping.Acting[1]
}
if err := fault.InjectObjectDataError(ctx, cluster, "app", "victim", replica); err != nil {
	t.Fatal(err)
}
if err := cluster.DeepScrubPG(ctx, mapping.PGID); err != nil {
	t.Fatal(err)
}
observer, err := ceph.Observe(cluster)
if err != nil {
	t.Fatal(err)
}
objects, err := observer.PGInconsistencies(ctx, mapping.PGID)
if err != nil {
	t.Fatal(err)
}
// objects[0].Name == "victim", objects[0].UnionShardErrors == []string{"read_error"}
if err := waitForHealthCheck(ctx, cluster, "PG_DAMAGED"); err != nil {
	t.Fatal(err)
}
if err := cluster.RepairPG(ctx, mapping.PGID); err != nil {
	t.Fatal(err)
}
```

After the repair the health checks clear, and a new `DeepScrubPG` reports no
inconsistency.

## Fence one client session

```golang
// address is the client's own session address, e.g. from rados_getaddrs or
// go-ceph Conn.GetAddrs: "172.19.0.6:0/3461995546".
fence, err := override.AddBlocklist(ctx, cluster, address, time.Minute)
// fence is never nil: after a refusal its Restore does nothing.
defer fence.Restore(context.Background())
if err != nil {
	t.Fatal(err)
}
// The fenced session's operations fail (ESHUTDOWN in librados); another session
// from the same host keeps working.
if err := fence.Restore(ctx); err != nil {
	t.Fatal(err)
}
// The client can resume or reconnect, depending on its library.
```

`BlocklistEntries` shows the entry and its expiry. For RBD exclusive-lock
handover, the lock owner's address comes from `rbd.ImageClients` (see
[rbd](../../rbd/README.md)).

## Detect a stuck watcher

A client that holds a watch and then stops responding blocks notifies on that
object until the OSD's `osd_client_watch_timeout` (30 seconds by default)
expires.

```golang
// `watcher` is a WithClient container whose process watches "notify-object".
pause, err := fault.Pause(ctx, cluster, watcher)
if pause != nil {
	defer pause.Resume(context.Background())
}
if err != nil {
	t.Fatal(err)
}
observer, err := ceph.Observe(cluster)
if err != nil {
	t.Fatal(err)
}
watchers, err := observer.ObjectWatchers(ctx, "app-data", "", "notify-object")
if err != nil {
	t.Fatal(err)
}
// The frozen client is still listed, and a notify from another client fails
// with ETIMEDOUT (errno 110) after its own timeout. Poll ObjectWatchers: after
// the watch timeout the list is empty and notifies complete at once.
_ = watchers
```

An expired watch does not blocklist its client; `BlocklistEntries` stays
unchanged.

## Scrape the MGR Prometheus exporter

```golang
module, err := override.SetMGRModule(ctx, cluster, "prometheus", true)
// module is never nil: after a refusal its Restore does nothing.
defer module.Restore(context.Background())
if err != nil {
	t.Fatal(err)
}
var url string
observer, err := ceph.Observe(cluster)
if err != nil {
	t.Fatal(err)
}
for url == "" {
	services, err := observer.ManagerServices(ctx)
	if err != nil {
		t.Fatal(err)
	}
	url = services["prometheus"] // e.g. "http://172.19.0.3:9283/"
	if url == "" {
		time.Sleep(time.Second) // bound this loop with ctx in real tests
	}
}
metrics, err := observer.ManagerMetrics(ctx) // parsed samples; see ManagerMetrics
```

In bridge mode each manager listens on its own container address on port 9283,
and standbys answer with an empty 200 response. After a failover
(`cluster.Ceph(ctx, "mgr", "fail")`), `ManagerServices` returns the new active
manager's URL.

In host mode all managers share the host network, so give each manager its own
port and bind the public address before enabling the module (using
[override.SetConfig](../../override/README.md#setconfig)). With the default settings and two
managers, `ManagerServices` stayed empty and no manager served metrics:

```golang
facts, err := ceph.FactsOf(cluster)
settings := []ceph.ConfigSetting{
	{Section: "mgr", Name: "mgr/prometheus/server_addr", Value: facts.PublicAddress},
	{Section: "mgr", Name: "mgr/prometheus/a/server_port", Value: "19283"},
	{Section: "mgr", Name: "mgr/prometheus/b/server_port", Value: "19284"},
}
```

Managers added later with `AddManager` need their own port setting too. Pick
ports that no other process or cluster uses.

## Connect a native client in host mode

On a Linux Docker host, host mode lets a test process use go-ceph or librados
directly:

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithHostNetwork())
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
cfg, err := cluster.ConnectionConfig(ctx)
// Write cfg.Conf and cfg.Keyring to files and point librados at them (see ConnectionConfig).
```

See [Host networking](host-network.md) for remote engines, Docker
Desktop and port selection.

## Run a cluster that accepts only encrypted connections

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithMessengerMode(ceph.MessengerV2Secure)),
)
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
cfg, err := cluster.ConnectionConfig(ctx)
// Hand cfg.Conf and cfg.Keyring to the client under test (for example an
// independent Messenger v2 implementation), or attach a container with
// cluster.WithClient() / client.WithIdentity(...).
```

The client must reach the advertised addresses: run it in a `WithClient`
container, or use host mode.

## Save diagnostics when a test fails

Use [TerminateOnCleanup](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#TerminateOnCleanup) with `Archive: true`: it
writes the diagnostics, the crash metadata and a manifest into the test's
artifact directory before the cluster is terminated. Run `go test -artifacts`
to keep them. Add caller-owned clients and mirror daemons with
`Diagnostics.AdditionalContainers`, remove them through
`Finalization.Cleanup`, and mask application secrets with `RedactValues`. To
collect by hand, see [CollectDiagnostics](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#CollectDiagnostics) and
[CollectCrashMetadata](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#CollectCrashMetadata).

To also fail a test whose client operations succeeded although a daemon
crashed or logged an unexpected warning, activate a post-test
[verification](ceph-operations.md#verify) on the same finalization, with the crashes and log
lines that the test causes on purpose as expectations.
