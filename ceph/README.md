# Ceph

The Testcontainers module for Ceph. It starts a small, disposable Ceph cluster
for the integration tests of a service that uses Ceph.

## What it is for

The module builds the Ceph side of an integration test, independently of the
system under test and of the test code, and lets the test check afterwards that
the cluster ended in the expected state. It reproduces as many operational
conditions as it can, including some injected faults, but it is not a full Ceph
deployment: large-scale experiments, performance measurement and tuning are out
of scope, and it does not wrap object, image, file or S3 data APIs. Your service
talks to the cluster with its own client (librados, go-ceph, librbd, libcephfs,
the Ceph CLI or an S3 SDK). See [API purpose and purpose tags](../docs/guides/api-purpose.md).

Real `ceph-mon`, `ceph-mgr` and `ceph-osd` daemons run in separate containers on
an isolated Docker network: one MON, one MGR and two OSDs by default, each OSD
on a sparse BlueStore file inside its container. The daemons need no privileged
mode, host disks, LVM, Docker socket or systemd; Testcontainers itself (and
Ryuk) still needs the Docker engine. Dependencies, requirements, installation
and the versioning policy are in the [root README](../README.md#requirements).

## Usual order of use

1. **Start the cluster.** `ceph.Run(ctx, img, opts...)` with `ceph.DefaultImage`
   (or role images through `WithImageOptions`) builds and waits for the whole
   topology. Settings are grouped values, each passed through one option:
   `WithClusterConfig` (MON/MGR/OSD counts, OSD layout, CRUSH root, pool
   defaults, initial pools), `WithOSDStorage`, `WithMonitorStorage`, `WithNetworkConfig`,
   `WithImageOptions`, `WithZoneConfig` (logical zones behind a routed
   boundary), plus `WithAuth`, `WithConfigFile`, `WithHostNetwork`,
   `WithManagerPorts` and `WithStartupTimeout`. Contradictory options fail before anything is created.
   For small functional OSDs, select
   `OSDStorage{}.WithStartupBenchmark(OSDBenchmarkSkip)` explicitly;
   [OSD startup calibration](../docs/guides/osd-startup-benchmark.md) explains
   native controls, configuration precedence and backing-storage costs.
   Register cleanup before checking the error: a partially started cluster is
   returned with the error and still owns containers.
2. **Prepare the state the test needs.** `WaitForClean` before client I/O;
   `CreatePool` and the `Pool` handle (replication, quota, placement, PG count);
   `CreateClient` for a Cephx identity with exact caps; `AddOSD`, `AddMonitor`,
   `AddManager` and the daemon handles' `Remove`, `Detach`, `Replace`, `SetIn`
   for topology. The [override](../override/README.md) package changes
   configuration with restoration handles, the [fault](../fault/README.md)
   package injects faults, and the [rbd](../rbd/README.md),
   [cephfs](../cephfs/README.md) and [rgw](../rgw/README.md) packages add their
   services to the same `Run`.
3. **Hand the connection to the system under test.** `WithClient` or
   `ClientConfig.WithIdentity` attach a client container to the cluster
   network with `ceph.conf` and a keyring; `ConnectionConfig` returns the same
   material for a process that connects itself (host mode, see
   [Host networking](../docs/guides/host-network.md)). `FactsOf` reports the
   images, network and Ceph version.
4. **Observe and verify.** `Observe` returns a resource-free `Observer` for readiness waits, tracked
   optional-service inventory, health, PG and
   pool reports, quorum, OSD states, MGR modules and services, watchers and
   blocklist entries. When no typed observation exists, `Ceph` runs the raw
   Ceph CLI in the control container, retrying only the connection to a MON
   (bounded by `ctx` and a minute), and `RunNativeCommand` runs a command
   under a supervisor that bounds its run time and output.
   The waits (`WaitForClean`, `WaitForPGClean`, `WaitForQuorum`,
   `WaitMGRModuleReady`) keep the cluster's startup timeout. The watches
   (`WatchOSDs`, `Pool.WatchPGs`, `WatchHealth`, `WatchMonitors`,
   `WatchManagers`) follow state while the SUT runs; their shared lifecycle and
   what each one fixes by its baseline are in
   [watching state](../docs/guides/contexts-and-cleanup.md#watching-state).
   `Pool.OverridePGPrimary` aims a fault at a known serving OSD (see
   [fault injection](../docs/guides/fault-injection.md)). To check one
   captured observation against the condition the test states, use
   [`ceph/expect`](expect/README.md), for example
   [`expect.PGs`](expect/README.md#pool-placement-groups).
5. **Finish.** `testcontainers.CleanupContainer` runs `Terminate`, which
   removes every owned container, network and volume. The
   [finalize](finalize/README.md) package replaces it when the test needs
   post-test checks (with crash and cluster-log verification), a failure
   archive and caller cleanups in reverse order before `Terminate`; the
   [diagnostics](diagnostics/README.md) package collects the same reports
   standalone.

```golang
func TestRADOSRoundTrip(t *testing.T) {
	ctx := t.Context()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage,
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithPools(ceph.PoolConfig{Name: "app-data", Application: "rados"})),
	)
	testcontainers.CleanupContainer(t, cluster) // also owns a partial cluster
	if err != nil {
		t.Fatal(err)
	}
	observer, err := ceph.Observe(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if err := observer.WaitForClean(ctx); err != nil { // Run does not wait for clean PGs
		t.Fatal(err)
	}
	// The client image must contain the tools the test uses. WithClient joins
	// the cluster network and copies ceph.conf and the admin keyring.
	client, err := testcontainers.Run(ctx, ceph.DefaultImage, cluster.WithClient(), ceph.WithIdleEntrypoint())
	testcontainers.CleanupContainer(t, client) // registered later, so removed first
	if err != nil {
		t.Fatal(err)
	}
	// ... run the system under test against the client, then check the result,
	// for example with ceph.Observe(cluster) or the client's own reads.
}
```

Starting a cluster takes tens of seconds and pulls a large image the first time;
give `go test` a generous `-timeout` when several clusters run in one package.

## Concepts and limits

- **One fixture per test.** Call `Run` for each test and create the pools,
  namespaces and identities it needs. Stop clients the test attached before the
  cluster is terminated, and use a fresh bounded context in cleanups:
  `t.Context()` has ended by then. Keep non-nil handles returned with errors;
  an uncertain operation may still own a resource. See
  [Contexts and cleanup](../docs/guides/contexts-and-cleanup.md).
- **Configuration composition.** A grouped value records which fields were set;
  a later call in one value replaces an earlier one, and two carriers that set
  the same field are refused, even when they agree.
- **Images.** One aggregate image supplies every role by default; an `ImageSet`
  selects essential, client, CephFS, RBD-aux and RGW images separately, and a
  missing optional role never falls back. Its MGR role is the one with a
  default: every manager runs the essential image unless `ImageSet.MGR`
  selects another, such as the official image or one with additional modules. See
  [Releases and images](../docs/guides/releases-and-images.md).
- **Networks.** Bridge mode by default: only MON `a` (and RGW gateways, and
  the MGRs with `WithManagerPorts` for a host-side client of a module
  listener such as the Dashboard) publishes ports, so native
  RADOS, RBD and CephFS clients run in a `WithClient` container. Host mode lets
  a process on a Linux Docker host connect directly. IPv6, dual-stack and a
  separate cluster network are bridge-mode options.
- **Placement is logical.** All daemons share one Docker engine; CRUSH hosts,
  racks and zones test placement rules, not physical failure tolerance, and OSD
  files are sparse, so fullness is simulated with full ratios.
- **Regional zones.** `WithZoneConfig` places one cluster's daemons and clients
  in logical availability zones with an owned routed boundary between them,
  which `fault.ApplyZoneProfile` delays or cuts and `Container.Zones` reports;
  `Container.WithZone` places a client or service daemon. See
  [Regional zones](../docs/guides/regional-zones.md).
- **Fixture defaults** differ from a stock cluster (small pools, PG
  autoscaling off, replica-1 pools allowed, faster MGR stats); the
  [configuration guide](../docs/guides/ceph-configuration.md#fixture-defaults-that-differ-from-a-stock-cluster)
  lists them.
- **Errors** carry categories (`ErrInvalidRequest`, `ErrUnsupported`,
  `ErrImageCapability`, `ErrEnvironmentUnavailable`, `ErrNativeFailure`) and
  `OperationError` / `NativeFailure` details; see [Errors](../docs/guides/errors.md).

## Reference

- API: `go doc github.com/JSYoo5B/ceph-testcontainers-go/ceph` (add `-all` for
  every symbol), or
  [pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph).
- [Cluster configuration](../docs/guides/ceph-configuration.md): the
  composition model, service packages, dual-stack address families, and
  limitations and release differences.
- [Cluster operations](../docs/guides/ceph-operations.md): removing, detaching
  and reopening OSDs, the Cephx key change model, the failure archive and
  post-test verification.
- Guides: [contexts and cleanup](../docs/guides/contexts-and-cleanup.md),
  [errors](../docs/guides/errors.md),
  [fault injection](../docs/guides/fault-injection.md),
  [host networking](../docs/guides/host-network.md),
  [Messenger client fixtures](../docs/guides/messenger-client-fixtures.md),
  [native commands](../docs/guides/native-commands.md),
  [regional zones](../docs/guides/regional-zones.md),
  [releases and images](../docs/guides/releases-and-images.md).
