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
Ryuk) still needs the Docker engine. The Go module has no go-ceph, librados or
cgo dependency and builds with `CGO_ENABLED=0`.

Requirements: Go 1.26 or later, a Docker engine that runs Linux containers, and
access to pull the Ceph and Ryuk images. The module is experimental; there are
no release tags yet.

```sh
go get github.com/JSYoo5B/ceph-testcontainers-go/ceph
```

## Usual order of use

1. **Start the cluster.** `ceph.Run(ctx, img, opts...)` with `ceph.DefaultImage`
   (or role images through `WithImageOptions`) builds and waits for the whole
   topology. Settings are grouped values, each passed through one option:
   `WithClusterConfig` (MON/MGR/OSD counts, OSD layout, CRUSH root, pool
   defaults, initial pools), `WithOSDStorage`, `WithNetworkConfig`,
   `WithImageOptions`, plus `WithAuth`, `WithConfigFile`, `WithHostNetwork` and
   `WithStartupTimeout`. Contradictory options fail before anything is created.
   Register cleanup before checking the error: a partially started cluster is
   returned with the error and still owns containers.
2. **Prepare the state the test needs.** `WaitForClean` before client I/O;
   `CreatePool` and the `Pool` handle (replication, quota, placement, PG count);
   `CreateClient` for a Cephx identity with exact caps; `AddOSD`, `AddMonitor`,
   `AddManager` and the daemon handles' `Remove`, `Detach`, `SetIn` for
   topology. The [override](../override/README.md) package changes
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
4. **Observe and verify.** `Observe` returns an `Observer` for health, PG and
   pool reports, quorum, OSD states, MGR modules and services, watchers and
   blocklist entries. `Ceph` and `RunNativeCommand` run bounded native commands
   when no typed observation exists.
5. **Finish.** `TerminateOnCleanup` registers finalization: checks, caller
   cleanups, an optional failure archive (`CollectDiagnostics`,
   `CollectCrashMetadata`) and `Finalization.Verify` for crash and cluster-log
   expectations, then `Terminate`, which removes every owned container,
   network and volume.

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
	if err := cluster.WaitForClean(ctx); err != nil { // Run does not wait for clean PGs
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
  missing optional role never falls back. See
  [Releases and images](../docs/guides/releases-and-images.md).
- **Networks.** Bridge mode by default: only MON `a` publishes ports, so native
  RADOS, RBD and CephFS clients run in a `WithClient` container. Host mode lets
  a process on a Linux Docker host connect directly. IPv6, dual-stack and a
  separate cluster network are bridge-mode options.
- **Placement is logical.** All daemons share one Docker engine; CRUSH hosts and
  racks test placement rules, not physical failure tolerance, and OSD files are
  sparse, so fullness is simulated with full ratios.
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
  [releases and images](../docs/guides/releases-and-images.md).
