# ceph-testcontainers-go

Testcontainers for Go modules that run a real, disposable Ceph cluster for
integration tests. The cluster runs the official Ceph daemons in containers:
one MON, one MGR and two OSDs by default, plus RBD pools, CephFS filesystems
with MDS daemons, and RGW gateways when you ask for them. Tests prepare and
change the server side through typed Go APIs. Your application talks to the
cluster with its own client: go-ceph, librados, librbd, libcephfs, the Ceph CLI
or an S3 SDK.

The modules control Ceph through the CLIs inside the containers. They have no
go-ceph, librados or cgo dependency and build with `CGO_ENABLED=0`. The Ceph
daemons need no privileged mode, host disks, LVM or systemd: each OSD stores
its data in a sparse BlueStore file inside its container by default. MemStore
and memory-backed BlueStore are also available for faster disposable fixtures.

Releases follow semantic versioning. Before v1.0.0, a minor release may change
the API without aliases for the old names. Final release notes are available
with the [published releases](https://github.com/JSYoo5B/ceph-testcontainers-go/releases).

Documentation for a published release is available at its Git tag.

## Packages

- [`ceph`](ceph/README.md): the cluster. MON, MGR and OSD topology, pools,
  Cephx identities, configuration, health and diagnostics.
- [`rbd`](rbd/README.md): RBD pools and namespaces, image watchers and locks,
  backup and restore.
  - [`rbd/mirror`](rbd/mirror/README.md): `rbd-mirror` replication between two
    clusters.
- [`cephfs`](cephfs/README.md): filesystems and MDS topology, subvolumes,
  snapshots and clones, client authorization and sessions.
  - [`cephfs/mirror`](cephfs/mirror/README.md): snapshot mirroring between two
    clusters.
- [`rgw`](rgw/README.md): S3 and Swift gateways, users, accounts and quotas,
  placement and storage classes.
  - [`rgw/multisite`](rgw/multisite/README.md): realms, zonegroups and zones,
    period transfer, sync status and sync policy.
- [`fault`](fault/README.md): container pause, network delay, loss, rate,
  packet faults and endpoint interruption on existing fixtures.
- [`override`](override/README.md): reversible cluster-wide changes with
  restoration handles (blocklist, full ratios, MGR modules, OSD flags, debug
  levels, central configuration, messenger policy).
- [`capture`](capture/README.md): opt-in packet capture on one container
  endpoint.

Every fixture package is a Testcontainers for Go module: `Run(ctx, image,
opts...)` returns a `Container` that embeds `testcontainers.Container`. The
service packages build on `ceph`: their `Container` embeds `*ceph.Container`,
and their options can be passed to `ceph.Run` together. `capture` starts no
fixture of its own; it observes a container that already exists.

## Requirements

- Go 1.26 or later; use a patched toolchain. The
  repository selects Go 1.26.9 for its own checks. A consumer must select its
  own toolchain; the toolchain line of this repository does not set it.
- A Docker engine that runs Linux containers. Docker Desktop works. Host
  networking has extra requirements, described in
  [the host network guide](docs/guides/host-network.md).
- Access to pull the Ceph image and the Ryuk image.

The default image is the published, pruned official Tentacle 20.2.4 aggregate
aggregate, pinned to its AMD64/ARM64 index digest as `ceph.DefaultImage`.
Explicit `ceph.ImageSet` selections use essential + client for ordinary
RADOS/RBD and add CephFS, mirror/cache or RGW images only for those features.
Runtime CI uses these dependency roles directly. Squid 19.2.x also works, with
the differences listed in [the releases guide](docs/guides/releases-and-images.md).

## Installation

```
go get github.com/JSYoo5B/ceph-testcontainers-go/ceph@latest
```

Add `rbd`, `cephfs` or `rgw` the same way when you need them.

## Quick start

```golang
package app_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func TestWithCeph(t *testing.T) {
	ctx := t.Context()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage,
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithPools(ceph.PoolConfig{Name: "app"})),
	)
	testcontainers.CleanupContainer(t, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}

	// Give a native client the cluster's ceph.conf and an admin keyring.
	cfg, err := cluster.ConnectionConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = cfg
}
```

The service packages start a cluster with their own defaults:

```golang
// An initialized RBD pool named rbd.
volumes, err := rbd.Run(ctx, ceph.DefaultImage)

// The filesystem tc-cephfs with one active MDS.
files, err := cephfs.Run(ctx, ceph.DefaultImage)

// One S3 gateway with a generated test user.
objects, err := rgw.Run(ctx, ceph.DefaultImage)
endpoint, err := objects.S3Endpoint(ctx)
gateways, err := objects.Gateways(ctx)
if err != nil {
	t.Fatal(err)
}
gateway := gateways[0] // AccessKey, SecretKey and Region
```

To serve several services from one cluster, pass their options to `ceph.Run`:

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	rbd.WithPools(ceph.PoolConfig{Name: "volumes"}),
	cephfs.WithFilesystems(cephfs.Config{Name: "shared"}),
	rgw.WithGateways(rgw.Config{Name: "s3"}),
)
```

## Documentation

Guides for tasks that span packages:

- [Ceph cluster configuration](docs/guides/ceph-configuration.md),
  [Ceph cluster operations](docs/guides/ceph-operations.md) and
  [Ceph recipes](docs/guides/ceph-recipes.md): the `ceph` concepts that span
  several APIs, and worked examples; each API is documented in its godoc.
- [Fault injection](docs/guides/fault-injection.md): stopping, pausing and
  disconnecting daemons and clients, delaying or dropping messages, fencing,
  full clusters and damaged replicas.
- [Host network](docs/guides/host-network.md): bridge and host networking, and
  connecting native clients from the test process.
- [Releases and images](docs/guides/releases-and-images.md): supported Ceph
  releases, role images and image requirements.
- [Contexts and cleanup](docs/guides/contexts-and-cleanup.md): how contexts
  bound calls, what each fixture owns, and how to restore temporary changes.
- [Running native commands](docs/guides/native-commands.md): how to do anything
  the typed API does not cover with the Ceph CLIs inside the containers.
- [Fixtures for Messenger client tests](docs/guides/messenger-client-fixtures.md):
  the profiles, daemon controls and differences for testing a Messenger client
  implementation against native daemons.
- [Classified errors](docs/guides/errors.md): telling a refused request, a
  feature the release lacks, a missing image tool, an engine limit and a
  failed native command apart with `errors.Is` and `errors.As`.
- [Bug compatibility](docs/guides/bug-compatibility.md): the registered
  `ceph_bug_<id>` tags and the policy for adding one.

For contributors:

- [Testing](docs/contributing/testing.md): running and writing unit and
  integration tests.
- [CI](docs/contributing/ci.md): the GitHub Actions workflows and how to read a
  failing batch.
- [Preparing a release](docs/contributing/releasing.md): the scope, evidence
  and publication checks for a candidate.

## Related projects

[ceph-testcontainers-images](https://github.com/JSYoo5B/ceph-testcontainers-images)
checks Ceph images against the requirements of these modules and publishes
smaller role images (essential, client, cephfs, rbd-aux, rgw, and the aggregate all) extracted from the official
images.

## License

The modules are licensed under the [MIT License](LICENSE), like Testcontainers
for Go.

They do not contain or link Ceph. Tests pull the Ceph container images at run
time and run the daemons and CLIs inside them; Ceph in those images is under
its own licenses, mainly LGPL-2.1 and LGPL-3.0 with some GPL-2.0 parts. Images
that a test selects with `ImageOptions.WithImageSet`, `ImageOptions.WithRGWImage` or
`ImageOptions.WithCephFSImage` keep the licenses of whoever publishes them.
