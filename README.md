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
the API; [CHANGELOG.md](CHANGELOG.md) lists every change.

Documentation for a published release is available at its Git tag.
See [Upgrading to v0.3.0](docs/guides/upgrading-to-v0.3.md) for the new native
input API, compatibility and the limits of the protocol receipts. For changes
from v0.1.0, see [Upgrading to v0.2.0](docs/guides/upgrading-to-v0.2.md).

## Modules

| Package | Import path | What it provides | Documentation |
| --- | --- | --- | --- |
| `ceph` | `github.com/JSYoo5B/ceph-testcontainers-go/ceph` | The cluster: MON, MGR and OSD topology, pools, Cephx identities, configuration, health, fault injection and diagnostics | [docs/ceph.md](docs/ceph.md) |
| `fault` | `github.com/JSYoo5B/ceph-testcontainers-go/fault` | Container pause, network delay, loss, rate, packet faults and endpoint interruption on existing fixtures | [docs/fault.md](docs/fault.md) |
| `override` | `github.com/JSYoo5B/ceph-testcontainers-go/override` | Reversible cluster-wide overrides with restoration handles; the client blocklist entry, the OSDMap full ratios, the MGR module state, the global OSD flag, the runtime debug level window, central configuration and messenger policy | [docs/override.md](docs/override.md) |
| `rbd` | `github.com/JSYoo5B/ceph-testcontainers-go/rbd` | RBD pools and namespaces, image watchers and locks, mirroring between clusters, backup and restore | [docs/rbd.md](docs/rbd.md) |
| `cephfs` | `github.com/JSYoo5B/ceph-testcontainers-go/cephfs` | Filesystems and MDS topology, subvolumes, snapshots and clones, client authorization and sessions, snapshot mirroring | [docs/cephfs.md](docs/cephfs.md) |
| `rgw` | `github.com/JSYoo5B/ceph-testcontainers-go/rgw` | S3 and Swift gateways, users, accounts and quotas, placement and storage classes | [docs/rgw.md](docs/rgw.md) |
| `rgw/multisite` | `github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite` | RGW multisite realms, zones and zonegroups, period transfer, sync status and sync policy | [docs/rgw-multisite.md](docs/rgw-multisite.md) |
| `capture` | `github.com/JSYoo5B/ceph-testcontainers-go/capture` | Opt-in packet capture on one container endpoint, with a caller-selected helper image, delivering a structurally checked classic pcap and a report | [docs/capture.md](docs/capture.md) |

Every fixture package follows the Testcontainers for Go module conventions: a `Run`
function that takes a context, an image and options, and a `Container` that
embeds `testcontainers.Container`. `capture` starts no fixture of its own: it
observes a container that the caller or a fixture already owns. The service packages build on `ceph`: their
`Run` starts a cluster with the service's defaults, their options can be passed
to `ceph.Run` together, and their `Container` embeds `*ceph.Container`. The API
reference for every package is also available with `go doc` and on
pkg.go.dev.

The implemented naming convention, source inventory and current-source retention
audit are recorded in [API naming](docs/design/api-naming.md). Upgrade guides
cover [ceph policy names](docs/guides/upgrading-ceph-policy-names.md),
[CephFS names](docs/guides/upgrading-cephfs-names.md), the
[RBD mirror package move](docs/guides/upgrading-rbd-mirror-package.md), the
[CephFS mirror package move](docs/guides/upgrading-cephfs-mirror-package.md),
[network profiles](docs/guides/upgrading-network-profile-names.md) and the
[RBD cache operation](docs/guides/upgrading-rbd-cache-name.md), the
[topology accessors](docs/guides/upgrading-context-accessors.md), the
[operations that moved onto handles](docs/guides/upgrading-handle-operations.md), the
[RGW multisite package](docs/guides/upgrading-rgw-multisite.md) and the
[typed OSD flags, PG IDs and connection config](docs/guides/upgrading-typed-values.md).

## Requirements

- Go 1.26 or later (v0.1.0 needs Go 1.25); use a patched toolchain. The
  repository selects Go 1.26.9 for its own checks. A consumer must select its
  own toolchain (see [the upgrade guide](docs/guides/upgrading-to-v0.2.md#go-and-client-platforms)).
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
		ceph.WithPools(ceph.PoolConfig{Name: "app"}),
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

Module pages, one per package, in the format of the Testcontainers for Go
module catalogue:

- [Ceph](docs/ceph.md): the cluster, its options and every `ceph.Container` method.
- [RBD](docs/rbd.md), [CephFS](docs/cephfs.md) and [RGW](docs/rgw.md).

Guides for tasks that span packages:

- [Fault injection](docs/guides/fault-injection.md): stopping, pausing and
  disconnecting daemons and clients, delaying or dropping messages, fencing,
  full clusters and damaged replicas.
- [Host network](docs/guides/host-network.md): bridge and host networking, and
  connecting native clients from the test process.
- [Releases and images](docs/guides/releases-and-images.md): supported Ceph
  releases, role images and image requirements.
- [Upgrading to v0.2.0](docs/guides/upgrading-to-v0.2.md): Go requirements,
  API and error compatibility, Messenger policies and release limitations.
- [Upgrading to v0.3.0](docs/guides/upgrading-to-v0.3.md): bounded native
  input, new fixture receipts and what remains unverified.
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

For contributors:

- [Testing](docs/contributing/testing.md): running and writing unit and
  integration tests.
- [CI](docs/contributing/ci.md): the GitHub Actions workflows and how to read a
  failing batch.
- [Package-split rewrite shards](docs/contributing/package-split-shards.md): add
  one shard per package move for the retained producer rewrites.
- [Behavioral bug compatibility](docs/contributing/bug-compatibility.md): explicit
  bug-ID selection policy and the planned supported-tag catalogue.
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
that a test selects with `RunOptions.WithImageSet`, `rgw.WithImage` or
`cephfs.WithMDSImage` keep the licenses of whoever publishes them.
