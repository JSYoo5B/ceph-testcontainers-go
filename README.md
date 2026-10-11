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
its data in a sparse BlueStore file inside its container.

This project is experimental and has no release tags yet.

## Modules

| Package | Import path | What it provides | Documentation |
| --- | --- | --- | --- |
| `ceph` | `github.com/jsyoo5b/ceph-testcontainers-go/ceph` | The cluster: MON, MGR and OSD topology, pools, Cephx identities, configuration, health, fault injection and diagnostics | [docs/ceph.md](docs/ceph.md) |
| `rbd` | `github.com/jsyoo5b/ceph-testcontainers-go/rbd` | RBD pools and namespaces, image watchers and locks, mirroring between clusters, backup and restore | [docs/rbd.md](docs/rbd.md) |
| `cephfs` | `github.com/jsyoo5b/ceph-testcontainers-go/cephfs` | Filesystems and MDS topology, subvolumes, snapshots and clones, client authorization and sessions, snapshot mirroring | [docs/cephfs.md](docs/cephfs.md) |
| `rgw` | `github.com/jsyoo5b/ceph-testcontainers-go/rgw` | S3 and Swift gateways, users, accounts and quotas, placement and storage classes, multisite replication and sync policy | [docs/rgw.md](docs/rgw.md) |

Every package follows the Testcontainers for Go module conventions: a `Run`
function that takes a context, an image and options, and a `Container` that
embeds `testcontainers.Container`. The service packages build on `ceph`: their
`Run` starts a cluster with the service's defaults, their options can be passed
to `ceph.Run` together, and their `Container` embeds `*ceph.Container`. The API
reference for every package is also available with `go doc` and on
pkg.go.dev.

## Requirements

- Go 1.25 or later.
- A Docker engine that runs Linux containers. Docker Desktop works. Host
  networking has extra requirements, described in
  [the host network guide](docs/guides/host-network.md).
- Access to pull the Ceph image and the Ryuk image.

The default image is the official `quay.io/ceph/ceph` Tentacle 20.2.x image,
pinned by digest as `ceph.DefaultImage`. Squid 19.2.x images also work, with
the differences listed in [the releases guide](docs/guides/releases-and-images.md).

## Installation

```
go get github.com/jsyoo5b/ceph-testcontainers-go/ceph
```

Add `rbd`, `cephfs` or `rgw` the same way when you need them.

## Quick start

```golang
package app_test

import (
	"testing"

	"github.com/jsyoo5b/ceph-testcontainers-go/ceph"
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
	conf, keyring, err := cluster.ConnectionConfig()
	if err != nil {
		t.Fatal(err)
	}
	_, _ = conf, keyring
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
gateway := objects.Gateways()[0] // AccessKey, SecretKey and Region
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
- [Contexts and cleanup](docs/guides/contexts-and-cleanup.md): how contexts
  bound calls, what each fixture owns, and how to restore temporary changes.
- [Running native commands](docs/guides/native-commands.md): how to do anything
  the typed API does not cover with the Ceph CLIs inside the containers.

For contributors:

- [Testing](docs/contributing/testing.md): running and writing unit and
  integration tests.
- [CI](docs/contributing/ci.md): the GitHub Actions workflows and how to read a
  failing batch.

## Related projects

[ceph-testcontainers-images](https://github.com/JSYoo5B/ceph-testcontainers-images)
checks Ceph images against the requirements of these modules and publishes
smaller role images (control, OSD, MDS, RGW) extracted from the official
images.
