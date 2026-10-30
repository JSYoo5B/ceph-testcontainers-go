# RBD

Import `github.com/JSYoo5B/ceph-testcontainers-go/rbd` to give an application
that uses RBD (librbd, go-ceph, the `rbd` CLI) a disposable Ceph cluster with
the server side its integration test needs: initialized pools, namespaces,
observation of image watchers and locks, backup transport between clusters and
the immutable object cache. Image operations themselves (create, clone,
snapshot, trash, migration, encryption, locks) belong to the client under test
and are not wrapped. See
[API purpose and purpose tags](../docs/guides/api-purpose.md).

```
go get github.com/JSYoo5B/ceph-testcontainers-go/rbd
```

## Order of use

1. **Start a cluster** with `rbd.Run(ctx, ceph.DefaultImage, opts...)`. It is a
   `ceph` cluster (MON, MGR, two OSDs by default) with an initialized pool
   named `rbd`; `rbd.WithPools` selects other pools. On a cluster started by
   `ceph.Run` or another service package, prepare a pool with `rbd.InitPool`.
   Call `WaitForClean` before the first client I/O.
2. **Prepare the resources** the scenario needs: `rbd.CreateNamespace` for
   tenant isolation, `ceph.Container.CreateClient` for least-privilege
   identities (`profile rbd`, `profile rbd-read-only`),
   `rbd.StartImmutableObjectCache` in a client container for parent caching.
3. **Hand the cluster to the SUT**: run the application in a container
   attached with `cluster.WithClient()` or `client.WithIdentity()`, which
   provides `ceph.conf` and a keyring. Clients reach OSDs directly, so in the
   default bridge mode they must run in such a container.
4. **Observe and verify**: `rbd.ImageClients` (watchers, lock owners),
   `rbd.ListNamespaces`, `ImmutableObjectCacheLease.Status`, the `ceph`
   observation APIs, or `rbd.ExportBackup` / `rbd.RestoreBackup` to move an
   image to another cluster and check it there.
5. **Clean up**: terminate the cluster (`testcontainers.CleanupContainer`);
   namespaces and images go with it. Restore leases such as the immutable object
   cache before removing the caller-owned container they run in.

```golang
cluster, err := rbd.Run(ctx, ceph.DefaultImage)
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
if err := cluster.WaitForClean(ctx); err != nil {
	t.Fatal(err)
}
app, err := testcontainers.Run(ctx, appImage, cluster.WithClient())
testcontainers.CleanupContainer(t, app)
if err != nil {
	t.Fatal(err)
}
// The application in app uses /etc/ceph/ceph.conf and the pool "rbd".
```

## Concepts and limits

- Everything in [`ceph`](../ceph/README.md) applies to the returned cluster:
  clients and Cephx, pools, configuration, health, faults and topology changes.
  Mirroring between two clusters is in [`rbd/mirror`](mirror/README.md).
- Images: `ceph.DefaultImage` (Ceph Tentacle) supplies every role. With an
  explicit image set, essential supplies the daemons, client the ordinary
  `rbd` commands, and rbd-aux the immutable object cache and mirroring; see
  [Releases and images](../docs/guides/releases-and-images.md).
- No kernel RBD, `rbd-nbd` or `/dev/mapper`: containers run unprivileged with no
  host block devices. Native clients on the test host need host network mode
  on a Linux Docker host; see [Host network](../docs/guides/host-network.md).
- Namespaces have no generation identity: do not delete and re-create a
  namespace that a descriptor still refers to. Incremental restore checks the
  baseline snapshot by name only.
- Permissions: a read-only identity must open images read-only (a writable
  open registers a header watch, which needs write permission). A refused
  write through a read-only handle (`EROFS`) is a client-side check; to prove
  the server-side permission, test operations the server must deny (image
  creation, a direct RADOS write, another namespace: `EPERM`/`EACCES`). Cephx
  changes apply to new connections only.
- Common pitfalls: one OSD with replica 1 passes `WaitForClean` but reports
  `POOL_NO_REDUNDANCY`; keep an `override.AddBlocklist` fence until the old
  lock owner has observed `ESHUTDOWN`.

### Preparing for client features

The client feature under test decides what the fixture must provide:

| Client feature | Server-side preparation |
| --- | --- |
| Clone, flatten | Format 2 images with `layering`; clone v1 needs a protected parent snapshot. |
| Trash move, restore, purge | An initialized pool; a dedicated namespace so purges do not touch other tests' trash. The Python binding converts trash deferment times with local time: run it with `TZ=UTC`. |
| Live migration | A replicated initialized pool, `layering` on source and target, source writers closed. |
| Group snapshots | Format 2 images; flush and close writers first. It is a checkpoint of those images, not an application transaction. |
| Exclusive lock | The `exclusive-lock` feature and two independent RADOS sessions; `rbd.ImageClients` shows the owner, `fault.Pause` and `override.AddBlocklist` freeze and fence it. |
| Encryption (LUKS1, LUKS2) | librbd with libcryptsetup (client role and `all` images); no `journaling` on the image. librbd has no rekey API: run `cryptsetup luksChangeKey` on the exported header. |
| Erasure-coded data | An EC pool with `AllowOverwrites`, passed as the image's data pool. |
| Backup to another cluster | `rbd.ExportBackup` / `ExportIncremental` on one cluster, `RestoreBackup` / `RestoreIncremental` on the other. |

## Reference

- API: [pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/rbd)
  or `go doc -all github.com/JSYoo5B/ceph-testcontainers-go/rbd`.
- [Native commands](../docs/guides/native-commands.md) (including common
  `rbd` and `rbd mirror` commands),
  [Contexts and cleanup](../docs/guides/contexts-and-cleanup.md),
  [Errors](../docs/guides/errors.md).
