# CephFS subvolumes

Import `github.com/JSYoo5B/ceph-testcontainers-go/cephfs/subvolume` to give an
application that works in CephFS subvolumes the state its integration test
needs in an existing filesystem: subvolume groups and subvolumes with quotas,
data pools and RADOS namespaces, snapshots and clones, a Cephx principal
restricted to one subvolume, and directory pin policies. The filesystem, its
MDS topology and its data pools stay in the [cephfs package](../README.md); the
package starts no container. See
[API purpose and purpose tags](../../docs/guides/api-purpose.md).

## Order of use

1. **Start a filesystem** with `cephfs.Run`, `cephfs.Start` or
   `cephfs.WithFilesystems`, and take its handle from
   `cephfs.Filesystems(cluster.Container)`.
2. **Create the resources** with `subvolume.CreateGroup(ctx, fs, cfg)` and
   `subvolume.Create(ctx, fs, cfg)`. Each returns an owned handle; operations
   on an existing resource are methods of its handle: `Resize`,
   `Subvolume.CreateSnapshot` and `Snapshot.Clone` (then `Clone.Wait`),
   `Subvolume.Authorize` (a principal for this subvolume), `OverridePin` on a
   subvolume or group.
3. **Hand it to the SUT**: pass `fs.FilesystemName` and `Subvolume.Path`, and
   run the client with `grant.Client.WithIdentity()` (the restricted
   principal) or `cluster.WithClient()` (admin keyring).
4. **Observe**: `subvolume.Info`, `subvolume.GroupInfo`,
   `subvolume.SnapshotInfo`, `subvolume.Names`, `subvolume.GroupNames` and
   `subvolume.AuthorizedClients` read native state by name, including
   resources the SUT created; `Subvolume.Snapshots`, `Clone.Status` and
   `PinPolicy` read an owned resource.
5. **Clean up**: terminate the cluster; it removes the filesystem with every
   owned resource and principal. Use `Remove`, `Clone.RemovePartial`,
   `Authorization.Deauthorize`, `Authorization.EvictClients` and
   `PinOverrideHandle.Restore` only to test those steps or to restore state
   mid-test.

```golang
cluster, err := cephfs.Run(ctx, ceph.DefaultImage)
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
fs := cephfs.Filesystems(cluster.Container)[0] // tc-cephfs
volume, err := subvolume.Create(ctx, fs, subvolume.Config{Name: "app", SizeBytes: 64 << 20, NamespaceIsolated: true})
if err != nil {
	t.Fatal(err)
}
grant, err := volume.Authorize(ctx, subvolume.AuthorizationConfig{ClientID: "app", Access: "rw"})
if err != nil {
	t.Fatal(err)
}
client, err := testcontainers.Run(ctx, appImage, grant.Client.WithIdentity())
testcontainers.CleanupContainer(t, client)
if err != nil {
	t.Fatal(err)
}
// The client mounts fs.FilesystemName with libcephfs and works under volume.Path.
```

## Concepts and limits

- A handle is bound to the native resource it created: its filesystem, path
  and birth time. Editing its exported fields does not redirect an operation,
  copies share their state, and a replaced resource or a handle of another
  filesystem is refused. The name-based functions grant no ownership.
- Creation refuses an existing name instead of relying on Ceph's idempotent
  create, which could change an existing quota or layout. A non-nil handle
  returned with an error records an attempted creation; only a confirmed one
  can be changed.
- Do not race the operations with raw `ceph fs subvolume` edits on the same
  resources; the CLI has no compare-and-swap. Operations of one filesystem
  wait for each other within the caller's context.
- `Authorize` needs a namespace-isolated subvolume and creates a fresh
  principal; it never adopts an existing one. Deauthorize a grant before
  removing its subvolume.
- Snapshots do not pause writers: use [`cephfs/quiesce`](../quiesce/README.md)
  or application synchronisation. Quota enforcement and usage accounting are
  native and can lag writes; removal moves data to Ceph's trash and purges
  asynchronously.
- Pin reads and clone identity checks run libcephfs in the client container,
  so the client image needs the Python `cephfs` binding. A pin policy reports
  the local virtual xattrs, not inherited policy or actual MDS authority.

## Reference

The full contract of every type and function is in go doc:
[pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/cephfs/subvolume)
or `go doc -all github.com/JSYoo5B/ceph-testcontainers-go/cephfs/subvolume`.
See also [Native commands](../../docs/guides/native-commands.md#cephfs-state-and-settings)
for what has no typed API,
[Contexts and cleanup](../../docs/guides/contexts-and-cleanup.md) and
[Errors](../../docs/guides/errors.md).
