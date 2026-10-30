# CephFS

Import `github.com/JSYoo5B/ceph-testcontainers-go/cephfs` to give an
application that uses CephFS (libcephfs, go-ceph, the Python binding) a
disposable cluster with the filesystem state its integration test needs:
filesystems and their MDS topology, data pools, subvolume groups and
subvolumes, snapshots and clones, principals restricted to one subvolume,
directory pins, quiesce sets and client session limits. File I/O belongs to the
client under test; the package never mounts CephFS on the test host. See
[API purpose and purpose tags](../docs/guides/api-purpose.md).

```
go get github.com/JSYoo5B/ceph-testcontainers-go/cephfs
```

## Order of use

1. **Start a cluster with filesystems**: `cephfs.Run(ctx, ceph.DefaultImage,
   opts...)` creates the default filesystem `tc-cephfs` with one active MDS;
   `cephfs.WithFilesystems(cephfs.Config{...})` selects others (also on
   `ceph.Run`, `rbd.Run` and `rgw.Run`), and `cephfs.Start` adds one to a
   running cluster. `cephfs.Filesystems(cluster.Container)` returns their handles.
2. **Prepare the state** the scenario needs on a `*cephfs.Filesystem`:
   `ScaleMDS`, `AddDataPool`, `CreateSubvolumeGroup` / `CreateSubvolume`
   (quotas, data pools, RADOS namespaces), `Subvolume.CreateSnapshot` and
   `SubvolumeSnapshot.Clone`, `Subvolume.Authorize` (a principal for one
   subvolume), `Subvolume.OverridePin`, `AddSnapshotSchedule`,
   `OverrideSessionTimeouts`.
3. **Hand it to the SUT**: run the client in a container attached with
   `cluster.WithClient()` (admin keyring) or `grant.Client.WithIdentity()` (one
   principal), and pass it `fs.FilesystemName` and the subvolume path.
4. **Reproduce conditions and observe**: stop an MDS or `fault.Pause` a client,
   `QuiesceSubvolumes`, then check `MDSStatus`, `WaitReady`, `Sessions`,
   `SubvolumeInfo`, `SubvolumeClone.Status`, `DataPools` and the `ceph`
   observation APIs.
5. **Clean up**: terminate the cluster; it removes owned filesystems,
   subvolumes, principals and daemons. Use the handles' `Remove`,
   `Deauthorize` and `Restore` only to test those steps or to restore state
   mid-test.

```golang
cluster, err := cephfs.Run(ctx, ceph.DefaultImage)
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
fs := cephfs.Filesystems(cluster.Container)[0] // tc-cephfs
volume, err := fs.CreateSubvolume(ctx, cephfs.SubvolumeConfig{Name: "app", SizeBytes: 64 << 20})
if err != nil {
	t.Fatal(err)
}
client, err := testcontainers.Run(ctx, appImage, cluster.WithClient())
testcontainers.CleanupContainer(t, client)
if err != nil {
	t.Fatal(err)
}
// The client mounts fs.FilesystemName with libcephfs and works under volume.Path.
```

## Concepts and limits

- Everything in [`ceph`](../ceph/README.md) applies to the cluster. Snapshot
  mirroring between two clusters is in [`cephfs/mirror`](mirror/README.md).
- Images by role: the `Run` image runs the MON, MGR and control CLI and needs
  the MGR `volumes` and `mirroring` modules and the Python `cephfs` binding
  (pin reads and clone identity checks run libcephfs there); MDSs use
  `ceph.ImageOptions.WithCephFSImage` or the `Run` image; mirror daemons take
  their own image. `ceph.DefaultImage` (Tentacle 20.2.4) contains every role.
  Keep every role on the same release. Squid 19.2.5 is supported with no
  visible CephFS difference. See
  [Releases and images](../docs/guides/releases-and-images.md).
- Handles are owned by the cluster and bound to the native resource they
  created; a handle of another filesystem, or of a replaced resource, is
  refused. Fixture operations serialize within the caller's context; do not
  race them with raw `ceph fs` edits on the same resources.
- Clients: no kernel or FUSE mount. Clients are userspace libraries in Linux
  containers, or the test process with host networking on a Linux Docker host.
  The fixture's own libcephfs probes appear briefly in `Sessions`.
- MDS: names, keys, entrypoint and networking are fixed; each MDS has a
  128 MiB cache. Damaged-metadata recovery, multi-rank replacement and
  `MDS.AddReplacement` with standbys are not provided.
- Snapshots do not pause writers: use `QuiesceSubvolumes` or application
  synchronisation. Quota enforcement and usage accounting are native and can
  lag writes; removal moves data to Ceph's trash and purges asynchronously.
- Native timing: a dead MDS is replaced only after `mds_beacon_grace` (15 s)
  unless failed with `ceph mds fail`; an unresponsive client is evicted after
  `session_timeout` (60 s) when another client wants its capabilities, otherwise
  after `session_autoclose` (300 s), and its address stays blocklisted for an
  hour; a mount against a silent MDS fails after `client_mount_timeout` (300 s).
  Shorten what a test waits for with `OverrideSessionTimeouts` and client
  options.

## Reference

- API: [pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/cephfs)
  or `go doc -all github.com/JSYoo5B/ceph-testcontainers-go/cephfs`.
- [Native commands](../docs/guides/native-commands.md#cephfs-state-and-settings)
  for what has no typed API; [Fault injection](../docs/guides/fault-injection.md)
  (MDS failover, frozen and evicted clients, an MDS that ignores clients);
  [Contexts and cleanup](../docs/guides/contexts-and-cleanup.md);
  [Errors](../docs/guides/errors.md).
