# CephFS

Import `github.com/JSYoo5B/ceph-testcontainers-go/cephfs` to give an
application that uses CephFS (libcephfs, go-ceph, the Python binding) a
disposable cluster with the filesystem state its integration test needs:
filesystems, their MDS topology and data pools. The feature packages act on
those filesystems: subvolume groups and subvolumes, snapshots and clones,
principals restricted to one subvolume and directory pins in
[`cephfs/subvolume`](subvolume/README.md), quiesce sets in
[`cephfs/quiesce`](quiesce/README.md), client sessions and their limits in
[`cephfs/session`](session/README.md), snapshot schedules in
[`cephfs/snapschedule`](snapschedule/README.md). File I/O belongs to the client under
test; the package never mounts CephFS on the test host. See
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
   `ScaleMDS`, `AddDataPool`, `OverrideMaxFileSize`; `snapschedule.Add`,
   `session.OverrideTimeouts`, `session.OverrideRefusal`;
   in [`cephfs/subvolume`](subvolume/README.md)
   `subvolume.CreateGroup` / `subvolume.Create` (quotas, data pools, RADOS
   namespaces), `Subvolume.CreateSnapshot` and `Snapshot.Clone`,
   `Subvolume.Authorize` (a principal for one subvolume) and
   `Subvolume.OverridePin`.
3. **Hand it to the SUT**: run the client in a container attached with
   `cluster.WithClient()` (admin keyring) or `grant.Client.WithIdentity()` (one
   principal), and pass it `fs.FilesystemName` and the subvolume path.
4. **Reproduce conditions and observe**: stop an MDS or `fault.Pause` a client,
   `quiesce.Start`, then check `MDSStatus`, `WatchMDSs`, `WaitReady`,
   `session.List`, `subvolume.Info`, `Clone.Status`, `DataPools` and the
   `ceph` observation APIs.
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
volume, err := subvolume.Create(ctx, fs, subvolume.Config{Name: "app", SizeBytes: 64 << 20})
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
  the MGR `volumes` and `mirroring` modules; the client image (the `Run` image
  only for an aggregate) needs the Python `cephfs` binding, because pin reads,
  clone identity checks and scheduled snapshot listings run libcephfs in the
  client container; MDSs use
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
  The fixture's own libcephfs probes appear briefly in `session.List`.
- MDS: names, keys, entrypoint and networking are fixed; each MDS has a
  128 MiB cache. Damaged-metadata recovery, multi-rank replacement and
  `MDS.AddReplacement` with standbys are not provided.
- Snapshots do not pause writers: use `quiesce.Start` or application
  synchronisation. Quota enforcement and usage accounting are native and can
  lag writes; removal moves data to Ceph's trash and purges asynchronously.
- Native timing: a dead MDS is replaced only after `mds_beacon_grace` (15 s)
  unless failed with `ceph mds fail`; an unresponsive client is evicted after
  `session_timeout` (60 s) when another client wants its capabilities, otherwise
  after `session_autoclose` (300 s), and its address stays blocklisted for an
  hour; a mount against a silent MDS fails after `client_mount_timeout` (300 s).
  Shorten what a test waits for with `session.OverrideTimeouts` and client
  options.

## Watching MDS ranks

To react to a filesystem's MDS topology while the SUT runs,
`fs.WatchMDSs(ctx, cfg)` polls the FSMap in one goroutine and sends a change
of `MDSStatus` (max_mds, active ranks, ordinary and replay standbys) on one
channel, with the FSMap and MDSMap epochs of that read. The baseline is
buffered before it returns. Wait for it before you stop an MDS. A takeover, a
restarted daemon's new GID and a standby that registers again are events; a
rank still in replay is in no list, as in `MDSStatus`. A failed read is a
`WatchGap` event and the next success is `WatchResumed`. A full buffer ends
the watch with `ceph.ErrWatchOverflow` instead of dropping events, and so do
a filesystem recreated under the same name, a removed filesystem and cluster
termination. The watch never waits for readiness; use `WaitReady` for the
requested standby capacity. Call `Stop` before the fixture's cleanup: it joins
the watch and returns why it ended. See
[watching state](../docs/guides/contexts-and-cleanup.md#watching-state) for the
lifecycle that every Watch shares.

## File size limit

`fs.OverrideMaxFileSize(ctx, bytes)` lowers the filesystem's `max_file_size`,
so that a client write ending past `bytes` or a truncate past it fails with
EFBIG while smaller files stay usable. `bytes` must be at least
`cephfs.MinMaxFileSize` (64 KiB), the native minimum. The limit is
filesystem-wide: it is not a quota, it does not count used capacity and it does
not shrink existing files. Clients check the limit from the MDSMap they hold,
so mount the client under test after the call returns.

The handle binds the filesystem's FSMap ID and the captured previous value.
`MaxFileSizeOverrideHandle.Restore` puts that value back; it refuses a value
that is neither the applied nor the captured one, and a filesystem recreated
under the same name. One override per filesystem may be active. Keep a non-nil
handle returned with an error, since a lost reply can leave the value changed.

```golang
limit, err := fs.OverrideMaxFileSize(ctx, cephfs.MinMaxFileSize)
if limit != nil {
	defer limit.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
// The SUT's write past 64 KiB now fails with EFBIG; a 4 KiB file still works.
```

## Directory-pin timing

For a fixture that prepares several directory-pin handoffs on two or more
active MDS ranks, opt in with
`fs.OverrideTiming(ctx, cephfs.TimingProfile{DirectoryPins: true})`. The ordinary
fixture keeps Ceph's default timing. The profile is qualified for MDS releases
19.2.5 and 20.2.4 with an effective five-second `mds_tick_interval`; it checks
each target's native version and value before changing anything. It applies a
one-second interval to the current healthy owned active MDSs and verifies their
effective readback. Other filesystems and future scaled or replaced daemons are
outside its scope.

The returned values are existing `override.ConfigHandle`s. Schedule restoration
of **all** returned handles, in reverse order, even when application returns an
error. A partial application or uncertain write still needs restoration. The
zero profile changes nothing. Application serializes with filesystem topology
operations; external configuration writes and daemon restarts must not race it.

An already queued tick keeps its previous deadline. The first handoff can
therefore still wait five seconds, and application/readback costs can outweigh
the saving for a single pin. The tick also drives other MDS upkeep: this profile
is for directory-pin fixture setup, not session expiry, reconnection, failure
detection or fault-overlap timing. Restore before testing those behaviors and
wait for each daemon's effective configuration to return to its previous value.
Restoration preserves the original database entry or its absence; it does not
rewrite higher-precedence local or runtime settings.

See the compiled `ExampleFilesystem_OverrideTiming`. The opt-in native
`TestCephFSPinTimingQualification` compares baseline/profile runs in a balanced ABBA order
with the same image, topology, polling and file data. Each handoff verifies actual
MDS subtree authority, fresh libcephfs I/O and pin restoration; it reports both
handoff time and scenario time including timing application and restoration.
Run it with `-tags=all,native_regression` on a Linux Docker host.

The consuming path is [MDS tick scheduling](https://github.com/ceph/ceph/blob/v20.2.4/src/mds/MDSDaemon.cc)
through [MDBalancer pin processing](https://github.com/ceph/ceph/blob/v20.2.4/src/mds/MDBalancer.cc),
which runs before the separate balancer interval check. Changing
`mds_bal_interval` does not select this wait. Numeric options can be updated at
runtime under [Ceph's option rules](https://github.com/ceph/ceph/blob/v20.2.4/src/common/options.h).

## Reference

- API: [pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/cephfs)
  or `go doc -all github.com/JSYoo5B/ceph-testcontainers-go/cephfs`.
- [Native commands](../docs/guides/native-commands.md#cephfs-state-and-settings)
  for what has no typed API; [Fault injection](../docs/guides/fault-injection.md)
  (MDS failover, frozen and evicted clients, an MDS that ignores clients);
  [Contexts and cleanup](../docs/guides/contexts-and-cleanup.md);
  [Errors](../docs/guides/errors.md).
