# Known release issues

This page lists native Ceph behaviour of particular releases that a test
written with this module can run into: defects, and results that differ from
what the S3, POSIX or Ceph documentation leads a client to expect. Release
differences that the module handles itself are in
[Releases and images](releases-and-images.md#behaviour-that-differs-on-squid).

Each entry names the releases that were run and showed the behaviour
(**Affected**), the releases that were run and did not (**Not affected**),
what a consumer's test sees, what to do about it, and the evidence. A release
named in neither list was not run for that entry. An experiment case manual
gives the trigger, controls, command and results of its case; see
[Experiment cases](../contributing/testing.md#experiment-cases) for how to
run one on another image.

- [Cluster: MON, MGR, OSD and PGs](#cluster-mon-mgr-osd-and-pgs)
- [RBD](#rbd)
- [CephFS](#cephfs)
- [RGW](#rgw)
- [RGW multisite](#rgw-multisite)

## Cluster: MON, MGR, OSD and PGs

### MON paused past the ticket lifetime

A MON paused for longer than the MON ticket lifetime can strand a daemon
whose MON session was on it: its reconnect is refused with `EACCES`, and it
stops authenticating until it is restarted. With 12-second tickets and a
25-second pause this happened to the active MGR in two of four native runs.

- Affected: Squid and Tentacle; the patches that were run are not recorded,
  and Umbrella was not run.
- Guidance: check MGR-dependent behaviour before such a pause, or restart the
  stranded daemon.
- Evidence: [Daemon controls](messenger-client-fixtures.md#daemon-controls).

### PG merge with a primary override

A reduction of a pool's PG count (`Pool.SetPGCount`) never starts a merge
while a merge participant has a `pg-upmap-primary` entry
(`Pool.OverridePGPrimary`, or the read balancer) that gives it another
primary than the other participant has: the pool stays at its old `pg_num`
with both PGs active and clean, and `Pool.WaitForPGCount` runs into its
deadline. The manager clears the upmap entries of merge participants but not
their primary overrides.

- Affected: Squid 19.2.5 and Tentacle 20.2.4.
- Not affected: Tentacle 20.2.5 and Umbrella 21.2.0 remove every primary
  override of the pool and merge; overrides of other pools stay.
- Guidance: restore the override before the reduction.
- Evidence: [`mgr_pg_merge_primary_override`](../../internal/integration/experiments/mgr_pg_merge_primary_override/README.md).

### Cephx key types before Tentacle 20.2.4

`ceph-authtool` has no `--key-type` and `monmaptool` no cipher options in
20.2.0 to 20.2.3, so these releases generate only `aes` keys, as Squid does.
`ceph.WithAuth` and `CreateClient` detect the options in the essential image:
they accept `aes` and refuse `aes256k` before any change with
`ceph.ErrUnsupported`, naming `ceph-authtool --key-type` as the capability.

- Affected: Tentacle 20.2.0 to 20.2.3.
- Not affected: Tentacle 20.2.4.
- Guidance: use `aes` keys on these releases, or assert the classified
  refusal (see [Classified errors](errors.md#tests-across-releases)).
- Evidence: `TestAuthCiphersWithoutKeyTypeOption` (`internal/integration`)
  with the official 20.2.3 image.

## RBD

### Group membership with explicit namespace options

Explicit namespace options for group image add and remove can silently
change membership in the default namespace. Positional group and image
specifications select the intended namespace.

- Affected: Squid 19.2.0.
- Not affected: Squid 19.2.1, where both forms select it.
- Guidance: give the group and the image as positional specifications.
- Evidence: [`rbd_group_image_namespace`](../../internal/integration/experiments/rbd_group_image_namespace/README.md).

### Group snapshots and the skip quiesce mode

The default-flags group snapshot call notifies quiesce watchers even when the
configured mode is skip. The individual image snapshot honors skip on that
release.

- Affected: Squid 19.2.2.
- Not affected: Squid 19.2.3, where both calls honor skip.
- Guidance: do not rely on skip for group snapshots on the affected release.
- Evidence: [`rbd_group_snap_default_quiesce`](../../internal/integration/experiments/rbd_group_snap_default_quiesce/README.md).

### Image I/O statistics filtered by an erasure-coded data pool

Filtering by an erasure-coded data pool can omit an active image whose
metadata resides in a separate pool. An unfiltered query identifies the same
image.

- Affected: Squid 19.2.5.
- Not affected: Squid 19.2.7, where the filtered query also identifies it.
- Guidance: query without the pool filter and select the image yourself.
- Evidence: [`rbd_perf_data_pool`](../../internal/integration/experiments/rbd_perf_data_pool/README.md).

### Group snapshot listed again after its removal

The class method that stores a group snapshot's record accepts a COMPLETE
update for a snapshot that was already removed and writes the record again
without its order entry. `rbd group snap ls` then succeeds and names the
removed snapshot as `complete`, although its image snapshots are gone. The
ordinary commands send such an update only when a creation races with the
removal; the case replays the stored record through the `rados` Python
binding.

- Affected: Tentacle 20.2.4 and Umbrella 21.2.0.
- Guidance: a test that removes a group snapshot while another creation may
  still complete must not trust `rbd group snap ls` alone; check the member
  images' snapshots.
- Evidence: [`rbd_group_snap_late_complete`](../../internal/integration/experiments/rbd_group_snap_late_complete/README.md).

### Python `rbd` snapshot list of a missing group

`rbd.Group(ioctx, name).list_snaps()` for a group that does not exist crashes
the Python process with `SIGSEGV` instead of raising `ObjectNotFound`: when
the native call fails, the binding's iterator is freed with its original
entry count and frees entries that the call never filled, so no `except`
clause runs. It crashed in every attempt with the published 20.2.4 aggregate.
The module does not list the snapshots of a missing group.

- Affected: Tentacle 20.2.4.
- Not affected: Tentacle 20.2.5 and Umbrella 21.2.0 raise the exception and
  continue. Squid 19.2.5 returns an empty list (see
  [Differences you may hit in your own native checks](releases-and-images.md#differences-you-may-hit-in-your-own-native-checks)).
- Guidance: check that the group exists (`rbd group ls`) before listing its
  snapshots from Python on 20.2.4, or run the listing in a child process.
- Evidence: native research in
  [#502](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/502); no
  case manual.

### Python `RBD().trash_purge` with a cutoff

The binding reads the calendar fields of `expire_ts` as local time of the
client process, so a timezone-aware UTC cutoff lands at the instant minus the
client's UTC offset. Nine hours ahead of UTC an image whose deferment ended
four hours before the cutoff stays in the trash; nine hours behind, an image
whose deferment ends five hours after it is removed.

- Affected: Squid 19.2.5, Tentacle 20.2.4 and Umbrella 21.2.0.
- Guidance: run such a purge with `TZ=UTC`, or pass a naive datetime of the
  client's local time.
- Evidence: [`rbd_trash_purge_timezone`](../../internal/integration/experiments/rbd_trash_purge_timezone/README.md).

### Snapshot copy resumed after a killed mirror receiver

When an `rbd-mirror` daemon is killed while it copies a mirror snapshot and
the same daemon is started again, it resumes the copy and marks the snapshot
complete, but the object numbered `last_copied_object_number` at the kill
keeps the previous snapshot's bytes. The receiver persists the number of
contiguous objects copied, and the restart reads it as the index of the last
copied one, so the copy resumes one object later. The object is lost only
when its copy had not finished at the kill; a checkpoint of zero restarts
from object 0. It happened for an ordinary mirror snapshot and for a
demotion snapshot (SIGKILL on all releases below, `Stop` without a grace
period on 19.2.4 and 20.2.4); a graceful stop was not tried. Squid 19.2.3
does not resume an interrupted demotion copy at all (Tentacle 20.2.0 has the
same code); the destination stays `up+unknown` ("remote image is not
primary").

- Affected: Squid 19.2.4 and 19.2.5, Tentacle 20.2.4 and Umbrella 21.2.0
  (native); every 19.2, 20.2 and 21 release tag through 21.3.0 has the same
  source. No release fixes it.
- Guidance: a test that kills a receiver during a snapshot sync must compare
  the destination's data afterwards rather than trust the completed snapshot.
- Evidence: [`rbd_mirror_resume_checkpoint`](../../internal/integration/experiments/rbd_mirror_resume_checkpoint/README.md)
  (the checkpoint and the lost object) and
  [`rbd_mirror_demote_restart`](../../internal/integration/experiments/rbd_mirror_demote_restart/README.md)
  (the demotion copy).

## CephFS

### libcephfs creation at a dangling symlink

`mkdir`, `symlink`, `link` and `mknod` through libcephfs follow a symlink at
the new name when its target does not exist: they succeed and make the entry
at the symlink's target, where POSIX requires `EEXIST` with nothing made. A
symlink whose target exists and any other occupied name report `EEXIST`.

- Affected: Squid 19.2.3 and 19.2.5, Tentacle 20.2.4 and 20.2.5, Umbrella
  21.2.0.
- Not affected: Squid 19.2.2 reports `EEXIST`.
- Guidance: a test that relies on `EEXIST` to detect an occupied name through
  libcephfs should check the name with `lstat` first, and a test that plants a
  symlink as an obstacle should point it at an existing entry.
- Evidence: [`cephfs_dangling_symlink_create`](../../internal/integration/experiments/cephfs_dangling_symlink_create/README.md).

### libcephfs snapshot listing with a small client cache

A libcephfs directory handle of a snapshot (`dir/.snap/name`) with more
entries than one readdir reply returns entries of the live directory, and
loses most of the snapshot's, once the client has trimmed its dentry of the
snapshot: the cache holds more than `client_cache_size` dentries and a client
tick (one second) passes between two requests of the handle. Nothing reports
an error. With the default of 16384 and fewer cached entries than that the
listing is exact.

- Affected: Squid 19.2.2 and 19.2.5, Tentacle 20.2.4 and 20.2.5, Umbrella
  21.2.0, all with `client_cache_size` 100.
- Guidance: a test that lists a snapshot through libcephfs should keep the
  default cache size, or read the whole listing before it does anything else
  on the mount.
- Evidence: [`cephfs_snapshot_readdir_eviction`](../../internal/integration/experiments/cephfs_snapshot_readdir_eviction/README.md).

### libcephfs snapshot listing after a snapdiff on the same mount

When two names of a directory have the same dentry hash and the second of
them changed between two snapshots, a libcephfs mount that has listed the
later snapshot and then reads a snapdiff of the directory (`opensnapdiff`)
lists that snapshot without the changed name afterwards. Nothing reports an
error, and a fresh mount lists both names. Directories whose names have
different hashes are listed in full.

- Affected: Squid 19.2.2 and 19.2.5, Tentacle 20.2.4 and 20.2.5, Umbrella
  21.2.0.
- Guidance: a test that lists a snapshot through libcephfs after a snapdiff
  should use a mount that did not read the snapdiff.
- Evidence: [`cephfs_readdir_after_snapdiff`](../../internal/integration/experiments/cephfs_readdir_after_snapdiff/README.md).

### libcephfs client diagnostics with concurrent requests

When two metadata requests are waiting, ordinary JSON decoding of the admin
socket's `mds_requests` reply retains only the final request.

- Affected: Squid 19.2.5.
- Not affected: Tentacle 20.2.4 reports both identities in an array.
- Guidance: on Squid, do not read the reply with a decoder that keeps only
  the last value of a repeated key.
- Evidence: [`cephfs_mds_requests_json`](../../internal/integration/experiments/cephfs_mds_requests_json/README.md).

### libcephfs statfs below a byte quota and a nearer file quota

At a directory that has only `ceph.quota.max_files`, below a directory with
`ceph.quota.max_bytes`, statfs reports the capacity and the free space of the
data pool, not of the byte quota. Directories with a byte quota of their own,
or with no quota of their own below one, report the quota.

- Affected: Tentacle 20.2.4.
- Not affected: Squid 19.2.5, Tentacle 20.2.5 and Umbrella 21.2.0 report the
  parent's byte quota there too.
- Guidance: a test that reads capacity through statfs on 20.2.4 should read
  it at the directory that has the byte quota.
- Evidence: [`cephfs_statfs_mixed_quota`](../../internal/integration/experiments/cephfs_statfs_mixed_quota/README.md).

### CephFS mirror daemons joining

When another `cephfs-mirror` daemon joins, the MGR `mirroring` module fails
to redistribute directories that are already assigned (`'DirectoryState'
object is not subscriptable`). Failover to a surviving daemon still works.
Ceph's Tentacle documentation also marks deployments with several CephFS
mirror daemons as untested upstream.

- Affected: Tentacle 20.2.4, 20.2.5 and Umbrella 21.2.0.
- Guidance: call `cephfsmirror.Container.RebalanceDirectories` after
  `AddDaemon`; see [`cephfs/mirror`](../../cephfs/mirror/README.md).
- Evidence: [`cephfs_mirror_daemon_redistribution`](../../internal/integration/experiments/cephfs_mirror_daemon_redistribution/README.md).

### CephFS mirror directory changes and the MGR hang

The MGR's mirroring module can hang for good when a directory is removed
while another assignment is changing. External directory changes, and mirror
daemons joining or leaving, can trigger it.

- Affected: Squid and Tentacle; the patches that were run are not recorded,
  and Umbrella was not run.
- Guidance: `cephfsmirror.Container.RebalanceDirectories` waits for an idle
  policy, plain removals do not, and `ceph mgr fail` recovers; see
  [`cephfs/mirror`](../../cephfs/mirror/README.md).

### CephFS mirror of a file whose object stayed empty

An incremental `cephfs-mirror` synchronization asks the source MDS for the
changed blocks of each changed file. When a data object of the file is empty
in the previous snapshot and in the new one, as after a file was truncated
and extended without data before both snapshots, the MDS aborts with
`FAILED ceph_assert(len > 0)` in `MDCache::aggregate_snap_sets`. Its container
exits, the directory stays at `syncing`, and `WaitSnapshotSynced` only times
out. An object that is empty after a version with data is synchronized
correctly.

- Affected: Squid 19.2.5.
- Not affected: Tentacle 20.2.4 and Umbrella 21.2.0 answer the request and
  synchronize the snapshot.
- Guidance: check the MDS container when a snapshot does not arrive on Squid.
- Evidence: [`cephfs_mirror_blockdiff_empty_versions`](../../internal/integration/experiments/cephfs_mirror_blockdiff_empty_versions/README.md).

### CephFS removal and the other filesystems' MDSs

`ceph fs rm` clears the `join_fscid` of every daemon of every remaining
filesystem, not only of the daemons that joined the removed one. Each
daemon's next beacon, which names its `mds_join_fs`, sets it back a few
seconds later. In that interval the remaining filesystems' standby and
standby-replay daemons are not bound to them, and when a standby-replay
daemon's beacon arrives before its active's, the monitor fails the active
over to it for its better affinity. `cephfs.Filesystem.Remove` returns only
after every owned daemon of the other owned filesystems carries its affinity
again, so `MDSStatus` and `WaitReady` count them at once; the failover itself
cannot be prevented.

- Affected: Squid 19.2.5 and 19.2.6.
- Not affected: Squid 19.2.7, Tentacle 20.2.4 and Umbrella 21.2.0 keep the
  affinity.
- Guidance: do not assert which daemon is active right after removing another
  filesystem on Squid.
- Evidence: native research in
  [#506](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/506);
  `TestCephFSRemovalKeepsSurvivorAffinity` covers the fixture's wait.

### Snapshot schedules of a removed filesystem

The MGR `snap_schedule` module keeps the schedules of a removed filesystem.
Their timers keep running and log a `FS '<name>' not found` traceback every
period, and a filesystem created later under the same name inherits the
schedules, with their counts, and receives scheduled snapshots.

- Affected: Squid 19.2.5 and 19.2.6.
- Not affected: Squid 19.2.7, Tentacle 20.2.4 and Umbrella 21.2.0 drop them.
- Guidance: restore every `snapschedule.Lease`, and remove the schedules the
  SUT added, before removing a filesystem.
- Evidence: native research in
  [#507](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/507); no
  case manual.

### Session RESET after MDS journal replay

An MDS that recreates a client session from its journal, because the session
map object in the metadata pool is older than the journal, loses the
session's Cephx entity name. A later `ceph_start_reclaim` with
`CEPH_RECLAIM_RESET` for that session's UUID by the same principal fails with
`EPERM`, although the MDS still evicts the target session. A session that its
live client reconnected after the replay keeps its name.

- Affected: Squid 19.2.5 and 19.2.6, Tentacle 20.2.4.
- Not affected: Squid 19.2.7, Tentacle 20.2.5 and Umbrella 21.2.0 keep the
  name and accept the RESET.
- Guidance: a test of session reclaim after an MDS restart must expect
  `EPERM` on the affected releases.
- Evidence: native research in
  [#513](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/513); no
  case manual.

### Unbuffered nonblocking writes when a data pool becomes full

A libcephfs client with `client_oc=false` that has
`ceph_ll_nonblocking_readv_writev` writes in flight when the pool turns full
deadlocks: no write completes, later calls on the same client hang, also
after the pool has space again, while other clients keep working. Buffered
writes fail with `ENOSPC` instead.

- Affected: Squid 19.2.5 and 19.2.6.
- Not affected: Squid 19.2.7 and Tentacle 20.2.4 complete such writes with
  `-ENOSPC` and recover when the pool has space again.
- Guidance: do not let a data pool fill under such a client on the affected
  releases; its later calls do not return.
- Evidence: native research in
  [#514](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/514); no
  case manual.

### CephFS authorization before Squid 19.2.5

These builds are not tested by the repository, and their authorization is
weaker than the tested releases': MDS caps with one clause per filesystem
(`allow r fsname=a, allow rw fsname=b`) grant every clause on every
filesystem before 19.2.5; namespace-isolated subvolumes with the same name in
two groups share one RADOS namespace before 19.2.4, so each one's principal
reads the other's file data through RADOS; a path-restricted principal is
refused reads of a snapshot of a directory it deleted before 19.2.3.

- Affected: Squid before 19.2.5, as listed per defect above.
- Guidance: use 19.2.5 or later.
- Evidence: [`cephfs_authorization_isolation`](../../internal/integration/experiments/cephfs_authorization_isolation/README.md)
  checks all three on a chosen image.

## RGW

The module itself sends no S3 requests; these entries concern the requests
your SUT or test client sends.

### User without a rate-limit policy

For a user that stores no policy, `radosgw-admin ratelimit get
--ratelimit-scope user` exits 254 with `ENOENT`. Tentacle and Squid print the
native default instead (every budget zero, disabled). Both kinds of release
report a missing user the same way as such a user. Nothing establishes the
`ENOENT` as intended: the release documents only its new `max_list_ops` and
`max_delete_ops` budgets and claims backward compatibility, and the bucket
scope and the Admin REST `ratelimit info` for the same user still report the
default. It is treated as an unintended issue, not a release difference that
later releases are expected to keep. `radosgw-admin metadata get user:<uid>`
tells the cases apart: an existing user without a policy lists no
`user.rgw.ratelimit` attribute.

`rgw.User.OverrideRateLimit` accepts the `ENOENT` as an unset policy only when
that metadata, read in the same call, lists no policy attribute and the user
is still the identity it created. A missing or replaced user is an error. The
check never looks at the release, so a release that prints the default again
is unaffected. `radosgw-admin` cannot delete a stored policy, so `Restore`
stores the native default. The user is throttled as before the override, and
your own `ratelimit get` then reports that default instead of `ENOENT`.

- Affected: Umbrella 21.2.0.
- Not affected: Squid 19.2.5 and Tentacle 20.2.4.
- Guidance: tell an unset policy from a missing user with `metadata get`.
- Evidence: `TestRGWRateLimitOverride` (`internal/integration/rgw`).

### SigV4 headers left out of the signature

RGW refuses with 403 `AccessDenied` a request that sends `Content-Type` or
`x-amz-content-sha256` without listing it in `SignedHeaders`, with header and
with query (presigned URL) authentication alike; its level-5 log names the
header (`Signature rejected: 'content-type' supplied but not in
CanonicalHeaders`). Clients whose signer leaves `Content-Type` unsigned, and
presigned URLs used with a `Content-Type` header, therefore fail on this
release only. All three releases accept signed requests. Every other unsigned
`x-amz-*` header is refused on Tentacle and Umbrella and accepted on Squid.

- Affected: Tentacle 20.2.4.
- Not affected: Umbrella 21.2.0 accepts both headers unsigned, and Squid
  19.2.5 checks no header at all.
- Guidance: sign `Content-Type` and `x-amz-content-sha256` when the client
  sends them.
- Evidence: native research in
  [#498](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/498); no
  case manual. A check of the same header stops RGW multisite on Squid 19.2.6
  ([below](#a-secondary-zone-cannot-pull-the-realm-on-squid-1926)).

### Conditional writes on a versioning-suspended bucket

For a key written while versioning was enabled, `If-Match` with the current
ETag (unquoted on Squid, see the next entry) answers 412, and after a delete
marker `If-None-Match: *` answers 412 although no current version exists and,
on Tentacle, `If-Match` answers 412 instead of 404. On Tentacle this holds for
an ordinary PUT and for `CompleteMultipartUpload` alike. On all three releases
`If-None-Match: *` on an existing object and a wrong `If-Match` answer 412,
and keys first written after suspension behave correctly.

- Affected: Squid 19.2.5 and Tentacle 20.2.4.
- Not affected: Tentacle 20.2.5 and Umbrella 21.2.0 follow AWS semantics for
  both.
- Guidance: do not rely on conditional writes for keys written before
  versioning was suspended.
- Evidence: [`rgw_suspended_conditional_writes`](../../internal/integration/experiments/rgw_suspended_conditional_writes/README.md).

### Conditional writes on any bucket on Squid

`If-Match` is compared with the stored ETag without removing the quotes, so
the quoted ETag that S3 returns never matches. With an unquoted ETag,
`If-Match` on a missing key answers 412 instead of 404.
`CompleteMultipartUpload` ignores `If-Match` and `If-None-Match`: a second
create-once completion of the same key answers 200.

- Affected: Squid 19.2.5.
- Not affected: Tentacle 20.2.4 and Umbrella 21.2.0 accept quoted ETags and
  evaluate both headers on the completion.
- Guidance: send the ETag unquoted, and do not rely on conditional
  completions on Squid.
- Evidence: native research in
  [#499](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/499); no
  case manual.

### SSE-C multipart completion without the key

`CompleteMultipartUpload` of an SSE-C upload without an additional checksum
answers 400 `InvalidArgument` unless it repeats the customer-key headers; S3
does not require them there. An upload with the `SHA256` checksum algorithm
needs the key on every release, and a wrong key is refused.

- Affected: Tentacle 20.2.4.
- Not affected: Tentacle 20.2.5 and Umbrella 21.2.0 complete without the key.
  Squid 19.2.5 completes without it too, but also accepts a wrong key there
  and echoes no part checksum.
- Guidance: repeat the customer-key headers on the completion.
- Evidence: [`rgw_ssec_multipart_completion`](../../internal/integration/experiments/rgw_ssec_multipart_completion/README.md).

### Multipart completion replay

Replaying the same signed completion returned HTTP 200 and left the object
intact. The response XML lost the ETag on replay on 19.2.5 and 20.2.2, while
20.2.4 retained it. Neither completion returned an HTTP ETag header on these
releases.

- Affected: Squid 19.2.5 and Tentacle 20.2.2.
- Not affected: Tentacle 20.2.4 kept the ETag in the replayed XML.
- Guidance: do not read the object's ETag from a replayed completion.
- Evidence: [`rgw_multipart_completion_replay_etag`](../../internal/integration/experiments/rgw_multipart_completion_replay_etag/README.md).

### Replication policy readback after upgrading a Squid writer

When the stored destination of a policy that 19.2.5 wrote already contained
the full ARN, a 20.2.2 reader returned the S3 ARN prefix twice. Each reader
returned the correct destination for a policy it wrote itself.

- Affected: a Tentacle 20.2.2 reader of a 19.2.5 policy.
- Not affected: a Tentacle 20.2.4 reader returned the prefix once.
- Guidance: a test that reads such a policy on 20.2.2 must expect the
  doubled prefix.
- Evidence: [`rgw_legacy_replication_destination`](../../internal/integration/experiments/rgw_legacy_replication_destination/README.md).

### Empty `x-amz-server-side-encryption`

A PUT whose header is present with an empty value is treated as if the header
were absent: it answers 200 and writes an unencrypted object (a new version
over an existing key).

- Affected: Tentacle 20.2.4.
- Not affected: Tentacle 20.2.5 and Umbrella 21.2.0 refuse it with 400
  `InvalidArgument`, like an invalid algorithm. Squid 19.2.5 refuses an
  invalid algorithm with 400 `InvalidRequest`.
- Guidance: a client must omit the header when it requests no encryption.
- Evidence: [`rgw_empty_sse_header`](../../internal/integration/experiments/rgw_empty_sse_header/README.md).

### `x-amz-expiration` on a versioned bucket

GET and HEAD report the `NoncurrentVersionExpiration` days of the matching
rule, for a plain request as well as for an explicit `versionId`, instead of
the current-version `Expiration` (or no header).

- Affected: Squid 19.2.5 and Tentacle 20.2.4.
- Not affected: Tentacle 20.2.5 and Umbrella 21.2.0 report only `Expiration`,
  and nothing for an explicit version.
- Guidance: do not derive an object's expiry from the header on the affected
  releases.
- Evidence: [`rgw_lifecycle_expiration_header`](../../internal/integration/experiments/rgw_lifecycle_expiration_header/README.md).

### Copies of compressed objects to an uncompressed storage class

A copy that decompresses the data (for example from a zlib `STANDARD` class to
an uncompressed one) is accounted with the compressed length: the bucket
index `accounted_size`, the listed `Size` and the bucket statistics' `size`
are too small, while GET returns every byte.

- Affected: Tentacle 20.2.4.
- Not affected: Tentacle 20.2.5 and Umbrella 21.2.0 account the logical
  length; Squid 19.2.5 keeps the copy compressed.
- Guidance: do not compare listed sizes of such copies with the payload on
  20.2.4.
- Evidence: [`rgw_compressed_copy_accounting`](../../internal/integration/experiments/rgw_compressed_copy_accounting/README.md).

### Part reads of a multipart object in a non-default storage class

When a multipart upload is made in a storage class whose data pool is not the
default one, `GET ?partNumber=N` and `HEAD ?partNumber=N` answer
`400 InvalidPart` for every valid part, while a whole-object GET returns every
byte; a `STANDARD` object answers each part.

- Affected: Squid 19.2.5, Tentacle 20.2.4 and 20.2.5, Umbrella 21.2.0.
- Guidance: read such objects whole or by range, not by part number.
- Evidence: [`rgw_multipart_part_storage_class`](../../internal/integration/experiments/rgw_multipart_part_storage_class/README.md).

### SSE-C multipart objects after a lifecycle transition

After a lifecycle rule moves an SSE-C multipart object to another storage
class, a GET with the customer key answers 200 (206 for a range) with the
right length but wrong plaintext from the end of the first part on. Reads
before the transition, unencrypted objects and encrypted objects that were
not moved are correct.

- Affected: Squid 19.2.6 and Tentacle 20.2.1.
- Not affected: Squid 19.2.7 and Tentacle 20.2.2 and 20.2.4.
- Guidance: do not transition SSE-C multipart objects in a test on an
  affected release, or expect their reads to differ from the payload.
- Evidence: [`rgw_encrypted_multipart_transition`](../../internal/integration/experiments/rgw_encrypted_multipart_transition/README.md).

### Received bytes of a browser form upload in the usage log

A successful form upload (`POST` to a bucket with a signed policy) is counted
in the usage log as one successful `post_obj` with `bytes_received` 0, while
a `PUT` of the same payload is counted with the payload.

- Affected: Squid 19.2.5 and Tentacle 20.2.4.
- Not affected: Tentacle 20.2.5 and Umbrella 21.2.0 count what the gateway
  read of the form body, the payload and the form fields around it.
- Guidance: a test that checks a service's metering against the usage log
  must not expect the received bytes of form uploads on the affected releases.
- Evidence: [`rgw_post_usage_bytes_received`](../../internal/integration/experiments/rgw_post_usage_bytes_received/README.md).

### HEAD of a Swift static large object

A plain HEAD of an SLO, without a range, makes the gateway read the whole
payload of every segment from RADOS and discard it: the OSD returns as many
bytes as for a full GET of the object. A HEAD of a dynamic large object, of an
S3 single PUT and of an S3 multipart upload reads only the attributes of the
head object.

- Affected: Squid 19.2.5, Tentacle 20.2.4 and 20.2.5, Umbrella 21.2.0.
- Guidance: a test that counts OSD reads or bytes around a HEAD must not use
  an SLO as its large object.
- Evidence: [`rgw_slo_head_segment_reads`](../../internal/integration/experiments/rgw_slo_head_segment_reads/README.md).

### `radosgw-admin role list` beyond 100 roles

With more than 100 roles the command never ends: it repeats the same roles
and its output grows without bound (more than 13 GB in 60 seconds with 105
roles). 99 roles list correctly. The fixture never lists roles.

- Affected: Tentacle 20.2.4.
- Not affected: Squid 19.2.5 and Umbrella 21.2.0 list 105.
- Guidance: read a known role with `role get` instead.
- Evidence: native research in
  [#500](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/500); no
  case manual.

### SSE-S3 key left in Vault

Deleting a bucket with SSE-S3 default encryption marks its Vault Transit key
deletable and stops there: the gateway logs `Unable to remove KEK ID ... got
-22` and sends no deletion, and the bucket DELETE still answers 204. Squid
accepts only an empty response to the key's config update, and Vault 1.21
answers it with the key's description.

- Affected: Squid 19.2.5.
- Not affected: Tentacle 20.2.4 and Umbrella 21.2.0 delete the key.
- Guidance: remove the key yourself.
- Evidence: [`rgw_kms_failure_modes`](../../internal/integration/experiments/rgw_kms_failure_modes/README.md)
  reads the outcome from Vault's audit log on every release.

## RGW multisite

### A secondary zone cannot pull the realm on Squid 19.2.6

On a secondary zone, `radosgw-admin realm pull` is rejected by the master
gateway with HTTP 403. The RGW debug log gives the reason
`'x-amz-content-sha256' supplied, but not in CanonicalHeaders`: the release's
`radosgw-admin` sends that header without signing it, and its RGW rejects
such requests. No setting disables the check, so `multisite.Run` cannot start
a secondary zone.

- Affected: Squid 19.2.6.
- Not affected: Squid 19.2.5, the Squid release CI runs.
- Guidance: use 19.2.5 for multisite tests on Squid.
- Evidence: `TestMultiClusterRGWMultisite` (`internal/integration/rgw`), which
  Periodic `discovery` runs on 19.2.5, 19.2.6 and 19.2.7 with the 19.2.6
  failure kept as a control (see
  [Periodic runs](../contributing/ci.md#selection-evidence-and-failure-routing));
  no case manual.

### Data sync after a source that did not answer for longer than the sync lease period

While 100 of a gateway's reads of one source zone's data log wait for an
answer, the gateway renews none of its sync leases. When the source answers
after more than `rgw_sync_lease_period` (30 seconds for fixture gateways
unless
[`TimingProfile.SyncLeasePeriod`](../../rgw/multisite/README.md#sync-lease-period)
selects another, 120 by default), a shard that finds its lease lost inside the
batch it read moves its position past the batch without syncing it: the
objects stay missing in the destination, `sync status` keeps the shard behind
and the error list stays empty until the destination's gateway is restarted
or a later change of the same bucket shard arrives.

- Affected: Squid 19.2.5, Tentacle 20.2.4 and Umbrella 21.2.0.
- Guidance: select a sync lease period longer than twice the outage your test
  makes (see the `rgw/multisite` README).
- Evidence: [`rgw_sync_lease_lapse`](../../internal/integration/experiments/rgw_sync_lease_lapse/README.md).

### A bucket's object lock rule in other zones

A default retention rule applied to a bucket with `PUT ?object-lock` stays in
the zone it was applied in: after metadata sync the other zones answer
`GET ?object-lock` with object lock enabled and no rule, and the bucket record
they store holds none.

- Affected: Squid 19.2.5 and Tentacle 20.2.4.
- Not affected: Tentacle 20.2.5 and Umbrella 21.2.0 answer the same rule in
  every zone, also after a change of it.
- Guidance: a test that relies on a default retention outside the zone it
  wrote to must read the configuration from that zone first.
- Evidence: [`rgw_multisite_object_lock_sync`](../../internal/integration/experiments/rgw_multisite_object_lock_sync/README.md).

### The redirect for a bucket of another zonegroup

A gateway that is asked for a bucket held by another zonegroup of its realm
answers `301` with a `Location` built from its own zonegroup's endpoint, so
the redirect names the gateway that was just asked.

- Affected: Squid 19.2.5 and Tentacle 20.2.4.
- Not affected: Tentacle 20.2.5 and Umbrella 21.2.0 build it from the
  endpoint of the zonegroup that holds the bucket.
- Guidance: a test whose client follows redirects across the zonegroups of an
  [`rgw/multisite`](../../rgw/multisite/README.md) realm does not reach the
  owning zonegroup on the affected releases; send the request to that
  zonegroup's gateway instead.
- Evidence: [`rgw_zonegroup_redirect_endpoint`](../../internal/integration/experiments/rgw_zonegroup_redirect_endpoint/README.md).
