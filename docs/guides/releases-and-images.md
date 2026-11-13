# Ceph releases and images

## Introduction

The module runs real Ceph daemons from a Ceph container image and configures
them at run time. This guide explains which image is used by default, which
Ceph releases are supported and how they differ, how to select a separate image
for each daemon role, and what an image must contain.

## Default image

`ceph.DefaultImage` is the published, pruned Tentacle dependency-role
aggregate, named by its release tag:

```text
ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all
```

The tag is a multi-architecture manifest for `linux/amd64` and
`linux/arm64`; Docker pulls the variant of the engine's native architecture.
[ceph-testcontainers-images](https://github.com/JSYoo5B/ceph-testcontainers-images#published-images)
publishes every image as `<variant>-<release>-<role>` and replaces a
published image under the same tag when it is rebuilt, for example after a
packaging correction or when a role gains a binary the module needs. The
default therefore follows compatible rebuilds without a new module version.

Which build a host runs is the caller's choice, as for any image reference:

- The module passes the reference to Docker unchanged. An engine that already
  has the tag keeps its local image until you pull the tag again
  (`docker pull`), so refreshing and caching follow your own pull policy.
- For one exact build, pass its digest reference
  (`ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:...`) or a local image
  ID to `Run` instead of the default. Tags, digests and local image IDs are
  all accepted, for `Run` and for every `ImageSet` role.
- The module neither resolves a tag to a digest, checks that an image is
  current, nor reports which build ran. Record `docker image inspect` output
  in your own test run when you need that evidence.

The module needs no image of its own. It supplies the FSID, keys,
configuration, daemon IDs, addresses, bootstrap commands and entrypoints when
it starts each container, so any image that meets the
[requirements](#role-images) can replace the default.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage)
```

## Supported releases

| Release | Status | Tested image |
| --- | --- | --- |
| Tentacle 20.2.x | Default; every feature on 20.2.4. 20.2.0 to 20.2.3 have no `aes256k` keys | Pruned official aggregate that `ceph.DefaultImage` names, and the `official-20.2.4-<role>` role tags |
| Umbrella 21.2.x | Periodic CI on 21.2.0, with the issues listed below; the default remains Tentacle | Published official role images, the `official-21.2.0-<role>` tags; official amd64 requires x86-64-v3 |
| Squid 19.2.x | Supported, with the differences listed below | `quay.io/ceph/ceph:v19.2.5@sha256:1bb011052bc6d347d3418adcbf7d88156860d45697bc6323594a11410084064b` |

Squid 19.2.6 behaves like 19.2.5 except that RGW multisite cannot start; see
[Known release-specific issues](#known-release-specific-issues). Tentacle
releases before 20.2.4 have no `aes256k` Cephx keys; see the same section. The
repository's continuous integration runs its test suites on all three releases; see
[CI](../contributing/ci.md). Native limits shared across releases, such as a
daemon stranded without a MON session after its ticket expired, are listed in
[Known release-specific issues](#known-release-specific-issues).

To run against Squid, pass a Squid image to `Run`:

```golang
cluster, err := ceph.Run(ctx,
	"quay.io/ceph/ceph:v19.2.5@sha256:1bb011052bc6d347d3418adcbf7d88156860d45697bc6323594a11410084064b")
```

## Detecting the release at run time

Right after the first MON starts, `Run` executes `ceph --version` in the
first MON's essential image and records the release. `FactsOf(cluster).CephVersion` returns it, for
example `"20.2.4"`. The fixture uses this value to choose commands that differ
between releases.

Where a tool reveals a feature, the fixture checks the tool instead of the
release number, so that patched and stripped images behave as they are: the
essential image's `ceph-authtool` decides Cephx key types, and the `rbd` of each
RBD mirror setup client and each cluster's client toolkit decides namespace
mapping. A feature that the relevant
target lacks fails before any change with `ceph.ErrUnsupported`, whose
`*ceph.OperationError` names the target, its version and the feature. Assert
that outcome instead of skipping on the version string; see
[Classified errors](errors.md#tests-across-releases).

- The version describes the image of the first MON and of the fixture's Ceph
  CLI. Daemons started from other role images report their own release.
- `Run` fails if `ceph --version` does not print
  `ceph version <major>.<minor>.<patch> ...`. The image requirements demand this
  output.
- Combine only images of the same release in one cluster. Mixing releases is
  not supported: the fixture chooses command forms from the essential image and the client toolkit
  alone. The one exception that has been run is the forward
  [rolling upgrade from Squid to Tentacle](#rolling-upgrade-from-squid-to-tentacle).

## Behavioral bug compatibility policy

Feature availability, release command/output differences and behavioral bugs
are separate compatibility questions. For future bug-specific paths, the
[bug compatibility policy](../contributing/bug-compatibility.md) requires explicit
bug-ID build selection rather than inferring a fix from a release or image-tag
suffix. Custom images can contain backports without changing the reported
release. No tag is
[registered](bug-compatibility.md#registered-tags) yet; this documentation does not add selectors or change
the existing runtime adaptations described below.

## Behaviour that differs on Squid

Most differences are handled inside the module and need no action. The table
lists every known difference and what you can observe.

| Area | Tentacle 20.2.x | Squid 19.2.x | Effect on your test |
| --- | --- | --- | --- |
| Cephx key types and MonMap ciphers | 20.2.4 supports `aes` and `aes256k`; earlier patches support only `aes`. | Only `aes`. | `WithAuth` and `CreateClient` check the target tools and return `ceph.ErrUnsupported` for an unavailable key type; no silent fallback. |
| CephFS filesystem creation | `fs new` sets `max_mds` and `standby_count_wanted` inline. | Squid does not accept the inline settings, so the fixture runs `fs set` right after `fs new`. | None. Both forms complete before any MDS starts. |
| RBD mirroring of named namespaces only | The default namespace is put in `init-only` mode. | Squid has no `init-only` mode, so the default namespace is put in `image` mode. | On Squid nothing in the default namespace replicates unless you enable an image explicitly. |
| RBD namespace mapping | `Config.SourceNamespace` and `DestinationNamespace` may differ. | Squid has no remote namespaces and its `rbd` no `--remote-namespace`, so only namespaces with the same name can be mirrored. | `rbdmirror.Run` returns `ceph.ErrUnsupported`, naming the side whose `rbd` lacks the option, before changing anything when the names differ. |
| RBD mirror UUID | `rbd mirror pool info` prints `mirror_uuid` and `remote_namespace`. | Neither field is printed. The fixture reads the UUID from the pool's `rbd_mirroring` object and uses the same namespace name as the remote namespace. | None for the typed API. Your own parsing of `rbd mirror pool info` must tolerate the missing fields. |
| RBD receiver status | The rbd-mirror admin socket's `rbd mirror status` reports `namespace_replayers` with local and remote namespace names. | The default namespace's `image_replayers` sit in the pool entry and named namespaces appear by name only under `namespaces`. | None. `rbdmirror.Container.ReceiverStatus` and `WaitReceiverReady` normalize both forms. |
| RGW sync pipe records | A system-mode pipe has no `user` in its `params`. | Squid records `"user": ""`. | None. Pipe readback accepts the empty value on Squid. |
| RGW account root as user-mode pipe principal | Objects replicate between buckets owned by the same account. | Squid resolves no source for such a pipe and replicates nothing. Ordinary-user user-mode pipes work. | `multisite.SyncGroup.CreatePipe` returns `ceph.ErrUnsupported`, naming the Squid zone, before changing anything when any zone runs Ceph 19. |
| RGW user-scope rate limit | A user's policy throttles every request that the user's S3 keys sign. | The gateway stores and reports the policy but never applies it to a request signed with the user's S3 key: the authenticated request user carries no stored user attributes, where the policy lives. A bucket's policy is applied. | `rgw.User.OverrideRateLimit` with `rgw.RateLimitScope{}` returns `ceph.ErrUnsupported`, naming the release, before changing anything when the cluster runs Ceph 19. The bucket scope works on both. |
| RGW bucket sync status | `radosgw-admin bucket sync status --format json` prints JSON. | Squid ignores `--format json` and prints text. | None for `BucketSyncStatus` and `WaitBucketSyncReady`, which parse the text into the same structure and fail on unknown lines. Your own calls get text. |

### Differences you may hit in your own native checks

These affect commands or client libraries you run yourself, not the typed API:

| Topic | Squid behaviour |
| --- | --- |
| Messenger encryption | There is no `messenger dump` admin command showing per-connection encryption. Check the daemons' `ms_*_mode` settings (for example with `ceph config show`) and, for your own librados or libcephfs clients, the `secure` mode on READY connections in `debug_ms=1` logs. |
| RGW STS roles | `radosgw-admin role get` for a missing role exits with code 2 and prints one ENOENT line. |
| Python `rbd` group API | `Group.id()` and `get_snap_info()` do not exist. Read the group ID from the `rbd_group_directory` object and member snapshots from each image's group snapshot namespace. `list_snaps()` of a group that does not exist returns an empty list; Tentacle 20.2.5 and Umbrella raise `ObjectNotFound`, and 20.2.4 crashes (see [Known release-specific issues](#known-release-specific-issues)). |
| `rbd mirror pool info` | No `mirror_uuid` or `remote_namespace` fields; no `--remote-namespace` option. |
| `radosgw-admin bucket sync status` | Text output only. |
| `ceph fs status --format json` | Up to 19.2.6, the `rank` of a standby-replay daemon is the integer rank it follows. 19.2.7, Tentacle and Umbrella print the string `<rank>-s`, as the plain output does. Classify daemons by `state`, or read `ceph fs dump`. |

## Known release-specific issues

- **Umbrella 21.2.0, RGW user without a rate-limit policy.** For a user that
  stores no policy, `radosgw-admin ratelimit get --ratelimit-scope user` exits
  254 with `ENOENT`. Tentacle and Squid print the native default instead
  (every budget zero, disabled). Both kinds of release report a missing user
  the same way as such a user. Nothing establishes the `ENOENT` as intended:
  the release documents only its new `max_list_ops` and `max_delete_ops`
  budgets and claims backward compatibility, and the bucket scope and the
  Admin REST `ratelimit info` for the same user still report the default. It
  is treated as an unintended issue, not a release difference that later
  releases are expected to keep. `radosgw-admin metadata get user:<uid>`
  tells the cases apart: an existing user without a policy lists no
  `user.rgw.ratelimit` attribute. `rgw.User.OverrideRateLimit` accepts the
  `ENOENT` as an unset policy only when that metadata, read in the same call,
  lists no policy attribute and the user is still the identity it created. A
  missing or replaced user is an error. The check never looks at the release,
  so a release that prints the default again is unaffected. `radosgw-admin`
  cannot delete a stored policy, so `Restore` stores the native default. The
  user is throttled as before the override, and your own `ratelimit get`
  then reports that default instead of `ENOENT`.

- **Tentacle 20.2.4, SigV4 headers left out of the signature.** RGW refuses
  with 403 `AccessDenied` a request that sends `Content-Type` or
  `x-amz-content-sha256` without listing it in `SignedHeaders`, with header
  and with query (presigned URL) authentication alike; its level-5 log names
  the header (`Signature rejected: 'content-type' supplied but not in
  CanonicalHeaders`). Clients whose signer leaves `Content-Type` unsigned,
  and presigned URLs used with a `Content-Type` header, therefore fail on
  this release only. Umbrella 21.2.0 accepts both headers unsigned, Squid
  19.2.5 checks no header at all, and all three accept signed requests. Every
  other unsigned `x-amz-*` header is refused on Tentacle and Umbrella and
  accepted on Squid. The module itself sends no S3 requests.
- **Squid 19.2.5 and Tentacle 20.2.4, conditional writes on a
  versioning-suspended bucket.** For a key written while versioning was
  enabled, `If-Match` with the current ETag (unquoted on Squid, see the next
  entry) answers 412, and after a delete marker `If-None-Match: *` answers 412
  although no current version exists and, on Tentacle, `If-Match` answers 412
  instead of 404. On Tentacle this holds for an ordinary PUT and for
  `CompleteMultipartUpload` alike; Umbrella 21.2.0 follows AWS semantics for
  both. On all three releases `If-None-Match: *` on an existing object and a
  wrong `If-Match` answer 412, and keys first written after suspension behave
  correctly. See `TestRGWSuspendedConditionalWrites` (optional
  `native_regression` batch `rgw_conditional_writes`, experiment case
  [`rgw_suspended_conditional_writes`](../../internal/integration/experiments/rgw_suspended_conditional_writes/README.md)).
- **Squid 19.2.5, conditional writes on any bucket.** `If-Match` is compared
  with the stored ETag without removing the quotes, so the quoted ETag that
  S3 returns never matches; send it unquoted. With an unquoted ETag, `If-Match`
  on a missing key answers 412 instead of 404. `CompleteMultipartUpload`
  ignores `If-Match` and `If-None-Match`: a second create-once completion of
  the same key answers 200. Tentacle 20.2.4 and Umbrella 21.2.0 accept
  quoted ETags and evaluate both headers on the completion.
- **Tentacle 20.2.4, SSE-C multipart completion without the key.**
  `CompleteMultipartUpload` of an SSE-C upload without an additional
  checksum answers 400 `InvalidArgument` unless it repeats the
  customer-key headers; S3 does not require them there. An upload with the
  `SHA256` checksum algorithm needs the key on every release, and a wrong key
  is refused. Tentacle 20.2.5 and Umbrella 21.2.0 complete without the key.
  Squid 19.2.5 completes without it too, but also accepts a wrong key there
  and echoes no part checksum. See `TestRGWSSECMultipartCompletion` (optional
  `native_regression` batch `rgw_ssec_completion`, experiment case
  [`rgw_ssec_multipart_completion`](../../internal/integration/experiments/rgw_ssec_multipart_completion/README.md)).
- **Tentacle 20.2.4, empty `x-amz-server-side-encryption`.** A PUT whose
  header is present with an empty value is treated as if the header were
  absent: it answers 200 and writes an unencrypted object (a new version over
  an existing key). Tentacle 20.2.5 and Umbrella 21.2.0 refuse it with 400
  `InvalidArgument`, like an invalid algorithm. Squid 19.2.5 refuses an
  invalid algorithm with 400 `InvalidRequest`. See `TestRGWEmptySSEHeader`
  (batch `rgw_empty_sse_header`, experiment case
  [`rgw_empty_sse_header`](../../internal/integration/experiments/rgw_empty_sse_header/README.md)).
- **Squid 19.2.5 and Tentacle 20.2.4, `x-amz-expiration` on a versioned
  bucket.** GET and HEAD report the `NoncurrentVersionExpiration` days of the
  matching rule, for a plain request as well as for an explicit `versionId`,
  instead of the current-version `Expiration` (or no header). Tentacle 20.2.5
  and Umbrella 21.2.0 report only `Expiration`, and nothing for an explicit
  version. See `TestRGWLifecycleExpirationHeader` (batch
  `rgw_expiration_header`, experiment case
  [`rgw_lifecycle_expiration_header`](../../internal/integration/experiments/rgw_lifecycle_expiration_header/README.md)).
- **Tentacle 20.2.4, copies of compressed objects to an uncompressed storage
  class.** A copy that decompresses the data (for example from a zlib
  `STANDARD` class to an uncompressed one) is accounted with the compressed
  length: the bucket index `accounted_size`, the listed `Size` and the
  bucket statistics' `size` are too small, while GET returns every byte.
  Tentacle 20.2.5 and Umbrella 21.2.0 account the logical length; Squid
  19.2.5 keeps the copy compressed. See `TestRGWCompressedCopyAccounting`
  (batch `rgw_compressed_copy`, experiment case
  [`rgw_compressed_copy_accounting`](../../internal/integration/experiments/rgw_compressed_copy_accounting/README.md)).
- **Every release, part reads of a multipart object in a non-default storage
  class.** When a multipart upload is made in a storage class whose data pool
  is not the default one, `GET ?partNumber=N` and `HEAD ?partNumber=N`
  answer `400 InvalidPart` for every valid part, while a whole-object GET
  returns every byte; a `STANDARD` object answers each part. Squid 19.2.5,
  Tentacle 20.2.4 and 20.2.5 and Umbrella 21.2.0 do this. See
  `TestRGWMultipartPartStorageClass` (batch `rgw_multipart_part_class`,
  experiment case
  [`rgw_multipart_part_storage_class`](../../internal/integration/experiments/rgw_multipart_part_storage_class/README.md)).
- **Every release, multisite data sync after a source that did not answer
  for longer than the sync lease period.** While 100 of a gateway's reads
  of one source zone's data log wait for an answer, the gateway renews none
  of its sync leases. When the source answers after more than
  `rgw_sync_lease_period` (30 seconds for fixture gateways unless
  [`TimingProfile.SyncLeasePeriod`](../../rgw/multisite/README.md#sync-lease-period)
  selects another, 120 by default),
  a shard that finds its lease lost inside the batch it read moves its
  position past the batch without syncing it: the objects stay missing in
  the destination, `sync status` keeps the shard behind and the error list
  stays empty until the destination's gateway is restarted or a later change
  of the same bucket shard arrives. Squid 19.2.5, Tentacle 20.2.4 and
  Umbrella 21.2.0 do this. See
  `TestMultiClusterRGWDataSyncAfterSyncLeaseLapse` (batch
  `rgw_sync_lease_lapse`, experiment case
  [`rgw_sync_lease_lapse`](../../internal/integration/experiments/rgw_sync_lease_lapse/README.md)).
- **Squid 19.2.5 and Tentacle 20.2.4, a bucket's object lock rule in other
  zones.** A default retention rule applied to a bucket with
  `PUT ?object-lock` stays in the zone it was applied in: after metadata
  sync the other zones answer `GET ?object-lock` with object lock enabled
  and no rule, and the bucket record they store holds none. Tentacle 20.2.5
  and Umbrella 21.2.0 answer the same rule in every zone, also after a
  change of it. A test that relies on a default retention outside the zone
  it wrote to must read the configuration from that zone first. See
  `TestMultiClusterRGWBucketObjectLockSync` (batch `rgw_object_lock_sync`,
  experiment case
  [`rgw_multisite_object_lock_sync`](../../internal/integration/experiments/rgw_multisite_object_lock_sync/README.md)).
- **Tentacle 20.2.4, `radosgw-admin role list` beyond 100 roles.** With more
  than 100 roles the command never ends: it repeats the same roles and its
  output grows without bound (more than 13 GB in 60 seconds with 105 roles).
  99 roles list correctly, and Squid 19.2.5 and Umbrella 21.2.0 list 105.
  Read a known role with `role get` instead; the fixture never lists roles.
- **Every release, Python `RBD().trash_purge` with a cutoff.** The binding
  reads the calendar fields of `expire_ts` as local time of the client
  process, so a timezone-aware UTC cutoff lands at the instant minus the
  client's UTC offset. Nine hours ahead of UTC an image whose deferment
  ended four hours before the cutoff stays in the trash; nine hours behind,
  an image whose deferment ends five hours after it is removed. Squid
  19.2.5, Tentacle 20.2.4 and Umbrella 21.2.0 do this. Run such a purge with
  `TZ=UTC`, or pass a naive datetime of the client's local time. See
  `TestRBDTrashPurgeCutoffAcrossClientTimezones` (batch
  `rbd_trash_purge_timezone`, experiment case
  [`rbd_trash_purge_timezone`](../../internal/integration/experiments/rbd_trash_purge_timezone/README.md)).
- **Tentacle 20.2.4 and Umbrella 21.2.0, RBD group snapshot listed again
  after its removal.** The class method that stores a group snapshot's
  record accepts a COMPLETE update for a snapshot that was already removed
  and writes the record again without its order entry. `rbd group snap ls`
  then succeeds and names the removed snapshot as `complete`, although its
  image snapshots are gone. The ordinary commands send such an update only
  when a creation races with the removal; the test replays the stored record
  through the `rados` Python binding. See
  `TestRBDGroupSnapshotLateCompleteAfterRemoval` (batch
  `rbd_group_snap_late_complete`, experiment case
  [`rbd_group_snap_late_complete`](../../internal/integration/experiments/rbd_group_snap_late_complete/README.md)).
- **Squid 19.2.5, SSE-S3 key left in Vault.** Deleting a bucket with
  SSE-S3 default encryption marks its Vault Transit key deletable and stops
  there: the gateway logs `Unable to remove KEK ID ... got -22` and sends no
  deletion, and the bucket DELETE still answers 204. Squid accepts only an
  empty response to the key's config update, and Vault 1.21 answers it with
  the key's description. Tentacle 20.2.4 and Umbrella 21.2.0 delete the key.
  Remove the key yourself; `TestRGWKMSFailureModes` (experiment case
  [`rgw_kms_failure_modes`](../../internal/integration/experiments/rgw_kms_failure_modes/README.md))
  reads the outcome from Vault's audit log on every release.
- **Squid 19.2.6, RGW multisite.** On a secondary zone, `radosgw-admin realm
  pull` is rejected by the master gateway with HTTP 403. The RGW debug log gives
  the reason `'x-amz-content-sha256' supplied, but not in CanonicalHeaders`: the
  release's `radosgw-admin` sends that header without signing it, and its RGW
  rejects such requests. No setting disables the check. `multisite.Run`
  therefore cannot start on 19.2.6. Use 19.2.5 for
  multisite tests on Squid; everything else behaves on 19.2.6 as on 19.2.5.
- **Tentacle 20.2.4, Python `rbd` snapshot list of a missing group.**
  `rbd.Group(ioctx, name).list_snaps()` for a group that does not exist
  crashes the Python process with `SIGSEGV` instead of raising
  `ObjectNotFound`: when the native call fails, the binding's iterator is
  freed with its original entry count and frees entries that the call never
  filled, so no `except` clause runs.
  It crashed in every attempt with the published 20.2.4 aggregate. Tentacle
  20.2.5 and Umbrella 21.2.0 raise the exception and continue. Check that the
  group exists (`rbd group ls`) before listing its snapshots from Python on
  20.2.4, or run the listing in a child process. The module does not list the
  snapshots of a missing group.
- **Tentacle before 20.2.4, Cephx key types.** `ceph-authtool` has no
  `--key-type` and `monmaptool` no cipher options in 20.2.0 to 20.2.3, so these
  releases generate only `aes` keys, as Squid does. `ceph.WithAuth` and
  `CreateClient` detect the options in the essential image: they accept `aes`
  and refuse `aes256k` before any change with `ceph.ErrUnsupported`, naming
  `ceph-authtool --key-type` as the capability. The repository tests this
  with 20.2.3.
- **Squid 19.2.3 and later, libcephfs creation at a dangling symlink.**
  `mkdir`, `symlink`, `link` and `mknod` through libcephfs follow a symlink
  at the new name when its target does not exist: they succeed and make the
  entry at the symlink's target, where POSIX requires `EEXIST` with nothing
  made. A symlink whose target exists and any other occupied name report
  `EEXIST`. Squid 19.2.3 and 19.2.5, Tentacle 20.2.4 and 20.2.5 and Umbrella
  21.2.0 all do this; Squid 19.2.2 reports `EEXIST`. A test that relies on `EEXIST` to detect an occupied
  name through libcephfs should check the name with `lstat` first, and a
  test that plants a symlink as an obstacle should point it at an existing
  entry. See `TestCephFSCreateAtADanglingSymlink` (batch
  `cephfs_dangling_symlink_create`, experiment case
  [`cephfs_dangling_symlink_create`](../../internal/integration/experiments/cephfs_dangling_symlink_create/README.md)).
- **Every release that was run, libcephfs snapshot listing with a small
  client cache.** A libcephfs directory handle of a snapshot
  (`dir/.snap/name`) with more entries than one readdir reply returns
  entries of the live directory, and loses most of the snapshot's, once the
  client has trimmed its dentry of the snapshot: the cache holds more than
  `client_cache_size` dentries and a client tick (one second) passes
  between two requests of the handle. Nothing reports an error. Squid
  19.2.2 and 19.2.5, Tentacle 20.2.4 and 20.2.5 and Umbrella 21.2.0 all do
  this with `client_cache_size` 100; with the default of 16384 and fewer
  cached entries than that the listing is exact. A test that lists a
  snapshot through libcephfs should keep the default cache size, or read
  the whole listing before it does anything else on the mount. See
  `TestCephFSSnapshotListingAcrossCacheEviction` (batch
  `cephfs_snapshot_readdir_eviction`, experiment case
  [`cephfs_snapshot_readdir_eviction`](../../internal/integration/experiments/cephfs_snapshot_readdir_eviction/README.md)).
- **Every release that was run, libcephfs snapshot listing after a snapdiff
  on the same mount.** When two names of a directory have the same dentry
  hash and the second of them changed between two snapshots, a libcephfs
  mount that has listed the later snapshot and then reads a snapdiff of the
  directory (`opensnapdiff`) lists that snapshot without the changed name
  afterwards. Nothing reports an error, and a fresh mount lists both names.
  Squid 19.2.2 and 19.2.5, Tentacle 20.2.4 and 20.2.5 and Umbrella 21.2.0
  all do this; directories whose names have different hashes are listed in
  full. A test that lists a snapshot through libcephfs after a snapdiff
  should use a mount that did not read the snapdiff. See
  `TestCephFSListingAfterSnapdiff` (batch `cephfs_readdir_after_snapdiff`,
  experiment case
  [`cephfs_readdir_after_snapdiff`](../../internal/integration/experiments/cephfs_readdir_after_snapdiff/README.md)).
- **Tentacle 20.2.4, libcephfs statfs below a byte quota and a nearer file
  quota.** At a directory that has only `ceph.quota.max_files`, below a
  directory with `ceph.quota.max_bytes`, statfs reports the capacity and the
  free space of the data pool, not of the byte quota. Directories with a
  byte quota of their own, or with no quota of their own below one, report
  the quota. Squid 19.2.5, Tentacle 20.2.5 and Umbrella 21.2.0 report the
  parent's byte quota there too. A test that reads capacity through statfs
  on 20.2.4 should read it at the directory that has the byte quota. See
  `TestCephFSStatfsWithNestedByteAndFileQuotas` (batch
  `cephfs_statfs_mixed_quota`, experiment case
  [`cephfs_statfs_mixed_quota`](../../internal/integration/experiments/cephfs_statfs_mixed_quota/README.md)).
- **Tentacle 20.2.4, CephFS mirror daemons.** When another `cephfs-mirror`
  daemon joins, the MGR `mirroring` module fails to redistribute directories
  that are already assigned (`'DirectoryState' object is not subscriptable`).
  Failover to a surviving daemon still works. Call
  `cephfsmirror.Container.RebalanceDirectories` after `AddDaemon`. Ceph's Tentacle
  documentation also marks deployments with several CephFS mirror daemons as
  untested upstream. See [`cephfs/mirror`](../../cephfs/mirror/README.md).
- **Squid 19.2.5, CephFS mirror of a file whose object stayed empty.** An
  incremental `cephfs-mirror` synchronization asks the source MDS for the
  changed blocks of each changed file. When a data object of the file is
  empty in the previous snapshot and in the new one, as after a file was
  truncated and extended without data before both snapshots, the MDS aborts
  with `FAILED ceph_assert(len > 0)` in `MDCache::aggregate_snap_sets`. Its
  container exits, the directory stays at `syncing`, and
  `WaitSnapshotSynced` only times out, so check the MDS container when a
  snapshot does not arrive on Squid. An object that is empty after a version
  with data is synchronized correctly. Tentacle 20.2.4 and Umbrella 21.2.0
  answer the request and synchronize the snapshot. See
  `TestCephFSMirrorBlockdiffAcrossEmptyObjectVersions` (optional
  `native_regression` batch `cephfs_mirror_blockdiff_empty`, experiment case
  [`cephfs_mirror_blockdiff_empty_versions`](../../internal/integration/experiments/cephfs_mirror_blockdiff_empty_versions/README.md)).
- **Every tested release, RBD snapshot copy resumed after a killed
  receiver.** When an `rbd-mirror` daemon is killed while it copies a mirror
  snapshot and the same daemon is started again, it resumes the copy and marks
  the snapshot complete, but the object numbered
  `last_copied_object_number` at the kill keeps the previous snapshot's bytes.
  It happened in every demotion run on 19.2.4, 19.2.5, 20.2.4 and 21.2.0
  (SIGKILL on all four, `Stop` without a grace period on 19.2.4 and 20.2.4),
  and for an ordinary mirror snapshot on 19.2.5 and 20.2.4; a graceful stop
  was not tried. Squid
  19.2.3 does not resume an interrupted demotion copy at all (Tentacle 20.2.0
  has the same code); the destination stays `up+unknown`
  ("remote image is not primary"). A test that kills a receiver during a
  snapshot sync must compare the destination's data afterwards rather than
  trust the completed snapshot; see
  `TestRBDMirrorDemotionResumesAfterReceiverRestart` (optional
  `native_regression` batch `rbd_demote_restart`, experiment case
  [`rbd_mirror_demote_restart`](../../internal/integration/experiments/rbd_mirror_demote_restart/README.md)).
- **Both releases, MON paused past the ticket lifetime.** A MON paused for
  longer than the MON ticket lifetime can strand a daemon whose MON session was
  on it: its reconnect is refused with `EACCES`, and it stops authenticating
  until it is restarted. With 12-second tickets and a 25-second pause this
  happened to the active MGR in two of four native runs. See
  [Daemon controls](messenger-client-fixtures.md#daemon-controls).
- **Squid 19.2.5 and 19.2.6, CephFS removal and the other filesystems' MDSs.**
  `ceph fs rm` clears the `join_fscid` of every daemon of every remaining
  filesystem, not only of the daemons that joined the removed one. Each
  daemon's next beacon, which names its `mds_join_fs`, sets it back a few
  seconds later. In that interval the remaining filesystems' standby and
  standby-replay daemons are not bound to them, and when a standby-replay
  daemon's beacon arrives before its active's, the monitor fails the active
  over to it for its better affinity. `cephfs.Filesystem.Remove` returns only
  after every owned daemon of the other owned filesystems carries its
  affinity again, so `MDSStatus` and `WaitReady` count them at once; the
  failover itself cannot be prevented. 19.2.7, Tentacle 20.2.4 and Umbrella
  21.2.0 keep the affinity.
- **Squid 19.2.5 and 19.2.6, snapshot schedules of a removed filesystem.** The
  MGR `snap_schedule` module keeps the schedules of a removed filesystem. Their
  timers keep running and log a `FS '<name>' not found` traceback every
  period, and a filesystem created later under the same name inherits the
  schedules, with their counts, and receives scheduled snapshots. Restore every
  `snapschedule.Lease`, and remove the schedules the SUT added, before removing
  a filesystem. 19.2.7, Tentacle 20.2.4 and Umbrella 21.2.0 drop them.
- **Squid 19.2.5 and 19.2.6 and Tentacle 20.2.4, session RESET after MDS
  journal replay.** An MDS that recreates a client session from its journal,
  because the session map object in the metadata pool is older than the
  journal, loses the session's Cephx entity name. A later `ceph_start_reclaim`
  with `CEPH_RECLAIM_RESET` for that session's UUID by the same principal fails
  with `EPERM`, although the MDS still evicts the target session. A session
  that its live client reconnected after the replay keeps its name. 19.2.7,
  Tentacle 20.2.5 and Umbrella 21.2.0 keep the name and accept the RESET.
- **Squid 19.2.5 and 19.2.6, unbuffered nonblocking writes when a data pool
  becomes full.** A libcephfs client with `client_oc=false` that has
  `ceph_ll_nonblocking_readv_writev` writes in flight when the pool turns full
  deadlocks: no write completes, later calls on the same client hang, also
  after the pool has space again, while other clients keep working. Buffered
  writes fail with `ENOSPC` instead. 19.2.7 and Tentacle 20.2.4 complete such
  writes with `-ENOSPC` and recover when the pool has space again.
- **Squid before 19.2.5, CephFS authorization.** These builds are not tested
  by the repository, and their authorization is weaker than the tested
  releases': MDS caps with one clause per filesystem (`allow r fsname=a, allow
  rw fsname=b`) grant every clause on every filesystem before 19.2.5;
  namespace-isolated subvolumes with the same name in two groups share one
  RADOS namespace before 19.2.4, so each one's principal reads the other's file
  data through RADOS; a path-restricted principal is refused reads of a
  snapshot of a directory it deleted before 19.2.3. Use 19.2.5 or later. The
  experiment case
  [`cephfs_authorization_isolation`](../../internal/integration/experiments/cephfs_authorization_isolation/README.md)
  checks all three on a chosen image.
- **Both releases, CephFS directory rebalancing.** External directory changes or
  mirror daemons joining or leaving can still trigger the upstream MGR hang; see
  [CephFS limitations](../../cephfs/README.md#concepts-and-limits).

## Role images

`ceph.ImageSet` selects Essential, Client, CephFS, RBDAux, RGW and MGR, or an
All image that supplies every role and may be combined only with MGR. This is
the image model used by the module and its CI.
`Run(ctx, image)` treats the image as an aggregate. Pass
`ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(set))` to `Run` for explicit roles, with an empty
image argument (or the set's essential image, which is not a second input); a
missing optional feature image is an error.

| Role | Runtime responsibility |
| --- | --- |
| essential | MON, MGR and OSD in separate containers; bootstrap/administration, MGR modules, OSD classes/plugins |
| mgr | The MGR daemons and their modules, when `ImageSet.MGR` selects an image other than essential |
| client | Ordinary CLI, Python rados/rbd/cephfs bindings, encryption/striper clients, offline map diagnostics |
| cephfs | MDS and CephFS mirror runtime |
| rbd-aux | RBD mirror and cache-capable client runtime |
| rgw | Gateway and RGW administration/setup |
| all | Aggregate supplying every role |

```go
cluster, err := ceph.Run(ctx, "", ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(ceph.ImageSet{
    Essential: essentialRef,
    Client: clientRef,
    CephFS: cephFSRef,
    RGW: rgwRef,
})), cephfs.WithFilesystems(cephfs.Config{Name: "shared"}),
   rgw.WithGateways(rgw.Config{Name: "s3"}))
if cluster != nil { testcontainers.CleanupContainer(t, cluster) }
if err != nil { t.Fatal(err) }
```

Use matching Ceph releases. The published role images are the
`<variant>-<release>-<role>` tags of
`ghcr.io/jsyoo5b/ceph-testcontainers-images`, for example
`official-20.2.4-essential`; this repository keeps no digest list of them.
The module default is the official-derived aggregate, `official-20.2.4-all`.

Cluster administration remains on MON/the stable multi-MON CLI. Ordinary
RBD/CephFS and native userspace oracles use the owned lazy client toolkit;
aggregate selections reuse their existing executor. Client setup and mirror
daemons use their respective client and feature images. RGW administration
runs in rgw without requiring a generic client. Cache daemon/plugin/socket
stay with their consuming librbd client. OSD plugins stay with essential, and
MGR modules with the MGR image. Caller-owned external clients remain caller-owned.

### The MGR image

`ImageSet.MGR` is the image of every manager the cluster starts: the initial
active and standby candidates of `Run` and the managers that `AddManager`
adds later. It replaces the whole MGR container, not a sidecar. Empty, it is
Essential (with `All`, the All image), so a set without it runs managers as
before; no other role has a default. Use it to run an image that ships
modules the essential role image omits, together with the role images of the
other daemons: the `mgr-modules` role image of the release, which the official
variant publishes (`official-<release>-mgr-modules`), the official image, or a
consumer image built from either with additional modules:

```go
// mgrModulesRef is the mgr-modules role image of the same release.
images := ceph.ImageSet{Essential: essentialRef, Client: clientRef, MGR: mgrModulesRef}
cluster, err := ceph.Run(ctx, "", ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(images)))
// FactsOf(cluster).Images.MGR is the MGR image; every ManagerContainer.Image reports it.
```

- The precedence for one manager is `DaemonOptions.WithImage` of its
  `AddManager` call, then `ImageSet.MGR`, then Essential. The override is the
  explicit per-daemon building block and is not checked.
- The MGR image must be of the cluster's release. When it is not the
  essential image, every manager start first runs `ceph-mgr --version` in a
  short-lived probe container of that image, which gets no keyring or cluster
  configuration and is removed afterwards. When the release differs from
  `ClusterFacts.CephVersion` the start fails before the manager's identity, key
  or container exists, so the daemon never runs: `Run` returns the partial
  cluster without that manager, and `AddManager` returns no handle. A missing
  `ceph-mgr` is `ceph.ErrImageCapability`. Adding modules does not make another
  release acceptable. The manager container is then created from the image ID
  that the probe checked, so rebuilding or retagging the image in between does
  not start an unchecked image; `ManagerContainer.Image` and
  `ClusterFacts.Images.MGR` keep the reference as given. The probe's removal is
  bounded (30 seconds) even when the start's context is cancelled, and a
  removal failure fails that start; `Terminate` removes a probe left behind.
- `ClusterFacts.Images.MGR` reports the resolved image and is never empty;
  finalization manifests and verification reports record it with the other
  roles. MGR containers are never recreated: `Stop` and `Start` keep the
  container, and a manager keeps its image until it is removed.
- Nothing in the module installs or names a module. Enable a module of the
  image with `override.SetMGRModule`, configure it with `override.SetConfig`
  (`mgr/<module>/<option>`), and read its state with `Observer.MGRModules`:
  a module whose dependency is missing is advertised with `CanRun` false and
  its `ErrorString`, and `SetMGRModule` refuses to enable it. A membership
  change respawns every manager; `SetMGRModule` and its `Restore` return once
  a respawned active manager is available, so `Observer.MGRModules` and other
  MGR commands work right after them.
  `Observer.WaitMGRModuleReady` knows only built-in modules; probe a module of
  your own through its own command. The Dashboard, which the official image
  ships and essential does not, is prepared by the
  [`dashboard`](../../dashboard/README.md) package. The prometheus module
  that `Observer.ManagerMetrics` scrapes is not a baseline module either;
  the `mgr-modules` role ships it.
- The enabled list of `ceph mgr dump` also names the release's
  `mgr_initial_modules` defaults (iostat, nfs and nvmeof since Squid). The
  essential role need not ship them; a manager without them stays
  `HEALTH_OK`, and `Observer.MGRModules` lists them as enabled.

`TestMGRImageOfficialSlot`, `TestMGRImageWithoutEssentialManager` and
`TestMGRImageConsumerModule` (batch `mgr_image`) run the official image in the
MGR slot, an essential image without `ceph-mgr`, and a consumer image built
from the official image with one small module, including a module whose
dependency is missing and a module option value that Ceph refuses. In the same
batch, `TestManagerPrometheusExporter`, `TestManagerMetrics`,
`TestManagerWatchReportsFailoverAndServiceAdvertisement` and
`TestMGRRuntimeRestoration` run their managers from the `mgr-modules` role
image. The batch declares
`image-roles=essential,client,mgr-modules`, so CI supplies
`CEPH_TEST_MGR_MODULES_IMAGE`; a role run without it fails before any cluster
starts, rather than falling back to the essential or the official image.

`RunNativeCommand` requires Python in the actual target container, and keeps
explicit capability checks. Selecting a toolkit does not add utilities to a
daemon. The complete [image requirements](https://github.com/JSYoo5B/ceph-testcontainers-images/blob/main/docs/IMAGE_REQUIREMENTS.md)
and [role contract](https://github.com/JSYoo5B/ceph-testcontainers-images/blob/main/docs/ROLES.md)
define the supported runtime dependencies.

### Only what the official image ships

Every baseline module feature works with the unmodified official image of each
supported release. The module relies only on components that image ships and
never installs packages at run time. This has visible consequences:

- No `tc`, `ip`, `iptables` or `nft` in the Ceph containers. Network faults
  of the baseline are whole-endpoint Docker operations or Ceph messenger
  options (see [Fault injection](fault-injection.md)).
- No kernel RBD mapping or kernel/FUSE CephFS mounts: clients use librbd and
  libcephfs in user space.
- No privileged containers, host disks or LVM: OSDs default to sparse
  BlueStore files; MemStore and memory-backed BlueStore are opt-in alternatives.
- Operation-only components of the official image, such as the dashboard,
  cephadm, ceph-volume, NFS Ganesha and iSCSI gateways, are never used, so
  custom images may omit them.

Extra tools and services for your own tests (go-ceph programs, S3 SDKs, a
Vault server for RGW server-side encryption, and so on) belong in their own
images, not in the Ceph role images.

### Opt-in extensions

An extension is a feature that needs tools the official image does not ship.
You select it explicitly, together with the image that provides the tools; no
baseline feature and no default uses it.

- The module checks the selected image against the extension's contract, and
  separately what the engine must grant (capabilities, kernel features), before
  it changes anything. A missing selection or tool is
  `ceph.ErrImageCapability`, and a denied permission or an unsupported kernel
  is `ceph.ErrEnvironmentUnavailable`, each with a narrower error of the
  extension (see [Classified errors](errors.md#image-capabilities-and-optional-extensions)).
  A tag name is not trusted, nothing is installed at run time, and there is
  no fallback.
- The Ceph containers keep their images. An extension's tools run in a helper
  container that the module starts and removes.
- [ceph-testcontainers-images](https://github.com/JSYoo5B/ceph-testcontainers-images/blob/main/docs/EXTENSIONS.md)
  defines each contract, checks images against it and publishes one image per
  extension. Any image that meets the contract works, including one from a
  private registry or a local build.

| Extension | API | Helper image | Contract |
| --- | --- | --- | --- |
| netem packet delay and loss | `ApplyNetworkProfile` (see [Fault injection](fault-injection.md#delaying-packets-below-ceph)) | `fault.NetemHelperImage` | netem contract 1 with its selector profiles: `sh`, `sleep`, `tc` and `ip` with JSON output, netem, `prio` and u32, `python3`; the engine grants `NET_ADMIN` to the helper and has the netem qdisc, and for peers the `prio` qdisc and the u32 classifier |

`NET_ADMIN` is granted only to the opt-in helper, not to the Ceph daemons.
This extension does not require Docker's privileged mode. It delays, drops,
rate-limits, reorders, duplicates or corrupts the packets that a container
sends, on the whole interface or only to selected peers, families and TCP
ports, and with `NetworkProfile.Direction` also what peers send to it (shaped
at the peers' endpoints). The module checks the kernel's `prio` qdisc and u32
classifier in its preflight helper before it selects peers. Packet inspection
is the separate [`capture`](../../capture/README.md) package.

## One daemon from another image

`DaemonOptions.WithImage` starts one new MON or MGR, and `OSDOptions.WithImage` one new
OSD, from an image other than the cluster's essential image. They are options of
`AddMonitor`, `AddManager` and `AddOSD`; see
[Per-daemon images](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#DaemonOptions.WithImage) for the full contract.

```golang
osd, err := cluster.AddOSD(ctx, ceph.OSDOptions{}.WithImage("registry.example/ceph:other"))
// osd.Image() is "registry.example/ceph:other"; FactsOf(cluster).Images.Essential is unchanged.
release, err := osd.Version(ctx) // what the running daemon reports, such as "20.2.4"
```

Read this scope before relying on it:

- It overrides one container. The cluster's images, `ClusterFacts` and the
  other daemons are untouched. For a MGR it takes precedence over
  `ImageSet.MGR`; to run every manager from another image, select it there.
- `CephVersion` is recorded once from the first MON and the CLI image. It does
  not describe an overridden daemon: use the daemon handle's `Version`, which
  reads the daemon's own admin socket and fails when the reply has no
  recognizable version. `Image` reports the image the container was started
  from, not its release.
- A re-launched daemon keeps its image unless the call names another:
  `DetachedOSD.Reopen` and `MonitorContainer.Replace` (with
  `MonitorStorage.WithVolume`) start the daemon from the image it ran from, or
  from the one their `WithImage` option names. MGR containers are not
  recreated; add a manager from the other image and remove the old one.
- The release check of a separately started client container still compares
  with `CephVersion` and is unchanged.
- Nothing checks that the image exists, has the daemon binary, or is
  compatible with the running cluster. See the next section for the one
  release transition that has been run.

## Rolling upgrade from Squid to Tentacle

`TestRollingUpgradeSquidToTentacle` (optional batch `rolling_upgrade`) moves a
running 19.2.5 cluster to 20.2.4 one daemon at a time, in the upstream order,
with the published official role images of both releases (the
`official-19.2.5-<role>` and `official-20.2.4-<role>` tags), on the host network and
on a bridge:

1. `osd set noout`.
2. Each of three MONs: `MonitorContainer.Replace` with the Tentacle image, on
   the store and address the MON keeps. The monmap's members and addresses do
   not change; after the last MON, `min_mon_release_name` becomes `tentacle`
   and the MONs commit a new monmap epoch for it.
3. The MGR: a Tentacle manager is added, then the Squid one is removed.
4. Each of three OSDs: `Detach`, then `Reopen` with the Tentacle image, so the
   Squid-written store is opened by Tentacle.
5. `osd require-osd-release tentacle`, `osd unset noout`, all PGs clean.

After every step `ceph versions` must show exactly the expected per-role
layout, every acknowledged write reads back with its digest, and a new batch of
writes is acknowledged. Tentacle MONs report the `AUTH_INSECURE_*` health
checks for the keys Squid created, so the cluster is in `HEALTH_ERR` from the
first Tentacle MON on; the fixture rotates no key.

What stays on the source release: `ClusterFacts.CephVersion`, the fixture's
Ceph CLI in the control container and the client toolkit. Command forms are
chosen from them, so the fixture keeps using Squid command forms against the
upgraded daemons. Mixing releases is therefore still not a general contract:
this is one pair, run forward only. Downgrade is not attempted.

### An interrupted upgrade, resumed

`TestRollingUpgradeResumesAfterInterruptions` (same batch, both network modes)
stops the same upgrade part-way in each daemon phase and resumes it forward:

| Phase | Interruption | State while stopped | Resume |
| --- | --- | --- | --- |
| MON | `Replace` of mon.b from an image without `ceph-mon`; the old container is already gone | quorum of a (Tentacle) and c (Squid); the monmap members and addresses are unchanged; writes are acknowledged | `Replace` on the handle the failed call returned, with the Tentacle image, on the retained store |
| MON | mon.c (Squid) replaced on its own Squid image while mon.a runs Tentacle | a Squid MON rejoins a quorum with a Tentacle MON | mon.c later moves to Tentacle |
| OSD | `Reopen` of osd.1 from an image without `ceph-osd` | the PGs keep two of three replicas; writes are acknowledged | `Detach` of the failed handle, then `Reopen` with the Tentacle image |
| OSD | osd.2 reopened on Squid while the MONs and MGR run Tentacle | a Squid OSD rejoins under Tentacle MONs | osd.2 later moves to Tentacle |

At the end every acknowledged write reads back, the monmap's members and
addresses (on the host network, their ports) are the ones the Squid cluster
started with, and the layout is all Tentacle. The monmap epoch moves once, when
the last MON runs Tentacle and the MONs commit `min_mon_release`. A stopped step never needs anything but the call that failed:
the retained store and the returned handle are enough to continue.

## Related APIs

- [`ceph`](../../ceph/README.md): `DefaultImage`, `Run`, `FactsOf`,
  `ImageSet` (including `MGR`), `ImageOptions` (`WithImageSet`, `WithCephFSImage`, `WithRGWImage`),
  `DaemonOptions.WithImage`, the error categories.
- [Classified errors](errors.md): `ErrUnsupported`, `ErrImageCapability` and
  how to write tests that hold on every release.
- [`cephfs/mirror`](../../cephfs/mirror/README.md): `Run`, `Container.RebalanceDirectories`.
- [`rgw/multisite`](../../rgw/multisite/README.md): `Run`, `WithTopology`,
  `Config.SetupImage`.
- [`rbd/mirror`](../../rbd/mirror/README.md): `Run`, namespace mapping.
- [Testing this repository](../contributing/testing.md): running the module's
  own suites against another image or release.
