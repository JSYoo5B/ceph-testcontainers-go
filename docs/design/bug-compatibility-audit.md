# Release-dependent branches: compatibility audit

This is step 1 of the [bug compatibility checklist](../contributing/bug-compatibility.md#implementation-and-review-checklist)
for [#24](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/24). It
lists the product code that behaves differently by Ceph release or by a
detected capability, and classifies each branch as the
[policy](../contributing/bug-compatibility.md#classify-the-difference-first)
does. A behavioral bug that needs a `ceph_bug_<id>` tag is the only class that
the registry would hold.

The audit covers the public packages `ceph`, `rbd`, `cephfs`, `rgw` and
`fault` at the commit that adds this file. It searched for the release helpers
(`cephBefore`, `servicehook.CephBefore`, `multicluster.CephBefore`), for every
use of `CephVersion()` and for capability probes whose result changes a
command. Test-only selections are listed separately at the end.

## Result

No branch is a behavioral bug workaround that needs a bug tag. The
[catalogue](../guides/bug-compatibility-tags.md) stays empty. One branch works
around a release defect, and its alternative is valid on every supported
release, so it is the ordinary path and needs no selection.

| Branch | What differs | Decided by | Class |
| --- | --- | --- | --- |
| `cephfs/filesystem.go`, `startFilesystem` rank counts | `fs new ... set max_mds <n> standby_count_wanted <n>` exists from Ceph 20; Ceph 19 sets the counts right after `fs new` | Release (`CephBefore(c, 20)`) | Release syntax |
| `cephfs/filesystem.go`, standby-replay and affinity settings | Ceph 20.2.4's inline setters look up the FSMap entry before `fs new` commits it, so these settings are always applied after `fs new` | Not branched: every release takes the same order | Bug avoided by an ordinary path that is valid everywhere; no tag |
| `rgw/multisite_sync_pipe.go`, system-mode pipe parameters | Ceph 19 stores a system-mode pipe with an explicit empty `user` | Release of the master zone's cluster | Release representation |
| `rgw/multisite_sync_pipe.go`, `accountPipeUnsupported` / `zoneBefore` | User-mode sync pipes with an RGW account principal resolve no source on Ceph 19 | Release of any owned zone's cluster; no tool reveals it | Feature availability by release, refused with `ErrUnsupported` |
| `rgw/multisite_sync_bucket_status.go`, `legacyBucketSyncStatusJSON` | Ceph 19's `bucket sync status` prints text even with `--format json`; it is converted to the later JSON document | Release of the destination zone's cluster | Release representation |
| `rbd/mirror_namespace.go`, `legacyCluster` / `legacyClient` | `rbd mirror pool enable --remote-namespace` exists from Ceph 20 | Detected from the usage of `rbd help mirror pool enable` of each setup client and cluster client toolkit; the release decides only for a link that `RunMirror` did not set up | Feature availability, detected |
| `rbd/mirror_namespace.go`, `legacyRBDRemoteNamespace` | Ceph 19 omits the remote namespace from the mirror policy it prints | The same detection as above | Release representation |
| `rbd/mirror_receiver_status.go`, `legacyRBDNamespaceReplayers` | Ceph 19's `rbd mirror status` admin-socket document has `image_replayers` and `namespaces` instead of `namespace_replayers` | Release of the destination cluster | Release representation |
| `ceph/auth_config.go`, `generateKeyArgs` | `ceph-authtool --key-type` is absent before 20.2.4; another key type than AES is refused | Detected from the first MON's essential image | Feature availability, detected |
| `ceph/dual_stack_admission.go` | Concrete dual-stack Messenger addresses are refused when the daemons do not advertise them | Native observation of the daemons' addresses | Feature availability, observed |

`ceph/images.go` also compares the client image's release with the core
image's. That is an admission rule for mixed role images, not a branch, and
`OperationError` values carry `CephVersion()` only as context.

## Limits of these decisions

- A branch decided by release cannot recognize a custom image of an older
  release that carries the later behavior. For the representation branches,
  such an image would get the older conversion. The account-pipe refusal
  would also refuse a backport. None of these is a behavioral bug: a fix of
  the image's own defect would not make the ordinary path fail. Detecting the
  output shape instead of the release would remove the limitation for the
  representation branches, and would be an ordinary change, not a bug tag.
- `CephVersion()` is the release of the image that runs the first MON and the
  CLI. Daemons from other role images may run another release. The RBD and
  RGW branches read the release of the cluster that owns the command's target.
- This audit lists source branches. It does not show that each branch is
  exercised natively on every supported release; that coverage belongs to
  the existing release scenarios.

## Test-only release selections

These choose expectations or log context in tests and do not change product
behavior. A bug tag would not apply to them:

- `internal/integration/release_capabilities_*`: the release capability table,
  which states which capabilities each release row supports and checks the
  product's detection and refusals against it.
- `internal/integration/auth_ciphers_*`: the expected cipher behavior with and
  without `ceph-authtool --key-type`.
- Version strings in log lines and receipts, and checks that a daemon's
  reported version starts with the cluster release
  (`native_command_*`, `daemon_admin_socket_*`, `crash_metadata_*`,
  `dual_stack_*`, `messenger_client_profiles_*`, `cephfs_fuse_mount_*`).

## Keeping the audit current

A change that adds a release or capability branch should add a row here with
its class. A branch that needs a different path for an image with a
behavioral defect, where that path fails on a fixed image, is the case for a
registered `ceph_bug_<id>` tag under the policy; adding one also adds its
catalogue entry, its paired paths and their tests.
