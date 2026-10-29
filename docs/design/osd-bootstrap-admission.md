# OSD bootstrap admission before writes

`osd.sh` decides before any write whether an OSD directory is initialized or
started. A non-empty store without a ready marker must never have its keyring
overwritten, and a marker with foreign local identities must never start the
daemon. Retained-store reopen has its own checks; see
[OSD retained reopen](osd-retained-reopen.md).

The [launch recipe](osd-launch-recipes.md) separately reconstructs fresh file
Readers and admits one container-creation attempt.

## Admission rules

- Validate a numeric OSD ID and a non-nil UUID before constructing the data path
  or copying credentials. Reject a symbolic-link data directory or a non-directory
  data path.
- Initialize only an absent or empty OSD directory. Hidden files, partial state,
  dangling links and a non-regular/symlink ready marker make it non-empty and
  cause refusal before keyring copy or mkfs. No cleanup, repair or retry is inferred.
- For an existing regular ready marker, require non-empty regular non-symlink
  `whoami`, `fsid`, `ceph_fsid` and `keyring` files. Compare numeric ID and OSD
  UUID exactly to the launch environment. Read the configured cluster FSID with
  the local `ceph-osd -i ID --show-config-value fsid` path, without a MON query,
  and compare it to stored `ceph_fsid` before daemon dispatch.
- A matching store starts without key copy or mkfs. A failed fresh initialization
  leaves its partial files owned for inspection/disposable-cluster cleanup;
  another script invocation refuses the resulting non-empty unready directory.
  It does not automatically replay mkfs to manufacture a successful startup.

The keyring check establishes presence and file kind, not key/capability equality
or current authentication. Local FSID matching is not a MON-registration or
exclusive-attachment check. External edits/concurrent starts must not race this
preflight. A ready marker alone is never retained-store admission.

## Validation layers

`ceph/osd_bootstrap_test.go` executes the actual embedded shell with only its
fixed filesystem paths relocated and `ceph-osd` replaced by an argv-recording
stub. Fresh/matching controls and negative inputs check command order, zero
mutating bootstrap calls on refusal, preserved file bytes/permissions/link
identity and refusal to replay failed initialization. These tests characterize
shell control flow, not real formatting, storage exclusion or native reopen.

Selected native checks must separately exercise fresh initialization and
same-container Stop/Start with known-byte readback. Disk-backed and tmpfs-backed
BlueStore profiles, supported releases and MemStore are different cohorts;
compilation or a stub success cannot substitute for those executions.
