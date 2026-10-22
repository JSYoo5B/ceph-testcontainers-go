# Owned ordinary OSD volume storage

This is a bounded storage-backend increment for #6. `WithOSDVolumeStorage`
selects classic BlueStore files in one cluster-owned ordinary Docker local named
volume. The default remains per-container writable-layer files. The tmpfs and
MemStore options cannot be combined with this option; admission precedes Docker
allocation. No existing volume, host mount, device or driver option is adopted.

## Allocation and ownership

Allocation is lazy, including `WithNoInitialOSDs`. Before the first native OSD
registration, the cluster records a UUID-derived volume name and ownership token,
then creates the ordinary local volume without driver options. A partial or
uncertain create stays owned and is not replayed. An isolated idle control-image
helper mounts it with no-copy, receives no Ceph credentials and has no network.

Directory operations inspect exact name, driver, owner label, absence of driver
options and the captured creation timestamp. A fresh registration refuses an
existing path or dangling symlink. The launch recipe captures the volume name
and reconstructs a writable no-copy mount at `/var/lib/ceph/osd`.

The helper's original running process identity is required for new directory
operations. Helper loss is a refusal, not proof of data loss: ordinary volume
bytes do not need a live keeper. Existing original OSD containers may restart
while the helper is stopped. There is no automatic repair or replacement.

## Purge and termination

`RemoveOSD` keeps its existing drain, native UUID/purge and target-command fences.
Only after purge and confirmed container removal does it delete that numeric
ID's directory. A cleanup failure retains the purged descriptor and refuses new
registrations until cleanup is retried; sibling OSD directories remain intact.

`Terminate` first removes every tracked OSD, then the helper and ordinary volume.
It never forces volume deletion. A busy mount or uncertain error retains the
storage owner for retry. The existing cluster-label scan removes owned containers
whose creation reply was lost, followed by another release attempt. A foreign
volume, changed metadata or unknown inspect failure is not treated as absent.
A confirmed missing volume settles cleanup.

The helper is not a public attachment lease. Metadata inspection and directory
operations/deletion are not an atomic Docker compare-and-swap. External
modification, volume replacement or concurrent external attachment is unsupported;
labels and creation timestamps are conservative checks, not a security boundary.
Docker refusing a busy deletion is necessary cleanup behavior, not exclusive
attachment validation.

## Validation and remaining #6 scope

Unit controls cover incompatible admission, lazy/uncertain allocation, partial
helper ownership, changed metadata, stale helpers, fresh paths, sibling-preserving
purge and non-forced retryable release. The native `osd_volume_storage` batch
checks actual mounts, ordinary local metadata, payload readback after purge/fresh
addition and after all original OSDs plus helper stop. A read-only observer
checks stored identities while every original mount user is stopped. Recovery
uses the same OSD containers; an independent observer reference makes volume
removal fail until that reference is removed and `Terminate` retried.

[Retained detach and reopen](osd-retained-reopen.md) builds on this backend:
`DetachOSD` removes the container after a fence and keeps the store and
registration, and `ReopenOSD` starts a new descriptor and container after a
read-only identity, registration and key preflight. Its native batch reads
known bytes back from a single-replica pool through a different container.
Same-container restart, replication and ordinary volume allocation above do
not prove those criteria by themselves.

`Terminate` always discards owned storage. No host/daemon-restart or power-loss
durability is claimed.
