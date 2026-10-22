# Retained OSD detach and reopen

This is the first retained lifecycle increment for #6. `DetachOSD` and
`ReopenOSD` restart one classic BlueStore store from the owned ordinary volume
of [WithOSDVolumeStorage](osd-volume-storage.md) in a different container,
with the same numeric OSD ID, OSD UUID and cluster FSID. `AddOSD` and
`RemoveOSD` keep their meanings: fresh registration and permanent purge.

## States

| State | Owner | Container | MON registration |
| --- | --- | --- | --- |
| Owned | `c.osds` | the running instance | up |
| Detaching | `c.osds`, `detaching` | fenced; stopped or removed (`detachRemoved`) | down |
| Detached | `c.detachedOSDs` | none | down, same UUID |
| Reopened | `c.osds`, a new descriptor | a new instance | up, same UUID |

Every transition holds the topology mutex, so two reopens of one store are
serialized and the second finds it consumed. `RemoveOSD` refuses a detaching or
detached OSD. `Terminate` needs no extra step: a detached store has no
container, and the owned volume is discarded as before.

## Detach

1. Admit the native registration with the captured UUID.
2. Refuse an instance that a held network delay, network interruption or pause
   targets. Their undo needs the container that detach removes. A completed
   delay lease does not count.
3. Fence the Docker instance against new native commands and drain its tracked
   ones, as `RemoveOSD` does. The fence is per instance and leaves other
   targets admitted.
4. Stop the container, wait for the registration to be down, then remove the
   container. Stop alone is not enough, because a raw handle could start the
   stopped container again.
5. Only after confirmed removal, read the store through the volume helper:
   the regular, non-symlink files `ready`, `whoami`, `fsid`, `ceph_fsid`,
   `keyring` and `block`, and the block's inode and size. Require the
   captured ID, UUID and the served cluster FSID, and a key for `osd.<id>`.
6. Publish the `DetachedOSD` and drop the descriptor from `c.osds`. The old
   `*OSDContainer` is not rebound and keeps naming the removed container.

A failure after the fence keeps the OSD owned with its fence. A retry resumes
after a completed removal and does not stop or remove again.

## Reopen

The preflight only reads. It requires the store identity, keyring and block
inode and size to equal what detach recorded, the served FSID to match, the
MON registration to hold the same UUID and be down, and `auth get-key` for the
OSD to equal the stored key. Any refusal leaves the OSD detached.

After the preflight the `DetachedOSD` is consumed before the container is
requested. A new descriptor is published, the ordinary launch recipe is built
with the stored keyring, and the container mounts the same volume. The OSD
bootstrap takes its ready branch, which checks `whoami`, `fsid` and
`ceph_fsid` again and never runs mkfs or copies a keyring. A failed or
uncertain start returns the new handle owned by the cluster, as `AddOSD`
does. It can be detached again, removed, or terminated with the cluster.

## Exclusion

Exclusive use of one store comes from three facts: the state machine above
under the topology mutex, the confirmed removal of the old container before
the store is published as detached, and a down registration before reopen.
BlueStore's own lock on its block file is an additional safeguard. None of
these revokes a raw Docker handle that a caller copied; the old container
is gone, so such a handle cannot start it.

## Validation and remaining scope

Unit tests use a fake control, a store-backed helper and a fake runner. They
cover the reopen identity and the distinct handle, every preflight refusal
before any write and its retry, refusals before stop, partial detach after a
failed removal or unreadable store, cross-cluster and forged handles, and a
failed start that consumes the detached OSD.

The native `topology/osd_retained_reopen` batch seeds 1,048,583 known bytes in
a single-replica, one-PG pool on the only OSD. It detaches the OSD, checks that
the old container is gone and that the registration stays down and in with
the same UUID, then reopens it. The test requires a different container, the
same ID, UUID, FSID and volume, a clean PG and an exact readback. No other
replica exists, so the bytes come from the retained store.

This does not cover abrupt exit, host or daemon restart durability, image
changes between detach and reopen, reopening in another cluster, the tmpfs and
MemStore backends, detaching with a fault held, or several retained stores in
one native run. A pending-resource ledger for a lost creation reply still
relies on the existing owner-label scan.
