# OSD storage and lifecycle

How an OSD store is admitted, launched, kept in an owned volume and reopened in
a different container.

## Bootstrap admission

`osd.sh` decides before any write whether an OSD directory is initialized or
started. A non-empty store without a ready marker must never have its keyring
overwritten, and a marker with foreign local identities must never start the
daemon. Retained-store reopen has its own checks; see
[OSD retained reopen](#retained-detach-and-reopen).

The [launch recipe](#launch-recipes) separately reconstructs fresh file
Readers and admits one container-creation attempt.

### Admission rules

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

### Validation layers

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

## Launch recipes

A launch recipe holds the inputs of one OSD container creation. `AddOSD`
creates a new native registration; `OSDContainer.Remove` drains and purges.
Same-container Stop/Start uses the existing handle, and retained reopen is
described in [OSD retained reopen](#retained-detach-and-reopen).

### Immutable inputs and fresh requests

An OSD registration records its owned descriptor before container creation.
After the fresh-store and admin-socket preparation succeeds, its launch
captures config/admin credentials, OSD keyring, numeric ID/UUID/placement, image
reference, network/volume names (including the optional ordinary OSD volume), ownership label and startup timeout as values.
The topology caller holds the cluster lock while preparing and starting OSDs;
concurrent initial starts use distinct recipes. Config bytes are copied under
the config lock. The bootstrap script remains the compiled embedded script.

Each request customization reconstructs Readers for all four files (config,
admin keyring, OSD keyring and script), plus independent environment/label maps,
wait strategy and mount options. Consuming or modifying one request cannot
empty another request's files. The image reference and mount names are captured
inputs, not proof of an immutable image build, storage identity or exclusive use.

### Creation is not replayable

Request reconstruction does not authorize repeated container creation. Copies
of one launch share an atomic attempt guard. `startOSD` admits one creation
attempt and refuses later/concurrent attempts, including after a runner returns
an error without a handle. A canceled context checked before admission creates
nothing and does not consume the attempt. An incomplete preparation or an
already attached descriptor is refused before the runner; an existing handle
is never rebound by this helper.

Any returned container remains attached to the original registered descriptor,
including a readiness/create error, for Terminate ownership and retryable
cleanup. A lost create reply relies on the cluster ownership label scan.
Internal callers retain their topology/admission obligations.
Creating a distinct replacement instance requires a separately validated
lifecycle, not replaying this fresh-registration recipe.

### Validation boundaries

Host-only tests consume real request Readers, mutate original inputs and prior
requests, build concurrent requests and race copied launches. They verify
one-attempt admission and preservation/cleanup of a partial fake container.
These are request/ownership controls, not native storage exclusion or reopen.

Native checks separately exercise actual initial/runtime OSD creation, selected
bridge/host and disk/tmpfs profiles, and same-container restart/readback. A
passing fresh launch is not evidence of exclusive attachment.

The ordinary volume backend is described in
[OSD volume storage](#volume-storage); it uses the same one-attempt
launch contract.

## Volume storage

`OSDStorage.WithVolume()` selects classic BlueStore files in one cluster-owned ordinary Docker local named
volume. The default is per-container writable-layer files. The tmpfs and
MemStore options cannot be combined with this option; admission precedes Docker
allocation. No existing volume, host mount, device or driver option is adopted.

### Allocation and ownership

Allocation is lazy, including `ClusterConfig.WithNoInitialOSDs`. Before the first native OSD
registration, the cluster records a UUID-derived volume name and ownership token,
then creates the ordinary local volume without driver options. A partial or
uncertain create stays owned and is not replayed. An isolated idle essential-image
helper mounts it with no-copy, receives no Ceph credentials and has no network.

Directory operations inspect exact name, driver, owner label, absence of driver
options and the captured creation timestamp. A fresh registration refuses an
existing path or dangling symlink. The launch recipe captures the volume name
and reconstructs a writable no-copy mount at `/var/lib/ceph/osd`.

The helper's original running process identity is required for new directory
operations. Helper loss is a refusal, not proof of data loss: ordinary volume
bytes do not need a live keeper. Existing original OSD containers may restart
while the helper is stopped. There is no automatic repair or replacement.

### Purge and termination

`OSDContainer.Remove` keeps its drain, native UUID/purge and target-command fences.
Only after purge and confirmed container removal does it delete that numeric
ID's directory. A cleanup failure retains the purged descriptor and refuses new
registrations until cleanup is retried; sibling OSD directories remain intact.

`Terminate` first removes every tracked OSD, then the helper and ordinary volume.
It never forces volume deletion. A busy mount or uncertain error retains the
storage owner for retry. The cluster-label scan removes owned containers
whose creation reply was lost, followed by another release attempt. A foreign
volume, changed metadata or unknown inspect failure is not treated as absent.
A confirmed missing volume settles cleanup.

The helper is not a public attachment lease. Metadata inspection and directory
operations/deletion are not an atomic Docker compare-and-swap. External
modification, volume replacement or concurrent external attachment is unsupported;
labels and creation timestamps are conservative checks, not a security boundary.
Docker refusing a busy deletion is necessary cleanup behavior, not exclusive
attachment validation.

### Validation

Unit controls cover incompatible admission, lazy/uncertain allocation, partial
helper ownership, changed metadata, stale helpers, fresh paths, sibling-preserving
purge and non-forced retryable release. The native `osd_volume_storage` batch
checks actual mounts, ordinary local metadata, payload readback after purge/fresh
addition and after all original OSDs plus helper stop. A read-only observer
checks stored identities while every original mount user is stopped. Recovery
uses the same OSD containers; an independent observer reference makes volume
removal fail until that reference is removed and `Terminate` retried.

[Retained detach and reopen](#retained-detach-and-reopen) builds on this backend:
`OSDContainer.Detach` removes the container after a fence and keeps the store and
registration, and `DetachedOSD.Reopen` starts a new descriptor and container after a
read-only identity, registration and key preflight. Its native batch reads
known bytes back from a single-replica pool through a different container.
Same-container restart, replication and ordinary volume allocation above do
not prove those criteria by themselves.

`Terminate` always discards owned storage. No host/daemon-restart or power-loss
durability is claimed.

## Retained detach and reopen

`OSDContainer.Detach` and `DetachedOSD.Reopen` restart one classic BlueStore
store from the owned ordinary volume of
[OSDStorage.WithVolume](#volume-storage) in a different container,
with the same numeric OSD ID, OSD UUID and cluster FSID. `AddOSD` and
`OSDContainer.Remove` mean fresh registration and permanent purge.

### States

| State | Owner | Container | MON registration |
| --- | --- | --- | --- |
| Owned | the registry's owned map | the running instance | up |
| Detaching | the owned map, `detaching` | fenced; stopped or removed (`detachRemoved`) | down |
| Detached | the registry's detached map | none | down, same UUID |
| Reopened | the owned map, a new descriptor | a new instance | up, same UUID |

Every transition holds the topology mutex, so two reopens of one store are
serialized and the second finds it consumed. `OSDContainer.Remove` refuses a detaching or
detached OSD. `Terminate` needs no extra step: a detached store has no
container, and the owned volume is discarded as before.

### Detach

1. Admit the native registration with the captured UUID.
2. Refuse an instance that a held network delay, network interruption or pause
   targets. Their undo needs the container that detach removes. A completed
   delay lease does not count.
3. Fence the Docker instance against new native commands and drain its tracked
   ones, as `OSDContainer.Remove` does. The fence is per instance and leaves other
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

### Reopen

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

### Exclusion

Exclusive use of one store comes from three facts: the state machine above
under the topology mutex, the confirmed removal of the old container before
the store is published as detached, and a down registration before reopen.
BlueStore's own lock on its block file is an additional safeguard. None of
these revokes a raw Docker handle that a caller copied; the old container
is gone, so such a handle cannot start it.

### Validation

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

The same batch checks native refusals before that positive path, and each one
must leave nothing changed.

1. With the OSD paused through `fault.Pause`, `OSDContainer.Detach` refuses. The same
   container stays running and paused, the handle stays owned and the
   registration is unchanged. After the resume, a native command on that
   instance still runs, so the refusal set no fence. Detach then succeeds.
2. After detach, `OSDContainer.Remove` refuses and the registration stays the same.
3. An independent observer container mounts the owned volume read/write and
   appends a line to the retained `keyring`. `DetachedOSD.Reopen` refuses because the
   store changed since detach. The Docker container list, the down
   registration with the same UUID and the store fingerprint stay unchanged:
   mode, inode, size and modification time of every entry, and a SHA-256 of
   every regular file except `block`. No OSD is published.
4. The observer writes the original keyring bytes back in place, so the
   fingerprint equals the detached one except the keyring's modification
   time. The observer is removed and the reopen then succeeds.
5. A second `DetachedOSD.Reopen` of the consumed handle refuses.

This does not cover abrupt exit, host or daemon restart durability, image
changes between detach and reopen, reopening in another cluster, the tmpfs and
MemStore backends, a held network delay or interruption at detach, cancelled
or partially failed native detach and reopen after admission, or several retained
stores in one native run. Unit tests cover a
cancelled reopen, partial detach and a failed reopen start with fakes only. A
lost creation reply is settled by the owner-label scan.

### Concurrent native admission and saved instances

The separate `topology/osd_retained_concurrency` batch runs two goroutines
against one detached pointer from a shared start barrier. Exactly one returns
a successful new descriptor; the other must return the consumed-handle refusal.
Docker create events bounded by daemon timestamps and filtered to this cluster
require exactly one creation, naming the returned replacement. The independent
Docker inventory includes stopped containers and requires exactly
one container mounting this owned volume at the OSD data path. The helper's mount
is at a different path. The replacement keeps the registration, OSD UUID, FSID
and volume, and passes the same single-replica, 1,048,583-byte oracle.

Both the saved descriptor and its separately captured raw container must refuse
`Start`; both old native targets must refuse a command that succeeds on the
replacement. These checks distinguish container generations from registration
identity. They do not revoke an arbitrary caller-owned Docker handle.

A third reopen call is cancelled only when its forwarding caller context observes
the topology lock's contended wait. The first admitted call signals when its
bounded operation context is derived, after acquiring topology. The waiter
observes `Done` before its own context derivation and cancels then. The test
requires that observation, a cancellation error and no returned candidate; an
unobserved wait fails the scenario. This source-bound context observation is
separate from the Docker inventory and byte checks. No sleep assumes admission,
and no Docker response is intercepted or replayed.

This qualification does not simulate uncertain native creation replies, partial
container removal or failed volume release. Those require their own supervision
and cleanup evidence; fake runner controls remain separate.

### Cancellation after creation

The optional `retained_cancel_after_admission` batch
cancels the caller context of one real `DetachedOSD.Reopen` right after the runner
returned a ready container. The call returns an error with the owned
candidate, the detached handle stays consumed and a retry refuses without
another dispatch. The candidate is then detached with a fresh context and
reopened once more with an exact known-byte readback. It detaches only a
booted candidate. It does not cover cancellation during creation, readiness or detach, unsettled creation
or failed release.

## Block-device OSDs

An OSD can run with a device-mapper node as its BlueStore main device. This
path is internal: `internal/faulthook.AddBlockDeviceOSD` reaches it for the
block-device fault tests, and no public option adds such an OSD.

### Launch and claims

`addBlockDeviceOSD` (`ceph/osd_device_launch.go`) is the one production caller
of `registerBlockDeviceOSD`. It takes the topology lock and keeps it through
registration, the device attachment, the claim, the container start and the
up/in wait, as `AddOSD` does. The cluster must use the default OSD storage: the
attachment refuses memory and volume storage and MemStore.

The caller owns the backing file, the loop device and the mapping, and names
them with an opaque lease token. The claim table only excludes a second OSD of
the same cluster from the same lease, node or device number. Dropping a claim
never authorizes releasing the chain.

### Restart on the same device

`osd.sh` compares the device number and size it finds in the container with the
values the caller verified, on every start. On a fresh store it passes the
device to mkfs, and BlueStore links `block` to the device path. When the same
container starts again, the script admits the ready store only when `block`
links to that device path. Any other ready store with a device configured is
refused.

### Releasing the chain

`internal/faulthook.JudgeDeviceChain` (`judgeDeviceChain`) is the one
production caller of the release gate. It collects the owner's evidence first,
without the lock, then reads the claim table under it. It also works after
`Terminate`. The owner reports four facts: whether the daemon stopped, whether
every observer borrow closed, whether its opener scan was complete, and how
many openers that scan found. Only the outcome `clear` raises no objection, and
even `clear` is not an instruction to release anything.

### Native evidence

`TestBlockDeviceFault` (`scratch_tmpfs` batch) runs one OSD on a 512 MiB linear
mapping. A privileged keeper container with the engine host's `/dev` creates
the backing file on its own tmpfs, the loop device and the mapping. An unused
mapping of the same kind is the control.

1. Attachment. Reading `/proc` in the OSD container, the observer finds one
   `ceph-osd` process holding read-write descriptors of the mapping's device
   number. O_PATH references and descriptors whose flags cannot be read never
   count. The store's `block` links to the mapped node, and its `fsid` is the
   OSD's UUID.
2. Exposure. The mapping's write counters in `/sys/dev/block/M:m/stat` move
   during the baseline writes, and the control's stay at zero.
3. Delay. A `delay` table slows the median write by at least three quarters of
   the configured delay, and the restored linear table makes writes fast again.
   Every write of both windows must be acknowledged, so a write that fails fast
   never stands for a fast restored table.
   The measurement ends before the restore, because suspending a delay mapping
   flushes its queue early. The window runs only when the kernel has the
   `delay` target registered, or on a GitHub-hosted runner, where loading the
   table may load the module on demand.
4. Error. An `error` table fails every sector. The observed outcome on
   Tentacle 20.2.4 is an abort: `_aio_thread got r=-5 ((5) Input/output error)`,
   then `ceph_abort_msg("Unexpected IO error ...")`, and the container exits 5.
   The client writes time out and are never promoted to acknowledged. The judge
   also accepts a client EIO with the OSD running, and rejects a write
   acknowledged inside the window.
5. Recovery. With the linear table back, the aborted OSD starts again in the
   same container, every acknowledged write reads back with its digest, and
   new writes are acknowledged.
6. Release. After `Terminate`, the keeper's evidence (the OSD container gone,
   no borrow, the kernel's open count of the mapping) and the claim table must
   both be clear. Only then are the mapping removed, the loop detached and the
   backing file deleted, each confirmed.

The fault spans the whole main device, which also carries BlueFS, so it is a
shared BlueStore/BlueFS fault, never a data-only one. If a test dies before its
cleanup, the keeper container is reaped, but the mapping and loop device stay
in the engine host's kernel until someone removes them by name (`tcblk-*`,
`tcctl-*`).

### Supervised table control

Every table change of the windows above goes through the supervised control
(`internal/integration/block_device_control_helpers_test.go`). A table change
is three kernel steps: load the new table into the inactive slot, suspend the
node, and resume it, which swaps the table in. The control keeps a record of
the node it owns:

| Field | Meaning |
| --- | --- |
| UUID, device number | the identity it created; the keeper gives each mapping the UUID `TC-BLK-<name>` |
| live table | the table it last saw live after a step it confirmed |
| loaded table | the table it loaded and has not seen swapped in |
| unsettled step | a step it started and has not seen finish |

Each `dmsetup` step runs detached in the keeper with its exit status written
to a file, and the control waits for it up to a bound. A step still running at
the bound is left to the kernel, never killed. Before and after every step the
control observes the node (UUID, device number, `ACTIVE` or `SUSPENDED`, live
and inactive tables, open count, in-flight reads and writes) and classifies it
against the record:

| State | Seen | Next step |
| --- | --- | --- |
| `gone`, `identity-changed` | no node, or another UUID or device number under the name | none |
| `step-unsettled` | a step of the control has not finished | none until it settles |
| `foreign` | a live or inactive table the record does not explain, including the wanted table loaded by someone else | none |
| `unchanged`, `suspended` | the recorded table live, nothing inactive | load |
| `loaded` | the recorded table live, the control's table inactive | suspend |
| `suspended-loaded`, `swapped-on-hold` | suspended, before or after the swap | resume |
| `applied` | the wanted table live, active, nothing inactive | done |

The control only moves forward from a state the record explains, and it can
revert a load it has not swapped in (`clear`, then `resume` if it suspended).
A node may be removed only in `applied` with the recorded table, no opener and
no I/O in flight. A loop may be detached only while it still backs the file
the control attached (same path and inode).

`TestBlockDeviceControlSupervision` (`scratch_tmpfs` batch) drives the control
on scratch nodes over loop devices on the keeper's tmpfs, with `linear` and
`error` tables only:

- Partial apply and restore. The control stops after the load and after the
  suspend, for the fault and for the restore. Each state is observed, the
  release gate holds the node, and the change is reverted or completed. A read
  through the applied error table fails, and data written after the restore
  reads back.
- Foreign table changes. Another actor swaps in a half-size linear table, so
  only the exact table tells it apart, and later loads the very table the
  control wants. In both cases the control does not act: the event number and
  the live table do not change, and the gate holds the node.
- Identity reuse. A node created again under the owned name gets another UUID.
  On Docker Desktop's kernel it also got the same device number (253:0), so the
  name and the device number alone would not have told them apart. A released
  loop number was given to the next file (`/dev/loop2`), and the gate refuses
  to detach it as the owned loop.
- Pending I/O and a cancelled step. The lower node of a two-node stack is
  suspended and a direct write to the top node is in flight. The control loads
  the error table, and its suspend waits for that write past its bound. The node
  stays `ACTIVE` with the error table inactive, one opener and one write in
  flight. The control starts nothing else, and the gate holds the node. Once
  the lower node resumes, the write completes on the old table, which the lower
  node reads back, and the abandoned suspend finishes on its own. A write queued
  on the suspended node then fails with EIO when the resume installs the error
  table, and its bytes never reach the store.

In `TestBlockDeviceFault` the same control applies the delay, error and
restore tables to the OSD's mapping; a step that outlives its bound is waited
for and the change continued from what is observed.
