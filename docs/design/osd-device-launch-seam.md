# OSD device launch seam

The internal launch seam maps one verified, owned block device into one OSD
before `mkfs`. There is no public host-device option and no native device run:
nothing shows yet that Docker maps a DM node into the OSD container or that
`ceph-osd --mkfs` succeeds on it. This page records what the launch path does,
what the pinned Ceph source does with a device path, and the internal claim
table and wiring step.

Pinned source: Ceph 7f793731f1b39eb4f465e960113d2363c311b964 (Tentacle 20.2.4).

## Launch path

| Step | Where | Fact |
| --- | --- | --- |
| Request | `osdLaunchRecipe.Customize` (`ceph/osd_launch.go`) | Adds network, config and keyring files, the bootstrap script, env, wait strategy and the three optional volumes (memory, ordinary OSD volume, run). It has no device, host-config or capability input. |
| Bootstrap | `ceph/internal/scripts/osd.sh` | Takes the fresh branch only for an empty `/var/lib/ceph/osd/ceph-<id>`, copies the keyring, then runs `ceph-osd --mkfs -i <id> --osd-uuid <uuid>`. A present regular `ready` marker takes the restart branch and never runs `mkfs`. |
| Block config | `ceph/internal/scripts/mon.sh` (rendered `[osd]` section) | `bluestore block create = true` and `bluestore block size = <OSDStorage.WithBlockSize>`, shared by every OSD through the one cluster config. |
| Reserved keys | `ceph/config_file.go` | `bluestore_block_path`, `bluestore_block_create` and `bluestore_block_size` are rejected from user overrides as fixture-managed. |
| Retained store | `retainedStore` (`ceph/osd_retained.go`) | Requires `block` to be a regular, non-symlink file in the volume directory. |

Consequences: a device cannot reach the daemon through the ordinary launch, the
cluster-wide config cannot carry a per-OSD path, and a symlinked block entry is
refused by the retained-store admission on purpose.

## What the pinned source does with a device path

`BlueStore::mkfs` calls `_setup_block_symlink_or_file("block",
bluestore_block_path, bluestore_block_size, bluestore_block_create)`
(`BlueStore.cc`, mkfs body near line 8614; function at line 8465). In order:

1. A non-empty path makes a `symlinkat(path, osd_data_fd, "block")`. An existing
   `block` entry fails with `EEXIST`, so the symlink must be created by this
   call or not at all. Pre-creating it in the data directory would also trip the
   bootstrap script's non-empty-directory refusal.
2. A non-zero size then opens `block` with `O_RDWR`, adding `O_CREAT` when
   `bluestore_block_create` is true. The result is resized only if it is a
   regular file of size 0. A device node is not regular, so the configured size
   does not truncate or resize it; the size comes from the device.
3. If the symlink target does not exist in the container, `O_CREAT` follows the
   dangling link and creates an empty regular file at the target path. With the
   shared `bluestore block create = true` and a device path that did not
   arrive, `mkfs` would silently fall back to a file in the container. The seam
   therefore has to set `bluestore_block_create=false` for a device OSD and must
   verify in the container that the target is a block device before `mkfs`.
4. `mkfs` then calls `_open_bdev(true)`. With `bdev_enable_discard` set it
   discards the whole device. The default is false (`global.yaml.in`), but the
   seam must treat `mkfs` as destructive for the whole mapped device and never
   map a device that holds data the fixture does not own.
5. On later starts `block` is the persisted symlink; `_open_bdev(false)` opens
   it through `KernelDevice::open`, which opens several descriptors for one
   device. A restart or reopen therefore
   needs the same node at the same container path, or an updated link.

## What a seam must carry

A device launch needs one typed, internal value, separate from `osdLaunchRecipe`
inputs that are plain strings, with these parts:

- Lease identity: the owned DM mapping UUID, major:minor and normalized table,
  the loop association and its backing identity, as recorded by the owned mapping
  lease before the daemon starts. This is the `lease` record that
  `.github/scripts/block_attachment_join.py` already joins; the Go side must
  not invent a second shape.
- Container path: a fixed in-container path chosen by the fixture (never a host
  path from the caller), the permissions to grant (`rw` only) and the
  declared device size. Docker's device mapping and its cgroup device rule live
  in the host config, so the seam is a host-config customization next to
  `osdLaunchVolume`, not a file or env change.
- Per-daemon config: `--bluestore-block-path` and `--bluestore-block-create=false`
  on the `mkfs` command line only, since the cluster config is shared. This needs
  an `osd.sh` input (for example one env variable consumed on the fresh branch
  only), validated like `CEPH_OSD_UUID`. It must not be reachable from user
  config overrides, which keep rejecting the three reserved keys.
- Metadata separation: the OSD metadata directory, keyring and MON registration
  remain on the ordinary storage backend. Only `block` comes from the lease.
- Admission order: verify the lease identity, create the container with the
  device, check inside the container that the path is the expected
  block device (type and major:minor), then run `mkfs`, then start the daemon.
  A failure before `mkfs` leaves the device unused.

## Ownership and cleanup

- The lease owner outlives the OSD. The OSD may be released only after the
  daemon has stopped, any observer borrow is closed
  (`block_fd_borrow.py` closure lease) and a fresh scan shows no device user,
  which is the existing DM/loop release barrier. Container removal alone does
  not satisfy it.
- A device OSD must not be accepted by `OSDContainer.Detach`, `DetachedOSD.Reopen`
  or `Remove`'s volume cleanup as if its `block` were a volume file. Until a
  separate typed admission exists, those paths refuse it. Weakening the
  regular-file check in `retainedStore` is out of scope.
- A `mkfs` that ran on the device makes the device contents the OSD's data. The
  lease owner, not the OSD handle, decides whether the chain is destroyed, and
  retains the whole chain when any control is uncertain.
- The one-creation-attempt rule of `startOSD` applies: the seam does not
  authorize replaying a device launch.

## Cluster-wide device claim table (internal primitive)

`ceph/osd_device_claims.go` (`osdDeviceClaims`, `Container.deviceClaims`) is the
claim table of the launch seam. It is a primitive: only the unexported wiring step below calls `claim` and it has no production caller, so no cluster holds a claim and the two
Terminate hooks and the `removeOSD` hook are no-ops.

- **What a claim is.** Exclusion bookkeeping in the Go cluster: OSD ID, an opaque
  lease token the caller chose, the `osdBlockDevice` identity and the recipe's
  `started` one-shot. It is not the file, loop and device-mapper lease. Dropping a
  claim says only that no container of this cluster can hold the device any more;
  it never permits the caller to release the DM/loop/backing chain, whose barrier
  stays the caller's.
- **Uniqueness.** One claim per OSD ID, lease token, `/dev/mapper` node and
  major:minor, each compared with every held claim. A refused claim changes
  nothing and repeating a request is a conflict with itself.
- **Lock.** `Container.mu` guards the table; it has no mutex of its own. `claim` is
  meant to run where `registerOSD` runs, before the recipe can start. The only
  state shared with `startOSD` (which runs without `mu`) is the recipe's atomic
  one-shot.
- **Partial start.** A claim whose creation was not attempted is released by
  sealing the one-shot first, so a recipe that outlives its claim cannot start and
  `startOSD` reports the ordinary "do not replay" error. A claim whose creation was
  attempted stays until a container removal is confirmed: a lost create reply can
  leave a running container that holds the device.
- **Release order.** `removeOSD` drops the claim after the container terminated and
  the storage directory was discarded, right before the handle is forgotten.
  `terminate` drops it per OSD after `osd.Terminate` and before the handle is
  forgotten, and `settle` drops the rest only after the labeled-container removal
  succeeded for an owned cluster. A claim that survives Terminate is named in its
  error so the lease owner keeps the whole chain.
- `Detach`, `Reopen` and the retained-store admission refuse a device OSD.

## Wiring step (internal, no production caller)

`registerBlockDeviceOSD` (`ceph/osd_device_wiring.go`) is the one serialized
step that registers an OSD, builds its recipe, attaches a verified
`osdBlockDevice`, claims it under the caller's opaque lease token and returns the
launch for `startOSD`. The caller holds `Container.mu`; the step takes no lock.
It is the only caller of `attachBlockDevice` and of `claim`, and nothing in
production calls it, so default launches do not use it. A failure after
registration seals the recipe (never attempted) and leaves the claim table
unchanged; an attempted or uncertain `startOSD` keeps the claim by the table's
rules. The caller keeps the external lease through uncertain startup and owned
cleanup; no result of the step releases it. The step's call order and guards
are pinned by `ceph/osd_device_wiring_test.go`. It establishes no attachment observer, native device opening, effect window,
restart/reopen or cross-process exclusion.

## Composition with the Python observers

The three observer units map onto launch phases and stay separate from it:

| Phase | Unit | Provides | Does not provide |
| --- | --- | --- | --- |
| Before `mkfs` | lease record (join input) | Owned chain identity recorded before the daemon starts | Any statement about the daemon |
| Daemon running | `block_fd_observer.py` | Flag-aware FD classification; O_PATH diagnostics kept apart from read/write direct and buffered FDs | Exclusivity or workload exposure |
| During observation | `block_fd_borrow.py` | Scoped borrow with a closure lease | Teardown evidence |
| After start | `block_attachment_join.py` | Point-in-time join of lease, DM/loop/file, container generation, PID start identity and snapshot | Fault effects |

The Go seam supplies the lease and the container/daemon generation records; it
does not interpret descriptors. The observers run on the Linux runner against
real `/proc` and are native evidence only when a CI job runs them on an exact head.
