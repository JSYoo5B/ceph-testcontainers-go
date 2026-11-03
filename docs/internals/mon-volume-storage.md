# Cluster-owned MON data volumes

Every MON of a cluster can keep its store in its own cluster-owned Docker volume,
so that a replacement container (possibly from another image) mounts the same
store and rejoins. `WithMonitorStorage(MonitorStorage{}.WithVolume())` selects
it, and `MonitorContainer.Replace` moves a MON to a new container; see
[Production wiring](#production-wiring) for what calls which primitive below.

Pinned source: Ceph 7f793731f1b39eb4f465e960113d2363c311b964 (Tentacle 20.2.4).

The store records, their state machine and the generation and address paths
live in the internal package `ceph/internal/monstore`, which imports no part of
package ceph. Package ceph keeps the container side of a generation
(`monitorGenerations` in `ceph/mon_store_container.go` and
`ceph/mon_store_host.go`) and the replacement (`ceph/mon_store_replace.go`), and
holds one `monstore.State` per cluster.

## The model, and what it forces

The model is the whole requirement. Every MON has its own volume (never a
shared cluster volume). The volume outlives the container. A MON upgrade means
the old container is stopped and removed, and a new container, possibly from a
new image, mounts the same volume of that MON and rejoins. Nothing else is
needed for the data to persist.

Only these choices follow from it:

- One volume per MON.
- The replacement mounts the volume of that MON, only after the old container is
  confirmed gone, and never formats: the entrypoint's require-store mode makes
  a missing store a failure.
- The new container needs the old address. The monmap pins it, so a replacement
  that cannot bind it cannot rejoin. See "Address handoff" below.

## Facts the design rests on

| Fact | Source | Consequence |
| --- | --- | --- |
| The first MON runs `mon.sh`; an added MON runs `mon-join.sh`. Both run mkfs only when `<data>/store.db` is absent, otherwise they `exec ceph-mon -f`. | `ceph/internal/scripts/mon.sh`, `mon-join.sh` | A fresh empty volume mounted where a retained store was required would silently format a new store and join as a new member. The retained launch needs a mode that fails closed when `store.db` is missing. |
| Without `MonitorStorage.WithVolume`, neither MON path mounts a data volume; the only mount is the run volume. | `runMonitor` in `ceph/ceph.go`, `AddMonitor` in `ceph/topology.go` | Without the volume the store lives in the container's writable layer, and a new container starts empty. |
| `ceph-mon --mkfs` refuses a data directory that holds anything except `kv_backend`; `ceph-mon` without mkfs exits when the directory is missing or empty. | `ceph_mon.cc` (`check_mon_data_empty`, `check_mon_data_exists`) | A half-formatted store is detectable by shape, and a failed first start cannot be retried on the same bytes without inspection. |
| `Monitor::mkfs` writes the cluster UUID, the seed monmap and the mon. key into the store, and moves the mon. key into `<data>/keyring`. | `Monitor.cc` (`mkfs`, `write_fsid`, `extract_save_mon_key`) | The identity inside `store.db` (`cluster_uuid`, monmap) is not readable with plain file operations. Offline file checks can bind the `keyring` bytes and the store shape; fsid and rank binding come from the live monmap while the member is down. Whether an image tool can read `store.db` offline is not established. |
| The first MON is the embedded `testcontainers.Container` of `*ceph.Container`. `Terminate` on it is cluster-wide, and `MonitorContainer.Remove` or `Replace` on MON a only marks it terminated (`monitorTerminated`). | `ceph/ceph.go`, `ceph/handles.go`, `ceph/monitor_topology.go` | The initial MON cannot be replaced by swapping a container into that field without a data race on a field users read without a lock. |
| OSD storage is one cluster-owned volume with a persistent helper, per-ID directories, publish-before-create, owner labels, a creation timestamp check, non-forced release and a second release stage after the label scan. | `ceph/osd_volume_storage.go`, `ceph/osd_storage_state.go`, `ceph/osd_storage_release.go`, [OSD volume storage](osd-storage.md#volume-storage) | The ownership vocabulary is reused. The topology (one volume, one helper) is not: a MON store has one volume per MON and no helper. |

## Ownership model

One record per MON name, owned by the cluster, kept in an internal state
component beside `osdStorageState` and `memberRoster`. Like those it has no
mutex: `Container.mu` guards it, and `Run` writes it only before the cluster is
shared. One volume per MON, not one volume with per-name directories, because a
MON generation must be retained, mounted and released independently of its
siblings. The volume is mounted with no-copy at the MON's data directory,
`/var/lib/ceph/mon/ceph-<name>`, so the volume root is the data directory.

Record fields: MON name; volume name derived from a UUID owner token; owner
token; Docker creation timestamp; state; generation counter; the current
attachment (empty, or the container of that generation with its Docker ID,
whether its creation outcome is known and whether its removal was requested);
the endpoint the first generation bound (see "Address handoff").

Volume metadata checks are those of the OSD volume: exact name, `local` driver,
owner label equal to the token, a MON-name label, no driver options, creation
timestamp unchanged. A volume that fails them is never removed or mounted.

### Store states

| State | Meaning | Store may be discarded | Store may be mounted by a new generation |
| --- | --- | --- | --- |
| planned | Record published, create not yet requested | yes | no |
| created | Volume exists and passed the metadata checks | yes | first generation only |
| fresh | A first generation was requested; mkfs may have run | yes, only after the attachment is confirmed gone | no |
| bound | The first generation reached its ready check | only after native removal of the MON, or by `Terminate` | only by a replacement generation in require-store mode, once the previous attachment is confirmed gone |
| released | Volume removal confirmed | n/a | no |

Rules:

1. Publish before create: the record, with its generated name, exists before
   the create call, so a lost reply stays owned. A later call never allocates a
   second volume to resolve the uncertainty.
2. A store is `fresh` until its first generation passes the same readiness check
   the MON has (admin socket for an added MON, `ceph status` for the first).
   A `fresh` store holds no state anyone needs. After a failed first start with
   a confirmed-gone container, it is discarded and the retry creates a new
   record and volume. This keeps the host-port retry loop of `AddMonitor` and
   `runMonitor` from reusing bytes an interrupted mkfs may have left behind.
3. A `bound` store is never silently replaced. If a later start finds the
   volume missing, changed or foreign, the start fails; it does not create a
   fresh volume.
4. At most one attachment exists. A new generation is dispatched only when the
   previous attachment is confirmed gone: the container is absent by inspect,
   and an independent Docker list filtered by the volume name shows no container
   (running or stopped) that mounts it. The ledger alone is not trusted.
5. An unknown creation outcome (no container returned, or a reply lost) keeps
   the attachment pending-unknown. The store cannot be released, discarded or
   reopened until the label scan of `Terminate` settles it. Creation is never
   replayed.

## Lock position and release order

Every operation runs under the topology lock `Container.mu` with a bounded
context, as the OSD operations do. `monstore.State` has no mutex and sits at the
position of `osdStorageState` (`TestPackageAddsNoMutex` checks that the
package imports no `sync`); Docker calls are made with `Container.mu` held, as `registerOSD` does for the OSD
volume. `Terminate` releases the stores in a first stage after the added and
primary MON containers are terminated (they are the volume users), then retries
after the labelled-container scan together with the OSD retry stages. Release
is never forced: a store with a live or unknown attachment, a changed or foreign
volume, or a busy removal stays owned and is reported. A confirmed missing
volume settles cleanup.

## Require-store mode

Implemented in `ceph/internal/scripts/mon.sh` and `mon-join.sh`, tested
Docker-free in `ceph/mon_require_store_test.go`. The container side sets the
selector for every generation after the first (`monitorGenerations.Create`).

- Selector: the environment variable `CEPH_MON_STORE_MODE`. Unset runs mkfs
  when `store.db` is absent. The value
  `require` selects the mode. Any other value, also an empty one, exits 2
  before any command or write, so a misspelled mode cannot fall back to
  formatting a store.
- Behavior of `require`: the check runs before the scripts create any
  directory or run any tool. A missing or non-directory data directory, a
  missing, symlinked or empty `store.db`, a `kv_backend` that is not a plain
  file reading `rocksdb`, and a `keyring` that is not a plain file exit 3 with
  nothing written. A shaped store skips the mkfs branch and goes to
  `exec ceph-mon -f`. The mkfs branch is unreachable in this mode.
- Ceph facts, at the pinned commit: `ceph-mon --mkfs` exits 0 without
  formatting when the data directory holds anything but `kv_backend`, and
  `ceph-mon` without mkfs exits only for a missing or empty directory
  (`check_mon_data_exists`, `check_mon_data_empty`), so the entrypoint is the
  only place that decides between format and reuse. `MonitorDBStore::open`
  treats an absent or empty `kv_backend` as RocksDB and writes it, and
  `Monitor::init` rebuilds a missing `keyring` from the store, so neither file
  is required, only checked when present.
- What it does not establish: the checks detect foreign or half-provisioned
  content by shape. They cannot tell a complete store from one whose mkfs was
  interrupted after `store.db` was created, or a damaged one; `ceph-mon`
  still opens and validates the store. The default fixture
  `TestAddressFamilyBootstrapRenderingGolden` pins the rendering of the default
  mode.
- The `require_store` function (block `mon-store-mode`) and the address
  helpers `messenger_address` and `mon_entries` (block `mon-address`) are
  duplicated in both scripts between marker comments because each script runs
  alone in the container; `TestMonSharedBlocksAreIdentical` keeps the copies
  of each block equal.

## Replacement path

Implemented in `ceph/internal/monstore/generation.go`, tested Docker-free in
`ceph/internal/monstore/generation_test.go` against a small fake of the container side
(`monstore.Generations`). `startMonitorStore` calls `StartGeneration` for the
first generation of a store, and `replaceMonitor` calls `ReplaceGeneration`.

- `StartGeneration`: the first generation formats a new store, a later one
  requires it. The pending attachment is published before the container is
  created, and an ID returned with an error is recorded. An unknown outcome is
  never replayed. A first generation that reaches its ready check marks the
  store ready (`ready`).
- `StopGeneration`: records the removal request on the attachment
  (`RemoveRequested`), removes the container and clears the attachment only
  after `confirmGone`. An attachment with an unknown creation outcome is left
  to `Terminate`. A caller that finds the old attachment after a failure tells
  from `RemoveRequested` whether the old container may already be stopped or
  gone.
- `ReplaceGeneration`: checks the next generation first (a ready generation
  exists, it has a recorded address, the volume is still the one this record
  created), then stops the old container, then starts the next one from the
  new image on the same volume. A failure of the new container leaves the
  volume and the record, and never formats.
- `dispatch`: a bound store is retained by its volume and the require-store
  mode. The volume metadata is validated before every generation, and a
  generation that finds no store fails instead of formatting.
- The Docker-free tests establish the order of the steps and the refusals.
  Real containers, the start of a new image on the old store and the rejoin
  are the native evidence of [Production wiring](#production-wiring).

## Address handoff

Implemented in `ceph/internal/monstore/address.go` and `StartGeneration`, called
through the generation path and tested Docker-free against the fake of the container side. The monmap pins the
address a MON joined with, so the replacement container binds exactly that
address, and nothing may take it between the removal of the old container and
the start of the new one.

- The record keeps the endpoint its first generation bound (`monstore.Endpoint`:
  the IP, and either the bridge network name or the host ports), read from the
  running container after the ready check (`Observe`). If that observation
  fails the store is ready but cannot be replaced, and `ReplaceGeneration`
  refuses before it touches the old container.
- A later generation is created, not started, for a copy of that endpoint. After
  its ready check the bound endpoint is observed and must equal the recorded one.
- In host mode no reservation can last for the MON's lifetime: the running MON
  owns its ports, and the old container has to be gone before anything can bind
  them. So the cluster takes an exact hold after `StopGeneration` confirmed the
  old container gone, keeps it while the new container is created, and releases
  it immediately before `Start`. The window between the release and the start is
  the one the first MON has in `runMonitor`. It fails closed: a taken port makes
  the hold or the start fail, the volume and the record stay, and nothing
  formats.
- In bridge mode the container is created with a static IP assignment, every
  container of the cluster is created under `Container.mu`, and a taken IP fails
  the creation. The container side's `Hold` only verifies that the address is
  free.
- The record owns every hold the container side returns, also one that comes
  with an error (a partial allocator). It is released at once, and kept on the
  record if that release fails. A typed nil hold is refused.
- A retained hold is released by the next `StartGeneration` before a new one is
  taken, so there is at most one. It is also released first by
  `monstore.Volume.release`, which `ReleaseStores`, `RetryStores` and
  `discardFresh` call before the attachment or the volume are looked at. A
  failed release makes the release fail, so the record, the volume and the hold
  stay owned and the next stage retries.
- Container side (`monstore.Generations`): `Create` makes the container without
  starting it, `Start`, `Observe` and `Hold` complete the port.
- Guards: the generation and address symbols are used in production only by
  the pinned callers (`StartGeneration`, and `StopGeneration` for a first
  generation that lost its host ports, from `ceph/mon_store_container.go`;
  `ReplaceGeneration` from `ceph/mon_store_replace.go`) and
  `releaseAddressHold` from `monstore.Volume.release`, the files import no `sync`,
  the container side is called only from its pinned methods, a hold is released
  only by `releaseAddressHold`, `StartGeneration` holds, dispatches, creates,
  releases, starts, waits and observes in that order, and `release` frees the
  hold before it looks at the attachment or the volume.
- Host mode reserves the exact ports with the allocator container of the
  first MON (`reserveExactHostPorts`); bridge mode uses the static-IP creation
  of `monitorGenerations.Create`.

## Production wiring

| Caller | Primitive | Container side |
| --- | --- | --- |
| `Run` (`runMonitor`, MON a) | `begin`, `create`, `StartGeneration` (format) | `mon.sh` recipe of `firstMonitorOptions` |
| `AddMonitor` and the initial MONs (`startJoiningMonitor`) | `begin`, `create`, `StartGeneration` (format) | `mon-join.sh` recipe of `monitorJoinOptions` |
| `MonitorContainer.Replace` (`replaceMonitor`) | `ReplaceGeneration`: checks, `StopGeneration`, `StartGeneration` (require) | `mon-join.sh` recipe with fresh join material, for MON a too |
| `MonitorContainer.Remove` (`retireMonitor`) | `retire`: release once nothing mounts the volume, then forget the record | none |
| `Terminate` | `ReleaseStores`, `RetryStores` | none |

`monitorGenerations` (`ceph/mon_store_container.go`, with the host half in
`ceph/mon_store_host.go`) is the container side for the two cases `Run` admits
with the volume: bridge networking with IPv4 addresses, and the host network.
On a bridge:

- `Create` mounts the volume with no copy at `/var/lib/ceph/mon/ceph-<name>`,
  sets `CEPH_MON_STORE_MODE=require` for a later generation, asks the bridge
  for the recorded IPv4 address, and creates the container without starting
  it. It first runs a preparation hook; the replacement clears the admin
  socket the old daemon left in the shared run directory there.
- `Observe` reads the container's IPv4 address on the cluster's public network.
- `Hold` only checks that no container of the network has the address: every
  container of the cluster is created under `Container.mu`, and a taken
  address fails the creation.
- `Remove` force-removes the container and never its volumes.

On the host network the endpoint is the public address and the MON's ports:

- `Create` of a first generation reserves ports the kernel chooses with the
  allocator container, publishes the reservation for `Terminate`, creates the
  container for them and releases them before it returns, right before the
  start. A first generation whose MON lost a port in that window (the daemon
  exits with an address-in-use error) is removed, its fresh store discarded and
  the MON started again on a new volume and new ports, up to `hostPortAttempts`
  times (`startMonitorStore`).
- `Create` of a later generation uses the recorded ports, which `Hold` holds.
- `Hold` reserves exactly the recorded ports with the allocator container. A
  port that any process listens on fails the hold before anything is created;
  a port with connections of the old MON in `TIME_WAIT` is taken
  (`SO_REUSEADDR`). The reservation is published for `Terminate` as well.
- `Observe` reads, in the running container, the listening TCP sockets of its
  `ceph-mon` process from `/proc`: they must be exactly the created ports on
  the public address.
- A later generation that loses a recorded port between the release and the
  start fails closed: the monmap pins the port, so no other port is tried, and
  the store stays retained for another `Replace`.

`replaceMonitor` refuses, before it touches the MON, a replacement that would
leave fewer than a majority of the monmap in quorum, a store that is not
attached to the handle's container, and a store without a recorded address
(the last inside `ReplaceGeneration`). A later generation of MON a also uses
the join recipe, with the live mon. key and monmap, so it needs no bootstrap
environment. Once the old container is gone, MON a is marked terminated and the
new generation is kept in the roster under the name `a`, so listing, handle
checks, `Remove` and `Terminate` treat it like an added MON.

A permanent removal retires the store after the membership and the container
are gone: the record is forgotten only when the volume removal succeeded, and a
failure keeps the handle and the record, so `Remove` can be retried. A MON added
later under the same name gets a new store; a removed member's store is never
reused.

Not wired: address families other than IPv4, which `Run` refuses with the
volume.
