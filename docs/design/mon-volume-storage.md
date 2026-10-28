# Cluster-owned MON data volumes (API proposal)

This note records the API boundary and the lifecycle for the owner's option 1c
decision on #64 (2026-10-28): optional MON data-volume support in the ordinary
`ceph` fixture, covering the initial MON and `AddMonitor`, with the source and
the replacement MON generation mounting the same cluster-owned store. It adds no
code, no public name and no native run. Every name and signature below is a
proposal for the owner to accept or change; none is frozen, and the API
catalogue is untouched until a later slice implements an accepted shape.

What it does not decide: mixed-version compatibility, the Squid to Tentacle
upgrade, MGR or OSD steps, address preservation and host-port lease handoff
(separate decisions, with a seam named below), and any native acceptance. The
rejected copy-based retention stays rejected. The merged per-daemon image
override (`WithDaemonImage`) is reused as it is; no cluster-wide image setting
is mutated.

Pinned source: Ceph 7f793731f1b39eb4f465e960113d2363c311b964 (Tentacle 20.2.4).
Repository references are to main at the time of writing.

## Facts the design rests on

| Fact | Source | Consequence |
| --- | --- | --- |
| The first MON runs `mon.sh`; an added MON runs `mon-join.sh`. Both run mkfs only when `<data>/store.db` is absent, otherwise they `exec ceph-mon -f`. | `ceph/internal/scripts/mon.sh`, `mon-join.sh` | A fresh empty volume mounted where a retained store was required would silently format a new store and join as a new member. The retained launch needs a mode that fails closed when `store.db` is missing. |
| Neither MON path mounts a data volume. The only mount is the run volume. | `runMonitor` in `ceph/ceph.go`, `AddMonitor` in `ceph/topology.go` | The store lives in the container's writable layer today; a new container starts empty. |
| `ceph-mon --mkfs` refuses a data directory that holds anything except `kv_backend`; `ceph-mon` without mkfs exits when the directory is missing or empty. | `ceph_mon.cc` (`check_mon_data_empty`, `check_mon_data_exists`) | A half-formatted store is detectable by shape, and a failed first start cannot be retried on the same bytes without inspection. |
| `Monitor::mkfs` writes the cluster UUID, the seed monmap and the mon. key into the store, and moves the mon. key into `<data>/keyring`. | `Monitor.cc` (`mkfs`, `write_fsid`, `extract_save_mon_key`) | The identity inside `store.db` (`cluster_uuid`, monmap) is not readable with plain file operations. Offline file checks can bind the `keyring` bytes and the store shape; fsid and rank binding come from the live monmap while the member is down. Whether an image tool can read `store.db` offline is not established. |
| The first MON is the embedded `testcontainers.Container` of `*ceph.Container`. `Terminate` on it is cluster-wide, and `RemoveMonitor("a")` only sets `monitorTerminated`. | `ceph/ceph.go`, `ceph/handles.go` | The initial MON cannot be replaced by swapping a container into that field without a data race on a field users read without a lock. |
| OSD storage is one cluster-owned volume with a persistent helper, per-ID directories, publish-before-create, owner labels, a creation timestamp check, non-forced release and a second release stage after the label scan. | `ceph/osd_volume_storage.go`, `ceph/osd_storage_state.go`, `ceph/osd_storage_release.go`, [OSD volume storage](osd-volume-storage.md) | The ownership vocabulary is reused. The topology (one volume, one helper) is not, see decisions D1 and D6. |

## Proposed public API

All of it is package `ceph`. Nothing below exists yet.

```go
// Option: the cluster owns one ordinary Docker local named volume per MON.
func WithMonitorVolumeStorage() Option

// Handle information.
func (m *MonitorContainer) StoreVolume() string // "" without the option

// Retained lifecycle (a later slice, after the address decision).
func (m *MonitorContainer) Detach(ctx context.Context) (*DetachedMonitor, error)
func (d *DetachedMonitor) Reopen(ctx context.Context, opts ...DaemonOption) (*MonitorContainer, error)
func (d *DetachedMonitor) Name() string
func (d *DetachedMonitor) StoreVolume() string
func (d *DetachedMonitor) ClusterFSID() string
func (d *DetachedMonitor) Image() string
```

Properties of the proposal:

- `WithMonitorVolumeStorage` is cluster-wide, so every MON (the initial one and
  each `AddMonitor`) gets a store volume. There is no per-MON opt-out, which
  keeps "which MONs are retained" a single fact (D2). Unlike the OSD option it has
  no tmpfs or MemStore conflict to refuse. Admission happens in `Run` before any
  Docker allocation.
- `MonitorContainer.StoreVolume` reports the Docker volume name so that an
  independent observer can inspect or mount it. It does not grant ownership.
- `DetachedMonitor` mirrors `DetachedOSD`: the store and the native membership
  stay, the container is gone, and the handle is consumed by the first
  `Reopen` that dispatches a creation request.
- `Reopen` takes the merged `DaemonOption`s. The default image is the one the
  MON ran from (the same rule as `DetachedOSD.Reopen`); `WithDaemonImage` selects
  the replacement's image for that one generation (D4). It never touches
  cluster settings or other daemons.
- `Detach` and `Reopen` need the address seam below to be implemented before
  they are published. The earlier slices do not depend on it.
- Old names: nothing is renamed or removed. `MonitorContainer.Remove` and
  `AddMonitor` keep their meanings (permanent removal and fresh join).
- A cluster that a `SharedCluster` shares refuses `Detach`, `Reopen` and the
  option, as it refuses `WithDaemonImage` and `DetachedOSD.Reopen`.

## Ownership model

One record per MON name, owned by the cluster, kept in a new internal state
component beside `osdStorageState` and `memberRoster`. Like those it has no
mutex: `Container.mu` guards it, and `Run` writes it only before the cluster is
shared. One volume per MON, not one volume with per-name directories, because a
MON generation must be retained, mounted and released independently of its
siblings (D1). The volume is mounted with no-copy at the MON's data directory,
`/var/lib/ceph/mon/ceph-<name>`, so the volume root is the data directory.

Record fields: MON name; volume name derived from a UUID owner token; owner
token; Docker creation timestamp; state; generation counter; the current
attachment (empty, or the container of that generation with its Docker ID and
whether its creation outcome is known); the store identity recorded when the
store became bound (keyring digest, store shape, fingerprint, served cluster
FSID, address vector from the live monmap).

Volume metadata checks are those of the OSD volume: exact name, `local` driver,
owner label equal to the token, a MON-name label, no driver options, creation
timestamp unchanged. A volume that fails them is never removed or mounted.

### Store states

| State | Meaning | Store may be discarded | Store may be mounted by a new generation |
| --- | --- | --- | --- |
| planned | Record published, create not yet requested | yes | no |
| created | Volume exists and passed the metadata checks | yes | first generation only |
| fresh | A first generation was requested; mkfs may have run | yes, only after the attachment is confirmed gone | no |
| bound | The first generation reached its ready check and the identity was recorded | only by `MonitorContainer.Remove` after native removal, or by `Terminate` | only through `Reopen` with a matching identity |
| detached | `bound`, no attachment | `Terminate` only | `Reopen` only |
| released | Volume removal confirmed | n/a | no |

Rules:

1. Publish before create: the record, with its generated name, exists before
   the create call, so a lost reply stays owned. A later call never allocates a
   second volume to resolve the uncertainty.
2. A store is `fresh` until its first generation passes the same readiness check
   the MON has today (admin socket for an added MON, `ceph status` for the first).
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

## Operations and lock order

Every operation runs under the topology lock `Container.mu` (taken by
`lockTopology`) with a bounded context, as the OSD operations do. No new mutex
is added: the state component sits at the position of `osdStorageState` in the
existing order (shared-gate admission, topology lock, then the config, control
and leaf locks), calls no method that takes a lock above it, and Docker calls
are made with `Container.mu` held, exactly as `registerOSD` does for the OSD
volume. The guards that pin this for the OSD state (no field touched outside the
type, callers hold `Container.mu`, no production writer of a test seam, release
stages keep their order, no mutex added) are copied for the new component in the
slice that adds it.

| Operation | Order of effects | Failure rule |
| --- | --- | --- |
| Initial MON | admission, `runDir.create`, store record published, volume created and validated, `runMonitor` with the mount, ready check, store bound | Record stays published on every error. A returned partial first MON stays the cluster's container; `Terminate` removes it, then the store. |
| `AddMonitor` | existing name and quorum preflight, key and monmap reads, then record published and volume created, roster entry added, container created, ready and quorum check, store bound | The roster entry and the record are published before the container, as today. A failed attempt leaves the descriptor owned. Host-port retry discards the `fresh` store of the failed attempt after its container is confirmed gone. |
| `MonitorContainer.Remove` | existing quorum check, native `mon remove`, container removal, then discard the store | A store that cannot be discarded keeps the descriptor and refuses a new MON of that name until the retry succeeds. Sibling stores are untouched. |
| `Detach` | admit; refuse a held network or pause fault on this instance; refuse when stopping this MON would leave no quorum majority of the map; fence native commands to the instance; stop; wait until native quorum no longer lists it; remove the container; confirm no volume user; read the store identity; record `detached` | Same retry shape as `OSDContainer.Detach`: a failure after the fence keeps the MON owned with the fence, and a retry resumes after a completed removal. Native membership is not changed. |
| `Reopen` | read-only preflight (store identity equals the detach record, served FSID equals, monmap member present with the retained address vector and out of quorum, no volume user); consume the detached handle; publish the new descriptor; create the container with the same volume and the require-store mode | A preflight refusal leaves the MON detached. Once creation is requested the handle is consumed; an error returns the new owned descriptor. No start ever formats a store. |
| `Terminate` | existing order, then new stage 1 after the added and primary MON containers are terminated: release every store whose attachment is gone; then, after the labelled-container scan, stage 2 retries the stores whose release failed | Never forced. A store with a live or unknown attachment, a changed or foreign volume, or a busy removal stays owned and is reported. A confirmed missing volume settles cleanup. |

Release ordering in `Terminate`, in full: faults undone, port leases released,
services, OSDs and the OSD storage, MGRs, added MONs, the primary MON, control
plane and toolkit containers, labelled-container scan, MON store release stage 2
together with the OSD retry stages, run volume, networks. The MON stage 1
placement (after the primary MON) is needed because the primary MON and the
added MONs are the volume users. A detached store has no user and is released
in stage 1.

## Identity binding

A store is bound to (cluster FSID, MON name, owner token, creation timestamp,
keyring digest, store shape). Digests are of secrets and are not logged or
returned; only equality is reported. The preflight of `Reopen` compares, without
writing:

1. Docker volume metadata as above.
2. Through a short-lived inspector container (D6) mounted read-only: `store.db`
   is a directory, `kv_backend` reads `rocksdb`, `keyring` is a regular non-symlink
   file whose SHA-256 equals the recorded one, and the fingerprint (mode, inode,
   size and modification time of every entry, SHA-256 of every regular file)
   equals the one taken at detach.
3. Live monmap: the name is a member, its address vector equals the retained
   vector, and it is not in quorum while detached.

A mismatch refuses before any write and leaves the MON detached. The fingerprint
is a change detector for the fixture's own store, not a statement that
`ceph-mon` accepts it. Whether the destination binary reads the source store is
a native question outside this note.

## The initial-MON special case

The first MON is `Container.Container`. Recommendation (D3): treat that
embedded container as the first generation only and never rebind it.
`Detach` of `a` removes its container, sets the existing `monitorTerminated`
state, and keeps the store and membership. `Reopen` publishes the replacement
as the roster entry for `a`; the promoted container methods of `*ceph.Container`
keep naming the first generation, as they already name the removed container
after `RemoveMonitor("a")`. Internal code reaches the CLI through `cliContainer`,
which prefers the independent control plane once one exists and otherwise uses
the embedded container, so the CLI path needs the control plane to be started
before the embedded MON is detached (to be settled in that slice). The sites that special-case the name `a` (`AddMonitor`'s
"primary monitor name a cannot be reused", `currentMonitor`, `monitorsLocked`,
`refreshMonitorConfigAfterRemoval`) change in the Detach/Reopen slice, not
before. The alternative, rebinding the embedded field under `Container.mu`,
adds a data race with lock-free readers and is not recommended.

## Seams with the separate decisions

- Address preservation: the retained monmap pins the MON's address vector, so
  `DetachedMonitor` carries it and `Reopen` rejects a missing or different
  reservation before launch. How the destination obtains the same endpoint is
  the host-network and exact port-lease slice (host first, bridge after). It is
  not designed here. Until it exists, `Detach` and `Reopen` are not published,
  and the store option stands alone.
- Per-daemon image: reused as merged. Version identity of the replacement is its
  own native report, never the cluster's recorded `CephVersion`.

## Failure paths

| Failure | Result |
| --- | --- |
| Create reply lost | Record stays; the next operation neither allocates nor removes; `Terminate` retries release after the label scan. |
| Volume metadata changed or foreign | Operation fails; the volume is never mounted or removed. |
| First start fails before ready | Container confirmed gone, then the `fresh` store is discarded; the descriptor stays owned for retry or `Terminate`. |
| Volume missing for a `bound` store | Start refused; no fresh store; descriptor owned. |
| Container creation reply lost during Reopen | Handle consumed, descriptor owned without a container, attachment pending-unknown; only `Terminate` recovers it. |
| Release busy | Store stays owned and is retried in stage 2; never forced. |
| Detach fails after the fence | MON stays owned and fenced; retry resumes after a completed removal. |
| Context cancelled | No state moves backward; unconfirmed work stays recorded as pending. |

## Slice plan

1. This note.
2. Internal ownership primitive: the unexported record, state machine, volume
   validation, release stages and guards, Docker-free, with negative controls,
   and the require-store mode of the two scripts. No option yet.
3. `WithMonitorVolumeStorage`, the mount in `runMonitor` and `AddMonitor`,
   `StoreVolume`, `Remove` discard and `Terminate` release, catalogue and
   release-note fragment.
4. Address retention and exact port lease (own decision).
5. `Detach`, `Reopen` and the initial-MON special case.
6. A same-release native retention batch, only after the owner authorizes native
   runs. Upgrade evidence is a later, separate step.

The primitive is not part of this slice. Its shape depends on D1 (one volume per
MON or one shared volume) and D6 (helper or inspector), which the owner has not
answered, and an unexported type with no caller would only be reviewed against a
guess.

## Owner decisions with recommendations

| ID | Question | Recommendation |
| --- | --- | --- |
| D1 | One volume per MON, or one cluster volume with per-name directories like the OSD storage? | One per MON: independent retention, mount and release, and no shared helper. |
| D2 | Cluster-wide `WithMonitorVolumeStorage` or a per-MON `DaemonOption`? | Cluster-wide, uniform for the initial MON and `AddMonitor`. |
| D3 | How is the initial MON handled? | First generation stays the embedded container; the reopened generation is a roster entry; no rebinding. |
| D4 | Does `Reopen` accept `DaemonOption`s (image)? | Yes; default is the image the MON ran from. |
| D5 | May `Detach` stop a MON whose absence loses quorum? | No, same rule as `Remove`; no override in the first public slice. |
| D6 | Persistent helper, as for OSDs, or a short-lived read-only inspector per operation? | Inspector: no helper whose loss refuses later operations. It counts as a volume user while it exists and carries the cluster's owned label. |
| D7 | May a caller-built replacement container be adopted by the cluster? | No. Cluster-owned `Detach` and `Reopen` produce every generation, so the cluster can order removal and release. |
| D8 | Can a detached MON be permanently discarded without reopening? | Not in the first public slice; `Reopen` then `Remove`, or `Terminate`. |
| D9 | Which slice ships `StoreVolume`? | Slice 3, with the option, so independent observers can inspect the store. |
