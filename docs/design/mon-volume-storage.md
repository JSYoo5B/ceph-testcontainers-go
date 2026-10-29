# Cluster-owned MON data volumes (API proposal)

This note records the API boundary and the lifecycle for the owner's option 1c
decision on #64 (2026-10-28): optional MON data-volume support in the ordinary
`ceph` fixture, covering the initial MON and `AddMonitor`, with the source and
the replacement MON generation mounting the same cluster-owned store. The note
itself added no code, no public name and no native run; the status of the
internal slice 2 that followed is at the end of the slice plan. Every name and
signature below is a proposal for the owner to accept or change; none is frozen, and the API
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

1. This note (merged).
2. Internal ownership primitive: the unexported record, state machine, volume
   validation, release stages and guards, Docker-free, with negative controls.
   Landed in the slice 2 change; its status is below.
3. `WithMonitorVolumeStorage`, the mount in `runMonitor` and `AddMonitor`,
   `StoreVolume`, `Remove` discard and `Terminate` release, and catalogue.
4. Address retention and exact port lease (own decision).
5. `Detach`, `Reopen` and the initial-MON special case.
6. A same-release native retention batch, only after the owner authorizes native
   runs. Upgrade evidence is a later, separate step.

### Slice 2 status

Implemented in `ceph/mon_store_volume.go`, `ceph/mon_store_identity.go` and
`ceph/mon_store_state.go`, with Docker-free tests. Nothing is public and no
production path publishes a record, so a cluster holds no store and default
behavior is unchanged. 0.x: no old name is renamed or removed, and no alias is
added.

What the slice follows from this note: the phases planned, created, fresh,
bound, detached and released; one volume per MON (D1, followed because the
owner has not answered it); no persistent helper (D6, followed for the same
reason: the record carries no helper and the volume-user check is an
independent container list); publish before create, also for the attachment;
an unknown volume or container creation outcome is never replayed and is
settled only by `Terminate` after its labeled-container scan; a bound store
whose volume is missing, changed or foreign fails instead of being recreated;
a retained generation is dispatched only after its identity was verified; the
volume is never removed with force; `Terminate` releases the stores after the
added and primary MON containers and again after the labeled-container scan.
The identity inputs (keyring digest, shape, fingerprint, FSID, address
vector) are validated and compared but not observed: nothing reads a store or a
monmap yet (the observation ports landed afterwards, see below).

Not in the slice: the require-store mode of `mon.sh` and `mon-join.sh` (it
landed afterwards, see the status below); the inspector container (D6) that
would observe the identity (its ports and the identity computation landed
afterwards, see the identity observation status below); every public name; the option,
the mounts, `StoreVolume`, `Remove` discard, `Detach`, `Reopen`; address and
port-lease handoff; any native run. The `fresh` to `bound` transition and the
discard of a fresh store are exercised only by the unit tests, since no
production caller starts a MON generation yet.

The mutex count is unchanged: `monStoreState` has no lock and sits at the position
of `osdStorageState` under `Container.mu` (`TestNativeStateAddsNoMutex` scans it).

### Require-store mode status

Implemented in `ceph/internal/scripts/mon.sh` and `mon-join.sh`, tested
Docker-free in `ceph/mon_require_store_test.go`. It has no Go caller: nothing
sets the selector yet, no public name exists and no native run is claimed. 0.x:
no old name is renamed or removed, and no alias is added.

- Selector: the environment variable `CEPH_MON_STORE_MODE`. Unset keeps the
  historical behavior unchanged (mkfs when `store.db` is absent). The value
  `require` selects the mode. Any other value, also an empty one, exits 2
  before any command or write, so a misspelled mode cannot fall back to
  formatting a store. The Go side that sets it comes with the slice that
  mounts the volume.
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
  still opens and validates the store, and the identity binding of this note
  (fingerprint, FSID, monmap) is a later slice. The default fixture
  `TestAddressFamilyBootstrapRenderingGolden` still pins the unchanged
  rendering of the default mode.
- The `require_store` function is duplicated in both scripts between marker
  comments because each script runs alone in the container;
  `TestMonRequireStoreBlocksAreIdentical` keeps the two copies equal.

### Identity observation status

Implemented in `ceph/mon_store_inspect.go`, tested Docker-free in
`ceph/mon_store_inspect_test.go`. Nothing calls it and nothing implements its
ports in production: no inspector container, no monmap adapter, no public name,
no native run. 0.x: no old name is renamed or removed, and no alias is added.

- Ports: `monStoreInspector` (D6 recommendation, a short-lived read-only
  inspector; the owner has not answered D6, so this is the recommended shape,
  not a decision) reports the entries of the data directory (kind, mode, inode,
  size, modification time, SHA-256 of regular files) and the `kv_backend`
  marker. `monStoreMonmap` reports one member: name, FSID, address vector and
  quorum. A nil inspector, monmap view or typed nil is refused.
- Computation: `storeFacts` derives the keyring digest, the shape and the
  fingerprint from a listing; `observeStopped` adds the FSID and address vector
  of the live monmap entry. The fingerprint hashes the entries in path order with
  length-delimited fields under a versioned domain line, and a golden value
  computed by an independent script pins the encoding.
- Refusals, all before any write: a member that is absent from the map, named
  differently, in quorum or of unknown quorum state (the inspector is not
  called); a listing with an unclean or absolute path, a duplicate, an orphan
  entry, a symlink or other non-file kind, a file without a content digest, a
  directory with one, a missing or empty `store.db` directory, a missing
  `kv_backend` or one that was not read or does not hold `rocksdb`, or a missing
  or non-regular `keyring`; an FSID that is not a canonical UUID; an empty,
  empty-entry or duplicate address vector. Absent evidence is never read as an
  empty value that matches.
- Stricter than the entrypoint on purpose: `require_store` accepts an absent
  `kv_backend` or `keyring` because `ceph-mon` recreates them. A binding
  refuses both, since `MonitorDBStore::open` writes `kv_backend` whenever a store
  is opened and the keyring digest is one of the bound fields.
- `monStoreVolume.verifyStopped` runs the preflight: the record must hold a
  retained identity with no attachment (so the inspector never reads a store a
  MON may be writing), the volume metadata must pass `validate`, then the
  observation is compared through `verify`. A mismatch error names the fields
  (keyring digest, store shape, store fingerprint, cluster FSID, address vector)
  and prints no value. A failure at any step leaves the record unverified.
- What it does not establish: that an inspector can mount the volume read-only
  while the cluster's labels, ordering and release rules hold; how the monmap
  adapter decodes `mon dump` and the quorum status (its missing or null fields
  must map to an absent value here); an identity at bind time, when the member
  runs and the store is being written (this slice observes a stopped member
  only; the bind-time source is part of the inspector slice); whether
  `ceph-mon`, in particular a different release, accepts the store.
- Open decisions it leans on: D6 (inspector, recommended) and D1 (one volume per
  MON, recommended). Both stay recommendations until the owner answers; the
  ports take a volume name and a MON name only, so neither answer changes their
  shape.

### Identity adapters status

Implemented in `ceph/mon_store_monmap.go`, `ceph/mon_store_inspect_container.go`
and `ceph/mon_store_json.go`, tested Docker-free. They are the concrete
adapters behind the two ports above. Nothing calls them, no Docker or moby
call exists in them, no public name was added, and no native run was made.
0.x: no old name is renamed or removed, and no alias is added.

- Monmap adapter. `monStoreMonmapCLI` runs `ceph mon dump --format json` and
  then `ceph quorum_status --format json` through a `monStoreCLI` function (stdout
  only; the CLI prints `dumped monmap epoch N` on stderr). Shapes were read at the
  pinned commit 7f793731f1b39eb4f465e960113d2363c311b964: `MonMap::dump`,
  `MonmapMonitor` (`quorum` is an array of ranks), `Monitor::_quorum_status`
  (`quorum` ranks, `quorum_names`, `quorum_leader_name`, `quorum_age`), and
  `entity_addrvec_t::dump`. The recorded 20.2.4 `mon dump` fixture of the
  daemon-address tests is the real input of the decoder tests; no recorded
  `quorum_status` exists, so that document is synthetic.
- Strictness. Exactly one JSON value, no duplicate key at any depth, no
  trailing data. A required field that is missing, null or of the wrong type is
  an error: epoch (at least 1), canonical lowercase FSID, a non-empty `mons`
  array in rank order with unique non-empty names, a non-empty address vector per
  member whose entries have type `v1` or `v2`, an IP and a port in 1..65535 that
  is not the unspecified address, and a 32-bit nonce, no repeated address. An
  empty `quorum` is an error too, since only a member of a quorum answers.
  Paired missing, null, wrong-type and empty controls run against each parser
  alone and through the decoder, for both documents.
- Three-state quorum. The two documents must agree on the epoch, FSID, member
  names, every address vector and the quorum set, and `quorum_status` must agree
  with itself (ranks and names, leader in the quorum). Only then is the member
  in quorum or out of quorum. Any disagreement, for example an election between
  the two commands, is an error, never the zero (unknown) state and never
  "out". A name that is not in the map yields a nil member with a nil error, as
  the port documents. The address strings are `type:addr/nonce` as Ceph prints
  them (`v2:172.20.0.4:3300/0`).
- Inspector request. `monStoreInspectRequest` is plain data, so it can be
  validated without Docker: the volume at `/store` read-only, network mode
  `none`, all capabilities dropped, no new privileges, a read-only root, not
  privileged, no engine-side auto-remove, removal that never takes a volume, the
  cluster owned label (so the `Terminate` scan finds a leaked inspector) and a
  per-inspection token label. The command is `python3 -I -c <script> /store`
  from the image given by the caller. The user must be the numeric uid:gid of the
  store owner: with every capability dropped, root cannot read a `0600` keyring
  of another owner. Which uid:gid that is for the retained image, and whether the
  chosen image carries `python3`, are native questions.
- Listing script and format. The embedded script classifies every entry from
  `lstat`, never follows a symlink, opens regular files with `O_NOFOLLOW` and
  requires the opened file to be the one `lstat` saw, treats a directory on
  another device as `other`, refuses a name that is not UTF-8, and refuses more
  than 100000 entries. It prints a format line, one JSON object per entry sorted
  by path (path, kind, permission bits, inode, size, `mtime_ns`, SHA-256 of
  regular files), and a trailer with the entry count and the `kv_backend`
  content (`null` when it was not read). The marker is read completely or
  refused: the script reads to EOF and fails when the file is larger than 4096
  bytes (the bound of the pinned `MonitorDBStore` reader, which strips only
  trailing whitespace) or is not UTF-8, so a suffix behind trailing whitespace
  cannot be dropped and listed as `rocksdb`. The only other reads, the content
  digests, loop to EOF. It exits 0 only after the trailer. The
  tests execute it with the host `python3` against temporary directories.
  Symlinks (to an outside file or directory, relative escapes, dangling, and a
  keyring, `kv_backend` or `store.db` replaced by a link) and a FIFO appear as
  entries that `storeFacts` refuses; nothing behind a link is listed or hashed.
- Parser. `parseMonStoreListing` requires the final newline, valid UTF-8,
  the format line, entries in strictly ascending path order, exactly the
  expected fields on every line, a known kind, no control or replacement
  character in a path, permission bits only, and a trailer whose count equals
  the number of entries. A listing without its trailer is a truncated listing
  and is refused. A `null` marker yields an unread marker, which `storeFacts`
  refuses, as it refuses an empty one.
- Driver. `monStoreContainerInspector.Inspect` validates the request, then
  calls an engine (`Create`, `Start`, `Wait`, `Logs`, `Remove`) at most once
  each. A failed `Create`, `Start` or `Wait` is an unknown outcome: it is not
  retried and nothing is read afterwards. After `Create` returned an ID the
  container is removed on every outcome, under a context that does not end with
  the caller's. A removal failure withholds the listing and is joined with the
  first failure, so the two stay separate. Output of more than 16 MiB, a
  truncated stream, a non-zero exit or anything on stderr is refused. A
  creation that returned an error leaves no ID to remove; the labelled
  container is left to the `Terminate` scan.
- Guards. `ceph/mon_store_adapters_test.go` pins that no production file
  outside the adapter files mentions the adapter symbols, that the symbol table
  matches the declarations, that the adapters import no `sync`, moby client or
  testcontainers package, and that the guard detects a synthetic caller. The
  port guard of `mon_store_inspect_test.go` now exempts the adapter files.
- Known gap of the script. A regular file swapped for a symlink between the
  `lstat` and the `open` is refused by `O_NOFOLLOW` and the inode comparison,
  but no Docker-free test can force that race, so the mutation that removes
  `O_NOFOLLOW` survives the tests. The mount is read-only and the inspector
  runs only against a stopped store, which is the real protection.

Engine translation status. `ceph/mon_store_inspect_engine.go` implements the
addendum below as `monStoreMobyEngine`, tested Docker-free in
`ceph/mon_store_inspect_engine_test.go` with a small fake of the Docker client.
Nothing constructs it: it has no default client, so the first caller supplies
the constructor, and the production-caller guards cover its symbols. It holds no
lock and no state, so the lock order is unchanged.

- `Create` runs `validate` first and then makes one create call: the numeric
  user, the entrypoint and command, the two labels, a named-volume mount with
  `ReadOnly` and `NoCopy`, `NetworkMode: none`, all capabilities dropped,
  `no-new-privileges`, a read-only root file system, no terminal, no
  auto-remove. Any error is an unknown outcome and is returned without a retry
  or a removal by guessed name.
- `Wait` returns the exit code only from the wait reply. A context end, a failed
  wait request, a reply that carries an error and a closed channel are errors.
- `Logs` demultiplexes the stream through a fixed buffer (it never allocates
  from an advertised frame size), keeps at most the limit per stream and reports
  truncation instead of returning a prefix. A stream that ends inside a frame,
  a frame of another stream and a context end are errors.
- `Remove` is forced, passes neither volume nor link removal, and treats a
  missing container as success. A failed removal keeps the container, which
  still carries the owned label for the `Terminate` scan.

What this establishes: the translation, the error and truncation handling and
the cleanup ordering against a fake client. What it does not: that a daemon
accepts these create options, that the image carries `python3` and runs as the
given owner, how a daemon ends the log stream, that the read-only named-volume
mount rejects a write natively, and that the `Terminate` scan (which removes
with `RemoveVolumes`) leaves a named store volume alone. No native run exists.

Design addendum for the engine, the part that this slice does not implement and
that only a native run settles:

1. A moby-backed `monStoreInspectEngine` translates `monStoreInspectRequest` one
   to one into the create call (user, entrypoint, command, labels, a read-only
   mount of the named volume, `NetworkMode: none`, `CapDrop: ALL`,
   `no-new-privileges`, read-only root file system, no auto-remove). Its
   `validate` runs on the request first.
2. `Wait` must return the exit code of the stopped container and must not treat a
   context end as an exit. `Logs` must demultiplex stdout and stderr, stop
   reading at the limit and report truncation instead of returning a prefix as a
   listing. `Remove` must be forced, must not pass `RemoveVolumes`, and must treat
   a missing container as success.
3. The image must be a cluster image that carries `python3` and runs as the
   numeric owner given. The uid:gid is read from the retained store at bind time
   or taken from the image's `ceph` user; which of the two is correct is a native
   question and must not be guessed in code.
4. The inspector is a volume user while it exists. The attachment bookkeeping
   (`requireRetained`, `inspect`) already refuses an inspection while a MON
   generation is attached; the caller slice must also keep a second `Reopen` or
   `Detach` from starting while an inspection runs, under the existing
   `Container.mu`, with no new mutex.
5. Bind time (a running member) is not covered: `observeStopped` refuses a member
   in quorum, and the identity at the first ready check needs its own source.

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
