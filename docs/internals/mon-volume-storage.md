# Cluster-owned MON data volumes

Every MON of a cluster can keep its store in its own cluster-owned Docker volume,
so that a replacement container (possibly from another image) mounts the same
store and rejoins. `WithMonitorStorage(MonitorStorage{}.WithVolume())` selects
it, and `MonitorContainer.Replace` moves a MON to a new container; see
[Production wiring](#production-wiring) for what calls which primitive below.
The identity observation and its adapters are still uncalled.

Pinned source: Ceph 7f793731f1b39eb4f465e960113d2363c311b964 (Tentacle 20.2.4).

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
| Neither MON path mounts a data volume. The only mount is the run volume. | `runMonitor` in `ceph/ceph.go`, `AddMonitor` in `ceph/topology.go` | Without the volume the store lives in the container's writable layer, and a new container starts empty. |
| `ceph-mon --mkfs` refuses a data directory that holds anything except `kv_backend`; `ceph-mon` without mkfs exits when the directory is missing or empty. | `ceph_mon.cc` (`check_mon_data_empty`, `check_mon_data_exists`) | A half-formatted store is detectable by shape, and a failed first start cannot be retried on the same bytes without inspection. |
| `Monitor::mkfs` writes the cluster UUID, the seed monmap and the mon. key into the store, and moves the mon. key into `<data>/keyring`. | `Monitor.cc` (`mkfs`, `write_fsid`, `extract_save_mon_key`) | The identity inside `store.db` (`cluster_uuid`, monmap) is not readable with plain file operations. Offline file checks can bind the `keyring` bytes and the store shape; fsid and rank binding come from the live monmap while the member is down. Whether an image tool can read `store.db` offline is not established. |
| The first MON is the embedded `testcontainers.Container` of `*ceph.Container`. `Terminate` on it is cluster-wide, and `RemoveMonitor("a")` only sets `monitorTerminated`. | `ceph/ceph.go`, `ceph/handles.go` | The initial MON cannot be replaced by swapping a container into that field without a data race on a field users read without a lock. |
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
| bound | The first generation reached its ready check and the identity was recorded | only after native removal of the MON, or by `Terminate` | only by a replacement generation with a matching identity |
| detached | `bound`, no attachment | `Terminate` only | a replacement generation only |
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
context, as the OSD operations do. `monStoreState` has no mutex and sits at the
position of `osdStorageState` (`TestNativeStateAddsNoMutex` scans it); Docker
calls are made with `Container.mu` held, as `registerOSD` does for the OSD
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
- The `require_store` function is duplicated in both scripts between marker
  comments because each script runs alone in the container;
  `TestMonRequireStoreBlocksAreIdentical` keeps the two copies equal.

## Identity observation

Implemented in `ceph/mon_store_inspect.go`, tested Docker-free in
`ceph/mon_store_inspect_test.go`. Nothing calls it in production and no native
run exists.

- Ports: `monStoreInspector` (a short-lived read-only inspector) reports the entries of the data directory (kind, mode, inode,
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
  runs and the store is being written (the observation covers a stopped member
  only); whether
  `ceph-mon`, in particular a different release, accepts the store.
## Identity adapters

Implemented in `ceph/mon_store_monmap.go`, `ceph/mon_store_inspect_container.go`
and `ceph/mon_store_json.go`, tested Docker-free. They are the concrete
adapters behind the two ports above. Nothing calls them in production and no
native run exists.

- Monmap adapter. `monStoreMonmapCLI` runs `ceph mon dump --format json` and
  then `ceph quorum_status --format json` through a `monStoreCLI` function (stdout
  only; the CLI prints `dumped monmap epoch N` on stderr). Shapes were read at the
  pinned commit 7f793731f1b39eb4f465e960113d2363c311b964: `MonMap::dump`,
  `MonmapMonitor` (`quorum` is an array of ranks), `Monitor::_quorum_status`
  (`quorum` ranks, `quorum_names`, `quorum_leader_name`, `quorum_age`), and
  `entity_addrvec_t::dump`. The recorded 20.2.4 `mon dump` fixture of the
  daemon-address tests is the real input of the decoder tests; no recorded
  `quorum_status` exists, so that document is synthetic.
- Strictness. Exactly one JSON value, no duplicate key at any depth, nesting
  within the shared limit of `internal/strictjson`, no trailing data. A
  required field that is missing, null or of the wrong type is an error: epoch (at least 1), canonical lowercase FSID, a non-empty `mons`
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
  port guard of `mon_store_inspect_test.go` exempts the adapter files.
- Known gap of the script. A regular file swapped for a symlink between the
  `lstat` and the `open` is refused by `O_NOFOLLOW` and the inode comparison,
  but no Docker-free test can force that race, so the mutation that removes
  `O_NOFOLLOW` survives the tests. The mount is read-only and the inspector
  runs only against a stopped store, which is the real protection.

Engine translation. `ceph/mon_store_inspect_engine.go` implements the engine
as `monStoreMobyEngine`, tested Docker-free in
`ceph/mon_store_inspect_engine_test.go` with a small fake of the Docker client.
Nothing constructs it: it has no default client, so the first caller supplies
the constructor, and the production-caller guards cover its symbols. It holds no
lock and no state.

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

The inspector image must carry `python3` and run as the numeric owner of the
store; whether that uid:gid is read from the retained store or taken from the
image's `ceph` user is a native question and must not be guessed in code. The
inspector is a volume user while it exists, so a caller must keep a second
replacement from starting while an inspection runs, under `Container.mu`.

## Replacement path

Implemented in `ceph/mon_store_generation.go`, tested Docker-free in
`ceph/mon_store_generation_test.go` against a small fake of the container side
(`monStoreGenerations`). `startMonitorStore` calls `startGeneration` for the
first generation of a store, and `replaceMonitor` calls `replaceGeneration`.

- `startGeneration`: the first generation formats a new store, a later one
  requires it. The pending attachment is published before the container is
  created, and an ID returned with an error is recorded. An unknown outcome is
  never replayed. A first generation that reaches its ready check marks the
  store ready (`ready`), without an identity.
- `stopGeneration`: removes the container and clears the attachment only after
  `confirmGone`. An attachment with an unknown creation outcome is left to
  `Terminate`.
- `replaceGeneration`: checks the next generation first (a ready generation
  exists, the volume is still the one this record created, an identity-bound
  store is verified), then stops the old container, then starts the next one
  from the new image on the same volume. A failure of the new container leaves
  the volume and the record, and never formats.
- `dispatch`: a store marked ready without an identity needs no verification;
  a store bound with an identity (`bind`) needs it.
- The Docker-free tests establish the order of the steps and the refusals.
  Real containers, the start of a new image on the old store and the rejoin
  are the native evidence of [Production wiring](#production-wiring).

## Address handoff

Implemented in `ceph/mon_store_address.go` and `startGeneration`, called
through the generation path and tested Docker-free against the fake of the container side. The monmap pins the
address a MON joined with, so the replacement container binds exactly that
address, and nothing may take it between the removal of the old container and
the start of the new one.

- The record keeps the endpoint its first generation bound (`monStoreEndpoint`:
  the IP, and either the bridge network name or the host ports), read from the
  running container after the ready check (`Observe`). If that observation
  fails the store is ready but cannot be replaced, and `replaceGeneration`
  refuses before it touches the old container.
- A later generation is created, not started, for a copy of that endpoint. After
  its ready check the bound endpoint is observed and must equal the recorded one.
- In host mode no reservation can last for the MON's lifetime: the running MON
  owns its ports, and the old container has to be gone before anything can bind
  them. So the cluster takes an exact hold after `stopGeneration` confirmed the
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
- A retained hold is released by the next `startGeneration` before a new one is
  taken, so there is at most one. It is also released first by
  `monStoreVolume.release`, which `releaseStores`, `retryStores` and
  `discardFresh` call before the attachment or the volume are looked at. A
  failed release makes the release fail, so the record, the volume and the hold
  stay owned and the next stage retries.
- Container side (`monStoreGenerations`): `Create` makes the container without
  starting it, `Start`, `Observe` and `Hold` complete the port.
- Guards: the generation and address symbols are used in production only by
  the pinned callers (`startGeneration` from `ceph/mon_store_container.go`,
  `replaceGeneration` from `ceph/mon_store_replace.go`) and
  `releaseAddressHold` from `monStoreVolume.release`, the files import no `sync`,
  the container side is called only from its pinned methods, a hold is released
  only by `releaseAddressHold`, `startGeneration` holds, dispatches, creates,
  releases, starts, waits and observes in that order, and `release` frees the
  hold before it looks at the attachment or the volume.
- Host mode is not wired: the exact-port allocator container is not written,
  and `Run` refuses the volume with the host network. Bridge mode uses the
  static-IP creation of `monitorGenerations.Create`.

## Production wiring

| Caller | Primitive | Container side |
| --- | --- | --- |
| `Run` (`runMonitor`, MON a) | `begin`, `create`, `startGeneration` (format) | `mon.sh` recipe of `firstMonitorOptions` |
| `AddMonitor` and the initial MONs (`startJoiningMonitor`) | `begin`, `create`, `startGeneration` (format) | `mon-join.sh` recipe of `monitorJoinOptions` |
| `MonitorContainer.Replace` (`replaceMonitor`) | `replaceGeneration`: checks, `stopGeneration`, `startGeneration` (require) | `mon-join.sh` recipe with fresh join material, for MON a too |
| `Terminate` | `releaseStores`, `retryStores` | none |

`monitorGenerations` (`ceph/mon_store_container.go`) is the container side for
bridge networking with IPv4 addresses, which is all `Run` admits with the
volume:

- `Create` mounts the volume with no copy at `/var/lib/ceph/mon/ceph-<name>`,
  sets `CEPH_MON_STORE_MODE=require` for a later generation, asks the bridge
  for the recorded IPv4 address, and creates the container without starting
  it. It first runs a preparation hook; the replacement clears the admin
  socket the old daemon left in the shared run directory there.
- `Observe` reads the container's IPv4 address on the cluster network.
- `Hold` only checks that no container of the network has the address: every
  container of the cluster is created under `Container.mu`, and a taken
  address fails the creation.
- `Remove` force-removes the container and never its volumes.

`replaceMonitor` refuses, before it touches the MON, a replacement that would
leave fewer than a majority of the monmap in quorum, a store that is not
attached to the handle's container, and a store without a recorded address
(the last inside `replaceGeneration`). A later generation of MON a also uses
the join recipe, with the live mon. key and monmap, so it needs no bootstrap
environment. Once the old container is gone, MON a is marked terminated and the
new generation is kept in the roster under the name `a`, so listing, handle
checks and `Terminate` treat it like an added MON.

Not wired: the identity binding (`bind`, the inspector and the monmap adapter),
host networking and address families other than IPv4, which `Run` refuses with
the volume.
