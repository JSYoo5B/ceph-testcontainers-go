# Changelog

All notable changes are listed here. Versions follow
[semantic versioning](https://semver.org); before v1.0.0 a minor release may
change the API.

## Unreleased

### Added

- The Messenger fixture guide explains how to tell which MON session belongs
  to a registered MGR. A MGR authenticates its beacon client, whose global
  ID is the MgrMap GID, and a client for each of its modules. A refusal in
  the MGR's own log therefore does not show that the registered slot lost
  its session; the global ID in the MON's refusal has to match the MgrMap
  GID. A test checks the sessions on every owned MON.

- The Messenger fixture guide has a recipe for a refused first MGR
  authentication: a test helper that keeps the rejection and the identities
  frozen after it as recorded and, within one budget, collects each MON's own
  `mon_status`, the quorum, each MGR's GID, container instance and MON
  sessions, and fresh native controls, with each query's status (complete,
  unsupported, timed out, truncated, failed or unavailable). Its classifier
  reports completeness, whether the target MGR kept its GID and container
  instance, which client a MON's refusal named, and reused secret IDs, and
  always reports generation equality as `unavailable/unverified`. The bundle
  holds no keys.

- The guide [Fixtures for Messenger client tests](docs/guides/messenger-client-fixtures.md)
  has an MDS session section. Its first receipt keeps a native libcephfs
  client mounted while the only MDS is killed, failed and started again:
  the session returns with the same client ID, PID and address, and the file
  descriptor opened before the failure keeps reading, writing, renaming and
  setting xattrs, as a client in another container confirms. A second
  receipt does the same while an idle standby, or the journal follower of
  `StandbyReplay`, takes over rank 0; the killed daemon then rejoins as a
  standby with a new GID. A third keeps the client's file in a directory
  pinned to rank 1 of two active ranks while a standby takes over rank 1, and
  rank 0 keeps its daemon and its session with the client. Another shows a
  capability handoff: a second client writes a file that a live client holds
  with exclusive, buffered capabilities, and the MDS's `dump inode` shows them
  revoked without an eviction. Another freezes a client across an MDS
  failover: it misses the shortened reconnect window, is evicted and
  blocklisted, and its open file then fails (`ESHUTDOWN` on Tentacle, `EPERM`
  on Squid). Another moves a directory from rank 0 to rank 1 and back while a
  client streams fsynced records into a file in it through one descriptor:
  rank 1 becomes the directory's only authority, the daemons' export and
  import counters grow, and the client keeps its sessions and reads every
  record. A last one holds one rename between its early, unsafe reply and its
  journal commit with `mds_log_pause`: the MDS's ops in flight and the
  client's own pending requests show the same TID, unsafe, until the pause is
  restored and the request commits.

- The same guide has a table of daemon-to-daemon paths, with what each test
  observes and how directly. The held replication write of the separate
  network tests now records a receipt: its PG and acting set, the request the
  primary held, controls that kept working during the cut, and the bytes and
  object version that an independent client reads afterwards.

- The daemon-to-daemon path table has a receipt that tells log recovery
  from backfill. A replica that missed writes rejoins with recovery held,
  and the primary's `pg query` shows which path it took. The test records
  that path, the objects recovered per PG, an independent readback and the
  replica serving every object alone. It also runs on a K=2, M=1
  erasure-coded pool, for the OSD of one shard.

- The legacy placement recipe keeps `hashpspool` off every pool, the MGR's
  `.mgr` pool included: `osd_pool_default_flag_hashpspool = false` in a
  `WithConfigFile` file applies before the MGR creates `.mgr`. While any pool
  has the flag, the MON requires the `OSDHASHPSPOOL` feature of every client,
  so `NoHashPSPool` on the test's own pool was not enough for a client
  without it. The tests of the recipe check the flags of every pool.

- The Messenger fixture guide has a recipe for a monmap-only membership
  change: `mon add` of a MON that never answers, then `mon remove`. Each
  change starts an election that holds MON commands and new client sessions
  for several seconds, the remove too on Tentacle; the recipe waits until `quorum_status` reports the
  same election epoch twice before starting new native clients.

## v0.2.0 (2026-10-14)

See [Upgrading to v0.2.0](docs/guides/upgrading-to-v0.2.md)
for compatibility changes, platform requirements and known native limits.
The completed [fixture-readiness milestone](https://github.com/JSYoo5B/ceph-testcontainers-go/milestone/1)
provides server-side profiles and native receipts; it does not certify a
downstream client's implementation or migration. Publication checks are in
[Preparing a release](docs/contributing/releasing.md).

### Added

- The guide [Fixtures for Messenger client tests](docs/guides/messenger-client-fixtures.md)
  maps the profiles and daemon controls of a Messenger client test suite to
  fixture options, with the tests that prove each one, and lists what differs
  from a single-container bootstrap. Its mutation oracle table names the
  native receipt of each mutation outcome, from zero, create and compare
  guards to operations that the OSD applies after the client gave up.

- `ceph.MessengerV2Only` for `WithMessengerMode` listens on and advertises
  only Messenger v2, like `MessengerV2Secure`, but leaves the connection modes
  configurable: `WithMessengerPolicy`, `TemporaryMessengerPolicy` and
  `WithClientMessengerPolicy` select CRC, secure or a per-daemon-type mix, for
  example a CRC-only cluster without v1. The MON publishes only `3300/tcp`,
  later daemons inherit the protocols, and `TemporaryConfig` refuses
  `ms_bind_msgr1` and `ms_bind_msgr2`.

- The RGW module page has a recipe for the smallest S3 fixture: one MON, one
  MGR, one OSD and one gateway with single-copy pools, started by `rgw.Run`.

- `ceph.WithAddressFamily(ceph.AddressIPv6)` works with
  `ceph.WithSeparateClusterNetwork()`. Both bridges are then dual-stack, and
  Ceph's public and cluster networks are their IPv6 subnets, so OSDs advertise
  public and heartbeat front addresses from one and cluster and heartbeat back
  addresses from the other. `InterruptNetwork` with `ClusterNetworkPlane`
  disconnects an OSD's whole cluster endpoint, both of its families, and
  keeps its public endpoint. `AddressIPv4MappedIPv6` is still refused with a
  separate cluster network.

- Classified errors. Every package reports a refused request
  (`ceph.ErrInvalidRequest`), a feature that the Ceph of the relevant target
  lacks (`ceph.ErrUnsupported`), a missing image tool or an unselected
  extension (`ceph.ErrImageCapability`), an engine, host or kernel limit
  (`ceph.ErrEnvironmentUnavailable`) and a native command that ran and failed
  (`ceph.ErrNativeFailure`), matched with `errors.Is`. `*ceph.OperationError`
  names the target, its observed version, its image ID and the missing
  capability, and `*ceph.NativeFailure` the program, exit code, reported
  errno and the tool's redacted reason; `ENOENT`, `EACCES`/`EPERM` and
  `EEXIST` also match `fs.ErrNotExist`, `fs.ErrPermission` and `fs.ErrExist`.
  Errors that hide native output keep this classification without the
  output. Context errors, `ErrNativeCommandIncomplete` and
  `NativeCommandUnstoppedError` are unchanged, and `RunNativeCommand` still
  returns a nonzero exit as its result. See
  [Classified errors](docs/guides/errors.md).

- `ceph.Container.TemporaryNetworkDelay` delays every packet that a container
  sends on one of the cluster's bridges, with the kernel's netem qdisc, until
  `NetworkDelayLease.Restore`. It is an opt-in extension: the caller selects a
  netem helper image (`ceph.NetemHelperImage` or another image that meets the
  netem contract of ceph-testcontainers-images), which runs in the target's
  network namespace with `NET_ADMIN` while the target keeps its own image.
  Before any change it checks the helper image, `NET_ADMIN` and the kernel's
  netem, and reports them as `ErrNetemImageUnsupported`,
  `ErrNetemPermissionDenied` and `ErrNetemKernelUnsupported`. The lease reads
  the kernel's qdisc and counters with `Status`, removes only its own qdisc,
  survives a lost helper, ends with the target's interface, and is restored
  by `Terminate`.

- `ceph.Container.CollectCrashMetadata` reads the crash reports that Ceph
  daemons wrote into `/var/lib/ceph/crash` (`ceph.CrashDir`), through Docker's
  archive API, also from stopped containers. It keeps only the known fields of
  each report's `meta` file, redacted, skips crash logs and dumps, rejects
  links, escaping paths and non-JSON files, bounds every read, and tells
  records, empty and missing directories, removed containers, failures and
  incomplete archives apart.

- `ceph.Container.RunNativeCommand` runs one argv command in the control
  container, an owned daemon or service container, or a caller's container on
  the cluster's network, under the supervisor of the module's own native
  queries. It bounds the run time and each output stream inside the
  container, returns stdout, stderr, the exit code and explicit timeout and
  truncation flags, and stops the command's process group inside the
  container when the context ends, also before Docker started it. A stop that
  cannot be confirmed returns a retryable `ceph.NativeCommandUnstoppedError`,
  and `Terminate` cancels and stops these commands. Errors carry a label but
  never the arguments or the output; a Docker or container failure appears
  only by its kind, and `errors.Unwrap` returns the original error.
- `ceph.Container.RedactSecrets` masks the cluster's keys and other secrets in
  native output, with the rules of `CollectDiagnostics`.
- The native commands guide has a container-only oracle recipe with
  `rados_stat2`, `rados_read` and `ceph-dencoder`.
- The native commands guide has an oracle recipe for self-managed snapshot
  write contexts, with the native results of Ceph 20.2.4 and 19.2.5 for
  older sequences with and without `ORDERSNAP`, removed and unallocated
  snapshot IDs, snapshot removal, HEAD deletion and an OSD restart.
- The native commands guide has an oracle recipe for write outcomes: the
  completion as the receipt across a `SIGKILL` of the OSD, a local timeout
  that does not cancel the write, a killed client whose write the OSD
  discards, and retries guarded with `assert_version`. The fault injection
  guide shows how `osd_debug_inject_dispatch_delay_*` makes an OSD run a
  write after its client gave up.
- The native commands guide has an oracle recipe for erasure-coded pools:
  the write operations that pools with and without `AllowOverwrites` accept
  or refuse with `EOPNOTSUPP`, aligned appends, snapshot clones, and reads
  and writes while a shard is missing.

- `ceph.WithOSDObjectStore(ceph.OSDMemStore)` runs every OSD on Ceph's
  in-memory MemStore, with `WithOSDBlockSize` as its capacity. A clean stop
  keeps its data.
- `ceph.PoolConfig.NoHashPSPool` clears a new pool's `hashpspool` flag.
- The module page shows how to install a legacy CRUSH map with the native tools
  and keep it across OSD restarts.
- `ceph.Container.DaemonAddresses` reads the names and advertised address
  vectors of every MON, the active and standby MGRs and every OSD from the
  native maps, with each map's epoch.
- `ceph.Container.AdminSocket` runs a command on the admin socket of an owned
  MON, MGR or OSD, without the Messenger or Cephx. The daemons of a cluster
  share a run directory, a Docker volume at `/var/run/ceph`, so it also works
  with OSD images that have no `ceph` CLI.
- `ceph.WithAuth` selects the Cephx key type (`aes` or `aes256k`), the MonMap
  service, allowed and preferred ciphers, and the MON and service ticket
  lifetimes, with fractions of a second, of a new cluster. An image whose
  `ceph-authtool` has no `--key-type`, such as Squid or Tentacle before 20.2.4,
  accepts only `aes` and fails before any daemon starts otherwise.
- `ceph.Container.CreateClient` takes `ceph.ClientOption` values;
  `ceph.WithClientKeyType` selects the key type of one identity.
- `ceph.WithMessengerPolicy` selects the Messenger v2 connection modes (`secure`,
  `crc`) that MONs, MGRs and OSDs accept from clients, and
  `ceph.Container.TemporaryMessengerPolicy` changes them on a running cluster
  without a restart. `ceph.WithClientMessengerPolicy` sets the modes of a
  native client container separately, through the new
  `ceph.ClientContainerOption` parameters of `WithClient` and
  `WithClientIdentity`.
- `ceph.WithAddressFamily` runs every Messenger listener on IPv6: on the
  container's IPv6 address of a dual-stack bridge (`ceph.AddressIPv6`), or on
  its IPv4 address as an IPv4-mapped IPv6 address (`ceph.AddressIPv4MappedIPv6`).
- `ceph.Container.RotateDaemonKey` replaces the Cephx key of a running MGR or
  OSD without a restart and keeps its keyring files in step, so later restarts
  authenticate with the new key. The native commands guide shows how to
  observe a daemon's rotating service secrets in its debug log.
- `ceph.Container.ReplaceClientKey` gives an owned client identity a fresh key
  in one `auth import` that keeps its name and exact capabilities, returns a
  descriptor for the new key and leaves the old descriptor's credentials
  available for rejection checks. A failed or canceled call settles the MONs
  and reports the outcome they hold. `UpdateClientCaps` and `DeleteClient` are
  fenced the same way, so a timed-out capability change or removal can no
  longer apply after a later operation on the identity.
- `ceph.Container.TemporaryClientRevocation` removes an owned client identity
  until `ceph.ClientRevocation.Restore` puts back exactly its key and
  capabilities, keeping the descriptor; `Restore` refuses an identity that was
  created again with another key or capabilities, and `DeleteClient` makes the
  removal permanent.

### Changed

- `CreateClient`, `WithClient` and `WithClientIdentity` now take variadic
  options. Existing direct calls still compile, but interfaces, mocks and
  stored method values with the old signatures need to be updated; see
  [API compatibility](docs/guides/upgrading-to-v0.2.md#api-compatibility).

- `TemporaryConfig` classifies its refusal of an option that the Messenger mode
  fixes as `ErrInvalidRequest`.

- Errors of the module's native commands, such as those of `Ceph`, no longer
  contain the command's standard output and standard error. They contain the
  program, exit code, errno and the tool's reason without its log lines, with
  keys redacted; `errors.As` with `*ceph.NativeFailure` returns them.

- `rbd.RunMirror` decides whether it can map differently named namespaces
  from the `rbd` of each setup client and of each cluster's control image
  (`--remote-namespace`), and refuses the mapping with `ceph.ErrUnsupported`
  naming the side that lacks it. It previously decided from the release
  numbers alone, so a backported Squid image could not map namespaces and a
  Squid setup client for Tentacle clusters failed after its first changes.

- Go 1.26 or later is required. `golang.org/x/crypto` v0.56.0, which fixes
  GO-2026-6354 and GO-2026-6355, needs it, and Go 1.25 is no longer supported
  upstream. Use a current patch release: Go 1.26.8, for example, has
  standard-library vulnerabilities that Go 1.26.9 fixes.

- The module requires `github.com/moby/moby/client` v0.6.2 and
  `github.com/moby/moby/api` v1.56.1 instead of v0.5.0 and v1.55.0. A module
  that uses the Docker client directly gets these versions too.

- The bootstrap `ceph.conf` sets `crash dir = /var/lib/ceph/crash`, and
  `ceph.WithConfigFile` rejects `crash_dir`, so every daemon's crash reports
  stay where `CollectCrashMetadata` reads them.

### Fixed

- On a bridge network, a MON that restarts after other containers started
  comes back at its monmap address. Docker allocated the address again at each
  start, so after all MONs stopped, the one started first could take another
  MON's address, which then failed to bind it with
  `Cannot assign requested address`. The fixture now requests each MON's
  original address before it starts again. A MON whose endpoint is
  interrupted stays detached, and `Start` returns `ErrInvalidRequest` until
  the interruption is restored.

- `cephfs.Mirror.RebalanceDirectories` no longer hangs the MGR's mirroring
  module. It sent all removals back to back, and the module deadlocks when an
  earlier directory's release is acknowledged while a later removal waits for
  its RADOS write: the removal holds the policy lock that the acknowledgement
  needs, and both callbacks run on one librados strand. It now removes one
  directory at a time, while every tracked directory is idle. The deadlock
  reproduces natively on Ceph 20.2.4 and 19.2.5. A mirror daemon that joins or
  leaves during a removal can still trigger it; see the CephFS limitations.

- A service start that fails before Docker creates the container, for
  example when the image cannot be pulled, now returns its error with no
  handle. It returned a non-nil handle that held a nil container:
  - `cephfs` read that handle's ID and panicked while holding the topology
    lock, so `Terminate` then waited for the lock until its context ended;
  - `rgw` returned a `Gateway` without a container along with the error.

- `Ceph` and the fixture's own `ceph` commands no longer fail with
  `timed out` when no MON answers within the CLI's 5 second connection
  timeout, as during a storage stall of a CI runner or an election. The CLI
  exits before it sends the command, so they connect again until a MON
  answers, the context ends or a minute has passed.

- `CollectDiagnostics` and `RedactSecrets` redact the token of an
  `Authorization: Bearer` header. The secret-assignment rule replaced only
  the word `Bearer` and left the token after it.

- Dependencies with vulnerabilities that govulncheck finds reachable are
  updated: `github.com/moby/go-archive` v0.3.0 (GO-2026-6253), `golang.org/x/crypto`
  v0.56.0 (GO-2026-6354, GO-2026-6355, GO-2026-6303) and
  `github.com/klauspost/compress` v1.18.7 (GO-2026-5841).

- `ceph.Container.Terminate` removes a container that Docker created although
  the reply to the create request was lost, for example when `Run`'s startup
  timeout ended during the request. The cluster had no handle for it, so it
  stayed, with the shared run volume that it held. Every container of a
  cluster now carries the label `org.testcontainers.ceph.cluster`.
- Redaction in `CollectDiagnostics` and `RedactSecrets` replaces the secrets
  in one pass. A short secret that occurs in `[REDACTED]`, given more than
  once, made the replacements rewrite their own output until the text grew
  without bound.
- The supervisor of `AdminSocket`, quorum reads, `RotateDaemonKey` and
  `CollectDiagnostics` now reaps the descendants of a command that it killed.
  They previously stayed as zombies in daemon containers, whose first process
  does not reap orphans. A canceled call also confirms at once a command that
  ended with an earlier instance of a restarted container.
- `rbd.Mirror.WaitReplayReady` no longer fails while rbd-mirror creates the
  destination image: the moment in which the new image reports mirroring
  disabled, before rbd-mirror enables it, was taken for a changed identity.
- `ceph.Container.RotateDaemonKey` no longer fails now and then with a MON
  command that applied: its librados client script now shuts the client
  down before exiting. Left to the interpreter's shutdown, the client aborted
  there in about 2.5% of runs on both Squid 19.2.5 and Tentacle 20.2.4.
- Canceling `ceph.Container.CollectDiagnostics` now stops its Ceph queries in
  the control container before returning. A query previously ran on for up to
  three quarters of `OperationTimeout`.
- Canceling `ceph.Container.QuorumStatus` or `ceph.Container.WaitForQuorum`
  now stops its `ceph quorum_status` process in the control container before
  returning. The process previously ran on for up to five seconds.

## v0.1.0 (2026-10-11)

The first release. It provides four Testcontainers for Go modules that start
a real, disposable Ceph cluster for integration tests.

### Packages

- `ceph`: the cluster. `Run` starts the MON, MGR and OSD topology from the
  official Ceph image, by default one MON, one MGR and two OSDs with file-backed
  BlueStore. The `Container` adds and removes MONs, MGRs and OSDs, creates and
  places pools, manages Cephx identities and configuration overrides, waits for
  health, quorum and clean PGs, injects faults (stopping, pausing and
  disconnecting daemons and clients, delaying or dropping messages, fencing,
  full clusters and damaged replicas) and collects diagnostics.
- `rbd`: initialized RBD pools and namespaces, image watchers and locks, pool
  and image mirroring between two clusters, and full or incremental backup and
  restore.
- `cephfs`: filesystems and their MDS topology, subvolume groups, subvolumes,
  snapshots and clones, pins, quiesce sets, client authorization and sessions,
  and snapshot mirroring between two clusters.
- `rgw`: S3 and Swift gateways, users, tenants, accounts and quotas, placement
  targets and storage classes, native TLS, usage, and multisite zones and sync
  policies across clusters.

Each package has a `Run` that starts a cluster with the service's defaults and
a `Container` that embeds `testcontainers.Container`; options of several
service packages can be passed to one `ceph.Run`.

### Requirements

- Go 1.25 or later and Testcontainers for Go v0.44.0.
- A Docker engine that runs Linux containers; no privileged mode or host
  devices are needed. Host networking has extra requirements, described in
  [the host network guide](docs/guides/host-network.md).
- Ceph Tentacle 20.2.x (`ceph.DefaultImage`, `quay.io/ceph/ceph:v20.2.4`
  pinned by digest) or Squid 19.2.x with the differences in
  [the releases guide](docs/guides/releases-and-images.md).

The modules control Ceph through the CLIs inside the containers and build with
`CGO_ENABLED=0`; they have no go-ceph, librados or cgo dependency.
