# Changelog

All notable changes are listed here. Versions follow
[semantic versioning](https://semver.org); before v1.0.0 a minor release may
change the API.

## Unreleased

### Added

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

- The bootstrap `ceph.conf` sets `crash dir = /var/lib/ceph/crash`, and
  `ceph.WithConfigFile` rejects `crash_dir`, so every daemon's crash reports
  stay where `CollectCrashMetadata` reads them.

### Fixed

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
