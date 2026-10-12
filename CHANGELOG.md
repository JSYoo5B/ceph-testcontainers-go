# Changelog

All notable changes are listed here. Versions follow
[semantic versioning](https://semver.org); before v1.0.0 a minor release may
change the API.

## Unreleased

### Added

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

### Fixed

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
