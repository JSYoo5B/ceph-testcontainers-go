# Changelog

All notable changes are listed here. Versions follow
[semantic versioning](https://semver.org); before v1.0.0 a minor release may
change the API.

## Unreleased

### API naming

- Finish the staged naming pass: `rbd.TemporaryImmutableObjectCache` becomes
  `rbd.StartImmutableObjectCache`. The lease, client options, Stop/Restore,
  error ownership and cleanup stay unchanged; there is no automatic expiry or
  compatibility alias. See `docs/guides/upgrading-rbd-cache-name.md`.
- Record the remaining public declaration/member retention decisions, including
  dependency-role APIs, and bind that review to generated source catalogues.
  Inventory regeneration alone cannot approve later API changes.

### Added

- `ceph.Finalization.TrackDebugLevel` registers a debug level window so that
  the failure manifest keeps a typed copy of its receipt when the archive is
  taken and another after the cleanups (`debug_windows`). Registrations are
  append-only and sequential windows on one daemon, option and generation stay
  separate; a nil handle is recorded as absent; the copies are bounded and
  redacted and never touch the daemon. The debug window state machine is
  unchanged.
- `ceph.Container.OverrideDebugLevel` opens one runtime `debug_ms` or
  `debug_osd` window on one running owned OSD through its admin socket. It
  records the effective prior level, verifies the normalized readback, and
  binds the window to the OSD container's generation. Only one window per
  daemon and option is admitted. `DebugLevelOverrideHandle.Restore` restores
  only the same generation that still shows the window's own value, verifies
  the restored readback, accepts an already restored level and leaves an
  outside edit in place. Each set is admitted only on the window's
  generation while the cluster's topology operations wait, and the generation
  is checked again after each read. A set whose end is unconfirmed keeps the
  window open, and `RemoveOSD` and `DetachOSD` stop it first or refuse, until
  it is confirmed stopped. A failed or lost apply returns a
  handle that reconciles before it restores, and `Receipt` keeps the states, readbacks,
  times and errors. Registered with `Finalization.Cleanup`, restoration runs
  after the failure archive and before earlier-registered client removals.
  Other roles, options, levels and replacement-spanning windows are not
  supported yet.
- `ceph.Finalization.Verify` activates an opt-in post-test verification of a
  dedicated cluster: it records the daemon container generations, the crash
  IDs already present and the window's start, and adds a check that
  evaluates the daemons' log lines and new crashes against separate
  `LogExpectation` and `CrashExpectation` rules. `Required` distinguishes
  requiring an event from permitting it. Reports are clean, unexpected or
  incomplete; removed, restarted or inaccessible containers, rotating or
  unsupported log drivers, cut or malformed streams, unreadable crash
  directories and incomplete crash metadata are gaps, never clean. A result
  that is not clean fails the test and, with `Archive`, is archived before
  the cleanups. `Verification.Evaluate` returns the report without failing.
  Shared clusters are refused. Native coverage of log windows and crash
  completion is not yet measured (#47), so a clean result is scoped evidence
  only. The native `short/post_test_verification` batch crashes the MGR and
  an OSD after activation (#48).

- `ceph.Container.TerminateOnCleanup` registers one test cleanup that runs
  the checks added with `Finalization.Check`, then, with
  `FinalizationConfig.Archive` and a failed test, a failure archive, then the
  cleanups added with `Finalization.Cleanup`, and finally `Terminate`. The
  archive writes the `CollectDiagnostics` and `CollectCrashMetadata` reports
  and a manifest with execution provenance and every outcome into a new
  directory under the test's `ArtifactDir` (kept with `go test -artifacts`).
  Each phase has a fresh bounded context; collection and storage errors are
  recorded without skipping cleanup, and a passing test collects nothing.
  The tagged runtime runner passes the artifact flags in both execution
  modes, lists the retained files in `native/report.json`, and the job
  summary records the upload of a failed run's artifact separately. A
  downloaded artifact of a failed native run has not yet verified it (#49).

- `capture.Start` and `capture.Session.Stop` record the packets of one
  container endpoint on a user-defined bridge with a caller-selected helper
  image that meets packet-capture contract 1 of ceph-testcontainers-images.
  The filter is compiled and the interface checked before the capture, a
  session is ready only after tcpdump listens and its file exists, and it is
  bound to the target's container instance and sandbox. Stop ends tcpdump with
  SIGTERM within a bounded grace, retrieves the classic pcap, checks its
  structure and removes the helper; `capture.Report` keeps the process,
  target, statistics (unknown when not printed), retrieval and file outcomes
  apart. No helper image is published or selected by default (#52).

- `ceph.Container.MonitorsContext` and `OSDsContext` list the owned MONs and
  OSDs like `Monitors` and `OSDs`, but stop waiting for a concurrent topology
  change when `ctx` ends, as `ManagersContext` already did. The MON
  replacement diagnostics of `TestMultiClusterMonitorBootstrapRefresh` use
  them for the quorum wait, every snapshot, the warm-daemon identities and
  the log traces, so a held topology mutex no longer outlives the failure
  bundle's bound (#4).

- `ceph.Container.DetachOSD` and `ReopenOSD` restart a retained OSD store in a
  new container. With `WithOSDVolumeStorage`, detach fences the instance,
  stops it, waits for down and removes the container, then returns a
  `ceph.DetachedOSD` for the store and registration that it keeps. Reopen
  checks that the store's identity files, keyring and block are unchanged,
  that the registration holds the same UUID and is down and that the MONs'
  key matches, before it starts a new `*OSDContainer` with the same ID and
  UUID. The native `topology/osd_retained_reopen` batch reads known bytes back
  from a single-replica pool through the new container. See
  `docs/design/osd-retained-reopen.md` (#6).

- The native `topology/osd_retained_reopen` batch also refuses, without
  mutation, a detach of a paused OSD, a reopen of a store whose keyring an
  independent observer changed, `RemoveOSD` of a detached OSD and a second
  reopen of one handle. It then restores the keyring and reopens (#6).

- `ceph.WithOSDVolumeStorage` places classic BlueStore OSD directories in an
  ordinary cluster-owned Docker local named volume, allocated on the first OSD.
  Purged OSD directories are discarded only after container removal; busy volume
  deletion stays retryable through `Terminate`. This is the storage backend
  increment for #6, without a public retained detach/reopen API. See
  `docs/design/osd-volume-storage.md`.

- Add the first machine-validated Messenger declaration/requirement/case join.
  The existing targeted-command recipe emits a source/build/image-bound receipt
  only after successful owned cleanup. An offline validator rejects incomplete,
  stale or mixed evidence and keeps unmapped/unexecuted scope visible. This
  bounded native-state increment does not claim wire or consumer completion;
  see `docs/design/messenger-evidence.md`.

- `TestCephFSFuseMount`, in the new optional batch `cephfs_fuse` (build tag
  `cephfs_fuse`, outside every required check), mounts a
  path-restricted CephX identity's directory with `ceph-fuse` from the
  official image in client containers that alone get `/dev/fuse`,
  `CAP_SYS_ADMIN` and `apparmor=unconfined`. An admission probe refuses
  smaller settings with a typed reason before any mount, and `ceph-fuse`
  then fails the same way. The test reads bytes written through the mount
  back through libcephfs, logs `CEPHFS_FCNTL_LOCK_RECEIPT` for a POSIX
  record lock between two mounts that the MDS holds waiting, and checks the
  cleanup of a killed, an unmounted and a cancelled `ceph-fuse`. The
  feasibility report `docs/research/ceph-fuse-mount.md` records the
  per-environment requirements and outcomes, and the Messenger coverage row
  `client-mds/file-locks` cites the receipt.

- `NetworkDelay.Reorder`, `Duplicate` and `Corrupt` add netem's packet
  faults to a `TemporaryNetworkDelay` or `TemporaryLinkDelay` lease, on the
  whole endpoint or only to the selected peers. Each is a new
  `ceph.NetworkPacketFault` with a `Percent` and an optional `Correlation`.
  A `Reorder` needs a `Delay` and refuses `Jitter` and `Rate`; `Duplicate`
  and `Corrupt` work with every other field and need no `Delay`. The
  preflight helper checks that `tc` parses each requested fault
  (`ErrNetemImageUnsupported`) and that the kernel reports it, its
  correlation and a reorder's gap back (`ErrNetemKernelUnsupported`) before
  the target changes. The lease records the request, `NetworkDelayStatus`
  reports the kernel's `Reorder`, `Duplicate`, `Corrupt` and `Gap`, and an
  apply whose installed faults differ returns the lease with an error. The
  `tc` of `ceph.NetemHelperImage` (iproute2 6.1) drops a duplicate's
  correlation unless the delay has a correlation too, so the lease passes a
  zero delay correlation with it. `TestNetworkPacketFaults` and
  `TestNetworkPacketFaultsIPv6`, in the new short batch
  `network_packet_faults`, fault osd.0's packets to one client on an IPv4
  and an IPv6 cluster and read a 4 MiB object with its exact SHA-256 from
  the client and from an unselected control container. The receiver's own
  kernel counters show each effect at the client and not at the control
  container: `TcpExtTCPOFOQueue` for a reorder, `TcpExtTCPDSACKOldSent` for
  a duplicate and `TcpInCsumErrors` for a corrupt, also with a duplicate and
  a corrupt composed with a delay and a rate; after `Restore` the reads are
  fast again.
- `NetworkDelay.Rate` limits the bandwidth of the packets that a
  `TemporaryNetworkDelay` or `TemporaryLinkDelay` target sends, on the whole
  endpoint or only to the selected peers, with netem's rate in the lease's
  netem qdisc, alone or with `Delay` and `Loss`. The new `ceph.NetworkRate`
  counts bits per second (`BitPerSecond` to `GigabitPerSecond`), from
  `MinNetworkRate` (64 Kbit/s) to `MaxNetworkRate` (10 Gbit/s) in whole bytes
  per second, and `NetworkDelay.QueueLimit` sets the netem queue in packets
  (`DefaultNetworkQueueLimit`, 1000, when zero). The preflight helper checks
  that `tc` parses netem's rate (`ErrNetemImageUnsupported`) and that the
  kernel keeps it (`ErrNetemKernelUnsupported`) before the target changes.
  The lease records the requested `Rate` and `QueueLimit`, and
  `NetworkDelayStatus` reports the kernel's `Rate` and `QueueLimit` apart
  from netem's `Overlimits`, `Requeues`, `Backlog` and `QueueLength`, and
  never a throughput. `TestNetworkBandwidth` and `TestNetworkBandwidthIPv6`,
  in the new short batch `network_bandwidth`, read a 16 MiB object through
  an IPv4 and an IPv6 peer limit of 16 Mbit/s, check that an unselected peer
  and the other family stay unlimited, and that `Restore` leaves the
  kernel's default root qdisc and no helper. `TestNetworkBandwidthSevere`
  shows severe limits: at 64 Kbit/s a client with a 10-second
  `rados_osd_op_timeout` fails a 1 MiB read with `ETIMEDOUT` and reads it
  again after `Restore`, a client without one reads 256 KiB in about 34 s,
  and a `QueueLimit` of 2 under a delay makes netem drop packets and the read
  many times slower while it still completes.
- `TestRBDMirrorBandwidthCatchUp`, in the new multicluster batch
  `rbd_mirror_bandwidth`, limits the source OSD's packets to the
  destination's rbd-mirror daemon to 2 Mbit/s while the source client
  rewrites a 48 MiB snapshot-mirrored image through the same OSD at full
  speed. The mirror's native status reports the new snapshot syncing with a
  growing `syncing_percent` within the limit over two 30-second status
  updates, and the destination's bytes still differ; after `Restore` the
  destination's own client reads exactly the source's bytes and the status
  reports the snapshot in sync with every byte copied once.
- `NetworkDelay.Peers` selects several peers in one `TemporaryNetworkDelay`,
  each a `ceph.NetworkPeer` with an address and an optional TCP port, of
  either family, up to `ceph.MaxNetworkPeers`. The lease owns one u32 filter
  per peer under its `prio` root and netem child: IPv4 peers in preference 10
  and table `800:`, IPv6 peers in the next preference and table. `Status`
  reports `Applied` only with every filter, and `NetworkDelayLease.Peers`
  records the whole selection. A delay with peers first checks the kernel's
  `prio` qdisc and u32 classifier in the preflight helper, and a kernel
  without either is `ErrNetemKernelUnsupported`. `TestCephFSMDSOSDsPathCut`
  cuts an MDS's paths to both OSDs of a filesystem with one lease.
- `rgw.Multisite.TemporaryZoneLinkDelay` delays or drops only the packets
  that one zone's gateway sends to another zone's gateway on the private
  multisite network, so one direction of replication stalls while the other
  continues, through the new package-level `ceph.TemporaryLinkDelay`, which
  applies a netem delay on a bridge that a fixture owns besides the cluster's
  planes. Every netem delay now also requires that the endpoint's network is
  a bridge.
- `ceph.AddressDualStack` (`"dual-stack"`) runs the cluster on a dual-stack
  bridge where every MON, MGR, OSD and service daemon binds and advertises
  both its IPv4 and IPv6 address, also with `WithSeparateClusterNetwork`. MON
  vectors list IPv6 first so that the MGR advertises a concrete IPv6 address
  instead of `[::]`, and `Run`, `AddManager` and `RemoveManager` fail with
  `ErrUnsupported` when a daemon advertises a wildcard or lacks a family.
- `ceph.NewShared` lends one lazily started cluster to the tests of a test
  binary. `SharedCluster.Lease` gives each test a `Tenant`: the RADOS
  namespace of its ID in the pool `ceph.SharedPool` and a client with
  `mon 'allow r'` and `osd 'allow rw pool=tenants namespace=ID'`, more clients
  of that namespace with `CreateClient`, `WithClient` and `RunNativeCommand`
  for its own client containers, and read-only views. A release removes the
  tenant's clients and objects and compares auth entities, pools, OSD flags,
  the configuration database, the blocklist, health and the shared pool's
  namespaces with the baseline taken at start. A difference, a failed tenant
  test (unless `WithKeepOnTenantFailure`) or an exited daemon poisons the
  cluster, and the next lease starts a fresh one, at most `WithMaxRecycles`
  times. `WithMaxTenants` bounds the leases held at once, `RunTests` closes
  the clusters after `TestMain`, and `Stats` reports starts, recycles and
  times. While shared, the `*ceph.Container` refuses the methods that reach
  outside one tenant with the new category `ceph.ErrSharedCluster`, including
  `Terminate`.
- `SharedCluster.Exclusive` lends a test the whole shared cluster: the
  `*ceph.Container` with every method except `Terminate`. It waits until no
  tenant holds a lease, and new leases wait behind it, so leases cannot starve
  it, while leases that asked first still go first. Its release retires the
  cluster, and the next lease starts a fresh one without spending
  `WithMaxRecycles`. A test that holds a tenant lease, or whose parent does,
  fails at once instead of waiting for itself, as does a lease of the test
  that holds the exclusive lease. `SharedStats` gains `Exclusives` and
  `ExclusiveWait`.
- Service tenants of a shared cluster: `rbd.Lease` gives a tenant the RBD
  namespace of its ID and a client with `profile rbd` limited to it,
  `cephfs.Lease` the subvolume group of its ID, with its file data in the
  namespace `fsns-ID` and a client limited to `/volumes/ID` and that
  namespace, and `rgw.Lease` the RGW tenant of its ID with the S3 user
  `ID$user`. A release removes them, evicting leftover CephFS sessions and
  waiting out leftover RBD watchers, and the baseline and releases also
  compare the RBD namespaces, subvolume groups and RGW users and buckets.
  Lease setup and release read the cluster state over one connection instead
  of a Ceph CLI process per command: on Tentacle a RADOS lease took about
  3.7 s to set up and 4.7 s to release, now about 0.2 s and 0.4 s.

- With `ceph.AddressDualStack`, `WithConfigFile` accepts `ms_bind_prefer_ipv4`,
  such as `[osd.1] ms bind prefer ipv4 = false`, to make a MGR, OSD or MDS
  list its IPv6 address first. Native clients connect to the first entry of
  a vector, so they then complete MGR, OSD and MDS operations with that daemon
  over IPv6. `Run` rejects the key for the other families. Dual-stack CephFS
  and RGW are now tested: MDS vectors are concrete in both families, and the
  gateway serves S3 on its IPv4 and IPv6 addresses.

- Replacing a dual-stack MON, OSD, MDS and MGR with new containers is now
  tested: each replacement advertises concrete addresses of both families,
  the maps drop every entry of the replaced daemon, `MonitorBootstrapAddresses`
  follows the MON change, and a client that stays connected throughout
  reaches each replacement over the family its vector lists first. Docker
  can give a replaced daemon's IP address to a later container, so the docs
  now say to tell daemon instances apart by nonce or GID.

- Cutting one address family of one path of a dual-stack cluster is now
  documented and tested: `TemporaryNetworkDelay` with a daemon's IPv6 address
  as `Peer`, its port as `PeerPort` and `Loss: 100` leaves a MON command or an
  OSD read on that path pending while the same daemon serves over IPv4 and the
  client's other paths complete. After `Restore` the operation completes on
  the same connection, without a new client instance or a blocklist entry.

- `rbd.StartImmutableObjectCache` starts the RBD immutable object cache
  daemon in a client container and returns a lease with `ClientOptions`,
  `Status` (cached objects and bytes, the daemon's RADOS reads), `Stop` (clients
  fall back to RADOS) and `Restore`. It needs the daemon and the librbd
  `parent_cache` plugin in that container's image.

- `ceph.Container.ManagerMetrics` scrapes the active manager's `prometheus`
  module and returns its samples parsed from the text exposition format, with
  `Select` and `Value` to pick samples by name and labels. It never enables
  the module.

- `ceph.NetworkDelay.Peer` and `PeerPort` limit `TemporaryNetworkDelay` to
  the packets that the target sends to one address, and optionally one TCP
  port, over IPv4 or IPv6. The lease owns a `prio` root, a netem child and one
  u32 filter, reports `Applied` only for that whole hierarchy with the
  requested keys, exposes tc's filter dump in `NetworkDelayStatus.Filters`,
  and restores an incomplete hierarchy too.
- `ceph.NetworkDelay.Loss` drops a percentage of the delayed or selected
  packets, with or without a `Delay`. With `Loss: 100` and a `Peer` it cuts
  one direction of one path, and `NetworkDelayStatus.Loss` reads the kernel's
  value.

- `cephfs.Filesystem.TemporarySnapshotSchedule` adds a snapshot schedule and
  retention to a directory through the MGR `snap_schedule` module, with
  `Status`, `Snapshots` and `Restore` on the returned lease. The lease binds
  the schedule by its native start and creation time, and `Restore` resumes
  from the directory's current state. It never enables the module or minute
  granularity.

- `TestMapPropagationReceipt` records which map each daemon holds, from its
  own admin socket: every OSD's `status` after a new OSDMap epoch, and every
  MON's `mon_status` with the MonMap epoch, members, quorum and election epoch
  after a MON is added and removed
- `TestRGWRadosReceipt` follows S3 requests to the RADOS objects that the
  gateway writes: a small object's head object and bucket index key, a
  6 MiB object's head and tail stripe, and the tail left to the garbage
  collector after a delete.

- `TestCephFSClientKilledDuringUnsafeRequest` SIGKILLs the client while one
  rename is held between its unsafe and safe replies: after the journal is
  released the MDS commits and replies to the dead session, keeps the session
  `open` until `session_autoclose`, then evicts and blocklists it, and an
  independent client finds the new name. The
  [MDS sessions](docs/guides/messenger-client-fixtures.md#mds-sessions)
  guide lists the receipt
- `TestCephFSBlocklistEpochBarrier` evicts a live client that holds a
  buffered, unflushed write: the MDS's `osdmap_epoch_barrier` becomes the
  OSDMap epoch whose blocklist first holds the client, the client reports
  itself blocklisted at that epoch, drops its dirty data and fails fsync,
  write and stat with `ESHUTDOWN`, and a second live client reads only the
  fsynced bytes and receives the barrier with a flush ack.
  `TestCephFSOSDFullRetainedClient` fills the OSDs with `TemporaryFullRatios`
  under a live client: its write fails with `ENOSPC` while metadata calls
  succeed, and after `Restore` the same descriptor writes again.
  `TestCephFSFlockContention` lets two live clients contend for one flock:
  `EWOULDBLOCK` for the second, its blocking lock waiting in the MDS's
  `dump_ops_in_flight`, and the grant when the first unlocks, without an
  eviction.
- `TestCephFSOSDInterruption` freezes, with `PauseContainer` and `nodown`
  held, the OSD that holds a live client's file while the client fsyncs a
  buffered write and a second client makes a directory: the client's write
  and the MDS's journal writes wait in their `objecter_requests`, the
  `mkdir` waits in the MDS without `journal_committed`, both clients keep
  their sessions, and after `Resume` the same fsync and `mkdir` return 0 and
  other clients read every byte. It runs in its own
  `cephfs_fixtures_osd_interruption` batch.
- `TestCephFSFlockAcrossMDSFailover` SIGKILLs the active MDS while one live
  client holds a flock and a second one was refused it: the standby that
  takes the rank learns the lock from the clients' reconnects, refuses the
  second client with `EWOULDBLOCK` again, holds its blocking lock waiting and
  grants it after the unlock, and both clients keep their sessions.
  `TestCephFSJournaledRenameAcrossMDSCrash` SIGKILLs the MDS after a rename
  was journaled and replied but before it reached the directory's dirfrag
  object: the standby replays the journal (its `mds_log` counters show the
  replay), the client keeps its session, every client finds the new name with
  the original inode, and the dirfrag holds it after the new MDS flushes. Both
  run in their own `cephfs_fixtures_mds_failover` batch.
- `TestCephFSMDSOSDPathCut` drops only the MDS's packets to the
  filesystem's one OSD with a peer-selective `TemporaryNetworkDelay`
  blackhole. A live client's data write and data-only sync complete and RADOS
  holds the bytes, while the MDS's `objecter_requests` holds its journal
  writes, a second client's `mkdir` waits without `journal_committed` and the
  first client's full fsync waits with a flushing cap. Both sessions stay
  open, and after `Restore` the same calls return 0 and other clients read
  the result. It runs in the `cephfs_mds_path_fault` batch.
- `TestCephFSOSDRecovery` stops the OSD that holds a live client's file with
  `noout`, `norecover` and `nobackfill` held, starts it again and then
  releases recovery. The client's appends and fsyncs through one descriptor
  return 0 while the PG is `active+undersized+degraded` and while it is
  `recovery_wait`, the pools recover to `active+clean`, and the client and
  another client read every byte without an eviction. It runs in the
  `cephfs_fixtures_osd_interruption` batch.
- `TestCephFSSessionAdmission` records CephFS session admission: rank 0's
  FSID, MDSMap epoch, daemon, GID and v2 address as the FSMap and the
  client's admin socket report them; one native session's open, two
  acknowledged renewals and close without a blocklist entry; a path-restricted
  identity that mounts its path and is refused with `EPERM` elsewhere; and a
  required client feature that evicts a client without it, refuses its next
  mount with `EPERM` and still admits an ordinary client.
- `TestRBDMirrorPeerPathCut` cuts one path of an RBD snapshot mirror, the
  source OSD's packets to the destination's rbd-mirror daemon, with a
  peer-selective `TemporaryNetworkDelay` blackhole. The daemon stays up but
  does not see or copy a new mirror snapshot while the source client keeps
  writing through the same OSD; after `Restore` only the changed bytes are
  copied.
- `TestCephFSMirrorPeerPathCut` cuts one path of a CephFS snapshot mirror,
  the source OSD's packets to the cephfs-mirror daemon, with a peer-selective
  `TemporaryNetworkDelay` blackhole. The daemon sees a new snapshot through
  the MDS but stays `syncing` it without finishing while the source client
  writes, snapshots and reads through the same OSD and MDS; after `Restore`
  the snapshot arrives with only the new bytes copied.
- `TestCephFSFlushingCapReplay` follows one cap flush across a SIGKILL and
  restart of the only MDS: while `mds_log_pause` holds the journal, the
  client keeps an unacknowledged `Fw` flush with the new size and the MDS keeps
  the old one; after the restart the client's flush is acknowledged without
  a second fsync, the MDS has the new size and an independent client reads
  every byte.
- `TestCephFSDentryLeaseRecall` records a dentry lease recall between two
  live clients: a client without the root's `Fs` capability gets a lease on a
  directory it looks up, and another client's rename and `rmdir` of it each
  recall the lease at once (`num_leases` 1 to 0) without evicting the holder,
  which then sees the new namespace.
- `TestCephFSMetadataRecipe` records one native libcephfs client's metadata
  operations with the errno and native values of each: a `readdir` of 1500
  names over 8 directory fragments (one MDS readdir request each, every name
  once), file create, write and read, `chmod`, `chown`, `truncate` and
  `utime`, a hard link (the MDS's `dump inode` reports two links), a rename
  over an existing file, a symlink, `user.*` xattrs with a binary value, the
  expected `EEXIST`, `ENOENT`, `ENOTEMPTY`, `EISDIR`, `ENOTDIR` and `ENODATA`,
  and `flock` contention (`EWOULDBLOCK`); a client in another container
  checks the final namespace.
- `TestCephFSClientMDSPathCut` cuts one direction of a libcephfs client's
  MDS path with a peer-selected `Loss: 100`, the MDS's packets to the client
  or the client's packets to the MDS: a `mkdir` stays in the client's
  `mds_requests` while an append and a data-only `fsync` on an open file
  reach the OSDs, both sides keep the session `open` past a 30 s session
  timeout while `cap_ttl` stops moving, the MDS has the `mkdir` journaled in
  one direction and never sees it in the other, and after `Restore` the
  request completes without eviction.
- `TestOSDReplicationPeerPathCut` cuts one direction of one OSD pair on the
  cluster network, the primary's packets to its replica, with a peer-selected
  `Loss: 100`: the primary holds a client write `waiting for sub ops` past
  `osd_heartbeat_grace`, both OSDs report each other failed but stay up with
  one reporter each, the replica's packets still reach the primary, and after
  `Restore` the write completes and both copies read back.
- `TestOSDHeartbeatFailure` records OSD heartbeat failure detection: with one
  OSD's cluster network cut in a cluster of three, the MON marks it down after
  `osd_heartbeat_grace` on two reports from different hosts, while its peers
  stay up and clients keep working; the marked OSD fails to rebind its cluster
  messenger and exits, and starts again after `Restore`.
- `TestCephFSUnsafeRequestReplay` follows one rename that got an early,
  unsafe reply across a SIGKILL and restart of the only MDS: the client keeps
  the request while no MDS runs, the restarted MDS executes the client's
  `REPLAY` of the same client ID and TID, and an independent client reads the
  result. The [MDS sessions](docs/guides/messenger-client-fixtures.md#mds-sessions)
  guide lists the receipt and what it leaves out.
- `TestOSDBackoffReceipt`, in the new recovery batch `osd_backoff`, makes a
  primary send a PG-wide backoff to a retained python-rados client: with
  `noout` set and a non-primary replica of a replicas/min_size 3 pool stopped,
  the PG peers but cannot go active. The client's and the primary's
  `debug_ms` logs join BLOCK, ACK_BLOCK and UNBLOCK by connection, OSD, PG,
  backoff ID and range, and the original read returns the seeded bytes once
  the replica is back. The `client-osd/backoff` coverage row becomes partial;
  object-scoped backoffs stay uncovered (#5). See
  [OSD backoff](docs/guides/messenger-client-fixtures.md#osd-backoff).
- The feasibility report `docs/research/bluestore-block-faults.md` records a
  bounded probe of a test-owned sparse file, loop device and device-mapper
  mapping on a hosted Linux runner, without Ceph: separate read, write and
  flush delays, `EIO` from `error` and direction-specific `flakey` windows,
  verified restore with exact readback, and owned cleanup. A whole-device
  delay left `dmsetup resume` timed out after the kernel had installed the
  table. The report gives a conditional go for proving OSD attachment in a
  separate step (#7). The probe and its workflow are research tooling, not API.

### Changed

- `ceph.Container.InjectObjectDataError` moves to
  `fault.InjectObjectDataError(ctx, cluster, pool, object, osdID)`, following the
  maintainer decision on #9 that dedicated synthetic-error injection belongs in
  `fault`. This is a source-breaking move for the next minor release: there is
  no deprecated wrapper or alias in `ceph` (it would need `ceph` to import
  `fault`), and published release tags are unchanged. Behavior is unchanged,
  including the slash refusal for object names: shared-cluster admission with
  its existing policy, BlueStore, owned OSD, replicated pool and object
  mapping checks, topology serialization, partial-failure results, and the
  one-shot contract with no handle, registry entry or Restore. Recover with
  `RepairPG` or an OSD restart. See the [migration table](docs/fault.md).

- OSD launch preparation captures immutable input values and reconstructs fresh
  file Readers and request state. Copies of a launch share one creation-attempt
  guard, so an uncertain start is not replayed and an existing descriptor is
  not rebound. Partial containers remain owned for cleanup. This is an internal
  prerequisite for #6, without a public retained-store or replacement API.

- Network fault APIs use `fault.NetworkProfile`, `NetworkProfileLease` and
  `NetworkProfileStatus`, with `ApplyNetworkProfile`, `ApplyLinkProfile` and
  `rgw.Multisite.ApplyZoneLinkProfile`. This is a source-breaking rename of
  the corresponding delay names; fields, error categories, ownership and
  `Status`/`Restore` behavior are unchanged. See the
  [migration guide](docs/guides/upgrading-network-profile-names.md). No aliases
  or automatic expiry are introduced.

- Rename CephFS policy methods to `OverrideSessionTimeouts`,
  `OverrideSubvolumePin` and `OverrideSubvolumeGroupPin`, returning explicit
  `SessionTimeoutsOverrideHandle` / `PinOverrideHandle` restoration objects.
  Rename schedule creation to `AddSnapshotSchedule`, retaining
  `SnapshotScheduleLease`. Ownership, uncertainty and explicit Restore semantics
  are unchanged; see `docs/guides/upgrading-cephfs-names.md`.

- Rename `Container.TemporaryConfig`, `TemporaryOSDFlag`, `TemporaryMGRModule`
  and `TemporaryMessengerPolicy` to `OverrideConfig`, `OverrideOSDFlag`,
  `OverrideMGRModule` and `OverrideMessengerPolicy`; rename
  `Container.TemporaryClientRevocation` to `RevokeClient`. Rename their management
  types to `ConfigOverrideHandle`, `OSDFlagOverrideHandle`,
  `MGRModuleOverrideHandle`, `MessengerPolicyOverrideHandle` and
  `ClientRevocationHandle`. Handle state, explicit `Restore`, admission and
  uncertainty/retry semantics are unchanged. These next-minor source breaks add no compatibility aliases or
  automatic expiry; see `docs/guides/upgrading-ceph-policy-names.md`.

- Rename `Container.TemporaryFullRatios` to `Container.OverrideFullRatios`,
  retaining `ceph.FullRatiosOverride` and its retryable `Restore`. This is an
  OSDMap policy override in `ceph`, with explicit caller-managed restoration,
  no expiry and no undo on context cancellation. Query/value types and existing
  ownership/recovery semantics are unchanged. The next-minor source break has
  no compatibility alias; see `docs/ceph.md#overridefullratios`.

- Revert the client-session blocklist extraction into `fault`.
  `Container.TemporaryBlocklist` and `ceph.BlocklistOverride` stay in `ceph`
  as session-admission controls, preserving expiration ownership and retryable
  restoration. Network faults and container pause remain in `fault`.

- The CI commands `batchselect` and `tagcatalog` moved from `tools/` to
  `internal/tools/`. They only serve this repository's CI planner, so they
  are no longer listed as packages of the published module.
- `ceph.Run` starts the initial OSDs concurrently, at most four containers at
  a time, after registering them in layout order, so OSD IDs and CRUSH host
  buckets stay the same on every run. The first manager boots meanwhile. A
  three-OSD cluster on `quay.io/ceph/ceph:v20.2.4` starts in about 11 seconds
  instead of 17, a five-OSD cluster in about 16 instead of 27. When an OSD
  fails to start, every OSD container created so far is still owned and
  removed by `Terminate`. `AddOSD` is unchanged.

### Fixed

- Messenger targeted-command case receipts include the already exercised client
  toolkit and separately owned limited client in their immutable image cohort.
  Split-role receipts retain essential, client and cephfs IDs; aggregate images
  remain deduplicated. Missing executor image evidence refuses the receipt.

- `TestRGWPlacementRealmStorageClasses` no longer accepts replica bytes that
  arrive after a read's deadline. Its replica wait reads through the shared
  bounded observation with a 30-second read bound inside the unchanged
  four-minute budget, and a failure names the last sample: HTTP status, S3
  error code (`NoSuchKey` or `NoSuchBucket`), byte count and SHA-256 (#2).

- The same replica wait now ends at the next probe when the source gateway
  exits, is removed or restarts, instead of polling the rest of the
  four-minute budget. It binds the source container ID and process start time
  before the replicated write, inspects it read-only within 10 seconds per
  probe, and reports the last replica sample with the source state. Paused and
  restarting sources are named, not treated as crashes, and inspect errors are
  counted rather than read as healthy. Exact replica bytes remain the only
  success (#2).

- The failure diagnostics of `TestRGWPlacementRealmStorageClasses` read both
  gateways' container state before any admin query, give each state, admin
  and log read its own bound inside the 45-second total, and report a failed,
  timed-out or truncated read instead of dropping it. An admin query that
  stalls can no longer hide an exited source gateway (#2).

- Fence and drain public native commands for an OSD Docker instance before
  `RemoveOSD` mutates or purges it. Preserve retry ownership and its admission
  fence after an unconfirmed stop, without closing unrelated targets. This
  establishes one prerequisite for #6; no retained-store API is added.

- Refuse non-empty unready OSD stores before overwriting a keyring or invoking
  mkfs. Validate local stored numeric ID, OSD UUID and configured cluster FSID
  before same-container restart, and retain partial initialization for cleanup
  rather than replaying mkfs. This is a bootstrap-admission prerequisite for #6,
  not a retained-store/replacement-container API.

- `rgw.Multisite.WaitBucketSyncReady` returns the last status it read when the
  wait ends; an attempt that failed or was canceled afterwards no longer
  replaces it with an empty one. Its unit test no longer depends on a 20 ms
  deadline that the checkpoint lookup could outlast.
- `rgw.Gateway.Admin` no longer leaves `radosgw-admin` running in the gateway
  container after the call returned. It stops a command that is still
  running shortly before the call's bound and reports it as
  `ceph.ErrNativeFailure`. When the caller's context ends first, it stops the
  command before it returns, or returns a `*ceph.NativeCommandUnstoppedError`
  whose `Stop` retries.

- `ceph.Run` returns the engine's error when it cannot create the MON
  container, for example for an image that cannot be pulled, instead of
  panicking while it read the logs of a container that did not exist. Adding
  a MON or MGR to a running cluster had the same typed-nil handle on that
  failure.

### Documentation

- The design document `docs/design/fault-package.md` inventories the fault
  injection APIs for a separate `fault` package: which APIs and types move
  and which stay in `ceph`, the apply, undo, overlap, cancellation,
  shared-cluster and `Terminate` contracts each must keep, the internal
  boundary that the next stage adds, and the checkpoints of the later
  stages. No API changes yet; this is stage 1 of the fault-package extraction.
- The feasibility report `docs/research/crimson-cyanstore.md` records where
  Crimson OSD builds are published (only `main` development builds, not the
  Tentacle or Squid releases), a manual run in which a Crimson OSD with
  CyanStore crashes on its first client operation while Crimson with
  MemStore serves native client I/O unprivileged, and a no-go
  recommendation with the conditions for revisiting it.
- `InterruptNetwork` with `ceph.ClusterNetworkPlane` no longer promises that
  the OSD keeps running: with three or more OSDs on separate hosts it is
  marked down after `osd_heartbeat_grace` and exits, and needs `Start` after
  `Restore`.
- The Messenger client fixtures guide explains why a MON can issue tickets
  from replaced service keys right after `auth wipe-rotating-service-keys`,
  why native clients are not affected, and how a client of your own avoids
  it.
- The native command guide warns that a Tentacle MON at `debug_auth` 10 logs
  session keys and service secrets.
- The Messenger client fixtures guide shows how to read which sessions
  negotiated on-wire compression and which features each MON session
  negotiated, how to tell which OSD, PG or MDS answered a targeted command
  and how its refusals and timeouts reach the caller, and describes the
  coverage manifest that maps the ceph-msgr-go SPEC's environment
  requirements to the tests proving them.

### Continuous integration

- The tag planner no longer collects `ceph_bug_<id>` tags as capabilities,
  so a bug tag in a test constraint cannot enable a workaround in every
  profile. `tag_scenarios.py --bug-compatibility` takes an explicit selection
  and validates it against `docs/guides/bug-compatibility-tags.json`, which
  registers no tag yet. The ordinary and compatibility test files of a bug
  must keep the same test names in the same batch; the selection is part of
  each profile's tags, build key and history identity. No workflow passes a
  selection, so CI behavior is unchanged (#24).
- `tagged-runtime.yml` takes one `bug_compatibility` input, empty by default,
  and passes it as an argument to every planner command of the run. The Plan
  job refuses an invalid selection before any runtime job starts, and a
  runtime job whose recomputed profile tags differ from its matrix entry's
  fails before building. History rows record the selection, refuse receipts
  of another selection, and the job summary shows it beside the role image
  IDs. No caller passes the input, so CI behavior is unchanged (#24).

- `ci_measurements.py cohorts` joins measured runtime jobs to their saved
  history rows by repository, run ID, run attempt and job ID, and groups the
  joined attempts by `scenario_history.config_key`. Each configuration keeps
  an outcome ledger of attempts and runner seconds for every outcome, and
  timing ranges of its passed attempts only. Rows and jobs that do not join
  are listed with the reason (#1). `fetch` now stores the jobs of cancelled
  runs for that ledger, while `report` still leaves cancelled runs out.

- `ci_measurements.py fetch --cache-logs N` stores the cache lines of at
  most N job logs, and the new `caches` command lists a cache-state ledger
  keyed by repository, run ID, run attempt and job ID: cache kind (Go
  module, Go build, registry), exact key or reference, toolchain and
  platform, event, whether the operation was requested, and an outcome read
  only from log lines (`hit`, `partial-restore`, `miss`, `restore-failed`,
  `saved`, `save-skipped`, `save-failed` or `unknown`). A green job is never
  taken as a cache write. `cohorts` marks each sample with its job's cache
  condition and splits each configuration by it without changing its
  ranges (#1).

- A collaborator can run chosen runtime batches on a pull request with a
  comment line `/ci run <batch|profile>... [on <release|codename|all>]`.
  The `CI command` workflow (`ci-command.yml`, on `issue_comment`) accepts
  only comments of an owner, member or collaborator, refuses pull requests
  from forks, and refuses the whole command when one name is not a batch,
  profile or release of the head commit's planner
  (`tag_scenarios.py vocabulary`). It records the head commit, runs the
  selected profiles there through `tagged-runtime.yml` (the new category
  `selected` and `tag_scenarios.py plan --profiles`), sets a
  `CI command: ...` commit status and replies with the result and the run
  link. A repeated command for the same commit, batches and release waits for
  the run in progress. Default pull request CI, pushes to `main` and the
  `ci:full` and `ci:selected-batches` labels are unchanged.
- The declaration-level batch selection (`tools/batchselect`) no longer
  selects every batch for an added Go file of `internal/integration` or a
  changed import. It treats an added file's declarations as changed, and an
  import as changed code only when no other file of the build imports the
  same package. `tag_scenarios.py plan --apply-declarations` keeps only the
  selected batches, and `--summary` writes them with their reasons as
  Markdown.
- A pull request can opt in, with the `ci:selected-batches` label, to a Ceph
  short run of only the batches that the declaration selection selects; the
  `Plan` job summary lists every batch with its reason. The selection stays
  advisory: without the label, and on every push to `main`, Ceph short runs
  every batch. Measured on a one-batch change, a pull request ran 13 jobs and
  21 job minutes instead of 60 and 203; on a shared helper that five batches
  reach, 17 jobs and 35 job minutes instead of 60 and 200.
- `replay_declaration_selection.py --github` checks every failed batch job of
  the last merged pull requests against the selection of the diff that the
  run tested. An excluded failure is explained only by an entry of
  `batch_selection_explained.json` with its job ID, batch, signature,
  evidence and reference; a passing rerun is reported but does not clear a
  miss. Over 60 pull requests the selection kept 51% of the Ceph short jobs;
  of 31 failed batch jobs, 5 were selected and 26 are explained, the last
  two by a test that required a log line the `ceph` CLI can lose and
  by an I/O stall of the runner.

## v0.3.0 (2026-10-16)

The v0.3.0 release adds bounded native input and protocol observation
recipes. See [Upgrading to v0.3.0](docs/guides/upgrading-to-v0.3.md) for
compatibility and the limits of the completed fixture scope.
The receipts below are test helpers and native evidence, not new public
observer APIs or certification of a downstream Messenger implementation.

### Added

- `RunNativeCommand` can give a command a bounded, binary-safe standard input:
  `NativeCommand.Input` takes a `NativeInput` from `NewNativeInput`, whose
  `Write` sends data in order while the command runs and whose `Close` ends
  it, and `MaxInput` bounds the bytes in all. The supervisor keeps the
  command's deadline, cancellation and confirmed stop. The result reports the
  bytes written and delivered and whether the end of input reached the
  command. The transport does not put input in arguments, files, errors or
  diagnostics; command output remains raw, so a worker handling secrets must
  not echo them. `Complete()` still covers time and output limits; assert
  input delivery and the worker's receipt separately.

- The Messenger fixture guide explains how to tell which MON session belongs
  to a registered MGR. A MGR authenticates its beacon client, whose global
  ID is the MgrMap GID, and a client for each of its modules. A refusal in
  the MGR's own log therefore does not show that the registered slot lost
  its session; the global ID in the MON's refusal has to match the MgrMap
  GID. A test checks the sessions on every owned MON.

- The Messenger fixture guide has a MGR transition receipt. The active
  MGR's process stops and the captured standby takes over with the same GID,
  container and start time; the old process starts again and returns as a
  standby with a new GID. A phase is accepted only on a sample whose MgrMap
  did not move while it was taken, whose registrations each have their MON
  beacon session, and whose fresh quorum and `rbd_support` module command
  succeed. A query that did not complete keeps its status and is never read
  as an absence.

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
The completed fixture scope provides server-side profiles and native receipts;
it does not certify a
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
