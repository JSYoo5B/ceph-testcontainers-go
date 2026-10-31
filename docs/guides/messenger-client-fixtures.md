# Fixtures for Messenger client tests

## Introduction

A Messenger client implementation, such as a client written in another
language, is tested against native daemons: it authenticates with Cephx,
negotiates secure or CRC connections, renews tickets and survives daemons
that pause, stop and fail over. This guide maps those test environments to
the module's public options and lists what differs when each daemon runs in
its own container.

The profiles are derived from the ceph-msgr-go integration environment. Each
row names a test of the integration test packages that starts the profile and checks
it with native clients and daemon observations. The tests prove fixture
readiness, not a client's protocol implementation.

## Profiles

| Profile input | Values | Fixture options | Proven by |
| --- | --- | --- | --- |
| Image | Tentacle 20.2.4, Tentacle 20.2.3 | `ceph.Run(ctx, image, ...)` | every test below; `TestMessengerClientProfileWithoutKeyTypes` runs 20.2.3 |
| Messenger v1 | never bound or advertised | `NetworkConfig.WithMessengerMode(MessengerV2Only)`, or `MessengerV2Secure` | every profile |
| Monitors | three | `ClusterConfig.WithMonitorCount(3)` | every profile |
| Managers | two | `ClusterConfig.WithManagerCount(2)` | every profile but the next |
| | none at bootstrap, two started later | `ClusterConfig.WithNoInitialManagers`, then `AddManager("a")` and `AddManager("b")` | `TestMessengerClientProfileDelayedManagers` |
| OSD | none | `ClusterConfig.WithNoInitialOSDs` | `TestMessengerClientProfileSecure` |
| | one MemStore OSD | `ClusterConfig.WithOSDCount(1).WithPoolDefaults(1, 1)`, `OSDStorage.WithObjectStore(OSDMemStore)` | `TestMessengerClientProfileCRC` |
| CRUSH and pool | legacy tunables, a straw root, a rule that chooses OSDs; a pool with one PG and size 1; no pool with `hashpspool`, `.mgr` included | compile the map with `crushtool` and install it with `osd setcrushmap`; `osd_pool_default_flag_hashpspool = false` in a `WithConfigFile` `[global]` section; `CreatePool(ceph.PoolConfig{PGNum: 1, Replicas: 1, MinSize: 1, NoHashPSPool: true})` | `TestMessengerClientProfileCRC`, `TestOSDMemStoreLegacyPlacement`, `TestRADOSMutationContracts` |
| Key type / service cipher | `aes256k`/`aes256k` | `WithAuth(ceph.AuthConfig{KeyType: ceph.CipherAES256K})` | `TestMessengerClientProfileSecure` |
| | `aes`/`aes` | `AuthConfig{KeyType: ceph.CipherAES}` | `TestMessengerClientProfileModeRejection/managers` |
| | `aes`/`aes256k` | `AuthConfig{KeyType: ceph.CipherAES, ServiceCipher: ceph.CipherAES256K}` | `TestMessengerClientProfileCRC/bridge` |
| | `aes256k`/`aes` | `AuthConfig{KeyType: ceph.CipherAES256K, ServiceCipher: ceph.CipherAES}` | `TestAuthCiphersAndTicketLifetimes` |
| Ticket lifetimes | 12 s MON and service tickets | `AuthConfig{MonTicketTTL: 12 * time.Second, ServiceTicketTTL: 12 * time.Second}` | `TestMessengerClientProfileSecure` (one librados session outlives two lifetimes of each ticket) |
| | 120 s, with fast MON timing | `MonTicketTTL` and `ServiceTicketTTL` of 120 s, and `mon_subscribe_interval`, `mon_tick_interval`, `mon_session_timeout`, `mon_client_ping_interval` and `mon_client_hunt_interval` in a `WithConfigFile` `[global]` section | `TestMessengerClientProfileIdleTickets` |
| | 1.5 s MON tickets, 12 s service tickets | `AuthConfig{MonTicketTTL: 1500 * time.Millisecond, ServiceTicketTTL: 12 * time.Second}` and `mon_tick_interval = 1` in `[global]` | `TestMessengerClientProfileShortTickets`, optional, behind the `native_regression` build tag; see [Differences](#differences-from-a-single-container-bootstrap) |
| `global_id` reclaim | not allowed insecurely | always `auth_allow_insecure_global_id_reclaim = false` | `TestMessengerClientProfileSecure` |
| Connection modes | every connection secure | `MessengerV2Secure` | `TestMessengerClientProfileSecure`, which reads all six `ms_*_mode` options from each MON and MGR |
| | MONs, MGRs and OSDs accept only CRC from clients | `MessengerV2Only` with `NetworkConfig.WithMessengerPolicy(ceph.MessengerPolicy{Monitors: crc, Managers: crc, OSDs: crc})` | `TestMessengerClientProfileCRC` |
| | MONs only CRC, MGRs only secure | `MessengerV2Only` with `MessengerPolicy{Monitors: crc, Managers: secure}` | `TestMessengerClientProfileModeRejection/monitors` |
| | MONs only secure, MGRs only CRC | `MessengerV2Only` with `MessengerPolicy{Monitors: secure, Managers: crc}` | `TestMessengerClientProfileModeRejection/managers` |
| Client modes | secure, CRC, or secure then CRC | `ClientContainerOptions.WithMessengerPolicy` on a `WithClient` or `ClientConfig.WithIdentity` container | `TestMessengerClientProfileModeRejection`, `TestMessengerClientProfileCRC` |
| Address family | IPv4 | the default | every other profile |
| | IPv6 | `NetworkConfig.WithAddressFamily(ceph.AddressIPv6)` | `TestMessengerClientProfileAddressFamilies/ipv6` |
| | IPv4-mapped IPv6 | `NetworkConfig.WithAddressFamily(ceph.AddressIPv4MappedIPv6)` | `TestMessengerClientProfileAddressFamilies/ipv4-mapped` |
| | IPv4 and IPv6 on every daemon | `NetworkConfig.WithAddressFamily(ceph.AddressDualStack)`; MON vectors list IPv6 first, the others IPv4 first (see [Dual-stack clusters](ceph-configuration.md#dual-stack-clusters)) | `TestDualStackAddressFamily`, `TestDualStackSeparateClusterNetwork` |
| Where the tests run | a container on the cluster network | `WithClient` or `ClientConfig.WithIdentity` | every profile |
| | a host process | `WithHostNetwork` (see [Host network](host-network.md)); every advertised MON, MGR and OSD address is reachable from the host | `TestMessengerClientProfileCRC/host` |
| Client identities | full access, read-only, revocable, and one limited to the `fsid` command | `CreateClient` with `ceph.ClientCaps`, for example `{Mon: "allow command \"fsid\"", MGR: "allow r"}`; keys get the cluster's `KeyType` | `TestMessengerClientProfileSecure` |

`crc` and `secure` abbreviate `ceph.MessengerCRC` and `ceph.MessengerSecure`.

While any pool has `hashpspool`, the MON refuses a client without the
`OSDHASHPSPOOL` feature (`0x40000000`) at its MON session. `NoHashPSPool`
clears the flag only on the pool that `CreatePool` creates, and Ceph creates
`.mgr` with the MON's defaults, so turn the default off at bootstrap with
`osd_pool_default_flag_hashpspool = false`. Check every pool's flags with
`Observer.Pools` and `Pool.Status`.

An image without `ceph-authtool --key-type`, such as Tentacle 20.2.3 or Squid,
makes only AES keys. `Run` refuses a profile that needs `aes256k` with
`ceph.ErrUnsupported` before any daemon starts; it does not fall back to AES.

### Readiness

`Run` returns once a MGR is available, or, with `ClusterConfig.WithNoInitialManagers`, once
the MONs have a quorum and the initial OSDs are up and in. To wait for every
MON and a standby MGR, poll `QuorumStatus` until it lists all three MONs and
`ManagerStatus` until it reports an active MGR and one standby.

## Daemon controls

| Control | Fixture and trap | Proven by |
| --- | --- | --- |
| Pause a MON or a MGR and resume it | `fault.Pause` and `fault.PauseLease.Resume`. The paused socket still accepts TCP but sends nothing; a paused MON leaves the quorum to the other two. A MON paused longer than the MON ticket lifetime can strand a daemon whose MON session was on it: its reconnect is refused with `EACCES` and its MON client stops authenticating. Check MGR-dependent behaviour before such a pause, or restart the stranded daemon. | `TestMessengerClientProfileSecure` |
| Fail over the active MGR | `cluster.Ceph(ctx, "mgr", "fail", name)`. The failed MGR restarts and registers as a standby again within seconds; wait until `ManagerStatus` lists it before the next control. A daemon that must authenticate again after the MONs rotated away its ticket's secret is refused with `EACCES` and can stay unregistered while its process runs; `Stop` and `Start` on its `Managers(ctx)` handle bring it back. See [MGR MonClients](#mgr-monclients) before treating a refusal as that case. | `TestMessengerClientProfileSecure` |
| Tell which MON session belongs to a registered MGR | `AdminSocket(ctx, "sessions")` on each `*ceph.MonitorContainer`; see [MGR MonClients](#mgr-monclients) | `TestMessengerClientProfileManagerMonClients` |
| Stop one MGR and start it again | `Managers(ctx)`, then `Stop` and `Start`. `Start` waits only for the admin socket; see [MGR transition receipt](#mgr-transition-receipt). | `TestMessengerClientProfileSecure`, `TestMessengerClientProfileManagerTransition` |
| Restart one MON | `Monitors(ctx)`, then `Stop` and `Start` | `TestMessengerClientProfileSecure` |
| Stop every MON and start them again | Start `b` and `c` before `a`: MON a's container checks `ceph status` when it starts, which needs a quorum. Each MON comes back at its monmap address. | `TestMessengerClientProfileSecure` |
| Stop MON a for good | `Stop` on its handle. The fixture's CLI runs in a separate control container and keeps working. | `TestMessengerClientProfileSecure` |
| Start MGRs later | `AddManager` | `TestMessengerClientProfileDelayedManagers` |
| Add a MON that never answers, then remove it | `cluster.Ceph(ctx, "mon", "add", name, addr)` with an address nothing listens on, later `mon remove`. Each monmap change starts an election that waits for the missing member, so MON commands and new client sessions stall for about 10 s. Wait until `quorum_status` reports the new monmap and the same election epoch twice, and give new native clients longer timeouts. While the member is in the monmap, `RefreshClientMonitorConfig` would write its address into `mon_host`. | `TestMessengerClientProfileUnreachableMonitor` |
| Make the MGRs accept only secure, then restart them | `override.SetMessengerPolicy(ctx, ceph.MessengerPolicy{Managers: secure})`, then `Stop` and `Start` each MGR. The mode survives the restart; `Restore` returns them to CRC. | `TestMessengerClientProfileModeRejection/managers` |
| Revoke a client key and restore it | `ClientConfig.Revoke` and `ClientConfig.ReplaceKey` | `TestClientRevocation`, `TestClientKeyReplacement` |
| Run native oracles | `python3-rados`, the `ceph` CLI, `crushtool` and `ceph-dencoder` in a `WithClient` container, or `RunNativeCommand` | `TestNativeCommands`, `TestRADOSMutationOutcomes`, `TestMessengerClientProfileCRC`, `TestOSDMemStoreLegacyPlacement` |

### MGR MonClients

A MGR process authenticates several MonClients under one entity name. Its
beacon client opens the MON session of type `mgr`, and its global ID is the
GID in the MgrMap. The active MGR's modules open sessions of type `client`
with other global IDs; a standby runs no modules.

A `(13) Permission denied` line in a MGR's log therefore does not say which
client was refused. The registered MGR has lost its MON session only when an
owned MON logs `attempt to reclaim global_id <N> using bad ticket` (or another
refusal naming the global ID) for N equal to the MgrMap GID, and the MGR then
drops out of the MgrMap. The MON logs are each MON container's output;
`CollectDiagnostics` keeps a bounded tail. Compare global IDs, not entity
names.

### MGR transition receipt

`testManagerTransitionReceipt` in
`internal/integration/manager_transition_integration_helpers_test.go` records
which daemon took over a MGR failover and which registration belongs to which
process. Copy it into your tests. It runs with three MONs, two MGRs and no
OSDs, in three phases:

1. Baseline: one available active and one standby. Freeze the FSID, MON
   names, both registrations (name and GID) and both containers' IDs and start
   times.
2. Promotion: `Stop` on the active's handle. Wait until the captured standby
   is active with the same GID, container ID and start time, and the stopped
   MGR has left the MgrMap. This takes over 30 s, so size the deadline for it.
3. Restoration: `Start` on the same handle. Wait until the old MGR is the only
   standby, with a new GID, the same container ID and a new start time, while
   the promoted MGR stays active. The old MGR need not become active again.

Each phase samples until one sample is accepted. A sample reads the MgrMap,
the `sessions` of every frozen MON, each MGR container's inspection,
`quorum_status`, `ceph rbd task list` (a MGR module command) and the MgrMap
again, each within its own bound. `classifyMgrSample` judges it:

- `incomplete`: a query did not complete (`missing`, `timeout`,
  `parse_failure`, `truncated`, `native_rejection` or `failed`). None of these
  is an observed absence.
- `transitional`: the two MgrMaps differ; take another sample.
- `violated`: the standby's container, start time or GID changed, an unowned
  MGR is active, the FSID changed, or the old MGR's container was replaced or
  kept its old GID after restart. The phase fails at once.
- `pending`: a condition is not met yet.
- `accepted`: every required registration has its beacon session (type `mgr`,
  global ID equal to the MgrMap GID) on some MON. Module clients do not count.

The test logs `MGR_TRANSITION_RECEIPT` with each phase's window, sample counts
by state and the last four samples, trimmed to epoch, availability and
registrations. The receipt does not show connection modes or follow OSD, MDS
or RGW daemons.

### Authentication first-failure bundle

When a client's first authentication to a MGR is refused, for example after a
rotating-key wipe, keep that rejection and collect what the stock images can
observe. Do not retry, wipe again, restart a daemon or wait for a pass: each
replaces the first failure with a different run.

`collectAuthFirstFailureBundle` in
`internal/integration/auth_first_failure_integration_helpers_test.go` is the
recipe; copy it. Right after the rejection, freeze the FSID, MON names and the
target MGR's name, GID, container ID and start time. Pass them with the
rejection, your provenance, a held session if any, and your installed tickets
with opaque run-scoped labels and the epochs their issuer connection showed
(the ticket does not carry them).

| Field | Source | Meaning |
| --- | --- | --- |
| `monitors` | `AdminSocket(ctx, "mon_status")` per expected MON | rank, state, election epoch, quorum, MonMap epoch, `auth_epoch` (Tentacle only) and FSID, without a MON session |
| `quorum_names` | `QuorumStatus` | the quorum as a MON session reports it |
| `managers`, `managers_after` | `mgr dump`, `Managers`, container start times, MON `sessions` | each MGR's GID and container instance before and after collection, with beacon and module sessions split |
| `native_controls` | `RunNativeCommand` of `ceph tell mgr.<name> version` for the target and the active MGR | whether a new client authenticates with its own ticket |
| `queries` | every query above | name, kind, times and status: `complete`, `unsupported`, `timed_out`, `truncated`, `failed` or `unavailable`, with a redacted excerpt of at most 1 KiB |
| `first_failure`, `held_session`, `consumer_tickets`, `provenance`, `expected_target` | your records | stored as given |
| `generation_equality` | | always `unavailable/unverified` |

The collection has one total budget and a bound per query, runs from its own
context, and does not retry or stop on a failed query. Empty or non-JSON
output, or a status without name, FSID or MonMap epoch, is a failed query; an
empty session list is an answer. An unknown command is `unsupported`. Only
the cleanup of a `RunNativeCommand` that could not be confirmed stopped has a
separate allowance; its failures go to `cleanup_errors`.

`classifyAuthBundle` reports completeness and its problems; whether the target
was `registered`, `changed`, `absent` or `unavailable`; which client a MON's
refusal named (registered MGR, module client or unknown); secret IDs shared
across runs (a wipe restarts them, so equal IDs are not the same key); and the
controls and rejection. It never reports generation equality: no stock command
shows which rotating key a MON or MGR used without the keys themselves.

`checkAuthBundleSecrecy` refuses a bundle holding a secret you pass or a field
beyond the schema. Do not add `debug_auth` output, which prints keys (see
[Observing Cephx keys from the daemon side](native-commands.md#observing-cephx-keys-from-the-daemon-side)).

`TestAuthFirstFailureBundle` runs the recipe after one
`auth wipe-rotating-service-keys` (Squid's MONs lack it, so the test collects
without a wipe), with a native client using a wrong key, plus bundles with an
exhausted budget and a truncated control. `TestAuthBundleRetainedFailure`
replays a retained consumer failure, and `TestAuthBundleClassification`
changes one fact at a time.

### Authenticating right after a rotating-key wipe

Right after a wipe, a MON can briefly still issue tickets sealed with the
replaced service keys, and a MGR that holds the new keys refuses them with
-13 (`could not decrypt ticket info`). On Tentacle 20.2.4 the wipe commits new
rotating keys and a new auth epoch together. A MON applies the commit under
its main lock and refreshes the MonMap, which already reaches subscribers,
before the auth state. Authentication of a new connection takes only the auth
lock, so it can issue old-key tickets until the auth state follows. Native
MonClients renew inside the open session with `MAuth`, handled under the main
lock, and are not affected.

A client that renews on a new MON connection as soon as it sees the new
MonMap can land in that window. For a client of your own:

- record, per MON, the auth epoch of the first MonMap of each new session
  (sent under the main lock, so the auth state of that epoch is applied);
- discard renewal tickets from a handshake that started before the MON showed
  the expected epoch, and authenticate once more;
- do not wait a fixed time instead, because the window has no bound.

## Mutation oracles

The module wraps no data APIs. These tests run native recipes with
`python3-rados` (and `ctypes` for librados calls the binding lacks) in a
client container, and read every result back with a second client.

| Contract | Native receipt | Proven by |
| --- | --- | --- |
| write at an offset, `write_full`, append | the exact bytes | `TestRADOSMutationOutcomes`, `TestRADOSSelfManagedSnapshots` |
| zero | the range reads as zeros, size unchanged | `TestRADOSMutationContracts` |
| truncate, remove | the new size; `ENOENT` after a remove | `TestRADOSSelfManagedSnapshots`, `TestRADOSMutationContracts` |
| create | exclusive create of an existing object fails with `EEXIST` (17), also inside a compound write, which then changes nothing; idempotent create succeeds | `TestRADOSMutationContracts` |
| version guard | `assert_version` older fails with `ERANGE` (34), newer with `EOVERFLOW` (75); a refused write still takes a PG version | `TestRADOSMutationOutcomes` |
| compare guards | failed `cmpext` returns `MAX_ERRNO` (4095) plus the first differing offset; failed `omap_cmp` and `cmpxattr` return `ECANCELED` (125); the whole compound op is refused | `TestRADOSMutationContracts`, `TestRADOSClientFixtures` |
| acknowledged outcome | a write that returned 0 survives SIGKILL of a BlueStore OSD | `TestRADOSMutationOutcomes` |
| unknown outcome | the client's timeout ends a delayed append, truncate, remove or guarded compound write with `ETIMEDOUT` (110); the OSD started and applied each exactly once afterwards | `TestRADOSMutationOutcomes`, `TestRADOSMutationContracts` |
| no automatic re-execution | an unguarded retry of an uncertain append appends again; a retry guarded by the earlier version is refused | `TestRADOSMutationOutcomes` |
| killed client | the OSD discards an op whose connection closed before it started | `TestRADOSMutationOutcomes` |
| OSD restart | SIGKILL and restart with BlueStore; clean restart with MemStore | `TestRADOSMutationOutcomes`, `TestOSDMemStoreLegacyPlacement` |
| xattrs | `getxattrs` lists binary and empty values; `getxattr` of a removed name is `ENODATA` (61); `rmxattr` of an unset name succeeds with a new version; a missing object is `ENOENT` (2) | `TestRADOSObjectReceipts` |
| OMAP | keys-only listings carry no values; `max_return` pages continue with `start_after` until an empty page, capped at `osd_max_omap_entries_per_request`; by-keys omits absent keys; clear also empties the header | `TestRADOSObjectReceipts` |
| rollback | to a self-managed snapshot restores bytes, xattrs and OMAP at a new version; an object created after the snapshot is deleted | `TestRADOSObjectReceipts` |
| hints | `set-alloc-hint` keeps existing bytes and creates a missing object empty; fadvise flags change no result | `TestRADOSObjectReceipts` |
| sparse read | the `rbd` CLI exports an image through librbd, which reads each 4 MiB data object with `CEPH_OSD_OP_SPARSE_READ`; `TestRBDSparseReadReceipt` joins each request, the OSD's reply and the client's receipt, a written object returning only its extent and the extent map and an absent one `-ENOENT`. python-rados and the `rados` CLI issue no sparse read | `TestRBDSparseReadReceipt` |
| statistics | pool statistics and `ceph df` follow known object sizes exactly; cluster statfs matches `ceph df` | `TestRADOSObjectReceipts` |

`TestRADOSMutationContracts` runs on the CRC MemStore profile above. A
MemStore OSD loses its data when killed, so crash durability needs BlueStore,
as in `TestRADOSMutationOutcomes`. OSD-side delays are in
[Fault injection](fault-injection.md).

## PG enumeration

A client lists a pool PG by PG with `CEPH_OSD_OP_PGNLS`: each request carries
a cursor, and the OSD returns at most `osd_max_pgls` entries, a handle to
continue from, and result 1 when the PG has no more objects.
`TestRADOSObjectReceipts` logs `RADOS_ENUMERATION_RECEIPT` for a 4-PG pool
with `osd_max_pgls` lowered to 3, with namespaces `enum-a` and `enum-b` and an
xattr `tag=blue` on some objects. It holds:

- `placement`: the objects of each PG by namespace;
- `listing`: every PGNLS request of `rados -N enum-a ls` from the CLI log
  (`debug_ms=1`, `debug_objecter=20`). PGs come in bitwise hash order (`5.0`,
  `5.2`, `5.1`, `5.3`) and each cursor is the previous handle. The OSD counts
  objects of other namespaces against the limit but leaves them out of the
  entries, so a continuation can hold fewer entries than the limit. The PG's
  last reply has result 1 and the next PG's start as handle (such as
  `5:40000000::::head`), `MAX` after the last PG;
- `per_pg_cli` and `all_namespaces`: `rados --pgid` and `rados --all ls`;
- `cursor`: `rados_object_list` over each `rados_object_list_slice` range, in
  calls of at most 2 entries with the filter `plain`, `_tag`, `blue` (the OSD
  stores user xattr `tag` as `_tag`), with cursor comparisons and the
  `pgnls-filter` requests on the wire;
- `filter.unknown`: an unknown filter type, refused with -22 (`EINVAL`).

A filter buffer is the type, then its arguments, each a string with a 32-bit
little-endian length; the client prefixes it with `pg` and `filter`. No image
tool sends the legacy `CEPH_OSD_OP_PGLS`.

## MDS sessions

These tests keep a native libcephfs client mounted in a client container under
`RunNativeCommand`, with a descriptor opened before the event, and publish its
client ID, PID and inode in run-scoped receipts. `Sessions` reports the
session; `ceph tell mds.<name> session ls` adds counters such as `num_leases`
and `num_completed_requests`. Every test runs with `MessengerV2Secure`.

| Profile | Native receipt | Proven by |
| --- | --- | --- |
| The only MDS is killed, its GID failed, its container started again | rank 0 returns with a new GID; the session is `open` again with the same client ID, PID, address and nonce; the old descriptor reads, appends, fsyncs, renames and sets an xattr, and another container sees the result | `TestCephFSRetainedClientAcrossMDSRestart` (bridge and host) |
| The active MDS is killed and an idle standby or a `StandbyReplay` follower takes the rank | the same, with the standby under its earlier GID; the killed daemon rejoins as standby with a new GID, which `WaitReady` waits for | `TestCephFSRetainedClientAcrossStandbyTakeover/standby` and `/standby-replay` |
| Two active ranks; the file is in a directory pinned to rank 1 with `ceph.dir.pin`, and rank 1's MDS fails | export and import counters record the move and rank 1 holds the subtree and a session; the standby takes rank 1; rank 0 keeps its session | `TestCephFSRetainedClientAcrossRankOneTakeover` |
| A second client opens and writes a file that a live client holds with exclusive buffered caps | `dump inode` shows the holder as loner with `Fsxcrwb`; the second client finishes far below `session_timeout` and reads the fsynced bytes; while both hold it, both have `pAsLsXsFrw`; nobody is blocklisted | `TestCephFSCapabilityHandoff` |
| A client looks up a directory that another client created, then renamed and removed | the MDS answers the lookup with a dentry lease (`num_leases` 1, dentry leased in `dump cache`); the rename and `rmdir` each recall it within milliseconds, the client sees `ENOENT` for the old name. A client mounted after the directory exists holds `Fs` on `/` and gets no lease | `TestCephFSDentryLeaseRecall` |
| Mount and unmount with `session_timeout` 30 s; a client with `allow rw path=/admitted` mounts `/admitted`, `/` and `/other`; a client without `metric_collect` (`client_debug_inject_features`) while `fs required_client_features` requires it | `fs dump` names rank 0's GID and v2 address; before the mount the client has GlobalID 0 and no session, after it one `open` session matching the client's `mds_sessions`; `cap_renew_seq` advances and `cap_ttl` is `last_cap_renew_request` plus 30 s; after unmount the session is gone, not blocklisted. `/` and `/other` fail with `EPERM` (1). Requiring a feature evicts and blocklists a mounted client lacking it, and refuses new ones with `EPERM` | `TestCephFSSessionAdmission` |
| A client frozen with `fault.Pause` while a standby takes rank 0, `mds_reconnect_timeout` 10 s | the standby passes `up:reconnect`, `up:rejoin`, `up:active` (read from `fs dump`; `MDSStatus` lists only active ranks); the frozen session is gone and blocklisted; after `Resume` the descriptor fails with `ESHUTDOWN` (108) on Tentacle and `EPERM` (1) on Squid | `TestCephFSLateReconnect` |
| Two active ranks; the client appends fsynced records while another client moves the directory's pin to rank 1 and back | rank 1 becomes the only authority in `get subtrees` of both ranks; export and import counters grow; records keep being acknowledged; the client has sessions on both ranks and reads every record in order | `TestCephFSLiveRankMigration` |
| A rename while `mds_log_pause` holds the journal | the rename returns with the early reply; `dump_ops_in_flight` holds it after `early_replied` without `journal_committed`, and the client's `mds_requests` holds the TID with `got_unsafe`; after restore it is committed | `TestCephFSUnsafeRequestWindow` |
| The same held rename, then SIGKILL of the only MDS and a restart without the pause | the client still holds the TID; the new MDS's `dump_historic_ops` shows the same `client.<id>:<tid> rename` flagged `REPLAY`, without `early_replied`; the new name has the original inode | `TestCephFSUnsafeRequestReplay` |
| The same held rename, then SIGKILL of the client, `session_timeout` 30 s, `session_autoclose` 60 s | after restore the MDS commits the dead client's rename; the session stays `open` until `session_autoclose` and is then blocklisted | `TestCephFSClientKilledDuringUnsafeRequest` |
| An append and fsync while `mds_log_pause` holds the journal, then SIGKILL of the MDS and a restart | the client holds a flushing `Fw` cap with a flush TID and the full size while the MDS has the base size; after the restart the original fsync returns 0 and the MDS has the full size | `TestCephFSFlushingCapReplay` |
| Two clients contend for `flock(LOCK_EX)` | non-blocking returns `EWOULDBLOCK` (11); blocking waits in `dump_ops_in_flight` as `setfilelock rule 2, type 2, ... wait 1` at `failed to add lock, waiting` and returns 0 after `LOCK_UN`. `dump inode` does not list flocks | `TestCephFSFlockContention` |
| The same lock held across a standby takeover | both sessions stay `open`; the contender is still refused with `EWOULDBLOCK` and its blocking lock waits on the new MDS until `LOCK_UN` | `TestCephFSFlockAcrossMDSFailover` |
| Two `ceph-fuse` mounts (`/dev/fuse`, `CAP_SYS_ADMIN`, `apparmor=unconfined`) contend for an `F_WRLCK` byte range; then the holder's `ceph-fuse` is killed | `F_SETLK` returns `EAGAIN` (11) and `F_GETLK` reports the holder's range; `F_SETLKW` waits as `setfilelock rule 1` (fcntl). After the kill the mount returns `ENOTCONN` (107) and the dead client's lock refuses the contender until its session is evicted. Logs `CEPHFS_FCNTL_LOCK_RECEIPT` | `TestCephFSFuseMount`, in the optional batch `cephfs_fuse` (see [CI](../contributing/ci.md#native-regressions)) |
| A journaled rename, then SIGKILL of the active MDS and a standby takeover | before the crash the rename is committed and replied while the dirfrag object (`<dir ino>.00000000`) still holds only the old name; the standby's `mds_log` shows `replayed` above 0; the new name survives; after `flush journal` the dirfrag holds only the new name | `TestCephFSJournaledRenameAcrossMDSCrash` |
| `client evict` of a client holding unfsynced data (`client_oc_max_dirty_age` 600 s) | the blocklist entry appears in OSDMap epoch E and the MDS's `osdmap_epoch_barrier` is E; the evicted client reports `blocklisted`, its fsync, write and `stat` return `ESHUTDOWN` (108), a second fsync returns 0 because the dirty data was dropped; another client reads only fsynced bytes and its own barrier becomes E | `TestCephFSBlocklistEpochBarrier` |
| Full ratios lowered with `override.SetFullRatios`, then `Restore` | once the client holds the full map, writes return `ENOSPC` (28) at once while fsync, `mkdir` and `stat` return 0; after restore the same descriptor writes again | `TestCephFSOSDFullRetainedClient` |
| Two OSDs, `nodown` held, the acting primary of the file frozen with `fault.Pause` | the client's fsync waits in `objecter_requests`; another client's `mkdir` (no early reply for a new inode) waits at `submit entry: journal_and_reply` while the MDS's journal writes (`200.*`) wait; sessions stay `open`; after `Resume` both return 0 | `TestCephFSOSDInterruption` |
| `ApplyNetworkProfile` on the MDS's container with `Peer` the OSD's address and `Loss: 100` | data writes and data-only syncs complete and RADOS holds the bytes; the MDS's journal writes wait, so a `mkdir` and a full fsync's cap flush wait; sessions stay `open`; after `Restore` both return 0. Logs `CEPHFS_MDS_OSD_PATH_RECEIPT` | `TestCephFSMDSOSDPathCut`; needs the netem helper |
| The same against two OSDs with one `Peers` profile | the same, with one u32 filter per OSD (`800::1`, `800::2`) | `TestCephFSMDSOSDsPathCut`; needs the netem helper |
| Two OSDs, `min_size` 1, `noout`, `norecover`, `nobackfill` held; the file's primary stops, starts, then flags are released | with the OSD down the PG is `active+undersized+degraded` and writes return within milliseconds; back up it is `recovery_wait` or `backfill_wait`; after release all pools are `active+clean`. A client write to a missing object recovers it even under `norecover`, so do not assert the degraded count before release. Logs `CEPHFS_OSD_RECOVERY_RECEIPT` | `TestCephFSOSDRecovery` |
| An administrator sets `ceph.quota.max_files` on a live client's directory to its recursive entry count, then one more | the client's own `getxattr` reads each limit; a create returns `EDQUOT` (122) at the limit and succeeds with one more slot; another client reads both files | `TestCephFSLiveQuota` |
| An administrator sets `ceph.quota.max_bytes` 4 MiB on a live client's directory, then removes it | once the client reads the limit, its 64 KiB write and fsync series ends in `EDQUOT` after every chunk below the limit and within 32 MiB beyond it, because the client enforces against the recursive bytes the MDS last sent; another client reads exactly the acknowledged bytes; after removal the same descriptor writes again | `TestCephFSLiveByteQuota` |
| Two clients with `client_collect_and_send_global_metrics`, one writing 128 KiB and fsyncing, one idle | `counter dump` of the active MDS has a `mds_client_metrics-<fs>` row per client and rank: both are zero before the write, then the writer's `total_write_ops`/`total_write_size` cover the acknowledged bytes and the idle row stays zero; the totals are cumulative, not message counts | `TestCephFSClientMetrics` |
| A client holding a flock is SIGKILLed; fresh handles call `ceph_start_reclaim` with `CEPH_RECLAIM_RESET` | the dead client's session and lock stand until the RESET; RESET of an unused UUID returns `-ENOENT` (-2); RESET of its UUID returns 0, its session disappears and its lock frees while an unrelated session and lock stay; the replacement mounts as a new client and reads the bytes. Locks and capabilities are not carried over | `TestCephFSReclaimReset` |

What a client of your own must do, by receipt:

- Restart and takeover receipts reconnect a session whose data was fsynced
  before the failure; they do not replay an in-flight request.
- Unsafe reply: a request that got an early reply is executed. If the MDS
  crashes before journaling it, the client must resend it flagged as a replay
  during reconnect, and the MDS executes it once. If the client dies instead,
  the MDS still commits it, so treat the operation as done. A request that
  allocates an inode, such as `mkdir`, gets no early reply. The client lists
  pending requests as a `requests` array on Tentacle and one `request` object
  each on Squid (`TestRequestSafety` and `TestReplayedRequest` keep native
  samples).
- A safe reply is durable across an MDS crash: the standby replays the
  journal, so do not resend.
- Cap flushes: the new size travels in a cap flush. Keep unacknowledged
  flushes with their TID and resend them on reconnect, or readers see only the
  base size. `TestParseClientCache` keeps `dump_cache` samples; only the entry
  with the head snapshot ID is the inode.
- Locks: the MDS keeps flocks in memory only. Send held locks with each
  inode's caps in the reconnect, or the new MDS grants them to the next
  contender. Waiting requests are not carried over. `cephfs.LibCephFS` cannot
  take `fcntl` locks, so those come from `ceph-fuse` and are not tested across
  a failover.
- Eviction: the MDS sends its epoch barrier with capability messages. A
  client that gets it waits for that OSDMap before more OSD ops. An evicted
  client must fail its writes and drop unfsynced data, as the native client
  does with `ESHUTDOWN` before anything reaches an OSD.
- Fullness is decided in the client from the map. An idle client may not
  fetch the new map, so the test reads uncached data to receive it.
- OSD stalls: neither client nor MDS times out an OSD op by default. Block the
  call, keep the session, and complete the same descriptor's write when the
  OSD answers. Without `nodown` the frozen OSD is marked down after the
  heartbeat grace (`TestPausedOSDFaults`). After a network `Restore`, held TCP
  segments go out on the backed-off retransmission timer, so allow tens of
  seconds.
- A dead client whose caps nobody wants stays `open` until
  `session_autoclose`; one whose caps are wanted is evicted after
  `session_timeout` (`TestCephFSPausedClientEviction`).
- Recovery is invisible to the client: writes need only `min_size` copies.
  Backfill after a long outage, an OSD returning empty, and PGs below
  `min_size` are not covered.

### Metadata operations

`TestCephFSMetadataRecipe` runs one libcephfs client (Python `cephfs`) on one
active MDS and records, per operation, the errno and what the binding
returned. It logs `CEPHFS_METADATA_RECIPE`.

| Operations | Native receipt |
| --- | --- |
| `mkdir` and 1500 files, `opendir`, `readdir` | with `mds_bal_split_size` 1000 the directory splits into 8 fragments; the listing returns `.`, `..` and every name once, with one readdir request per fragment |
| `open` with `O_CREAT`, `write` 1000 bytes, `fsync`, `fstat`, `stat`, `lstat`, `read` | one inode, mode `100644`, one link, the written size and bytes |
| `chmod 0640`, `chown 4242:4343`, `truncate` to 600, `utime` | `stat` returns those values on the same inode (times in whole seconds) |
| `link` | both names, one inode, two links |
| `rename` over an existing file | the target has the source's inode; the source is `ENOENT`; the old target sits under `~mds0/stray<N>` |
| `symlink`, `readlink`, `lstat`, `stat` | the target string, mode `120777`, size equal to the target's length; `stat` follows |
| `user.*` xattrs with a binary value | exact bytes; `listxattr` lists only set names; a missing name is `ENODATA` (61) |
| errors | `EEXIST` (17), `ENOENT` (2), `ENOTEMPTY` (39), `EISDIR` (21), `ENOTDIR` (20) for the usual cases |
| `flock` with two owners in one client | the second owner gets `EWOULDBLOCK` (11) until the first unlocks |

A client in another container compares every name and attribute. libcephfs
asks for one fragment per readdir and a reply holds at most about 512 KiB, so
the test splits the directory to get several requests.

## Daemon-to-daemon paths

A client test that follows a request past its first daemon needs to know
which internal paths a fixture exercises. *Native state* means the daemons
reported the intermediate condition; *outcome* means only the result was
checked. Apart from the PG messages in
[PG creation, peering and scrub messages](#pg-creation-peering-and-scrub-messages),
no test inspects the messages on these paths.

| Path | What the test observes | Evidence | Proven by |
| --- | --- | --- | --- |
| OSD primary to replica, cluster network | With one OSD's cluster endpoint cut, the primary reports a write `waiting for sub ops` for 10 s while MON commands answer and both OSDs stay up; after restore another client reads the same bytes and version. Logs `HELD_REPLICATION_RECEIPT`. | Native state | `TestSeparateClusterNetworksAndInterruptions`, `TestSeparateIPv6ClusterNetworksAndInterruptions` |
| OSD primary to replica, one direction | `ApplyNetworkProfile` on the primary's cluster endpoint with `Peer` the replica and `Loss: 100`. The write waits past `osd_heartbeat_grace`; each OSD is `reported failed by` the other, but one reporter is below `mon_osd_min_down_reporters` 2, so both stay up. After `Restore` the write completes and both copies read back. Logs `OSD_PEER_PATH_RECEIPT`. | Native state | `TestOSDReplicationPeerPathCut` |
| MON to MON | Cutting two of five MONs leaves a quorum of three; restoring them brings back five. | Native state (quorum membership) | `TestFiveMonitorQuorumAndNetworkRecovery` |
| Map propagation to MONs and OSDs | Each daemon's admin socket reports the new OSDMap epoch after `CreatePool`, and the new MonMap epoch and members after `AddMonitor("d")` and `MonitorContainer.Remove("d")`. Logs `MAP_PROPAGATION_RECEIPT`; times order reads and are not latencies. | Native state | `TestMapPropagationReceipt` |
| OSD to OSD when a replica missed writes | With `norecover` or `nobackfill`, `pg query` tells log recovery (replica in the acting set, `last_backfill` `MAX`) from backfill (replica only in the up set, `last_backfill` `MIN`); `num_objects_recovered` grows by the objects written meanwhile. Also on a K=2, M=1 EC pool, where an empty acting slot is `2147483647`. Logs `OSD_RECOVERY_RECEIPT`. | Native state | `TestOSDRecoveryAndBackfillReceipts/replicated` and `/erasure` |
| OSD to OSD after a placement change | With flags held, PGs map to another OSD in `recovery_wait` or `backfill_wait`; after release they are clean there. Client reads recover on demand, so log recovery and backfill are not told apart. | Native state | `TestPoolPGRelocation` |
| MDS to MDS subtree export | An export pin moves authority, as `get subtrees` reports, and the `exported`/`imported` counters grow. | Native state for pinned directories; outcome for subvolume pins | `TestCephFSRetainedClientAcrossRankOneTakeover`, `TestCephFSLiveRankMigration`, `TestCephFSPins` |
| MGR to MON, registration and failover | See [MGR transition receipt](#mgr-transition-receipt). | Native state (MgrMap and MON sessions) | `TestMessengerClientProfileManagerTransition` |
| OSD heartbeats, cluster network | Three OSDs on separate CRUSH hosts; one cluster endpoint is cut. After `osd_heartbeat_grace` the MON marks it down with 2 reporters; the OSD logs `marked osd.N down, but it is still running`, fails `rebind cluster_messenger` and exits; after `Restore` and `Start` it returns with the same UUID. Logs `OSD_HEARTBEAT_RECEIPT`. | Native state | `TestOSDHeartbeatFailure` |
| RGW to RADOS | A 1031-byte S3 object is one head object `<marker>_<key>` with its key in a `.dir.<id>.<shard>` omap; a 6 MiB object is a 4 MiB head and a tail `<marker>__shadow_.<prefix>_1` with a random prefix. Deleting removes the head and index key; GC lists the tail until `gc process --include-all`. Logs `RGW_RADOS_RECEIPT`. | Native state | `TestRGWRadosReceipt` |
| Source OSD to the destination's rbd-mirror daemon | The source OSD drops only its packets to the daemon. For 30 s the daemon stays `up+replaying` but does not see the new snapshot; after `Restore` only the changed 1 MiB is copied. Logs `RBD_MIRROR_PATH_RECEIPT`. | Native state | `TestRBDMirrorPeerPathCut` (bridge) |
| Source OSD to the cephfs-mirror daemon | The source OSD drops only its packets to the daemon. The daemon sees the snapshot through the MDS, but `fs mirror peer status` stays `syncing` it; the MGR's `dirmap` leaves `mapped` and is recorded, not asserted. After `Restore` exactly the new bytes are copied. Logs `CEPHFS_MIRROR_PATH_RECEIPT`. | Native state | `TestCephFSMirrorPeerPathCut` (bridge) |
| MDS to client after an MDS restart, a takeover or a live export | See [MDS sessions](#mds-sessions). | Native state (session identity) | `TestCephFSRetainedClientAcrossMDSRestart`, `TestCephFSRetainedClientAcrossStandbyTakeover`, `TestCephFSRetainedClientAcrossRankOneTakeover`, `TestCephFSLiveRankMigration` |
| MDS to OSD, one direction | See [MDS sessions](#mds-sessions). | Native state | `TestCephFSMDSOSDPathCut`, `TestCephFSMDSOSDsPathCut` |
| Client to MDS and MDS to client, one direction | `ApplyNetworkProfile` with `Loss: 100` in either direction, `session_timeout` 30 s. The client's `mkdir` hangs in `mds_requests` while appends and data-only fsyncs complete. For 50 s both sides keep the session `open` (`mds_defer_session_stale`); `cap_renew_seq` advances but `cap_ttl` stays. With the MDS's packets cut, the MDS has already journaled the `mkdir`. After `Restore` the `mkdir` returns within about a minute. Logs `CEPHFS_MDS_PATH_RECEIPT`. | Native state | `TestCephFSClientMDSPathCut/mds-to-client`, `/client-to-mds` |

For the backfill case shorten the PG log with `override.SetConfig` on
`osd_min_pg_log_entries`, `osd_max_pg_log_entries` and `osd_pg_log_trim_min`;
with the default trim minimum of 100 a short log is never trimmed. A fault on
one peer in one direction uses `ApplyNetworkProfile` with `Peer` and `Loss`
100; `InterruptNetwork` cuts a whole endpoint.

## PG creation, peering and scrub messages

`TestOSDPeeringMessages` follows one PG's messages through its creation,
peering, lease and deep scrub, and logs `OSD_PEERING_MESSAGES_RECEIPT`. A
`[mon]` section with `debug_ms = 1` and an `[osd]` section with
`debug_ms = 1` and `debug_osd = 10` in `ceph.WithConfigFile` make the MON and
the OSDs log every message they send (`-->`) and receive (`<==`) and the OSDs
every peering state they enter (`enter Started/...`, level 5) and scrubber
state (`scrubber<...>: FSM: -- state -->>`, level 10). The recipe creates a
one-PG pool with two replicas on two OSDs, writes one object, runs
`DeepScrubPG` and joins each message's send line in one daemon's log with its
receive line in the other's: the receiver is one of the sender's messengers,
the sender is the receiver's peer entity and address, and the printed
message is the same.

| Message | Prints as | Direction | Placed in the logs |
| --- | --- | --- | --- |
| `MSG_OSD_PG_CREATE2` | `pg_create2` | MON to primary | |
| `MSG_OSD_PG_QUERY2` | `pg_query2` | primary to replica | sent between the primary entering `Peering/GetInfo` and `Peering/GetLog` |
| `MSG_OSD_PG_NOTIFY2` | `pg_notify2` | replica to primary | received in the same interval |
| `MSG_OSD_PG_LOG` | `PGlog` | primary to replica | sent after the primary enters `Active`; received before the replica enters `ReplicaActive` |
| `MSG_OSD_PG_INFO2` | `pg_info2` | replica to primary | received after `PGlog` was sent and before the primary enters `Active/Clean` |
| `MSG_OSD_PG_LEASE`, `MSG_OSD_PG_LEASE_ACK` | `pg_lease`, `pg_lease_ack` | primary to replica, and back | |
| `MSG_OSD_REP_SCRUB` | `replica_scrub` | primary to replica | sent after the primary's scrubber enters `ActiveScrubbing` |
| `MSG_OSD_REP_SCRUBMAP` | `rep_scrubmap` | replica to primary | sent after the replica's scrubber enters `ReplicaBuildingMap`; received after the primary's enters `WaitReplicas` |

The replica has no copy of a new PG when the primary queries it, so it
answers `pg_notify2` with `DNE` and creates the PG from the `PGlog`. Squid
19.2.5 and Tentacle 20.2.4 print these messages and states alike. Debug
logging at these levels is verbose, so keep such a cluster short-lived.
`osd_peering_messages_fixture_test.go` judges saved Squid lines and broken
variants of them without Docker.

## Negotiated compression and features

On-wire compression is negotiated per session, after authentication, and only
between peers whose policy names each other: `ms_osd_compress_mode` covers
sessions with OSDs, and a secure session is compressed only with
`ms_compress_secure`. Setting the options is no evidence that a session is
compressed. `TestMessengerCompressionReceipt` changes the OSD policy with
`override.SetConfig`, restarts `osd.1` so the sessions renegotiate, and reads
each session back:

| Policy | Sessions between the OSDs | Other sessions |
| --- | --- | --- |
| none (the default) | CRC, uncompressed | uncompressed |
| `ms_osd_compress_mode=force`, `ms_osd_compression_algorithm=snappy` | CRC, `snappy` | uncompressed; the OSD does not compress client sessions even if the client forces it |
| also `ms_cluster_mode=secure` | secure (AES-128-GCM), uncompressed | uncompressed |
| also `ms_compress_secure=true` | secure, `snappy` | uncompressed |

```golang
force, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "osd", Name: "ms_osd_compress_mode", Value: "force"})
// Existing sessions keep what they negotiated; restart an OSD first.
out, err := osd.AdminSocket(ctx, "messenger", "dump", "cluster") // osd is the *ceph.OSDContainer of osd.0
```

On Tentacle, `messenger dump <messenger>` names each session's `con_mode`,
`crypto`, `compression` (`rx`, `tx`) and `rev1`. Squid has no
`messenger dump`; with `debug_ms = 1` each daemon logs an `s=READY` line with
the mode and `crypto` and `comp` handler pointers (`0` means none). The
accepting side of a lossless session reports the mode as `unknown`; its
cipher still tells secure from CRC.

Without `messenger dump` every READY line of the phase, bound to the OSD's
container generation and log range, must also print `rev1=1`.

The test also reads `con_features` from `ceph tell mon.a sessions` and checks
names such as `SERVER_SQUID`, `MSG_ADDR2` and `CEPHX_V2` with their
incarnation, as `HAVE_FEATURE` does: several names share a bit, and an older
incarnation of a bit means something else. It logs
`MESSENGER_COMPRESSION_RECEIPT`.

## Session revision and reconnect

With `debug_ms = 1` a daemon logs one `s=READY` line each time a msgr2
session becomes ready, on the first connection and on every reconnect, on
both supported releases. Set it for the daemons only, with
`ceph.WithConfigFile` and `[mon]`, `[mgr]`, `[osd]` and `[mds]` sections;
a `[client]` section would also change the fixture's own CLI. The line names
the peer address and entity, the policy (`l=1` lossy, `l=0` lossless),
`cs` (connect_seq), `pgs` (peer global sequence) and `rev1`, and ends with
the session's `client_cookie`, `server_cookie`, `in_seq` and `out_seq`.
Squid prints fewer fields inside `conn(...)` than Tentacle; both print these.
`harness.ParseReadySessions` reads them and returns lines that lack one as
unusable instead of dropping them.

| Recipe | What the test observes | Proven by |
| --- | --- | --- |
| v2-only CephFS cluster | The MDS advertises only v2 in `fs dump` and runs with `ms_bind_msgr1` off. Every READY line of each MON, MGR, OSD and the MDS has `rev1=1`, the MDS's with a MON, a MGR, an OSD and a mounted client among them. On Tentacle every READY connection in `messenger dump` is `rev1` and each listening messenger advertises v2 only. Logs `MESSENGER_V2_SESSIONS_RECEIPT`. | `TestCephFSMessengerV2Sessions` |
| v2-only RGW cluster | The gateway's container listens on TCP 7480 only (Docker's resolver at 127.0.0.11 aside): radosgw binds no messenger. radosgw's messengers are the ones the MONs' `sessions` report from the gateway's container once `rgw.Run` has returned; the READY lines that the MON, MGR and OSDs logged with those messengers' nonces all have `rev1=1`. Lines of `radosgw-admin`, which creates the S3 user from the same container, are counted apart. On Tentacle radosgw's admin socket reports exactly those nonces, messengers with no listening socket and `rev1` connections to a MON and an OSD. Logs `MESSENGER_V2_RGW_RECEIPT`. | `TestRGWMessengerV2Sessions` |
| Client-side socket faults | A libcephfs client sets `ms_inject_socket_failures` to 40 for 2 s while it writes, fsyncs and stats records. The MDS logs reconnects of the one lossless session: the same connection object and cookies, `cs` and `pgs` growing, `in_seq` and `out_seq` never going back. The OSDs log new lossy sessions: `cs=0`, `server_cookie=0`, a new `client_cookie`, sequences at 0. On Tentacle the MDS's `messenger dump` shows the same `conn_id` with the last logged `connect_seq`. The MDS session stays `open` and another client reads every acknowledged record. Logs `MESSENGER_RECONNECT_RECEIPT`. | `TestCephFSMessengerReconnect` |

```golang
lines := harness.ContainerLogLines(t, ctx, mds) // mds is the *cephfs.MDS of the active rank
sessions, unusable := harness.ParseReadySessions(lines[mark:])
```

The actor calls `fs.conf_set("ms_inject_socket_failures", "40")` on its own
python-cephfs handle and clears it again from a thread that does not wait on
the workload: a rate high enough to break handshakes keeps every operation,
and so a clearing call behind it, waiting. `cs` can skip values,
because an attempt that faults before READY takes one too. A connection
object's address can be reused after it is freed, so tell lossy sessions
apart by their client cookie. Keepalive and ACK frames themselves appear in
neither the READY lines nor `messenger dump`; only the sequence numbers that
a reconnected session carries show what was acknowledged.

## Targeted commands

`TestTargetedCommandRecipe` sends commands to one OSD (`ceph tell osd.N`,
`rados_osd_command`), one PG's primary (`ceph pg <pgid> query`,
`rados_pg_command`) or one MDS (`ceph tell mds.<name>`), and logs
`TARGETED_COMMAND_RECEIPT`:

| Case | What the native client reports |
| --- | --- |
| `status` to `osd.N` | `whoami`, `osd_fsid`, `cluster_fsid` and `newest_map` match `osd dump` and the cluster |
| `query` to a PG | `info.pgid` and `acting` match `osd map <pool> <object>` |
| `status` to `mds.<name>` | `whoami`, `id`, `fs_name` and `state` match `fs dump`, with the MDS's map epochs |
| no `allow *` on OSDs or MDSs | librados returns `-13`; the CLI exits 13 with `problem getting command descriptions` |
| OSD id or pool missing from the client's OSDMap | `-2` (ENOENT) before sending |
| OSD marked down | `-6` (ENXIO) before sending |
| paused OSD kept up (`override.SetOSDFlag(ceph.OSDFlagNoDown)`, `fault.Pause`) | sent, unanswered, ended by the client's timeout with `-110`; whether it ran is unknown |
| unknown command prefix | `-22` (EINVAL), `unknown command prefix` |

librados reports the signed result and the CLI exits with its absolute value.
The Python binding has `osd_command` and `pg_command`; for an MDS use the CLI
or `cephfs.LibCephFS.mds_command`.

### Binary input and output buffers

Every command carries its input buffer in the data segment, and every reply
its output buffer. The same test sends binary buffers through python-rados
and logs `TARGETED_BINARY_IO_RECEIPT`. It sets `debug_ms = 1` on the client
(with `log_to_file = true` and `log_file`, since the image does not log to a
file by default) and through `ceph tell <daemon> config set` on the daemons.
The receiver of every message logs one dispatch line:

```
-- 172.18.0.2:0/83799995 <== mon.0 v2:172.18.0.2:3300/0 7 ==== mon_command_ack([{"prefix": "config-key get", "key": "tc/binary-io/4275"}]=0  v6) ==== 90+0+65536 (secure 0 0 0) 0x7515d8002430 con 0x561a1b6a7b90
```

The three numbers are the front, middle and data segment lengths. Recognise
the client by address and nonce: a MON logs it as `client.?`, an OSD as
`client.<global id>`. A status ending in a newline spans two log lines.

| Channel | What the receipt holds |
| --- | --- |
| `osd_command` (MCommand) | `status` with 1 MiB of every byte value, received as a 1048576-byte data segment and ignored; `config show` returns about 77 KiB |
| `mon_command` (MMonCommand) | `config-key set` of exactly `mon_config_key_max_entry_size` (65536) bytes round-trips by SHA-256; one byte more is `-27` (EFBIG) |
| | `osd getcrushmap` and `osd getmap` decode with `crushtool -d` and `osdmaptool --print`; the status of `getcrushmap` is the CRUSH version |
| | `osd setcrushmap` with one more rule bumps the CRUSH version and epoch by one; a broken magic is `-22`; resending with the same `prior_version` is acknowledged without a new version |
| `mgr_command` (MMgrCommand) | `crash post` of a 96 KiB report; `crash info` returns those bytes plus a newline; non-UTF-8 input is `-22` |

For cancellation, a 256 KiB `status` to a paused OSD under `nodown` ends in
`-110` at the client's `rados_osd_op_timeout` and the Objecter logs
`command_op_cancel tid N`. After resume the OSD answers tid N and the client
drops the late reply (`handle_command_reply tid N not found`); the session
stays usable.

Limits of the official image:

- `mon_command` or `mgr_command` with `target` is an MCommand that reaches
  only the daemon's admin socket commands, so `osd getcrushmap` and `crash ls`
  are `-22` there.
- No OSD, MON or MGR admin socket command returns binary output, and none
  reads the input buffer except the OSD's `rotate-stored-key`, so a byte-exact
  round trip runs over the MON session.
- A MGR module receives and returns UTF-8 text.

## RBD and CephFS object layout

Two receipts write through the native library and read metadata and every
data object back with `rados` and a separate librados process.

`TestRBDLayoutReceipt` creates, in an `rbd.CreateNamespace` namespace, a
16 MiB image with order 22 striped in 1 MiB units over two objects
(`STRIPINGV2`), writes two ranges, snapshots, protects, overwrites 8 KiB, and
writes 4 KiB to a clone. `RBD_LAYOUT_RECEIPT` holds the `rbd_id.<name>` and
`rbd_header.<id>` omap fields, the clone's decoded `parent` value, every
`rbd_data.<id>.<objno as %016x>` object with size and sha256 (the clone has
only its copied-up object), the snapshot clone from `rados listsnaps` with its
pre-overwrite bytes, and each object's placement from `ceph osd map`.

`TestCephFSLayoutReceipt` sets the same striping and a namespace through
`ceph.file.layout.*`, writes the same ranges, snapshots with `mkdir .snap/base`
and overwrites 8 KiB. `CEPHFS_LAYOUT_RECEIPT` holds the same records for
`<ino as hex>.<objno as %08x>` objects.

Both use one formula: byte `off` is in stripe unit `b = off / su`, stripe
`s = b / sc`, object `(s / (os / su)) * sc + b % sc` at
`(s % (os / su)) * su + off % su`. Unwritten parts below a write read as
zeros, and an object ends at its last written byte.

## Watch/notify and MON subscriptions

`TestNativeWatchNotifyAndSubscriptions` runs python-rados clients against one
OSD and logs three receipts.

`RADOS_WATCH_NOTIFY_RECEIPT`, from a notifier and three watchers:

- each watch's client id and cookie, as `rados listwatchers` lists them;
- per notify, the notify id, notifier id, cookie and payload each watcher saw,
  and the acks as `[watcher client id, cookie, payload hex]`;
- with a watcher that never acks, a python-rados notify returns only `-110`,
  so `rados notify` lists the acks and `timeout client.<id> cookie <c>`;
- after `ceph osd blocklist add`, the blocklisted watcher's error callback
  with `-107`, and after the object is removed, `-107` for the others.

python-rados sends a notify payload as the UTF-8 encoding of a `str`, so keep
payloads ASCII. A `rados notify` payload arrives length-prefixed
(`02000000 6e34` for `n4`).

`MON_SUBSCRIPTION_RECEIPT`, from one client with a watch and a cluster-log
subscription (`debug_ms=1`, `debug_monc=10`, `debug_mgrc=4`,
`debug_objecter=3`):

- `mon_subscribe` contents as `name: [start(+)]`: `monmap`, `mgrmap`,
  `config` and `log-info` are continuous; `osdmap` is onetime for the next
  epoch, renewed on the Objecter tick while the watch exists;
- the `osd_map` messages and decoded epochs around `ceph osd pool set-quota`,
  the Objecter `map_epoch` and `map_inc` counters (counted from the first map
  after connect, so read the start from the log), and the MON's `osd_epoch`
  for the client;
- `config` messages for `ceph config set client debug_rados 3/7` and its
  `config rm`, with `conf_get` after each;
- `log` messages and a `ceph log` marker reaching the callback.

`MAP_EPOCH_DELIVERY_RECEIPT` follows MonMap and MgrMap changes to the client
through its session MON (`via`):

- `monmap`: the epochs around `ceph mon set_location`, the `mon_map` messages
  (which name no epoch), and the decoded map with the new
  `crush_location`;
- `mgrmap`: the active MGR before `ceph mgr fail`, right after, and once a new
  GID is active and the client caught up; the `mgrmap(e N)` messages, the
  MgrClient's epochs with the active address vector, and its new session.
  The failover moves the MgrMap several epochs, so do not expect consecutive
  epochs.

## Keepalive and acknowledgement frames

A ProtocolV2 session exchanges KEEPALIVE2, KEEPALIVE2_ACK and message ACK
frames that neither `debug_ms 1` nor `messenger dump` names; they appear only
at `debug_ms 20`. `TestCephFSMessengerKeepalive` mounts a libcephfs client at
`debug_ms 20`, writes, fsyncs and idles in cycles, and logs
`CEPHFS_KEEPALIVE_RECEIPT`. From the client's own log it counts, by substring
after the connection's `conn(...)` state:

- `send_keepalive` — a KEEPALIVE2 frame written;
- `handle_keepalive2_ack got KEEPALIVE_ACK` — its KEEPALIVE2_ACK received;
- `handle_message_ack got ack seq` and `write_event try send msg ack` — ACK
  frames received and sent for the MDS cap messages.

Keepalive runs on the client's lossy MON session, so a MON's `debug_ms 20`
log shows `handle_keepalive2 got KEEPALIVE2` from the client, while the MDS's
log shows it acking the client's cap messages. The client's lossless MDS and
OSD sessions are acknowledged but not keepalived. These frames are observed,
not decoded from the wire; `cephfs_keepalive_fixture_test.go` tests the
counting without Docker.

## OSD backoff

`TestOSDBackoffReceipt` makes a primary send a PG-wide backoff
(`CEPH_MSG_OSD_BACKOFF`) to a client that negotiated
`CEPH_FEATURE_RADOS_BACKOFF`, and logs `OSD_BACKOFF_RECEIPT`.
`PrimaryLogPG::do_request` backs off a PG that is peered but not active, so no
debug switch is needed:

1. A one-PG pool on three OSDs, three replicas, `min_size` 3, holds one
   object; wait for `active+clean`.
2. Raise the primary's `debug_ms` to 1; a python-rados client with
   `debug_ms=1` and `debug_objecter=10` stats the object to open its session.
3. With `noout`, stop a non-primary replica until the PG is peered, not
   active, under the same primary. `min_size` is never lowered.
4. Submit one asynchronous read. The client must log a BLOCK from the primary
   and the primary the client's ACK_BLOCK on the same connection, with the
   read still pending.
5. Start the replica. The client must get the UNBLOCK, then the read's reply
   with result 0 and the seeded bytes.

The chain is joined by client address and entity, primary, PG, backoff ID and
range; IDs count per OSD session, and a reconnect between BLOCK and UNBLOCK
leaves the chain unjoined. ACK_BLOCK carries the BLOCK's epoch, while UNBLOCK
may carry a later one. An interval change can also release a backoff, so the
recipe requires the read's own result. `OSD_BACKOFF_RAW` holds the joined
log lines.

`TestOSDObjectBackoffReceipt` makes a primary back off one object, and logs
`OSD_OBJECT_BACKOFF_RECEIPT`. A primary backs off a write to a degraded object
only with `osd_backoff_on_degraded`, which is off by default:

1. A one-PG pool on two OSDs, two replicas, `min_size` 1, holds one object.
2. With `noout`, `norecover` and `nobackfill`, stop the replica, rewrite the
   object, and start the replica again. The PG is active and the object
   degraded, missing on the replica.
3. Set `debug_ms` 1 and `osd_backoff_on_degraded` true on the primary. A
   python-rados client with the same log levels submits one asynchronous full
   write of the object.
4. The client must log a BLOCK whose range is empty at the object's head,
   `[2:<hash>:::<object>:head,2:<hash>:::<object>:head)`, and the primary the
   ACK_BLOCK on the same connection. The write itself makes the primary
   recover the object although `norecover` is set, so the UNBLOCK follows
   within milliseconds, then the write's reply with result 0. A fresh client
   reads the written bytes back.

Both recipes share the parser and the chain judge; `backoffTarget` names the
held operation and whether the BLOCK must cover the whole PG or the object
alone, so a PG-wide BLOCK never stands in for an object backoff.
`OSD_OBJECT_BACKOFF_RAW` holds the joined lines, and
`messenger_backoff_fixture_test.go` tests the parsing and both scopes without
Docker.

## Authentication refusal causes

A client sees most refusals as the same `[errno 13] RADOS permission denied`,
and its MonClient prints release-specific text: for a wrong key Tentacle
20.2.4 prints `failed to auth with my available methods` and Squid 19.2.5
`allowed_methods [2] but i only support [2]`. The MON that refused it logs
the cause at its default levels. `TestAuthFailureCauses` refuses four
commands and logs `AUTH_FAILURE_CAUSE_RECEIPT`; `TestAuthFirstFailureBundle`
attaches the same record to its first-failure bundle, whose cause is then
`wrong-key`.

| Refusal | MON line (level) | Cause |
| --- | --- | --- |
| A key that is not the entity's | `cephx server client.admin:  unexpected key: req.key=... expected_key=...` (0) | `wrong-key` |
| An entity with no key | `cephx server <entity>: couldn't find entity name: <entity>` (0) | `unknown-entity` |
| `--auth-client-required none` against cephx | none; the client prints `allowed_methods [2] ... support [1]` | `method-mismatch` |
| A command beyond the entity's caps | `handle_command access denied` (1) | `command-denied` |

The record counts every MON's log lines before the attempt and reads them
again after it, so a capture belongs to one attempt and to the MON container
generation it started in. A MON restarting in between fails the recipe. The
record also carries the attempt's operation identity, its entity and the time
from the first count to the last read; a bundle takes a cause only from a
record with its rejection's identity and entity whose window holds the
rejection, so a control, a retry or another entity's refusal never supplies
the first failure's cause. Only
lines naming the attempted entity count, except the command refusal, which
names none and which the capture bounds. `req.key` and `expected_key` are
values that cephx derives from the keys, so the record masks them. One cause
among the lines is that cause; lines of two causes, or a MON line beside
disjoint client methods, are `conflicting`; a missing or unbound capture
leaves the cause `unavailable`, as does a command that succeeded.
`auth_failure_cause_fixture_test.go` tests the classification without Docker.

## Coverage manifest

[`internal/integration/testdata/messenger_coverage.json`](../../internal/integration/testdata/messenger_coverage.json)
lists the environment requirements of the ceph-msgr-go SPEC at a pinned
commit, each with its Ceph symbols, its status (`covered`, `partial` or
`uncovered`), the proving tests and receipt markers, and otherwise the gap and
any tracking issue. Symbols come from
[`messenger_reference.json`](../../internal/integration/testdata/messenger_reference.json):
the OSD operations, message types and feature bits of the pinned Ceph headers,
with feature aliases kept separate.

`TestMessengerCoverageManifest` checks the manifest without Docker: every
named test and symbol exists, covered and partial rows have cases, every test
the daemon-to-daemon table cites is mapped, and a cited `TestName/subtest`
runs through `t.Run` in the test, a helper or another subtest. The validator
follows subtest names that are string literals, literal table fields in the
same function, or `harness.NetworkModes` modes; cite the top-level test otherwise. A
receipt marker must be in code the cited test reaches. The manifest records
whether a recipe exists, not whether it passed. Regenerate the reference from
a Ceph checkout at the pinned commit:

```sh
python3 .github/scripts/messenger_reference.py generate --source ~/ceph \
  --commit 7f793731f1b39eb4f465e960113d2363c311b964 --release 20.2.4 \
  --output internal/integration/testdata/messenger_reference.json
```

The declaration/native-case evidence join, which connects reference
declarations to validated receipts, is documented in
[`messenger_evidence.py`](../../.github/scripts/messenger_evidence.py).

## Differences from a single-container bootstrap

- **Addresses and ports.** The fixture assigns each daemon's address and
  ports. Read them with `MonitorBootstrapAddresses` and `DaemonAddresses`.
  There is no option for fixed ports, loopback addresses such as `::1` or
  `::ffff:7f00:1`, or IPv6 on the host network.
- **Reaching daemons from the host.** On a bridge network only MON a publishes
  ports, and MGRs and OSDs advertise bridge addresses. Use `WithHostNetwork`
  or a relay in a container on the cluster network.
- **FSID.** Random per cluster; read it with `Status`.
- **Monmap.** It starts with MON a and `b` and `c` join, so its epoch and
  initial features differ from a `monmaptool --enable-all-features` map. Read
  it with `ceph mon dump`.
- **MonMap ciphers.** With `KeyType` set, the fixture writes all three MonMap
  ciphers; `auth_allowed_ciphers` defaults to the distinct types of `KeyType`,
  `ServiceCipher` and `PreferredCipher`. Without `WithAuth` the release
  defaults apply.
- **Connections between daemons.** `ms_cluster_mode` and
  `ms_mon_cluster_mode` keep Ceph's defaults (both modes) except under
  `MessengerV2Secure`, which fixes them to secure. Under `MessengerV2Only`,
  `override.SetConfig` can change them.
- **Ticket lifetimes.** Fixed at bootstrap; `override.SetConfig` cannot change
  them.
- **Short MON tickets.** Under ten seconds, a MON client that renews late is
  refused with `EACCES` and does not authenticate again (see
  [WithAuth](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#WithAuth)); this is Ceph's MON client
  behaviour. A MGR busy loading modules can lose its session this way and
  abort, which is why the 1.5 s profile is optional. With 1.5 s MON tickets on
  Tentacle with AES256K keys, native librados sessions and MGR commands stop
  working within 30 s, so keep native checks in that profile short and early.
- **Keys.** Each identity has its own keyring; read a key from
  `ClientConfig.ConnectionConfig`.
- **Stopping a daemon abruptly.** `Stop` sends SIGTERM and waits. Kill a
  container through the Docker API for a crash; see
  [Fault injection](fault-injection.md).
