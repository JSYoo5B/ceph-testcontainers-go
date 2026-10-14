# Fixtures for Messenger client tests

## Introduction

A Messenger client implementation, such as a client written in another
language, is tested against native daemons: it authenticates with Cephx,
negotiates secure or CRC connections, renews tickets and survives daemons
that pause, stop and fail over. This guide shows how to build those test
environments with the module instead of a hand-written bootstrap script, and
what differs when each daemon runs in its own container.

The profiles below are derived from the ceph-msgr-go integration environment.
Each row names a test in this repository's `internal/integration` that starts
the profile with public options and checks it with native clients and daemon
observations. These prove fixture readiness, not ceph-msgr-go's own protocol
implementation or its migration to these APIs. For the compatibility changes
when replacing a custom bootstrap, see [Upgrading to v0.2.0](upgrading-to-v0.2.md).

## Profiles

| Profile input | Values | Fixture options | Proven by |
| --- | --- | --- | --- |
| Image | Tentacle 20.2.4, Tentacle 20.2.3 | `ceph.Run(ctx, image, ...)` | every test below; `TestMessengerClientProfileWithoutKeyTypes` runs 20.2.3 |
| Messenger v1 | never bound or advertised | `WithMessengerMode(MessengerV2Only)`, or `MessengerV2Secure` | every profile |
| Monitors | three | `WithMonitorCount(3)` | every profile |
| Managers | two | `WithManagerCount(2)` | every profile but the next |
| | none at bootstrap, two started later | `WithNoInitialManagers`, then `AddManager("a")` and `AddManager("b")` | `TestMessengerClientProfileDelayedManagers` |
| OSD | none | `WithNoInitialOSDs` | `TestMessengerClientProfileSecure` |
| | one MemStore OSD | `WithOSDCount(1)`, `WithOSDObjectStore(OSDMemStore)`, `WithPoolDefaults(1, 1)` | `TestMessengerClientProfileCRC` |
| CRUSH and pool | legacy tunables, a straw root, a rule that chooses OSDs; a pool with one PG, size 1 and no `hashpspool` | compile the map with `crushtool` and install it with `osd setcrushmap` (see [Reproduce a minimal legacy placement setup](../ceph.md#reproduce-a-minimal-legacy-placement-setup)); `CreatePool(ceph.PoolConfig{PGNum: 1, Replicas: 1, MinSize: 1, NoHashPSPool: true})` | `TestMessengerClientProfileCRC`, `TestOSDMemStoreLegacyPlacement` |
| Key type / service cipher | `aes256k`/`aes256k` | `WithAuth(ceph.AuthConfig{KeyType: ceph.CipherAES256K})` | `TestMessengerClientProfileSecure` |
| | `aes`/`aes` | `AuthConfig{KeyType: ceph.CipherAES}` | `TestMessengerClientProfileModeRejection/managers` |
| | `aes`/`aes256k` | `AuthConfig{KeyType: ceph.CipherAES, ServiceCipher: ceph.CipherAES256K}` | `TestMessengerClientProfileCRC/bridge` |
| | `aes256k`/`aes` | `AuthConfig{KeyType: ceph.CipherAES256K, ServiceCipher: ceph.CipherAES}` | `TestAuthCiphersAndTicketLifetimes` |
| Ticket lifetimes | 12 s MON and service tickets | `AuthConfig{MonTicketTTL: 12 * time.Second, ServiceTicketTTL: 12 * time.Second}` | `TestMessengerClientProfileSecure`, where one librados session outlives two lifetimes of each ticket |
| | 120 s, with fast MON timing | `AuthConfig{MonTicketTTL: 120 * time.Second, ServiceTicketTTL: 120 * time.Second}` and a `WithConfigFile` `[global]` section with `mon_subscribe_interval`, `mon_tick_interval`, `mon_session_timeout`, `mon_client_ping_interval` and `mon_client_hunt_interval` | `TestMessengerClientProfileIdleTickets` |
| | 1.5 s MON tickets, 12 s service tickets | `AuthConfig{MonTicketTTL: 1500 * time.Millisecond, ServiceTicketTTL: 12 * time.Second}` and `mon_tick_interval = 1` in `[global]` | `TestMessengerClientProfileShortTickets`, an optional test that needs the `native_regression` build tag: the bootstrap, the lifetimes, and a MON and a MGR command from a client; see [Differences](#differences-from-a-single-container-bootstrap) |
| `global_id` reclaim | not allowed insecurely | always `auth_allow_insecure_global_id_reclaim = false` | `TestMessengerClientProfileSecure` |
| Connection modes | every connection secure | `MessengerV2Secure` | `TestMessengerClientProfileSecure`, which reads all six `ms_*_mode` options from each MON and MGR |
| | MONs, MGRs and OSDs accept only CRC from clients | `MessengerV2Only` with `WithMessengerPolicy(ceph.MessengerPolicy{Monitors: crc, Managers: crc, OSDs: crc})` | `TestMessengerClientProfileCRC`, which reads each daemon's service mode |
| | MONs accept only CRC, MGRs only secure | `MessengerV2Only` with `MessengerPolicy{Monitors: crc, Managers: secure}` | `TestMessengerClientProfileModeRejection/monitors` |
| | MONs accept only secure, MGRs only CRC | `MessengerV2Only` with `MessengerPolicy{Monitors: secure, Managers: crc}` | `TestMessengerClientProfileModeRejection/managers` |
| Client modes | secure, CRC, or secure then CRC | `WithClientMessengerPolicy` on a `WithClient` or `WithClientIdentity` container | `TestMessengerClientProfileModeRejection`, `TestMessengerClientProfileCRC` |
| Address family | IPv4 | the default | every other profile |
| | IPv6 | `WithAddressFamily(ceph.AddressIPv6)` | `TestMessengerClientProfileAddressFamilies/ipv6` |
| | IPv4-mapped IPv6 | `WithAddressFamily(ceph.AddressIPv4MappedIPv6)` | `TestMessengerClientProfileAddressFamilies/ipv4-mapped` |
| Where the tests run | a container on the cluster network | a container with `WithClient` or `WithClientIdentity` | every profile |
| | a host process | `WithHostNetwork` (see [Host network](host-network.md)); every advertised MON, MGR and OSD address is reachable from the host | `TestMessengerClientProfileCRC/host`, where the test process reads each daemon's Messenger v2 banner |
| Client identities | full access, read-only, revocable, and one limited to the `fsid` command | `CreateClient` with `ceph.ClientCaps`, for example `{Mon: "allow command \"fsid\"", MGR: "allow r"}`; keys get the cluster's `KeyType` | `TestMessengerClientProfileSecure` |

`MessengerConnectionMode` values are `ceph.MessengerSecure` and
`ceph.MessengerCRC`; the table abbreviates them.

An image without `ceph-authtool --key-type`, such as Tentacle 20.2.3 or Squid,
makes only AES keys. `Run` refuses a profile that needs `aes256k` with
`ceph.ErrUnsupported` before any daemon starts. It does not fall back to AES.

### Readiness

`Run` returns once a MGR is available, or, with `WithNoInitialManagers`, once
the MONs have a quorum and the initial OSDs are up and in. A suite that also waits for every MON and a standby MGR polls
`QuorumStatus` until the quorum lists all three MONs, and `ManagerStatus`
until it reports an active MGR and one standby.

## Daemon controls

| Control | Fixture | Proven by |
| --- | --- | --- |
| Pause a MON or a MGR and resume it | `PauseContainer` and `ContainerPause.Resume`. The paused daemon's socket still accepts TCP connections, but the daemon sends nothing. A paused MON leaves the quorum to the other two. A MON paused for longer than the MON ticket lifetime can strand a daemon whose MON session was on it: the daemon's reconnect is refused with `EACCES`, and its MON client stops authenticating. With 12 s tickets and a 25 s pause, this happened to the active MGR in two of four runs. Check MGR-dependent behaviour before such a pause, or restart the stranded daemon. | `TestMessengerClientProfileSecure` |
| Fail over the active MGR | `cluster.Ceph(ctx, "mgr", "fail", name)` | `TestMessengerClientProfileSecure` |
| Stop one MGR and start it again | `Managers()`, then `Stop` and `Start` on the handle. `Start` waits only for the admin socket. | `TestMessengerClientProfileSecure` |
| Restart one MON | `Monitors()`, then `Stop` and `Start` | `TestMessengerClientProfileSecure` |
| Stop every MON and start them again | `Stop` each MON; start `b` and `c` before `a`. MON a's container checks `ceph status` when it starts, which needs a quorum. Each MON comes back at its monmap address. | `TestMessengerClientProfileSecure`, with the MONs down longer than a MON ticket lives |
| Stop MON a for good | `Stop` on its handle. With three MONs, the fixture's own CLI runs in a separate control container and keeps working. | `TestMessengerClientProfileSecure` |
| Start MGRs later | `AddManager` | `TestMessengerClientProfileDelayedManagers` |
| Make the MGRs accept only secure, then restart them | `TemporaryMessengerPolicy(ctx, ceph.MessengerPolicy{Managers: secure})`, then `Stop` and `Start` each MGR. The MGRs keep the mode across the restart; `Restore` returns them to CRC. | `TestMessengerClientProfileModeRejection/managers` |
| Revoke a client key and restore it | the suite's own client, or `TemporaryClientRevocation` and `ReplaceClientKey` | `TestClientRevocation`, `TestClientKeyReplacement` |
| Run native oracles | `python3-rados`, the `ceph` CLI, `crushtool` and `ceph-dencoder` in a `WithClient` container, or `RunNativeCommand` | `TestNativeCommands` (`ceph-dencoder`), `TestRADOSMutationOutcomes` (`python3-rados`), `TestMessengerClientProfileCRC` and `TestOSDMemStoreLegacyPlacement` (`crushtool`) |

## Mutation oracles

A client that implements object mutations needs native receipts for each
outcome it reports. The module wraps no data APIs; these tests show the
native recipes with `python3-rados` (and `ctypes` for librados calls that
python-rados lacks) in a client container. `TestRADOSMutationContracts` and
`TestRADOSMutationOutcomes` read every result back with a second client.

| Contract | Native receipt | Proven by |
| --- | --- | --- |
| write at an offset, `write_full`, append | readback of the exact bytes | `TestRADOSMutationOutcomes`, `TestRADOSSelfManagedSnapshots` |
| zero | the range reads as zeros and the size is unchanged | `TestRADOSMutationContracts` |
| truncate, remove | the new size; `ENOENT` after a remove | `TestRADOSSelfManagedSnapshots`, `TestRADOSMutationContracts` |
| create | an exclusive create of an existing object fails with `EEXIST` (17), also inside a compound write, which then changes nothing; an idempotent create succeeds | `TestRADOSMutationContracts` |
| version guard | `assert_version` with an older version fails with `ERANGE` (34), with a newer one `EOVERFLOW` (75); a refused write still takes a PG version | `TestRADOSMutationOutcomes` |
| compare guards | a failed `cmpext` returns `MAX_ERRNO` (4095) plus the offset of the first differing byte; failed `omap_cmp` and `cmpxattr` return `ECANCELED` (125); the whole compound operation is refused | `TestRADOSMutationContracts`, `TestRADOSClientFixtures` |
| acknowledged outcome | a write whose completion returned 0 survives a SIGKILL of a BlueStore OSD | `TestRADOSMutationOutcomes` |
| unknown outcome | the client's own timeout ends an append, truncate, remove or guarded compound write with `ETIMEDOUT` (110) while the OSD delays it; the OSD's operation records show that it started each one afterwards, and it applied each exactly once, a compound write whole | `TestRADOSMutationOutcomes`, `TestRADOSMutationContracts` |
| no automatic re-execution | an unguarded retry of an uncertain append appends again; a retry guarded by the version read before is refused | `TestRADOSMutationOutcomes` |
| killed client | the OSD discards an operation whose client's connection closed before it started | `TestRADOSMutationOutcomes` |
| OSD restart | SIGKILL and restart with BlueStore; a clean restart with MemStore | `TestRADOSMutationOutcomes`, `TestOSDMemStoreLegacyPlacement` |

`TestRADOSMutationContracts` runs on the topology of the profiles above:
Messenger v2 only with CRC, AES256K keys where the image can make them, and a
MemStore OSD with the legacy CRUSH map and a pool without `hashpspool`. A
MemStore OSD loses its data when it is killed, so durability across a crash
needs BlueStore, as in `TestRADOSMutationOutcomes`. Delays inside the OSD are
described in [Fault injection](fault-injection.md).

## MDS sessions

A client that implements CephFS sessions needs to know what the native client
keeps when its MDS fails. These tests keep a native libcephfs client mounted
in a client container under `RunNativeCommand`, with a file descriptor that it
opened before the failure, and publish its client ID, PID and inode in
receipts tied to one run. `Sessions` reports the session; `ceph tell
mds.<name> session ls` adds counters that `Session` leaves out, such as
`num_leases` and `num_completed_requests`.

| Profile | Native receipt | Proven by |
| --- | --- | --- |
| The only MDS is killed, its GID failed with `mds fail`, and its container started again | rank 0 returns with a new GID in the same container; the client's session is `open` again with the same client ID, PID and address and nonce; through the descriptor opened before the failure the client reads its bytes, appends and fsyncs, renames the file and sets a binary xattr; a client in another container reads the same inode, the exact bytes and the xattr | `TestCephFSRetainedClientAcrossMDSRestart`, with `MessengerV2Secure` on bridge and host networks |

The client fsynced its data before the failure, so this receipt shows a
session reconnect, not the replay of a request that was in flight. A standby
takeover, several active ranks, contended capabilities and a client that
reconnects too late are separate profiles.

## Daemon-to-daemon paths

A client test that follows a request past its first daemon needs to know which
internal paths a fixture has actually exercised, and how directly. In the
table, *native state* means that the daemons themselves reported the
intermediate condition, such as a write waiting for its replica; *outcome*
means that only the result was checked. No test inspects the messages on
these paths.

| Path | What the test observes | Evidence | Proven by |
| --- | --- | --- | --- |
| OSD primary to replica, cluster network | The pool has one PG on both OSDs, with two replicas and `min_size` 2. While one OSD's cluster endpoint is cut, the primary reports the client's write `waiting for sub ops`, with no replica commit, for 10 seconds. Meanwhile a MON command answers, both OSDs stay up with the same acting set, and the cut OSD's public listener accepts TCP. After the restore the writer completes, and a client of its own in another container reads the same bytes and object version. The test logs the whole receipt as `HELD_REPLICATION_RECEIPT`. | Native state | `TestSeparateClusterNetworksAndInterruptions`, `TestSeparateIPv6ClusterNetworksAndInterruptions` |
| MON to MON | Cutting two of five MONs off the public network leaves a quorum of three; restoring them brings back five. | Native state (quorum membership). Which daemon received which map is not observed. | `TestFiveMonitorQuorumAndNetworkRecovery` |
| OSD to OSD after a placement change | With `nobackfill` and `norecover` held, a one-replica pool's PGs map to another OSD and report `recovery_wait` or `backfill_wait` while the objects stay on the old one. After the flags are restored, the PGs are clean on the new OSD and the data reads back. | Native state. Client reads recover objects on demand, so the test does not tell log recovery from backfill. | `TestPoolPGRelocation` |
| MDS to MDS subtree export | An export pin moves a directory's authority to the pinned rank, as `get subtrees` on the owned ranks reports. | Outcome | `TestCephFSPins` |
| MDS to client after an MDS restart | See [MDS sessions](#mds-sessions). | Native state (session identity) | `TestCephFSRetainedClientAcrossMDSRestart` |

No receipt exists yet for MGR registration and control, erasure-coded shard
reads across OSDs, heartbeat failure detection, RGW to RADOS, or the RBD and
CephFS mirror peers with one path cut. A fault on one peer or in one direction
needs a peer-selective fault, which the fixture does not provide:
`InterruptNetwork` cuts a whole endpoint, and `TemporaryNetworkDelay` delays
everything that leaves an interface.

## Differences from a single-container bootstrap

- **Addresses and ports.** Each daemon runs in its own container, and the
  fixture assigns its address and ports. Read the MON addresses with
  `MonitorBootstrapAddresses` and every daemon's with `DaemonAddresses`. There
  is no option for fixed ports, for loopback addresses such as `::1` or
  `::ffff:7f00:1`, or for IPv6 on the host network.
- **Reaching daemons from the host.** On a bridge network only MON a publishes
  ports. MGRs and OSDs advertise bridge addresses. A host process that talks
  to them needs `WithHostNetwork`, or a relay of its own in a container on the
  cluster network.
- **FSID.** Each cluster gets a random FSID; read it with `Status`.
- **Monmap.** The monmap starts with MON a, and `b` and `c` join it, so its
  epoch and initial features differ from a map that `monmaptool` created with
  all three MONs and `--enable-all-features`. Read the actual map with
  `ceph mon dump`.
- **MonMap ciphers.** When `WithAuth` sets `KeyType`, the fixture writes all
  three MonMap ciphers, including `auth_allowed_ciphers`, which defaults to
  the distinct key types of `KeyType`, `ServiceCipher` and `PreferredCipher`.
  Without `WithAuth` the release's defaults apply.
- **Connections between daemons.** `ms_cluster_mode` and
  `ms_mon_cluster_mode` keep Ceph's defaults, which accept both modes, except
  under `MessengerV2Secure`, which fixes them to secure. Under
  `MessengerV2Only`, `TemporaryConfig` can change them after bootstrap.
- **Ticket lifetimes.** They are fixed at bootstrap; `TemporaryConfig` cannot
  change them later.
- **Short MON tickets.** With MON tickets under ten seconds, a MON client
  that renews late is refused with `EACCES` and does not authenticate again;
  see [WithAuth](../ceph.md#withauth). This is the behaviour of Ceph's MON
  client, not of the fixture.
  - A MGR busy loading its modules can lose its MON session this way, and Ceph
    then aborts it. That is why the 1.5-second profile's test is optional.
  - With 1.5-second MON tickets, native clients stopped working within
    30 seconds on Tentacle 20.2.4 with AES256K keys:
    - a single librados session meant to last 30 seconds lost its MON session
      in each of eight runs;
    - a new session every half second worked at first, but MGR commands went
      unanswered within 30 seconds in each of four runs.

    On Squid 19.2.5 with AES keys, the new sessions kept working for
    30 seconds. Keep native checks in this profile short and early.
- **Keys.** Each identity has its own keyring. Read a key from
  `ClientConfig.ConnectionConfig`.
- **Stopping a daemon abruptly.** `Stop` sends SIGTERM and waits. Kill a
  container through the Docker API for a crash; see
  [Fault injection](fault-injection.md).
