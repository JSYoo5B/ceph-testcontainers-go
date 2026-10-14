# Fixtures for Messenger client tests

## Introduction

A Messenger client implementation, such as a client written in another
language, is tested against native daemons: it authenticates with Cephx,
negotiates secure or CRC connections, renews tickets and survives daemons
that pause, stop and fail over. This guide shows how to build those test
environments with the module instead of a hand-written bootstrap script, and
what differs when each daemon runs in its own container.

The profiles below are those of the ceph-msgr-go integration environment.
Each row names the test in `internal/integration` that starts the profile with
public options and checks the native result.

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
| | 1.5 s MON tickets, 12 s service tickets | `AuthConfig{MonTicketTTL: 1500 * time.Millisecond, ServiceTicketTTL: 12 * time.Second}` and `mon_tick_interval = 1` in `[global]` | `TestMessengerClientProfileShortTickets`, an optional test that needs the `native_regression` build tag; see [Differences](#differences-from-a-single-container-bootstrap) |
| `global_id` reclaim | not allowed insecurely | always `auth_allow_insecure_global_id_reclaim = false` | `TestMessengerClientProfileSecure` |
| Connection modes | every connection secure | `MessengerV2Secure` | `TestMessengerClientProfileSecure` |
| | MONs, MGRs and OSDs accept only CRC from clients | `MessengerV2Only` with `WithMessengerPolicy(ceph.MessengerPolicy{Monitors: crc, Managers: crc, OSDs: crc})` | `TestMessengerClientProfileCRC` |
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
| Pause a MON or a MGR and resume it | `PauseContainer` and `ContainerPause.Resume`. The paused daemon's socket still accepts TCP connections, but the daemon sends nothing. A paused MON leaves the quorum to the other two. | `TestMessengerClientProfileSecure` |
| Fail over the active MGR | `cluster.Ceph(ctx, "mgr", "fail", name)` | `TestMessengerClientProfileSecure` |
| Stop one MGR and start it again | `Managers()`, then `Stop` and `Start` on the handle. `Start` waits only for the admin socket. | `TestMessengerClientProfileSecure` |
| Restart one MON | `Monitors()`, then `Stop` and `Start` | `TestMessengerClientProfileSecure` |
| Stop every MON and start them again | `Stop` each MON; start `b` and `c` before `a`. MON a's container checks `ceph status` when it starts, which needs a quorum. Each MON comes back at its monmap address. | `TestMessengerClientProfileSecure`, with the MONs down longer than a MON ticket lives |
| Stop MON a for good | `Stop` on its handle. With three MONs, the fixture's own CLI runs in a separate control container and keeps working. | `TestMessengerClientProfileSecure` |
| Start MGRs later | `AddManager` | `TestMessengerClientProfileDelayedManagers` |
| Make the MGRs accept only secure, then restart them | `TemporaryMessengerPolicy(ctx, ceph.MessengerPolicy{Managers: secure})`, then `Stop` and `Start` each MGR. The MGRs keep the mode across the restart; `Restore` returns them to CRC. | `TestMessengerClientProfileModeRejection/managers` |
| Revoke a client key and restore it | the suite's own client, or `TemporaryClientRevocation` and `ReplaceClientKey` | `TestClientRevocation`, `TestClientKeyReplacement` |
| Run native oracles | `python3-rados`, the `ceph` CLI, `crushtool` and `ceph-dencoder` in a `WithClient` container, or `RunNativeCommand` | `TestNativeCommands`, `TestRADOSMutationOutcomes` |

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
- **Short MON tickets.** With MON tickets under ten seconds, a MGR that renews
  late while it loads its modules can lose its MON session, and Ceph then
  aborts it; see [WithAuth](../ceph.md#withauth). The same happens to a
  hand-built cluster with the same lifetimes. That is why the 1.5-second
  profile's test is optional.
- **Keys.** Each identity has its own keyring. Read a key from
  `ClientConfig.ConnectionConfig`.
- **Stopping a daemon abruptly.** `Stop` sends SIGTERM and waits. Kill a
  container through the Docker API for a crash; see
  [Fault injection](fault-injection.md).
