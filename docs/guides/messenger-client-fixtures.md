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
implementation.

## Profiles

| Profile input | Values | Fixture options | Proven by |
| --- | --- | --- | --- |
| Image | Tentacle 20.2.4, Tentacle 20.2.3 | `ceph.Run(ctx, image, ...)` | every test below; `TestMessengerClientProfileWithoutKeyTypes` runs 20.2.3 |
| Messenger v1 | never bound or advertised | `NetworkConfig.WithMessengerMode(MessengerV2Only)`, or `MessengerV2Secure` | every profile |
| Monitors | three | `WithMonitorCount(3)` | every profile |
| Managers | two | `WithManagerCount(2)` | every profile but the next |
| | none at bootstrap, two started later | `WithNoInitialManagers`, then `AddManager("a")` and `AddManager("b")` | `TestMessengerClientProfileDelayedManagers` |
| OSD | none | `WithNoInitialOSDs` | `TestMessengerClientProfileSecure` |
| | one MemStore OSD | `WithOSDCount(1)`, `WithOSDStorage(OSDStorage{}.WithObjectStore(OSDMemStore))`, `WithPoolDefaults(1, 1)` | `TestMessengerClientProfileCRC` |
| CRUSH and pool | legacy tunables, a straw root, a rule that chooses OSDs; a pool with one PG and size 1; no pool with `hashpspool`, the MGR's `.mgr` included | compile the map with `crushtool` and install it with `osd setcrushmap` (see [Reproduce a minimal legacy placement setup](../../ceph/README.md#reproduce-a-minimal-legacy-placement-setup)); `osd_pool_default_flag_hashpspool = false` in a `WithConfigFile` `[global]` section; `CreatePool(ceph.PoolConfig{PGNum: 1, Replicas: 1, MinSize: 1, NoHashPSPool: true})` | `TestMessengerClientProfileCRC`, `TestOSDMemStoreLegacyPlacement`, `TestRADOSMutationContracts`, which check the flags of every pool |
| Key type / service cipher | `aes256k`/`aes256k` | `WithAuth(ceph.AuthConfig{KeyType: ceph.CipherAES256K})` | `TestMessengerClientProfileSecure` |
| | `aes`/`aes` | `AuthConfig{KeyType: ceph.CipherAES}` | `TestMessengerClientProfileModeRejection/managers` |
| | `aes`/`aes256k` | `AuthConfig{KeyType: ceph.CipherAES, ServiceCipher: ceph.CipherAES256K}` | `TestMessengerClientProfileCRC/bridge` |
| | `aes256k`/`aes` | `AuthConfig{KeyType: ceph.CipherAES256K, ServiceCipher: ceph.CipherAES}` | `TestAuthCiphersAndTicketLifetimes` |
| Ticket lifetimes | 12 s MON and service tickets | `AuthConfig{MonTicketTTL: 12 * time.Second, ServiceTicketTTL: 12 * time.Second}` | `TestMessengerClientProfileSecure`, where one librados session outlives two lifetimes of each ticket |
| | 120 s, with fast MON timing | `AuthConfig{MonTicketTTL: 120 * time.Second, ServiceTicketTTL: 120 * time.Second}` and a `WithConfigFile` `[global]` section with `mon_subscribe_interval`, `mon_tick_interval`, `mon_session_timeout`, `mon_client_ping_interval` and `mon_client_hunt_interval` | `TestMessengerClientProfileIdleTickets` |
| | 1.5 s MON tickets, 12 s service tickets | `AuthConfig{MonTicketTTL: 1500 * time.Millisecond, ServiceTicketTTL: 12 * time.Second}` and `mon_tick_interval = 1` in `[global]` | `TestMessengerClientProfileShortTickets`, an optional test that needs the `native_regression` build tag: the bootstrap, the lifetimes, and a MON and a MGR command from a client; see [Differences](#differences-from-a-single-container-bootstrap) |
| `global_id` reclaim | not allowed insecurely | always `auth_allow_insecure_global_id_reclaim = false` | `TestMessengerClientProfileSecure` |
| Connection modes | every connection secure | `MessengerV2Secure` | `TestMessengerClientProfileSecure`, which reads all six `ms_*_mode` options from each MON and MGR |
| | MONs, MGRs and OSDs accept only CRC from clients | `MessengerV2Only` with `NetworkConfig.WithMessengerPolicy(ceph.MessengerPolicy{Monitors: crc, Managers: crc, OSDs: crc})` | `TestMessengerClientProfileCRC`, which reads each daemon's service mode |
| | MONs accept only CRC, MGRs only secure | `MessengerV2Only` with `MessengerPolicy{Monitors: crc, Managers: secure}` | `TestMessengerClientProfileModeRejection/monitors` |
| | MONs accept only secure, MGRs only CRC | `MessengerV2Only` with `MessengerPolicy{Monitors: secure, Managers: crc}` | `TestMessengerClientProfileModeRejection/managers` |
| Client modes | secure, CRC, or secure then CRC | `WithClientMessengerPolicy` on a `WithClient` or `WithClientIdentity` container | `TestMessengerClientProfileModeRejection`, `TestMessengerClientProfileCRC` |
| Address family | IPv4 | the default | every other profile |
| | IPv6 | `NetworkConfig.WithAddressFamily(ceph.AddressIPv6)` | `TestMessengerClientProfileAddressFamilies/ipv6` |
| | IPv4-mapped IPv6 | `NetworkConfig.WithAddressFamily(ceph.AddressIPv4MappedIPv6)` | `TestMessengerClientProfileAddressFamilies/ipv4-mapped` |
| | IPv4 and IPv6 on every daemon | `NetworkConfig.WithAddressFamily(ceph.AddressDualStack)`; MON vectors list IPv6 first, the others IPv4 first (see [Dual-stack clusters](../../ceph/README.md#dual-stack-clusters)) | `TestDualStackAddressFamily`, `TestDualStackSeparateClusterNetwork` |
| Where the tests run | a container on the cluster network | a container with `WithClient` or `WithClientIdentity` | every profile |
| | a host process | `WithHostNetwork` (see [Host network](host-network.md)); every advertised MON, MGR and OSD address is reachable from the host | `TestMessengerClientProfileCRC/host`, where the test process reads each daemon's Messenger v2 banner |
| Client identities | full access, read-only, revocable, and one limited to the `fsid` command | `CreateClient` with `ceph.ClientCaps`, for example `{Mon: "allow command \"fsid\"", MGR: "allow r"}`; keys get the cluster's `KeyType` | `TestMessengerClientProfileSecure` |

`MessengerConnectionMode` values are `ceph.MessengerSecure` and
`ceph.MessengerCRC`; the table abbreviates them.

The MON requires the `OSDHASHPSPOOL` feature (`0x40000000`) of every client
while any pool has `hashpspool`, so a client without it is refused when it
opens its MON session. `NoHashPSPool` clears the flag only on the pool that
`CreatePool` creates. Ceph creates the MGR's `.mgr` pool on its own, with the
MON's default flags, so the default has to be off before that:
`osd_pool_default_flag_hashpspool = false` in a `WithConfigFile` file does
that at bootstrap. Check every pool's flags with `Observer.Pools` and `Pool.Status`, not
only the pool under test.

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
| Pause a MON or a MGR and resume it | `fault.Pause` and `fault.PauseLease.Resume`. The paused daemon's socket still accepts TCP connections, but the daemon sends nothing. A paused MON leaves the quorum to the other two. A MON paused for longer than the MON ticket lifetime can strand a daemon whose MON session was on it: the daemon's reconnect is refused with `EACCES`, and its MON client stops authenticating. With 12 s tickets and a 25 s pause, this happened to the active MGR in two of four runs. Check MGR-dependent behaviour before such a pause, or restart the stranded daemon. | `TestMessengerClientProfileSecure` |
| Fail over the active MGR | `cluster.Ceph(ctx, "mgr", "fail", name)`. The failed MGR restarts its process and registers as a standby again, about 1.5 s later on 20.2.4; wait until `ManagerStatus` lists it before the next control, such as a monmap change. A daemon that must authenticate again after the MONs have rotated away the secret of its ticket, for example after an election that outlasted the ticket lifetime, is refused with `EACCES` and can stay unregistered while its process runs (see the pause row); `Stop` and `Start` on its `Managers(ctx)` handle bring it back. See [MGR MonClients](#mgr-monclients) before treating a refusal as that case. | `TestMessengerClientProfileSecure`, which waits for the standby before its next control |
| Tell which MON session belongs to a registered MGR | the owned MONs' `sessions` admin socket command, through the `AdminSocket(ctx, "sessions")` method of each `*ceph.MonitorContainer`; see [MGR MonClients](#mgr-monclients) | `TestMessengerClientProfileManagerMonClients` |
| Stop one MGR and start it again | `Managers(ctx)`, then `Stop` and `Start` on the handle. `Start` waits only for the admin socket; see [MGR transition receipt](#mgr-transition-receipt) for when the MGR is registered again. | `TestMessengerClientProfileSecure`, `TestMessengerClientProfileManagerTransition` |
| Restart one MON | `Monitors(ctx)`, then `Stop` and `Start` | `TestMessengerClientProfileSecure` |
| Stop every MON and start them again | `Stop` each MON; start `b` and `c` before `a`. MON a's container checks `ceph status` when it starts, which needs a quorum. Each MON comes back at its monmap address. | `TestMessengerClientProfileSecure`, with the MONs down longer than a MON ticket lives |
| Stop MON a for good | `Stop` on its handle. With three MONs, the fixture's own CLI runs in a separate control container and keeps working. | `TestMessengerClientProfileSecure` |
| Start MGRs later | `AddManager` | `TestMessengerClientProfileDelayedManagers` |
| Add a MON that never answers, then remove it | `cluster.Ceph(ctx, "mon", "add", name, addr)` with an address nothing listens on, later `mon remove`. Each monmap change starts an election, and with a member missing the MONs wait for its election timeout: MON commands and new client sessions waited 6 to 11 s after the add. On Tentacle 20.2.4 the remove also took three election rounds and about 7 s; on Squid 19.2.5 one short round. Wait until `quorum_status` reports the new monmap and the same election epoch twice before starting new native clients, and give them more than that time. While the member is in the monmap, `RefreshClientMonitorConfig` would write its address into a client's `mon_host`. | `TestMessengerClientProfileUnreachableMonitor` |
| Make the MGRs accept only secure, then restart them | `override.SetMessengerPolicy(ctx, ceph.MessengerPolicy{Managers: secure})`, then `Stop` and `Start` each MGR. The MGRs keep the mode across the restart; `Restore` returns them to CRC. | `TestMessengerClientProfileModeRejection/managers` |
| Revoke a client key and restore it | the suite's own client, or `ClientConfig.Revoke` and `ClientConfig.ReplaceKey` | `TestClientRevocation`, `TestClientKeyReplacement` |
| Run native oracles | `python3-rados`, the `ceph` CLI, `crushtool` and `ceph-dencoder` in a `WithClient` container, or `RunNativeCommand` | `TestNativeCommands` (`ceph-dencoder`), `TestRADOSMutationOutcomes` (`python3-rados`), `TestMessengerClientProfileCRC` and `TestOSDMemStoreLegacyPlacement` (`crushtool`) |

### MGR MonClients

A MGR process authenticates more than one MonClient. Its beacon client opens
the MON session of type `mgr`, and its global ID is the GID that the MgrMap
registers. The active MGR's modules open clients of their own, sessions of
type `client` under the same entity name with other global IDs; a standby
runs no modules. In `TestMessengerClientProfileManagerMonClients` the active
mgr.a had its beacon session with global ID 4161, the MgrMap GID, and three
more with 4179, 4185 and 4191.

So a `(13) Permission denied` line in a MGR's own log does not say which of
its clients was refused, and a module client can be refused while the
registered slot stays healthy. The registered MGR has lost its MON session
only when an owned MON logs `attempt to reclaim global_id <N> using bad
ticket` (or another refusal that names the global ID) for N equal to the
GID that the MgrMap last registered for it, and the MGR then drops out of
the MgrMap. The MON logs are each MON container's output, of which
`CollectDiagnostics` keeps a bounded tail. Compare the global IDs; the entity
name alone is the same for every client of the daemon.

### MGR transition receipt

A client test that follows a MGR failover needs to know which daemon took
over and which registration belongs to which process at each step.
`testManagerTransitionReceipt` in
`internal/integration/manager_transition_integration_helpers_test.go` is the
recipe as a test helper; copy it into your tests. It runs with three MONs,
two MGRs and no OSDs, at the image's default Messenger policy, in three
phases:

1. Baseline: one available active and one standby, both owned. The test
   freezes the FSID, the MON names, both MgrMap registrations (name and GID)
   and both containers' IDs and start times.
2. Promotion: `Stop` on the active's `Managers(ctx)` handle. The phase waits
   until the captured standby is active with the same GID, container ID and
   start time, and the stopped MGR has left the MgrMap.
3. Restoration: `Start` on the same handle. `Start` waits only for the admin
   socket, so the phase waits until the old MGR is the only standby, with a
   new GID, the same container ID and a new start time, while the promoted
   MGR stays active with its identity. The old MGR need not become active
   again.

Each phase has a deadline of its own and takes samples until one is
accepted. A sample reads the MgrMap, the `sessions` of every frozen MON,
each MGR container's inspection, a fresh `quorum_status`, `ceph rbd task list`
(a command of the `rbd_support` MGR module, not RBD I/O) and the MgrMap again,
each query within its own bound. `classifyMgrSample` then judges it:

- `incomplete`: a query did not complete. Its status stays as observed:
  `missing`, `timeout`, `parse_failure`, `truncated`, `native_rejection` or
  `failed`. None of them is an observed absence, and truncated JSON never
  decodes to an empty map.
- `transitional`: the two MgrMaps differ, so the sample may mix the
  registrations of two maps. The phase keeps it and takes another sample.
- `violated`: the captured standby's container or start time changed, or it
  registered with another GID; an unowned MGR is active; the FSID changed;
  the old MGR's container was replaced, or after its restart it holds its old
  GID. The phase fails at once.
- `pending`: a condition is not met yet, for example a MON out of the quorum
  or a registration without its beacon session.
- `accepted`: every registration that the phase requires has its beacon
  session on some MON: type `mgr`, under the MGR's entity, with the MgrMap GID
  as its global ID (see [MGR MonClients](#mgr-monclients)). Module clients of
  the same entity do not count, and a MGR need not have a beacon session on
  every MON.

The test logs the receipt as `MGR_TRANSITION_RECEIPT`. For each phase it
keeps the window, the count of samples by state and the last four samples,
with selected fields only: `mgr dump` prints a few hundred KB of module
option descriptions, of which the receipt keeps the epoch, the availability
and the registrations. A failed `Stop` or `Start`, or a phase that ends
without an accepted sample, fails the test with the receipt so far.

On 20.2.4 and 19.2.5, with the official image and with the CI role images,
the MgrMap epochs went 4, 6, 7. The MgrMap moved to the standby 23 to 35 s
after the stop, and the promotion was accepted 35 to 39 s after it. Before the
move, a sample timed out on `rbd task list`, which waited for the stopped
active, and the sample during the move was transitional. The restarted MGR
was a standby with a new GID within 5 s of `Start`.

The receipt does not show which Messenger mode a connection used, and it
does not follow OSD, MDS or RGW daemons across the transition.

### Authentication first-failure bundle

When a client's first authentication to a MGR is refused, for example after
a rotating-key wipe, keep that rejection and collect what the stock images can
observe around it. Do not retry the authentication, wipe again, restart a
daemon or wait a fixed time to get a pass: each would replace the first
failure with a different run.

`collectAuthFirstFailureBundle` in
`internal/integration/auth_first_failure_integration_helpers_test.go` is the
recipe as a test helper; copy it into your tests. Right after the rejection,
before any diagnostic, freeze what you expect: the FSID, the MON names, and
the target MGR's name, GID, container ID and start time. Pass them with the
rejection as you recorded it, your declared provenance (producer and subject
sources, run, report), a held session if you kept one, and your installed
tickets with opaque, run-scoped labels and the epochs that their issuer
connection showed. Those epochs are not carried by the ticket. The helper
collects:

| Field | Source | Meaning |
| --- | --- | --- |
| `monitors` | `AdminSocket(ctx, "mon_status")` of each expected MON's `*ceph.MonitorContainer` | each MON's own rank, state, election epoch, quorum, MonMap epoch, `auth_epoch` (Tentacle; absent on Squid) and FSID, without a MON session |
| `quorum_names` | `QuorumStatus` | the quorum as a MON session reports it, a different observation from each MON's own state |
| `managers`, `managers_after` | `mgr dump`, `Managers` and each MGR container's start time, and every expected MON's `sessions` | each MGR's GID and container instance, before and after the collection, and its MON sessions split as in [MGR MonClients](#mgr-monclients): the beacon session's global ID and the module clients' |
| `native_controls` | `RunNativeCommand` of `ceph tell mgr.<name> version` for the tested target and the active MGR | behavioral controls, with their phase: a new client authenticated with its own ticket |
| `queries` | every query above | name, kind, start and end, and a status: `complete`, `unsupported`, `timed_out`, `truncated`, `failed` or `unavailable`, with a redacted error and output excerpt of at most 1 KiB otherwise |
| `first_failure`, `held_session`, `consumer_tickets`, `provenance`, `expected_target` | your records | stored as given |
| `generation_equality` | | always `unavailable/unverified` |

The collection has one total budget, here a minute, and a bound per query.
It runs from a context of its own, so a test context that ended with the
failure does not stop it, and it keeps going after a query fails. Every wait
ends with the budget: the MONs come from your frozen list, the MGR
containers from `Managers`, and errors are redacted within what is
left of the budget. An error that could not be redacted in time is replaced
by a fixed "text withheld" marker. The one exception is a control that
`RunNativeCommand` could not confirm as stopped: its `Stop` has a separate
cleanup allowance of a minute, outside the budget. A query that
fails is recorded with its status, not retried. Empty or non-JSON output, or a
status without its name, FSID or MonMap epoch, is a failed query, not an
observation; an empty session list is an answer. A command that the daemon
does not know, such as `quorum_status` on a MON's admin socket, is
`unsupported`. A native command whose stop cannot be confirmed is stopped
again and, if that fails, recorded under `cleanup_errors`. The rejection stays
as you recorded it, also when every query fails.

`classifyAuthBundle` then reports:
- whether the bundle is complete, and the problems if not: a query that did
  not complete, an expected MON without an observation or with another FSID,
  a MGR whose GID or container instance changed during the collection, a
  failed cleanup, a missing rejection, or a claimed generation equality;
- whether the target MGR still had the GID and container instance that you
  froze (`registered`), had another (`changed`), was missing from a MgrMap
  that was read (`absent`), or could not be observed (`unavailable`);
- which client a MON's refusal named, when you recorded its global ID: the
  registered MGR, one of its module clients, or unknown;
- secret IDs that tickets of different runs or cycles share. A wipe
  recreates the MGR key set, and its secret IDs start again, so an equal ID is
  not the same key;
- whether every control succeeded, and the rejection's result, unchanged.

It never reports generation equality. No stock command shows the rotating
key that the issuing MON used, or the one that the MGR selected to verify a
ticket, without the keys themselves. A successful control, before or after
the failure, or a later passing run does not show it either. Proving
equality needs a native observer outside the stock images. For the
cause of four analyzed failures, see
[Authenticating right after a rotating-key wipe](#authenticating-right-after-a-rotating-key-wipe).

`checkAuthBundleSecrecy` refuses a bundle that contains a secret you pass or a
field beyond its schema. The bundle holds no key, ticket, proof or auth
dump. Do not add `debug_auth` output, which prints keys (see
[Observing Cephx keys from the daemon side](native-commands.md#observing-cephx-keys-from-the-daemon-side)).

`TestAuthFirstFailureBundle` runs the recipe with three MONs and two MGRs
after one `auth wipe-rotating-service-keys`. Squid's MONs do not know that
command, and the test then collects without a wipe. Its tested
authentication is a native client with a wrong key, which the MONs refuse with
errno 13. Besides the complete bundle, it collects one with a budget that
ends at once (every query `timed_out`) and one whose control may keep 8 bytes
(`truncated`); both are incomplete and keep the rejection.
`TestAuthBundleRetainedFailure` replays a consumer's retained failure as a
bundle, with synthetic labels that keep their equality: complete, target
registered, controls successful, -13 preserved and generation equality still
unverified. `TestAuthBundleClassification` changes one fact at a time.

### Authenticating right after a rotating-key wipe

Right after a wipe, a MON can briefly still issue tickets sealed
with the service keys that the wipe replaced. A MGR that already holds the new
keys refuses such a ticket with -13 (`could not decrypt ticket info`).

On Tentacle 20.2.4, `auth wipe-rotating-service-keys` commits the new rotating
keys and a new auth epoch together. A MON applies the commit under its main
lock and refreshes the MonMap before the auth state. The MonMap refresh
already sends the new MonMap to the MON's subscribers. The authentication of a
new connection takes only the MON's auth lock, so until the auth state
follows, it issues tickets from the old keys.

A client that renews its tickets on a new MON connection as soon as it sees
the new MonMap can land in that window. Native clients do not: a MonClient
renews inside its open session with an `MAuth` message, which the MON
handles under its main lock, after the whole commit.

For a client of your own:
- Record, for each MON, the auth epoch of the first MonMap of each new
  session. The MON sends that MonMap under its main lock, so the MON had
  applied the auth state of that epoch by then.
- Discard renewal tickets from a handshake that started before the MON
  showed the epoch you expect, and authenticate to it once more.
- Do not wait a fixed time instead. The window has no bound.

The consumer analysis found this window as the common cause of four
auth-epoch failures and fixed those in its client using the epoch checks
above. Earlier rejections were not part of that analysis. Their
cause is not determined. The first-failure bundle above still does not show
which key a MON or MGR used, and a later passing run does not either.

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
| xattrs | `getxattrs` lists binary and empty values; `getxattr` of a removed name fails with `ENODATA` (61); `rmxattr` of a name never set succeeds and takes a new version; a missing object is `ENOENT` (2) | `TestRADOSObjectReceipts` |
| OMAP | keys-only listings carry no values; `max_return` pages continue with `start_after` until an empty page; the OSD caps a page at `osd_max_omap_entries_per_request` whatever the client asks; by-keys omits absent keys; header, single-key and key-range removal; clear also empties the header | `TestRADOSObjectReceipts` |
| rollback | rolling back to a self-managed snapshot restores its bytes, xattrs and OMAP at a new version; an object created after the snapshot is deleted | `TestRADOSObjectReceipts` |
| hints | `rados set-alloc-hint` keeps the bytes of an existing object and creates a missing one empty, and the object info holds the hinted sizes; fadvise flags change no result. No tool of the image issues a sparse read | `TestRADOSObjectReceipts` |
| statistics | pool statistics and `ceph df` follow objects of known sizes exactly; cluster statfs matches `ceph df` | `TestRADOSObjectReceipts` |

`TestRADOSMutationContracts` runs on the topology of the profiles above:
Messenger v2 only with CRC, AES256K keys where the image can make them, and a
MemStore OSD with the legacy CRUSH map and a pool without `hashpspool`. A
MemStore OSD loses its data when it is killed, so durability across a crash
needs BlueStore, as in `TestRADOSMutationOutcomes`. Delays inside the OSD are
described in [Fault injection](fault-injection.md).

## PG enumeration

A client lists a pool PG by PG with `CEPH_OSD_OP_PGNLS` requests. Each
request carries a cursor; the OSD returns at most `osd_max_pgls` entries and
a handle to continue from, and a result of 1 when the PG has no more objects.
`TestRADOSObjectReceipts` logs `RADOS_ENUMERATION_RECEIPT` for a pool of 4
PGs on one OSD, with `osd_max_pgls` lowered to 3 through `ceph tell` (and
restored afterwards), so that every PG needs several replies. Use it as the
native answer for a client's listing loop.

The recipe places objects with the MON's `osd map` command until every PG
holds at least seven objects of namespace `enum-a`, then writes those and
eight objects of namespace `enum-b`. Every third `enum-a` object of a PG and
every `enum-b` object carry the xattr `tag=blue`. The receipt holds:

- `placement`: the objects of each PG, by namespace, and the `blue` ones of
  `enum-a`;
- `listing`: every PGNLS request of a `rados -N enum-a ls`, read from the
  CLI's log (`debug_ms=1`, `debug_objecter=20`). Each step has the PG and
  operation of the `osd_op`, the cursor the Objecter sent (`pos`, `MIN` for
  the first), and the reply's result, entries and handle. PGs come in
  bitwise hash order (`5.0`, `5.2`, `5.1`, `5.3` for a pool with id 5). Each
  cursor is the previous handle. A continuation (result 0) holds at most 3
  entries, and its handle names the PG's first object not yet returned, in
  either namespace: the OSD counts objects of other namespaces against the
  limit and leaves them out of the entries. The PG's last reply has result 1
  and the next PG's start as its handle (`pg_ends`, such as `5:40000000::::head`),
  `MAX` after the last PG. A final request at `MAX` ends the listing;
- `per_pg_cli`: `rados --pgid <pg> -N enum-a ls` for each PG, equal to the
  placement;
- `all_namespaces`: `rados --all ls` lists both namespaces;
- `cursor`: `rados_object_list` (librados through `ctypes`, which
  python-rados lacks) over each PG's range from `rados_object_list_slice`,
  in calls of at most 2 entries with the filter `plain`, `_tag`, `blue` (the
  OSD stores the user xattr `tag` as `_tag`). For each PG: the entries of
  every call (only the blue `enum-a` objects of that PG; the last call can
  return none), the comparison of
  each cursor with the next (always -1), the comparison of the last cursor
  with the slice end (0), whether it is the pool end (only for the last PG),
  and the `pgnls-filter` requests on the wire with their results (0 until
  the last, which is 1);
- `filter.unknown`: a filter of the unknown type `nosuch`, which the OSD
  refuses with -22 (`EINVAL`).

A filter buffer is the type, then the type's arguments, each a string with a
32-bit little-endian length; the client prefixes it with `pg` and `filter`
in the request. The rados CLI and python-rados take no listing filter, and
no tool of the image sends the legacy `CEPH_OSD_OP_PGLS`.

## MDS sessions

A client that implements CephFS sessions needs to know what the native client
keeps when its MDS fails. These tests keep a native libcephfs client mounted
in a client container under `RunNativeCommand`, with a file descriptor that it
opened before the failure or the move, and publish its client ID, PID and inode in
receipts tied to one run. `Sessions` reports the session; `ceph tell
mds.<name> session ls` adds counters that `Session` leaves out, such as
`num_leases` and `num_completed_requests`.

| Profile | Native receipt | Proven by |
| --- | --- | --- |
| The only MDS is killed, its GID failed with `mds fail`, and its container started again | rank 0 returns with a new GID in the same container; the client's session is `open` again with the same client ID, PID and address and nonce; through the descriptor opened before the failure the client reads its bytes, appends and fsyncs, renames the file and sets a binary xattr; a client in another container reads the same inode, the exact bytes and the xattr | `TestCephFSRetainedClientAcrossMDSRestart`, with `MessengerV2Secure` on bridge and host networks |
| The active MDS is killed and its GID failed; an idle standby, or the journal follower of rank 0 (`StandbyReplay`), takes the rank | the same receipts, with rank 0 served by the standby under the GID it had before; afterwards the killed daemon starts again and rejoins as a standby or follower with a new GID, which `WaitReady` waits for | `TestCephFSRetainedClientAcrossStandbyTakeover/standby` and `/standby-replay`, with `MessengerV2Secure` |
| Two active ranks with a standby; the client's file lives in a directory pinned to rank 1 with `ceph.dir.pin`, and rank 1's MDS is killed and its GID failed | before the failure, rank 0's export and rank 1's import counters record the move, rank 1 reports the pinned directory as an authoritative subtree, and it holds an open session with the client; the standby takes rank 1 under its GID and reports the subtree in turn; rank 0 keeps its daemon and its session with the client; the descriptor and independent-reader receipts are the same as above | `TestCephFSRetainedClientAcrossRankOneTakeover`, with `MessengerV2Secure` |
| A second client opens, writes and fsyncs a file that a live client holds open with exclusive, buffered file capabilities, and keeps it open | the MDS's `ceph tell mds.<name> dump inode <ino>` shows the holder as the loner with `Fsxcrwb` before; the second client finishes at once, far below `session_timeout`, and reads the holder's fsynced bytes; while both clients hold the file, 10 s later, both have the shared `pAsLsXsFrw` and the holder's `last_sent` has grown; the holder keeps its session and is not blocklisted, and through its open descriptor it reads the second client's write; a third client checks the final file | `TestCephFSCapabilityHandoff`, with `MessengerV2Secure` |
| A client mounts; a second client mounts, creates a directory and keeps its mount; the first client looks the directory up; the second renames it and then removes it | creating the directory takes the shared file capability (`Fs`) on `/` from the first client, as `dump inode 1` shows, so the MDS answers its lookup with a dentry lease: its `num_leases` in `session ls` becomes 1 and the dentry's lock in `dump cache` is leased. The rename and the `rmdir` each finish within milliseconds; after each the client's `num_leases` is 0 and the old dentry is not leased, and the client sees `ENOENT` for the old name and the same inode under the new one (looking it up leases the new dentry). The client keeps its session and is not blocklisted. A client that mounts after the directory exists holds `Fs` on `/` and gets no lease | `TestCephFSDentryLeaseRecall`, with `MessengerV2Secure` |
| One active MDS; a client initializes libcephfs, mounts the root with an admin socket, unmounts; `session_timeout` is 30 s through `OverrideSessionTimeouts`. Then a client whose MDS cap is `allow rw path=/admitted` mounts `/admitted`, `/` and `/other`. Then a client that leaves `metric_collect` out of its features with `client_debug_inject_features` mounts, and `ceph fs required_client_features <fs> add metric_collect` requires it | `fs dump` names rank 0's daemon, GID and v2 address vector, which no MON, MGR or OSD in `DaemonAddresses` uses; the client reports the `ceph fsid` FSID, and before the mount it has GlobalID 0 and its process (hostname and PID in `client_metadata`) has no session. After the mount, `session ls` holds one `open` session with the client's GlobalID, `entity_id`, root and address, and the client's admin socket (`mds_sessions`) shows the same GlobalID, one rank 0 session to the same address vector and an MDSMap epoch no older than the one read. Within a minute (11 s in the recorded runs) `cap_renew_seq` moves on by two and `cap_ttl` is `last_cap_renew_request` plus 30 s, which the client sets only from the MDS's renew reply; the MDS's session uptime grows. After the unmount the session is gone and its address is not blocklisted. The restricted client mounts `/admitted` (its session's root); for `/` and `/other` the MDS rejects the session open and the mount fails with `EPERM` (1), leaving no session. A client of the release advertises every feature that `ceph fs feature ls` lists (`0xffffff` on Tentacle 20.2.4), so the test removes one: requiring it evicts the mounted client, whose address is blocklisted (`mds_session_blocklist_on_evict`), with a "missing required features" warning, and its later unmount returns 0; an ordinary client still mounts; a client without the feature is refused with `EPERM` and a "lacks required features" warning, and mounts again after `rm` | `TestCephFSSessionAdmission`, with `MessengerV2Secure` |
| A client is frozen with `fault.Pause` while the only active MDS is killed and a standby takes rank 0, with `mds_reconnect_timeout` set to 10 s through `override.SetConfig` | the FSMap shows the standby in `up:reconnect` for the window (about 15 s), then `up:rejoin` and `up:active`; the frozen client's session is gone and its address is in the blocklist; after `Resume`, its open descriptor fails, with `ESHUTDOWN` (108) on Tentacle and `EPERM` (1) on Squid, and it shuts down cleanly; a fresh client reads the bytes the frozen one had fsynced | `TestCephFSLateReconnect`, with `MessengerV2Secure` |
| Two active ranks; the client appends 16-byte records through one descriptor to a file in a directory pinned to rank 0, fsyncing each before the next; after a few records, another client sets the directory's `ceph.dir.pin` to 1, and after the stream to 0 again | rank 1 becomes the only rank that reports the directory as an authoritative subtree, in `get subtrees` of both ranks read between two `MDSStatus` reads with unchanged GIDs; rank 0's `exported` and `exported_inodes` and rank 1's `imported` and `imported_inodes` grow; more records are acknowledged under rank 1; the client has sessions on both ranks with one address and PID and is not blocklisted; after the stream, the client reads every record in order through its descriptor, and a client in another container reads the same bytes and xattr; a fresh mount reads the restored pin, and rank 0 is again the only authority before the client closes | `TestCephFSLiveRankMigration`, with `MessengerV2Secure` |
| One active MDS; the client renames a fsynced file within its directory once while `mds_log_pause`, set through `override.SetConfig`, holds the MDS's journal | the running daemon's own `config get` shows `mds_early_reply` true and the pause applied; the rename call returns with the early reply; twice, 3 s apart, the MDS's `dump_ops_in_flight` holds the client's request (`client.<id>:<tid> rename`) after `early_replied` and without `journal_committed`, and the client's admin socket (`mds_requests`) holds the same TID with `got_unsafe` and the file's inode; after the pause is restored, the client no longer holds it, and the MDS's `dump_historic_ops` shows it `journal_committed` and `replying`; the MDS keeps its GID; a client in another container finds the new name with the original inode and no old name | `TestCephFSUnsafeRequestWindow`, with `MessengerV2Secure` |
| The same held rename; the only MDS is then killed with SIGKILL, so the paused journal entry is lost, and its GID is failed. The pause is restored while no MDS runs, and the MDS container starts again | while no MDS runs, the client's `mds_requests` still holds the TID with `got_unsafe`; rank 0 returns with a new GID in the same container, and the restarted daemon's own `config get` shows `mds_log_pause` false; its `dump_historic_ops` shows the same `client.<id>:<tid> rename` with the `REPLAY` flag (Tentacle 20.2.4 and Squid 19.2.5 both print `RETRY=1 REPLAY QUEUED_FOR_REPLAY`), without `early_replied`, `journal_committed` before `replying`; then the client no longer holds the TID; a client in another container finds the new name with the original inode and no old name. The test issues no second rename | `TestCephFSUnsafeRequestReplay`, with `MessengerV2Secure` |
| The same held rename; the client's process is then killed with SIGKILL inside its container, with `session_timeout` 30 s and `session_autoclose` 60 s set through `OverrideSessionTimeouts`. The pause is restored a few seconds later, and the MDS keeps running | `/proc/<pid>/cmdline` of the receipt's PID carries the run's token before the kill, and `RunNativeCommand` returns exit -9; with the pause still applied, the MDS's `dump_ops_in_flight` holds the dead client's request after `early_replied` and the session is still `open`; after the restore the MDS's `dump_historic_ops` shows the same `client.<id>:<tid> rename` `journal_committed` and `replying`, and the MDS keeps its GID; every 2 s the session is seen only as `open`, and it is gone about 55 s after the kill (Tentacle 20.2.4), and its address is in the blocklist; a client in another container finds the new name with the original inode and no old name | `TestCephFSClientKilledDuringUnsafeRequest`, with `MessengerV2Secure` |
| One active MDS; the client fsyncs a file, then, while `mds_log_pause` holds the MDS's journal, appends through the same descriptor and fsyncs again; the only MDS is then killed with SIGKILL and its GID failed, the pause is restored while no MDS runs, and the MDS container starts again | twice, 3 s apart, the client's `dump_cache` holds the inode with a flushing `Fw` cap, a flush TID and the full size, the MDS's own `dump inode` has the base size, and the fsync has not returned; with no MDS running the client still holds the flush; rank 0 returns with a new GID in the same container without the pause; then the client holds no flushing cap, the MDS has the full size and the original fsync returns 0, with no second fsync; a client in another container reads every byte | `TestCephFSFlushingCapReplay`, with `MessengerV2Secure` |
| Two live clients in two containers open one file; the holder takes `flock(LOCK_EX\|LOCK_NB)`; the contender tries the same, then a blocking `LOCK_EX`; the holder unlocks | the contender's non-blocking lock returns `EWOULDBLOCK` (11); its blocking lock does not return, and twice, 3 s apart, the MDS's `dump_ops_in_flight` holds the same `client.<id>:<tid> setfilelock rule 2, type 2, ... wait 1 #0x<ino>` request (rule 2 is flock, type 2 exclusive) at `failed to add lock, waiting`; after the holder's `LOCK_UN` returns 0, the waiting call returns 0 and the request leaves the MDS; the former holder's non-blocking lock now returns `EWOULDBLOCK`; both clients keep their sessions and neither is blocklisted. The MDS's `dump inode` does not list the held flocks | `TestCephFSFlockContention`, with `MessengerV2Secure` |
| One active MDS and one standby; two live clients in two containers open one file; the holder takes `flock(LOCK_EX\|LOCK_NB)` and the contender's same call is refused; the active MDS is then killed with SIGKILL and its GID failed, and the standby takes rank 0 | the standby becomes `up:active` under the GID it had before (3 s after the failure in the recorded runs); both clients keep their sessions with the same address and PID, `open` and not `reconnecting` on the new MDS. The standby never saw the lock, but the contender's next `LOCK_EX\|LOCK_NB` returns `EWOULDBLOCK` (11), and the new MDS's own `dump_historic_ops` holds that `setfilelock ... wait 0` request. The contender's blocking `LOCK_EX` waits in the new MDS's `dump_ops_in_flight` at `failed to add lock, waiting` and has not returned; after the holder's `LOCK_UN` it returns 0 and leaves the MDS, and the former holder's non-blocking lock returns `EWOULDBLOCK`. Neither client is blocklisted | `TestCephFSFlockAcrossMDSFailover`, with `MessengerV2Secure` |
| Two `ceph-fuse` mounts, in two client containers with `/dev/fuse`, `CAP_SYS_ADMIN` and `apparmor=unconfined`, of one directory that a CephX identity with `allow rw path=` caps may mount; the holder takes `F_SETLK` with `F_WRLCK` on bytes 0 to 4095 of one file; the contender tries the same, asks `F_GETLK`, then waits in `F_SETLKW`; the holder unlocks and takes the lock again; its `ceph-fuse` is then killed with SIGKILL | the contender's `F_SETLK` returns `EAGAIN` (11) and `F_GETLK` reports the holder's `F_WRLCK` on the same range; twice, 3 s apart, the MDS's `dump_ops_in_flight` holds the contender's `client.<id>:<tid> setfilelock rule 1, type 2, ... wait 1 #0x<ino>` request (rule 1 is fcntl) at `failed to add lock, waiting`, and the call has not returned; after the holder's `F_UNLCK` it returns 0 and the request leaves the MDS, and the former holder's `F_SETLK` returns `EAGAIN`. After the kill the holder's mount stays in its container's mount table and returns `ENOTCONN` (107), its session stays `open`, and the dead client's lock still refuses the contender with `EAGAIN`; once the mount is unmounted and the session evicted (its address is blocklisted), the contender's `F_SETLK` returns 0. The test logs `CEPHFS_FCNTL_LOCK_RECEIPT` | `TestCephFSFuseMount`, with `MessengerV2Secure`, in the optional batch `cephfs_fuse`; needs `/dev/fuse` on the engine and the official image's `ceph-fuse` (see [CI](../contributing/ci.md#native-regressions)) |
| One active MDS and one standby; a client with an admin socket creates and fsyncs a file in a new directory, and `flush journal` writes the directory back; the client renames the file once within the directory; the active MDS is then killed with SIGKILL and its GID failed, and the standby takes rank 0 | before the rename the directory's dirfrag object (`<dir ino>.00000000` in the metadata pool, read with `rados listomapkeys`) holds only `source_head`. The MDS's `dump_historic_ops` shows the rename `early_replied`, `journal_committed` and `replying`, the client's `mds_requests` no longer holds it, and the MDS's `perf dump mds_log` shows `evadd` and `wrpos` moved, while the dirfrag object still holds only `source_head`: the rename exists in the journal alone. After the takeover the standby's own `mds_log` counters show `replayed` above 0, `expos` before the rename's entry and `wrpos` at or past the killed daemon's; the client keeps its session and finds the new name through its own mount; a client in another container finds the new name with the original inode and no old name; after the new MDS's `flush journal` the dirfrag object holds only `target_head` | `TestCephFSJournaledRenameAcrossMDSCrash`, with `MessengerV2Secure` |
| One active MDS; a client with an admin socket fsyncs base bytes, then writes a tail without fsync (`client_oc_max_dirty_age` 600 s keeps it in its cache); a second live client in another container has the file looked up; `ceph tell mds.<name> client evict id=<client>` | the client's address first appears in the blocklist of OSDMap epoch E (the epoch before does not list it), and the MDS's `status` reports `osdmap_epoch_barrier` E (it was the previous epoch); the MDS drops the session; the evicted client's admin socket `status` reports `osd_epoch` E and `blocklisted` true, and its stderr says `I was blocklisted at osd epoch E`. Its fsync then returns `ESHUTDOWN` (108), a new write and a `stat` return 108, and a second fsync returns 0 because the client dropped its dirty data; it still closes and unmounts with 0. The second client reads only the fsynced bytes; it appends and fsyncs, and the MDS's flush ack sets its own `osd_epoch_barrier` to E | `TestCephFSBlocklistEpochBarrier`, with `MessengerV2Secure` |
| One active MDS; a client with an admin socket fsyncs base bytes and keeps its descriptor; full ratios of 0.0001, 0.0002 and 0.0003 through `override.SetFullRatios`, then `Restore` | the OSDs report themselves full and the MON flags the file's data pool `full` in OSDMap epoch F with `OSD_FULL`; the client then reads a file that another client wrote, and once its `status` reports `osd_epoch` F, its write returns `ENOSPC` (28) at once and the size stays the base size, while fsync, `mkdir` and `stat`, which need only the MDS, return 0; after `Restore` the flag clears, and once the client holds that map (it reads a second such file) the same descriptor writes and fsyncs with 0; a client in another container reads every byte; the client keeps its session and is not blocklisted | `TestCephFSOSDFullRetainedClient`, with `MessengerV2Secure` |
| One active MDS and two OSDs with replica-2 pools, so every PG holds both OSDs; a client with an admin socket fsyncs base bytes and keeps its descriptor, and a second client in another container is mounted. `nodown` is held through `override.SetOSDFlag` and the acting primary of the file's first object is frozen with `fault.Pause`; the first client writes a tail and fsyncs, the second makes a directory; then `Resume` | the buffered write returns at once and the fsync does not: the client's `objecter_requests` holds a `write` of `<ino>.00000000` to the frozen OSD. The `mkdir` is the second client's first request that creates an inode, so the MDS gives it no early reply: its `dump_ops_in_flight` holds the request at `submit entry: journal_and_reply` without `journal_committed`, the client's `mds_requests` holds the TID without `got_unsafe`, and the MDS's own `objecter_requests` holds writes of its journal objects (`200.*`) in the metadata pool. All three are held for at least 5 s, both sessions stay `open` and neither client is blocklisted. After `Resume` the same fsync and the same `mkdir` return 0 (about 7.5 s after they started in the recorded runs), and the MDS's history shows the `mkdir` committed before its reply; `nodown` is restored only then. Through its original descriptor the first client reads every byte and finds the new directory, a third client reads every byte, and the MDS keeps its GID | `TestCephFSOSDInterruption`, with `MessengerV2Secure` |
| One active MDS and one OSD; a client with an admin socket fsyncs base bytes and keeps its descriptor, and a second client in another container is mounted. The OSD's public address comes from the OSDMap and the MDS's v2 address from the FSMap, each matched to its container's endpoint. `ApplyNetworkProfile` on the MDS's container with `Peer` set to the OSD's address and `Loss: 100` drops only the MDS's packets to the OSD; the first client appends a tail and syncs the data only, then fsyncs fully, and the second makes a directory; then `Restore` | the data-only sync returns 0 within milliseconds, RADOS holds the new bytes in the file's first object, and the client's `objecter_requests` holds no data op: the clients still reach the OSD. The MDS does not: its own `objecter_requests` holds writes of its journal objects (`200.*`) to the OSD, the second client's `mkdir` (its first inode, so no early reply) waits in `dump_ops_in_flight` at `submit entry: journal_and_reply` without `journal_committed` and in that client's `mds_requests` without `got_unsafe`, and the first client's full fsync waits with a flushing `Fw` cap in its `dump_cache` while the MDS's `dump inode` keeps the base size. All are held for at least 5 s, both sessions stay `open`, and netem counts the dropped packets. After `Restore` the same fsync and `mkdir` return 0 (about 13 s after they started in the recorded runs, as TCP retransmits on its backed-off timer), the MDS's history shows the `mkdir` committed before its reply, and the MDS has the full size. Through its original descriptor the first client reads every byte and finds the directory, a third client reads every byte, no client is blocklisted and the MDS keeps its GID. The test logs `CEPHFS_MDS_OSD_PATH_RECEIPT` | `TestCephFSMDSOSDPathCut`, with `MessengerV2Secure`; needs the netem helper |
| The same with two OSDs, each holding a copy of the file's object and of the journal. One `ApplyNetworkProfile` on the MDS's container with `Peers` set to both OSDs' public addresses and `Loss: 100` drops the MDS's packets to both, through one u32 filter per OSD | the same receipts, with the MDS's journal writes held toward the journal's primary while the clients' data syncs complete; the receipt lists both OSDs, their addresses and the kernel's filters `800::1` and `800::2` | `TestCephFSMDSOSDsPathCut`, with `MessengerV2Secure`; needs the netem helper |
| One active MDS and two OSDs with replica-2, `min_size` 1 pools; a client with an admin socket fsyncs base bytes and keeps its descriptor. `noout`, `norecover` and `nobackfill` are held through `override.SetOSDFlag`, and the acting primary of the file's first object is stopped; the client appends and fsyncs; the OSD starts again and the client appends and fsyncs once more; then `norecover` and `nobackfill` are restored, and after recovery `noout` | with the OSD down, `Pool.PGs` reports the object's PG `active+undersized+degraded` on the other OSD alone, with a degraded object counted, and the append and fsync through the original descriptor return 0 within milliseconds. With the OSD up again the PG is back on both OSDs as `active+recovery_wait+degraded` (or `backfill_wait`) with its degraded object counted, and the next append and fsync return 0. A client write to a missing object makes the primary recover that object even under `norecover`, so the PG can count no degraded object before the flags are released, as it did in most recorded runs; the reports reach the MGR on their own schedule, so the test only logs that count. After the release the metadata and data pools reach `active+clean` with no degraded object, and the PG returns to its original acting set. The client keeps its session under the same address and reads every byte through its descriptor, a second client reads every byte, no client is blocklisted and the MDS keeps its GID. The test logs `CEPHFS_OSD_RECOVERY_RECEIPT` with the states it saw | `TestCephFSOSDRecovery`, with `MessengerV2Secure` |

In the failover profiles above the unsafe-request ones, the client fsynced its
data before the failure, so those receipts show a session reconnect, not the
replay of a request that was in flight. The
capability receipt shows the revocation in the MDS's state, not the messages
that carried it, and it reads that state while both clients hold the file:
once the second client closes it, the MDS may make the holder the loner again
and give the capabilities back. `MDSStatus` lists only active ranks; the
late-reconnect test reads the recovery states from `fs dump`. The live
migration test sets the pin through one mount, reads it back through a fresh
one and only then waits for the authority: the pin is a policy, and the
subtree follows it later. Each of its records was fsynced before the next one
started, so it does not cover a request that was in flight when the export
froze the directory.

The unsafe-window receipt is the one profile with a request in flight: the
client got an early, unsafe reply to its rename and holds the request until
the MDS journals it. The test asks the client for its pending requests
through an admin socket that the actor sets with
`conf_set("admin_socket", path)` before it mounts. The client prints them as a
`requests` array on Tentacle and as one `request` object per request on
Squid. The MDS replies early only when it can, so a request that allocates an
inode, such as the actor's `mkdir`, has no unsafe reply (`TestRequestSafety`
keeps that native sample).

The replay receipt follows the same request across an MDS crash. SIGKILL
drops the paused journal entry with the process, so the restarted MDS has no
record of the rename. During reconnect the client resends each request that
got an unsafe reply, flagged as a replay, and the MDS executes it again,
journals it and replies once, without an early reply. The test checks this in
the MDS's history with the client ID and TID of the original call
(`TestReplayedRequest` keeps the native sample), and it never renames twice.
A crash after the entry reached the journal is the journaled-crash receipt
below. Dirty file data, capability flushes and lease recalls that were in
flight with the request stay outside it.

The killed-client receipt keeps the MDS and drops the client instead. The
MDS already executed the rename when it sent the early reply, so the client's
death does not undo it: once the journal moves, the MDS commits the request
and sends the safe reply to a session whose client is gone, and the new name
is what every other client sees. A client of your own that dies after an
unsafe reply must therefore treat the operation as done, not as lost. The
dead session holds capabilities that nobody else asks for, so the MDS keeps
it `open` and closes it only at `session_autoclose`, as for an idle client,
then blocklists the address. A client that needed those capabilities would
see the eviction after `session_timeout` instead, as `TestCephFSPausedClientEviction`
shows. The receipt does not cover a client killed before the early reply, or
one killed while its MDS also fails.

The cap flush receipt holds the other kind of unacknowledged change: file
metadata that a client sends with its capabilities instead of a request. The
fsync writes the appended bytes to the OSDs, which the pause does not hold,
and then sends the new size in a cap flush. The MDS cannot journal it, so it
sends no ack, and the client keeps the flush with its TID (Ceph prints the
field as `flushings_caps`). After the crash the restarted MDS knows only the
base size; the client sends its flushing caps again when it reconnects, and
the MDS journals the size and acknowledges them. Without that, a reader would
see only the base bytes, because the size bounds every read.
`TestParseClientCache` keeps the client's native `dump_cache` samples; a
dentry there also names the inode it links, so only the entry with the head
snapshot ID is the inode. The receipt does not cover a cap flush that reached
the journal before the crash, or a lease recall.

The flock failover receipt shows where a lock lives across a failover. The
MDS keeps flocks in memory and does not journal them, so a standby that takes
the rank knows no lock until the clients reconnect: each client sends the
flocks it holds with the capabilities of each inode, and the new MDS rebuilds
the lock table from them before it goes active. A client of your own must
therefore send its held locks in its reconnect; a client that does not loses
them silently, and the new MDS grants the lock to the next contender.
Requests that were waiting for a lock when the MDS failed are not in the
receipt: the contender asks again only after the failover. `fcntl` locks take
the same path in Ceph, but `cephfs.LibCephFS` cannot take them, so the
`fcntl` receipt comes from two `ceph-fuse` mounts instead. It covers
contention, a waiting lock held in the MDS and a killed holder whose lock
lasts until its session is evicted; it does not cover `fcntl` locks across
an MDS failover.

The journaled-crash receipt is the other side of the replay receipt. The
rename's safe reply came after its journal entry was committed, and stock
Ceph offers no gate between the two, so the crash comes after the safe reply.
The MDS writes metadata back to the directory objects only when it expires a
journal segment, so the entry is still in the journal alone when the daemon
dies. The standby reads the journal from the last expire position and
replays it, and the rename survives without any request from the client. A
client of your own may treat a safe reply as durable across an MDS crash, and
must not send the request again.

The epoch barrier receipt shows how eviction fences a client's data. The MDS
asks the MON to blocklist the client's address, records the OSDMap epoch
that holds the entry as its barrier, and from then on sends that barrier with
its capability messages; a client that gets it waits for that map before it
sends more OSD operations, so it cannot act on capabilities that the evicted
client lost while an OSD still accepts that client's writes. The evicted
client is subscribed to OSDMap updates, so it gets epoch E at once, aborts its
MDS sessions, cancels its OSD writes and drops its dirty data with
`ESHUTDOWN`. Its writes therefore fail in the client before they reach an
OSD, and the receipt does not show an OSD rejecting a write sent with an older
map (the OSD would answer such a write with `EBLOCKLISTED`, 108). A client of
your own must fail the same way: dirty data that it did not fsync before the
eviction is lost, and only the fsynced bytes survive. The fullness receipt is
also decided in the client: with a map that flags the pool full, the native
client returns `ENOSPC` for a write without sending it, and the MDS still
serves metadata. An idle client does not always fetch the new map by itself
(in the recorded runs a Squid client never had it, a Tentacle client
sometimes did), so the
test has it read data that it has not cached, once after the pool fills and
once after it drains; the OSD that serves the read sends it the newer map.

The OSD interruption receipt shows a client and an MDS waiting on an OSD
that is up in the map but answers nothing. Neither the client nor the MDS
times out an OSD operation by default, so both keep the operation in flight
until the OSD answers or a new OSDMap moves the PG; with `nodown` held no
such map comes, and the calls return when the OSD thaws. The MDS itself keeps
running: its session renewals and admin commands need no OSD, so the
clients keep their sessions. A client of your own should behave the same
way: block the call, keep the session, and complete the same descriptor's
write and fsync when the OSD answers, without reopening anything. The
receipt holds the MDS's journal at the OSD, not with `mds_log_pause`, so the
journal entry has reached the Objecter. Every PG holds the frozen OSD, so it
stalls the file's data and the journal together; it does not separate a
stall of the MDS's path to the OSD from the client's. Without `nodown` the other OSD
reports the frozen one down after the heartbeat grace and degraded I/O
resumes before the thaw, which `TestPausedOSDFaults` shows for RADOS
clients.

The MDS-to-OSD receipt separates the two. A peer-selective
`ApplyNetworkProfile` on the MDS's container drops only its packets to the
OSD, so the clients' data path stays up while the MDS cannot write its
journal. On a one-OSD fixture this cuts the MDS off from every OSD; with more
OSDs, the one lease that a target endpoint can hold cuts only one peer. A
client of your own sees the split as the native client does: data writes and
data-only syncs complete, while anything that needs the MDS's journal waits,
both a request without an early reply and the cap flush that a full fsync
sends with the new size. Nothing in the session reports the stall: the MDS
keeps renewing it, and no timeout fires. After the restore the MDS's held TCP
segments go out on the backed-off retransmission timer, so the waiting calls
can take several more seconds to return.

The recovery receipt keeps a live client writing through one descriptor
while an OSD that holds its file is down, then up again with recovery held,
then recovered. A write needs only `min_size` copies, so none of the client's
calls waits, and the client never sees the PG states. The flags make the
intermediate states observable; without them a few objects recover before a
`pg ls` can report `recovery_wait`. The receipt covers log-based recovery
after a short outage. Backfill after a long one, an OSD that comes back
empty, and writes held because a PG fell below `min_size` are not covered.

### Metadata operations

`TestCephFSMetadataRecipe` gives a client implementation one native answer
per metadata operation to compare with. A single libcephfs client (the
Python `cephfs` binding, under `RunNativeCommand`) runs a fixed sequence on
one active MDS with `MessengerV2Secure`, and its receipt records for each
operation the errno (0 on success) and what the binding returned: inode,
mode, link count, owner, size, `atime` and `mtime` (whole seconds, as the
binding's `stat` reports them), bytes, names and xattr values. The test logs
each row and a `CEPHFS_METADATA_RECIPE` line.

| Operations | Native receipt |
| --- | --- |
| `mkdir`, then 1500 empty files in one directory; `opendir` and `readdir` | with `mds_bal_split_size` lowered to 1000 through `override.SetConfig` and read back from the daemon, the MDS splits the directory into 8 fragments (`dirfrag ls`); the listing returns `.`, `..` and every name exactly once, and the MDS's `req_readdir_latency` count in `perf dump mds_server` grows by one request per fragment (8 on Tentacle 20.2.4) |
| `open` with `O_CREAT`, `write` of 1000 random bytes, `fsync`, `fstat`, `close`, `stat`, `lstat`, `read` | one inode, mode `100644`, one link, the written size and bytes |
| `chmod 0640`, `chown 4242:4343`, `truncate` to 600, `utime` | `stat` returns exactly those values on the same inode |
| `link` | both names return the same inode with two links; the MDS's `dump inode` reports `nlink` 2 and size 600 |
| `rename` over an existing file | the target name returns the source's inode and size, the source name `ENOENT`; the MDS holds the old target inode with no links under `~mds0/stray<N>` |
| `symlink` to a relative target, `readlink`, `lstat`, `stat` | the target string, mode `120777` and the target's length as the link's size; `stat` follows to the file's inode |
| `setxattr`, `getxattr`, `listxattr`, `removexattr` of `user.*` names with a binary value | the exact value bytes; `listxattr` returns only the `user.*` names that were set; a removed or missing name returns `ENODATA` (61) |
| errors | `mkdir` of an existing directory `EEXIST` (17); `stat` of a missing name `ENOENT` (2); `rmdir` of a non-empty directory `ENOTEMPTY` (39); `open` of a directory for writing and `unlink` of a directory `EISDIR` (21); `mkdir` below a file and `rmdir` of a file `ENOTDIR` (20) |
| `flock` with two owners on two descriptors of one client | the second owner's exclusive or shared non-blocking lock returns `EWOULDBLOCK` (11) while the first holds an exclusive lock, and succeeds after the first unlocks |

A client in another container then lists the root and compares every name,
inode, link count, mode, owner, size and time with the actor's last
receipts, the bytes of both files, the symlink target and the xattrs.
`cephfs.LibCephFS` exposes no `fcntl` (POSIX record) locking, so the recipe
covers only `flock`, and libcephfs has no option for the size of a readdir
reply: it asks the MDS for one fragment at a time, and a reply holds at most
about 512 KiB of entries, so the test splits the directory to get several
requests.

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
| OSD primary to replica, one direction, cluster network | A separate cluster network, two OSDs, and a pool with one PG on both, two replicas and `min_size` 2. `ApplyNetworkProfile` on the primary's cluster endpoint with `Peer` set to the replica's cluster address and `Loss: 100` drops only the primary's packets to the replica. The primary reports the client's write `waiting for sub ops` from the replica, with no replica commit, for the whole 45 second window, which outlasts `osd_heartbeat_grace` (20 s). The back-interface heartbeats fail both ways: the primary's pings are dropped, and the replica's pings arrive but the replies do not. The MON's cluster log therefore has each OSD `reported failed by` the other, but with one reporter each against a `mon_osd_min_down_reporters` of 2, both OSDs stay up in the same OSD map epoch. Meanwhile MON commands answer and the same client writes and reads an object in a one-replica pool. The netem qdisc sends none of the selected packets and drops some, while the primary's cluster interface keeps receiving the replica's packets, so the other direction is not cut. After `Restore` the writer completes, and clients of their own read the same bytes and version through the primary and then, with the primary stopped and `min_size` 1, from the replica alone. The test logs `OSD_PEER_PATH_RECEIPT`. | Native state (the primary's in-flight op, MON failure reports and OSD map, netem and interface counters) | `TestOSDReplicationPeerPathCut` |
| MON to MON | Cutting two of five MONs off the public network leaves a quorum of three; restoring them brings back five. | Native state (quorum membership). Which daemon received which map is not observed. | `TestFiveMonitorQuorumAndNetworkRecovery` |
| MON to MON and MON to OSD, map propagation | Each daemon is asked for the map it holds on its own admin socket, without a MON session. After `CreatePool` makes a new OSDMap epoch, each OSD's `status` reaches it in `newest_map`. After `AddMonitor("d")` makes MonMap epoch N+1, each of MONs a, b, c and d reports in `mon_status` that epoch, all four members, a quorum of four and the same election epoch; after `MonitorContainer.Remove("d")`, a, b and c report N+2 with three members. The test logs `MAP_PROPAGATION_RECEIPT` with the first read that showed each daemon the new epoch. `AddMonitor` returns once the MON has joined, so those times order the reads and are not a propagation latency. | Native state (each daemon's own map epoch) | `TestMapPropagationReceipt` |
| OSD to OSD when a replica missed writes | One replica of a one-PG, two-replica pool stops while writes continue, then starts again with recovery held by `norecover` or `nobackfill`. The primary's `pg query` tells the two paths apart. In log recovery the replica stays in the acting set, has no backfill target and `last_backfill` `MAX`, and the PG log still reaches back to the replica's last entry. In backfill the PG log has been trimmed past that entry, and the replica is only in the up set, as the backfill target with `last_backfill` `MIN`. While the flag is set, `num_objects_recovered` does not move; afterwards it grows by exactly the objects written while the replica was down. A client of its own reads every object, and with the primary stopped the replica serves them alone. The same receipt runs on a K=2, M=1 erasure-coded pool, where the replica is the OSD of one shard: `pg query` names it like `2(1)`, backfill leaves its acting slot empty (`2147483647`), and with the primary stopped no object can be rebuilt without the recovered shard. The test logs `OSD_RECOVERY_RECEIPT`. | Native state | `TestOSDRecoveryAndBackfillReceipts/replicated` and `/erasure` |
| OSD to OSD after a placement change | With `nobackfill` and `norecover` held, a one-replica pool's PGs map to another OSD and report `recovery_wait` or `backfill_wait` while the objects stay on the old one. After the flags are restored, the PGs are clean on the new OSD and the data reads back. | Native state. Client reads recover objects on demand, so the test does not tell log recovery from backfill. | `TestPoolPGRelocation` |
| MDS to MDS subtree export | An export pin moves a directory's authority to the pinned rank, as `get subtrees` on the owned ranks reports. With `ceph.dir.pin` on a directory that a client has open, rank 0's `exported` and `exported_inodes` perf counters and rank 1's `imported` and `imported_inodes` grow, read with `ceph tell mds.<name> perf dump mds` before and after. In the live migration the client keeps writing during the move, and both ranks must agree on the single authority. | Native state (per-daemon counters) for the pinned directory; outcome for subvolume pins | `TestCephFSRetainedClientAcrossRankOneTakeover`, `TestCephFSLiveRankMigration`, `TestCephFSPins` |
| MGR to MON, registration and failover | The MONs drop the active MGR whose process stopped and promote the captured standby, which keeps its GID and process. The restarted MGR registers again as a standby with a new GID. Each registration has its beacon session on a MON, and the MON quorum and a MGR module command answer in every phase. The test logs `MGR_TRANSITION_RECEIPT`; see [MGR transition receipt](#mgr-transition-receipt). | Native state (MgrMap and MON sessions) | `TestMessengerClientProfileManagerTransition` |
| OSD to OSD heartbeats, cluster network | Three OSDs on separate CRUSH hosts; one OSD's cluster endpoint is cut. Its back-interface heartbeats with both peers fail, and after `osd_heartbeat_grace` (20 s) the MON's cluster log records it `failed` with 2 reporters from different hosts, `mon_osd_min_down_reporters` and `mon_osd_reporter_subtree_level` being 2 and `host`. `osd dump` marks it down while both peers stay up from the same epoch, and a client keeps reading and writing a pool with 3 replicas and `min_size` 2. The marked OSD logs `Monitor daemon marked osd.N down, but it is still running`, cannot rebind its cluster messenger (`rebind cluster_messenger failed`), tells the MONs it is dead as of that epoch or the next map it is handling, and exits. After `Restore` and `Start` it comes up with the same UUID from a later epoch. The test logs `OSD_HEARTBEAT_RECEIPT`. | Native state (MON failure record, OSD map, the OSD's own log) | `TestOSDHeartbeatFailure` |
| RGW to RADOS | A gateway with one OSD. The test reads what the gateway wrote with `rados` and `radosgw-admin`, not through the gateway. A 1031-byte S3 object is one head object `<marker>_<key>` in the bucket data pool with the same bytes, and its key is in the omap of one of the bucket's index shard objects (`.dir.<id>.<shard>`). A 6 MiB object is a 4 MiB head object and a 2 MiB tail stripe `<marker>__shadow_.<prefix>_1`; the tail name has a random upload prefix, not the key. Deleting it removes the head and the index key at once, and the garbage collector lists the tail until `gc process --include-all` removes it. The test logs `RGW_RADOS_RECEIPT`. | Native state (RADOS objects, index omap, GC queue) | `TestRGWRadosReceipt` |
| Source OSD to the destination's rbd-mirror daemon | Two clusters with one OSD each and a snapshot-mode RBD mirror. On the source network, the source OSD drops every packet it sends to the rbd-mirror daemon's address (`ApplyNetworkProfile` with that `Peer` and `Loss` 100), and only those. The source client writes 1 MiB and creates a mirror snapshot through the same OSD. For 30 s the daemon stays `up+replaying`, but its status keeps the old remote and local snapshot timestamps: it does not even see the new snapshot. The destination keeps the old bytes. netem counts the dropped packets. After `Restore`, the new snapshot is reported in sync and the destination has the new bytes; only the changed 1 MiB was copied. The test logs `RBD_MIRROR_PATH_RECEIPT`. | Native state (the mirror's snapshot progress, netem counters) | `TestRBDMirrorPeerPathCut` (bridge) |
| Source OSD to the cephfs-mirror daemon | Two clusters with one OSD each and a CephFS snapshot mirror of one directory. On the source network, the source OSD drops every packet it sends to the cephfs-mirror daemon's address (`ApplyNetworkProfile` with that `Peer` and `Loss` 100), and only those; the source MON and MDS still reach the daemon. The source client writes a new 51200-byte file, snapshots the directory and reads the snapshot back through the same OSD and MDS. The daemon sees the new snapshot through the MDS, and for 30 s its own peer status (admin socket `fs mirror peer status`) stays `syncing` that snapshot with the previous one as the last synced and the same `snaps_synced`. The destination has no such snapshot. The MGR mirroring module's `dirmap` of the directory is recorded, not asserted: it left `mapped` before the 30 s ended, for `stalled` in most runs and `shuffling` in one. netem counts the dropped packets. After `Restore`, the snapshot arrives with the new bytes, the daemon reports it synced with exactly the 51200 new bytes copied, and the MGR maps the directory again. The test logs `CEPHFS_MIRROR_PATH_RECEIPT`. | Native state (the daemon's snapshot progress, the MGR's directory map, netem counters) | `TestCephFSMirrorPeerPathCut` (bridge) |
| MDS to client after an MDS restart, a standby takeover or a live export | See [MDS sessions](#mds-sessions). | Native state (session identity) | `TestCephFSRetainedClientAcrossMDSRestart`, `TestCephFSRetainedClientAcrossStandbyTakeover`, `TestCephFSRetainedClientAcrossRankOneTakeover`, `TestCephFSLiveRankMigration` |
| MDS to OSD, one direction, public network | One active MDS and one or two OSDs. `ApplyNetworkProfile` on the MDS's container with `Peer` set to the OSD's public address and `Loss: 100` drops only the MDS's packets to the OSD. Clients' data writes and data-only syncs reach the OSD and RADOS holds the bytes, while the MDS's `objecter_requests` holds its journal writes (`200.*`) to that OSD for at least 5 s and a `mkdir` without an early reply and a full fsync's cap flush wait for them. After `Restore` both return 0 and other clients read the result. With two OSDs, one delay with `Peers` cuts the MDS's paths to both. See [MDS sessions](#mds-sessions). | Native state (both Objecters' `objecter_requests`, the MDS's request dumps and `dump inode`, the clients' `mds_requests` and `dump_cache`, netem counters, the RADOS object) | `TestCephFSMDSOSDPathCut`, `TestCephFSMDSOSDsPathCut` |
| Client to MDS and MDS to client, one direction | One active MDS, `session_timeout` 30 s. A native libcephfs client in its own container holds a file open after an fsynced write. The MDS's v2 address comes from `fs dump` and the client's from the MDS's `session ls`, and each matches its container's endpoint. `ApplyNetworkProfile` with `Loss: 100` drops either the MDS's packets to the client (on the MDS, `Peer` the client) or the client's packets to the MDS (on the client, `Peer` the MDS). In both directions the client's `mkdir` of a new name stays in its admin socket's `mds_requests` and does not return, while an append through the open descriptor and a data-only `fsync` return within a second and RADOS holds the new bytes in the file's first object. For 50 seconds, past the session timeout, `session ls` keeps the session `open` and so does the client: with `mds_defer_session_stale` the MDS does not mark a session stale while nobody waits on its caps. The client keeps renewing (`cap_renew_seq` moves on), but `cap_ttl`, which only the MDS's renew reply sets, stays put. With the MDS's packets cut, the MDS's `dump_historic_ops` has the `mkdir` journaled and replied, so the directory exists before the client hears of it; with the client's packets cut, the MDS never sees the request. After `Restore` the `mkdir` returns, between one second and about a minute later because TCP retransmits the held segments on its backed-off timer, the client holds no request, the session is `open` with the same GID and the client is not blocklisted, and a client in another container reads the bytes and the new directory. The test logs `CEPHFS_MDS_PATH_RECEIPT`. | Native state (the client's `mds_requests` and `mds_sessions`, the MDS's `session ls` and request dumps, netem counters, the RADOS object) | `TestCephFSClientMDSPathCut/mds-to-client`, `/client-to-mds` |

A short log for the backfill case comes from `override.SetConfig` on
`osd_min_pg_log_entries`, `osd_max_pg_log_entries` and `osd_pg_log_trim_min`;
with the default `osd_pg_log_trim_min` of 100, a log of a few dozen entries is
not trimmed at all.

A fault on one peer and in one direction uses `ApplyNetworkProfile` with a
`Peer`, and `Loss` 100 for a blackhole, as the two mirror rows do;
`InterruptNetwork` cuts a whole endpoint.

## Negotiated compression and features

On-wire compression is negotiated per session, after authentication, and
only between peers whose policy names each other: `ms_osd_compress_mode`
covers sessions with OSDs, and a secure session is compressed only with
`ms_compress_secure`. Setting the options is therefore no evidence that a
session is compressed. `TestMessengerCompressionReceipt` starts two OSDs with
a two-replica pool under `MessengerV2Only`, changes the OSD policy with
`override.SetConfig` and restarts `osd.1`, so that every session between the
OSDs is negotiated again, and reads each session back:

| Policy | Sessions between the OSDs | Other sessions |
| --- | --- | --- |
| none (the default) | CRC, uncompressed | uncompressed |
| `ms_osd_compress_mode=force`, `ms_osd_compression_algorithm=snappy` | CRC, `snappy` | uncompressed, also the session of a librados client that sets `ms_osd_compress_mode=force` itself: the OSD does not compress client sessions |
| also `ms_cluster_mode=secure` | secure (AES-128-GCM), uncompressed although compression is forced | uncompressed |
| also `ms_compress_secure=true` | secure, `snappy` | uncompressed |

```golang
force, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "osd", Name: "ms_osd_compress_mode", Value: "force"})
// Existing sessions keep what they negotiated; restart an OSD, or wait for
// new sessions, before reading the result.
out, err := osd.AdminSocket(ctx, "messenger", "dump", "cluster") // osd is the *ceph.OSDContainer of osd.0
```

On Tentacle, `messenger dump <messenger>` on a daemon's admin socket names
each session's `con_mode`, `crypto` and `compression` (`rx` and `tx`, such as
`snappy` or `UNCOMPRESSED`), and `rev1` for msgr2.1. Squid has no
`messenger dump`; with `debug_ms = 1` (for example in a `WithConfigFile`
`[osd]` section) each daemon logs a session's `s=READY` line with its mode
and its `crypto` and `comp` handlers as pointers, `0` meaning none. The
accepting side of a lossless session reports the mode as `unknown` in both;
its cipher still tells secure from CRC.

The same test reads the MON's `sessions` (`ceph tell mon.a sessions`), whose
`con_features` are the features each daemon and client negotiated on its MON
session. It checks names such as `SERVER_SQUID`, `MSG_ADDR2` and `CEPHX_V2`
with their incarnation, as `HAVE_FEATURE` does: several names share one bit,
for example five share bit 21 of incarnation 2, and an older incarnation of a
bit means something else. The test logs `MESSENGER_COMPRESSION_RECEIPT`.

## Targeted commands

A command can go to one OSD (`ceph tell osd.N`, `rados_osd_command`), to the
primary of one PG (`ceph pg <pgid> query`, `rados_pg_command`) or to one MDS
(`ceph tell mds.<name>`). `TestTargetedCommandRecipe` shows how to tell from
the reply which daemon answered and how each failure reaches the caller:

| Case | What the native client reports |
| --- | --- |
| `status` to `osd.N` | `whoami`, `osd_fsid` and `cluster_fsid` equal the OSD id, its UUID in `osd dump` and the cluster's FSID; `newest_map` is the OSD's OSDMap epoch |
| `query` to a PG | `info.pgid` and `acting` equal what `osd map <pool> <object>` reports; `epoch` is the PG's map epoch |
| `status` to `mds.<name>` | `whoami`, `id`, `fs_name` and `state` equal the rank, GID and filesystem that `fs dump` lists for the daemon; `mdsmap_epoch` and `osdmap_epoch` are the MDS's epochs |
| an identity without `allow *` on OSDs or MDSs | the daemon refuses: librados returns `-13`; the CLI, which first asks the daemon for its command descriptions, exits 13 with `Error EACCES: problem getting command descriptions` |
| an OSD id or a pool missing from the client's OSDMap | librados refuses before sending anything: `-2` (ENOENT) |
| an OSD that the OSDMap marks down | librados refuses before sending anything: `-6` (ENXIO) |
| a paused OSD that the OSDMap keeps up (`override.SetOSDFlag(ceph.OSDFlagNoDown)`, `fault.Pause`) | the command is sent and not answered; the client's own timeout ends it with `-110` (ETIMEDOUT), so whether it ran is unknown |
| an unknown command prefix | the OSD refuses it: `-22` (EINVAL), with the status `unknown command prefix` |

librados reports the signed result of the reply and the CLI exits with its
absolute value; neither shows the message itself. The Python binding has
`osd_command` and `pg_command`; for an MDS use the CLI or
`cephfs.LibCephFS.mds_command`. The test logs `TARGETED_COMMAND_RECEIPT`.

### Binary input and output buffers

Every command message carries the caller's input buffer in its data segment
and every reply carries the output buffer in its own. The same test sends
commands with binary buffers through python-rados and logs
`TARGETED_BINARY_IO_RECEIPT`. To see the messages it sets `debug_ms = 1`
on the client (`log_to_file = true` and `log_file`, as the image does not
log to a file by default) and, at runtime with `ceph tell <daemon> config
set debug_ms 1`, on the OSDs and MONs, and sets each back afterwards. The
receiving side of every message logs one dispatch line:

```
-- 172.18.0.2:0/83799995 <== mon.0 v2:172.18.0.2:3300/0 7 ==== mon_command_ack([{"prefix": "config-key get", "key": "tc/binary-io/4275"}]=0  v6) ==== 90+0+65536 (secure 0 0 0) 0x7515d8002430 con 0x561a1b6a7b90
```

The three numbers are the front, middle and data segment lengths. The test
pairs each reply with the call that librados returned, per channel in send
order, and requires the result and the output buffer's length to match the
reply's data segment. On the daemon side it finds each input buffer as a
data segment of the same length from the client's address. Use the address
and its nonce to recognise the client: a MON logs messages on a client's MON
session as from `client.?`, while an OSD names `client.<global id>`. A status
that ends in a newline makes the line span two log lines.

| Channel | What the receipt holds |
| --- | --- |
| `osd_command` to `osd.0` (MCommand) | `status` with 1 MiB holding every byte value, received by the OSD as a 1048576-byte data segment; the OSD ignores it and answers with its identity. `config show` returns about 77 KiB |
| `mon_command` on the MON session (MMonCommand) | `config-key set` of exactly `mon_config_key_max_entry_size` (65536) arbitrary bytes, then `config-key get` returning the same SHA-256; one byte more is refused with `-27` (EFBIG) after the MON received it all; `config-key rm` removes the key |
| | `osd getcrushmap` and `osd getmap`, decoded with `crushtool -d` and `osdmaptool --print`; the CRUSH map that `osdmaptool --export-crush` takes from the OSDMap decompiles to the same text. The status of `osd getcrushmap` is the CRUSH version, that of `osd getmap` `got osdmap epoch N` |
| | `osd setcrushmap` with a map that `crushtool -c` compiled with one more rule: the CRUSH version and the OSDMap epoch go up by one and the rule is listed. The same bytes with a broken magic are refused with `-22` (`Failed to parse crushmap: bad magic number`). The original bytes, set back with `prior_version`, give version +2; sent again with the same `prior_version` they are acknowledged without a new version, and `osd getcrushmap` returns the original SHA-256 |
| `mgr_command` on the MGR session (MMgrCommand) | the crash module's `crash post` with a 96 KiB report written as `json.dumps(indent=4, sort_keys=True)`; `crash info` returns those bytes followed by one newline. A report that is not UTF-8 is refused with `-22` (`UnicodeDecodeError`). `crash rm` removes the report, which is dated 30 days back so that it raises no `RECENT_CRASH` |

The MGR replies come from `mgr.<active gid>` and the MON replies from the
rank of the client's MON session, which the receipt names.

Cancellation uses the paused OSD of the table above. One client sends
`status` with 4 KiB to `osd.1`; the test pauses the OSD under `nodown`; the
next `status`, with 256 KiB, ends in `-110` at the client's 5 s
`rados_osd_op_timeout`, and with `debug_objecter = 10` the Objecter logs
`command_op_cancel tid N`. After the resume the OSD receives tid N with all
262144 bytes and answers it; the client logs the late `command_reply(tid N)`
and drops it (`handle_command_reply tid N not found`). The next command on
the same session returns 0.

These limits hold for the official image and are part of the receipt:

- A command targeted at a named MON (`mon_command` with `target`) or MGR
  (`mgr_command` with `target`) is an MCommand that reaches only that
  daemon's admin socket commands. `osd getcrushmap` and `crash ls` are
  refused there with `-22` (`unknown command prefix`).
- No admin socket command of an OSD, MON or MGR returns binary output, and
  none reads the input buffer except the OSD's `rotate-stored-key`, which
  replaces the OSD's stored key and is not used here. A binary round trip
  byte for byte therefore runs over the MON session. An OSD still receives
  the whole input buffer.
- A MGR module receives its input as UTF-8 text and returns text.

## RBD and CephFS object layout

A client that routes RBD or CephFS I/O to RADOS objects can compare its
layout logic with two receipts. Each writes through the native library, then
reads the metadata and every data object back with clients of its own:
`rados`, and librados in a separate Python process.

`TestRBDLayoutReceipt` uses the `rbd` package's pool with a namespace from
`rbd.CreateNamespace`. librbd creates a 16 MiB image with 4 MiB objects
(order 22) striped in 1 MiB units over two objects (`STRIPINGV2`), writes
8 MiB at offset 0 and 1.5 MiB at 8.5 MiB, snapshots and protects it, and
overwrites 8 KiB of its first object. A clone of the snapshot with the same
layout gets one 4 KiB write. Its receipt, `RBD_LAYOUT_RECEIPT`, holds:

- each image's id (`rbd_id.<name>`) and its header's `object_prefix`,
  `order`, `size`, `features`, `stripe_unit`, `stripe_count`, `snap_seq` and
  `snapshot_<id>` keys, decoded from the `rbd_header.<id>` omap and equal to
  what librbd reports;
- the clone's `parent` omap value as hex and decoded (pool id, namespace,
  image id, snapshot id, overlap), equal to librbd's parent spec;
- every data object `rbd_data.<id>.<objno as %016x>` that `rados ls`
  lists, which are exactly those the striping of the writes names, with
  their size and sha256 read by `rados get`. The clone has only the object
  it wrote, copied up whole from the snapshot;
- the clone that `rados listsnaps` reports for the snapshot, only on the
  overwritten object, and that object's bytes read at the snapshot id,
  which are those from before the overwrite;
- each object's PG, raw PG, up and acting sets and the OSD map epoch from
  `ceph osd map <pool> <object> <namespace>`.

`TestCephFSLayoutReceipt` gives an empty file the same striping and a pool
namespace through the `ceph.file.layout.*` xattrs, writes the same two ranges
through libcephfs, snapshots its directory with `mkdir .snap/base` and
overwrites 8 KiB of the first object. Its receipt,
`CEPHFS_LAYOUT_RECEIPT`, holds the inode, the `ceph.file.layout` value, the
snapshot id from `snap_info`, and the same object, clone, snapshot read and
placement records for the data objects `<ino as hex>.<objno as %08x>`.

Both use one object-to-extent formula: the byte at offset `off` is in stripe
unit `b = off / su`, stripe `s = b / sc`, object `(s / (os / su)) * sc + b % sc`
at `(s % (os / su)) * su + off % su`. An object part below a write that
nothing wrote reads as zeros, and an object ends at its last written byte.

## Watch/notify and MON subscriptions

`TestNativeWatchNotifyAndSubscriptions` runs python-rados clients in the
client container against one OSD and logs three receipts. Use them as the
native answers for a client's watch/notify, MonClient subscription and map
delivery logic.

`RADOS_WATCH_NOTIFY_RECEIPT` comes from a notifier and three watchers, each
its own client:

- the client id and cookie of every watch, which `rados listwatchers` lists
  the same way;
- for two notifies, the notify id, notifier id, cookie and payload that each
  watcher's callback saw, and the acks the notify returned as
  `[watcher client id, cookie, payload hex]`. One watcher acks with
  `w1:<notify id>`, the other with an empty payload. The notify ids differ;
- a third watcher that never acks. A python-rados notify then returns only
  `-110` (the binding decodes acks and timeouts only for a notify that
  returned 0), so a `rados notify` process lists the two acks with their
  byte counts and the silent watcher as `timeout client.<id> cookie <c>`.
  The silent watcher keeps its watch;
- after `ceph osd blocklist add` of one watcher's address, its error
  callback with its own cookie and error (`-107` on Squid and Tentacle), and
  the watch listing without it;
- after the object is removed, the error callback (`-107`) of each remaining
  watcher.

python-rados takes a notify payload as `str` and sends `len(str)` bytes of
its UTF-8 encoding, so the recipe keeps the payloads ASCII. A `rados notify`
payload arrives length-prefixed (`02000000 6e34` for `n4`).

`MON_SUBSCRIPTION_RECEIPT` comes from one client with an admin socket and a
messenger log (`debug_ms=1`, `debug_monc=10`, `debug_mgrc=4`,
`debug_objecter=3`) that holds a watch and subscribes to the cluster log:

- the subscriptions its `mon_subscribe` messages carried, as
  `name: [start(+)]` where `+` marks a continuous subscription: `monmap`,
  `mgrmap`, `config` and `log-info` are continuous; `osdmap` is onetime, for
  the next epoch, renewed on the Objecter tick while the watch exists;
- the OSD map epochs before and after `ceph osd pool set-quota`, the
  `osd_map` messages it received with their sender and epoch range, the
  epochs its Objecter decoded, its Objecter `map_epoch` and `map_inc`
  counters, and the `osd_epoch` that the MON's `sessions` dump reports for
  its global id. The counters start with the first map after the one the
  client connected with, so the starting epoch is read from the log;
- the key counts of the `config` messages it received, including those for
  `ceph config set client debug_rados 3/7` and the following `config rm`,
  with the value `conf_get` returned after each;
- the `log` messages it received and the marker of a `ceph log` entry that
  reached its callback.

`MAP_EPOCH_DELIVERY_RECEIPT` follows MonMap and MgrMap epoch changes from the
MON command that commits them to the same client. `via` is the sender
(`mon.<rank>`) of the MON that holds the client's session, as the MON
`sessions` dumps report it before and after both changes:

- `monmap`: the epoch before and after `ceph mon set_location <mon>
  datacenter=<location>` (one epoch apart), the `mon_map` messages the client
  then received (all from `via`; the message names no epoch), and the MonMap
  its MonClient decoded and dumped at the new epoch (`epoch N` and the
  `mon.<name>; crush_location {datacenter=<location>}` line), next to the
  epochs it decoded since connecting;
- `mgrmap`: the epoch and active MGR (name, GID, address vector as
  `type:addr/nonce`) before `ceph mgr fail <active>`, the epoch and active GID
  that `ceph mgr dump` reports right after it, and the epoch and active MGR
  once an MGR under a new GID is active and available and the client has
  caught up (the failed MGR respawns; the test's single MGR becomes active
  again). `mgrmap_messages` are the `mgrmap(e N)` messages the client received as
  `[sender, epoch]`, including both epochs above; `client_versions` pairs each
  epoch its MgrClient took with the active address vector it logged (empty
  while no MGR is active); `client_session` is the MgrClient's new session
  with the replacement MGR.

The MON commands are read from the MONs' own command list (`ceph mon -h`,
`ceph mgr -h`) before anything changes. The MGR failover restarts the MGR, so
expect the MgrMap to move several epochs between the two reported ones.

## OSD backoff

`TestOSDBackoffReceipt` makes a primary send a PG-wide backoff
(`CEPH_MSG_OSD_BACKOFF`) to a native client whose connection negotiated
`CEPH_FEATURE_RADOS_BACKOFF`, and logs `OSD_BACKOFF_RECEIPT`. In both
supported releases `PrimaryLogPG::do_request` backs off a whole PG that is
peered but not active, so the recipe needs no optional backoff or peering
debug switch:

1. A one-PG pool on three OSDs with three replicas and `min_size` 3 holds one
   object of known bytes. The test waits for `active+clean` and records the
   PG, acting set and primary.
2. The primary's `debug_ms` is raised to 1 through a configuration override.
   A retained python-rados client with `debug_ms=1` and `debug_objecter=10`
   in its own log file stats the object, which opens its session with the
   primary.
3. With `noout` held, a non-primary replica stops. The PG must become peered
   without `active`, without that replica and under the same primary; a PG
   that never gets there fails the setup. `min_size` is never lowered.
4. The client submits one asynchronous read. Before the replica starts again,
   the client's log must show a BLOCK of the PG from the primary after the
   read, and the primary's log the client's ACK_BLOCK on the connection that
   carried the read. The read must still be pending and the PG still held.
5. The replica starts. The client must receive the UNBLOCK, the primary must
   answer the original read only after it, with result 0, and the read must
   return the seeded length and SHA-256. The PG must be clean again under the
   same primary, and `noout` and `debug_ms` are restored with bounded
   contexts of their own.

The chain is joined by the client's address and entity, the primary, the PG,
the backoff ID and the range. Backoff IDs count per OSD session, so the ID
alone joins nothing, and an ACK_BLOCK from an earlier connection of the same
client is ignored. ACK_BLOCK must carry the BLOCK's epoch; UNBLOCK carries
the epoch of its release, which may be later. A connection is told apart by
the sender's address, its message sequence number and the Objecter's
`ms_handle_reset`; a reconnect between BLOCK and UNBLOCK leaves the chain
unjoined. Other BLOCKs and reconnects stay in the receipt as `extra_blocks`
and `reconnects`. The receipt also keeps the placement before, during and
after the hold with its OSD map epochs, the read's duration, the OSD and
client image IDs, the release, the platform, the CI run and attempt, and the
cleanup outcomes. `OSD_BACKOFF_RAW` lines hold the joined log lines.

A PG backoff can also be released on an interval change, so the UNBLOCK alone
proves neither recovery nor a completed read; the recipe requires the read's
own result. It does not cover object-scoped backoffs (a degraded or unfound
object), and a pull request runs it on Tentacle only. The log parsing is
tested without Docker in `messenger_backoff_fixture_test.go`.

## Coverage manifest

[`internal/integration/testdata/messenger_coverage.json`](../../internal/integration/testdata/messenger_coverage.json)
lists the environment requirements of the ceph-msgr-go SPEC (a pinned
commit), each with the Ceph symbols it concerns, its status (`covered`,
`partial` or `uncovered`), the tests and receipt markers that prove it, and,
when it is not covered, the gap and, if one tracks it, the issue that owns it. The symbols come
from [`messenger_reference.json`](../../internal/integration/testdata/messenger_reference.json),
the OSD operations, message types and feature bits that the pinned Ceph
headers declare, with their source lines; feature aliases of one bit and
incarnation stay separate entries.

`TestMessengerCoverageManifest` checks the manifest without Docker: every
named test exists, every symbol is in the reference, a covered or partial row
has a case and an uncovered one none, and every test that the daemon-to-daemon
table above cites is mapped. It parses the package's sources to bind each
case to its test. A cited `TestName/subtest` path must be one that the test
runs, through `t.Run` in the test, in a helper it calls or in another
subtest. The validator follows a subtest name when it is a string literal,
a field or element of a literal table in the same function, or a
`networkModes` mode; a citation below a name it cannot follow fails, so such
a case cites the top-level test. A receipt marker must be in code that the
cited test reaches, not only somewhere in the package. It logs the partial and uncovered rows and the reference
symbols no row maps. The manifest records whether a recipe exists, not
whether its last run passed. Regenerate the reference from a Ceph checkout
at the pinned commit with:

```sh
python3 .github/scripts/messenger_reference.py generate --source ~/ceph \
  --commit 7f793731f1b39eb4f465e960113d2363c311b964 --release 20.2.4 \
  --output internal/integration/testdata/messenger_reference.json
```

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
  `MessengerV2Only`, `override.SetConfig` can change them after bootstrap.
- **Ticket lifetimes.** They are fixed at bootstrap; `override.SetConfig` cannot
  change them later.
- **Short MON tickets.** With MON tickets under ten seconds, a MON client
  that renews late is refused with `EACCES` and does not authenticate again;
  see [WithAuth](../../ceph/README.md#withauth). This is the behaviour of Ceph's MON
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

The [declaration/native-case evidence join](../design/messenger-evidence.md)
connects a bounded reference family to validated post-cleanup receipts without
promoting fixture availability into wire coverage or consumer implementation.
