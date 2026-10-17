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
| CRUSH and pool | legacy tunables, a straw root, a rule that chooses OSDs; a pool with one PG and size 1; no pool with `hashpspool`, the MGR's `.mgr` included | compile the map with `crushtool` and install it with `osd setcrushmap` (see [Reproduce a minimal legacy placement setup](../ceph.md#reproduce-a-minimal-legacy-placement-setup)); `osd_pool_default_flag_hashpspool = false` in a `WithConfigFile` `[global]` section; `CreatePool(ceph.PoolConfig{PGNum: 1, Replicas: 1, MinSize: 1, NoHashPSPool: true})` | `TestMessengerClientProfileCRC`, `TestOSDMemStoreLegacyPlacement`, `TestRADOSMutationContracts`, which check the flags of every pool |
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

The MON requires the `OSDHASHPSPOOL` feature (`0x40000000`) of every client
while any pool has `hashpspool`, so a client without it is refused when it
opens its MON session. `NoHashPSPool` clears the flag only on the pool that
`CreatePool` creates. Ceph creates the MGR's `.mgr` pool on its own, with the
MON's default flags, so the default has to be off before that:
`osd_pool_default_flag_hashpspool = false` in a `WithConfigFile` file does
that at bootstrap. Check every pool's flags with `cluster.Pools`, not only
the pool under test.

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
| Fail over the active MGR | `cluster.Ceph(ctx, "mgr", "fail", name)`. The failed MGR restarts its process and registers as a standby again, about 1.5 s later on 20.2.4; wait until `ManagerStatus` lists it before the next control, such as a monmap change. A daemon that must authenticate again after the MONs have rotated away the secret of its ticket, for example after an election that outlasted the ticket lifetime, is refused with `EACCES` and can stay unregistered while its process runs (see the pause row); `Stop` and `Start` on its `Managers()` handle bring it back. See [MGR MonClients](#mgr-monclients) before treating a refusal as that case. | `TestMessengerClientProfileSecure`, which waits for the standby before its next control |
| Tell which MON session belongs to a registered MGR | the owned MONs' `sessions` admin socket command, through `cluster.AdminSocket(ctx, "mon.<name>", "sessions")`; see [MGR MonClients](#mgr-monclients) | `TestMessengerClientProfileManagerMonClients` |
| Stop one MGR and start it again | `Managers()`, then `Stop` and `Start` on the handle. `Start` waits only for the admin socket; see [MGR transition receipt](#mgr-transition-receipt) for when the MGR is registered again. | `TestMessengerClientProfileSecure`, `TestMessengerClientProfileManagerTransition` |
| Restart one MON | `Monitors()`, then `Stop` and `Start` | `TestMessengerClientProfileSecure` |
| Stop every MON and start them again | `Stop` each MON; start `b` and `c` before `a`. MON a's container checks `ceph status` when it starts, which needs a quorum. Each MON comes back at its monmap address. | `TestMessengerClientProfileSecure`, with the MONs down longer than a MON ticket lives |
| Stop MON a for good | `Stop` on its handle. With three MONs, the fixture's own CLI runs in a separate control container and keeps working. | `TestMessengerClientProfileSecure` |
| Start MGRs later | `AddManager` | `TestMessengerClientProfileDelayedManagers` |
| Add a MON that never answers, then remove it | `cluster.Ceph(ctx, "mon", "add", name, addr)` with an address nothing listens on, later `mon remove`. Each monmap change starts an election, and with a member missing the MONs wait for its election timeout: MON commands and new client sessions waited 6 to 11 s after the add. On Tentacle 20.2.4 the remove also took three election rounds and about 7 s; on Squid 19.2.5 one short round. Wait until `quorum_status` reports the new monmap and the same election epoch twice before starting new native clients, and give them more than that time. While the member is in the monmap, `RefreshClientMonitorConfig` would write its address into a client's `mon_host`. | `TestMessengerClientProfileUnreachableMonitor` |
| Make the MGRs accept only secure, then restart them | `TemporaryMessengerPolicy(ctx, ceph.MessengerPolicy{Managers: secure})`, then `Stop` and `Start` each MGR. The MGRs keep the mode across the restart; `Restore` returns them to CRC. | `TestMessengerClientProfileModeRejection/managers` |
| Revoke a client key and restore it | the suite's own client, or `TemporaryClientRevocation` and `ReplaceClientKey` | `TestClientRevocation`, `TestClientKeyReplacement` |
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
2. Promotion: `Stop` on the active's `Managers()` handle. The phase waits
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
| `monitors` | `AdminSocket(ctx, "mon.<name>", "mon_status")` for each expected MON | each MON's own rank, state, election epoch, quorum, MonMap epoch, `auth_epoch` (Tentacle; absent on Squid) and FSID, without a MON session |
| `quorum_names` | `QuorumStatus` | the quorum as a MON session reports it, a different observation from each MON's own state |
| `managers`, `managers_after` | `mgr dump`, `ManagersContext` and each MGR container's start time, and every expected MON's `sessions` | each MGR's GID and container instance, before and after the collection, and its MON sessions split as in [MGR MonClients](#mgr-monclients): the beacon session's global ID and the module clients' |
| `native_controls` | `RunNativeCommand` of `ceph tell mgr.<name> version` for the tested target and the active MGR | behavioral controls, with their phase: a new client authenticated with its own ticket |
| `queries` | every query above | name, kind, start and end, and a status: `complete`, `unsupported`, `timed_out`, `truncated`, `failed` or `unavailable`, with a redacted error and output excerpt of at most 1 KiB otherwise |
| `first_failure`, `held_session`, `consumer_tickets`, `provenance`, `expected_target` | your records | stored as given |
| `generation_equality` | | always `unavailable/unverified` |

The collection has one total budget, here a minute, and a bound per query.
It runs from a context of its own, so a test context that ended with the
failure does not stop it, and it keeps going after a query fails. Every wait
ends with the budget: the MONs come from your frozen list, the MGR
containers from `ManagersContext`, and errors are redacted within what is
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
equality needs a native observer outside the stock images; see
[#71](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/71). For the
cause of four analyzed failures in #71, see
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

The consumer in
[#71](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/71) found
this window as the common cause of four analyzed auth-epoch failures, at
consumer commits `b01a3cf`, `37d7a4d`, `494071b` and `af2c4e9`
([root cause](https://github.com/JSYoo5B/ceph-msgr-go/blob/96e7531fff9827723a7e06c2d13a323c7a56584f/integration/fixture/SERVICE_KEY_EPOCH_FAILURE_20261015.md#root-cause-ci-triage-2026-10-16)),
and fixed those in its client
([JSYoo5B/ceph-msgr-go@3d2645e](https://github.com/JSYoo5B/ceph-msgr-go/commit/3d2645e)).
Earlier rejections reported in #71, such as the one at consumer `2d1cc46`,
were not part of that analysis. They keep their original status: their
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
| A client is frozen with `PauseContainer` while the only active MDS is killed and a standby takes rank 0, with `mds_reconnect_timeout` set to 10 s through `TemporaryConfig` | the FSMap shows the standby in `up:reconnect` for the window (about 15 s), then `up:rejoin` and `up:active`; the frozen client's session is gone and its address is in the blocklist; after `Resume`, its open descriptor fails, with `ESHUTDOWN` (108) on Tentacle and `EPERM` (1) on Squid, and it shuts down cleanly; a fresh client reads the bytes the frozen one had fsynced | `TestCephFSLateReconnect`, with `MessengerV2Secure` |
| Two active ranks; the client appends 16-byte records through one descriptor to a file in a directory pinned to rank 0, fsyncing each before the next; after a few records, another client sets the directory's `ceph.dir.pin` to 1, and after the stream to 0 again | rank 1 becomes the only rank that reports the directory as an authoritative subtree, in `get subtrees` of both ranks read between two `MDSStatus` reads with unchanged GIDs; rank 0's `exported` and `exported_inodes` and rank 1's `imported` and `imported_inodes` grow; more records are acknowledged under rank 1; the client has sessions on both ranks with one address and PID and is not blocklisted; after the stream, the client reads every record in order through its descriptor, and a client in another container reads the same bytes and xattr; a fresh mount reads the restored pin, and rank 0 is again the only authority before the client closes | `TestCephFSLiveRankMigration`, with `MessengerV2Secure` |
| One active MDS; the client renames a fsynced file within its directory once while `mds_log_pause`, set through `TemporaryConfig`, holds the MDS's journal | the running daemon's own `config get` shows `mds_early_reply` true and the pause applied; the rename call returns with the early reply; twice, 3 s apart, the MDS's `dump_ops_in_flight` holds the client's request (`client.<id>:<tid> rename`) after `early_replied` and without `journal_committed`, and the client's admin socket (`mds_requests`) holds the same TID with `got_unsafe` and the file's inode; after the pause is restored, the client no longer holds it, and the MDS's `dump_historic_ops` shows it `journal_committed` and `replying`; the MDS keeps its GID; a client in another container finds the new name with the original inode and no old name | `TestCephFSUnsafeRequestWindow`, with `MessengerV2Secure` |
| The same held rename; the only MDS is then killed with SIGKILL, so the paused journal entry is lost, and its GID is failed. The pause is restored while no MDS runs, and the MDS container starts again | while no MDS runs, the client's `mds_requests` still holds the TID with `got_unsafe`; rank 0 returns with a new GID in the same container, and the restarted daemon's own `config get` shows `mds_log_pause` false; its `dump_historic_ops` shows the same `client.<id>:<tid> rename` with the `REPLAY` flag (Tentacle 20.2.4 and Squid 19.2.5 both print `RETRY=1 REPLAY QUEUED_FOR_REPLAY`), without `early_replied`, `journal_committed` before `replying`; then the client no longer holds the TID; a client in another container finds the new name with the original inode and no old name. The test issues no second rename | `TestCephFSUnsafeRequestReplay`, with `MessengerV2Secure` |
| One active MDS; the client fsyncs a file, then, while `mds_log_pause` holds the MDS's journal, appends through the same descriptor and fsyncs again; the only MDS is then killed with SIGKILL and its GID failed, the pause is restored while no MDS runs, and the MDS container starts again | twice, 3 s apart, the client's `dump_cache` holds the inode with a flushing `Fw` cap, a flush TID and the full size, the MDS's own `dump inode` has the base size, and the fsync has not returned; with no MDS running the client still holds the flush; rank 0 returns with a new GID in the same container without the pause; then the client holds no flushing cap, the MDS has the full size and the original fsync returns 0, with no second fsync; a client in another container reads every byte | `TestCephFSFlushingCapReplay`, with `MessengerV2Secure` |

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
Two things stay outside it: a crash after the entry reached the journal, where
the MDS replays its own journal rather than the client's request, and dirty
file data, capability flushes and lease recalls that were in flight with the
request.

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
| `mkdir`, then 1500 empty files in one directory; `opendir` and `readdir` | with `mds_bal_split_size` lowered to 1000 through `TemporaryConfig` and read back from the daemon, the MDS splits the directory into 8 fragments (`dirfrag ls`); the listing returns `.`, `..` and every name exactly once, and the MDS's `req_readdir_latency` count in `perf dump mds_server` grows by one request per fragment (8 on Tentacle 20.2.4) |
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
| MON to MON | Cutting two of five MONs off the public network leaves a quorum of three; restoring them brings back five. | Native state (quorum membership). Which daemon received which map is not observed. | `TestFiveMonitorQuorumAndNetworkRecovery` |
| MON to MON and MON to OSD, map propagation | Each daemon is asked for the map it holds on its own admin socket, without a MON session. After `CreatePool` makes a new OSDMap epoch, each OSD's `status` reaches it in `newest_map`. After `AddMonitor("d")` makes MonMap epoch N+1, each of MONs a, b, c and d reports in `mon_status` that epoch, all four members, a quorum of four and the same election epoch; after `RemoveMonitor("d")`, a, b and c report N+2 with three members. The test logs `MAP_PROPAGATION_RECEIPT` with the first read that showed each daemon the new epoch. `AddMonitor` returns once the MON has joined, so those times order the reads and are not a propagation latency. | Native state (each daemon's own map epoch) | `TestMapPropagationReceipt` |
| OSD to OSD when a replica missed writes | One replica of a one-PG, two-replica pool stops while writes continue, then starts again with recovery held by `norecover` or `nobackfill`. The primary's `pg query` tells the two paths apart. In log recovery the replica stays in the acting set, has no backfill target and `last_backfill` `MAX`, and the PG log still reaches back to the replica's last entry. In backfill the PG log has been trimmed past that entry, and the replica is only in the up set, as the backfill target with `last_backfill` `MIN`. While the flag is set, `num_objects_recovered` does not move; afterwards it grows by exactly the objects written while the replica was down. A client of its own reads every object, and with the primary stopped the replica serves them alone. The same receipt runs on a K=2, M=1 erasure-coded pool, where the replica is the OSD of one shard: `pg query` names it like `2(1)`, backfill leaves its acting slot empty (`2147483647`), and with the primary stopped no object can be rebuilt without the recovered shard. The test logs `OSD_RECOVERY_RECEIPT`. | Native state | `TestOSDRecoveryAndBackfillReceipts/replicated` and `/erasure` |
| OSD to OSD after a placement change | With `nobackfill` and `norecover` held, a one-replica pool's PGs map to another OSD and report `recovery_wait` or `backfill_wait` while the objects stay on the old one. After the flags are restored, the PGs are clean on the new OSD and the data reads back. | Native state. Client reads recover objects on demand, so the test does not tell log recovery from backfill. | `TestPoolPGRelocation` |
| MDS to MDS subtree export | An export pin moves a directory's authority to the pinned rank, as `get subtrees` on the owned ranks reports. With `ceph.dir.pin` on a directory that a client has open, rank 0's `exported` and `exported_inodes` perf counters and rank 1's `imported` and `imported_inodes` grow, read with `ceph tell mds.<name> perf dump mds` before and after. In the live migration the client keeps writing during the move, and both ranks must agree on the single authority. | Native state (per-daemon counters) for the pinned directory; outcome for subvolume pins | `TestCephFSRetainedClientAcrossRankOneTakeover`, `TestCephFSLiveRankMigration`, `TestCephFSPins` |
| MGR to MON, registration and failover | The MONs drop the active MGR whose process stopped and promote the captured standby, which keeps its GID and process. The restarted MGR registers again as a standby with a new GID. Each registration has its beacon session on a MON, and the MON quorum and a MGR module command answer in every phase. The test logs `MGR_TRANSITION_RECEIPT`; see [MGR transition receipt](#mgr-transition-receipt). | Native state (MgrMap and MON sessions) | `TestMessengerClientProfileManagerTransition` |
| OSD to OSD heartbeats, cluster network | Three OSDs on separate CRUSH hosts; one OSD's cluster endpoint is cut. Its back-interface heartbeats with both peers fail, and after `osd_heartbeat_grace` (20 s) the MON's cluster log records it `failed` with 2 reporters from different hosts, `mon_osd_min_down_reporters` and `mon_osd_reporter_subtree_level` being 2 and `host`. `osd dump` marks it down while both peers stay up from the same epoch, and a client keeps reading and writing a pool with 3 replicas and `min_size` 2. The marked OSD logs `Monitor daemon marked osd.N down, but it is still running`, cannot rebind its cluster messenger (`rebind cluster_messenger failed`), tells the MONs it is dead as of that epoch or the next map it is handling, and exits. After `Restore` and `Start` it comes up with the same UUID from a later epoch. The test logs `OSD_HEARTBEAT_RECEIPT`. | Native state (MON failure record, OSD map, the OSD's own log) | `TestOSDHeartbeatFailure` |
| RGW to RADOS | A gateway with one OSD. The test reads what the gateway wrote with `rados` and `radosgw-admin`, not through the gateway. A 1031-byte S3 object is one head object `<marker>_<key>` in the bucket data pool with the same bytes, and its key is in the omap of one of the bucket's index shard objects (`.dir.<id>.<shard>`). A 6 MiB object is a 4 MiB head object and a 2 MiB tail stripe `<marker>__shadow_.<prefix>_1`; the tail name has a random upload prefix, not the key. Deleting it removes the head and the index key at once, and the garbage collector lists the tail until `gc process --include-all` removes it. The test logs `RGW_RADOS_RECEIPT`. | Native state (RADOS objects, index omap, GC queue) | `TestRGWRadosReceipt` |
| MDS to client after an MDS restart, a standby takeover or a live export | See [MDS sessions](#mds-sessions). | Native state (session identity) | `TestCephFSRetainedClientAcrossMDSRestart`, `TestCephFSRetainedClientAcrossStandbyTakeover`, `TestCephFSRetainedClientAcrossRankOneTakeover`, `TestCephFSLiveRankMigration` |

A short log for the backfill case comes from `TemporaryConfig` on
`osd_min_pg_log_entries`, `osd_max_pg_log_entries` and `osd_pg_log_trim_min`;
with the default `osd_pg_log_trim_min` of 100, a log of a few dozen entries is
not trimmed at all.

No receipt exists yet for the RBD and CephFS mirror peers with one path
cut. A fault on one peer or in one direction
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
