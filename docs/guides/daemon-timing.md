# Fixture daemon timing

Timing profiles prepare disposable fixture state. They shorten selected daemon
waits whose native behavior has been checked; they do not accelerate clocks,
physical I/O, authentication expiry or failure detection. Every profile is
opt-in and the ordinary fixture keeps its normal timing.

| Scenario | Configuration | Scope |
| --- | --- | --- |
| Repeated directory-pin handoffs | `fs.OverrideTiming(ctx, cephfs.TimingProfile{DirectoryPins: true})` | Current owned active MDSs; restore the returned configuration handles |
| Incremental RGW multisite sync | `multisite.Config{Timing: multisite.TimingProfile{IncrementalSync: true}}` through `WithConfig` | Gateways started by this fixture, including its added zones |
| RBD journal replay polling | `rbdmirror.Config{Timing: rbdmirror.TimingProfile{JournalPolling: true}}` through `WithConfig` | Journal receiver daemons started by this mirror |

The startup profiles apply native arguments only to their owned daemons. They
outlive individual client requests and end when those daemons terminate; they
write no persistent central configuration entry. Their command-line settings
take precedence over local files and central configuration. Do not replace the
fixture's required command, entrypoint or internal environment with a Docker
customizer. A partial fixture returned with an error still needs termination.

## Directory pins

The CephFS profile requires two or more active ranks owned by the original
filesystem, native MDS releases 19.2.5 or 20.2.4, and an effective five-second
`mds_tick_interval`. It sets one second on each current active MDS and verifies
the effective readback. It uses existing `override.ConfigHandle`s: retain every
returned handle even after an error and restore them in reverse order. Future
scaled or replaced MDSs are outside that returned scope.

An already queued tick keeps its previous deadline. The first handoff may
therefore still take five seconds. Application and restoration have a cost;
short pin sequences may not save time. The interval also drives other MDS
upkeep, so restore before testing expiry, reconnection, failover or a required
fault-overlap window. See [CephFS](../../cephfs/README.md#directory-pin-timing).

## Incremental multisite sync

Use the RGW profile on qualified 20.2.4 gateways when the fixture needs repeated
incremental metadata or data catch-up. It sets `rgw_meta_sync_poll_interval` and
`rgw_data_sync_poll_interval` to one second. The default caught-up polling
interval is twenty seconds. Native notifications can wake these loops earlier;
the profile does not guarantee a fixed saving for every operation.

Bootstrap and the first object of a new bucket include work that is different
from caught-up incremental polling. Keep the native metadata-ready barrier
before relying on object replication. The profile does not shorten the sync
lease period, retries, failure backoff or readiness timeouts, and it does not
make a readiness observation prove object bytes arrived. Verify payloads using
the client under test. See [RGW multisite](../../rgw/multisite/README.md).

## Journal replay

Use the RBD profile on qualified 20.2.4 journal receivers. It sets
`rbd_mirror_journal_poll_age` to one second before replay instances start. A
journal player captures its watch interval: changing a running daemon's central
setting and reading it back does not prove an existing player changed its wait.
Ending the owned daemon also ends its captured replay configuration.

Snapshot replay is notification-driven and is outside this profile. Pool peer
refresh and image state-check intervals govern discovery or retries, rather
than every incremental write. Replay delay, snapshots on a schedule, leader
heartbeats and reconnect/failure observations remain their original scenario
conditions. Check actual destination bytes after readiness. See
[RBD mirror](../../rbd/mirror/README.md).

## Delays that define the assertion

The clone worker's `mgr/volumes/snapshot_clone_delay` defaults to zero. An
injected delay is useful when a test needs a pending clone long enough to
observe source protection, cancellation, duplicate-target refusal or a native
copy failure. It is a MGR-module setting shared across filesystems. A running
worker captures the delay; restoring the central setting does not shorten its
already-started sleep. Use existing `override.SetConfig` handles only in an
explicitly owned clone scenario, and preserve its full observation/setup
window. There is no default clone wait to remove and no filesystem-scoped
acceleration profile for this global setting.

Authentication ticket lifetimes and renewal windows are also part of the
assertion. `ceph.WithAuth(ceph.AuthConfig{...})` already groups startup MON and
service TTLs. This fixture writes them into local configuration, which takes
precedence over a central override. Already issued tickets retain their encoded
validity; a server setting cannot retroactively shorten their expiry. Preserve
observation across the required number of lifetimes and outages beyond expiry.
See [Messenger client fixtures](messenger-client-fixtures.md).

## Rechecking a profile

The opt-in native tests keep the image, topology, data, concurrency and success
polling fixed. They report phase and complete scenario wall time, actual daemon
settings or worker consumption receipts, byte-level outcomes and cleanup:

- `TestCephFSPinTimingQualification`: balanced ABBA baseline/profile order and
  actual native subtree authority with fresh libcephfs reads.
- `TestRGWIncrementalPollTimingQualification`: bootstrap/new-bucket work separated from
  caught-up incremental metadata and data sync.
- `TestRBDJournalPollingTiming`: fresh journal replay instances for each selected
  poll age, initial enrollment separated from incremental replay.
- `TestCephFSCloneTimingQualification`: deliberately injected pending windows,
  captured worker delay, cancellation, native collision failure and source data.

Run the selected test with `-tags=all,native_regression` on Linux. These are
qualification scenarios rather than the default CI workload. A configured value
or a successful compile alone is not native timing evidence. A faster success
poll, weaker assertion or shortened fault window is not daemon-wait removal.
