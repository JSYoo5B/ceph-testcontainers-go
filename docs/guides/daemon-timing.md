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

For mClock startup I/O and allocation policy, see
[OSD startup calibration](osd-startup-benchmark.md).

## Profile details

Each package documents its profile beside the operations it shortens: the
qualified releases and native settings, what the profile leaves at native
timing, how its condition ends and what a test must still check. See
[CephFS directory-pin timing](../../cephfs/README.md#directory-pin-timing),
[RGW incremental sync timing](../../rgw/multisite/README.md#incremental-sync-timing)
and [RBD journal polling timing](../../rbd/mirror/README.md#journal-polling-timing).

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
