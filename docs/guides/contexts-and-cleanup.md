# Contexts, ownership and cleanup

## Introduction

A test cluster consists of many containers, networks and Ceph-side objects,
and many operations change several of them in sequence. This guide explains:

- how the `context.Context` you pass bounds each call;
- which resources the cluster owns and removes in `Terminate`, and which you
  own;
- why many calls return a non-nil handle together with an error, and what to
  do with it;
- how override handles are restored;
- how to recover from a partial failure.

## How a context bounds a call

Every method that talks to Docker or Ceph takes a `ctx`. Cancellation normally
returns an error matching `context.Canceled` or `context.DeadlineExceeded`;
it does not undo native work already submitted. Identity changes settle
submitted MON commands and report the observed outcome, and supervised
commands may need their own stop-confirmation budget. Preserve returned
results and errors, including `NativeCommandUnstoppedError`; see
[Partial failures](#partial-failures-and-retries) and
[Native cancellation](native-commands.md#cancellation-and-cleanup).
Waiting for a serialized operation also honors `ctx`, except for the
finalization steps described below. A deadline is not a universal return-time
guarantee.

### The startup timeout

`Run`, readiness waits and topology operations are also bounded by the
cluster's startup timeout, 3 minutes by default. Change it with
`ceph.WithStartupTimeout(d)`. The shorter of the startup timeout and your
context deadline applies to each operation. This covers, for example,
bootstrap, `AddOSD`, `OSDContainer.Remove`, `WaitForClean`, `WaitForPGClean`,
`WaitForQuorum`, `Pool.WaitForPGCount`, `DeepScrubPG`, `RepairPG`,
`cephfs.Filesystem.WaitReady`, `subvolume.Clone.Wait` and the typed RGW
user, account and placement helpers. Native escape hatches whose duration you
choose, such as `Container.Ceph`, `rgw.Gateway.Admin` and
`multisite.Fixture.ZoneAdmin`, are bounded by your context alone.

Some operations have their own fixed upper bound, which your context can only
shorten:

| Operation | Bound |
| --- | --- |
| `rbdmirror.Mirror.WaitReplayReady`, `WaitReceiverReady` | 4 minutes |
| `multisite.Fixture.WaitSyncReady`, `SyncGroup.WaitBucketSyncReady`, `SyncGroup.WaitBucketPolicyReady` | 4 minutes |
| `cephfsmirror.Mirror.WaitDirectoryReady`, `WaitSnapshotSynced` | 4 minutes |
| `cephfsmirror.DirectoryRemoval.WaitReleased`, `cephfsmirror.PeerRemoval.WaitDrained` | 2 minutes |
| `rbdmirror.Mirror.ReceiverStatus`, `BindNamespace` | 30 seconds |
| `rbdmirror.Mirror.RefreshMonitorConfig` | 2 minutes |
| `cephfsmirror.Run` waiting for the MGR `mirroring` module | 90 seconds |
| `diagnostics.Collect` | `diagnostics.Config.Timeout` (1 minute by default) |

These bounds are upper limits for the fixture, not a budget for your test.
Pass a context deadline that fits the condition you wait for, so that a
failure is reported as early as possible.

### Serialized operations

Operations that change topology or ownership are serialized per fixture: one
gate for each cluster, mirror link and multisite fixture. Examples are adding
and removing daemons, creating services, interrupting networks, pausing
containers, terminating, and creating or changing Cephx clients and CephFS
subvolumes.

Waiting for the gate honors your context. A call that is cancelled before it
gets the gate returns the context error and has made no native change, stopped
no container and changed no ownership. It checks the context again right after
getting the gate. You can retry the same call with a fresh context.

Once a call holds the gate, the native commands and Docker operations follow
their own context handling. Cancellation in the middle can leave a partial
result; the fixture still records every resource it created so that cleanup and
retries keep working. A cancelled call does not drop the bookkeeping of what it
already did.

The same holds for the methods that serialize with topology operations
without changing topology themselves. Examples are the override functions and
their `Restore` methods, the `Pool` methods, the RBD pool, namespace and image
client helpers, the RGW user helpers, the `rgw/account` and `rgw/placement`
helpers, `notification.Queue` and the admission of `rgw.Gateway.Admin`. A queued call that your
context cancels sends no native command, and its handle keeps everything it
owns: a cancelled `User.Remove`, `Account.Remove` or override `Restore` leaves
the handle as it was, so retry it with a fresh context.

Calls on one handle also wait for each other under your context. Examples are
`snapschedule.Lease.Restore` and the `Status`, `Stop` and `Restore` methods of
`rbd.ImmutableObjectCacheLease`.

Finalization steps are the exception. After a call has started a container or
changed native state, it records the result under the gate without a context,
so that a cancelled caller never loses the cleanup handle. Examples are
publishing a started gateway or MDS, removing a retired MDS from the
inventory, recording a new filesystem's native identity and settling a network
delay lease. Such a step waits for a running topology operation to finish.
Some cleanup steps also use their own short context, for example removing
temporary keyring files or stopping a packet capture. So a deadline bounds how
long the wait for admission and the native work run, but does not guarantee
that every call returns before the deadline while another operation holds the
cluster.

### Calls that are safe during long operations

`cluster.Ceph(ctx, ...)` does not take the cluster gate, so you can run it while
another goroutine performs a long topology operation. These variants also bound
their waits by `ctx`:

| Method | Returns |
| --- | --- |
| `ConnectionConfig(ctx)` | Independent copies of `ceph.conf` and the admin keyring; refuses a terminated or incompletely bootstrapped cluster |
| `Monitors(ctx)` | MON descriptors sorted by name |
| `Managers(ctx)` | MGR descriptors sorted by name |
| `OSDs(ctx)` | OSD descriptors sorted by ID |
| `rgw.Gateways(ctx, cluster)` | Gateway descriptors sorted by name, including partially started ones |
| `ControlContainer(ctx)` | The control CLI container; the snapshot does not guarantee the process stays alive |

A refused
or unavailable cluster and a cancelled `ctx` are returned as errors.

### Waiting in the background

Wait methods are synchronous. To wait while doing something else, run the wait
in your own goroutine and pass its single result through a buffered channel:

```golang
type replayResult struct {
	status rbdmirror.ImageStatus
	err    error
}

waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
results := make(chan replayResult, 1) // capacity 1: the worker never blocks
done := make(chan struct{})
// Registered after the fixture cleanups, so it runs before them.
t.Cleanup(func() { cancel(); <-done })
go func() {
	defer close(done)
	status, err := mirror.WaitReplayReady(waitCtx, "disk")
	results <- replayResult{status: status, err: err}
}()

// ... write to the source image ...

var result replayResult
select {
case result = <-results:
case <-otherEvent:
	cancel()
	<-done
	result = <-results
}
if result.err != nil {
	t.Fatalf("replay not ready: %v (last status %+v)", result.err, result.status)
}
```

- Send exactly one result and keep the channel buffered, so the worker can
  publish the real result even if the caller already chose another branch.
- Cancel tells the worker to stop; receiving from `done` confirms that it has
  returned. Do both before running callbacks or fixture cleanup.
- If the result and another event are ready at the same time, `select` may
  pick either. Keep the result the method returned: cancelling afterwards does
  not turn a completed success into a failure.
- On cancellation many waits return their last partial status together with
  the error. Report both.
- After a cancelled wait, use the still-live parent context for follow-up
  calls such as `fs.MDSStatus(ctx)`.
- `context.AfterFunc` runs on cancellation, not on completion, so it cannot
  replace joining the worker.
- A goroutine does not make fixture methods concurrent with each other. RGW
  waits keep the multisite fixture's gate while they poll, so other calls on
  the same fixture wait for them. RBD scoped waits pin the set of daemons they
  observe on their first read; after `AddDaemon`, `Daemon.Remove` or a
  replacement, start a new wait.

### Watching state

A wait returns once. To observe state repeatedly while the SUT runs, use a
Watch: one goroutine polls a native report and sends events on one
receive-only channel. Fan-out to several consumers is yours to build. Every
Watch takes a `ceph.WatchConfig` and has the same lifecycle:

| Watch | Observes | Fixed by the baseline | Default interval |
| --- | --- | --- | --- |
| `ceph.Observer.WatchOSDs` | up, in and weight of the owned OSDs | OSD IDs and native UUIDs | 2s |
| `ceph.Pool.WatchPGs` | PG states, up and acting sets, primaries and the PG set of one pool | FSID and native pool ID | 5s |
| `ceph.Observer.WatchHealth` | health status, check codes, severities, counts and mutes | FSID | 5s |
| `ceph.Observer.WatchMonitors` | monmap members and addresses, quorum and leader | FSID | 1s |
| `ceph.Observer.WatchManagers` | active MGR name and GID, availability, standbys and service URLs | FSID | 3s |
| `cephfs.Filesystem.WatchMDSs` | active ranks, standbys, standby-replay daemons and `max_mds` | FSMap ID and pool IDs | 2s |
| `subvolume.Clone.Watch` | native clone state and progress, until a terminal state | the clone request's target and source | 3s |
| `quiesce.Set.Watch` | native state, version, timeout and expiration of one quiesce set, until a terminal state | FSMap ID, the set ID, its members and the version `Start` confirmed | 1s |
| `snapschedule.Lease.Watch` | active state, created and pruned counts, first and last snapshot times and retention of one schedule | FSMap ID and the schedule's native start and creation time | 5s |
| `session.Watch` | admission, native state and removal of the selected client sessions, from samples bracketed by two reads of the active-rank holders | FSMap ID | 2s |
| `mirror.Mirror.WatchReceivers` and `Namespace.WatchReceivers` (RBD) | receiver membership, instances and leader agreement | the receiver cohort, peer and policies | 5s |
| `mirror.Mirror.WatchImage` and `Namespace.WatchImage` (RBD) | replay state and receiver assignment of one image pair | source image ID, global ID and the destination once enrolled | 5s |
| `mirror.Mirror.WatchDirectory` (CephFS) | assignment, sync state and last synced snapshot of one directory | filesystems, peer and the directory policy | 5s |
| `multisite.Fixture.WatchSync` (RGW) | metadata sync and the selected sources' data sync of one zone | realm, zonegroup and zone IDs and the committed period | 5s |
| `multisite.SyncGroup.WatchBucketSync` (RGW) | the bucket checkpoint of one pipe between two zones | the group's policy, both bucket instances and the committed period | 5s |
| `multisite.SyncGroup.WatchBucketPolicy` (RGW) | period, policy and bucket metadata import of one bucket policy into one zone, until imported | the committed period, the group's policy, the zone and the referenced bucket instances | 2s |

The default interval follows the cost of one read: an OSD watch read is one
`osd dump`, while a mirror read runs about a dozen commands on both clusters.

```golang
watch, err := observer.WatchOSDs(ctx, ceph.WatchConfig{Interval: time.Second})
if err != nil {
	t.Fatal(err) // the baseline read failed; nothing runs
}
// Registered after the fixture cleanups, so it runs before them.
t.Cleanup(func() { _ = watch.Stop() })
baseline := <-watch.Events() // already buffered

// ... stop an owned OSD and run the SUT ...

for event := range watch.Events() {
	if event.Kind == ceph.WatchGap {
		continue // a failed read: event.Err says why
	}
	if slices.ContainsFunc(event.States, func(s ceph.OSDState) bool { return s.ID == osd.ID && !s.Up }) {
		break // the MON reported it down at event.Epoch
	}
}
if err := watch.Stop(); err != nil {
	t.Fatal(err)
}
```

- The baseline is the first event and the read that the Watch call made
  before returning. It fixes the identities the watch follows. A later change
  that the watch reports, such as a failover or a new daemon instance, is an
  event; a replaced identity, such as a recreated pool or filesystem of the
  same name or another FSID, ends the watch with an error. Receive the
  baseline before you cause the change to observe.
- An event is sent only when the compared fields differ from the previous
  successful read. Each Watch documents the fields it compares. Epochs,
  counters and status text travel with the events but cause none by
  themselves.
- A failed read is a `WatchGap`, never an empty or healthy observation, and
  the next success is `WatchResumed` even when nothing changed. Two
  observations either side of a gap or of one interval do not show what
  happened between them: a transition shorter than the interval can be
  missed.
- `Sequence` increases by one per event and no event is dropped. A consumer
  that leaves the buffer full for one interval ends the watch with
  `ErrWatchOverflow`, which names the first undelivered event.
- `Started` and `Finished` are the local clock around the read, not when Ceph
  changed the state. Native epochs appear on an event only when the read
  supplied them.
- Each read is bounded by `QueryTimeout`, including its wait for fixture
  locks, and holds no lock between reads or while an event waits for the
  consumer. The test can call fixture operations between events. A read that
  waits too long for a lock is a gap.
- `Stop` cancels the watch, waits for its goroutine and returns the cause:
  nil for `Stop` itself, a finished clone or an imported bucket policy, the
  context's error,
  `ErrWatchOverflow`, or the read error that ended it. Cancelling the context
  alone does not wait. Stopping a watch never changes what it observed: a
  clone keeps running and a mirror keeps replaying.
- A Watch reports what Ceph reported. Up, active+clean, replaying or synced
  are not proof that a client reaches the daemon or that particular data
  arrived, so check the SUT's own results as well.

## Ownership

### What the cluster owns

`cluster.Terminate(ctx)` removes everything the cluster created, including
resources from calls that failed halfway:

| Resource | Created by |
| --- | --- |
| MON containers and the separate control CLI container (multi-MON clusters) | `Run`, `AddMonitor` |
| MGR containers | `Run`, `AddManager` |
| OSD containers and the in-memory OSD volume with its keeper container | `Run`, `AddOSD`, `OSDStorage.WithInMemory` |
| Shared `/var/run/ceph` volume | `Run`; MON/MGR/OSD and the control container share admin sockets |
| RGW gateways | `rgw.WithGateways`, `rgw.Start` |
| MDS daemons | `cephfs.WithFilesystems`, `cephfs.Start`, `ScaleMDS` |
| Host-port reservation helpers | Host mode MON and RGW startup |
| The public bridge and the separate cluster bridge | `Run`, `NetworkConfig.WithSeparateClusterNetwork` |
| Network delays and their netem helper containers | `ApplyNetworkProfile` (removed first, also on your client containers) |
| Container pauses | `fault.Pause` (resumed before the teardown) |
| Network interruptions | `fault.InterruptNetwork` (restored before the teardown, also on your client containers) |
| Native commands that are still running or not confirmed as stopped | `RunNativeCommand` (canceled and stopped, also in your client containers) |

Everything stored inside Ceph (pools, Cephx identities, configuration
database entries, OSD map flags and ratios, blocklist entries, CephFS and RGW
metadata) disappears with the containers. `Terminate` does not restore
override handles one by one; it discards the whole cluster.

`Terminate` works in a fixed order: it cancels the running native commands,
then removes network delays, restores network interruptions, resumes paused
containers and stops the cancelled commands, releases host-port reservations,
stops gateways and MDS daemons while the OSDs are still running, then removes
OSDs, the OSD memory or volume storage, MGRs, MONs, the MON store volumes, the
control container, the client toolkit container, then any container left with
the cluster's label `org.testcontainers.ceph.cluster` (one that Docker created
although the create reply was lost), the shared run volume, and finally the
networks. Each
container gets a 2-second stop timeout unless you pass
`testcontainers.StopTimeout`. A container or network that fails to go away
stays tracked and is reported in the joined error; calling `Terminate` again
retries only what is left. A network whose interruption could not be restored
is kept, because a container may still be attached to it.

After `Terminate`, cluster operations fail, most of them with "ceph cluster is
terminated". `ceph.FactsOf` and the descriptor getters still answer.

### What you own

| Resource | How to clean it up |
| --- | --- |
| Containers attached with `WithClient` or `ClientConfig.WithIdentity` | Terminate them before the cluster. The cluster's networks cannot be removed while they are attached. |
| `rbdmirror.Mirror`, `cephfsmirror.Mirror`, `multisite.Fixture` | `Terminate` before both clusters. They own their daemons, setup clients, the multisite HTTP network and the network attachments they added. Ceph-side peers, auth entities, realms and policies stay in the clusters. |
| Handles from the package function `fault.InterruptLink` | `Restore` them; no fixture tracks them. |
| Copies from `ConnectionConfig` and `ClientConfig.ConnectionConfig`, and files you write from them | Delete them; they contain credentials. |
| Diagnostics reports | They may contain log lines with secrets; review before sharing. |
| Override handles | Optional when the cluster is discarded afterwards; see [Override handles](#override-handles). |

### Registering cleanup

`t.Cleanup` functions run in last-in, first-out order. Register the cluster
first and anything that depends on it afterwards, so dependants are removed
first. `testcontainers.CleanupContainer` is a no-op for a nil container, so
call it before checking the error: a non-nil cluster returned with an error
still needs to be terminated.

```golang
source, err := ceph.Run(ctx, ceph.DefaultImage, rbd.WithPools(ceph.PoolConfig{Name: "images"}))
testcontainers.CleanupContainer(t, source)
if err != nil {
	t.Fatal(err)
}
destination, err := ceph.Run(ctx, ceph.DefaultImage, rbd.WithPools(ceph.PoolConfig{Name: "images"}))
testcontainers.CleanupContainer(t, destination)
if err != nil {
	t.Fatal(err)
}

sourceFacts, err := ceph.FactsOf(source)
client, err := testcontainers.Run(ctx, sourceFacts.Images.Client,
	source.WithClient(), ceph.WithIdleEntrypoint())
testcontainers.CleanupContainer(t, client) // runs before both clusters
if err != nil {
	t.Fatal(err)
}

mirror, err := rbdmirror.Run(ctx, sourceFacts.Images.RBDAux, rbdmirror.WithConfig(rbdmirror.Config{
	Source: source, Destination: destination, Pool: "images",
}))
testcontainers.CleanupContainer(t, mirror) // runs first
if err != nil {
	t.Fatal(err)
}
```

`rbdmirror.Mirror` and `cephfsmirror.Mirror` embed a container handle, so
`testcontainers.CleanupContainer` accepts them. `multisite.Fixture` does not; clean
it up with `t.Cleanup`:

```golang
site, err := multisite.Run(ctx, ceph.DefaultImage, multisite.WithConfig(multisite.Config{
	Source: source, Destination: destination,
}))
if site != nil {
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := site.Terminate(cleanupCtx); err != nil {
			t.Error(err)
		}
	})
}
if err != nil {
	t.Fatal(err)
}
```

### Use fresh contexts in cleanup

`t.Context()` is cancelled just before cleanup functions run. Do not capture it
in cleanup code. `testcontainers.CleanupContainer` uses
`context.Background()`, which has no deadline; for a bounded cleanup, call
`Terminate` yourself with a context derived from `context.Background()`, as in
the multisite example above. The same applies to deferred `Restore` and
`Resume` calls in the examples of these guides, written as `cleanupCtx`.

If the test process dies, testcontainers' Ryuk reaper removes the labeled
containers, networks and volumes, unless Ryuk is disabled.

### Collect diagnostics before cleanup

`diagnostics.Collect` reads container state, log tails and fixed Ceph queries
without changing anything. Register it after the cluster cleanup so it runs
first, and give it its own context:

```golang
t.Cleanup(func() {
	if !t.Failed() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	report, err := diagnostics.Collect(ctx, cluster, diagnostics.Config{})
	if report != nil {
		data, _ := json.MarshalIndent(report, "", "  ")
		t.Logf("ceph diagnostics:\n%s", data)
	}
	if err != nil {
		t.Logf("diagnostics incomplete: %v", err)
	}
})
```

A report is returned together with an error when some observations failed;
keep it. See [Running native commands](native-commands.md#diagnostics) for the
options. `diagnostics.CollectCrashMetadata` works the same way for the crash
reports that daemons wrote; it also reads containers that have stopped.

To run post-test checks, write both reports into a failure archive and remove
caller-owned clients before `Terminate` on one ordered path, register
[`finalize.TerminateOnCleanup`](../../ceph/finalize/README.md) right after
`ceph.Run` instead of `testcontainers.CleanupContainer`. Each of its phases
uses a fresh context bounded by its own timeout in `finalize.Config`.

### A fixture without a testing.TB

`finalize.TerminateOnCleanup` needs a `testing.TB`. A fixture that runs in a
process of its own, driven by a test over IPC, has none. It ends the cluster
itself: on the driver's failure signal, on the driver going away or on an
interrupt, it runs one `diagnostics.Collect` with a fresh bounded context and
saves the report, even one returned with an error, and then calls `Terminate`
with another fresh bounded context. Keep the failure signal in the process's
result whatever the collection and `Terminate` return, so that a complete
report or a clean `Terminate` does not turn a failed run into a pass.
`Finish` in `internal/consumerevidence` and its command `ipcfixture` show it
with public calls only. Disable the reaper in that process
(`TESTCONTAINERS_RYUK_DISABLED=true`) only when the driver checks the cleanup
itself and removes what is left on failure.

## Handles returned with errors

Many calls return a non-nil result together with an error:

```golang
pause, err := fault.Pause(ctx, cluster, osd)
if pause != nil {
	defer pause.Resume(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
```

The reason is that a failed reply does not prove that nothing happened. Docker
may have paused the container before the connection broke; Ceph may have
applied a setting before the CLI was cancelled; a container may have been
created before its readiness check timed out. The returned handle records what
was attempted, so that you or the cluster can undo it. Always keep a non-nil
result:

| Call | Non-nil result on error | What to do |
| --- | --- | --- |
| `Run` (all service `Run`s) | Partially created cluster | `Terminate` it (`CleanupContainer` does) |
| `AddMonitor`, `AddManager`, `AddOSD`, `rgw.Start`, `cephfs.Start` | Partial daemon or service, owned by the cluster | Inspect it, or let `Terminate` remove it |
| `CreatePool` | Pool whose creation was attempted | Inspect it or discard the cluster; data is never deleted automatically |
| `rgw.Gateway.CreateRole` | Role whose creation or permission policies failed or are unknown | `Role.Remove`, or discard the cluster; do not create the same name again |
| `CreateClient` | Attempted identity | Inspect auth state or discard the cluster; do not retry with broader caps |
| `ClientConfig.ReplaceKey` | Descriptor for the attempted key, possibly still unusable | Preserve it and the original descriptor; settle the identity before use/retry (see [ClientConfig.ReplaceKey](ceph-operations.md#clientconfigreplacekey)) |
| `override.SetConfig`, `override.SetMessengerPolicy`, `ClientConfig.Revoke`, `override.SetOSDFlag`, `override.SetMGRModule`, `override.SetFullRatios`, `override.AddBlocklist`, `session.OverrideTimeouts`, `session.OverrideRefusal`, `session.OverrideRequiredFeatures`, `cephfs.Filesystem.OverrideMaxFileSize`, `subvolume.Subvolume.OverridePin`, `subvolume.Group.OverridePin`, `rgw.User.OverrideRateLimit`, `Pool.OverridePGPrimary` | Override handle | `Restore`, or discard the cluster |
| `fault.Pause` | Pause handle | `Resume` |
| `quiesce.Start` | Quiesce set handle (the set may exist and hold I/O paused) | `Release`, or let its `Expiration` pass before terminating the cluster |
| `RunNativeCommand` | `*ceph.NativeCommandUnstoppedError` (found with `errors.As`) | `Stop`, after resuming a paused container |
| `InterruptNetwork`, `InterruptPeerLink`, `InterruptZoneLink` | Interruption handle | `Restore` |
| `ApplyNetworkProfile`, `ApplyLinkProfile`, `ApplyZoneLinkProfile` | Delay lease (the change may have reached the kernel) | `Restore` |
| `rbdmirror.Run`, `cephfsmirror.Run`, `multisite.Run` | Partial fixture | `Terminate` it |
| `AddDaemon` on a mirror | Partial daemon | `Daemon.Remove` or the fixture's `Terminate` |
| `multisite.Fixture.AddZone`, `AddZonegroup` | Partial zone or zonegroup | `Terminate` the fixture; do not add the same name again |

## Override handles

The override calls below change fixture state and return a handle that can put it
back. Some wrap several native settings:

| Method | Changes |
| --- | --- |
| `override.SetConfig` | One central configuration entry (section, optional mask, name) |
| `override.SetMessengerPolicy` | The selected MON/MGR/OSD client-facing mode entries; established connections keep their modes |
| `ClientConfig.Revoke` | Removes one owned client identity; `Restore` puts back its exact key and capabilities |
| `override.SetOSDFlag` | One global OSD map flag; a refused call still returns a handle whose `Restore` does nothing |
| `override.SetMGRModule` | Whether one MGR module is enabled; a refused call still returns a handle whose `Restore` does nothing |
| `override.SetFullRatios` | The nearfull, backfillfull and full ratios; a refused call still returns a handle whose `Restore` does nothing |
| `override.AddBlocklist` | Adds one exact client-session blocklist entry; a refused call still returns a handle whose `Restore` does nothing |
| `session.OverrideTimeouts` | A filesystem's `session_timeout` and `session_autoclose` |
| `session.OverrideRefusal` | Sets a filesystem's `refuse_client_session`; a filesystem that already refuses new sessions is refused without a handle |
| `session.OverrideRequiredFeatures` | Adds named features to a filesystem's `required_client_features`; a feature already required is recorded and owned by nobody, and `Restore` removes only the added bits |
| `cephfs.Filesystem.OverrideMaxFileSize` | A filesystem's `max_file_size` |
| `subvolume.Subvolume.OverridePin`, `subvolume.Group.OverridePin` | One pin policy field of a subvolume or group |
| `rgw.User.OverrideRateLimit` | The rate-limit policy of one owned user or of one bucket it owns; a refused call still returns a handle whose `Restore` does nothing |
| `Pool.OverridePGPrimary` | The pg-upmap-primary entry of one PG of a replicated pool, and `require_min_compat_client` when the caller lets it raise the level; a refused call returns no handle |

They share these rules:

- **One handle per target.** A second override of the same key, flag, module,
  filesystem or pin field is refused while the first is active, even when it
  would set the same value.
- **The original state is recorded exactly.** `override.SetConfig` distinguishes
  an absent entry from one that is set, so `Restore` removes an entry that did
  not exist instead of pinning the inherited default. `override.SetOSDFlag`
  remembers whether the flag was already set.
- **Copies share state.** Copying a handle does not create a second override.
- **`Restore` is idempotent.** After a successful restore, further calls on any
  copy do nothing. Call `Restore` explicitly when the condition should end; a
  deferred `Restore` afterwards is harmless.
- **`Restore` refuses outside edits.** If the current value is neither the one
  the handle applied nor the original one, `Restore` returns an error and
  leaves the value alone rather than overwriting someone else's change. Ceph
  has no compare-and-swap for these settings, so an outside write of the
  same value cannot be detected; do not change the same target with native
  commands while a handle is active.
- **Uncertain replies are reconciled.** If a command's reply was lost,
  retrying `Restore` with a fresh context first reads the native state and
  only then decides what to change.
- **Unrelated state is untouched.** Restoring a flag never touches other flags;
  restoring ratios never touches configuration or pools.
- **Termination is enough.** When the cluster is discarded at the end of the
  test, restoring is optional. Restore when the same cluster keeps being used,
  for example by later subtests.

Handle-specific details:

- `override.SetConfig` records the value as Ceph normalized it, which can differ
  from the string you passed. A key set by `ceph.WithConfigFile` cannot be changed
  this way, because local files take precedence over the configuration
  database. Ticket lifetimes written by `WithAuth` also take precedence over
  later central overrides. `MessengerV2Only` reserves `ms_bind_msgr1` and
  `ms_bind_msgr2`; `MessengerV2Secure` reserves those and its six mode
  settings. Refusals of these Messenger settings match `ErrInvalidRequest`.
- `override.SetMessengerPolicy` uses `override.SetConfig` overrides, restoring in
  reverse order. Application can partly succeed, so keep a non-nil handle
  returned with an error. An empty policy, an unknown connection mode and any
  use under `MessengerV2Secure` are refused before any change with
  `ErrInvalidRequest`.
- `Restore` of a `ClientConfig.Revoke` handle refuses an identity recreated outside
  the fixture with another key or capabilities. `ClientConfig.Delete` makes the
  removal permanent instead of restoring it.
- `override.BlocklistHandle.Restore` removes only the entry it created. An entry
  that expired on its own counts as removed; an entry whose expiry someone
  renewed is refused.
- `override.FullRatiosHandle.Restore` accepts only the last tuple it applied, or
  either side of one uncertain command. The native CLI changes one ratio per
  command, so apply and restore are not atomic.
- `session.TimeoutsOverrideHandle.Restore` refuses a filesystem that was recreated
  under the same name.
- `session.RequiredFeaturesOverrideHandle` owns each bit it added, not the
  features that were already required. The native command adds one feature
  at a time, so a failed later step returns the handle with the earlier
  steps owned, and `Steps` reports each one. `Restore` removes the owned
  bits in reverse order and refuses, before any change, an owned feature
  that was removed outside it; an add whose outcome was never confirmed is
  removed only when it is required. The commands have no precondition, so an
  outside remove and re-add between two reads is not detected. Removing a
  requirement does not reopen a session it evicted.
- `Pool.OverridePGPrimary` overrides of different PGs share one raise of
  `require_min_compat_client`: the level goes back to its prior release when
  the last of them is restored. Ceph cancels an entry whose OSD left the PG's
  placement or became its CRUSH primary, so `Restore` refuses an entry that
  disappeared when a prior entry existed. A removed pool takes its entries
  with it, which counts as restored.

`fault.PauseLease.Resume` and `NetworkInterruption.Restore` follow the same
pattern for Docker state, with one difference: the cluster resumes and
restores them itself during `Terminate`, because a frozen or disconnected
container would block cleanup.

`ApplyNetworkProfile` also returns a restorable lease, with ownership tied
to its interface and qdisc. It removes only its own qdisc and is restored
before daemon teardown. Its kernel-state checks and helper requirements are
described in [Fault injection](fault-injection.md#delaying-packets-below-ceph).

## Partial failures and retries

Operations made of several native steps can fail in the middle. None of them
rolls back automatically. Each one keeps enough state to retry or to clean up:

| Operation | After an error |
| --- | --- |
| `OSDContainer.Remove` | Retry with a fresh context. If the purge reply was lost, the retry accepts the OSD's absence; if the purge completed, only the Docker cleanup is repeated. `AddOSD` is refused until the removal is finished. A removal that timed out because data could not move leaves the OSD out and reweighted. |
| `MonitorContainer.Remove`, `ManagerContainer.Remove` | The member stays owned; retry or terminate. |
| `AddMonitor` that joined but could not update every daemon's `ceph.conf` | `RefreshMonitorConfig(ctx)`; do not add the same name again. |
| `Pool.Remove` | Retry; it finishes removing whichever owned resources remain. |
| `Pool.SetQuota`, `Pool.SetReplication`, `Pool.SetPGCount` | Inspect `Pool.Status`; commands may have partly succeeded. |
| `ClientConfig.UpdateCaps`, `ClientConfig.Delete`, `ClientConfig.ReplaceKey`, `ClientConfig.Revoke` | Submitted MON changes are fenced and settled; when the outcome remains uncertain, later operations on the identity settle first. Preserve partial descriptors/handles and inspect `ClientConfig.Capabilities` or auth state before relying on a change. |
| `RotateKey` | Keep the failure and inspect the daemon/auth state; the operation fences submitted MON commands and reconciles its keyring files. Do not replace keys with a raw command while it runs. |
| `Filesystem.Remove` | Retry; it continues with whatever remains. |
| `cephfs.Filesystem.ScaleMDS` | Desired counts and partial daemons are kept for cleanup; the filesystem and its data are never deleted. |
| `RefreshMonitorConfig`, `RefreshClientMonitorConfig`, mirror `RefreshMonitorConfig` | Successful copies are kept; retry with a fresh context and up-to-date files are skipped. |
| `multisite.Fixture.RemoveZone` | Retry with the same name. |
| `Daemon.Remove` on a mirror | Membership is kept; retry. |
| Mirror and multisite `Terminate` | Retry; successful removals are not repeated. |

General rules:

- Retry with a fresh context, not the one that expired.
- Do not retry a creation under the same name to "fix" a partial failure. The
  fixture never adopts a resource it cannot prove it created.
- Do not run native commands that edit the same resources while a fixture
  operation runs. Ceph offers no compare-and-swap for most of these changes.
- When in doubt, discard the cluster. It is disposable by design.

## Sharing one cluster across tests

Starting a cluster takes time, so a package may share one cluster between
tests:

```golang
var shared *ceph.Container

func TestMain(m *testing.M) {
	ctx := context.Background()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage)
	if err != nil {
		if cluster != nil {
			_ = cluster.Terminate(ctx)
		}
		log.Fatal(err)
	}
	shared = cluster
	code := m.Run()
	if err := cluster.Terminate(ctx); err != nil {
		log.Print(err)
	}
	os.Exit(code)
}

func TestSomething(t *testing.T) {
	ctx := t.Context()
	pool, err := shared.CreatePool(ctx, ceph.PoolConfig{Name: "t-something"})
	if pool != nil {
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := pool.Remove(cleanupCtx); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	// ...
}
```

- Give every test its own pool and remove it with `Pool.Remove`, which also
  removes the CRUSH rule and erasure-code profile, so long runs do not reach
  PG limits.
- Restore every override handle, delete Cephx clients with
  `ClientConfig.Delete`, and resume or restore every fault before the test ends.
- Tests that use overlapping overrides (the same configuration key, flag or
  module) cannot run in parallel: the second override is refused.
- Topology operations are serialized per cluster, so parallel tests that add
  or remove daemons wait for each other.

## Related APIs

- [`ceph`](../../ceph/README.md): `Run`, `Terminate`, `WithStartupTimeout`,
  `RunNativeCommand`, context-aware getters.
- [`diagnostics`](../../ceph/diagnostics/README.md): `Collect`,
  `CollectCrashMetadata`, `Redact`.
- [`finalize`](../../ceph/finalize/README.md): `TerminateOnCleanup` and the
  `Finalization` checks, cleanups and failure archive.
- [`override`](../../override/README.md): the override calls and their
  restoration handles.
- [`rbd`](../../rbd/README.md), [`cephfs`](../../cephfs/README.md), [`rgw`](../../rgw/README.md): service
  and multicluster fixtures and their `Terminate`.
- [Fault injection](fault-injection.md): pauses, interruptions and overrides
  in practice.
- [Running native commands](native-commands.md): inspecting state after a
  partial failure.
- [Classified errors](errors.md): telling refused requests, missing features,
  image tools, engine limits and native failures apart.
