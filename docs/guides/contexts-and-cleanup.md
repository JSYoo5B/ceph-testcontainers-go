# Contexts, ownership and cleanup

## Introduction

A test cluster consists of many containers, networks and Ceph-side objects,
and many operations change several of them in sequence. This guide explains:

- how the `context.Context` you pass bounds each call;
- which resources the cluster owns and removes in `Terminate`, and which you
  own;
- why many calls return a non-nil handle together with an error, and what to
  do with it;
- how `Temporary*` overrides are restored;
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
Some methods also wait for a serialized operation regardless of `ctx`, as
described below. A deadline is not a universal return-time guarantee.

### The startup timeout

`Run`, readiness waits and topology operations are also bounded by the
cluster's startup timeout, 3 minutes by default. Change it with
`ceph.WithStartupTimeout(d)`. The shorter of the startup timeout and your
context deadline applies to each operation. This covers, for example,
bootstrap, `AddOSD`, `RemoveOSD`, `WaitForClean`, `WaitForPGClean`,
`WaitForQuorum`, `WaitForPoolPGCount`, `DeepScrubPG`, `RepairPG`,
`cephfs.Filesystem.WaitReady`, `WaitForSubvolumeClone` and
`rgw.Gateway.Admin`.

Some operations have their own fixed upper bound, which your context can only
shorten:

| Operation | Bound |
| --- | --- |
| `rbd.Mirror.WaitReplayReady`, `WaitReceiverReady` | 4 minutes |
| `rgw.Multisite.WaitSyncReady`, `WaitBucketSyncReady`, `WaitBucketSyncPolicyReady` | 4 minutes |
| `cephfs.Mirror.WaitDirectoryReady`, `WaitSnapshotSynced` | 4 minutes |
| `cephfs.MirrorDirectoryRemoval.WaitReleased`, `cephfs.MirrorPeerRemoval.WaitDrained` | 2 minutes |
| `rbd.Mirror.ReceiverStatus`, `BindNamespace` | 30 seconds |
| `rbd.Mirror.RefreshMonitorConfig` | 2 minutes |
| `cephfs.RunMirror` waiting for the MGR `mirroring` module | 90 seconds |
| `CollectDiagnostics` | `DiagnosticsConfig.Timeout` (1 minute by default) |

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

Not every call waits for the gate under your context. Many other methods of
the same cluster also serialize with topology operations but wait for a running
one to finish regardless of `ctx`, then apply `ctx` to their native work.
Examples are `OverrideConfig`, `OverrideOSDFlag`, `OverrideMGRModule`,
`TemporaryBlocklist` and their `Restore` methods, `SetOSDIn`, `SetPoolQuota`,
`SetPoolReplication`, the RBD pool and namespace helpers, the RGW user, account
and placement helpers, `rgw.Gateway.Admin`, and descriptor getters such as
`OSDs(ctx)` and `Monitors(ctx)`. Some cleanup steps also use their own short context,
for example removing temporary keyring files. So a deadline bounds how long
native work runs, but does not guarantee that every call returns before the
deadline while another operation holds the cluster.

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

The context-free versions (`ConnectionConfig`, `Monitors`, `Managers`, `OSDs`,
`rgw.Gateways`, `ControlContainer`) remain available.

### Waiting in the background

Wait methods are synchronous. To wait while doing something else, run the wait
in your own goroutine and pass its single result through a buffered channel:

```golang
type replayResult struct {
	status rbd.MirrorImageStatus
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
  observe on their first read; after `AddDaemon`, `RemoveDaemon` or a
  replacement, start a new wait.

## Ownership

### What the cluster owns

`cluster.Terminate(ctx)` removes everything the cluster created, including
resources from calls that failed halfway:

| Resource | Created by |
| --- | --- |
| MON containers and the separate control CLI container (multi-MON clusters) | `Run`, `AddMonitor` |
| MGR containers | `Run`, `AddManager` |
| OSD containers and the in-memory OSD volume with its keeper container | `Run`, `AddOSD`, `AddOSDWithConfig`, `WithOSDInMemoryStorage` |
| Shared `/var/run/ceph` volume | `Run`; MON/MGR/OSD and the control container share admin sockets |
| RGW gateways | `rgw.WithGateways`, `rgw.Start` |
| MDS daemons | `cephfs.WithFilesystems`, `cephfs.Start`, `ScaleMDS` |
| Host-port reservation helpers | Host mode MON and RGW startup |
| The public bridge and the separate cluster bridge | `Run`, `WithSeparateClusterNetwork` |
| Network delays and their netem helper containers | `ApplyNetworkProfile` (removed first, also on your client containers) |
| Container pauses | `fault.Pause` (resumed before the teardown) |
| Network interruptions | `cluster.InterruptNetwork` (restored before the teardown, also on your client containers) |
| Native commands that are still running or not confirmed as stopped | `RunNativeCommand` (canceled and stopped, also in your client containers) |

Everything stored inside Ceph (pools, Cephx identities, configuration
database entries, OSD map flags and ratios, blocklist entries, CephFS and RGW
metadata) disappears with the containers. `Terminate` does not restore
`Temporary*` overrides one by one; it discards the whole cluster.

`Terminate` works in a fixed order: it removes network delays, restores network
interruptions, resumes paused containers, cancels and stops native commands,
releases host-port reservations, stops gateways and MDS daemons while the OSDs
are still running, then removes OSDs, the in-memory OSD volume, MGRs, MONs, the
control container, then any container left with the cluster's label
`org.testcontainers.ceph.cluster` (one that Docker created although the create
reply was lost), the shared run volume, and finally the networks. Each
container gets a 2-second stop timeout unless you pass
`testcontainers.StopTimeout`. A container or network that fails to go away
stays tracked and is reported in the joined error; calling `Terminate` again
retries only what is left. A network whose interruption could not be restored
is kept, because a container may still be attached to it.

After `Terminate`, cluster operations fail, most of them with "ceph cluster is
terminated". `ControlImage()` and the descriptor getters still answer.

### What you own

| Resource | How to clean it up |
| --- | --- |
| Containers attached with `WithClient` or `WithClientIdentity` | Terminate them before the cluster. The cluster's networks cannot be removed while they are attached. |
| `rbd.Mirror`, `cephfs.Mirror`, `rgw.Multisite` | `Terminate` before both clusters. They own their daemons, setup clients, the multisite HTTP network and the network attachments they added. Ceph-side peers, auth entities, realms and policies stay in the clusters. |
| Handles from the package function `ceph.InterruptNetwork` | `Restore` them; no fixture tracks them. |
| Copies from `ConnectionConfig` and `ClientConfig.ConnectionConfig`, and files you write from them | Delete them; they contain credentials. |
| Diagnostics reports | They may contain log lines with secrets; review before sharing. |
| `Temporary*` handles | Optional when the cluster is discarded afterwards; see [Temporary overrides](#temporary-overrides). |

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

client, err := testcontainers.Run(ctx, source.ControlImage(),
	source.WithClient(), ceph.WithIdleEntrypoint())
testcontainers.CleanupContainer(t, client) // runs before both clusters
if err != nil {
	t.Fatal(err)
}

mirror, err := rbd.RunMirror(ctx, source.ControlImage(), rbd.MirrorConfig{
	Source: source, Destination: destination, Pool: "images",
})
testcontainers.CleanupContainer(t, mirror) // runs first
if err != nil {
	t.Fatal(err)
}
```

`rbd.Mirror` and `cephfs.Mirror` embed a container handle, so
`testcontainers.CleanupContainer` accepts them. `rgw.Multisite` does not; clean
it up with `t.Cleanup`:

```golang
site, err := rgw.RunMultisite(ctx, ceph.DefaultImage, rgw.MultisiteConfig{
	Source: source, Destination: destination,
})
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

`CollectDiagnostics` reads container state, log tails and fixed Ceph queries
without changing anything. Register it after the cluster cleanup so it runs
first, and give it its own context:

```golang
t.Cleanup(func() {
	if !t.Failed() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	report, err := cluster.CollectDiagnostics(ctx, ceph.DiagnosticsConfig{})
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
options. `CollectCrashMetadata` works the same way for the crash reports that
daemons wrote; it also reads containers that have stopped.

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
| `AddMonitor`, `AddManager`, `AddOSD`, `AddOSDWithConfig`, `rgw.Start`, `cephfs.Start` | Partial daemon or service, owned by the cluster | Inspect it, or let `Terminate` remove it |
| `CreatePool` | Pool whose creation was attempted | Inspect it or discard the cluster; data is never deleted automatically |
| `CreateClient` | Attempted identity | Inspect auth state or discard the cluster; do not retry with broader caps |
| `ReplaceClientKey` | Descriptor for the attempted key, possibly still unusable | Preserve it and the original descriptor; settle the identity before use/retry (see [ReplaceClientKey](../ceph.md#replaceclientkey)) |
| `OverrideConfig`, `OverrideMessengerPolicy`, `RevokeClient`, `OverrideOSDFlag`, `OverrideMGRModule`, `OverrideFullRatios`, `TemporaryBlocklist`, `cephfs` `OverrideSessionTimeouts`, `OverrideSubvolumePin`, `OverrideSubvolumeGroupPin` | Override handle | `Restore`, or discard the cluster |
| `fault.Pause` | Pause handle | `Resume` |
| `RunNativeCommand` | `*ceph.NativeCommandUnstoppedError` (found with `errors.As`) | `Stop`, after resuming a paused container |
| `InterruptNetwork`, `InterruptPeerLink`, `InterruptZoneLink` | Interruption handle | `Restore` |
| `ApplyNetworkProfile`, `ApplyLinkProfile`, `ApplyZoneLinkProfile` | Delay lease (the change may have reached the kernel) | `Restore` |
| `rbd.RunMirror`, `cephfs.RunMirror`, `rgw.RunMultisite`, `rgw.RunTopology` | Partial fixture | `Terminate` it |
| `AddDaemon` on a mirror | Partial daemon | `RemoveDaemon` or the fixture's `Terminate` |
| `Multisite.AddZone`, `AddZonegroup` | Partial zone or zonegroup | `Terminate` the fixture; do not add the same name again |

## Temporary overrides

`Temporary*` methods change fixture state and return a handle that can put it
back. Some wrap several native settings:

| Method | Changes |
| --- | --- |
| `OverrideConfig` | One central configuration entry (section, optional mask, name) |
| `OverrideMessengerPolicy` | The selected MON/MGR/OSD client-facing mode entries; established connections keep their modes |
| `RevokeClient` | Removes one owned client identity; `Restore` puts back its exact key and capabilities |
| `OverrideOSDFlag` | One global OSD map flag |
| `OverrideMGRModule` | Whether one MGR module is enabled |
| `OverrideFullRatios` | The nearfull, backfillfull and full ratios |
| `TemporaryBlocklist` | Adds one exact client-session blocklist entry |
| `cephfs.Filesystem.OverrideSessionTimeouts` | A filesystem's `session_timeout` and `session_autoclose` |
| `cephfs.Filesystem.OverrideSubvolumePin`, `OverrideSubvolumeGroupPin` | One pin policy field of a subvolume or group |

They share these rules:

- **One handle per target.** A second override of the same key, flag, module,
  filesystem or pin field is refused while the first is active, even when it
  would set the same value.
- **The original state is recorded exactly.** `OverrideConfig` distinguishes
  an absent entry from one that is set, so `Restore` removes an entry that did
  not exist instead of pinning the inherited default. `OverrideOSDFlag`
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

- `OverrideConfig` records the value as Ceph normalized it, which can differ
  from the string you passed. A key set by `ceph.WithConfigFile` cannot be changed
  this way, because local files take precedence over the configuration
  database. Ticket lifetimes written by `WithAuth` also take precedence over
  later central overrides. `MessengerV2Only` reserves `ms_bind_msgr1` and
  `ms_bind_msgr2`; `MessengerV2Secure` reserves those and its six mode
  settings. Refusals of these Messenger settings match `ErrInvalidRequest`.
- `OverrideMessengerPolicy` uses `OverrideConfig` overrides, restoring in
  reverse order. Application can partly succeed, so keep a non-nil handle
  returned with an error. An empty policy, an unknown connection mode and any
  use under `MessengerV2Secure` are refused before any change with
  `ErrInvalidRequest`.
- `RevokeClient.Restore` refuses an identity recreated outside
  the fixture with another key or capabilities. `DeleteClient` makes the
  removal permanent instead of restoring it.
- `TemporaryBlocklist.Restore` removes only the entry it created. An entry
  that expired on its own counts as removed; an entry whose expiry someone
  renewed is refused.
- `FullRatiosOverride.Restore` accepts only the last tuple it applied, or
  either side of one uncertain command. The native CLI changes one ratio per
  command, so apply and restore are not atomic.
- `OverrideSessionTimeouts.Restore` refuses a filesystem that was recreated
  under the same name.

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
| `RemoveOSD` | Retry with a fresh context. If the purge reply was lost, the retry accepts the OSD's absence; if the purge completed, only the Docker cleanup is repeated. `AddOSD` is refused until the removal is finished. A removal that timed out because data could not move leaves the OSD out and reweighted. |
| `RemoveMonitor`, `RemoveManager` | The member stays owned; retry or terminate. |
| `AddMonitor` that joined but could not update every daemon's `ceph.conf` | `RefreshMonitorConfig(ctx)`; do not add the same name again. |
| `RemovePool` | Retry; it finishes removing whichever owned resources remain. |
| `SetPoolQuota`, `SetPoolReplication`, `SetPoolPGCount` | Inspect `PoolStatus`; commands may have partly succeeded. |
| `UpdateClientCaps`, `DeleteClient`, `ReplaceClientKey`, `RevokeClient` | Submitted MON changes are fenced and settled; when the outcome remains uncertain, later operations on the identity settle first. Preserve partial descriptors/handles and inspect `ClientCapabilities` or auth state before relying on a change. |
| `RotateDaemonKey` | Keep the failure and inspect the daemon/auth state; the operation fences submitted MON commands and reconciles its keyring files. Do not replace keys with a raw command while it runs. |
| `cephfs.Remove` | Retry; it continues with whatever remains. |
| `cephfs.Filesystem.ScaleMDS` | Desired counts and partial daemons are kept for cleanup; the filesystem and its data are never deleted. |
| `RefreshMonitorConfig`, `RefreshClientMonitorConfig`, mirror `RefreshMonitorConfig` | Successful copies are kept; retry with a fresh context and up-to-date files are skipped. |
| `rgw.Multisite.RemoveZone` | Retry with the same name. |
| `RemoveDaemon` on a mirror | Membership is kept; retry. |
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
			if err := shared.RemovePool(cleanupCtx, pool.Name); err != nil {
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

- Give every test its own pool and remove it with `RemovePool`, which also
  removes the CRUSH rule and erasure-code profile, so long runs do not reach
  PG limits.
- Restore every `Temporary*` override, delete Cephx clients with
  `DeleteClient`, and resume or restore every fault before the test ends.
- Tests that use overlapping overrides (the same configuration key, flag or
  module) cannot run in parallel: the second override is refused.
- Topology operations are serialized per cluster, so parallel tests that add
  or remove daemons wait for each other.

## Related APIs

- [ceph.md](../ceph.md): `Run`, `Terminate`, `WithStartupTimeout`,
  `Temporary*`, `RunNativeCommand`, `CollectDiagnostics`, context-aware
  getters.
- [rbd.md](../rbd.md), [cephfs.md](../cephfs.md), [rgw.md](../rgw.md): service
  and multicluster fixtures and their `Terminate`.
- [Fault injection](fault-injection.md): pauses, interruptions and overrides
  in practice.
- [Running native commands](native-commands.md): inspecting state after a
  partial failure.
- [Classified errors](errors.md): telling refused requests, missing features,
  image tools, engine limits and native failures apart.
