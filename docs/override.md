# Reversible overrides

Import `github.com/JSYoo5B/ceph-testcontainers-go/override` alongside `ceph`.
The package changes cluster-wide state of a cluster created by `ceph.Run` for a
while and puts it back. Each operation is a function that takes the cluster and
returns a handle; the handle owns the change and its `Restore` undoes only what
the call did. This page documents seven operations: the client blocklist entry,
OSDMap full ratios, MGR module state, global OSD flags, runtime debug levels,
central configuration entries and messenger policy. The package and restoration
boundary follow the [design](design/override-package-boundary.md).

0.x: old names are removed without aliases. The seven `Container` methods and
their handle types no longer exist in `ceph`; each is an `override` function
with its own handle.

`ceph.BlocklistEntry`, `Observer.BlocklistEntries`, `ceph.OSDFlag`,
its constants, `Observer.OSDFlags`, `ceph.FullRatios`,
`ceph.FullRatioSnapshot`, `Observer.FullRatios`, `ceph.ConfigSetting`,
`ceph.ConfigEntry`, `Observer.Configuration`, `ceph.MessengerPolicy`,
`ceph.MessengerConnectionMode`, `ceph.MGRModuleState`,
`Observer.MGRModules` and `Container.WaitMGRModuleReady` stay in `ceph`: they
only read cluster state. The package imports `ceph`; `ceph` never imports it.

## Handle contract

- The function returns a non-nil handle for every outcome, so the handle can be
  deferred or registered for cleanup before the error is looked at.
- A call that is refused or never applied returns a handle that owns nothing.
  Its `Restore` returns nil, takes no lock, never reports a restoration and
  leaves the cluster and the registry alone. The reason is the returned error.
- A call whose outcome is uncertain (the command may have been applied, for
  example after a lost reply or a canceled context) returns a real handle
  together with the error. Its `Restore` still removes the entry it may have
  created.
- A copy of a handle shares its state, and `Restore` is retryable with a fresh
  context after an uncertain failure.
- The zero value of a handle, and a nil handle, are unavailable: `Restore`
  returns an error.
- `Terminate` does not undo an override. The change ends with the cluster, and a
  `Restore` after `Terminate` is refused with the error of an operation on a
  terminated cluster.

Every function admits the cluster as a method of `ceph.Container` does. A
cluster that a `SharedCluster` lends to tenants refuses the call with
`ceph.ErrSharedCluster` and `OperationError.Op` set to the function name, such as
`override.AddBlocklist`, `override.SetFullRatios`, `override.SetMGRModule`,
`override.SetOSDFlag`, `override.SetDebugLevel`, `override.SetConfig` or
`override.SetMessengerPolicy`; a cluster lent to one exclusive lease accepts it.
`Tenant` has no forwarder. A nil cluster returns a not-applied handle and an
error classified `ceph.ErrInvalidRequest`.

## AddBlocklist

```golang
func AddBlocklist(ctx context.Context, c *ceph.Container, address string, duration time.Duration) (*override.BlocklistHandle, error)
```

Fences one specific client session for `duration` (at least one second). The
address must be the session's exact `IP:port/nonce` with a nonzero nonce, so
other clients on the same host are unaffected. Get it from the client after it
connects (librados `rados_getaddrs`, go-ceph `Conn.GetAddrs`), from
[ObjectWatchers](ceph.md#objectwatchers), from an RBD lock owner or from a CephFS
session. A v1/v2 address vector is accepted only when all entries denote the same
IP, port and nonce; protocol types become native `ANY` entries. Bare IPs, CIDR
ranges, nonce zero and existing entries are refused, and an existing entry is
never renewed or adopted.

The verb is `Add` because the call creates one entry that did not exist, and
`Restore` removes only that entry.

| Outcome | Handle | Error |
| --- | --- | --- |
| Entry added and read back | Real | nil |
| Nil cluster | Not applied | `ErrInvalidRequest`, `Op` `override.AddBlocklist` |
| Shared cluster | Not applied | `ErrSharedCluster`, `Op` `override.AddBlocklist` |
| Invalid address | Not applied | `ErrInvalidRequest` |
| Duration below one second | Not applied | plain error |
| Terminated cluster or no control container | Not applied | the cluster's own error |
| This session is already held by another handle | Not applied | plain error |
| The native blocklist cannot be read before the add | Not applied, reservation released | the native error |
| The session already has a native entry | Not applied, reservation released | `ErrInvalidRequest` |
| The add or the readback after it fails | Real, uncertain | the joined errors |
| The add succeeds but no entry is stored | Real, uncertain | plain error |

## BlocklistHandle

| Method | Meaning |
| --- | --- |
| `Address() string` | The exact session that the handle fences, as canonicalized (`IP:port/nonce`). A call refused before the address was accepted returns the address as passed. |
| `Restore(ctx) error` | Removes only this entry. |

`Restore` checks the expiration captured at the add before it removes anything:
an entry renewed by someone else is refused with `ErrInvalidRequest`, and an
entry that is gone (natural expiry) counts as removed. An unknown readback at the
add cannot authorize the removal of a present entry; inspect the native state or
terminate the disposable cluster. A lost removal reply is retried: after
confirmed absence, copies are no-ops. After removal OSDs admit the address again,
but how the session recovers depends on the client library; with librados both
the existing session and a new connection recover. External blocklist edits must
not race the helper.

```golang
// address is the client's own session address, e.g. from rados_getaddrs or
// go-ceph Conn.GetAddrs: "172.19.0.6:0/3461995546".
fence, err := override.AddBlocklist(ctx, cluster, address, time.Minute)
// fence is never nil: after a refusal its Restore does nothing.
defer fence.Restore(context.Background())
if err != nil {
	t.Fatal(err)
}
// The fenced session's operations fail (ESHUTDOWN in librados); another session
// from the same host keeps working.
if err := fence.Restore(ctx); err != nil {
	t.Fatal(err)
}
```

`ceph.Observer.BlocklistEntries` shows the entry and its expiry. For RBD
exclusive-lock handover, the lock owner's address comes from `rbd.ImageClients`
(see [rbd](rbd.md)).

## SetOSDFlag

```golang
func SetOSDFlag(ctx context.Context, c *ceph.Container, flag ceph.OSDFlag, enabled bool) (*override.OSDFlagHandle, error)
```

Sets or clears one global OSDMap flag and remembers its prior state. Supported
flags are the eleven `ceph.OSDFlag` constants: `OSDFlagNoUp`, `OSDFlagNoDown`,
`OSDFlagNoOut`, `OSDFlagNoIn`, `OSDFlagNoBackfill`, `OSDFlagNoRebalance`,
`OSDFlagNoRecover`, `OSDFlagNoScrub`, `OSDFlagNoDeepScrub`, `OSDFlagNoSnapTrim`
and `OSDFlagNoAutoscale`. Any other value, including an untyped string constant
that names another flag, is refused with an error before Ceph is contacted. The
composite `pause` flag and irreversible compatibility flags are not supported.
Overlapping handles for one flag are refused. External edits to the same flag
must not race the helper: a boolean flag has no generation and Ceph provides no
compare-and-set. While set, flags such as `noout` raise the `OSDMAP_FLAGS`
health warning (per-OSD flags raise `OSD_FLAGS` instead).

| Outcome | Handle | Error |
| --- | --- | --- |
| Flag set or cleared and read back | Real | nil |
| Flag already in the requested state | Real, a lease without a write | nil |
| Nil cluster | Not applied | `ErrInvalidRequest`, `Op` `override.SetOSDFlag` |
| Shared cluster | Not applied | `ErrSharedCluster`, `Op` `override.SetOSDFlag` |
| Unsupported flag | Not applied | plain error |
| Terminated cluster or no control container | Not applied | the cluster's own error |
| This flag is already held by another handle | Not applied | plain error |
| The native OSDMap cannot be read before the write | Not applied, reservation released | the native error |
| The write or the readback after it fails | Real, uncertain | the error |

## OSDFlagHandle

| Method | Meaning |
| --- | --- |
| `Restore(ctx) error` | Returns only this flag to its original state. |

`Restore` never touches other flags. A flag that was already set stays set. It
is idempotent and reconciles a lost restoration reply before another native
mutation. A lease without a write refuses an outside change instead of
overwriting it: when the flag no longer has the state the call left, the restore
fails with `ErrInvalidRequest`. Outside writes that are indistinguishable from the
applied or prior state cannot be detected. A restore after `Terminate` is refused
with the error of an operation on a terminated cluster.

```golang
hold, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoOut, true)
// hold is never nil: after a refusal its Restore does nothing.
defer hold.Restore(context.Background())
if err != nil {
	t.Fatal(err)
}
```

## SetMGRModule

```golang
func SetMGRModule(ctx context.Context, c *ceph.Container, name string, enabled bool) (*override.MGRModuleHandle, error)
```

Enables or disables one manager module and restores its original membership with
`Restore`. The verb is `Set` because the call replaces the module's enabled state
and `Restore` puts the previous state back. The name must match a Ceph module
name (lower-case letters, digits and underscores, starting with a letter). The
call refuses:

- to enable a module whose metadata is missing or reports that it cannot run
  (it never forces dependencies);
- to disable an always-on module, and to enable one that was force-disabled,
  because restoring it would need a force-disable. An enabled always-on module
  accepts only a no-op `true` request;
- to disable `volumes` while configured or owned CephFS filesystems, or native
  filesystems, use it, and to disable `volumes` or `mirroring` while a native
  mirror policy exists. This protection runs before the module is disabled;
- a module that another handle holds.

A module change can restart the active manager; the call waits for a coherent
module snapshot before it reads and before it writes, and reconciles the result
with a fresh bounded read. It verifies membership, not command readiness: use
[WaitMGRModuleReady](ceph.md#waitmgrmoduleready) or probe the module with your
intended command afterwards. Ceph has no compare-and-swap for module membership,
so external edits must not race the call or `Restore`.

| Outcome | Handle | Error |
| --- | --- | --- |
| Module changed and read back | Real | nil |
| Module already in the requested state | Real (a lease with no write) | nil |
| Nil cluster | Not applied | `ErrInvalidRequest`, `Op` `override.SetMGRModule` |
| Shared cluster | Not applied | `ErrSharedCluster`, `Op` `override.SetMGRModule` |
| Invalid module name | Not applied | plain error `invalid Ceph manager module name` |
| Terminated cluster or no control container | Not applied | the cluster's own error |
| This module is already held by another handle | Not applied | plain error |
| The module snapshot cannot be read | Not applied, reservation released | the native error or the context error |
| Module missing, cannot run, always-on rule or protection refuses | Not applied, reservation released | plain error |
| The change or the readback after it fails | Real, uncertain | the joined errors |

## MGRModuleHandle

| Method | Meaning |
| --- | --- |
| `Restore(ctx) error` | Returns only this module to its original membership. |

`Restore` is idempotent and reconciles an uncertain reply before it writes again.
It checks the module's dependency metadata before it re-enables the module and
the current filesystem and mirror usage before it disables it. A module that
changed outside the override, an always-on policy that changed, or a no-op lease
whose module was edited is refused and nothing is overwritten. After a refused
or failed `Restore` the handle stays registered and can be retried; a `Restore`
after `Terminate` is refused.

```golang
module, err := override.SetMGRModule(ctx, cluster, "prometheus", true)
// module is never nil: after a refusal its Restore does nothing.
defer module.Restore(context.Background())
if err != nil {
	t.Fatal(err)
}
// Membership is verified, not readiness: wait for the module's own service
// (for prometheus, Observer.ManagerServices) before you use it.
```

## SetFullRatios

```golang
func SetFullRatios(ctx context.Context, c *ceph.Container, requested ceph.FullRatios) (*override.FullRatiosHandle, error)
```

Sets the global `nearfull`, `backfillfull` and `full` ratios of the OSDMap and
restores all three with `Restore`. The verb is `Set` because the call replaces
existing values and `Restore` puts the captured tuple back. This is a cluster
policy override, even when low thresholds are used to induce FULL or ENOSPC in a
test. It has no expiry, timer or implicit scope-exit restoration. Cancelling the
apply context does not undo committed changes, and `Terminate` does not reset
individual thresholds.

Requested values must be finite and satisfy `0 < NearFull < BackfillFull < Full
<= 1` after float32 normalization (inputs that collapse to equal values are
refused), and the current map must also be ordered. The CLI changes one ratio per
command; the helper orders the commands so that every intermediate state stays
ordered, so apply and restore are not atomic. Only one handle per cluster is
allowed, including no-op requests. The control container and the original
bootstrap identity are captured once per apply or restore; the request deadline
is the cluster's startup timeout and is set before the wait for the topology lock.

Low ratios reproduce full conditions with a small amount of existing data; the
module does not fill real devices. Success verifies the map, not OSD reports:
wait for `OSD_NEARFULL`, `OSD_BACKFILLFULL` or `OSD_FULL` with
[HealthDetails](ceph.md#healthdetails). See
[Fill the cluster with low full ratios](ceph.md#fill-the-cluster-with-low-full-ratios).

| Outcome | Handle | Error |
| --- | --- | --- |
| All three thresholds applied and read back | Real | nil |
| Nil cluster | Not applied | `ErrInvalidRequest`, `Op` `override.SetFullRatios` |
| Shared cluster | Not applied | `ErrSharedCluster`, `Op` `override.SetFullRatios` |
| Context already ended, or ended while waiting for the topology lock | Not applied | the context error |
| Invalid, unordered or float32-collapsed ratios | Not applied | `ErrInvalidRequest` |
| This cluster already holds a full ratios override | Not applied | plain error |
| Terminated cluster, no control container or unconfirmed bootstrap identity | Not applied, reservation released | the cluster's own error |
| The native map cannot be read, or the original is not ordered | Not applied, reservation released | the native error or a plain error |
| Any failure from the first native step on (command, readback, canceled context) | Real, uncertain, tracked before the first step | the error |

## FullRatiosHandle

| Method | Meaning |
| --- | --- |
| `Restore(ctx) error` | Returns all three thresholds to their captured values. |

`Restore` is idempotent and reconciles uncertain native replies before another
mutation. A different current tuple is refused rather than overwriting outside
edits: only the latest owned tuple, or the two sides of one uncertain command,
are accepted, and older apply prefixes do not authorize restoration. A changed
bootstrap identity or a map epoch that went backwards is refused too. Unrelated
flags, pool policies and central configuration are not changed. A copy shares the
state of its handle and a lost reply can be retried with a fresh context. A
handle of a call that was not applied returns nil without taking a lock. A
`Restore` after `Terminate` is refused and the override stays registered.

```golang
change, err := override.SetFullRatios(ctx, cluster, ceph.FullRatios{
	NearFull: 0.0001, BackfillFull: 0.0002, Full: 0.0003,
})
// change is never nil: after a refusal its Restore does nothing.
defer change.Restore(context.Background())
if err != nil {
	t.Fatal(err)
}
// Wait for OSD_FULL in HealthDetails, exercise the client, then:
if err := change.Restore(ctx); err != nil {
	t.Fatal(err)
}
```

## SetDebugLevel

```golang
func SetDebugLevel(ctx context.Context, c *ceph.Container, setting ceph.DebugLevelSetting) (*override.DebugLevelHandle, error)
```

0.x: old names are removed without aliases.

`ceph.DebugLevelSetting`, `ceph.DebugLevelReceipt`, `ceph.DebugLevelState` and
`ceph.DebugLevelTransition` stay in `ceph` because the failure manifest copies
the receipt; see [Debug level types](ceph.md#debug-level-types).

Opens one runtime debug level window on one running OSD that the cluster owns.
`ceph.DebugLevelSetting` has `Daemon` (such as `osd.0`), `Name` (`debug_ms` or
`debug_osd`) and `Level` (`N` or `N/M`, from 0 to 99). Other roles and options
are refused; neither verbose levels nor all daemons are ever selected
automatically.

The window changes the daemon's runtime value through its admin socket, as
[AdminSocket](ceph.md#adminsocket) reaches it, and never the central
configuration database; `override.SetConfig` is
the stored-entry model. A daemon restart drops a runtime value. The call reads
the effective prior level, sets the requested level and verifies the normalized
readback, so `20` reads back as `20/20`. The window is bound to the OSD
container's generation (container ID and start time). Only one window per daemon
and option is admitted at a time; the registry that admits it stays in the
cluster and has a lock of its own, so a restoration does not wait behind a long
topology operation. Each set is issued only while the OSD still runs that
generation, and the cluster's own topology operations, such as
`OSDContainer.Remove`, `OSDContainer.Detach` and `DetachedOSD.Reopen`, wait until
the set has ended.

| Outcome | Handle | Error |
| --- | --- | --- |
| Window applied and read back | Real, `applied-verified` | nil |
| Nil cluster | Not applied | `ErrInvalidRequest`, `Op` `override.SetDebugLevel` |
| Shared cluster | Not applied | `ErrSharedCluster`, `Op` `override.SetDebugLevel` |
| Context already ended | Not applied | the context error |
| Option or level not supported | Not applied | `ErrInvalidRequest` |
| Daemon is not an OSD such as `osd.0` | Not applied | `ErrInvalidRequest` |
| OSD unknown, purged, being purged or detaching | Not applied | `ErrInvalidRequest` |
| Terminated cluster | Not applied | the error of an operation on a terminated cluster |
| A window for this daemon and option is open | Not applied | `ErrInvalidRequest` |
| The daemon is not an owned running one, or the prior level cannot be read | Not applied | the native or inspection error |
| The generation changed before the set was issued | Not applied | the refusal |
| The set, its settling or its readback fails or differs | Real, `apply-uncertain` | the joined errors |
| The generation changed during the set | Real, `target-changed` | the joined errors |

A refused or never-applied call releases its registry slot and returns a handle
whose `Receipt` has the state `not-applied`, the request as given and the
refusal as `Reason`; a real handle never reports `not-applied`. A set or readback
that fails returns a real handle with the error, because a lost reply can still
have applied. Readbacks after a set use a fresh context bounded by
[WithStartupTimeout](ceph.md#withstartuptimeout).

## DebugLevelHandle

| Method | Meaning |
| --- | --- |
| `Receipt() ceph.DebugLevelReceipt` | A copy of the window's record; it reads only local state and never queries the daemon. A nil or zero-value handle returns a receipt without a state. |
| `Restore(ctx) error` | Restores the prior level as described below. A not-applied handle returns nil without taking a lock. |
| `String() string`, `GoString() string` | The daemon and option without their values. |

`Restore` first checks the same generation and that the daemon still shows the
window's own value, then sets the prior level and verifies it:

- A prior level that is already in place counts as restored.
- Any other value is an outside edit; it is preserved, not overwritten.
- A stopped, restarted, replaced or removed OSD, or a terminated cluster, is not
  restored: the window's process is gone, and the window ends as
  `target-changed`.
- A paused OSD, an unavailable socket or an unverified readback leaves the
  window open; a later `Restore` reconciles it first.
- A set of the window whose end could not be confirmed may still apply. The
  window keeps it open as unconfirmed, and a later `Restore` first confirms that
  the set has stopped before it reads, sets or releases anything. Until then
  `OSDContainer.Remove` and `OSDContainer.Detach` first stop that set or refuse,
  so it cannot reach a replacement OSD through the shared socket path.

The generation is checked again after each read, so a replacement during a read
is reported as a changed target instead of a level to restore. Restored, conflict
and target-changed are terminal and release the admission; `Restore` then
returns nil or the same error again.

With [TerminateOnCleanup](ceph.md#terminateoncleanup), register the removal of
clients first and the restoration afterward. The cleanups run in reverse order
after the failure archive, so a known failure is archived while the level is
still raised, and the level is restored before the clients are removed. A
restoration that is the first failure triggers the `after-cleanup` archive, and
Terminate still runs:

```golang
window, err := override.SetDebugLevel(ctx, cluster, ceph.DebugLevelSetting{
	Daemon: "osd.0", Name: "debug_osd", Level: "20",
})
// window is never nil, whatever err says.
finalization.TrackDebugLevel(window) // optional: receipts in the failure manifest
finalization.Cleanup("restore osd.0 debug_osd", window.Restore)
if err != nil {
	t.Fatal(err)
}
```

The readbacks around each set are not an atomic compare-and-swap. Do not change
the same daemon and option elsewhere while a window is open; an outside write of
the window's own value cannot be told apart from it. A restart of the OSD
container from outside the cluster after the last check is not held off. Verbose
levels cost log volume and can change timing; levels and durations for
particular investigations are not established yet.

## SetConfig

```golang
func SetConfig(ctx context.Context, c *ceph.Container, setting ceph.ConfigSetting) (*override.ConfigHandle, error)
```

Sets one entry in the MON configuration database (`ceph config set`) and records
the native normalized value. `ceph.ConfigSetting` has `Section`, `Mask`, `Name`
and `Value`:

- `Section` is `global`, a daemon type (`mon`, `mgr`, `osd`, `mds`, `client`) or
  one identity such as `osd.0` or `mds.a`.
- `Mask` is optional: `class:ssd`, `host:node-a` or `host:node-a/class:ssd`.
- `Name` can be any option, including MGR module keys such as
  `mgr/volumes/max_concurrent_clones` or `mgr/prometheus/server_addr`.

The verb is `Set` because the call replaces a value that may already exist and
`Restore` puts the previous entry back, or removes the entry if it was absent,
instead of pinning the inherited or default value. Overlapping handles for the
same section, mask and name are refused. Never race external writes to the key
with the apply or the restore: Ceph has no compare-and-set for configuration.

The helper verifies the stored value, not its effect:

- Local `ceph.conf` entries (including
  [WithConfigFile](ceph.md#withconfigfile)), command-line arguments and runtime
  overrides take precedence over the database.
- Some options need a daemon restart; for example `osd_op_queue` only applies when
  an OSD starts.
- Check the effective value of a running daemon with
  `cluster.Ceph(ctx, "config", "show", "osd.0", "osd_max_backfills")`.
- With [MessengerV2Secure](ceph.md#withmessengermode), the Messenger mode and bind
  options are refused; with `MessengerV2Only`, the `ms_bind_msgr1` and
  `ms_bind_msgr2` options. The refusal does not depend on the section or mask.

Configuration output can contain secrets; log only selected fields. Typical uses:
lowering thresholds such as `osd_op_complaint_time` (when `SLOW_OPS` is
reported), enabling Messenger fault injection (`ms_blackhole_client`,
`ms_inject_delay_*`, see [Fault injection](guides/fault-injection.md)), and
preparing OSDs before they start.

| Outcome | Handle | Error |
| --- | --- | --- |
| Entry written and read back | Real | nil |
| The stored value already equals the request | Real, a lease with no write | nil |
| Nil cluster | Not applied | `ErrInvalidRequest`, `Op` `override.SetConfig` |
| Shared cluster | Not applied | `ErrSharedCluster`, `Op` `override.SetConfig` |
| Context already ended, or ended while waiting for the topology lock | Not applied | the context error |
| Invalid section, mask, name or value | Not applied | `ErrInvalidRequest` |
| Option fixed by the `MessengerV2Secure` or `MessengerV2Only` bootstrap | Not applied | `ErrInvalidRequest` |
| Terminated cluster or no control container | Not applied | the cluster's own error |
| This cluster already holds an override of the same section, mask and name | Not applied | plain error |
| The configuration cannot be read before the write | Not applied, reservation released | the native error |
| `config set` or the readback fails | Real, uncertain | the joined errors |
| Success without a stored entry | Real | plain error |

## ConfigHandle

| Method | Meaning |
| --- | --- |
| `Restore(ctx) error` | Returns the exact entry to its prior value, or removes it if it was absent. |
| `String() string`, `GoString() string` | The section and option, never the value, for logging. |

`Restore` refuses a different current value to preserve outside edits, and after
an uncertain reply a retry reconciles the native state first. If the readback of
the apply was unavailable, `Restore` refuses to adopt an unknown normalized value
and asks for an inspection of the native state. A copy shares the state of its
handle. A handle of a call that was not applied returns nil without taking a
lock. A `Restore` after `Terminate` is refused and the override stays registered.

```golang
change, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{
	Section: "osd", Name: "osd_max_backfills", Value: "1",
})
// change is never nil, and a failed apply can still have persisted: schedule
// the restore before looking at the error.
defer func() {
	if err := change.Restore(context.Background()); err != nil {
		t.Error(err)
	}
}()
if err != nil {
	t.Fatal(err)
}
```

## SetMessengerPolicy

```golang
func SetMessengerPolicy(ctx context.Context, c *ceph.Container, policy ceph.MessengerPolicy) (*override.MessengerPolicyHandle, error)
```

Changes the connection modes that the running daemons accept from clients (see
[WithMessengerPolicy](ceph.md#withmessengerpolicy) for the fields). Each non-nil
field becomes a [SetConfig](#setconfig) override of its option; nil fields stay as
they are. The call is admitted once under its own name and then sets the options
in the order monitors, managers, OSDs.

- The daemons apply the change to new connections without a restart.
  Established connections keep their mode.
- A policy that selects no valid mode, and any policy on a `MessengerV2Secure`
  cluster, is refused with `ceph.ErrInvalidRequest` before any change.
- When the first option is refused, the handle is not applied and holds no
  option. When a later option is refused, or an option's outcome is uncertain,
  the handle holds the options that were made (a refused option is not among
  them) together with the error, and must be restored.
- A single daemon, such as only `osd.0`, is changed with [SetConfig](#setconfig)
  and the option of the policy, with the daemon name as `Section`. It takes
  precedence over the daemon type's entry.

## MessengerPolicyHandle

| Method | Meaning |
| --- | --- |
| `Restore(ctx) error` | Returns every option to its previous value, in reverse order. |

`Restore` is idempotent and can be retried after a failure; the errors of the
options are joined. A handle of a call that was not applied returns nil.

```golang
policy, err := override.SetMessengerPolicy(ctx, cluster, ceph.MessengerPolicy{
	Monitors: []ceph.MessengerConnectionMode{ceph.MessengerCRC},
})
defer policy.Restore(ctx) // a no-op after a refusal
if err != nil {
	t.Fatal(err)
}
```
