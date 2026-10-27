# Reversible overrides

Import `github.com/JSYoo5B/ceph-testcontainers-go/override` alongside `ceph`.
The package changes cluster-wide state of a cluster created by `ceph.Run` for a
while and puts it back. Each operation is a function that takes the cluster and
returns a handle; the handle owns the change and its `Restore` undoes only what
the call did. This page documents four operations: the client blocklist entry,
the OSDMap full ratios, one central configuration entry and the messenger
policy. The other override kinds are still methods of
`ceph.Container` and move here in separate changes (see the
[design](design/override-package-boundary.md)).

0.x: old names are removed without aliases. `Container.TemporaryBlocklist`,
`Container.OverrideFullRatios`, `Container.OverrideConfig`,
`Container.OverrideMessengerPolicy` and their handle types no longer exist in
`ceph`:

| Previous call or type | New call or type |
| --- | --- |
| `cluster.TemporaryBlocklist(ctx, address, duration)` | `override.AddBlocklist(ctx, cluster, address, duration)` |
| `*ceph.BlocklistOverride` | `*override.BlocklistHandle` (same `Address` and `Restore`) |
| `cluster.OverrideFullRatios(ctx, ratios)` | `override.SetFullRatios(ctx, cluster, ratios)` |
| `*ceph.FullRatiosOverride` | `*override.FullRatiosHandle` (same `Restore`) |
| `cluster.OverrideConfig(ctx, setting)` | `override.SetConfig(ctx, cluster, setting)` |
| `*ceph.ConfigOverrideHandle` | `*override.ConfigHandle` (same `Restore`, `String` and `GoString`) |
| `cluster.OverrideMessengerPolicy(ctx, policy)` | `override.SetMessengerPolicy(ctx, cluster, policy)` |
| `*ceph.MessengerPolicyOverrideHandle` | `*override.MessengerPolicyHandle` (same `Restore`) |

`ceph.BlocklistEntry`, `Observer.BlocklistEntries`, `ceph.FullRatios`,
`ceph.FullRatioSnapshot`, `Observer.FullRatios`, `ceph.ConfigSetting`,
`ceph.ConfigEntry`, `Observer.Configuration`, `ceph.MessengerPolicy` and
`ceph.MessengerConnectionMode` stay in `ceph`: they only describe or read
cluster state, or configure a cluster at `Run`. The package imports `ceph`; `ceph` never imports
it. See the [migration guide](guides/upgrading-override-package.md).

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
`override.AddBlocklist`, `override.SetFullRatios`, `override.SetConfig` or
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
