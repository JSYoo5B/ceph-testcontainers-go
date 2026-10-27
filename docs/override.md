# Reversible overrides

Import `github.com/JSYoo5B/ceph-testcontainers-go/override` alongside `ceph`.
The package changes cluster-wide state of a cluster created by `ceph.Run` for a
while and puts it back. Each operation is a function that takes the cluster and
returns a handle; the handle owns the change and its `Restore` undoes only what
the call did. This page documents the client blocklist entry and the runtime
debug level window. The other override kinds are still methods of
`ceph.Container` and move here in separate changes (see the [design](design/override-package-boundary.md)).

0.x: old names are removed without aliases. `Container.TemporaryBlocklist` and
`ceph.BlocklistOverride` no longer exist in `ceph`:

| Previous call or type | New call or type |
| --- | --- |
| `cluster.TemporaryBlocklist(ctx, address, duration)` | `override.AddBlocklist(ctx, cluster, address, duration)` |
| `*ceph.BlocklistOverride` | `*override.BlocklistHandle` (same `Address` and `Restore`) |

`ceph.BlocklistEntry` and `Observer.BlocklistEntries` stay in `ceph`: they only
read the OSDMap. The package imports `ceph`; `ceph` never imports it. See the
[migration guide](guides/upgrading-override-package.md).

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
`override.AddBlocklist`; a cluster lent to one exclusive lease accepts it.
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

## SetDebugLevel

```golang
func SetDebugLevel(ctx context.Context, c *ceph.Container, setting ceph.DebugLevelSetting) (*override.DebugLevelHandle, error)
```

0.x: old names are removed without aliases. `Container.OverrideDebugLevel` and
`ceph.DebugLevelOverrideHandle` no longer exist in `ceph`:

| Previous call or type | New call or type |
| --- | --- |
| `cluster.OverrideDebugLevel(ctx, setting)` | `override.SetDebugLevel(ctx, cluster, setting)` |
| `*ceph.DebugLevelOverrideHandle` | `*override.DebugLevelHandle` (same `Receipt`, `Restore`, `String`, `GoString`) |

`ceph.DebugLevelSetting`, `ceph.DebugLevelReceipt`, `ceph.DebugLevelState` and
`ceph.DebugLevelTransition` stay in `ceph` because the failure manifest copies
the receipt; see [Debug level types](ceph.md#debug-level-types) and the
[migration guide](guides/upgrading-override-debug-level.md).

Opens one runtime debug level window on one running OSD that the cluster owns.
`ceph.DebugLevelSetting` has `Daemon` (such as `osd.0`), `Name` (`debug_ms` or
`debug_osd`) and `Level` (`N` or `N/M`, from 0 to 99). Other roles and options
are refused; neither verbose levels nor all daemons are ever selected
automatically.

The window changes the daemon's runtime value through its admin socket, as
[AdminSocket](ceph.md#adminsocket) reaches it, and never the central
configuration database; `override.SetConfig` and `Container.OverrideConfig` are
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
