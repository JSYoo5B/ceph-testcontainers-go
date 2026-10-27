# Reversible overrides

Import `github.com/JSYoo5B/ceph-testcontainers-go/override` alongside `ceph`.
The package changes cluster-wide state of a cluster created by `ceph.Run` for a
while and puts it back. Each operation is a function that takes the cluster and
returns a handle; the handle owns the change and its `Restore` undoes only what
the call did. This page documents the kinds that have moved: the client
blocklist entry and the MGR module state. The other override kinds are still
methods of `ceph.Container` and move here in separate changes (see the
[design](design/override-package-boundary.md)).

0.x: old names are removed without aliases. `Container.TemporaryBlocklist`,
`ceph.BlocklistOverride`, `Container.OverrideMGRModule` and
`ceph.MGRModuleOverrideHandle` no longer exist in `ceph`:

| Previous call or type | New call or type |
| --- | --- |
| `cluster.TemporaryBlocklist(ctx, address, duration)` | `override.AddBlocklist(ctx, cluster, address, duration)` |
| `*ceph.BlocklistOverride` | `*override.BlocklistHandle` (same `Address` and `Restore`) |
| `cluster.OverrideMGRModule(ctx, name, enabled)` | `override.SetMGRModule(ctx, cluster, name, enabled)` |
| `*ceph.MGRModuleOverrideHandle` | `*override.MGRModuleHandle` (same `Restore`) |

`ceph.BlocklistEntry`, `Observer.BlocklistEntries`, `ceph.MGRModuleState`,
`Observer.MGRModules` and `Container.WaitMGRModuleReady` stay in `ceph`: they
only read cluster state. The package imports `ceph`; `ceph` never imports it. See
the migration guides for the [blocklist](guides/upgrading-override-package.md)
and the [MGR module](guides/upgrading-override-mgr-module.md).

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
`override.AddBlocklist` or `override.SetMGRModule`; a cluster lent to one exclusive lease accepts it.
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
