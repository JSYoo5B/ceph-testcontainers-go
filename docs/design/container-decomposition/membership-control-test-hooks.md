# Membership/control test-hook pointers

Moved without rewriting from the section "Ninth internal slice: membership/control test-hook pointers" of [container-decomposition.md](../container-decomposition.md) when the decomposition records became one file per component (issue #216, owner decision of 2026-10-28). The ordinal is historical: statements below that cite another slice by its number use the [historical ordinals](../container-decomposition.md#historical-ordinals) of the shared document.

0.x: old names are removed without aliases. Inspected at `4293511` (main after
#269); citations are at the branch head. This is the first safe step of the
membership/control group named in the audit of the eighth slice: it removes the
raw pointers that `serviceHooks.TestState` (`ceph/service_hooks.go:466`)
published to other packages. It moves no field, adds no component type and
changes no lock. Exported API and production behavior are unchanged.

## What was exposed

`TestState` returned pointers into the cluster. Those that reached the
membership/control group or its options were:

| Pointer | Field | Writers outside the package (tests) |
| --- | --- | --- |
| `Closed` | `closed` | 23 sites in `cephfs`, `fault`, `override`, `rgw`, `internal/overridehook` |
| `ControlPlane` | `controlPlane` | 2 (`cephfs`, `override`) |
| `InitialOSDs`, `PoolReplicas`, `PoolMinSize`, `StartupTimeout`, `CephFSImage`, `RGWImage` | `settings.*` | 8; `RGWImage` had no user |

All uses were plain writes made while a test held `Mutex`, or on a cluster value
that was not shared. No caller read through a pointer.

## What replaced them

Six hooks on `serviceHooks`, mirrored by functions in `internal/servicehook`
(the pattern of `TestReplaceOSDs` and `TestOSDCount` from the OSD registry
slice):

- `TestSetClosed(c, closed)`
- `TestSetControlPlane(c, control)`
- `TestSetInitialOSDs(c, n)`
- `TestSetPoolDefaults(c, replicas, minSize)`
- `TestSetStartupTimeout(c, d)`
- `TestSetCephFSImage(c, image)`

Each is one assignment, makes no call and takes no lock. Its doc comment names
the lock the caller holds (`Mutex` for the closed flag and the options,
`ControlMutex` for the control plane) or that the cluster value is not shared.
`TestState` keeps the three lock identities, `Config`, `Keyring`, `CephVersion`,
`NetworkRemoved` and the `PortLeases` count. The unused `RGWImage` pointer was
dropped. Every caller in the tests was migrated, including the lock-then-write
sequences (the lock stays in the test, the write is the hook).

## Why a hook per value and not a typed component

The audit showed that `closed` and `controlPlane` are written by a handful of
functions under three different locks (`mu`, `controlMu`, and the unshared
builder), so a component type would either need those locks or lie about them.
The hooks are the narrowest seam that keeps each write under the lock domain it
already has. A component type for the group stays a later step, after the
`closed` and `monitorTerminated` readers are audited.

## Guards, in `ceph/membership_control_test.go`

- `TestTestStateExposesNoMembershipControlPointer`: the exact field set of
  `testState` and of `servicehook.TestStateView` is pinned, so a new pointer
  needs a review.
- `TestMembershipControlStateIsNeverAddressed`: no production function takes
  the address of `closed`, `controlPlane`, `settings`, `monitorTerminated`,
  `manager`, `monitors`, `managers`, `services` or `extensions`, or of anything
  under them.
- `TestClosedAndControlPlaneWritersAreAudited`: the writers of `closed` and
  `controlPlane` are pinned by function and field (`terminate`, `TestCluster`,
  `ensureControlPlane` with `controlMu`, and the two hooks); a new writer or a
  stale row fails.
- `TestTestSetHooksAreSingleWritesWithoutCalls`: the hook set, the fields each
  assigns, no call, and the documented lock contract.
- `TestTestSetHooksAreCalledOnlyFromTests`: the cross-package caller table. No
  non-test file in the module calls a `servicehook.TestSet*` function, so the
  hooks cannot become a second production writer.
- `TestClusterFactsRecordedSourcesAreNotRewrittenOutsideStartup` lists the four
  settings hooks as the only writers of `c.settings` after `Run`. Only
  `TestSetCephFSImage` writes a field `FactsOf` reads (`cephfsOverride`); the
  others are listed because the guard treats all of `settings` as recorded.

Negative controls, each failing against the deliberately broken version and
passing after restoring it: the `Closed` pointer put back in both views, the
address of `c.closed` taken, the address of `c.settings.osds` taken, a new
`c.closed` writer in `ensureControlPlane`, a lock call in a hook, a hook writing
a second field, a hook doc without the contract, an unpinned `TestSet*` hook, a
non-test file calling `servicehook.TestSetClosed`, and a mutex added to
`portLeaseState`. Putting the pointer back in only the `ceph` view panics at
package initialization instead (the two views must be the same type), which is
also a failure.

## The mutex count is now eight

`TestNativeStateAddsNoMutex` listed the component types by name and left out
`debugLevelRegistry`, whose own `mu` (`ceph/debug_level.go:136`) made the real
total eight while the test expected seven. The test now includes
`debugLevelRegistry` and expects eight: `mu`, `controlMu`, `configMu`,
`extensionsMu`, `native.mu`, `toolkit.mu`, `toolkit.publishMu` and the
debug-level registry's `mu`. This slice adds no mutex and changes no lock order;
the number changed only because the test now counts what exists. Removing the
type from the list again makes the test fail (negative control).

## Limits of the guards

The guards are syntactic and follow names inside the package. The writer table
judges receivers by the identifiers the package uses for a cluster (`c`,
`cluster`, `owner`). Callers of the hooks in other packages are scanned only for
non-test use; their lock discipline is documentation. No Docker or native Ceph
behavior was run.

## Not covered by this slice

The group is not moved. The three lock identities (`Mutex`, `ControlMutex`,
`ConfigMutex`) are still published as pointers, because the lock queueing tests
need the lock itself and a different seam would be a lock-order review.
`Config`, `Keyring` and `CephVersion` (bootstrap record, `config` under
`configMu`) and `NetworkRemoved` (network component) are still pointers and
belong to their own slices. Also not covered: `monitorTerminated`, `monitors`,
`managers`, `services`, `extensions` and `sharing`, a typed component for the
group, the fault and debug-level registries as components, any lock change, and
any exported API.
