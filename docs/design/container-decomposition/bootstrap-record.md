# Bootstrap record

Moved without rewriting from the section "Twelfth internal slice: bootstrap record" of [container-decomposition.md](../container-decomposition.md) when the decomposition records became one file per component (issue #216, owner decision of 2026-10-28). The ordinal is historical: statements below that cite another slice by its number use the [historical ordinals](../container-decomposition.md#historical-ordinals) of the shared document.

0.x: old names are removed without aliases. Inspected at `2f1abd4` (main after
#274); path:line citations are at the branch head unless they name another
revision. Two independent changes in one slice. First, the last raw pointers
that `serviceHooks.TestState` published for the bootstrap record are replaced
by setters, as #270 did for the closed flag, the control plane and the options.
Second, `Container.config`, `Container.keyring` and `Container.cephVersion`
move into a `bootstrapRecord` (`ceph/bootstrap_record.go:35`, field
`ceph/ceph.go:53`) that also owns the read-write lock that guards the first
two, the former `Container.configMu`. Exported API and behavior are unchanged.

## Fresh audit of what is left of the membership and control group

Counts are textual uses by field name in non-test files of package `ceph`
before this change, so they overstate real call sites.

| Field(s) | Lock domain | Verdict |
| --- | --- | --- |
| `config`, `keyring` with `configMu`; `cephVersion` | `configMu` for the two byte slices; `cephVersion` has no lock (written once in `Run`, read lock-free by `FactsOf`) | Picked. The lock guards only these two fields, every reader is a method-sized section that calls no other lock, and the three pointers in `TestState` were the last raw pointers into the group |
| `extensions` with `extensionsMu` | its own leaf lock | Not picked. A generic keyed store (`extensionsWith`), six uses in two files; independent of this slice and the next candidate that needs no `mu` argument |
| `closed`, `monitorTerminated` | `mu` | Not picked, for the reasons given in the eleventh slice: a flag type would add a method per read in about a dozen functions and `closed` is read by the shared-gate admission |
| `manager`, `monitors`, `managers`, `services` | `mu` | Not picked. The maps hold daemon handles that several functions rewrite in place and `sharedDaemons` and `terminate` iterate; the callers are not yet pinned |
| `sharing` | atomic | Not picked. One atomic owned by the gate |
| `settings` | none after `Run` | Not picked. Immutable options read in 37 files |
| `keyTypeOption`, `firstMonitorImageID`, `owner` | none, written once in `Run` | Not picked for this component. They are bootstrap facts but have no lock and no `TestState` pointer, so putting them in the record would widen it without a lock-domain reason |

The earlier audit (ninth slice) listed `config`, `keyring`, `cephVersion`,
`keyTypeOption`, `firstMonitorImageID` and `owner` as one "bootstrap record" and
called it unclean because of the published pointers and unverified readers.
The pointers are now gone and the readers were all checked below, so the
subset that shares a lock domain is a clean move. The three fields that have no
lock stay where they are.

## Part 1: setters instead of pointers

`TestState` and `servicehook.TestStateView` no longer have `Config`, `Keyring`
and `CephVersion`. Their field set is now `ConfigMutex`, `ControlMutex`,
`Mutex`, `NetworkRemoved` and `PortLeases`. Three hooks replace them, mirrored
by functions in `internal/servicehook/testaccess.go`:

- `TestSetConfig(c, config)` and `TestSetKeyring(c, keyring)`: one assignment
  each, the caller holds `ConfigMutex` or the cluster value is not shared.
- `TestSetCephVersion(c, version)`: one assignment, the caller holds `Mutex` or
  the cluster value is not shared (the field has no lock of its own).

Every caller was migrated: 20 setter calls replace the pointer writes in `override`, `cephfs`, `rgw/multisite`,
`rbd/mirror` and `internal/overridehook`. All were plain writes; none read through a
pointer. `NetworkRemoved` is still a pointer, because it belongs to the network
component.

## Part 2: what moved

| Before | After |
| --- | --- |
| `Container.configMu`, `config`, `keyring`, `cephVersion` | `Container.bootstrap` (a `bootstrapRecord`) with `mu`, `config`, `keyring`, `cephVersion` |
| `Run`: `c.config, err = readFile(...)`, `c.keyring, err = ...` | `bootstrap.recordConfig`, `recordKeyring` (no lock, before the cluster is shared) |
| `recordCephVersion`: `c.cephVersion = version` | `bootstrap.recordVersion` |
| `adminClient`: `configMu.RLock`, clone both, `RUnlock` | `bootstrap.clones()` |
| `currentMonitorBootstrapAddresses`: lock, clone config, keyring present, unlock | `bootstrap.templateAndConfirmed()` |
| `newOSDLaunchRecipe`: lock, strings of both, unlock | `bootstrap.launchStrings()` |
| `refreshMonitorConfigAfterRemoval`: read lock and clone; write lock and assign | `bootstrap.template()`, `bootstrap.publishConfig(...)` |
| `configureDefaultCRUSHRoot`: write lock, rewrite, assign on success, unlock | `bootstrap.applyDefaultCRUSHRule(value)` |
| `HealthDetails`, `fullRatiosSession`, `poolPGs`, `poolUsage`, `ClusterIdentity`: `lockTopologyReadMutex(ctx, &configMu)`, FSID, keyring present, unlock | `bootstrap.identity(ctx)` returning a `bootstrapIdentity` |
| `ConnectionConfig`: bounded read lock, completeness check, clone both | `bootstrap.connection(ctx)` |
| `diagnosticsSnapshot`: `diagnosticLock(ctx, configMu.TryRLock)`, scan the keyring, unlock | `bootstrap.diagnosticKeyring(ctx)` (a copy) |
| `TestState.ConfigMutex: &c.configMu` | `c.bootstrap.mutex()` (the same lock, now inside the component) |

Readers that never took `configMu` keep reading without a lock, now through
methods whose name says so: `rawConfig`, `rawKeyring`, `rawVersion`,
`hasConfig`, `isComplete`. Their callers are `createClient`,
`withClientIdentity`, `createSharedClient`, `startService` and `AddMonitor`, each
after it took the topology lock (`lockTopology`, `ceph/topology.go:383`, or
`mu.Lock` directly), and the lock-free `staticFacts` that `FactsOf` reads. Making
those reads locked would be a lock-order change, so none was.

One incidental behavior is preserved on purpose: `Run` stored the result of
`readFile` before checking its error, and `recordConfig`/`recordKeyring` are
called before the check as well.

## Why no new mutex and no lock-order change

`configMu` is the lock that moved; its type (`sync.RWMutex`), its holders and its
position are unchanged. It is a leaf below `Container.mu`: `adminClient`
(`ceph/ceph.go:471`, inside `startService` with `mu` held),
`currentMonitorBootstrapAddresses` (`ceph/client_monitor_config.go:87`, "called
under c.mu"), `newOSDLaunchRecipe` (`ceph/osd_launch.go:37`, "the caller holds
c.mu"), `refreshMonitorConfigAfterRemoval` (`ceph/topology.go:425,439`) and
`ConnectionConfig` (`ceph/topology_snapshot_context.go:29`, after the
`lockTopology` call there) take it with `mu` held. `diagnosticsSnapshot`
(`ceph/diagnostics.go:283`), the health and pool readers and `ClusterIdentity`
(`ceph/service_hooks.go:352`) take it after releasing `mu`, and
`configureDefaultCRUSHRoot` (`ceph/placement.go:138`) takes it during startup.
No method of the type takes another lock or calls into `Container`: the only
calls out are the pure helpers `monitorConfigClusterIdentity` and
`defaultCRUSHRuleConfig` and the two bounded-lock helpers, so a holder of the
record lock never waits for `Container.mu` or any other lock. `cephVersion` is
in the component but outside its lock; it is written only in `Run` and read by
`staticFacts`, which the recorded-sources guard in
`cluster_facts_termination_test.go` already pins.

## Mutex count unchanged

`TestNativeStateAddsNoMutex` scans `bootstrapRecord` as a component and pins
`bootstrapRecord.mu` where it pinned `Container.configMu`: still eight mutexes
(`Container.mu`, `extensionsMu`, `controlPlaneState.mu`, `bootstrapRecord.mu`,
`nativeState.mu`, `featureClientState.mu`, `featureClientState.publishMu`,
`debugLevelRegistry.mu`). Adding a second mutex to the component fails it
(negative control).

## Guards, in `ceph/bootstrap_record_test.go`

- `TestContainerDeclaresNoBootstrapFieldsOrLock`: `Container` no longer
  declares `config`, `keyring`, `cephVersion` or `configMu`, and the record has
  exactly its four fields.
- `TestBootstrapRecordFieldIsOnlyTouchedByItsMethods`: outside the type,
  `Container.bootstrap` is the receiver of a method call, or (the three
  `TestSet*` hooks only) the owner of a field the hook assigns; no `Container`
  literal in production sets it.
- `TestBootstrapRecordMethodsMakeOnlyTheirPinnedCalls`: the method set and the
  calls of each are pinned, no method names `Container`, `closed`,
  `controlPlane` or `settings`, or another mutex.
- `TestBootstrapRecordWritersAreAudited`: the writers of the three fields are
  the three startup recorders (no lock), `applyDefaultCRUSHRule` and
  `publishConfig` (both take the write lock) and the three hooks (docs name the
  lock contract).
- `TestBootstrapRecordCallersAreAudited`: the caller table with the guard kind
  of each (`free`, `takes`, `startup`, `lockfree`, `identity`); a `takes` caller
  takes the topology lock before its first use, the single `lockfree` caller is
  `staticFacts`, the single caller of `mutex()` is `TestState`, and a stale or
  new row fails.
- The existing guards were updated, not weakened: the `TestState` field set and
  the hook table (`membership_control_test.go`), `bootstrap` in the no-address
  list, the mutex owner table, and the recorded-sources guard, which now flags
  both a direct write of `bootstrap.cephVersion` and a call of
  `bootstrap.recordVersion` outside `Run`, `recordCephVersion` and
  `TestSetCephVersion`.
- Behavior: an empty record is inert and incomplete; the locked readers return
  copies; `identity` separates the FSID, the identity error and the keyring
  presence; a failed `applyDefaultCRUSHRule` leaves the record unchanged; the
  three bounded readers return the context error while the write lock is held;
  concurrent readers against a writer under `-race`; `TestState` publishes the
  component's own lock.

Negative controls, each failing against the deliberately broken version and
passing after restoring it: a second mutex in the component, `publishConfig`
without the lock, a direct field read in `adminClient`, an extra call in
`template`, a method that names `Container`, a second caller of `mutex()`, an
unlisted caller, a `takes` caller that uses the record before the topology lock,
a `Container` literal that sets `bootstrap`, a hook doc without the contract, the
`Config` pointer put back in the view, `configMu` declared again on `Container`,
a `clones` that returns the stored slices, an `identity` that ignores its
context, `recordVersion` called from `staticFacts`, `bootstrap.cephVersion`
assigned directly, the address of `bootstrap.config` taken, and a lock identity
that is a copy.

## Limits of the guards

The guards are syntactic and follow names inside the package; the receiver
identifiers `c`, `cluster` and `owner` stand for a cluster. The `takes` check
proves the order of two calls in a function body, not that the topology lock is
still held at the use. The cross-package setters are scanned only for non-test
callers by the existing caller table; their lock discipline is documentation. No
Docker or native Ceph behavior was run for this slice.

## Not covered by this slice

The rest of membership and control stays on `Container`: `mu`, `extensionsMu`,
`manager`, `monitors`, `managers`, `services`, `closed`, `monitorTerminated`,
`extensions`, `sharing`, `settings`, `firstMonitorImageID`, `owner` and
`keyTypeOption`. Also not covered: the `Mutex`, `ControlMutex` and `ConfigMutex`
pointers in `TestState`, `NetworkRemoved` (network component), any change to which
lock guards what, any new lock and any exported API.
