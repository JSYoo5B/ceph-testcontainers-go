# Native-command state

Moved without rewriting from the section "Second internal slice: native-command state" of [container-decomposition.md](../container-decomposition.md) when the decomposition records became one file per component (issue #216, owner decision of 2026-10-28). The ordinal is historical: statements below that cite another slice by its number use the [historical ordinals](../container-decomposition.md#historical-ordinals) of the shared document.

Inspected at `134a752` (main after #239). The second component moved out of
`Container` is the native-command state, held as `Container.nativeCmds` of the
unexported type `nativeState` (`ceph/native_state.go`): the tracked invocations
(`nativeInvocations`), whether Terminate closed admission (`nativeClosed`), the
fenced OSD instances (`nativeFenced`) and the cancel function of a client-image
startup (`clientStartupCancel`). The lock moves with them: `nativeMu` becomes
`nativeState.mu`. All `Container` methods, their signatures and their behavior
are unchanged.

Why this component next, and why no new mutex:

- The table above names no order after networking. Of the remaining
  components, this one has a single, already documented lock: every
  production access to the five fields, all in `ceph/native_command.go`,
  `ceph/images.go` and `ceph/debug_level.go`, happened inside a
  `nativeMu` critical section, and `ceph/ceph.go` only declared them. Client
  and auth uses `clientMu`, `clientPublishMu` and maps whose guards were not
  audited for this note, and OSD storage is tied to the OSD lifecycle under
  `Container.mu`; both stay for later slices. Moving `nativeMu` with its
  fields keeps one lock for the same data, so the number of mutexes stays the
  same and no lock is added to the order.
- `nativeMu` is already a leaf lock. Every critical section is a few map or
  flag operations (`ceph/native_command.go:430-447`, `469-475`, `522-540`,
  `664-668`, `697-708`, `738-740`, `ceph/images.go:151-153`, `164-172`,
  `194-196`, `ceph/debug_level.go:646-669`, `678-680`); cancellations and
  stops run after the unlock. The component's methods preserve that and take
  `mu` exactly once.

Lock-order rules preserved, unchanged by this move:

- `terminate` calls `cancelNativeInvocations` (`ceph/ceph.go:1015`) before it
  waits for `c.mu` (`ceph/ceph.go:1016`), because an invocation's caller may
  hold `c.mu`. It calls `stopNativeInvocations` twice with `c.mu` held
  (`ceph/ceph.go:1031`, `1149`); `nativeMu` is below `c.mu` there.
- `fenceNativeTarget` is called from OSD removal (`ceph/ceph.go:813`,
  `ceph/osd_retained.go:239`) under the topology lock and takes `nativeMu`
  inside it. The client startup takes `clientMu` and then `nativeMu`
  (`ceph/images.go:146-151`). Nothing takes another lock while holding
  `nativeMu`, so the order `c.mu` or `clientMu`, then `nativeMu` is the only
  one that exists.
- Admission checks stay in one critical section and in the same order for
  the three callers: closed, then fenced, then (for `RunNativeCommand` with an
  input) the one-shot input claim. The refusals and their error text are the
  callers' own. An owned shell has no target instance, which no fence can
  name, so the shared fence check never refuses it.
- An unconfirmed end keeps the invocation registered with its marker, also
  when the marker is empty, as before.

Guards, in `ceph/native_state_test.go`:

- `TestNativeStateFieldsAreOnlyTouchedByItsMethods`: outside the type,
  `Container.nativeCmds` appears only as the receiver of a `nativeState`
  method, so no code touches a field or the lock directly.
- `TestNativeStateMethodsHoldOnlyTheLeafLock`: each method takes `n.mu`
  exactly once and calls nothing else while it runs but the claim hook,
  builtins and two collection helpers; the method set is pinned.
- `TestNativeStateAddsNoMutex`: `Container`, `nativeState` and `networkState`
  declare the same seven mutexes as before the split.
- `TestTerminateCancelsNativeInvocationsBeforeTheTopologyLock`.
- Behavior tests for the admission order, the unconfirmed end, the close and
  fence results, and a concurrent run under `-race`.

The `FactsOf` write-site guard is untouched: no field moved here is read by
`FactsOf`.

Not covered by this slice: client and auth, OSD storage, overrides and faults,
test seams and membership and control; any change to which lock guards what;
any new lock; the order in which `Terminate` closes the other components; the
`sharedBackend` port that #155 needs; and any exported API.
