# Post-termination diagnostics: representation comparison

Design note for the `ceph.Container` size reduction (Refs #146). It compares
two ways to give callers diagnostic data after a cluster has been terminated.
It does not choose between them, and it defines no public type, function name
or lifecycle-state API. Every signature below is a placeholder.

## Principles already agreed

1. Diagnostic information stays accessible after cluster termination.
2. Diagnostic data gives no way to control or reactivate the cluster.
3. Unavailable information, failed collection and omissions are explicit.
   Absence of retained evidence never implies a clean outcome.

Earlier owner decisions stay in force: static facts are an independent copy
with exported fields (slices, maps and nested references cloned), a nil
cluster returns an error and the zero value, and a new static-facts snapshot
can be obtained from a non-nil terminated cluster.

## What exists today

Read from the source at the revision this note was written against; nothing
was run.

| Data | Where | State after a successful `Terminate` |
| --- | --- | --- |
| Static facts (address family, essential image, image set, messenger mode, network names, host network, public address, Ceph version) | Ten accessors on `Container`; internal helpers in `ceph/facts.go` take no lock, run no command and make no Docker call. No snapshot type exists yet. | Retained. `settings`, `cephVersion` and the network pointers are never cleared by `terminate`; `EssentialImage` documents that it stays available. |
| Tracked container handles | `CollectDiagnostics` (`ceph/diagnostics.go`) reads them under `mu` | Gone. `terminate` deletes each OSD, MON, MGR and service entry once its removal succeeds, so a later report has little to inspect. |
| Container state, logs, network inspect, Ceph queries | `collectDiagnostics`, Docker inspect and log reads, fixed Ceph CLI queries | Destroyed with the containers and networks. They cannot be re-read. |
| Crash metadata | `CollectCrashMetadata` (`ceph/crash_metadata.go`) | Destroyed with the container; its own doc says a removed container takes its records along. |
| Redaction inputs | Client secrets, keyring, service secrets read by `diagnosticsSnapshot` | Not a diagnostic to return. A later report would need the same redaction, so the inputs must be handled with the data. |

Existing reports already show the explicitness style the principles ask for.
`DiagnosticArtifact` carries a per-observation `Error` and `Truncated`;
`DiagnosticsReport.Complete` is only true when every requested observation
succeeded; the `container-limit` artifact records containers omitted by
`MaxContainers`; `CrashMetadataContainer` has an `Outcome` and an `Omitted`
list. `finalizationNotes` states that nothing was collected after termination
and that no crash record is not proof that no daemon crashed.

## Capture boundaries

The representation choice does not move these boundaries.

- Native and runtime information (container state, logs, Ceph queries, crash
  metadata, network inspect) exists only until teardown. It must be collected
  before `terminate` removes the containers, and `terminate` starts by setting
  `closed`, undoing faults and removing the containers in the same call. A
  diagnostic object created afterwards cannot recover it. A separate object
  alone therefore guarantees nothing about whether it was collected.
- Retained static metadata can be copied at any time, including after
  termination, because the helpers in `ceph/facts.go` do not read live state.
- Any collection that must precede teardown needs an explicit trigger: a caller
  call before `Terminate`, a `Finalization` step (the existing
  `finalizationManifest` already records collector outcomes), or a new automatic
  step inside termination. The last one is a larger feature and is not decided
  here.

## Option A: independent diagnostic snapshot returned separately

A package function takes the cluster and returns a self-contained value, in
the same style as the planned facts function.

- Available data: the static-facts snapshot, plus whatever native reports the
  caller collected earlier and attached (for example a `DiagnosticsReport`).
- After termination: only the static part can be created fresh. Native parts
  exist only if they were collected before and kept by the caller or by a
  finalization step.
- Explicitness: each part needs its own state, such as collected, failed,
  not collected or unavailable after termination, so that an empty native part
  is never read as healthy. A failed collection carries its error text, in the
  style of `DiagnosticArtifact.Error`.
- Control: the value holds copies and strings only, with no `Container`,
  Docker handle, context or lock. Nothing in it can start, stop or reconfigure
  anything.
- Cost: one new public type and one new package function, no new `Container`
  method. The caller must remember to collect before teardown and to keep the
  result.

## Option B: obtain the snapshot through the existing handle

The existing `*Container` handle yields the same independent snapshot, for
example by a method on it. The handle remains the only lifecycle owner.

- Available data: the same as Option A. The handle can also hold what the
  cluster itself captured before or during teardown, so a post-termination call
  can return native evidence that the caller never collected, if such a capture
  is added.
- After termination: the handle still exists, so the call can run. It must
  return a static snapshot and the retained native record without touching
  destroyed Docker objects, and it must not use the `closed` admission that
  refuses other operations.
- Explicitness: the same per-part states. Because the handle can hold a
  capture record, "collected during teardown" and "never collected" can be
  told apart, which Option A can do only if the caller supplies the record.
- Control: the returned value carries no handle, but the call itself is made on
  a control handle. The method must be read-only and must not publish the handle
  or any mutable cluster state into the value.
- Cost: adds to the surface that #146 is trying to shrink (a new `Container`
  method), and keeps diagnostic retention state inside the type whose size is
  being reduced. That conflicts with the rule that no new accessor is added for
  an extracted group, unless the owner makes an exception.

## Comparison

| Question | Option A: separate snapshot | Option B: through the handle |
| --- | --- | --- |
| Static facts after termination | Yes, by value | Yes, by value |
| Native evidence after termination | Only if the caller kept it | Only if the cluster kept a capture record, which does not exist today |
| Guarantees that native data was collected | No | No by itself; possible only with an added capture step |
| Distinguishes "never collected" from "collected, nothing found" | Needs a field and the caller's cooperation | Needs a field; the handle can supply it |
| Control or reactivation through the data | None | None in the value; the entry point is a handle method, so it must be read-only |
| Effect on `Container` method count | None | Increases it |
| Retention state added to `Container` | None | Likely |

## Closure versus cleanup completion

The internal `closed` flag is set at the start of `terminate`, before any
resource is removed. The call can then fail partway: removals that fail stay
in the maps, and `monitorTerminated`, `networkRemoved` and
`clusterNetworkRemoved` are separate flags set only after the matching removal.
A retry of `Terminate` is possible after a partial failure. Both existing
reports copy the flag as `Closed`, and `finalizationArchive` has
`ClusterClosed`. None of those values means every owned resource is gone.

So a diagnostic record must keep two facts apart:

- object usability: whether the handle still accepts operations (`closed`);
- owned-resource cleanup: per resource family, whether removal was attempted,
  succeeded, failed with an error, or was not attempted.

Reporting only the first would let a partially failed teardown look clean. The
second needs its own recorded outcome, derived from the errors `terminate`
already joins and from the removal flags, and must not be inferred from `closed`.
Which states to expose is left to the reviewed contract.

## Not decided

- Option A or B, any type or function name, and any field set.
- A lifecycle-state API, or whether a state is public at all.
- Whether termination collects anything automatically, and with what bounds.
- How an unavailable part is spelled (field, enum or error), beyond the
  requirement that it is explicit.
- Redaction policy for retained native data.
- Synchronization of the static capture against a concurrent `Terminate`.
- Tenant and `SharedCluster` behavior; `CollectDiagnostics` is `sharedOpen`
  today and keeps that gate until a reviewed decision says otherwise.

## What a later implementation would need to prove

- The snapshot is independent: mutating its slices, maps and nested values
  changes neither the cluster nor another snapshot, and it stays readable after
  `Terminate`.
- Capture of the static part is race-free against a concurrent `Terminate`.
- A nil cluster returns an error and the zero value; a non-nil terminated
  cluster returns the recorded facts and does not claim that resources exist.
- Native evidence is collected before destruction on every teardown path
  covered by the contract, including a failed or canceled `Terminate`, and a
  collection that never ran is distinguishable from one that found nothing.
- A failed, truncated, timed-out or omitted collection is reported as such, and
  a complete-looking empty report is impossible when collection did not run.
- After a partially failed `Terminate`, the record shows closed and
  per-resource cleanup outcome separately.
- The value exposes no handle, lock, credential, executor or unredacted secret,
  and no function reachable from it can start, stop or reconfigure a cluster.
- The public API inventory, package pages, CHANGELOG and upgrade mapping are
  updated together, and the work is checked on the exact implementation head.

## Limits of this note

Source reading only. No Docker, native Ceph or timing behavior was run, and no
claim here is native evidence. This revision adds no product code, test or
public API.
