# Startup identity

Moved without rewriting from the section "Fifteenth internal slice: startup identity" of [container-decomposition.md](../container-decomposition.md) when the decomposition records became one file per component (issue #216, owner decision of 2026-10-28). The ordinal is historical: statements below that cite another slice by its number use the [historical ordinals](../container-decomposition.md#historical-ordinals) of the shared document.

0.x: old names are removed without aliases. Inspected at `eb933e7` (main after
#280); path:line citations are at the branch head. `Container.owner`,
`Container.keyTypeOption`, `Container.firstMonitorImageID` and the test seam
`Container.ownedContainerDocker` move into a `startupIdentity`
(`ceph/startup_identity.go`, field `Container.startup`, `ceph/ceph.go:63`). The
other two test seams and the remaining membership fields get a recorded
disposition below. Exported API and behavior are unchanged. The slice
overlaps `ceph/ceph.go` (the `Container` struct and `Run`) with the member
roster slice (#281); the textual conflict is expected and left to the owner.

## Disposition of the remaining startup fields and test seams

Counts are textual uses by field name in non-test files of package `ceph`
before this change.

| Field | Lock domain | Verdict |
| --- | --- | --- |
| `owner` | none: set by the composite literal in `Run` (`ceph/ceph.go:193`) | Moved. Its only users are the label (`ownedLabel`, 12 call sites in 7 files), the labeled-container removal, and three reads that copy it into a record (`ceph/osd_launch.go:45`, `ceph/verification.go:559`, `ceph/finalization.go:124`) |
| `keyTypeOption` | none: written once in `Run` (`ceph/ceph.go:235`) | Moved. One reader, `generateKeyArgs` (`ceph/auth_config.go:183,187`) |
| `firstMonitorImageID` | none: written once in `Run` (`ceph/ceph.go:239`) | Moved. Two readers that name it in an error (`ceph/auth_config.go:184`, `ceph/dual_stack_admission.go:39`) |
| `ownedContainerDocker` (test seam) | none: set by tests before the cluster starts | Moved. Its one reader is the labeled-container removal, which uses nothing else of `Container` but `owner`; the seam travels with the dependency it replaces |
| `keyRotationDocker` (test seam) | none | Stays on `Container`. One reader, `keyRotationExec` (`ceph/daemon_key_rotation.go:707`, called from `:717,751`). The rotation code is a set of `Container` methods that call `authRecords`, `bootstrap` and the control plane, not a component; moving the seam alone would create a one-field type whose owner has no state of its own |
| `daemonRunner` (test seam) | none | Stays on `Container`. One reader, `runDaemon` (`ceph/daemon_image.go:79`), called by `AddMonitor` and `startManager`. It replaces container creation for the membership functions, which stay whole on `Container` (the roster slice and the lock domains of `mu` are separate reviews) |
| `settings` | none after `Run` | Stays. Immutable options read across the package; a component would add an accessor per option and hide nothing (unchanged from the twelfth and thirteenth slices) |
| `sharing` | atomic | Stays. One atomic already owned by `sharedGate` by name (unchanged) |

## What moved

| Before | After |
| --- | --- |
| `Container.owner`, `keyTypeOption`, `firstMonitorImageID`, `ownedContainerDocker` | `Container.startup` (a `startupIdentity`) |
| `Container.ownedLabel()` | `startupIdentity.ownedLabel()` |
| `Container.removeLabeledContainers(ctx)` | `startupIdentity.removeLabeledContainers(ctx)` (`ceph/ceph.go:1053` is its one caller) |
| `Run`: `c.keyTypeOption, err = detect...`; `c.firstMonitorImageID = ...` | `recordKeyTypeOption` before the error check, as the field was assigned before it, then `recordFirstMonitorImage` |
| reads of the three fields | `ownerID()`, `supportsKeyType()`, `firstMonitorImage()` |

The old names are deleted; there are no aliases or wrappers. The test callers
were migrated (`ceph/owned_containers_test.go`, `ceph/verification_test.go`,
`ceph/finalization_test.go`, `ceph/osd_launch_test.go` and others).

## Why no lock, no new mutex and no lock-order change

The type has no mutex and `Container.mu` was not given a new job. Every fact is
written while the cluster value belongs to the goroutine that runs `Run`: the
owner in the literal at `ceph/ceph.go:193`, the other two after the first MON
started (`ceph/ceph.go:235,239`) and before any other daemon is started or the
value is returned. A cluster that fails part way is returned with what was
recorded, so `Terminate` can still list by the owner ID. After `Run` returns,
the fields are read only. No method of the type calls into `Container` or takes
a lock, so the type adds no edge to the lock order. The one place that runs
under a lock is `terminate`: `startup.removeLabeledContainers` is called with
`Container.mu` held (`ceph/ceph.go:1053`, after `lockTopology` at `:963`), as
`Container.removeLabeledContainers` was, and it takes no lock itself. Its
position in Terminate (after the OSD storage release and the control plane,
before the storage retry and the run directory) is pinned by the existing
order guards, which were changed only where they matched the full call
expression (`ceph/control_plane_state_test.go`).

## Mutex count unchanged

`TestNativeStateAddsNoMutex` now scans `startupIdentity` as a component. The
count stays at eight (`Container.mu`, `extensionStore.mu`,
`controlPlaneState.mu`, `bootstrapRecord.mu`, `nativeState.mu`,
`featureClientState.mu`, `featureClientState.publishMu`,
`debugLevelRegistry.mu`); a mutex in the new type fails it.

## Guards, in `ceph/startup_identity_test.go`

- `TestContainerDeclaresNoStartupIdentityFields`: the four fields are gone from
  `Container`, and the component has exactly those four members.
- `TestStartupIdentityFieldIsOnlyTouchedByItsMethods`: outside the type,
  `Container.startup` is only the receiver of a component method; no member is
  read, assigned or addressed; the only production `Container` literal that
  sets it is `Run`'s; no literal sets a removed field.
- `TestStartupIdentityMethodsMakeOnlyTheirPinnedCalls`: the method set and the
  calls of each are pinned; no method names `Container`, `closed`,
  `controlPlane`, `settings` or `bootstrap`, or touches a mutex; a field is
  assigned only by its recorder.
- `TestStartupIdentityCallersAreAudited`: the caller table (function and the
  methods it calls); the two recorders are called exactly once each, by `Run`;
  a new or stale row fails.
- `TestStartupIdentitySourceIsInTheRightFile`.
- Behavior: a zero identity is inert and never reaches the Docker seam; each
  recorder changes only its own fact; the label carries the owner ID and two
  owners differ; the removal lists by its own label only and reports a client
  failure.

Negative controls, each failing against the deliberately broken version and
passing after restoring it: a mutex in the component, `owner` declared again on
`Container`, a method that names `Container`, a recorder called outside `Run`,
a direct read of `Container.startup.keyTypeOption`, an unlisted caller, a
second production `Container` literal that sets `startup`, and a recorder that
drops its write.

## Limits of the guards

The guards are syntactic and follow names inside the package; the receiver
identifiers `c`, `cluster` and `owner` stand for a cluster. "Written before the
cluster is shared" is argued above and pinned only as far as the recorders have
one caller, `Run`; no guard proves that `Run` publishes the value after the
writes. No Docker or native Ceph behavior was run for this slice.

## Not covered by this slice

The rest of membership and control stays on `Container`: `mu`, `manager`,
`monitors`, `managers`, `services`, `closed`, `monitorTerminated`, `sharing`,
`settings`, and the test seams `keyRotationDocker` and `daemonRunner`. Also not
covered: any change to which lock guards what, any new lock, the member roster
(#281) and any exported API.
