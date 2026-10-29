# Disposition of the remaining fields

> Current boundary: the shared-cluster/tenant harness and its core-only policy,
> lease and hook plumbing have been removed. Harness references and source
> locations below are historical design evidence, not retained APIs or future
> extraction requirements. See [independent fixtures](../../guides/remove-shared-harness.md).

Moved without rewriting from the section "Sixteenth internal slice: disposition of the remaining fields" of [container-decomposition.md](../container-decomposition.md) when the decomposition records became one file per component (issue #216, owner decision of 2026-10-28). The ordinal is historical: statements below that cite another slice by its number use the [historical ordinals](../container-decomposition.md#historical-ordinals) of the shared document.

0.x: old names are removed without aliases. This slice removes no name and moves
no field. Inspected at `5b32b38` (main after #282); path:line citations are at
the branch head. After the fifteenth slice every field of `Container` is either
a component with its own type and guards, or has a recorded reason to stay. The
reconciliation is pinned by `ceph/container_residual_fields_test.go`, so a new
field cannot be added without a verdict. The slice changes no production code,
no lock, no lock order and no exported API.

## Measurement, as information

Lexical AST counts of the non-test files of package `ceph`; promoted Docker
methods are excluded. They are reference numbers, not a target (owner decision
of 2026-10-27).

| Measure | Value |
| --- | --- |
| Exported / unexported methods declared on `Container` | 39 / 186 |
| Named fields / embedded field | 22 / 1 |
| Mutexes declared by `Container` and its state components | 8, unchanged (`TestNativeStateAddsNoMutex`) |

## Verdict per field

Uses are selector uses of the field through a `c` or `cluster` receiver in
non-test files, with the number of files in parentheses. They overstate real
call sites.

| Field | Lock domain | Uses | Verdict |
| --- | --- | --- | --- |
| `testcontainers.Container` | `mu` | | Stays. The first MON and the public embedded Docker surface; it cannot move into an unexported component. Shared-cluster refusals of its methods stay with `Container` |
| `mu` | the lock | 109 (40) | Stays. The cluster mutex (`ceph/ceph.go:43`) guards the roster, the OSD registry, the fault registry, the OSD storage, the port leases and the two flags; every lock-order citation in this note is measured from it |
| `controlPlane` | own leaf lock | 10 (7) | Owner: `controlPlaneState` (eleventh slice) |
| `toolkit` | own locks | 14 (3) | Owner: `featureClientState` (third slice) |
| `bootstrap` | own leaf lock | 28 (17) | Owner: `bootstrapRecord` (twelfth slice) |
| `authRecords` | `mu` | 38 (6) | Owner: `authState` (fourth slice) |
| `startup` | none, written before sharing | 22 (12) | Owner: `startupIdentity` (fifteenth slice) |
| `roster` | `mu` | 53 (10) | Owner: `memberRoster` (fourteenth slice) |
| `extensions` | own leaf lock | 2 (2) | Owner: `extensionStore` (thirteenth slice) |
| `nets` | `mu` and its own fields | 22 (6) | Owner: `networkState` (first slice) |
| `osdHandles` | `mu` | 38 (17) | Owner: `osdRegistry` (seventh slice) |
| `stores` | `mu` | 17 (5) | Owner: `osdStorageState` (fifth slice) |
| `monStores` | `mu` | not counted | Owner: `monStoreState` (MON store volumes, see [Cluster-owned MON data volumes](../mon-volume-storage.md)); added after the inspection above. No production path publishes a record yet; `terminate` calls its two release stages |
| `deviceClaims` | `mu` | not counted | Owner: `osdDeviceClaims` (cluster-wide claim table of OSD block devices, see [OSD device launch seam](../osd-device-launch-seam.md)); added after the inspection above. No production path publishes a claim; the unexported wiring step `registerBlockDeviceOSD` has no production caller, and `removeOSD` and `terminate` call the release hooks; see [OSD device claims](osd-device-claims.md) |
| `runDir` | per field | 12 (7) | Owner: `runDirState` (sixth slice) |
| `portLeases` | `mu` | 5 (4) | Owner: `portLeaseState` (eighth slice) |
| `nativeCmds` | own leaf lock | 14 (3) | Owner: `nativeState` (second slice) |
| `faults` | `mu` | 13 (6) | Owner: `faultRegistry` (tenth slice) |
| `debugLevels` | own leaf lock | 1 (1) | Owner: `debugLevelRegistry` (tenth slice) |
| `settings` | none after `Run` | 141 (37) | Stays. Immutable options read across the package; a component would add an accessor per option and hide nothing |
| `sharing` | atomic | 4 (3) | Stays. One `atomic.Int32` that `sharedGate` already owns by name (`ceph/shared_gate.go:252,273`) |
| `closed` | `mu` | 44 (27) | Stays, see below |
| `monitorTerminated` | `mu` | 10 (6) | Stays, see below |
| `keyRotationDocker` | none | 2 (1) | Stays. Test seam with one reader, `keyRotationExec` (`ceph/daemon_key_rotation.go:707`); the rotation code is a set of `Container` methods, so a component would be a one-field type without state of its own |
| `daemonRunner` | none | 2 (1) | Stays. Test seam with one reader, `runDaemon` (`ceph/daemon_image.go:79`), for the membership functions that stay whole on `Container` |

## Why `closed` and `monitorTerminated` stay

Both are plain booleans that `mu` guards. No new lock is needed to move them,
so the question is only whether a component would own something. It would not.

- `closed` is assigned once, at the top of `terminate`, after `lockTopology`
  (`ceph/ceph.go:950,957`). It is the admission fact: the entry points read it
  in the same critical section as the state they are about to change (for
  example `ceph/topology.go:146,258`), and `faultRegistry` is told its value by
  the caller instead of reading it (`ceph/fault_registry.go:166`). A component that owned it would have to be
  called by all of them, which is the dependency the eleventh slice avoided.
- `monitorTerminated` records one fact about the embedded first MON: whether
  `Terminate` or `Remove` already took it away. It is set in two places, both
  under `mu` (`ceph/ceph.go:1024` in `terminate`, `ceph/topology.go:335` in
  `removeMonitor`, whose lock is taken at `:254`), and is never cleared. Eight
  methods read it, in the pattern "the first MON stands" (`c.Container != nil
  && !c.monitorTerminated`). A component would have to own the embedded
  container to keep the fact and its subject together, and the embedded
  container is part of the public type. A predicate method that wraps the five
  repeats would move no state and is not an ownership change, so it is not made
  here.

## Guards, in `ceph/container_residual_fields_test.go`

- `TestContainerResidualFieldsAreAllDispositioned`: the field set of
  `Container` (including the embedded field) equals the verdict ledger; `mu`
  is the only `sync.Mutex`, `sharing` the only atomic, the two flags are
  `bool`, and no other field is a sync primitive.
- `TestContainerResidualFieldsAreInTheDesignNote`: the table above names
  exactly the ledger's fields in its first column.
- `TestLifecycleFlagsHaveAuditedWritersAndReaders`: the functions that assign
  `closed` (`terminate`) and `monitorTerminated` (`terminate`,
  `removeMonitor`) are pinned, the readers of `monitorTerminated` are a caller
  table, and the only `Container` literal that names a flag is the unit
  constructor in `ceph/unit_access.go`.
- Scanner and drift controls: `TestContainerResidualFieldCheckRejectsDrift`,
  `TestResidualDocFieldsReadsOnlyTheSixteenthSection` and
  `TestLifecycleAccessScannerSeesWritersReadersAndLiterals`.

`TestNativeStateAddsNoMutex` is unchanged and still counts eight mutexes; no
component was added, so its owner list does not change.

## The lock identities that `UnitState` publishes

`unitAccess.UnitState` (`ceph/unit_access.go:58`) returns `&c.mu` and the
control-plane and bootstrap locks (`ceph/control_plane_state.go:103`,
`ceph/bootstrap_record.go:168`). They are not fields of `Container`. The
lock-queueing tests of `override`, `rgw` and `internal/clustertest` hold them to
prove that an entry point waits instead of racing (for example
`override/full_ratios_test.go:264-270`, `rgw/entry_test.go:104`). Replacing them
with a callback would move the same lock handling into the hook without
removing the pointer from the process, and the test-support isolation they
belong to is #248. They stay, as the thirteenth slice recorded.

## What the owner still decides

- For #155 (public harness packages): the exported names and signatures of
  `ceph/harness` and the three service harnesses, the cross-package
  registration slot (it waited for the membership and control split, which is
  now merged), what replaces the test-only `TenantCluster` access in the final
  import graph, and the extraction-specific native acceptance. This note makes
  none of those decisions.
- For the original acceptance of #146: nothing in this note adds a criterion.
  The remaining items are reconciliation of validation already recorded on the
  merged pull requests and the final privacy review, which are the owner's.
- For #248: whether the three lock identities and the four nil-default seams
  (`keyRotationDocker`, `daemonRunner`, `stores` seams and the
  `startupIdentity` hook) are kept inside the test-only boundary. Ordinary
  untagged internal support is allowed, so this is not a question of leaving
  the ordinary build.

## Limits of the guards

The guards are syntactic. The receiver names `c` and `cluster` stand for a
cluster, a writer through another name is not seen, and `closed` readers are not
enumerated (44 uses by design). They do not prove that every writer holds `mu`;
that is argued above from the two lock calls and is not machine-checked. No
Docker or native Ceph behavior was run for this slice, and the use counts are
textual.

## Not covered by this slice

Any production change, any change to which lock guards what, any new lock, a
predicate for the first-MON fact, the public harness extraction and the
test-support isolation.
