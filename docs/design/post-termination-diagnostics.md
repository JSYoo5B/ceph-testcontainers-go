# Post-termination diagnostics: representation comparison

Design note for the `ceph.Container` size reduction (Refs #146). It compares
two ways to give callers diagnostic data after a cluster has been terminated,
and one hybrid. It defines no final public type, function name or lifecycle-state
API: every signature below is a proposal for review, not a decision.

This revision replaces an earlier version of the same note. That version
predates `ceph.FactsOf` and the `Observer`; it said no snapshot type existed.
The inventory, boundaries and comparison are redone against the revision below.

Source citations use `path:line` at commit
`ad2ac0a355c515cd82bb2860015c1f480b853366` (the merge of #231 into `main`).
Everything is read from source. Nothing was run, and no claim here is native
evidence.

## Principles already agreed

1. Diagnostic information stays accessible after cluster termination.
2. Diagnostic data gives no way to control or reactivate the cluster.
3. Unavailable information, failed collection and omissions are explicit.
   Absence of retained evidence never implies a clean outcome.

Earlier owner decisions stay in force: static facts are an independent copy
with exported fields; a nil cluster returns an error and the zero value; a new
static-facts snapshot can be obtained from a non-nil terminated cluster;
recorded information is never relabeled as a fresh native observation; the
closed flag is not evidence that owned resources are gone.

## What exists today

| Data | Where | State after a successful `Terminate` |
| --- | --- | --- |
| Static facts (address family, control image, image set, messenger mode, network names, host network, public address, Ceph version) | `ClusterFacts` and `FactsOf` (`ceph/cluster_facts.go:15`, `:43`). It takes no lock, runs no command, makes no Docker call and never reads `closed`. | Retained. Works on a terminated non-nil cluster; a nil cluster returns the zero value and an `ErrInvalidRequest` error. |
| Tracked container handles, network IDs, redaction inputs | Read by `diagnosticsSnapshot` under `mu`, `controlMu`, `clientPublishMu` and `configMu` (`ceph/diagnostics.go:214`) | Mostly gone. `terminate` deletes each service, OSD, manager and monitor entry once its removal succeeds (`ceph/ceph.go:1042-1107`). The primary monitor handle and the network records stay, so a later report names objects that no longer exist. |
| Container state, network addresses, log tails, fixed Ceph queries | `CollectDiagnostics` returning `DiagnosticsReport` (`ceph/diagnostics.go:55-85`) | Not retained anywhere. A call after termination runs, but its Docker inspects of removed objects fail per artifact, so the report is incomplete by construction (read from code, not run). |
| Crash metadata | `CollectCrashMetadata` returning `CrashMetadataReport` (`ceph/crash_metadata.go:74`, `:202`) | Destroyed with the container; its own documentation says a removed container takes its records along. |
| Failure archive | `Finalization.archive` writes `diagnostics.json`, `crash-metadata.json` and `manifest.json` (`ceph/finalization.go:346`) | Written before cleanups and `Terminate`, and only when `Archive` is set and the test failed. A passing run collects nothing (`ceph/finalization.go:28-33`). |
| Termination outcome | `finalizationManifest.Termination` is a name, two times and an error string (`ceph/finalization.go:265`, `:607`) | Recorded as one joined string. Which resource family failed is not structured. |
| Cleanup record of one override | `DebugLevelReceipt` with state, generation and transitions (`ceph/debug_level.go:62`, `:103`) | Per-window only. It is the closest existing per-resource cleanup record. |
| Shared-cluster outcome | `sharedInstance.poisoned` (a reason string), `SharedStats` counters (`ceph/shared.go:136`, `:267`), release problems logged through `t.Logf` (`ceph/shared.go:565-600`) | Nothing structured survives. The instance is deleted from the registry on a successful termination (`ceph/shared.go:491-530`). |
| Error attribution | `OperationError` with `Kind`, `Op`, `Target`, `Version`, `Image`, `Capability`, `Err` (`ceph/errors.go:65`) | Applies to classified failures. The errors that `terminate` joins are plain wrapped errors, not `OperationError` values, in the lines read. |

Existing reports already use the explicitness style the principles ask for.
`DiagnosticArtifact` has a per-observation `Error` and `Truncated`;
`DiagnosticsReport.Complete` means every requested observation succeeded
without truncation; `CrashMetadataContainer.Outcome` is an enum with
`unavailable`, `failed` and `incomplete` apart from `empty`; the manifest lists
`Collectors` with `Reported` and `Complete`, and its notes say that no crash
record is not proof that no daemon crashed and that nothing was collected after
termination (`ceph/finalization.go:653`).

Two gaps follow from the table.

- No structured record says which resource families `terminate` removed, which
  failed and which it never reached. The caller gets one joined error.
- No exported state says whether a native collection ran before teardown. A
  report that was never produced and a report that found nothing are
  indistinguishable to a later reader, because nothing remembers either.

## Capture boundaries and ownership

Phases of a cluster's life, and what can be captured in each:

| Phase | Static facts | Runtime and native evidence | Owner of the trigger |
| --- | --- | --- | --- |
| Running | Yes | Yes (`CollectDiagnostics`, `CollectCrashMetadata`, `Observer` queries) | The caller, or `Finalization` |
| During `Terminate` | Yes, lock-free | `terminate` holds `mu` from its first lock until it returns (`ceph/ceph.go:1019-1022`) and `diagnosticsSnapshot` waits for `mu` with `TryLock` polling until its context ends (`ceph/diagnostics.go:198`, `:218`). A concurrent collection therefore stalls or times out, it does not observe a half-removed cluster. | Nobody |
| After a successful `Terminate` | Yes | None survives; Docker objects are removed | Nobody |
| After a failed `Terminate` | Yes | Whatever failed to be removed may still exist, and `closed` is already true. A retry is possible because `monitorTerminated`, `networkRemoved` and `clusterNetworkRemoved` are set only after their own removal (`ceph/ceph.go:1104`, `:1150-1163`). | The caller |

Consequences that no representation changes:

- Runtime and native evidence has to be collected before `terminate` takes
  `mu`. A diagnostic object created afterward cannot recover it. A separate
  object alone guarantees nothing about whether collection happened.
- `Finalization` is the one existing pre-teardown trigger: it archives before
  its cleanups and before `Terminate` (`ceph/finalization.go:217-262`). It does so
  only for a failed test with `Archive` set, and only for clusters registered
  through `TerminateOnCleanup`.
- A plain `Terminate` call has no capture step. Adding one inside `terminate`
  would be a larger feature (bounds, timeouts, redaction, behavior under
  cancellation) and is not decided here.
- Static facts can be copied at any time. `FactsOf` documents why it needs no
  synchronization with `Terminate`: the underlying values are written before the
  cluster is returned to the caller and termination does not clear them
  (`ceph/cluster_facts.go:30-42`).
- Static facts are recorded configuration. They must carry that provenance
  (see the model below) and must not be presented as an observation.

## Failure and omission reporting model

This applies to every option. Names are placeholders.

1. Every part of a record has its own state. The zero value of the state means
   "unspecified" and never "collected", so a zero record cannot read as clean.
2. A part is in exactly one state: `collected`, `partial` (incomplete or
   truncated), `failed` (with a bounded, redacted error text), `not-collected`
   (nothing ran), `unavailable` (could not exist, for example runtime data after
   termination) or `omitted` (excluded by a limit, with a count).
3. Each part names its provenance: recorded configuration, or native
   observation with a capture time. Recorded data never takes the observation
   label.
4. A summary flag such as `Complete` is true only when every part is `collected`
   and none is truncated. It does not mean the cluster is healthy or that cleanup
   succeeded.
5. Two facts that today share one flag stay separate:
   - object usability (the handle's `closed`, set at `ceph/ceph.go:1023` before any
     resource is removed);
   - owned-resource cleanup, per resource family, as `removed`, `failed` (with
     error text), `not-attempted` or `unknown`.
   Both existing reports copy `closed` as `Closed`, and the manifest has
   `ClusterClosed` (`ceph/finalization.go:624`). None of those may be read as
   successful removal, and a record built from them alone would let a partially
   failed teardown look clean.
6. Cleanup families that `terminate` handles today: fault undo phases, native
   invocations, port leases, services, OSDs, OSD memory and volume helpers,
   managers, monitors, the primary monitor container, the control plane, the
   feature client, the label sweep, the run directory, the cluster network and
   the public network (`ceph/ceph.go:1030-1165`). Which of these are exposed is a
   contract decision.
7. A cleanup outcome is derived from a recorder inside `terminate`, not from
   leftover state. Leftover maps and flags cannot tell "never attempted" from
   "attempted and failed", and they say nothing about the fault phases or the
   label sweep.
8. No part holds a handle, context, lock, executor, credential or unredacted
   secret. Retained native data is already redacted when produced, and the
   redaction inputs are not retained.

## Options

All types and functions below are proposals.

```go
// Shared building blocks (proposal).
type EvidenceState string // collected, partial, failed, not-collected, unavailable, omitted

type Evidence[T any] struct {
	State      EvidenceState // zero value is invalid, never "collected"
	Provenance string        // "recorded" or "observed"
	CapturedAt time.Time     // zero when nothing was captured
	Value      T
	Error      string // bounded and redacted
}

type CleanupOutcome string // removed, failed, not-attempted, unknown

type TeardownRecord struct {
	Usable   bool                      // false once the handle refuses operations; not a cleanup claim
	Families map[string]CleanupFamily  // keyed by resource family
}
```

### Option A: independent snapshot returned separately

A package function takes the cluster and returns a self-contained value, as
`FactsOf` and `Observe` do.

```go
type ClusterRecord struct {
	Facts       ClusterFacts
	Diagnostics Evidence[*DiagnosticsReport]
	Crash       Evidence[*CrashMetadataReport]
	Teardown    Evidence[TeardownRecord]
}

func RecordOf(c *Container, opts ...RecordOption) (ClusterRecord, error)
func WithDiagnostics(report *DiagnosticsReport, err error) RecordOption
func WithCrashMetadata(report *CrashMetadataReport, err error) RecordOption
```

- Data: facts always (from a non-nil cluster, terminated or not). Native parts
  only if the caller supplies them, otherwise `not-collected`. Teardown is only
  what the function can derive from visible state, which per point 7 is weak, or
  `unknown`.
- Capture timing: the caller owns it entirely. `Finalization` could call it
  before `Terminate`, but a plain `Terminate` leaves nothing for a later call
  except facts.
- Explicitness: good for parts the caller supplied; "never collected" is told
  apart from "found nothing" only because the option was absent.
- Control: the value holds copies and strings only.
- Benefits: no new `Container` method, no new state in `Container`, symmetric
  with `FactsOf` and `Observe`, smallest migration.
- Risks: cleanup completion cannot be reconstructed after the fact; a caller
  that forgets to collect gets facts only (explicitly reported as such, but still
  nothing to diagnose with); the value must be kept by the caller across the
  cluster's life.
- Migration cost: one type family, one function, API catalogue entries and a
  release fragment, docs. No change to `terminate`.

### Option B: obtain the snapshot through the existing handle

The handle yields the same independent value, and the cluster keeps what it
captured.

```go
func (c *Container) Record() (ClusterRecord, error) // read-only, works after Terminate

// Optional capture trigger, a separate decision:
func WithTerminationCapture(DiagnosticsConfig) RunOption
```

- Data: the same value. In addition the handle can own retained state: a
  teardown record written by `terminate`, and, if a capture step is added, the
  native evidence collected just before removal.
- Capture timing: the cluster owns the teardown record, so cleanup outcome is
  reliable. Native pre-teardown capture exists only if the larger feature is
  accepted; without it B has no more native evidence than A.
- Explicitness: "collected during teardown" versus "never collected" is told
  apart by the handle itself.
- Control: the value carries nothing, but the entry point is a method on a
  control handle. It must be read-only, must not use the `closed` admission that
  refuses other operations, and must not become a path that reaches the cluster.
- Benefits: one place to ask; the record exists for callers that never thought
  of collecting.
- Risks: adds an exported `Container` method and retained state to the type #146
  shrinks; needs its own entry in `sharedMethodPolicies` and the refusal test
  (`ceph/shared_gate.go`); a `Tenant` cannot offer the same call because a
  released tenant refuses every method by contract (`ceph/tenant.go:256`).
  Retained native data keeps memory alive for the life of the handle.
- Migration cost: A's cost, plus `terminate` instrumentation, a retained field
  with its own synchronization, gate policy, and tests for every teardown path
  including cancellation and a failed teardown.

### Hybrid C: A-shaped entry point, cluster-owned teardown record

The package function of A, and the cluster-owned retention of B, without a new
`Container` method.

```go
func RecordOf(c *Container, opts ...RecordOption) (ClusterRecord, error)
```

- `terminate` writes an unexported teardown record through a small recorder
  (separate mutex or atomically published copy, so a reader never waits behind
  `mu`, which `terminate` holds for the whole teardown).
- `RecordOf` copies facts, copies the teardown record, and attaches caller
  supplied native evidence as in A. Native evidence stays caller or
  `Finalization` owned until the capture feature is decided.
- It removes A's main weakness (cleanup completion is recorded at the moment of
  removal, not inferred) without B's exported method.
- It still adds one unexported field to `Container`. That is the only cost to
  the size goal and can move into the networking and teardown component when the
  internal state split happens.

## Comparison

| Question | A | B | C |
| --- | --- | --- | --- |
| Static facts after termination | Yes | Yes | Yes |
| Native evidence after termination | Only if the caller kept it | Only with an added capture step | Only if the caller kept it, unless the capture step is added later |
| Guarantees native data was collected | No | No by itself | No by itself |
| Per-family cleanup outcome | Not reliably | Yes | Yes |
| Tells "never collected" from "nothing found" | By absent option | Yes | By absent option; teardown part yes |
| Control through the data | None | None in the value; read-only method required | None |
| New exported `Container` method | No | Yes | No |
| Retained state in `Container` | No | Yes (teardown, maybe native) | Yes (teardown only) |
| Matches `FactsOf` and `Observe` style | Yes | No | Yes |
| Instrumentation of `terminate` | No | Yes | Yes |

## Tenant handles and the shared-cluster harness (#155)

- A `Tenant` never exposes `*Container` and refuses every method once released
  or once its shared cluster is terminated (`ceph/tenant.go:256-264`). The
  `Observer` keeps that: its tenant form validates on each query. Therefore an
  `Observer` must not carry post-termination data. It retains query capability
  (`ceph/observer.go:22-27`), which principle 2 forbids for a diagnostic value.
- `RecordOf(*Container)` and a Container method are unavailable to a tenant
  holder. A tenant record would need its own constructor taking the tenant, in
  the way `Observe` and `ObserveTenant` pair up. After release the tenant refuses
  calls, so its record has to be produced at release time, by the code that owns
  `cleanAndVerify` (`ceph/shared.go:603`), and handed out as a value. A method on
  the tenant handle cannot serve it.
- Tenant-scoped content is a separate decision. `CollectDiagnostics` covers the
  whole shared cluster, including sibling tenants' daemon logs. A tenant record
  would have to carry only: lease identity, the tenant's own release outcome
  (removal errors, scope problems, poison reason, today only a string and a log
  line) and, possibly, the shared cluster's static facts.
- For #155 the harness moves out of `ceph` and may import core, never the other
  way. So the cluster record types belong in core `ceph` (they describe the
  cluster, and `FactsOf` is already there). The lease/tenant record belongs with
  the harness and reuses `ClusterFacts`. Option A and C functions on `*Container`
  stay in core and are unaffected by the move. Option B's method on `Tenant`
  would move with it, so B would be redone once.
- Shared-cluster termination goes through `SharedCluster.terminate`
  (`ceph/shared.go:491`), because `Container.Terminate` is refused with
  `ErrSharedCluster`. A cluster-owned teardown record written inside the
  unexported `terminate` still covers it, since that path calls the same
  function.

## Ordering relative to the merged #146 work

- Done: static facts (`FactsOf`) and the live `Observer` (`Observe`,
  `ObserveTenant`), including the CLI, topology-locked, control-resolving and
  rados/metrics query groups.
- The record reuses `ClusterFacts` unchanged; it adds nothing to the facts
  group.
- It is independent of the override extraction, which does not touch `terminate`.
- It does interact with the planned internal state split. The networking fields
  (`network`, `clusterNetwork`, `networkRemoved`, `clusterNetworkRemoved`) and the
  teardown flags are what a cleanup recorder would be written against. Defining
  the internal recorder first means that split moves one recorder, not several
  flags that each need re-derivation.
- #155 should see the record types before the harness is moved, so the lease
  record is designed once against its final package.

## Open questions for the owner

1. Entry point: package function (A or C), or handle method (B)? Is a new
   unexported retained field in `Container` acceptable for C, given the size
   goal?
2. Is pre-teardown native capture inside `terminate` wanted at all, or does it
   stay with `Finalization` and the caller? If wanted: bounds, timeout when the
   caller's context is already canceled, behavior on a failed `Terminate`.
3. Should the record exist while `Terminate` is running (an "in progress"
   cleanup state), or only after it returns? Exposing in-progress state is a
   lifecycle-state API and is not assumed here.
4. Which cleanup families are part of the contract, and are they a closed enum or
   open strings?
5. What does the retry of `Terminate` do to an earlier record: replace, append an
   attempt, or keep the first failure and the last outcome separately?
6. Tenant record: which fields, and does it include the shared cluster's static
   facts? Is it produced by release and delivered through the test, or kept by
   the `SharedCluster`?
7. Should `CollectDiagnostics` and `CollectCrashMetadata` keep working on a
   terminated cluster as they do now (incomplete report plus error), or refuse
   with a clear error now that a record exists?
8. Redaction policy for retained native data, and its memory bound.
9. Whether the reports' existing `Closed` fields stay as they are or are
   renamed so they are not read as cleanup claims. That would be a breaking
   change.
10. Is the state spelling (field, enum or error) per part acceptable, as long as
    the zero value is never "collected"?

## Recommendation

This is a recommendation, not a decision.

Adopt Hybrid C in two stages.

1. Record the teardown outcome per family inside `terminate`, and expose it with
   the static facts through a package function that also accepts caller-supplied
   native reports. This closes the one gap that no caller can close alone (cleanup
   completion), keeps the entry point symmetric with `FactsOf` and `Observe`, adds
   no exported `Container` method, and needs no automatic collection.
2. Decide pre-teardown native capture separately, after stage 1 is reviewed. Until
   then `Finalization` remains the only automatic pre-teardown trigger, and the
   record reports every other native part as `not-collected` or `unavailable`.

Reasons: Option A alone cannot report cleanup completion after the fact, which is
the point of keeping `closed` apart from removal. Option B repeats the retained
state and the exported method that #146 is trying to remove, and it does not fit
a released tenant. C costs one unexported field and an instrumented `terminate`.

## What a later implementation would need to prove

- The record is independent: mutating its slices, maps and nested values changes
  neither the cluster nor another record, and it stays readable after
  `Terminate`.
- A reader never blocks behind a running `Terminate`, and sees no torn record.
- A nil cluster returns an error and the zero value; a terminated non-nil cluster
  returns the recorded facts without claiming that resources exist.
- Every teardown path writes its record: success, a failed removal, a retry after
  failure, and a canceled context.
- `closed` is not used as a cleanup claim, and a partially failed teardown shows
  failed families next to usable-false.
- A part that never ran is distinguishable from one that found nothing, and a
  complete-looking empty record is impossible.
- The value exposes no handle, lock, credential, executor or unredacted secret.
- The API catalogue, package pages, CHANGELOG fragment and upgrade mapping are
  updated together and checked at the implementation head.

## Limits of this note

Source reading only at the commit above. No Docker, native Ceph or timing
behavior was run. The statement that a post-termination `CollectDiagnostics`
returns an incomplete report with per-artifact errors is read from the code, not
executed. This revision adds no product code, test, fragment or API change.
