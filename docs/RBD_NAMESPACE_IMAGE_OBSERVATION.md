# Images in a shared RBD namespace view

`RBDMirrorNamespace.ImageStatus` and `WaitReplayReady` observe an existing image
in a mapping retained by `BindNamespace`. They share the original mirror owner,
clients and receiver cohort. They create no pool, namespace policy, peer, image,
checkpoint, daemon or separate cleanup owner.

```go
view, err := owner.BindNamespace(ctx, "source-namespace", "replica-namespace")
if err != nil {
    return err
}
report, err := view.WaitReplayReady(ctx, "volume")
if err != nil {
    return err // report may retain a useful original source observation.
}
_ = report
// Verify the intended bytes or the application's checkpoint independently.
```

Use an image name without pool, namespace, snapshot or option syntax. The view
selects its original pool and reciprocal namespace mapping. Its immutable
[namespace authority](RBD_NAMESPACE_BINDING.md) still protects original cluster
FSIDs, pool IDs, base and owner-selected policies, bound-selected policies,
receiving peer and generation, and the original private setup handles. A foreign
replacement is rejected rather than adopted. An actual Rebootstrap attempt
invalidates the old view; busy, canceled or rejected native-zero preflight keeps
it valid. There is no implicit rebind.

## Readiness and image mode

The result is the existing `RBDMirrorImageStatus`. It contains the selected
pool/namespaces/name, source and destination local IDs, global ID, actual mode,
primary/mirroring states, destination local status and attributed daemon/instance.
It does not expose new FSID, pool-ID or peer fields; those remain retained guard
authority. `ReplayReady` requires a source primary, a destination secondary,
enabled images and `up+replaying` attributable to one currently running owned
receiver for the original pool/peer/selected namespace. A stale native service
report cannot certify a stopped, partial, removed or substituted worker.

This is image readiness. It does not require every owned receiver to be ready or
an exact-cohort election, and it does not establish byte delivery, an explicit
snapshot's completion, drain or durable irreversibility. Receiver construction
continues to use `ReceiverStatus` and `WaitReceiverReady` separately.

The view derives `snapshot` or `journal` from the positive source image's native
mode. It does not inherit the original owner's configured mode. Both image-policy
modes are supported; a selected pool policy accepts journal only. Missing,
unknown, mismatching or changed mode is rejected, never defaulted or reconfigured.
The destination and repeated reads must retain that mode and image identities.
Recognized creating/disabling transitions are non-ready. State-only progress
during the repeated read clears readiness and is retried after final authority
checks; identity, mode or primary changes remain guard failures.

An original source may be present while replica info still returns native ENOENT.
The result can retain its source local/global IDs and mode with a retryable query
error. This is useful partial evidence, not replay readiness. New view-path
errors expose fixed operation text and canonical caller cancellation/deadline
causes, without arbitrary native stderr or transport errors through error chains.
The original owner's `ImageStatus` and `WaitReplayReady` retain their configured
mode, original mapping and existing error behavior.

## Bounded waits and explicit topology changes

`ImageStatus` has a 30-second ceiling including owner/member admission.
`WaitReplayReady` has a four-minute ceiling shortened by caller context and
releases gates between polls. A deadline returns the last useful observation and
clears readiness; no success is returned after cancellation.

The scoped wait pins source local/global ID and mode at its first positive source
read, including a source-present/replica-pending result. It also captures the
original all-owned receiver cohort at its first admitted observation, even with
zero members or before image info succeeds. Add, Remove or same-name new-handle
replacement during that wait is a guard failure. Start a fresh wait after an
explicit membership change. A later stable same-CID restart may produce a new
native instance; the process must be coherent during each observation. One-shot
status uses the current cohort and therefore works after an explicit Add.

## Focused native fixture

`TestMultiClusterRBDNamespaceImageObservation` is an isolated
`integration,multicluster` fixture with one fresh pair per bridge/host child,
two OSDs per cluster and a shared pool with two replicas/min-size one, one owner
and three existing reciprocal mappings: two image-snapshot mappings and one automatic-enrollment pool-journal mapping. Every mapping uses
`volume`, with distinct local/global identities and per-stage nonce payloads.
Default and isolated same-name unmirrored controls retain their original bytes.

A policy-only zero-daemon phase performs one-shot source observations and a
completed ten-second non-ready wait before the explicit Add. A raw original-engine
list/exact-CID oracle verifies the two retained setup CLIs and absence of hidden
mirror launches. The later wait is fresh; this phase does not claim a native
in-flight first-admission barrier or adoption of a later cohort.

Positive initial, same-CID restart and new-CID replacement phases compare scoped
status and wait results with fresh native info, service, socket and process reads.
Original FSIDs, pool IDs, policies, receiving peer, client CIDs and image identities
are retained. The live attribution checks the actual owned handle/full CID,
normal task, native daemon service/instance, original pool/peer and reciprocal
namespace discovery. It does not impose an election prerequisite. The original
owner's status remains confined to its first snapshot mapping. Stopped and removed
phases are non-ready, and new waits begin after completed membership changes.

Each positive phase separately checks exact 2 MiB source and replica bytes using
fresh native RADOS/RBD processes. Snapshot data is written after enrollment and
followed by an explicit positive source mirror snapshot ID retained in fresh
source status. Exact unique replica bytes establish that checkpoint's data effect;
no destination snapshot-ID equality schema is invented. Journal enrollment stays
automatic and has no fabricated snapshot ID. `ReplayReady` alone proves neither.

After actual clean readiness, each site must reach raw `HEALTH_OK` with empty
checks under a shared 90-second clean/query/poll/module context at pool init and
final data closure. Reported `MGR_MODULE*` failures stop the leaf immediately.
Required `volumes`, `rbd_support`, `mirroring`, enabled modules and published
always-on modules must be present with `can_run=true` and empty errors. Unused
optional catalog failures are not silently promoted to policy violations. Every
health warning must disappear; none is accepted or suppressed as success.

The new private pair helper accepts this fixture's 30-minute caller context for
all construction, client and setup reads. The original helper delegates with
`t.Context()` and preserves its prior behavior and Background cleanup. Both
network children are sequential, and a failed child stops before the next pair.
The two operation ceilings total 60 minutes. Explicit owner cleanup has two
minutes; retained fallback hooks have two minutes for the owner, one minute per
client and three minutes per cluster, with separate five-second failure-log
contexts per container. These are source bounds, not measured runtime promises.
The isolated `make scenario-rbd-namespace-observation` target uses Go90 minutes
and its job100 minutes, `scenario-default` dependency, the existing four role
override unsets, its own baseline/always cleanup and `-failfast`. These source
ceilings and compile selection do not promise runtime success or timing.

Owner cleanup positively removes the original setup and worker CIDs through the
retained raw client before fixture completion. Existing bounded pair/client hooks
remain responsible for their clusters; a dedicated root baseline/outer cleanup
must separately verify no new containers or networks after the parent terminates.
The native source logs readiness, pending reports, source checkpoint IDs, bytes,
health/modules and owner cleanup. Private/source assertions are not invented as
additional emitted marker fields. Deterministic in-flight cohort/mode drift and
HA with one attributed live receiver belong to units unless a real native
admission barrier is available.

## Fixed image boundary

The existing control image contract requires the CLI, `rbd-mirror`, Python
RADOS/RBD modules and required MGR modules/dependencies even when a phase does not
use them. The selected OSD payload contract also remains required. This fixture
adds no build, entrypoint, role, compiler, go-ceph/cgo or alternate dependency.
[Fixed image requirements](../../ceph-testcontainers-images/docs/IMAGE_REQUIREMENTS.md)
remain unchanged at SHA-256
`4682819b3174a632b9da32eb955f9c52593a24112f780f64280380f0e6ccee62`.

A substantiated runtime payload/dependency or policy conflict must immediately
stop dependent work and retain the exact policy section, image digest,
reproducing command, actual error/detail and fixed source evidence. Do not build
an image, modify policy, waive a role component, substitute another tool or
convert the failure to skip/PASS. Health/module checks and this focused fixture
are not a full image checker, new whole CI/image matrix, global process-absence
proof, or certification of every image/platform/version combination.

## Validation status

The exact runtime/API/unit Final2 and context-helper/native overlays passed
offline compilation. Merged root `make check` session65520 completed EXIT0
before the native-only two-line topology repair; production, units and the
context helper then stayed unchanged. Modified Final3 jointly compiled with
the same real API in minimal-tag session63508 and all-tag session43788, both
EXIT0 with no tests run, and four fresh root compiled lists completed EXIT0. The actual
[compiled selection](../artifacts/rbd-namespace-image-observation-20261007/compiled-selection.json)
contains116 required names: the prior N115 plus this single parent, with default14
and SDK11/full-input selecting2 unchanged. Raw all-internal116 includes the two
excluded optional shuffle names; required internal114 plus SDK2 gives116. This
is compile/selection evidence, not new whole-CI runtime certification.

First exclusive native session35557 is preserved as FAIL: package156.187 seconds,
parent/bridge155.81 seconds, and host never started. One OSD and target replicas1
made the strict health check persistently report POOL_NO_REDUNDANCY for `.mgr`
and the target pool. The [original terminal metadata](../artifacts/rbd-namespace-image-observation-20261007/native-final2-rejected/checks-provenance.json),
[raw log](../artifacts/rbd-namespace-image-observation-20261007/native-final2-rejected/native.log)
and [fixture classification](../artifacts/rbd-namespace-image-observation-20261007/native-final2-rejected/fixture-classification-first-run.json)
retain this failure and its dedicated cleanup. No image requirement violation
was established. Final3 changes exactly the fixture OSD count and pool replica
count to2, keeping min-size1 and strict health/module checks unchanged.

Final3 exclusive native session18044 completed EXIT0 on original Quay Linux
ARM64: package886.778 seconds,parent886.38,bridge450.08 andhost436.31. All three
named tests ran and passed, with FAIL/SKIP0. The [actual summary](../artifacts/rbd-namespace-image-observation-20261007/native-summary.json)
and [raw native log](../artifacts/rbd-namespace-image-observation-20261007/native.log)
record18 READY,18 BYTES,12 source CHECKPOINT,18 PENDING,8 HEALTH,8 MODULES,
2 owner CLEANUP and2 COMPLETE. Every raw health detail was HEALTH_OK with
empty checks and mutes; required/enabled/published-always-on module closure
passed. No warning or dependency was waived.

Each positive phase separately read exact2MiB source and replica nonce data:
18 BYTES records describe18 distinct per-mapping/phase datasets, not36 logged
role proofs or a throughput benchmark. The12 explicit positive source checkpoint
IDs plus exact nonce replica bytes prove snapshot data effect. Journal mappings
remain automatic and use no fabricated snapshot ID; ReplayReady alone establishes
neither byte delivery nor checkpoint completion. Live raw CID/task/socket/service
attribution and immutable original scope checks remain source assertions matched
to the public reports, not new public FSID/pool-ID/peer fields.

The253 runtime source inputs matched before/after at SHA-256
`18c3b78cdf452e41bd9f433298537953ded26300c3eeabe9a8e178c224233cfb`.
The [separate outer cleanup](../artifacts/rbd-namespace-image-observation-20261007/runtime-cleanup-final3/after.json)
completed EXIT0/PASS with the same original engine and no new containers or
networks, after native terminal; per-owner cleanup remains a separate source
check. The [run/check provenance](../artifacts/rbd-namespace-image-observation-20261007/checks-provenance.json)
links that actual episode, fresh compiled lists, original rejected run and
unchanged fixed image policy.

This closes the focused retained-view image observation and zero-cohort/lifecycle
extension under the three selected mappings. Deterministic in-flight admission,
cohort/mode/identity drift and HA with one attributed live member remain unit
claims; native zero waits completed before Add, and this fixture has one receiver.
It does not certify new whole-CI116, every image component or other image/platform
combinations. Prior owner-observer, namespace-binding and zero-construction
results retain their original scope; N115, M114 and source `d9115f4` whole-CI101
remain historical evidence rather than this run's new PASS.
