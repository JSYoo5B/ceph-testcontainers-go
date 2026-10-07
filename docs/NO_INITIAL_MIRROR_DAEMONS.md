# Configure mirroring before starting receivers

`RBDMirrorConfig.NoInitialDaemons` and
`CephFSMirrorConfig.NoInitialDaemons` configure a mirror link without starting
its initial mirror daemons. Start processes later with that fixture's
`AddDaemon`. This option controls the fixture's initial process count; it does
not prove that a shared pool or filesystem has no external mirror processes.

| NoInitialDaemons | DaemonCount | Initial daemons |
| --- | --- | --- |
| false | 0 | 1, preserving the existing default |
| false | positive N | N |
| true | 0 | 0 |
| true | positive N | rejected |
| either | negative N | rejected |

Contradictory counts and negative counts are rejected before cluster inspection,
native configuration, container allocation, daemon customization, or the CephFS
original-process client factory. An already canceled or expired caller context
also returns its context cause before those effects. Setup failures after
allocation may return a nonnil fixture; always register cleanup before handling
the error. A context that expires during setup can leave native policy behind,
just like other partial setup failures.

## RBD

```go
link, err := multicluster.RunRBDMirror(ctx, image, multicluster.RBDMirrorConfig{
    Source: source, Destination: destination, Pool: "rbd",
    NoInitialDaemons: true,
})
if link != nil {
    defer link.Terminate(cleanupCtx)
}
if err != nil {
    return err
}
receiver, err := link.AddDaemon(ctx, "receiver")
if err != nil {
    // A nonnil receiver remains owned, including partial startup results.
    return err
}
_ = receiver
_, err = link.WaitReceiverReady(ctx)
```

Zero initial daemons still creates and owns the two setup CLI containers. It
configures and captures the original clusters, pools, base and selected
namespace policies, and destination receiving peer. A successful zero Run has
the same confirmed receiver-observation capability as other successful Runs.
`ReceiverStatus` reports that original scope with `Ready=false` and no owned
daemon rows. `WaitReceiverReady` waits for readiness; it never starts a process.
These observers keep their existing exact owned-cohort semantics.

A freshly configured zero link can differ from a link whose receivers have
already run and then been removed: the source may not yet know the remote
transmitting peer. For snapshot mirroring, a reproducible sequence is ordinary
image creation and writes, an explicit receiver Add, receiving-scope discovery
and native source transmit-peer discovery, explicit image enrollment, then an
explicit source mirror snapshot containing the intended writes. Do not start
an implicit temporary receiver to establish that discovery. Pool journal
scope continues to enroll journal-enabled images automatically and needs no
explicit image enrollment or mirror snapshot.

## CephFS

```go
link, err := multicluster.RunCephFSMirror(ctx, image,
    multicluster.CephFSMirrorConfig{
        Source: source, Destination: destination,
        SourceFilesystem: sourceFS, DestinationFilesystem: destinationFS,
        Directories: []string{"/data"}, NoInitialDaemons: true,
    })
if link != nil {
    defer link.Terminate(cleanupCtx)
}
if err != nil {
    return err
}
_, err = link.AddDaemon(ctx, "receiver")
if err != nil {
    return err
}
_, err = link.WaitDirectoryReady(ctx, "/data")
```

Zero construction keeps source MGR mirroring configuration, CephX authorization,
filesystem identity capture, peer bootstrap/import and initial directory
registration. Initial directories remain required, absolute and nonoverlapping;
this option does not add empty-directory construction. The constructor skips
its initial daemon-registration barrier when no daemon was requested.

No mirror container is created during zero construction. Bridge-mode setup can
still own MGR network attachments and its manager-networking Docker client;
host mode does not require those attachments. This is a reduction in mirror
processes, not a claim of zero Docker/client resources.

Daemon customizers and `OriginalProcessClientFactory` are not invoked by zero
construction. Each explicit Add applies daemon customizers. The optional raw
client factory is invoked only for a successfully started daemon; partial
startup is retained with an unavailable `partial-startup` binding. Optional
binding failure preserves the existing Add success contract.

Typed directory addition can register and reconcile another owned path while
the fixture has no daemon. Directory and snapshot readiness remain non-ready
until an actual owned assignment is observed. A first explicit receiver can
replay both initial and subsequently registered directory snapshots.

Native live expansion has separate requirements. On the tested Ceph 20.2.4
line, adding a second receiver after directory registration can encounter the
known MGR shuffle defect. `NoInitialDaemons` neither fixes that native defect nor
automatically calls `Rebalance`. Use explicit rebalance where that environment
requires it; zero-to-one construction and later two-daemon redistribution are
separate acceptance cases.

## Process ownership and cleanup

After a successful zero Run, `Daemons()` is empty and the embedded `Container`
is nil. The first explicit Add that returns a nonnil container becomes the
initial compatibility handle, including partial startup results. Remove that
process with `RemoveDaemon`, or terminate the fixture to release all owned
runtime resources. Removing the initial process clears the embedded handle;
later Adds do not reassign it. Use `Daemons()` for current membership.

This option leaves existing ownership boundaries intact: fixtures do not own
the supplied Ceph clusters, pools or filesystem data. Termination cleans up
owned runtime resources and leaves native auth, peer and mirror policy in the
caller-owned disposable clusters. Failed cleanup remains retryable.

## Acceptance evidence

The offline unit suite covers legacy defaults, explicit zero, contradictions,
negative counts, context causes, rejection before runtime/customizers/factory,
and a zero RBD observer with confirmed original identity and no automatic Add.

`TestMultiClusterNoInitialMirrorDaemons` is a separate native scenario for both
bridge and host networking. Each child owns a fresh pair and requires exclusive
Docker execution for its uncached engine/list/exact-CID resource oracle. It
checks zero mirror containers and two retained RBD CLI containers, or zero new
CephFS containers; it does not infer memory usage from those counts.

The RBD snapshot child proves ordinary unmirrored payload at zero followed by a
normal first Add, discovery, enrollment and a positive explicit checkpoint ID.
The named-namespace journal child proves automatic enrollment and an explicit
partial first Add, removal and successful retry. Both verify the original
FSIDs, positive pool IDs, immutable base/selected policies, receiving peer,
exact native election and independent full source/destination payload bytes.

The CephFS child proves a normal first Add replays source snapshots from both
initial and typed-added paths. It verifies original filesystem/metadata-pool/
peer/directory identities and independently checks source and destination
snapshot bytes, including later partial-startup cleanup and resumed delivery.
No native pass is implied by compiling this scenario; runtime acceptance must
be recorded separately on the exact image and source under test.

At source `a141901`, the merged repository `make check` passed full units, race,
vet and all fixture tag compile. That source's actual compiled CI selection had
112 required parents; all prior
profile sets remain unchanged and this separate scenario adds one parent. The
exact `integration,multicluster` name list also compiled successfully.

On 2026-10-07, Docker Desktop Linux ARM64 with a 4GiB VM used the pinned original
`ceph.DefaultImage`. All six fresh-pair cases passed: bridge 465.32s, host 466.10s,
parent 931.42s and package 931.698s. Nine ordered RUN/PASS names cover the parent,
two networks and six leaves. The independent verifier accepted 12 zero-resource
records, four 2MiB RBD byte records and four destination reader records, two
positive explicit RBD snapshot IDs, 16 CephFS source/destination byte records,
four continuous pre-receiver absence intervals, eight public CephFS checkpoints
and two final identity/counter records. Journal checkpoint IDs remain zero.

`artifacts/mirror-initial-daemons-20261007/native-runtime.log` and
`native-evidence.json` bind those records to terminal exit zero and the exact
reviewed source. All 243 runtime files matched before/after; the manifest excludes
Python bytecode caches, docs and artifacts. The unchanged fixed image policy and
actual pinned image ID/digest/platform are recorded separately. This primary
run's own `native-cleanup/after.json` has zero introduced or remaining labeled
running/stopped containers and networks on the original engine. The verifier
also passed 24 synthetic acceptance/rejection fixtures. These are selected local
native results, separate from a new complete CI run or an image/platform matrix.
The 150-minute Go and 160-minute CI budgets do not promise another environment's
execution time. Resource counts establish construction behavior rather than
measured memory savings.

A later [read-only namespace binding](RBD_NAMESPACE_BINDING.md) can share this
zero-daemon RBD owner across existing native mappings. It owns no extra runtime
and does not change the initial-daemon option contract.
