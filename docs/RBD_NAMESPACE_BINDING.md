# Observe existing namespace mappings through one RBD mirror owner

`rbd.Mirror.BindNamespace` creates a read-only view of existing native namespace
policies. The view uses the original owner's setup clients, receiving peer,
daemon inventory and gate. It creates no container, peer, image or mirror
policy, and it has no independent cleanup lifecycle.

```go
view, err := owner.BindNamespace(ctx, "source-data", "replica-data")
if err != nil {
    return err
}
status, err := view.ReceiverStatus(ctx)
// Ready checks this namespace mapping and the owner's exact receiver cohort.
_ = status
if err != nil {
    return err
}
_, err = view.WaitReceiverReady(ctx)
```

Both namespace policies must already be active `image` or `pool` policies with
the same scope and reciprocal remote mappings. The namespace catalogs must
contain each requested named namespace. Empty strings identify the actual
default namespace. The view derives scope from validated native policy; image
snapshot/journal enrollment remains an explicit client operation.

The original owner determines the two clusters, pool name and original numeric
pool IDs, site names, receiving peer and runtime image. Bind does not accept
those again and cannot borrow another fixture's receiver. Named policy
`site_name` may be omitted by the native schema; the original base site stays
authoritative, and a conflicting nonempty selected site is rejected.

Use the owner's existing `SourceRBD` and `DestinationRBD` methods to configure
another namespace pair explicitly before binding it. Pass complete scoped CLI
arguments, including the intended remote namespace. Do not start another
`rbd.RunMirror` to reuse the first fixture's cohort.

For example, after both namespace catalogs exist, these explicit commands
configure one named sibling while preserving the owner's captured base:

```go
_, err := owner.SourceRBD(ctx, "mirror", "pool", "enable",
    "--site-name", "source", "rbd/source-data", "image",
    "--remote-namespace", "replica-data")
if err != nil {
    return err
}
_, err = owner.DestinationRBD(ctx, "mirror", "pool", "enable",
    "--site-name", "destination", "rbd/replica-data", "image",
    "--remote-namespace", "source-data")
if err != nil {
    return err
}
view, err := owner.BindNamespace(ctx, "source-data", "replica-data")
```

These are caller-requested native writes through the existing raw CLI methods.
They need compatible original pool/site configuration and can persist partially
if a later command fails. Bind itself performs only readback and returns nil on
failure; there is no configuration receipt, retry intent or automatic rollback.

## Original authority and lifecycle

Bind requires the owner's successful confirmed setup. It retains the actual
owner, private setup handles, original cluster/pool/policy capture and current
confirmed receiving-peer pointer/generation. Selected UUID, scope, remote
namespace and site are captured independently for each bound pair. Every
observation checks original owner/base/owner-selected identity and that view's
selected policies before and after process reads. Unrelated namespace discovery
cannot substitute for the selected local/remote pair.

Default namespace binding succeeds only when the original base policy is
already active and reciprocal. Bind cannot activate an init-only default or
silently rebase a captured policy. Configure the desired default before owner
construction if it is needed. Shared policy-generation transitions are separate
future work.

Each Bind call returns a new immutable view after strict readback and final
caller-context validation. Rebinding is an explicit new capture of presently
valid namespace policy; it never updates an earlier view. No view registry or
additional runtime resource is created.

An owner Rebootstrap that actually attempts its first native bootstrap mutation
permanently invalidates prior views, including failed calls or lost responses.
Busy, canceled or preflight rejection before any native mutation preserves the
prior peer authority. After successful owner Rebootstrap call Bind explicitly
for a new view. The prior view and its waits never adopt that new generation,
even when native peer UUID happens to remain the same.

Call AddDaemon, RemoveDaemon, Rebootstrap and Terminate only on the owner. A
view has no Container, process control or Terminate method. Owner termination
invalidates all its views; original partial-startup ownership and retryable
cleanup remain unchanged. Bind never grants ownership of namespace policies,
pools, images or data for removal.

## Readiness and topology changes

Status uses the owner's current exact owned cohort. An empty owner inventory
is non-ready; the view does not discover or adopt an external receiver. Explicit
expected daemon names can select an owned HA survivor. Foreign native members
make the exact-cohort predicate false; this does not diagnose another fixture
or the entire pool as unhealthy.

Wait pins the owned names, actual handles/full container IDs, daemon clients and
peer generation on its first admitted observation. A replacement or later Add
is not adopted by that existing wait. Use a new Status or Wait after an explicit
topology change. A same-container restart can legitimately change its native
instance, and remains observable through the original view.

Binding and Status each have a 30-second ceiling; Wait has a four-minute ceiling.
Caller context can shorten either, including admission to the real owner and
member gates. Polls release the owner gate. Errors preserve canonical context
causes without native stderr or private transport data in the error chain.

Ready means selected namespace discovery and agreement on one owned native
pool leader/member cohort. It proves no image replay, checkpoint delivery,
write drain or global absence. Separate views can become ready at different
times; their results are not an atomic aggregate observation.

## Acceptance

Unit coverage exercises shared real owner/member gates, image-free zero and
multi-member scopes, reciprocal default mappings and mixed image/pool scopes.
It rejects selected/base/owner policy, native catalog, receiving tuple, private
setup handle and numeric pool/cluster identity drift. Independent tests cover
cross-namespace false positives, foreign/partial/ambiguous native election,
same-CID restart, original wait replacement/generation guards, between-poll
owner admission, cancellation/sanitization and explicit Rebootstrap lifecycle.

`TestMultiClusterRBDNamespaceBinding` has bridge and host children, each with a
fresh pair, one pool, one initially zero-daemon owner and three named mappings.
Two mappings use explicit snapshot enrollment and positive source checkpoint
IDs; one uses automatic journal enrollment. A raw uncached Docker oracle checks
that binding retained only the owner's two setup clients before a single
explicit Add. Every view is compared with independent native base/selected
policy, peer, positive pool ID, FSID, raw election and actual full-CID evidence.

The native scenario opens independent source/destination sessions for the same
image name in three namespaces, verifies distinct native global IDs and full
nonce payloads, and preserves unmirrored default/unregistered controls. It then
checks shared Stop/Start, zero removal and explicit replacement with new writes
and source checkpoints. The original per-namespace image IDs stay fixed across
those process changes. Views have no child cleanup or second daemon owner.

Compilation is not native acceptance. Record runtime results on the exact
source and image after exclusive cleanup; this scenario does not certify every
image, network environment, arbitrary multiple-peer composition or timing bound.

On 2026-10-07, the pinned original Quay Ceph 20.2.4 image passed on Docker
Desktop Linux ARM64 with a 4GiB VM: bridge 346.73s, host 349.24s, parent 695.97s
and package 696.407s. The exclusive run has three ordered RUN/PASS names,
18 readiness records, 12 full 2MiB replica records and 12 independent destination
reader records. Both networks retained the two setup clients before explicit
Add, and logged eight positive snapshot checkpoint IDs across the two snapshot
mappings and two data phases. Journal checkpoint IDs remain zero.

The raw log, compiled selector inventory, image inspection and independent
accepted audit are under `artifacts/rbd-namespace-binding-20261007/`. The exact
246-file runtime/CI source manifest is unchanged before and after this run;
strict original-engine cleanup found no new container/network/Ryuk resources.
The fixed image requirements policy is unchanged. Root and independent final
raw-evidence audits agree byte for byte. The first verifier rejected the valid
single-object Docker inspect JSON; that failure is preserved separately, and
the corrected verifier accepts the raw object with the same strict image and
platform checks. The raw runtime/image evidence was not rewritten.

Full repository units, race, vet and fixture tag compilation passed. The new
isolated `scenario-rbd-namespaces` profile adds one required parent, making the
actual compiled inventory 113 while preserving all prior profiles. This local
focused run does not certify a new full CI or Debian/Ubuntu image matrix.
Existing receiver lifecycle regression on the same source is tracked separately
under `receiver-regression/`; its data and readiness records are not added to
this namespace-sharing run.

The existing `scenario-rbd-receivers` also passed on the same unchanged source:
bridge 858.37s, host 879.70s, parent 1738.07s and package 1738.542s. Its separate
13 RUN/PASS names, 70 readiness records, 10 full replica byte records and eight
explicit snapshot checkpoints retain the five original default/named and
pool-journal scopes. A new baseline captured after primary cleanup and its own
same-engine cleanup found no new resources. These regression observations and
source/terminal evidence remain separate from the new shared-namespace run.
