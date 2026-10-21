# OSD launch recipes and one creation attempt

This is an internal preparation step for #6. It does not add a public retained
store, detach/reopen API, attachment lease or replacement-container support.
`AddOSD` still creates a new native registration; `RemoveOSD` drains and purges.
Same-container Stop/Start remains available through its existing handle.

## Immutable inputs and fresh requests

An OSD registration records its owned descriptor before container creation.
After the existing fresh-store and admin-socket preparation succeeds, its launch
captures config/admin credentials, OSD keyring, numeric ID/UUID/placement, image
reference, network/volume names (including the optional ordinary OSD volume), ownership label and startup timeout as values.
The topology caller holds the cluster lock while preparing and starting OSDs;
concurrent initial starts use distinct recipes. Config bytes are copied under
the config lock. The bootstrap script remains the compiled embedded script.

Each request customization reconstructs Readers for all four files (config,
admin keyring, OSD keyring and script), plus independent environment/label maps,
wait strategy and mount options. Consuming or modifying one request cannot
empty another request's files. The image reference and mount names are captured
inputs, not proof of an immutable image build, storage identity or exclusive use.
A future validated retained descriptor still needs those separate contracts.

## Creation is not replayable

Request reconstruction does not authorize repeated container creation. Copies
of one launch share an atomic attempt guard. `startOSD` admits one creation
attempt and refuses later/concurrent attempts, including after a runner returns
an error without a handle. A canceled context checked before admission creates
nothing and does not consume the attempt. An incomplete preparation or an
already attached descriptor is refused before the runner; an existing handle
is never rebound by this helper.

Any returned container remains attached to the original registered descriptor,
including a readiness/create error, for existing Terminate ownership and retryable
cleanup. A lost create reply still relies on the existing cluster ownership
label scan; this change does not add a candidate ledger or solve attachment
uncertainty. Internal callers retain their topology/admission obligations.
Creating a distinct replacement instance requires a separately validated
lifecycle, not replaying this fresh-registration recipe.

## Validation boundaries

Host-only tests consume real request Readers, mutate original inputs and prior
requests, build concurrent requests and race copied launches. They verify
one-attempt admission and preservation/cleanup of a partial fake container.
Existing initial-start cancellation and bounded concurrency tests remain intact.
These are request/ownership controls, not native storage exclusion or reopen.

Native checks separately exercise actual initial/runtime OSD creation, selected
bridge/host and disk/tmpfs profiles, and same-container restart/readback. A
passing fresh launch does not close #6's different-container, single-replica
known-byte oracle, current MON-registration validation or exclusive attachment.

The ordinary volume backend is described in
[OSD volume storage](osd-volume-storage.md); it does not change this one-attempt
launch contract.
