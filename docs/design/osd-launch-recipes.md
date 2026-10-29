# OSD launch recipes and one creation attempt

A launch recipe holds the inputs of one OSD container creation. `AddOSD`
creates a new native registration; `OSDContainer.Remove` drains and purges.
Same-container Stop/Start uses the existing handle, and retained reopen is
described in [OSD retained reopen](osd-retained-reopen.md).

## Immutable inputs and fresh requests

An OSD registration records its owned descriptor before container creation.
After the fresh-store and admin-socket preparation succeeds, its launch
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

## Creation is not replayable

Request reconstruction does not authorize repeated container creation. Copies
of one launch share an atomic attempt guard. `startOSD` admits one creation
attempt and refuses later/concurrent attempts, including after a runner returns
an error without a handle. A canceled context checked before admission creates
nothing and does not consume the attempt. An incomplete preparation or an
already attached descriptor is refused before the runner; an existing handle
is never rebound by this helper.

Any returned container remains attached to the original registered descriptor,
including a readiness/create error, for Terminate ownership and retryable
cleanup. A lost create reply relies on the cluster ownership label scan.
Internal callers retain their topology/admission obligations.
Creating a distinct replacement instance requires a separately validated
lifecycle, not replaying this fresh-registration recipe.

## Validation boundaries

Host-only tests consume real request Readers, mutate original inputs and prior
requests, build concurrent requests and race copied launches. They verify
one-attempt admission and preservation/cleanup of a partial fake container.
These are request/ownership controls, not native storage exclusion or reopen.

Native checks separately exercise actual initial/runtime OSD creation, selected
bridge/host and disk/tmpfs profiles, and same-container restart/readback. A
passing fresh launch is not evidence of exclusive attachment.

The ordinary volume backend is described in
[OSD volume storage](osd-volume-storage.md); it uses the same one-attempt
launch contract.
