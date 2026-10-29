# Image role terminology and responsibility map

> Current boundary: the shared-cluster/tenant harness and its core-only policy,
> lease and hook plumbing have been removed. Harness references and source
> locations below are historical design evidence, not retained APIs or future
> extraction requirements. See [independent fixtures](../ceph.md#usage-example).

This note is the first acceptance item of #227: a terminology and
responsibility map for everything in this repository that names an image role.
It classifies each occurrence group of the retired control-image vocabulary,
inventories the image-oriented names that should follow the current role
contract, separates a naming-only change from a place where the old name hides
a real routing or admission question, and orders the implementation.

It changes no code, no public signature, no image pin and no behavior. It is
source reading only. No Docker and no native Ceph ran, so nothing here is
native evidence, and nothing here claims an image was rebuilt or published.

## Method and limits

- Module snapshot: `main` at `7015c62` (merge of #219). Image contract
  snapshot: `ceph-testcontainers-images@8076bf59d5eeb933d1818c97771a377d5d624723`
  (`docs/ROLES.md` and `docs/IMAGE_REQUIREMENTS.md`). The pinned snapshot is the
  vocabulary authority; this note does not restate it beyond what the map needs.
- Occurrences were found with `git grep` over the whole tree (Go identifiers,
  comments, documentation, README, guides, research notes, scripts, JSON,
  fixtures, workflows), case-insensitive, for `control image`,
  `controlImage`/`ControlImage`/`controlImageID`, `official-<release>-control`,
  `control or all`, the daemon-role tag family wording and `control, OSD`.
  A second pass listed every identifier that contains `control` to find
  meanings the first pattern misses (`ControlContainer`, `controlPlane`,
  diagnostic `Role` values, `Target: "control"`, script and test fixtures).
- The first pattern matches 229 lines in 107 files outside the per-package API
  catalogues (three more lines in `docs/design/api-naming/ceph`). Per area:

| Area | Lines | Files |
| --- | --- | --- |
| `internal/integration` test helpers | 85 | 58 |
| API reference pages (`docs/ceph.md`, `docs/rbd-mirror.md`, `docs/rbd.md`, `docs/cephfs.md`, `docs/cephfs-mirror.md`, `docs/rgw.md`, `docs/rgw-multisite.md`) | 43 | 7 |
| `ceph` non-test Go | 34 | 14 |
| `docs/guides`, `docs/contributing` | 25 | 6 |
| `ceph` unit tests | 13 | 4 |
| `docs/design` (earlier notes) | 11 | 5 |
| `rbd`, `cephfs`, `rgw` non-test Go | 9 | 7 |
| `.github` retained producers and rewrite map | 4 | 3 |
| `docs/research`, `CHANGELOG.md`, `README.md` | 3, 1, 1 | 1, 1, 1 |

- The word also appears a few thousand times with unrelated meanings
  (negative controls and control groups in tests, `controller`, cgroup
  controllers, `CONTROL_TIMEOUT` in a probe script, `unicode.IsControl`).
  Those are a fourth group that this issue does not touch and the final scan
  must not flag.
- Consumers below were resolved by reading the call sites, not the spelling.

## The current vocabulary (the authority)

| Name | Meaning | Where the module uses it |
| --- | --- | --- |
| `essential` | MON, MGR, OSD, `ceph` CLI, bootstrap tools, daemon-local MGR modules and OSD classes/plugins | `ImageSet.Essential`, the `Run` image argument |
| `client` | ordinary `ceph`/`rados`/`rbd`, Python bindings, striper, encryption, offline map tools | `ImageSet.Client`, `ClientContainer` |
| `cephfs` | MDS and `cephfs-mirror` | `ImageSet.CephFS`, `cephfs.WithMDSImage` |
| `rbd-aux` | `rbd-mirror` and the immutable object cache | `ImageSet.RBDAux` |
| `rgw` | RGW and its administration/setup runtime | `ImageSet.RGW`, `rgw.WithImage`, multisite setup clients |
| `all` | explicit union of the five | `ImageSet.All` |

`ceph/images.go` (`ImageSet.normalized`, `configureImages`) already implements
this and `.github/ceph-role-images.json` and `.github/scripts/run_image_matrix.py`
already use it. There is no `control`, `osd`, `mds` or `rgw`-as-tag-family role
left in code that selects images. The residue is names, comments and prose.

## The three classes

1. **Dependency-role image selection or reference.** A name, field, option,
   comment or sentence that picks, passes, describes or requires a role image.
   These follow the contract above.
2. **Execution responsibility.** The control plane (the `ceph` CLI executor
   and its container), and the MON, MGR, OSD, MDS, RGW, `rbd-mirror` and
   `cephfs-mirror` processes. These are not image roles. They are kept, and
   clarified where the text implies an image role.
3. **History and deliberate negatives.** Published tags, raw receipts, pinned
   provenance, released changelog and migration text, and fixtures that
   exist to assert refusal of an obsolete name. These are preserved and
   marked.

### Class 2: keep, clarify

The control container is real and the image project's own contract still
says so (IMAGE_REQUIREMENTS, `pgrep` row: "process inspection in the control
container"). What the module must say is that this container is an executor
created from the **essential** image, not a published role.

| Name | Where | What it is | Action |
| --- | --- | --- | --- |
| `Container.ControlContainer`, `controlContainerContext`, `controlPlane`, `controlMu`, `ensureControlPlane` | `ceph/topology_snapshot_context.go`, `ceph/topology.go` | The independent CLI container, started from `options.controlImage`, so topology operations survive a stopped first MON | Keep. Doc sentence: "runs from the essential image". Do not rename. |
| `ClientContainer`, `clientContainerContext`, `featureClient` | `ceph/images.go` | The client-role executor; reuses the control executor only when `Client == Essential` (aggregate) | Keep. Already named by role. |
| Diagnostic `Role` values `"control"/"cli"` and `"client"/"toolkit"` | `ceph/diagnostics.go` (`add("control", "cli", ...)`), `ceph/verification_collect.go` | Executor labels in `DiagnosticsContainer` and the shared-backend daemon map (`ceph/shared_backend.go`) | Keep. Schema and historical interpretation must not change (as the issue requires). |
| `OperationError.Target` value `"control"` | `ceph/auth_config.go` (`generateKeyArgs`), `ceph/errors.go` doc, `docs/guides/errors.md` | Names the container that ran the probe (the control container) | Keep the label (executor). The message text "the control image lacks" is class 1 (see N2 below). |
| `multicluster.ValidatePair` "control handle", cephfs MDS "auth control", `readRBDReceiverFSID` | `internal/multicluster/resources.go`, `cephfs/*`, `rbd/mirror/mirror_receiver_identity.go` | Admission against the control-plane container; all of these run `ceph` commands | Keep. |
| Process identities MON, MGR, OSD, MDS, RGW, `rbd-mirror`, `cephfs-mirror` | everywhere | Processes, not images | Keep. |
| `control` as a local variable for a container | `internal/integration`, `cephfs`, `rbd` | Mostly `ControlContainer` results | Keep when it is the control-plane container. Rename when the value comes from `ClientContainer` (rows R6 and N5). |

### Class 3: preserve with context

| Occurrence | Why it stays | Proposed marking |
| --- | --- | --- |
| `CHANGELOG.md` (v0.2.0 entry about `rbd.RunMirror`; v0.2.0 text about OSD images without a CLI) | Published history | None; published sections are not rewritten. |
| `docs/research/ceph-fuse-mount.md` (`official-<release>-control` tag, quoted refusal line) | A recorded measurement of what ran | Add a "Historical tag family" sentence stating that `-control` tags belonged to the retired role layout and that the current roles are essential/client/cephfs/rbd-aux/rgw/all. Keep the quoted log line verbatim. |
| `.github/scripts/fixtures/cephfs-quiescence-native.source.go.txt`, `cephfs-recovery-native.source.go.txt` | Retained native producer sources; their SHA-256 is asserted in `test_scenario_quiescence.py` | Never edited. |
| `.github/scripts/fixtures/package-split.json` (`cephfsmirror.Run(ctx, source.ControlImage(), ...)` rewrite) | Rewrites the retained producers to current names so declaration parity with current helpers holds | Any change to the call text in current helpers needs a matching rewrite entry here (see "Coordination: retained producers"). |
| Negative role fixtures: `test_run_image_matrix.py` `test_unknown_role_and_missing_essential_are_refused` (`control`, `osd`), `test_tag_scenarios.py` (`image-roles` values `osd`, `control,...`) | They assert that retired names are refused | Keep; add a comment that the values are retired names. |

## Class 1 inventory: old to new

"Kind" is **N** for naming-only (the code already routes by role, only the
spelling or the explanation is stale) and **R** where the old name hides a real
routing or admission question that code or an example currently gets wrong or
could get wrong.

### Public API

| Old | Proposed | Consumers (from code) | Kind |
| --- | --- | --- | --- |
| `(*ceph.Container).ControlImage()` (`ceph/ceph.go`) | **Decided (O1): both.** Every call site that is not the essential role moves to the role accessor it runs: `Images().Client`, `Images().RBDAux`, `Images().CephFS`, `Images().RGW`. Remaining real essential uses call `EssentialImage()`, the renamed accessor. No function named `ControlImage` remains and no alias is kept (O2) | 55 call sites in 43 Go files (the definition is not counted). 11 feed `nativeClientImage` (client toolkit oracle), 8 feed `nativeMirrorImage(..., "rbd-aux", ...)` and 10 feed `nativeMirrorImage(..., "cephfs", ...)` (all by the test helper in `internal/integration/services_integration_test.go`), 11 are locals that feed the mirror constructors, 2 pass it to `multisite.Run`/`RunTopology` as the gateway image, 6 only log it in receipts, 1 builds a Docker container request labelled as a MON image, 2 are negative fixtures (an image without `tc`; a stranger container), 1 is the FUSE helper, plus the definition and 3 unit-test uses | **R** for the mirror, client, multisite and FUSE call sites (the accessor returns the essential image, which is the wrong role for all of them under split roles); **N** for the rest |
| `ceph.ClusterFacts.ControlImage` (`ceph/cluster_facts.go`, added by #146's `FactsOf`) | **Remove.** `ClusterFacts.Images.Essential` already carries it; no replacement field | `FactsOf`, `ceph/cluster_facts_test.go`, `docs/ceph.md` | **N** |
| Static-fact policy key `"ControlImage": sharedOpen` (`ceph/shared_gate.go`) | Renamed to `EssentialImage` with the method in the same change (the gate test cross-checks method names) | `ceph/shared_gate.go` | **N** |
| `cephfs.WithMDSImage`, `rgw.WithImage` | **No rename proposed.** Document that they set the role-wide `ImageSet.CephFS` / `ImageSet.RGW` | `ceph/service_hooks.go` (`WithServiceImage` also writes `imageSet.CephFS`/`.RGW`); `cephfs/mirror` `RunWithImages` reads `Images().CephFS`, so `WithMDSImage` also selects the CephFS mirror daemon image | **R** (naming understates scope; owner decision O6) |
| `rbdmirror.Run(ctx, image, ...)`, `cephfsmirror.Run(ctx, image, ...)` parameter `image` | No signature change. Doc: "the mirror **daemon** image: rbd-aux for RBD, cephfs for CephFS" | `rbd/mirror/mirror.go` (`Mirror.image` is used only to start daemons in `AddDaemon`; setup clients come from `config.*.Images().Client` through `multicluster.RunClient`); `cephfs/mirror/mirror.go` (`daemonImage`; setup through `ClientContainer`). `ValidatePair` only checks the argument is non-blank | **R** (see R1) |
| Error text of `CreateClient`/`WithAuth` refusal: "which the control image lacks" (`ceph/auth_config.go`) | "which the essential image lacks" (the probe runs `ceph-authtool` from the first MON image, an essential responsibility) | `ceph/auth_config.go`, `ceph/errors_test.go`, `docs/guides/errors.md` | **N**, but the string is observable (O10) |

### Unexported fields, hooks and helpers

| Old | Proposed | Consumers (from code) | Kind |
| --- | --- | --- | --- |
| `options.controlImage` (`ceph/options.go`) | `essentialImage` | Starts the first MON (`Run`), additional MONs (`runMonitor`, `topology.go`), MGRs (`topology.go`), the control-plane CLI container (`ensureControlPlane`), host-port reservation helpers (`host_port_tracking.go`, `topology.go`), the OSD tmpfs keeper (`osd_memory_storage.go`) and volume helper (`osd_volume_storage.go`), the default `ImageSet` (`images.go`, `facts.go`) | **N**: every consumer is an essential responsibility |
| `options.osdImage` | Fold into `essentialImage` or keep as the per-launch image of the OSD recipe | `configureImages` always sets it to `Essential`; read only by `newOSDLaunchRecipe` (`ceph/osd_launch.go`) | **N** now; touches #64 identity (see Coordination) |
| `options.mdsImage`, `options.rgwImage` | Keep one source of truth: `imageSet.CephFS` / `imageSet.RGW` | Written by `WithServiceImage`, read by `ServiceImage` and copied back and forth by `configureImages`. If `WithMDSImage` is applied before `WithImages`, `configureImages` overwrites `mdsImage` from the set (read from code, not run). | **R** (ordering; needs a unit test before any consolidation) |
| `Container.controlImageID` (`ceph/ceph.go`) | `firstMonitorImageID` (identifies one container generation, not a role) | `Run` sets it from the first MON; used as `OperationError.Image` for the key-type refusal (`auth_config.go`) and the dual-stack admission refusal (`dual_stack_admission.go`) | **N**; #64 may replace it with a per-daemon record |
| `Container.keyTypeOption` comment "control image's ceph-authtool" | "the first MON image's `ceph-authtool` (essential)" | `generateKeyArgs` | **N** |
| `servicehook.ServiceImage(c, role)` / `WithServiceImage(role, image)` role strings `"mds"`, `"rgw"`, `"client"`, `"rbd-aux"`, `"essential"` | Image-role strings: `"cephfs"` instead of `"mds"` | `cephfs/filesystem.go`, `rgw/gateway.go`, `cephfs/entry.go`, `rgw/entry.go`, `ceph/images_test.go`. The blank-image error text is built from the role string (`"MDS image must not be blank"`) and is asserted by `cephfs/entry_test.go`, so keep the text | **N** (mixes a daemon name with image roles) |
| Test hooks `TestState().MDSImage/RGWImage`, `TestCluster` literal `controlImage: "unit-all"` | Follow the field renames | `internal/servicehook`, `cephfs/*_test.go`, `ceph/service_hooks.go` | **N** |
| Local `control` for `ClientContainer` results, "cluster control handle", "cluster toolkit handle" messages | `toolkit` / "client toolkit" | `rbd/mirror/mirror_namespace.go` (`detectNamespaceMapping`, `namespaceMappingUnsupported`), `rbd/mirror/mirror_connection_refresh.go`, `cephfs/*` call sites of `ClientContainer` | **N** (executor is already the client role) |
| `Mirror.legacyClusters` comment "control image's rbd" and `legacyCluster` doc | "the cluster's client toolkit `rbd`" | `rbd/mirror/mirror.go`, `mirror_namespace.go`, `mirror_namespace_test.go` | **N** (the probe runs in `ClientContainer`) |
| Comments naming a control image | the matching role | `ceph/options.go` (`WithNoInitialManagers`: MGR needs essential; OSD in-memory keeper and volume helper: essential), `ceph/mgr_metrics.go` (essential `python3`, run in the control executor), `rgw/gateway.go` ("control image supplies Python"), `rgw/multisite/topology.go` and `zonegroups.go` ("control image unless ... ControlImage": no such option exists; the code uses `Images().RGW` through `rgwSetupImage` and `Config.SetupImage`), `cephfs/mirror/mirror.go` and `rbd/mirror/mirror.go` ("control or all role") | **N** |
| `ceph/admin_socket.go`, `ceph/handles.go` (x3), `ceph/version.go` "OSD role image", "OSD images that have no CLI" | Reword: daemon containers share the run volume; the control executor reaches their socket whatever the daemon image contains. There is no OSD image role now | `AdminSocket` on OSD, MON, MGR | **N** |

### Tests, scripts and fixtures

| Old | Proposed | Consumers | Kind |
| --- | --- | --- | --- |
| `nativeClientImage(t, aggregate)` fed by `cluster.ControlImage()` | Feed `cluster.Images().Client` | 11 native helpers in `internal/integration` | **R** (see R5) |
| `nativeMirrorImage(t, role, aggregate)` fed by `source.ControlImage()` | Feed `source.Images().RBDAux` / `Images().CephFS` | 18 call sites plus 11 locals | **R** (see R1) |
| `cephfs_fuse_mount_integration_test.go` `fuseClientImage` | Probe and return the same role (client); message "client image" | The probe runs `test -x ceph-fuse` in the client container (`m.client`) but returns and reports `ControlImage()` (essential) | **R** (R6) |
| Receipt `image=%s` fed by `ControlImage()` (dual-stack receipts, capability log line) | Log the full `Images()` value; keep receipt key names | 6 receipt lines | **N**; receipts are raw evidence, so only the value source changes |
| `network_delay_integration_helpers_test.go` "official image has neither tc nor ip" using `ControlImage()` as the helper image | Use `Images().Essential` and say it is a deliberate image without `tc` | 2 lines | class 3 negative; **N** |
| `.github/scripts/test_run_image_matrix.py` `test_declared_control_role_never_touches_other_roles` | Rename to `..._essential_only_batch_...` (the test declares `essential`) | one test | **N** |
| `.github/scripts/test_dispatch_cost.py` roles key `"control"`, `test_scenario_history.py` role keys `"control"`, `"osd"` | Use current role names in synthetic rows | `dispatch_cost.py` and `scenario_history.py` read role maps opaquely (`runtime["roles"].values()`, `summary["images"]`); no production code branches on the key | **N**, but run those Python tests; confirm no key is special-cased |
| `.github/scripts/test_tag_scenarios.py` batch named `control` | Rename the synthetic batch | arbitrary name | **N** |

### Documentation

| Where | Statement to correct | Kind |
| --- | --- | --- |
| `README.md` "role images (control, OSD, MDS, RGW)" | "role images (essential, client, cephfs, rbd-aux, rgw, all)" | **N** |
| `docs/contributing/testing.md` "What features may rely on in images" table (`control`, `osd`, `rgw`, `mds`, `all`) and "RGW images need no Python or `ceph` CLI ... control-image container" | Replace with the current six-row summary and a pointer to ROLES.md; the RGW setup runtime now has a `ceph`/Python runtime by contract | **R**: contributors read this to decide where a feature may run |
| `docs/contributing/testing.md` capability table ("`rbd help ...` in the control image"), "same release as the control image", "Squid RGW role image beside a Tentacle control image" | The probe runs in `ClientContainer` (`release_capabilities_integration_helpers_test.go`, `cachedCapability`); say "client toolkit" and "essential" | **N** |
| `docs/guides/native-commands.md` ("Runs in" table: `rados`, `rbd`, `radosgw-admin`, `ceph-authtool`, Python via `ControlContainer`; "every control image has python3"; "Mirror daemons run from the control image"; "the control image is a convenient client image") | `ControlContainer` is the essential `ceph` CLI executor. `rados`/`rbd` belong to the client role (`ClientContainer`), `radosgw-admin` to the rgw role (`Gateway.Admin`, multisite setup clients). Mirror daemons run from rbd-aux or cephfs. Client containers use `Images().Client` | **R** (R3, R4, R5) |
| `docs/guides/host-network.md` ("control image always has `ceph`, `rados`, `rbd` and `radosgw-admin`"; example `cluster.ControlImage()`) | Same correction; the port helper is an essential-image container | **R** |
| `docs/guides/releases-and-images.md` ("control image's `ceph-authtool` decides ...", "`rbd` of each setup client and each cluster's control image", "choose command forms from the control image alone") | `ceph-authtool` from the essential image of the first MON; `rbd` from each setup client and each cluster's client toolkit; command forms from the first MON image and the toolkit | **N** |
| `docs/guides/errors.md`, `docs/guides/contexts-and-cleanup.md` | Same wording corrections; update the examples that pass `ControlImage()` | **N** |
| `docs/rbd-mirror.md` ("`Run` takes an image. It runs the setup CLI clients and the `rbd-mirror` daemons, so it needs the `control` or `all` role. Pass the source cluster's `ControlImage()`") the `Run` parameter list and 10 examples | The argument is the daemon image (rbd-aux, or `all`); setup clients use each cluster's `Images().Client`; examples use `RunWithImages` or `Images().RBDAux` | **R** (R1) |
| `docs/cephfs-mirror.md`, `docs/cephfs.md` (table row "usually `cluster.ControlImage()`") | The daemon image is the cephfs role | **R** |
| `docs/rgw.md` ("The image passed to `Run` is the control image ... `ceph.WithImages` for OSDs"; "control image has Python") and `docs/rgw-multisite.md` ("setup client of each zone (control image)") | Passed image is the essential image; OSDs always use essential; multisite setup clients use the RGW image unless `SetupImage` is set (already stated elsewhere in that page) | **N** (the code is right) |
| `docs/rbd.md` ("included in `control` and `all` images", "`control`/`all` image ships `cryptsetup`", `ControlContainer` runs `rbd`/`rados`) | client role (`libcryptsetup`, `cryptsetup`, `rbd`, `rados` are client capabilities) | **R** (R3) |
| `docs/ceph.md` (18 lines): `ControlImage` section ("used for MONs, MGRs and auxiliary CLI, client and mirror containers ... use it as the image of your client containers"), `CephVersion`, `WithNoInitialManagers`, OSD memory/volume helpers, `WithClient` examples, `RunNativeCommand`, `ControlContainer` | The `ControlImage` section describes a role mix that no longer exists; client examples use `Images().Client`; the rest use "essential" | **R** for the examples and the `ControlImage` section; **N** for the rest |
| `docs/design/api-naming-audit.md` row "Role selection and feature executors" | It says `ControlImage` retains its control-plane meaning. Superseded by O1 and O2: amend the row to name `EssentialImage` and keep `ControlContainer` | **N** |
| `docs/design/*` (container-decomposition, post-termination-diagnostics, osd-volume-storage, bug-compatibility-audit) | Earlier design notes and audit tables that use the accessor names or "detected from the control image" | Update the two live audit tables (`bug-compatibility-audit.md` rows for `rbd mirror pool enable --remote-namespace` and `generateKeyArgs`); leave the dated decomposition notes and add a pointer here |

## Real routing and admission questions the old names hide

The module's role admission is intact: `ImageSet.normalized` refuses `All`
combined with roles and requires `Essential`; `configureImages` refuses initial
RBD, CephFS or RGW services without their role; `ClientContainer` refuses an
empty client role; `rgwSetupImage` refuses an empty RGW role; `startGateway`
refuses an empty RGW role. None of this is touched by renaming. What follows is
where a name, example or comment points an operation at the wrong image.

| # | Question | Evidence | Docs/naming only, or behavior? |
| --- | --- | --- | --- |
| R1 | Which image runs `rbd-mirror` and `cephfs-mirror`, and is the argument checked? | `rbd/mirror.AddDaemon` runs `Mirror.image` with `rbd-mirror`; `RunWithImages` passes the destination's `Images().RBDAux`. `cephfs/mirror.RunWithImages` passes the source's `Images().CephFS`. Guides and 18 native call sites pass the source's essential image, which only works in an aggregate layout. `ValidatePair` checks only that the string is non-blank, so a wrong role fails at container start, not at admission. | Docs and test routing are naming/doc work. A capability or role admission of the supplied image would be a behavior change (O4). |
| R2 | RBD mirror setup clients | Already correct: `Run` takes `config.Source.Images().Client` and `config.Destination.Images().Client` and starts them with `multicluster.RunClient`; the constructor comment and `docs/rbd-mirror.md` still say the one argument supplies them. | Comment and docs only. |
| R3 | What does `ControlContainer` promise? | It starts from `options.controlImage` (essential). The contract lists `rados`, `rbd`, `radosgw-admin` under client and rgw, not essential. Guides and `docs/rbd.md` tell users to run `rados`/`rbd`/`radosgw-admin` in it. Internal callers run `ceph` commands in it (checked for `cephfs/*`, `rbd/mirror_receiver_identity.go`, `internal/multicluster`). | Docs only; no internal caller needs to change. |
| R4 | Mirror admin sockets "run from the control image" | `docs/guides/native-commands.md` says `--admin-daemon` works because mirror daemons run from the control image. They run from rbd-aux or cephfs; both contain a local `ceph` CLI per the contract. | Docs only. |
| R5 | Native client oracles use the essential image | 55 `ControlImage()` call sites; `nativeClientImage` swaps in `CEPH_TEST_CLIENT_IMAGE` only under the roles layout. A user following `WithClient` examples with `ControlImage()` gets an image without the client role under split roles. | Docs and tests. Moving tests to `Images().Client` may make the env switch in `nativeClientImage` redundant for clusters started with a set (O8). |
| R6 | FUSE helper | `fuseClientImage` probes the client container and returns `ControlImage()`. If the probe succeeds the returned image is the essential one, which is not the container that was probed. | Test-only; a behavior change of an optional batch (`cephfs_fuse`). |
| R7 | OSD, RGW and MDS "need not have python3" (`docs/guides/native-commands.md`, `docs/ceph.md` `RunNativeCommand`) | The current requirements table lists `python3` under essential and client and says the rgw role has a Python runtime for setup; it says nothing about cephfs. | Needs the contract owner's answer before the wording changes (O3). |
| R8 | `WithMDSImage` selects the role-wide cephfs image | See the public API table. | Documentation first; any rename is an owner call. |
| R9 | `mdsImage`/`rgwImage` against `imageSet` | See the fields table. | Only a unit test can say; do not consolidate without one. |

## Naming-only versus behavior

- **Naming-only (no native scenario needed):** every N row. They change
  identifiers, comments, prose, test names and Python fixture keys. They are
  verified by compilation, `make check`, the API catalogue checks and the
  affected Docker-free unit tests.
- **Observable but not routing:** the key-type refusal text, the "cluster
  control handle" messages, receipt values. These need the affected unit tests;
  no native run.
- **Possible behavior change (needs a targeted native split-role scenario with
  actual image IDs, original data/state assertions and owned cleanup):** only
  if a later unit adds role admission to the mirror `Run`/`AddDaemon` path
  (R1), changes the FUSE helper (R6), or merges `mdsImage`/`rgwImage` into the
  set in a way that changes precedence (R9). Existing native split-role
  coverage is the optional `role_routing` batch
  (`internal/integration/role_images_integration_test.go`: ordinary RBD without
  optional roles, RGW without a client) and the roles-layout mirror
  scenarios that already route daemons through `nativeMirrorImage`. A rename
  PR must not claim either as new evidence.

## Coordination

- **#146 (static facts).** `FactsOf` and `ClusterFacts` exist. The earlier
  decomposition note counts `ControlImage` among the ten static accessors that
  are to become one function. Do not migrate the 55 `ControlImage()` callers to
  `FactsOf(c).ControlImage`. First migrate each non-essential caller to the
  role accessor it needs (`Images()`), then rename `ControlImage` to
  `EssentialImage` for the remaining essential uses and delete
  `ClusterFacts.ControlImage` in one `api-break` PR, before the
  accessor-to-`FactsOf` caller migration, so no caller moves twice. Old names
  are deleted outright: no alias, no shim and no mapping table (O2).
- **#64 (per-daemon image identity).** The role image reference
  (`ImageSet`) and the image ID of a container generation are different
  facts. `controlImageID` is one value from the first MON at `Run`; after a
  MON, MGR or OSD image change it would describe nothing current.
  `VerificationSource.ImageID` (`ceph/verification.go`; `docs/ceph.md`,
  verification report) already records a per-container ID next to the report's
  role `Images`. Renaming must not collapse the two or introduce a
  cluster-wide "essential image ID". `options.osdImage` is the only place an
  OSD generation's image enters a launch recipe (`newOSDLaunchRecipe`); keep
  that seam as a per-launch image when fields are folded.
- **#216 (metadata).** Use the package-scoped catalogues.
  Public removal touches only `docs/design/api-naming/ceph/` (regenerate for
  the `ceph` package, then approve as a separate step) and `docs/design/api-naming-audit.md`. Pull requests do not edit
  `CHANGELOG.md`.
- **#155/#156/#157 (package moves).** The `rbd/mirror`, `cephfs/mirror` and
  `rgw/multisite` call sites are already in their new packages. The shared
  harness policy map in `ceph/shared_gate.go` names `ControlImage`; it must
  change in the same commit as the method.
- **Retained producers.** Current helpers
  `cephfs_process_quiescence_integration_helpers_test.go` and the recovery
  counterpart are compared declaration by declaration with the retained
  producers after `package-split.json` rewrites them. Changing
  `source.ControlImage()` in those helpers requires one new `calls` entry in
  `package-split.json`; the `.source.go.txt` files stay byte-identical.
- **CI classification of this page.** `docs/design/*.md` is on the
  documentation-only allowlist (`UNREAD_DOC_DIRS` in
  `.github/scripts/docs_only.py`; `docs/contributing/ci.md`, "Documentation-only
  changes"), so a PR that only adds this page runs the documentation-only
  path. The file name must not begin with `api-naming`, which is a read prefix.
  By contrast the API reference pages (`docs/ceph.md`, `docs/rbd-mirror.md`,
  `docs/rbd.md`, `docs/cephfs.md`, `docs/rgw.md`, `docs/rgw-multisite.md`),
  `docs/guides/messenger-client-fixtures.md`, `docs/guides/bug-compatibility-tags.*`
  and any Go or Python file run the full Code checks. A later PR that edits
  those pages is not documentation-only.

## Proposed sequence of implementation PRs

Order is chosen so that no caller is migrated twice and each PR is small and
independently checked.

1. **Guides and README wording (documentation-only CI).** README,
   `docs/guides/*`, `docs/contributing/testing.md`, the research note's
   historical sentence. Correct R3, R4, R5 and R7 claims using accessors that
   exist today (`Images().Client`, `ClientContainer`). First, because it
   removes wrong instructions at no API risk and the examples stop using
   `ControlImage()`.
2. **API reference pages and Go comments (full Code checks, no signature
   change).** `docs/ceph.md`, `docs/rbd-mirror.md`, `docs/rbd.md`,
   `docs/cephfs*.md`, `docs/rgw*.md` and the Go comments in the class 1 table.
   Separate from step 1 only because of CI cost and because these pages are
   API-checked.
3. **Caller migration to role accessors.** Every non-essential
   `ControlImage()` caller (native clients, rbd-aux and cephfs mirror
   daemons, multisite, FUSE helper R6) moves to `Images().Client`/`RBDAux`/
   `CephFS`/`RGW`; receipts log `Images()`. Test-only; `make check` and
   `tag-compile`; package-split rewrite entries as above. No native claim
   from compilation.
4. **Internal renames.** `options.controlImage` to `essentialImage`,
   `controlImageID`, hook state fields, `ServiceImage` role strings, Python and
   comment vocabulary. Unexported only; Docker-free unit tests. Sequenced with
   #146's extractions to avoid shared-file conflicts.
5. **Rename and removal in one `api-break` PR, coordinated with #146.**
   `Container.ControlImage` becomes `EssentialImage` for the remaining real
   essential uses, `ClusterFacts.ControlImage` and the old reference-page
   section are deleted, the `shared_gate.go` key follows, the audit row is
   amended, and the `ceph` catalogue is
   regenerated and then approved as a separate step. No alias, shim or
   old-name mapping. Afterwards no function named `ControlImage` exists.
6. **Option consolidation (R9) and any mirror admission (R1/O4).** Only with
   Docker-free precedence tests; and, for admission, a targeted native
   split-role scenario with actual image IDs.
7. **Final scan guard.** A check that the current-contract wording does not
   regress: it lists retained historical records by path (class 3) and fails
   on `control image`, `ControlImage`, `control or all` or `official-*-control`
   elsewhere, while allowing `control plane`, `control container` and
   `ControlContainer`. If a Go test reads the allowlist file, that file must
   be added to `READ_DOC_FILES`/`READ_DOC_PREFIXES` in `docs_only.py`.

## Open questions and what is not decided

- **O1 (decided).** Do both. Non-essential call sites move to the role
  accessor the pinned contract names; the remaining real essential uses call
  `EssentialImage`, the renamed `ControlImage`. No function named
  `ControlImage` remains.
- **O2 (decided).** Compatibility with published versions is not a concern.
  Retired names are deleted outright, with no deprecated alias, shim or kept
  mapping table. The catalogue process still applies because the API
  surface changes: a package-scoped catalogue regenerate followed by a separate
  approve. The
  class 3 list applies only to text that is a historical record (published
  `CHANGELOG.md` sections, retained fixtures), not to live
  API or documentation.
- **O3.** Whether essential, cephfs and rgw guarantee `python3` for
  `RunNativeCommand` targets. The pinned table says essential and client only;
  the guide text must not move until the image project answers.
- **O4.** Whether `Run`/`AddDaemon` of the mirror packages should refuse an
  image without the daemon (a capability probe) or keep failing at container
  start. Not decided; it would be a behavior change.
- **O5.** Which cluster's `RBDAux` image is canonical for `rbd-mirror`
  (`RunWithImages` uses the destination's; guides imply the source's). Not
  decided.
- **O6.** Whether `WithMDSImage` and `rgw.WithImage` are renamed or only
  documented as role-wide.
- **O7.** Whether released migration guides get a vocabulary note.
- **O8.** Whether `nativeClientImage`/`nativeMirrorImage` remain after tests
  read the role from `Images()`.
- **O9.** The exact name for the first-MON image ID (`firstMonitorImageID` is a
  proposal) and whether #64 replaces it.
- **O10.** The key-type refusal text.

Nothing here selects a new role, adds a fallback from an optional role to
essential, installs packages at run time, rebuilds or republishes an image,
or changes a diagnostic `Role` value or schema.
