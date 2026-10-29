# RGW multisite package boundary: decision and inventory

Status: design decision and source inventory for #157, plus the record of the
package move that implements it (see "Implemented move" at the end). The
inventory sections describe the source before the move.

Inspected source: `main` at 0a65b75 (includes #151, #160 and #161); the first revision of this note used 3fc29fe and the counts were re-checked unchanged. Counts below are measured
at that revision and are evidence for the decision, not targets. The
declaration inventory covers package `rgw` only. Callers and documents outside
the module's `rgw` directory are listed where they must move.

## Decision

Move multisite orchestration into a real child package `rgw/multisite`
(package name `multisite`). The dependency is one way: `rgw/multisite` imports
`rgw`; `rgw` never imports its child. No forwarding layer or deprecated alias
stays in `rgw`, following the direct v0.x migration approach of #146.

Reasons, from the inventory below:

- The multisite family reaches the single-gateway code only through exported
  `rgw` API and one unexported helper (`invalid`). Moving it needs no export
  of private gateway, lock or ownership state.
- The reverse direction has no production dependency. The only code in `rgw`
  that refers to multisite state is `network.go`, which consists entirely of
  `Multisite` methods and moves with the family.
- The `rgw` package page and `go doc` lose 56 declared exports (of 125) that a
  single-gateway user never calls.

Alternative considered and rejected: keeping one package and only grouping the
multisite files in `docs/rgw.md`. It leaves the import surface unchanged and
does not reduce what `go doc rgw` lists. It would be the right choice only if
the move required exporting private gateway internals, and the inventory shows
it does not.

## Evidence

Method: a one-off `go/types` pass over package `rgw` (non-test and test
sources, one run each, repeated on 0a65b75), recording every use of a package-level object, method
or struct field whose definition is in a different file than the use. The tool
is not committed. Results were cross-checked with `go list` import data and
`grep`. Declarations that are not exported (functions, methods, constants,
variables, types and fields) are included in the cross-file check.

### Family definition

The set to move is wider than the `multisite*.go` filename family.

| Group | Files | Moves |
| --- | --- | --- |
| Filename family | 10 non-test files (`multisite.go`, `multisite_bootstrap_staging.go`, `multisite_sync_bucket_status.go`, `multisite_sync_period.go`, `multisite_sync_pipe.go`, `multisite_sync_policy.go`, `multisite_sync_policy_ready.go`, `multisite_sync_status.go`, `multisite_topology.go`, `multisite_zonegroups.go`) | yes |
| Outside the filename family | `network.go` (106 lines): `Multisite.InterruptZoneLink` and `Multisite.ApplyZoneLinkProfile` | yes |
| Filename-family tests | 9 test files (`multisite_*_test.go`) | yes |
| Tests outside the family | `context_lock_test.go` (one test, `TestSerializedMultisiteMutationQueuedDeadline`), `network_test.go` (one test, `TestTemporaryZoneLinkDelayRefusesBeforeAnythingRuns`) | yes |
| Stays | `admin.go`, `bootstrap.go`, `doc.go`, `entry.go`, `errors.go`, `gateway.go`, `placement.go`, `run.go`, `tenant.go`, `tenants_accounts.go`, `tls.go`, `usage.go`, `user_placement.go` and their tests, including `topology_test.go` (gateway start and removal, not multisite) | no |

Other files mention multisite only in comments or in single-gateway scope
handling: `admin.go` (Realm/Zonegroup/Zone flags of the gateway's own native
command, and a refusal to let callers override them), `entry.go` and
`gateway.go` (godoc and `Config.Realm`/`Zonegroup`/`Zone` validation and
environment), `tenants_accounts.go` (godoc). These stay.

### Exported declarations that move

Declared exports in `rgw` at this revision: 125. Of these, 56 move:

| Kind | Count | Declarations |
| --- | ---: | --- |
| Functions | 2 | `RunMultisite`, `RunTopology` |
| Types | 21 | `Multisite`, `MultisiteConfig`, `TopologyConfig`, `Zone`, `ZoneConfig`, `Zonegroup`, `ZonegroupConfig`, `SyncStatus`, `MetadataSyncStatus`, `DataSyncStatus`, `BucketSyncStatus`, `SyncBucketIdentity`, `BucketSyncPolicyStatus`, `SyncGroup`, `SyncGroupConfig`, `SyncGroupStatus`, `SyncFlowConfig`, `SyncPipeConfig`, `SyncPolicyScope`, `SyncBucketSelector`, `SyncObjectTag` |
| Constants | 3 | `SyncAllowed`, `SyncEnabled`, `SyncForbidden` |
| Methods | 30 | 28 in the filename family (on `Multisite` and `SyncGroup`) and 2 in `network.go` |

The remaining 69 stay: `Gateway`, `Config`, `Container`, `Run`, `Start`,
`Gateway.Remove`, `Gateways`, `WithGateways`, `WithImage`, `Lease`, `TenantUser`,
users, accounts, quotas, placement, storage classes, TLS and usage.

### Dependency from the family into the rest of `rgw`

Cross-file uses from moving files into staying files (production code), by
object:

- Types and methods, all exported: `Gateway`, `Config`, `Start`, `Gateways`,
  `Gateway.Remove`, `Gateway.DaemonEndpoint`, `Gateway.UserInfo`,
  `Gateway.PlacementStatus`, `User`, `UserInfo`, `Placement`,
  `StorageClassConfig`.
- Fields, all exported: `Gateway.Container`, `Gateway.AccessKey`,
  `Gateway.SecretKey`, `Config.Name`/`Realm`/`Zonegroup`/`Zone`/`Region`/
  `SkipUserCreation`, plus exported fields of `UserInfo`, `User` and
  `Placement` records (for example `Placement.RealmID`, `ZoneID`,
  `ZonegroupID`, `StorageClasses`).
- Unexported: exactly one, `invalid` (`errors.go`), which wraps an error as
  `ceph.ErrInvalidRequest`. It is a one-line wrapper over the public
  `ceph.OperationError`, so the child package defines its own copy. No
  exported helper is added.

No use of an unexported `Gateway` field (`owner`, `config`, `port`,
`tlsPort`, `publicAddress`, `networkName`), of the gateway registry, or of the
cluster topology lock occurs in a moving file. The test files that move use
only `Gateway.Container`, `Gateway` and exported record types of `rgw`.

Cross-file uses from staying files into moving files (production code): only
`network.go`, which moves. The two moving tests outside the filename family
call into `Multisite` internals and move with it. No staying file refers to a
moving declaration.

Consequence: no new exported lock, ownership or registry API is needed. The
decision to keep `Gateway`'s private state private is unchanged.

### Import graph

Current (production):

```text
rgw -> ceph, fault, internal/{clusterutil,dockerbridge,multicluster,naming,servicehook}
internal/multicluster -> ceph, fault, internal/servicehook
internal/clusteraccess -> ceph, rgw          (test helper)
```

After the move:

```text
rgw/multisite -> rgw, ceph, fault, internal/{clusterutil,dockerbridge,multicluster,servicehook}
rgw           -> ceph, internal/{clusterutil,servicehook,naming}
```

Neither `ceph`, `fault`, `internal/multicluster` nor `internal/servicehook`
imports `rgw`, and `fault` mentions multisite only in documentation comments,
so the move creates no cycle. In the current source only moving files import `fault` (`network.go`),
`internal/dockerbridge` and `internal/multicluster` (all in the family), so `rgw`
should drop these three imports after the move; this is derived from the import
lists and unconfirmed until compiled. `internal/multicluster` is shared with
RBD and CephFS mirroring (#156) and stays in place.

### Ownership decisions

- Gateway lifecycle: `rgw.Gateway`, `rgw.Start`, `(*rgw.Gateway).Remove` and
  `rgw.Gateways` stay in `rgw`. `multisite.Fixture.Source`/`Destination` and
  `Zone.Gateway` keep the type `*rgw.Gateway`. No second gateway abstraction.
- Realm, zonegroup and zone selection for an ordinary gateway
  (`rgw.Config.Realm`/`Zonegroup`/`Zone`, their name validation and the
  `--rgw-*` flags and environment): stays in `rgw`. They are gateway
  startup inputs and exist independent of a multisite fixture. The fixture
  fills them in; it does not own them.
- Realm/period/zone identities, epochs, metadata master, endpoint
  generation, bootstrap staging, sync policy, bucket/account/tenant identity
  translation and the native status parsers (`radosgw-admin sync status`,
  `bucket sync status` and policy readiness): move with `Multisite`. Nothing
  in `rgw` consumes them.
- `rgw.Placement` realm/zone/zonegroup IDs stay with the placement record in
  `rgw`. The fixture reads them through `Gateway.PlacementStatus`.
- Shared S3 test credentials: `Gateway.AccessKey`/`SecretKey` stay in `rgw`.
  The fixture's practice of sharing one test user across gateways is built on
  `Config.SkipUserCreation` and is implemented in the child package.
- Cleanup state: the fixture's owned resources (setup clients, HTTP bridge,
  per-zone gateways) stay tracked by `internal/multicluster`. Per-cluster
  gateway ownership stays in `rgw`'s registry, which the child package reaches
  only through `Start`, `(*Gateway).Remove` and `Gateways`.
- Lock order: topology and scope lock order, period commit/reload ordering,
  cancellation, removed-zone settlement and uncertain-outcome behavior are
  internal to the moving files and are moved without edits to their logic.

### Names

The names below were proposals in the first revision and are the names the move
implements. The `Multisite` type name would stutter as `multisite.Multisite`,
so it is `multisite.Fixture`.

| Current | Proposed |
| --- | --- |
| `rgw.RunMultisite` | `multisite.Run` |
| `rgw.RunTopology` | `multisite.RunTopology` |
| `rgw.Multisite` | `multisite.Fixture` (alternative: keep `Multisite`) |
| `rgw.MultisiteConfig` | `multisite.Config` |
| `rgw.TopologyConfig`, `Zone`, `ZoneConfig`, `Zonegroup`, `ZonegroupConfig` | same names in `multisite` |
| `rgw.SyncStatus`, `MetadataSyncStatus`, `DataSyncStatus`, `BucketSyncStatus`, `SyncBucketIdentity`, `BucketSyncPolicyStatus` | same names in `multisite` |
| `rgw.SyncGroup`, `SyncGroupConfig`, `SyncGroupStatus`, `SyncFlowConfig`, `SyncPipeConfig`, `SyncPolicyScope`, `SyncBucketSelector`, `SyncObjectTag`, `SyncAllowed`, `SyncEnabled`, `SyncForbidden` | same names in `multisite` |
| All 30 methods | same names on the moved receiver |

The implementation change also records the old-to-new mapping in the v0.4
upgrade guide and CHANGELOG, as for earlier moves.

## Work the implementation change must include

Evidence from this inventory; the "Implemented move" section records how each
item was done.

- Callers: 14 test files under `internal/integration` name a moving type or
  function (`rgw.Multisite`, `rgw.RunMultisite`, `rgw.RunTopology`, `rgw.Zone`,
  `rgw.Sync*` and similar). Their `Test` function names and `//ci:` lines stay,
  so scenario planning, batch grouping and `Makefile` selectors are not
  affected by the move itself. `internal/clusteraccess` does not use the
  moving declarations.
- Docs: `docs/rgw.md` sections from "RunMultisite and RunTopology" through
  "Multisite Types" and the multisite examples move to a page for the child
  package; `docs/rgw.md` keeps a link. `README.md`, `ceph/doc.go`, `rgw/doc.go`,
  `docs/guides/*` and `docs/design/api-naming.md` mention multisite and need a
  consistent update.
- `internal/apiinventory`: its public package table is keyed by directory and
  page, and the naming inventory and member catalogue are regenerated from it.
  Adding `rgw/multisite` needs an explicit decision on the package label
  (`multisite.` against `rgw/multisite.`) before `-update-api-naming` is run,
  because the receipt binds both catalogues. Unconfirmed: whether the tool
  can take a nested directory without change.
- `.github/scripts/fixtures/package-split.json` rewrite rules cover earlier
  `ceph.*` to `rgw.*` moves. Retained producer sources use the old `rgw.*`
  multisite names, so new rewrite rules are likely needed. The assertions stay
  as strict as now. Unconfirmed until the fixture is run against the move.
- `.github/scripts/run_image_matrix.py` hashes `rgw/**/*.go`, so a nested
  directory is already covered (unconfirmed for any other path-based tooling).

## What the first revision did not cover

- No code, API, test or CI change, and no native run. The statements above are
  static analysis of one revision.
- Coordination with #144 (public API moves) and #146 (core state and locks)
  was not performed there. No `ceph` package declaration is touched by this
  design or by the move.

## Internal preparation (no public change)

Before any public move, the family is separated inside package `rgw`
without changing an exported name or behavior:

- `network.go`, `network_test.go` and `context_lock_test.go` (whose single test
  covers `Multisite` mutations) are renamed to `multisite_network.go`,
  `multisite_network_test.go` and `multisite_context_lock_test.go`, so the
  filename prefix matches the family listed above.
- The family no longer calls the unexported single-gateway helper `invalid`.
  It uses its own copy, `multisiteInvalid` in `multisite_errors.go`, with the
  same error kind. This removes the only unexported dependency listed above.
- `TestMultisiteFamilyBoundary` fails when a `multisite*` file refers to an
  unexported package-level declaration of a single-gateway file, or when a
  single-gateway file refers to a package-level declaration of the family.
  It is a static check on package-level identifiers (not fields or methods).

Both open decisions were recorded before the move: the destination names are
those in the table above, and the package label for `internal/apiinventory` is
the directory, `rgw/multisite`.

## Implemented move

The family listed above moved to `rgw/multisite` with `git mv`, so history
follows the files. The unit tests moved with it. The files lose their
`multisite_` prefix because the package name carries it (`multisite.go` is
`fixture.go`, `multisite_errors.go` is `errors.go`).

- Source changes in the moved files are limited to the package clause, the
  `rgw.` qualifier on the single-gateway declarations listed under
  "Dependency from the family into the rest of `rgw`", the three renames, and
  the comment text that named them. No logic, lock order, error text or
  timeout changed.
- `TestMultisiteFamilyBoundary` is replaced by the compiler: a moved file can
  only reach exported `rgw` API, and `TestPackageDoesNotImportMultisite` in
  `rgw` fails when `rgw` imports the child package. The measured import graph
  matches the prediction above: `rgw` imports `ceph` and
  `internal/{clusterutil,naming,servicehook}`; `rgw/multisite` imports `rgw`,
  `ceph`, `fault` and `internal/{clusterutil,dockerbridge,multicluster,servicehook}`.
- `multisiteInvalid` stays as the child's own copy of `invalid`.
- The 14 integration test files that name the moved declarations changed only
  their qualifiers and imports. Their `Test` function names, build tags and
  `//ci:` batch lines are unchanged, so scenario planning, batch grouping and
  Makefile selectors still resolve.
- `internal/apiinventory` keys packages by directory, so the label is
  `rgw/multisite`, and the tool needed only a new `packages` entry for
  `docs/rgw-multisite.md`. The catalogues were regenerated with
  `-update-api-naming` and the receipt was refreshed by hand after checking that
  the 56 moved entries map one to one onto the new package (125 `rgw`
  declarations before; 69 plus 56 after; 204 members before; 91 plus 113 after).
- `.github/scripts/fixtures/package-split.json` keeps its rewrite keys (the
  retained producers still use the old `multicluster.*` spellings) and its
  targets now name `multisite.*`. The scenario quiescence tests that apply the
  rewrites pass unchanged, so the retained sources are compared as strictly as
  before.
- Documentation: `docs/rgw-multisite.md` holds the moved reference, examples,
  native-command and selective-sync sections; `docs/rgw.md` links to it.

Not covered: no native run of the moved scenarios is claimed beyond what CI
reports for the exact head of the change that carries this move, and the move
does not touch #146 core state or the `ceph` package.
