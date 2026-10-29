# Public API naming

This page is the review note that the package receipts name. It describes the
package-scoped API catalogues and the naming convention for public API.

## Catalogues

The `declarations.json` file of each public package in [api-naming/](api-naming)
is a source-derived catalogue of package-level exports and declared methods on exported receiver
types in the public packages. Every row names
the current identifier, kind, proposed identifier and decision. Functions and
methods include their **current** Go signature, including returned handles; methods also record their pointer/value receiver.
A proposal for a type also propagates to its declared method names in the
catalogue; signatures are not rewritten to pretend the proposed API exists.

The catalogue includes functions, methods on exported receiver types, types,
constants and variables. It scans all non-test Go source files, including
build-tag alternatives, and refuses conflicting duplicate declarations.
The separate per-package `members.json` catalogues record the
exported field/nested-record/embedding entries. They also scan
declared interface methods; this source has no exported declared interfaces.
Promoted dependency methods are not duplicated. The catalogue does not rename
Testcontainers or standard-library APIs. Implementing a rename still requires
compilation of the resulting method sets and affected callers.

`baseline` means the current name was recorded. Each package's separately issued
receipt (`api-naming/<package>/review.json`) binds that package's two catalogues;
regenerating them cannot silently extend that review to future names. `proposal` means a candidate source
rename, not a compatibility alias or approval to change behavior.

The API reference inventory test also checks this catalogue
against source names and signatures, catches stale proposal keys, and refuses
proposals that collide with each other or a retained declared name. Regenerate
only after inspecting a source/proposal change:

```sh
go run ./internal/tools/apinaming regenerate rgw/multisite
# equivalent test flag:
go test ./internal/apiinventory -run '^TestPublicNaming(Inventory|Members)$' -update-api-naming -api-naming-packages=rgw/multisite
```

Regeneration rewrites only the `declarations.json` and `members.json` of the
named packages (all packages when none is named) and never reads or writes a
receipt. Approval is a separate, explicit command that writes only the named
packages' `review.json`, and only for a catalogue that matches the source:

```sh
go run ./internal/tools/apinaming approve rgw/multisite
```

It refuses a stale catalogue, a pending `proposal` row, and a missing review
note or reference page. The receipt names the review note
(this page by default, `-review` selects another) and the API
reference page of the package (`docs/<package>.md`, with `/` written as `-`).
The other commands are `check` (what the tests verify) and `aggregate` (the
per-package counts and totals, computed on demand and never committed).

Normal `go test ./internal/apiinventory` verifies without modifying files. The
proposal map is `catalogue.Proposals` in
`internal/apiinventory/catalogue/proposals.go`; rationale lives here.
Updating the map/catalogues does not implement any runtime rename. A changed
surface also requires reviewing the names and explicitly approving it; the
regeneration command never edits a receipt.

The public package list is discovered: every directory outside `internal`,
`testdata`, `vendor` and hidden directories with a non-test Go source is a
public package and needs a catalogue directory with all three files. A catalogue
directory without a package fails as well.

## Naming convention

1. Read the full call, including the package and receiver. Use an operation
   verb for a mutation and a noun for its request, observation or returned
   handle. Keep a domain word where the package covers multiple fault or
   service kinds; do not remove every repeated word mechanically.
2. A reversible change of cluster-wide policy is an `override.SetX` function
   (or a service package's `OverrideX` method) that saves the prior state and
   returns a handle with an explicit `Restore`. It implies no expiry,
   context-cancellation rollback or scope-exit cleanup.
   `override.SetFullRatios` is the established example.
3. Use `AddX`, `StartX` or `ApplyX` when the operation owns a new resource or
   disruption rather than adopting arbitrary existing state. The returned
   lease records ownership; its documentation must say who restores it and
   when fixture teardown does so. `Lease` alone does not promise automatic
   restoration, a timer, or interchangeability with another lease type.
4. Keep `Temporary` where native time actually bounds the effect, and document
   that clock independently of explicit restoration. A Restore handle alone
   is insufficient reason for the prefix.
5. Preserve specific undo verbs. `Resume` thaws a process; `Restore` restores
   owned state; `Close` and `Terminate` describe resource or owner lifetimes.
   Do not add a generic cleanup interface or rename these methods into equivalence without reviewing their distinct contracts.
6. Keep native domain operations such as backup restore and one-shot error
   injection distinct from fixture rollback. An injection does not acquire a
   reversible handle merely to match the naming convention.
7. Name stateful management/restoration objects so callers can distinguish them
   from configuration input and observed values. The `override` operations
   return explicit `Handle` types retaining prior state, ownership and possibly
   partial or uncertain application. `Handle` promises no successful apply, timer
   or automatic cleanup. Verb-object operations and noun-phrase types are valid;
   shared words or their order alone are not defects. Do not force past-participle
   names or mechanically suffix unrelated Lease/resource types.
8. Keep standard Testcontainers `Run`, `Container` and functional `WithX`
   options unless a concrete conflict justifies a separate proposal. A
   descriptive query name does not need an added `Get` prefix by default.
9. Domain data-restore verbs such as `rbd.RestoreBackup` and
   `rbd.RestoreIncremental` consume input; they are not cleanup handles and must
   not be converted into a generic cleanup contract.
