# Design: contributor metadata ownership and integration cost

Design note for #216. It inventories the shared bookkeeping files that
concurrent pull requests edit, selects an ownership policy per family, and
orders the implementation into small reviewable changes. It changes no product
code, no catalogue, no changelog entry and no CI policy. Every file name and
layout below that does not exist today is a proposal for the implementing
changes to confirm or reject.

Source snapshot: `main` at the merge of #211 (`f371839`). Counts come from
reading the files and from `git log` on the first-parent history. A count of
commits that touched a file measures how often a file is edited, not how often
two pull requests actually collided.

## What the evidence shows

### Edit frequency

First-parent history of `main` from 2026-10-20 to the snapshot has 175 entries
(162 merge commits). Entries that touched each shared file:

| File | Entries | Share |
| --- | ---: | ---: |
| `CHANGELOG.md` | 42 | 24% |
| `docs/ceph.md` (package reference page) | 29 | 17% |
| `docs/design/api-naming-inventory.json` | 23 | 13% |
| `docs/design/api-naming-review.json` | 16 | 9% |
| `docs/design/api-naming-audit.md` | 15 | 9% |
| `docs/design/api-naming.md` | 11 | 6% |
| `docs/design/api-naming-members.json` | 10 | 6% |
| `internal/integration/testdata/messenger_coverage.json` | 6 | 3% |
| `.github/scripts/fixtures/package-split.json` | 5 | 3% |
| `.github/ceph-role-images.json`, `.github/scripts/batch_selection_explained.json`, `internal/integration/testdata/messenger_evidence.json` | 2 each | 1% |
| `docs/guides/bug-compatibility-tags.json` | 1 | under 1% |

The first four rows dominate. The registry and coverage files at the bottom are
rarely edited, so splitting them would add structure without removing measured
contention.

### Four pull requests that overlapped

| PR | Edited shared files | Conflict-resolution heads |
| --- | --- | --- |
| #210 (rgw/multisite move) | CHANGELOG, inventory, members, review, audit, `package-split.json`, the `packages` map in `inventory_test.go` | none (merged first) |
| #211 (`ceph.FactsOf`) | CHANGELOG, inventory, members, review | `6ac89ac`: textual conflict in the review receipt; CHANGELOG and both catalogues re-combined |
| #212 (rbd/mirror move) | CHANGELOG, inventory, members, review, audit, `package-split.json`, `inventory_test.go` | `f5a24e3`: "keep both entries, regenerate the naming catalogues and refresh the review receipt"; open and conflicting again at the snapshot |
| #214 (observer) | CHANGELOG, inventory, review | `e3891e6` and `0d0313c`: each regenerated the inventory and updated the receipt |

The four API-bearing pull requests touched different API families (a package
move, a new function in `ceph`, a package move in `rbd`, a new type in `ceph`)
but every one of them edited the same changelog section and the same
repository-wide catalogue and receipt. A fifth open pull request (#215, three
integration test files) edited no shared bookkeeping file and needed no
conflict-resolution head. That is the control: the collisions come from the
shared files, not from concurrency itself.

Each of the four conflict-resolution heads started the same set of seven
workflow runs as a normal push (the release matrix is skipped). Workflow-run
spans (creation to last update, which includes queueing and is not billed
time): for `6ac89ac`, Code checks ran about 19 minutes and Ceph short about
23 minutes; for `e3891e6`, Code checks ran about 21 minutes and its Ceph short
run was cancelled. Those heads also merged `main` code into the branch, so the
spans are an upper bound on what the bookkeeping conflict itself cost.
`ci_measurements.py` excludes cancelled runs and counts only first attempts, so
it is the tool for the real comparison (see [Measurements](#measurements)).

Why a head had to be created at all: the `main protection` ruleset requires six
Code checks with `strict_required_status_checks_policy` set to false. A branch
need not be up to date to merge, so a pull request whose files do not collide
textually merges on the head that was already validated. Only a textual
conflict forces the author to merge `main` and start another cycle. Removing
incidental conflicts therefore removes the extra head and its cycle, and it does
so without any change to what CI executes.

That also states the limit. With a non-strict policy, two independent pull
requests are combined for the first time by the push-to-`main` run, which runs
every check. This is already true for every pull request that does not
conflict. The proposal keeps it and does not claim to add a pre-merge combined
run.

## Inventory by family

Classes: authored input (humans edit it and checks read it), source-derived
output (a command regenerates it from code), review evidence (a human states
that a reviewed state is acceptable), historical receipt (records something that
happened and is only appended to).

| Family | Class | Producer | Consumers | Merge point today | Contention |
| --- | --- | --- | --- | --- | --- |
| `CHANGELOG.md` (Unreleased section, about 780 lines of a 1,179-line file) | authored input, also a historical receipt once released | hand edit in each PR | readers; `docs/contributing/releasing.md` copies from it for release notes; `test_scenario_shards.py` only asserts it is a runtime-ignored path | one shared section, appended to by every PR | high |
| `api-naming-inventory.json` (declarations) | source-derived output | `namingInventoryFromSource` via `-update-api-naming` in `TestPublicNamingInventory` | `TestPublicNamingInventory` (byte equality), `TestNamingReviewBindsCurrentSurface` | one array sorted by package, one `counts` map, one file | high |
| `api-naming-members.json` | source-derived output | `namingMembersFromSource` via the same flag in `TestPublicNamingMembers` | the same two tests | one map, one `counts` map | medium |
| `api-naming-review.json` | review evidence | edited by hand (the audit text records the receipt as "refreshed by hand") | `TestNamingReviewBindsCurrentSurface`: two global SHA-256 values, two count maps, a `review` file name | every API change must change the two global hashes | high; any two API pull requests collide |
| `api-naming-audit.md`, `api-naming.md` | review evidence (prose and a hand-kept count table) | hand edit | the receipt's `review` field (existence only); `docs_only.py` treats the `docs/design/api-naming` prefix as read | one count table and one append-only narrative | medium |
| `packages` map in `internal/apiinventory/inventory_test.go` | authored input | hand edit when a public package appears | every apiinventory test | one Go map literal | medium (edited by #210 and #212) |
| `docs/<package>.md` pages | authored input | hand edit | `TestPackagePagesNameEveryExport` | already one file per package | real same-package overlap only |
| `.github/scripts/fixtures/package-split.json` | authored input and historical provenance (rewrites retained native producers to current names) | hand edit; `docs/design/shared-harness-boundary.md` says `-update-api-naming` regenerates it, but `naming_test.go` does not reference the file, so that sentence is not backed by code | `package_split` in `test_scenario_quiescence.py` and the recovery tests | one `identifiers` map, one ordered `calls` list, one `description` string | medium (edited by #210 and #212) |
| `bug-compatibility-tags.json` | authored input | hand edit | `BUG_REGISTRY` in `tag_scenarios.py`, `bug_binding_history.py` | one registry | low (1 entry in the window) |
| `messenger_coverage.json`, `messenger_evidence.json`, `messenger_reference.json` | authored input, authored bindings, and source-derived output respectively (the reference is regenerated by `messenger_reference.py --output`) | hand edit, hand edit, script | `messenger_coverage_fixture_test.go`, `messenger_evidence.py`, the embedding helper test | one file each | low |
| `.github/ceph-role-images.json` | authored input (image digests owned by `ceph-testcontainers-images`) | image publication | `run_image_matrix.py`, the role-image tests | one map | low |
| `batch_selection_explained.json`, `diagnostic-rewrites.json`, `role-overhead-results.json` | historical receipts | append when a case is explained | `replay_declaration_selection.py`, the diagnostic rewrite helpers, `role-overhead.md` | one file each | low |
| Plan JSON (`tag-plan.json`) | source-derived output | `tag_scenarios.py plan` from test build tags and `//ci:` headers, saved as a CI artifact (`code-tag-coverage`) | the dispatch jobs | none: no plan file is tracked in the repository | none |

Two consequences.

1. The plan JSON is already in the target state. It is never committed and
   nobody edits it, so there is nothing to split. The issue's wording matches the
   code here.
2. The review receipt's two global hashes are the strongest source of
   collisions. They change on every API change, so two pull requests that touch
   different packages still conflict on a value that cannot be merged by hand;
   each resolution re-derives the hashes. That regeneration is also where
   "regenerating must not approve" matters most.

## Selected policy per family

### CHANGELOG: release fragments with release-time assembly

Alternatives:

| Option | Removes the conflict | Keeps migration links | Review burden | Verdict |
| --- | --- | --- | --- | --- |
| Status quo (shared Unreleased section) | no | yes | none new | rejected: 42 of 175 entries |
| Fragment per change, assembled at release time | yes | yes | one extra file per release-worthy change | selected |
| Fragments plus a committed aggregate regenerated per PR | no (the aggregate is the shared file again) | yes | high | rejected |
| Notes generated from PR descriptions at release | yes | not verifiable in the repository | no committed review evidence, depends on GitHub state | rejected as the source; usable only as a hint during editing |
| One changelog per package | partly | yes | cross-package changes touch several files | rejected: moves and cross-cutting changes are the common case |

Layout (proposal): `docs/changes/<change-id>.md`. The directory sits under
`docs/` on purpose. Every runtime workflow already has `docs/**` in its
`paths-ignore` for push and pull request, so a fragment-only pull request does
not start Docker, Ceph short, topology, recovery or multicluster runs. A
top-level `changes/` directory would need that list changed in the six workflow
files that carry it (Docker checks, Ceph short, topology, recovery,
multicluster and the release matrix) and would be a CI policy change by itself.

Change identifier. The issue number is not unique (#146 and #156 each have many
partial pull requests) and the pull request number does not exist before the
pull request is opened. Adding it afterwards would force an extra push and an
extra cycle, which is exactly what this work removes. The identifier is
therefore author-chosen and validated: `<issue>-<slug>` or `<topic>-<slug>`,
lowercase `[a-z0-9-]`, at most 64 characters, and equal to the file name stem
and to the `change:` front-matter field. The pull request number is recovered
at assembly from the commit that introduced the file; it is not stored in the
fragment. Uniqueness is by construction (one file per name) and checked
case-insensitively. A second author picking the same name produces an add/add
conflict in Git, which is the correct loud outcome for a genuine collision.

Front matter (proposal):

```
---
change: 146-observer-object
kind: api-break | feature | behavior | compatibility | scope | none
migration: docs/guides/upgrading-example.md   # required for api-break
supersedes: [146-older-change]                # optional
reason: why no release note is needed         # required when kind is none
---
User-facing text. State what a consumer must change or can now do.
```

Rules:

- Discovery is by directory listing. No index, count or hash file is edited.
- Release-worthy items are the four classes the issue names: breaking API or
  import changes with a migration link, user-visible behavior and fixes,
  compatibility and requirement changes, and meaningful supported-scope changes.
  Internal refactors, synthetic fixtures and research progress need no fragment.
- A justified "no note" decision is itself a fragment with `kind: none` and a
  `reason`. It lives in the pull request's own file, so no shared exemption list
  exists. It is required only when a check can tell the public surface changed
  (see the API policy below); until that check exists the format is validated
  but a missing fragment is not an error.
- `supersedes` must name a fragment that exists in the tree. Cycles, self
  reference and duplicates are rejected. Assembly omits superseded fragments.
  A later change that replaces an earlier unreleased entry deletes or supersedes
  it in its own pull request, which is a real semantic overlap and may need
  coordination.
- Assembly sorts by `kind` in a fixed order and then by `change`, so output does
  not depend on file system order, merge order or commit time.
- Published history is not rewritten. Released sections (`v0.3.0`, `v0.2.0`,
  `v0.1.0`) stay byte for byte; assembly compares them with the previous tag's
  file and refuses to proceed on a difference. After a release the consumed
  fragments are deleted in the release pull request and each assembled entry
  carries its identifier, so a later fragment cannot reuse a released
  identifier (the validator checks released sections for the identifier).
- A fragment does not replace a migration guide. For `api-break`, the
  `migration` link must resolve to an existing `docs/guides/` file, which keeps
  the link requirement the current Unreleased section satisfies by convention.
- Release assembly is owned by the release preparation change described in
  `docs/contributing/releasing.md`: the release owner runs the assembler,
  edits the draft into a compact, user-focused section and reviews it. The
  assembler is a generator, not an approver. Editorial deduplication is a
  reviewed human step, so the output is a draft that the release pull request
  diff makes reviewable.

### API catalogues and receipts: package-scoped files, aggregate on demand

Alternatives named in the issue:

| Option | Unrelated churn | Reviewability of an API change | Keeps stale-review rejection | Verdict |
| --- | --- | --- | --- | --- |
| 1. Package-scoped catalogues and package-scoped reviewed receipts; aggregate computed on demand | removed for different packages | the diff shows only the touched package's rows | yes, per package | selected |
| 2. Catalogues generated in CI as artifacts; separate approval | removed | reviewers lose an in-PR diff; the approval must bind to a hash of content they cannot read in the PR | needs a hash-only receipt | rejected as the primary design: opaque approval |
| 3. Keep global files, add a dedicated integration owner | no, it serializes the same bottleneck | unchanged | yes | rejected: moves the wait to a person |

Option 1 is selected because the existing tests already iterate per package.
`namingInventoryFromSource` and `namingMembersFromSource` loop over
`slices.Sorted(maps.Keys(packages))`, and the count checks, the
reference-page check (`exported(t, pkg)`) and the proposal-collision check
(`targets`, keyed by the package-qualified name) are already per package. The
only truly global data are the single JSON documents and the two hashes over
them.

Layout (proposal), one directory per public package keyed by import path
relative to the module:

```
docs/design/api-naming/<package>/declarations.json
docs/design/api-naming/<package>/members.json
docs/design/api-naming/<package>/review.json
```

For example `docs/design/api-naming/rgw/multisite/declarations.json`. Each
`review.json` holds the receipt for that package alone: schema, package, the two
SHA-256 values over that package's catalogue serialization, the two counts, the
review note it relies on, and the page it is checked against (replacing the
hand-kept `packages` map). The package list is discovered: every non-internal
directory with non-test Go sources is a public package and must have a
catalogue directory and a receipt. This is stricter than today, where a new
public package is invisible to the catalogue until someone edits the `packages`
map. A missing package, a catalogue directory without a package and a
missing page all fail.

The aggregate (totals, the combined table that `api-naming-audit.md` keeps by
hand) is computed by a command and is not committed. It is a view for reviewers
and for the audit's count table, whose hand-maintained table is replaced by a
pointer to the command. No global index, count or hash file remains to edit.

Preserved properties, each tied to an existing check:

- Source binding and missing/stale detection: the byte-equality test in
  `TestPublicNamingInventory` and `TestPublicNamingMembers`, run per package.
- Stale review rejection: `TestNamingReviewBindsCurrentSurface`, run per
  package against that package's receipt. A source change in package A makes
  only A's receipt stale.
- Name collision and stale proposal checks: unchanged, still keyed by the
  qualified name.
- Reviewed exceptions: `namingProposals` stays a reviewed input; a row with
  decision `proposal` still blocks a finished review.
- The conflicting-duplicate refusal for build-tag alternatives stays in the
  generator.

What this does not remove: two changes to the same package still edit that
package's catalogue and receipt, and still conflict. That is a real overlap in
the same API and keeps the integration review the issue asks to retain.

Accidental serialization. Today any two API pull requests conflict on the
global hash. That has been the only thing forcing the second one to re-run
against the first. After the split, pull requests on different packages merge
without a textual conflict, and what they have in common is checked by the
push-to-`main` run and by the per-package tests on the merged tree. Catalogue
rows are textual and package-local, so the merged tree of two pull requests on
different packages is made of two package states that each passed on their own.
Cross-package consistency (a move) is covered below.

### Regeneration stays separate from approval

Two commands with disjoint outputs (names are placeholders):

- Regenerate: `-update-api-naming` writes only `declarations.json` and
  `members.json`, only for the packages named or changed. It never reads or
  writes `review.json`. It keeps refusing a conflicting duplicate or a
  collision before writing.
- Approve: a separate flag writes only `review.json` for named packages, and
  refuses unless the catalogue on disk matches the source (no stale catalogue),
  no row has decision `proposal`, and the named review note exists. Today the
  receipt is edited by hand; a command only removes the manual hash arithmetic,
  and the approval act remains a deliberate, separately reviewed edit.

CI runs neither flag. It only checks. A pull request that regenerates without
approving fails `TestNamingReviewBindsCurrentSurface`, exactly as it does now. A
reviewer sees the receipt change as its own diff hunk in the package directory,
next to the review note, instead of two hashes in a shared file.

### Cross-package moves are one reviewed migration

A move edits at least two packages. Rules:

- Both packages' catalogues and both receipts change in the same pull request.
  With per-package receipts this is automatic: either side left stale fails its
  own test.
- A move record, `docs/design/api-naming/moves/<change-id>.json`, lists
  `{from, to}` pairs. A static check requires each `from` to be absent from its
  source package catalogue and each `to` present in its target catalogue, and
  that both packages' receipts name the move's review note.
- A pull-request-only check compares the catalogues with the merge base and
  requires that every removed row is a `from` in a move record or is listed as a
  reviewed removal in the review note, and every added row is a `to` or listed
  as a reviewed addition. Without the base comparison a vanished name would be
  indistinguishable from a rename. This is the one check that needs the base
  commit and so cannot run on pushes; it is skipped, not passed, when no base is
  available, and the per-package tests still run.
- `package-split.json` entries for the same move are added in the same pull
  request (next section).

The existing precedent is #210: the audit records that the combined `rgw` and
`rgw/multisite` counts equal the former `rgw` counts. A move record turns that
prose claim into a checked statement.

### `package-split.json`: shard by move, refuse chains

The file is a historical rewrite map applied by `package_split` to retained
native producer sources. It is not a regenerated catalogue and is not read by
`naming_test.go`. Its three parts behave differently:

- `identifiers` are applied sequentially in order of decreasing key length, so
  the keys behave as a set only while no rewrite target is itself the key of
  another entry. A later move that moves a name an earlier move had already
  rewritten would compose two rewrites in an order-dependent way.
- `calls` are regular expressions applied in list order, so order is part of
  the meaning.
- `description` is one prose string naming the whole move set, so every move
  edits it.

Proposal: one shard per move, `.github/scripts/fixtures/package-split/<change-id>.json`
with its own identifiers, calls and description, merged by the loader. The
loader refuses a duplicate key, a key that is also a target of another shard
(a chain), and a call list whose result depends on shard order (the check
applies the shards in two orders to the retained producer corpus and requires
identical output). A chain is reported as a semantic overlap: the later move
edits the earlier shard's target in its own pull request, which is the one
coherent reviewed migration. The loader keeps the sequence `identifiers` then
`calls` then `gofmt`, so every byte not named by a shard stays significant.
Correcting the `shared-harness-boundary.md` sentence about regeneration belongs
to the implementing change.

### Coverage, registry and receipt JSON: leave as is, with triggers

Per the edit counts above these files are not contested now. Splitting them
would add loaders and validators without a measured benefit, and several are
single-owner by design (`ceph-role-images.json` mirrors the images repository).
Split a file only when a re-measured collision shows independent ownership and
the file can be split without losing complete discovery, strict identity
validation, duplicate or conflicting entry refusal, deterministic ordering and
reference integrity. `batch_selection_explained.json` and the other historical
receipts are keyed by unique identifiers and would be the first candidates; a
directory of per-entry files with duplicate-key refusal is the natural form.
Historical provenance fixtures keep their exact meaning and are not merged.

## CI classification

### What `docs_only.py` does now

`is_unread_doc` returns true only for: top-level `*.md` and `LICENSE`, and
Markdown files that are direct children of `docs/contributing`, `docs/design`,
`docs/guides` and `docs/research`, minus `READ_DOC_FILES` and
`READ_DOC_PREFIXES` (`docs/design/api-naming*`). Everything else, including a
nested path, a non-Markdown file under `docs/` and any `.github` path, runs
the full Code checks. `decide` returns `docs_only=false` for non-pull-request
events, forks, a missing base, an empty list and any error. The changed files
are `git diff --name-only --no-renames` between the base and the head, so a
rename shows as a deletion and an addition. `ReaderInventoryTests` in
`test_docs_only.py` scans Go sources and `tag_scenarios.py` for
`docs/...(md|json)` strings and fails when one is on the allowlist.

The design page itself is `docs/design/contributing-metadata-ownership.md`:
a direct Markdown child of `docs/design`, not matching `docs/design/api-naming*`,
so `is_unread_doc` is true and a pull request containing only this page is
documentation-only. That was checked against the function, not assumed.

### Required changes for the proposed layout

1. Fragments. Add `docs/changes` to `UNREAD_DOC_DIRS`. A fragment is
   Markdown that no Go test or planner reads, and the existing runtime
   `paths-ignore: docs/**` already keeps native workflows off. The fragment
   validator is a Python script, not a Go test, so adding it does not make
   fragments a "read" file; it does not change the heavy-check decision.
2. Catalogue files stay read. `docs/design/api-naming/<package>/*.json` is
   nested and non-Markdown, so `is_unread_doc` is already false and the full
   Code checks run for them. This is the intended result: a Go test reads these
   files and the move would otherwise hide them. The `READ_DOC_PREFIXES` entry
   for `docs/design/api-naming` can stay for the Markdown review notes, which
   `TestNamingReviewBindsCurrentSurface` checks for existence. New review notes
   under `docs/design/api-naming/` are nested, hence not allowlisted, hence full
   checks (the conservative result).
3. Generalize `ReaderInventoryTests` so a new reader of any path under the new
   directories fails the test. The current expression already matches
   `docs/changes/...md` and the catalogue JSON, which is why those directories
   stay under `docs/`. If a layout ever moves outside `docs/`, the expression
   and the allowlist must change in the same pull request. That is the
   "moved file must not become unread" control.
4. Run the fragment and metadata validator on every pull request at the head,
   including when `docs_only=true`. A skipped job reports success, and
   `Detect documentation-only changes` is not one of the six required checks,
   so a failure there would not block merging. The workflow already has a
   precedent in the vulnerability matrix: the job always runs and each step is
   gated. The same shape applies to `Vet, compile and CI tooling` (a required
   check): remove the job-level `docs_only` gate and gate each existing step
   instead, then add the checkout and the validator as the ungated steps. The required check
   names do not change. Adding a new required context instead would need a
   change to the `main protection` ruleset settings, which is the maintainer's
   decision.
5. Add symlink and mode awareness for the allowlisted classes. `git diff
   --name-only` does not show that `docs/changes/x.md` is a symlink to a code
   file. The validator should require regular files (mode 100644) for
   fragments; the same hole already exists for the current allowlisted
   Markdown and should be recorded for the implementing change.

### Lightweight current-head validation

Today a documentation-only head (this design PR, run 40430609561, head
`80542f4`) has this disposition, read from the run's jobs and `code.yml`:

| Required context | Today with `docs_only=true` |
| --- | --- |
| `Unit tests` (121947200709), `Race detector` (121947200168), `Vet, compile and CI tooling` (121947200786), `Compiled tag coverage and helpers` (121947199828) | the job is skipped by its job-level condition and reports success; no step ran |
| `Vulnerability check (Go 1.26.x)` (121947198335) and `(Go 1.27.x)` (121947198409) | the job executes and reports success in seconds, because every step is gated on `docs_only`; no scan ran |
| `Detect documentation-only changes` (121947055442, not a required context) | executed: it ran `docs_only.py` |
| Runtime workflows | not started: only the Code checks run exists for that head |

So today two required contexts execute as jobs but run no scan, four are
skipped jobs, and no step validates the content of a documentation-only head
beyond the classifier. None of them is a new native or scan PASS for that head.

The proposed disposition for a head whose net change is only fragments and
other allowlisted documentation differs in one context:

| Required context | Proposed with `docs_only=true` |
| --- | --- |
| `Vet, compile and CI tooling` | the job executes (its job-level `docs_only` gate is replaced by per-step gates, as the vulnerability jobs already do); the checkout and the fragment and assembly validator are the ungated steps, and vet, lint, tag compilation, the Python tooling tests and actionlint stay skipped by policy |
| `Vulnerability check (Go 1.26.x)` and `(Go 1.27.x)` | unchanged: executed jobs whose steps are skipped |
| `Unit tests`, `Race detector`, `Compiled tag coverage and helpers` | unchanged: skipped jobs that report success |
| `Detect documentation-only changes` | executed, unchanged |
| Runtime workflows | not started (`docs/**` ignored) |

The validator result is a real result for this head, and only that. The
skipped-by-policy jobs and steps are not reported or described as having
executed, and a green `Vet, compile and CI tooling` on such a head means the
validator passed, not that vet or tag compilation ran.

For a change that touches catalogue, receipt or `package-split` files, the Go
and Python checks that read them run at the current head. No new check
reuses a result from another SHA.

The classification is on the pull request's net change against the base. A
conflict-resolution merge does not change that net change, so a conflict
resolved by combining two fragments (which cannot conflict, since they are
different files) or by combining catalogue rows leaves the classification of
the pull request as it was. If the merge brings in new code from the base, the
tested tree contains that code and the checks that read it run on the new head.
That is not waived. The saving comes from not having a conflict at all.

### Why no cross-SHA evidence reuse

Evidence from a previous commit is not carried forward in this design. To
reuse it, an independent proof would be required that the source tree, build
inputs, test selection, toolchain, image digests and environment are equal and
that the origin run is that commit's own. A merge of `main` changes the tree
except when `main` advanced only by documentation, which is the rare case and
already skipped by `docs_only.py`. A general attestation service would be much
larger than removing the contested files, so it is not proposed. If the
measurements below show a residual cost, reuse can be reconsidered under #1 as a
reviewed CI policy change.

### Controls the implementing changes must keep

| Input | Required result |
| --- | --- |
| Only `docs/changes/<id>.md`, valid | `docs_only=true`; validator executed |
| Fragment with duplicate identifier, unknown kind, missing migration, malformed or case-colliding name | validator fails |
| `docs/changes/x.json`, `docs/changes/sub/x.md`, `changes/x.md` | not allowlisted, full checks |
| Fragment that is a symlink | validator fails |
| Catalogue, receipt, `package-split` file | full Code checks |
| Code file renamed into `docs/changes/` | a deletion in code, full checks |
| Empty list, fork, missing base, push, schedule, manual run | full checks (unchanged) |
| A Go test that starts reading a new path under `docs/` | `ReaderInventoryTests` fails if the path is allowlisted |
| Catalogue regenerated without approval | per-package review test fails |
| A source change in package A | only A's catalogue and receipt go stale |

## Combining independent changes (acceptance check)

The verification the implementing changes must show, with actual outcomes
reported and not promised:

- Two fragments for different changes combine without any shared edit. A
  repository test builds two branches that each add one fragment and merges
  them; the merge has no conflict and the assembly contains both entries.
- The validator and assembler reject: a duplicate identifier (including case
  variants and an identifier already in a released section), a dropped fragment
  that another still `supersedes`, an `api-break` fragment without a resolvable
  migration link, and a released section that differs from the tag.
- Two API changes in different packages touch disjoint catalogue and receipt
  files; the merged tree passes every package test. Two changes in the same
  package still conflict, and the test records that as expected.
- Stale review: change a source in package A after approval and show only A
  fails. Lost entry: delete one catalogue row and show the byte-equality test
  fails. Inconsistent mapping: a move record whose `to` is missing or whose
  `from` still exists fails. Semantic conflict: two proposals that map to one
  qualified name fail the collision check.
- Each regression test is shown to fail against a deliberately broken version.

## Measurements

Measure before and after each implementing change on the same definitions.
Keep the quantities separate and do not present job counts or queue span as
billed savings:

| Quantity | Source |
| --- | --- |
| Merge-conflict incidence: pull requests that needed a conflict-resolution head, per merged pull request | `git log --first-parent` and the PR commit lists, as in the four-PR table above |
| Author and reviewer effort | count of regeneration and approval steps in the resolution commit messages; reviewer time is not recorded and stays qualitative |
| New CI attempts | distinct head SHAs per pull request that started the seven workflows |
| Native jobs actually executed | jobs in `ci_measurements.py report` with a non-skipped conclusion in the runtime workflows |
| Summed runner windows | `ci_measurements.py` per-job start-to-completion sum |
| Elapsed integration time | pull request creation to merge, and critical-path wall time per execution |

`ci_measurements.py` already excludes cancelled runs and executions that lost a
run to cancellation, counts first attempts only and separates runner time from
acquisition delay. Take the baseline from the merged pull requests of the
window used above (before any implementing change), then repeat after
the fragments change and after the catalogue change. The sample is small, so
report counts with the window and the sampling limits, and do not claim a
stable rate from one window.

## Transition and in-flight pull requests

Open pull requests at the snapshot and the shared files they edit:

| PR | Shared bookkeeping files | State |
| --- | --- | --- |
| #212 | CHANGELOG, inventory, members, review, audit, `package-split.json`, `inventory_test.go`, README | conflicting with `main` |
| #214 | CHANGELOG, inventory, review, `docs/ceph.md` | mergeable |
| #215 | none | mergeable |

Plan:

1. The fragment change is additive. It validates and accepts fragments and
   leaves the existing Unreleased section editable, so #212 and #214 stay
   valid. A pull request may still edit the legacy Unreleased block until a
   later cutover change forbids it. The cutover waits until the pull requests
   open at that time have merged or been rebased.
2. The assembler treats the existing Unreleased block as a legacy block and
   places it first in the draft; fragments follow. Nothing in the legacy block
   is rewritten.
3. The catalogue change is a hard cutover of the global files, because keeping
   both layouts would double the review surface. It should land when #212 and
   #214 have merged, since both edit the files it deletes. A pull request still
   open then resolves with a fixed recipe: take the base's per-package layout,
   run the regenerate command for the packages it touched, run the approve
   command for those packages only, and keep its review note.
4. The migration itself is one reviewed change. It splits the three global
   documents into per-package files without changing any row, and its
   verification shows that concatenating the per-package catalogues in package
   order reproduces the previously approved global catalogues byte for byte
   before the global files are deleted. The receipts for the new layout are
   issued by that explicit review and are not inferred from the old global hash.
5. Documentation changes with each implementing change, so contributors are not
   told to edit a file that no longer exists: `docs/contributing/releasing.md`
   (the Unreleased handling and release notes steps), `docs/contributing/ci.md`
   (the allowlist paragraph and the reader list), `docs/contributing/testing.md`
   (the regenerate and approve commands), `AGENTS.md` (a short pointer: add a
   fragment for a release-worthy change; regenerate and approve API catalogues
   separately), and the `shared-harness-boundary.md` sentence noted above.
6. Push, manual and scheduled runs and uncertain classifications keep their
   current full coverage. Changing default selection remains an explicit
   reviewed CI policy change under #1.

## Implementation sequence

Small, independently reviewable pull requests, in order:

1. **Fragments and validator.** `docs/changes/` format, the Python validator,
   `docs/changes` in `UNREAD_DOC_DIRS`, the always-run validator step in
   `Vet, compile and CI tooling`, the controls table, and the contributor
   documentation. The legacy Unreleased block stays editable. No assembly yet.
2. **Release assembly.** The deterministic assembler, the released-section
   immutability check, the `releasing.md` rewrite and, once open pull requests
   have drained, the rule that new entries go to fragments only.
3. **Per-package catalogues and receipts.** Discovery of packages, per-package
   files and tests, the regenerate and approve commands, the on-demand
   aggregate, the one-time reviewed migration and the audit's count table.
4. **Move records and the base-diff check.** The move record, the
   removed/added row accounting, and making a fragment (or `kind: none`) required
   when a package's catalogue changed.
5. **`package-split.json` shards.** Loader, duplicate and chain refusal, order
   independence test.
6. **CI classification, only if the measurements ask for it.** Any narrowing of
   which checks run for metadata-only changes beyond steps 1 to 5, as a
   separately reviewed CI policy change.

Step 3 as implemented (the names below replace the placeholders above):
`internal/apiinventory/catalogue` holds the discovery, derivation, receipt and
aggregate logic, and `internal/tools/apinaming` provides `regenerate`,
`approve`, `aggregate` and `check`; `-update-api-naming` in the apiinventory
tests is kept as an equivalent of `regenerate` and never writes a receipt. The
per-package files use schema `v2` (the package name and its own count replace
the global scope text and count map), each receipt also names its reference
page, and the old `packages` map is gone. A directory below
`docs/design/api-naming` counts as a catalogue only when it holds one of the
three files, so the `moves/` directory of step 4 and a review note next to it
need no change to the layout rules, and `package-split.json` shards (step 5)
are independent of it.

Start with 1. Fragments are the most edited file (42 of 175 entries against 23
for the inventory), the change is additive with no data migration, it does not
require waiting for #212 and #214, and it introduces the validator step and the
allowlist change that steps 2 and 4 reuse. Step 3 removes the more expensive
collisions (a conflict there needed regeneration plus a re-derived hash, as the
conflict-resolution commits record). It can be developed in parallel with step
2, but it is a hard cutover, so it should merge only after #212 and #214 have
landed.

## Not decided and limits

- The directory and field names above are proposals.
- Whether the approve step may also be run as a pull-request-only check against
  the base is open.
- Whether the justified-no-note requirement should apply to `docs/<package>.md`
  edits as well as catalogue changes is open.
- The numbers above come from one window of one repository and measure edit
  frequency and four overlapping pull requests. They are not a rate, and the
  workflow-run spans are not billed time.
- Nothing here was run natively. This note is source reading and repository
  history only.

Refs #216
