# Agent instructions

## Release compatibility

Keep published release tags immutable. Make corrections on `main` and publish
a new version when needed. Use maintained source paths, test names and release
tags when explaining behavior.

For changes to Ceph behavioral bug compatibility, follow the
[compatibility policy](docs/contributing/bug-compatibility.md).

## Release notes

For a release-worthy change (a breaking API or import change, user-visible
behavior, a compatibility or requirement change, or a supported-scope change),
add a fragment `docs/changes/<change-id>.md` instead of editing the shared
`Unreleased` section of `CHANGELOG.md`. Follow
[Release-note fragments](docs/contributing/release-fragments.md): choose a
unique change identifier, use `kind: none` with a reason when you decided that
no note is needed, and link a migration guide for an `api-break`. The legacy
`Unreleased` section stays editable while open pull requests drain; do not
rewrite published sections.

Editing a package page (`docs/<package>.md`) needs no fragment by itself;
judge it by the content, and a public API catalogue change still needs one.

## Privacy review

Before publishing source, documentation, fixtures, commit messages, release notes,
pull requests, issues or GitHub discussions, search for private
execution-environment details. Review matches in context, including hidden CI
files and generated reports. These rules apply to internal code as well as
pull request and issue descriptions, reviews and comments.

- Omit actual personal hostnames, private domains and addresses, account-specific
  filesystem paths, remote-access helpers, checkout locations and private log names.
- When describing paths, use only file paths within this repository or related
  repositories. Do not expose unrelated filesystem locations or unnecessary
  arbitrary paths.
- Software versions, architecture and hardware specifications may remain when they
  explain reproducibility, compatibility or performance. Remove their association
  with an identifiable private host or access path.
- Preserve functional platform strings, synthetic test inputs, public CI runner
  information, general product requirements and public author attribution or image
  namespaces. Do not globally replace operating-system or architecture strings.
- When logs or other diagnostic information must be shown as examples, mask
  information unrelated to the point being demonstrated. Retain only the details
  needed to explain or reproduce that point.

## API catalogues

Public API names are catalogued per package in
`docs/design/api-naming/<package>/` (`declarations.json`, `members.json`,
`review.json`). Do not edit them by hand and do not look for a global
inventory, count or hash file: none exists. After a public API change run
`go run ./internal/tools/apinaming regenerate <package>` and then, as a
separate deliberate step, `go run ./internal/tools/apinaming approve <package>`.
Regeneration never approves a changed API. After a `git merge` that conflicts in these
files, run `go run ./internal/tools/apinaming resolve`; it rewrites them only when the merge
just composes the rows of two approved parents (see [Testing](docs/contributing/testing.md)). A move of API between packages, or
a deliberate removal or addition of a declaration row, needs a record under
`docs/design/api-naming/moves/`, and any catalogue change needs a release-note
fragment under `docs/changes/` (kind `none` with a reason is accepted). Check
with `go run ./internal/tools/apinaming moves -base origin/main`; `SKIPPED`
means no base was available and is not a pass. See
[Testing](docs/contributing/testing.md).

## Package-split rewrites

A package move that renames API used by a retained native producer adds its own
shard under `.github/scripts/fixtures/package-split/`; it does not edit a shared
file. See [Package-split rewrite shards](docs/contributing/package-split-shards.md).

## Test doubles

Do not use mocks by default. Use a mock only when its substituted behavior and
verification purpose are explicit at the test's reading site and easier to
review than a real implementation or a small fake. Prefer small, explicit
substitutes; do not grow a second implementation of external-system semantics
or hide long expectation scripts behind helpers. Mock/fake success is not
native compatibility evidence. Follow the test-double guidance in
[Testing](docs/contributing/testing.md#test-doubles); existing doubles are in
the [inventory](docs/contributing/test-double-inventory.md).

## Validation

Use [Testing](docs/contributing/testing.md) and the Makefile for commands.
`make check` runs unit tests, race tests, vet and tagged compilation; it does not
prove Docker or native Ceph interoperability. Run substantial checks and Docker
integration on an authorized Linux runner. Use a desktop engine only for an
explicitly required desktop-specific behavior check.

For documentation-only changes, check the diff, privacy candidates and affected
links. For fixture changes, run the affected fixture tests. Match every CI claim
to its exact commit and attempt; skipped, canceled and earlier-commit jobs do not
count as passes of the current commit. Keep implementation, compilation, native
execution and downstream-consumer validation separate.

Image production belongs to `ceph-testcontainers-images`. Verify registry pulls
and Actions publication permissions separately. Module downloads do not prove
image publication or native test success.
