# Agent instructions

## Release compatibility

Keep published release tags immutable. Make corrections on `main` and publish
a new version when needed. Use maintained source paths, test names and release
tags when explaining behavior.

For changes to Ceph behavioral bug compatibility, follow the
[compatibility policy](docs/contributing/bug-compatibility.md).

## Release notes

Development branches do not carry `CHANGELOG.md`. Do not create, update or
review development changelog entries. A pull request does not add release-note
fragments, no-note declarations, compatibility ledgers, migration tables or
upgrade guides. Record intent and validation in the pull request description.
Before 1.0, a breaking change removes the old names without aliases. The
maintainer writes concise final release notes at release time, from the previous
release and the final source (see [Preparing a release](docs/contributing/releasing.md)).

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
`docs/design/api-naming/moves/`. Check with `go run ./internal/tools/apinaming moves -base origin/main`; `SKIPPED`
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
