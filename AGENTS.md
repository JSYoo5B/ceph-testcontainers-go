# Agent instructions

## Testcontainers modules

This repository is a set of Testcontainers for Go modules, and every package
that runs containers keeps the testcontainers-go module conventions. A change
that breaks them is not allowed, however convenient it looks:

- `Run(ctx, img, opts ...testcontainers.ContainerCustomizer)` returns a
  `Container` that embeds `testcontainers.Container` (a service package embeds
  `*ceph.Container`). The two-cluster fixtures add only a `Config` before the
  variadic. Never change an existing `Run` signature or add a second entry point
  for the same fixture; carry new configuration through the existing variadic
  (a module `Option` or one value-group carrier).
- `Run` returns the partially started container together with its error, so the
  caller's `testcontainers.CleanupContainer` still terminates it.
- Container settings are testcontainers customizers. Do not build a parallel
  option, lifecycle or cleanup framework next to them.
- Assume the reader knows Testcontainers for Go and Go. Do not explain its basics
  in code comments or documentation.

## Pull requests (for agents)

Implement a task as one pull request, even when it has many steps. Every pull
request runs the full CI, which is expensive, so a series of small pull
requests only repeats CI without adding value. Review is done by an agent, so
the size of a pull request is not a review problem. Do not split work to avoid
merge conflicts; resolve conflicts yourself.

## Release compatibility

Keep published release tags immutable. Make corrections on `main` and publish
a new version when needed. Use maintained source paths, test names and release
tags when explaining behavior.

For changes to Ceph behavioral bug compatibility, follow the
[compatibility policy](docs/guides/bug-compatibility.md).

## Release notes

The repository never contains a `CHANGELOG.md`. Do not create, update or
review changelog entries. A pull request does not add release-note
fragments, no-note declarations, compatibility ledgers, migration tables or
upgrade guides. Record intent and validation in the pull request description.
Before 1.0, a breaking change removes the old names without aliases. The
maintainer writes concise final release notes at release time, from the previous
release and the final source, directly in the GitHub Release (see
[Preparing a release](docs/contributing/releasing.md)).

## Historical documentation

Documentation describes current behavior, current usage and current
operational requirements. Do not add chronology, increment or slice status,
retired designs, abandoned proposals or rename and migration accounting;
Git history, issues and pull requests keep that. A concise warning about a
concrete, easy-to-make mistake (the trap and its consequence) may stay. Delete
local review artifacts, logs and scratch notes once they have been used.

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
`internal/apiinventory/catalogues/<package>/` (`declarations.json`, `members.json`,
`review.json`). Do not edit them by hand and do not look for a global
inventory, count or hash file: none exists. After a public API change run
`go run ./internal/tools/apinaming regenerate <package>` and then, as a
separate deliberate step, `go run ./internal/tools/apinaming approve <package>`.
Regeneration never approves a changed API. After a `git merge` that conflicts in these
files, run `go run ./internal/tools/apinaming resolve`; it rewrites them only when the merge
just composes the rows of two approved parents (see [Testing](docs/contributing/testing.md)).

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
