# Preparing a release

This page describes release preparation for the module. Completing the
fixture implementation, checking a candidate, publishing a tag and adopting
that tag in a consumer are separate steps. Do not treat a closed milestone
or a successful retry as evidence for all four.

## v0.2.0 scope

The [v0.2.0 milestone](https://github.com/JSYoo5B/ceph-testcontainers-go/milestone/1)
is complete. Its producer-owned result is the public Messenger fixture
profiles, daemon controls and P0 replicated mutation oracles described in
[Fixtures for Messenger client tests](../guides/messenger-client-fixtures.md).
The release is identified by the root module tag `v0.2.0`. Its release notes
record the final commit and CI evidence. A later candidate needs its own
check disposition; a successful run for another SHA is not its CI result.

It does not certify ceph-msgr-go's wire implementation or complete that
project's adapter migration. Full daemon-path/MDS coverage, selective network
fault extensions and unresolved native investigations are separate work.
The [upgrade guide](../guides/upgrading-to-v0.2.md) records compatibility and
the remaining native limits. Image production and the authoritative image
contracts remain in ceph-testcontainers-images.

## Candidate checklist

1. Freeze the candidate's full commit SHA and confirm the worktree is clean.
   Review the diff from the prior tag, including exported signatures,
   `go.mod`/`go.sum`, bootstrap defaults and failure/cleanup contracts.
2. Keep `CHANGELOG.md` under `Unreleased` until publication. Update the
   upgrade guide, module pages and README links together. State the Go
   minimum separately from this repository's toolchain and native clients'
   platform requirements.
3. Run the Docker-free checks in [Testing](testing.md): `make check` and
   `make lint`, plus the Python CI-tooling tests. Check local Markdown links
   and compile new examples. Compilation is not a native run.
4. Reuse independently verified CI at the candidate's exact SHA. The
   [required workflows](ci.md) cover code, Docker, short, topology,
   recovery, multicluster and Squid tests. Record the workflow/run and
   attempt identities, chosen image references and actual image IDs. A
   canceled, skipped or missing scenario is not a native pass. Record
   vulnerability-scan results from Code checks as well.
5. Review failed attempts, not just their latest conclusion. Keep fixed
   cases linked to their correction and regression. Unresolved cases need
   a disposition and live owner. [#41](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/41)
   completed its audit by retaining MON replacement/join investigations in
   [#61](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/61) and
   infrastructure/storage observations in [#31](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/31).
6. Confirm native completion and cleanup for the claimed profiles. Strict
   upstream-defect tests under `native_regression` are opt-in, outside
   `all`; document their limits instead of counting them as required passes.
7. If code or dependency behavior changes after those checks, validate the
   affected paths again at the new SHA. For documentation-only changes,
   retain the implementation's native evidence and record the new static
   checks separately; do not describe old CI as CI of the new SHA.

Do not dispatch duplicate native runs merely because a release is being
prepared, and do not cancel existing main CI as part of documentation work.

## Publication handoff

All four packages belong to one root Go module. Publish one repository tag
(`v0.2.0`), not per-package `ceph/v0.2.0`, `rbd/v0.2.0` or other nested tags.
There is currently no workflow that publishes a release automatically.

Before a separately authorized publication:

- Select the final candidate SHA and prepare release notes from the
  changelog, including compatibility, support and unresolved native limits.
- When publishing, replace `Unreleased` with the version and actual release
  date, and update the README's development-version notice so it does not
  tell release users to install an older tag.
- Rewrite the pre-release wording of the [upgrade guide](../guides/upgrading-to-v0.2.md)
  for the published tag, including its introduction, version table and
  installation instructions. Identify the release by its tag and keep the
  final SHA's validation evidence in the release notes.
- Point the tag and GitHub Release at the reviewed commit, then verify that
  the Go module proxy resolves that tag and that an external integration
  module builds with it. Do not claim published-tag consumption before this
  succeeds.
- Remove a consumer's test-only `replace`/workspace wiring and validate the
  published dependency in that consumer. Consumer migration and actual
  client execution remain owned by the consumer project.

Release preparation does not itself create a tag or publish a GitHub Release.
