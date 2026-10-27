# Preparing a release

This page describes release preparation for the module. Completing the
fixture implementation, checking a candidate, publishing a tag and adopting
that tag in a consumer are separate steps. Do not treat a closed milestone
or a successful retry as evidence for all four.

## v0.3.0 scope

The three completed, bounded v0.3.0 deliverables are:

| Item | Implementation | Maintained evidence |
| --- | --- | --- |
| supervised bounded binary input | public `NativeInput` and result fields | `TestNativeCommandInput`, `TestNativeCommandInputCancellation`, input/supervisor unit tests; `native_input` batch |
| MDS unsafe-to-safe observation | test-only receipt | `TestCephFSUnsafeRequestWindow`, saved native parser/classifier fixtures; `cephfs_fixtures_sessions` batch |
| stock-image first-failure diagnostics | test-only collector and classifier | `TestAuthFirstFailureBundle`, retained-failure and partial-observation fixtures; `messenger_client_profiles` batch |

Other changes since v0.2.0 include the MDS session, daemon-path and MGR
transition receipts listed in [CHANGELOG.md](../../CHANGELOG.md). The
[v0.3.0 guide](../guides/upgrading-to-v0.3.md) records compatibility and the
limits of each claim. MDS replay across a crash, selected auth-key
generation equality and the intermittent rejection, and the remaining
daemon-path/mode matrix stay separate. No downstream wire
implementation or adapter migration is certified. Image production and the
authoritative image contracts remain in ceph-testcontainers-images.

The previous release's scope remains in
[Upgrading to v0.2.0](../guides/upgrading-to-v0.2.md). A later candidate needs
its own check disposition; a successful run for another SHA is not its CI
result. The root tag `v0.3.0` identifies this release; its
[release notes](https://github.com/JSYoo5B/ceph-testcontainers-go/releases/tag/v0.3.0)
record the final source and checks.

## Candidate checklist

1. Freeze the candidate's full commit SHA and confirm the worktree is clean.
   Review the diff from the prior tag, including exported signatures,
   `go.mod`/`go.sum`, bootstrap defaults and failure/cleanup contracts.
2. Keep `CHANGELOG.md` under `Unreleased` until publication. Unreleased
   changes are recorded in the legacy `Unreleased` section and in the
   fragments under `docs/changes/` (see
   [Release-note fragments](release-fragments.md)); both are part of the
   release notes until an assembler exists, so review the fragments too and
   run `python3 .github/scripts/release_fragments.py`. Update the
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
   a disposition and live owner.
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

All four packages belong to one root Go module. Publish one repository tag,
such as `v0.3.0`, not per-package `ceph/v0.3.0`, `rbd/v0.3.0`
or other nested tags. There is currently no workflow that publishes a
release automatically.

Before a separately authorized publication:

- Select the final candidate SHA and prepare release notes from the
  changelog and the unreleased fragments, including compatibility, support and unresolved native limits.
- When publishing, replace `Unreleased` with the version and actual release
  date, and update the README's development-version notice so it does not
  tell release users to install an older tag.
- Rewrite the pre-release wording of the [upgrade guide](../guides/upgrading-to-v0.3.md)
  for the published tag, including its introduction, compatibility table and
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
