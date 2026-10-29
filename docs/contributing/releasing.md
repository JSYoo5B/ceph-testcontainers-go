# Preparing a release

This page describes release preparation for the module. Completing the
fixture implementation, checking a candidate, publishing a tag and adopting
that tag in a consumer are separate steps. Do not treat a closed milestone
or a successful retry as evidence for all four.

Pull requests do not write release notes, fragments or upgrade guides. The
release notes are written once, at release time, as described below. Image
production and the authoritative image contracts remain in
ceph-testcontainers-images.

## Candidate checklist

1. Freeze the candidate's full commit SHA and confirm the worktree is clean.
   Review the diff from the prior tag, including exported signatures,
   `go.mod`/`go.sum`, bootstrap defaults and failure/cleanup contracts.
2. Write concise final release notes from the previous release and the final
   source. Describe the net changes users receive compared with the previous
   published release, not the intermediate states of unreleased work; a feature
   that was introduced and removed before the release has no entry. Put the
   notes in a `CHANGELOG.md` created only for release preparation. Do not keep
   that file or an `Unreleased` section on the ongoing development branch.
   Published tags and release notes remain immutable. Update
   the module pages and README links together, and state the Go minimum
   separately from this repository's toolchain and native clients' platform
   requirements.
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

- Select the final candidate SHA and prepare the release notes, including
  compatibility, support and unresolved native limits.
- When publishing, make the reviewed section's heading carry the version and
  the actual release date, and update the README's development-version notice so it does not tell release users to
  install an older tag.
- Identify the release by its tag and keep the final SHA's validation evidence
  in the release notes.
- Point the tag and GitHub Release at the reviewed commit, then verify that
  the Go module proxy resolves that tag and that an external integration
  module builds with it. Do not claim published-tag consumption before this
  succeeds.
- Remove a consumer's test-only `replace`/workspace wiring and validate the
  published dependency in that consumer. Consumer migration and actual
  client execution remain owned by the consumer project.

Release preparation does not itself create a tag or publish a GitHub Release.
