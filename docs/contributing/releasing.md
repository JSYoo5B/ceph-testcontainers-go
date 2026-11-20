# Preparing a release

This page describes release preparation for the module. Completing the
fixture implementation, checking a candidate, publishing a tag and adopting
that tag in a consumer are separate steps. Do not treat a closed milestone
or a successful retry as evidence for all four.

Pull requests do not write release notes, fragments or upgrade guides. The
release notes are written once, at release time, as described below. Image
production and the authoritative image contracts remain in
ceph-testcontainers-images.

## Stable versions and public APIs

Project planning calls the three positions **product.major.minor**. In a Go
tag such as `v1.3.2` they are the Go major (1), minor (3) and patch (2).
A new stable feature release increments the middle number for new features
or compatible public APIs. A patch increments the last number for bug or
security fixes. Reserve a new product for a substantial redesign or accumulated
compatibility costs that justify coordinating a breaking release with consumers.

Throughout a product line, preserve import paths, public signatures and
documented behavior. Move implementation behind the existing entry point
when possible. To replace an API, ship the replacement alongside the old
entry point, add a Go `Deprecated:` paragraph naming the replacement to the
old API's documentation, and keep tests covering the old entry point.

The grace period starts with the first stable release containing both the
replacement and the deprecation notice. Before removal can ship, three
calendar months must elapse **and** two subsequent stable feature releases
in that product must ship. Patches and prereleases do not count. Meeting
both conditions makes removal eligible for a later product release. It
never schedules removal or permits it within the existing product.

For example, an API deprecated in v1.1.0 remains usable in v1.2.x, v1.3.x
and every later v1 release. Once v1.2.0 and v1.3.0 have shipped and three
months have elapsed since v1.1.0, v2 may omit it. A routine package move
alone does not justify v2. Published tags remain immutable.

## Supported lines and backports

Maintain the two most recently introduced stable product.feature lines.
Support starts at the line's first stable release. At v1.0.0, 1.0.x is
the sole stable product line. Order lines by when
they were introduced. A later patch on an older line does not move it
ahead of the newer line. Both receive applicable ordinary bug fixes and
security fixes. New features go to ongoing development.

| Newly released | Lines receiving fixes |
| --- | --- |
| v1.2.0 | 1.2.x and 1.1.x |
| v2.0.0 | 2.0.x and the final 1.N.x |
| v2.1.0 | 2.1.x and 2.0.x |

For each fix, identify the affected supported lines from their shipped code
and tests. Record the affected versions or the evidence that the defect is
absent on a line. Apply the correction and regression coverage to each
affected line, adapting the change to its API and dependencies. Verify
each backport on its own candidate, including the original failing condition,
the corrected result and applicable release checks. Publish a separate patch
tag for each affected line. Carry the correction into newer affected lines,
any release under stabilization and ongoing development. The
[branch flow](git-flow.md) describes where these PRs go.

A line retires when two newer stable lines have been introduced. Retirement
ends new patch releases on that line. Its tags remain available, and later
releases in the same product still owe consumers the public API compatibility
described above.

## Candidate checklist

1. Freeze the candidate's full commit SHA and confirm the worktree is clean.
   Review the diff from the prior tag, including exported signatures,
   `go.mod`/`go.sum`, bootstrap defaults and failure/cleanup contracts.
2. Write concise final release notes from the previous release and the final
   source. Describe the net changes users receive compared with the previous
   published release, not the intermediate states of unreleased work; a feature
   that was introduced and removed before the release has no entry. Write the
   notes directly in the GitHub Release; the repository never contains a
   `CHANGELOG.md`. Published tags and release notes remain immutable. Update
   the module pages and README links together, and state the Go minimum
   separately from this repository's toolchain and native clients' platform
   requirements.
3. Run the [local checks without Docker](testing.md#local-checks-without-docker)
   of the testing guide. Check local Markdown links
   and compile new examples. Compilation is not a native run.
4. Verify Code checks and the complete release matrix for the exact
   production or support branch push commit that will receive the tag.
   Record the branch, full SHA, workflow runs, attempts, job conclusions and
   image references. Check the six [Code checks](ci.md#code-checks) and
   [release candidate validation](ci.md#release-candidate-validation),
   including native execution on every supported Ceph release. A canceled,
   skipped or missing required scenario cannot qualify a candidate.
5. Review failed attempts, not just their latest conclusion, and the first
   failures that a [release retry](ci.md#release-retry) passed. Keep fixed
   cases linked to their correction and regression. Unresolved cases need
   a disposition and live owner; classify each as described in
   [Candidate failures](#candidate-failures).
6. Confirm native completion and cleanup for the claimed profiles. Strict
   upstream-defect tests under `native_regression` are opt-in, outside
   `all`; document their limits instead of counting them as required passes.
7. If the candidate changes, repeat candidate verification at the new SHA,
   including for a documentation-only change. Change the candidate only for a
   failure that blocks it.

## Candidate failures

Every failure of a candidate's validation is recorded in an issue with its
run, job, release, images and the first run's receipts, whether or not a
retry passed. The kind of failure decides whether it blocks the candidate:

| Failure | Example | Candidate |
| --- | --- | --- |
| Execution environment | A registry or runner error before any Ceph assertion ran | Not blocked. Re-run the failed jobs once the run completes. |
| A test's intermittent failure that its retry passed | An oracle or timing defect of the test, or an undiagnosed single failure | Not blocked. Fix it on `develop`; it reaches the next patch release. |
| A test that failed in its retry too | The same test failed twice in one job | Blocked until it is diagnosed. |
| A defect of the library's behaviour | A public function returns a wrong result or leaks a resource | Blocked, unless the release notes name it as a known issue with its limit. |

A fix of a test or of CI alone does not change the candidate: the release
line keeps the validated commit and the fix lands on `develop` only. A
library fix that blocks the candidate lands on `develop`, the release line
follows `develop` again, and the new commit is validated anew.

Do not dispatch duplicate native runs merely because a release is being
prepared, and do not cancel existing main CI as part of documentation work.

## Publication handoff

All public packages belong to one root Go module. Publish one repository tag,
such as `v0.3.0`, not per-package `ceph/v0.3.0`, `rbd/v0.3.0`
or other nested tags. There is currently no workflow that publishes a
release automatically.

Before a separately authorized publication:

- Select the final candidate SHA and prepare the release notes, including
  compatibility and unresolved native limits. Name the stable lines currently
  receiving fixes and any line retiring with this release.
- When publishing, check that the README does not tell release users to
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
