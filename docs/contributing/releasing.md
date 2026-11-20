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
security fixes. Reserve a new product for a substantial redesign of the
product itself. A planned API removal does not need one.

By default, preserve import paths, public signatures and documented behavior
throughout a product line. Move implementation behind the existing entry
point when possible. The one exception is the removal of a deprecated API
after its grace period, described below.

This is the project's compatibility policy, and it differs from conventional
[Go module semantic versioning](https://go.dev/ref/mod#versions), under which
every incompatible removal of a public API needs a new major version and
module path. Here an eligible removal ships in a stable feature release of
the same product line, so that release is a breaking change for consumers
that still use the removed API: they move to its replacement before
upgrading. Requiring a new product for each planned removal would turn
routine API maintenance into repeated major releases and module-path
migrations; the announced grace period gives consumers the time to move
instead.

### Deprecation and grace period

To replace an API, ship the replacement alongside the old entry point, add a
Go `Deprecated:` paragraph naming the replacement to the old API's
documentation, and keep the old API, its documented behavior and its tests.

The grace period starts with the declaration release: the first stable
release that contains both the replacement and the deprecation notice. The
old API stays through the next two stable feature releases of that product
line. Removal becomes eligible in a later stable feature release only when
both conditions hold at that release's publication:

- three calendar months have elapsed since the declaration release, and
- two stable feature releases after the declaration release have already
  shipped with the old API still present.

Patches and prereleases do not count as feature releases, and a patch never
removes an API. Eligibility is not a schedule: it only permits the removal
decision below.

### Removal after the grace period

At an eligible stable feature release, the cost of keeping the compatibility
layer decides:

| The deprecated API | Removal |
| --- | --- |
| Materially blocks development, a needed correctness fix or a design change | May be removed in the first eligible stable feature release. Record the concrete blocker and the replacement in the GitHub Release notes. |
| Can stay as a thin compatibility layer | Stays until an eligible stable feature release raises the module's minimum supported Go version, the `go` directive in `go.mod`. A change of the `toolchain` directive or of the Go version that CI uses does not count. While the minimum is unchanged, the layer stays. |

A raised Go minimum never shortens the grace period: a release that raises
it before removal is eligible keeps the API. Record every removal in the
GitHub Release notes of the release that ships it.

### Worked example

An API is deprecated in v1.1.0, the declaration release.

| Situation | Result |
| --- | --- |
| v1.1.0 (declaration) | The old API and its tests stay beside the replacement. |
| v1.1.1, v1.2.0-rc.1 | Patches and prereleases neither count nor remove anything. |
| v1.2.0 and v1.3.0 | The old API is still present in both. They are the two feature releases that must ship with it. |
| Three months have elapsed, but only v1.2.0 has shipped | Not eligible. v1.3.0 keeps the API. |
| v1.2.0 and v1.3.0 have shipped, but v1.4.0 would be published before three months have elapsed | Not eligible. v1.4.0 keeps the API; reconsider at v1.5.0. |
| v1.2.0 and v1.3.0 have shipped and three months have elapsed when v1.4.0 is published | v1.4.0 is the first eligible release. |
| v1.4.0 is eligible and the API blocks a needed change | v1.4.0 may remove it, with the blocker and the replacement in its release notes. |
| v1.4.0 is eligible, the API is a thin layer and `go.mod` keeps its `go` directive | v1.4.0 keeps it, and so does every later release until one raises the directive. |
| v1.4.0 is eligible, the API is a thin layer and v1.4.0 raises the `go` directive | v1.4.0 may remove it. |
| v1.3.0 raised the `go` directive | Not eligible yet, so v1.3.0 keeps the API. The raise is spent: a thin layer then waits for the next raise at or after v1.4.0. |
| v1.5.0 updates only the `toolchain` directive or CI's Go version | A thin layer stays. |

No v2.0.0 is needed for this removal. Published tags remain immutable.

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
releases in the same product follow the compatibility policy above: they keep
its public APIs except for a deprecated API whose removal has become
eligible.

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

Once a failure is confirmed to block the candidate, cancel that candidate's
Release matrix run: its remaining results decide nothing, the new commit is
validated from the start, and the run holds nearly every runner of the
account ([Runner budget](ci.md#runner-budget)) that the fix's pull request
needs. Do not open other pull requests during the merge window; one that does
not fix a blocking failure waits behind the Release matrix.

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
