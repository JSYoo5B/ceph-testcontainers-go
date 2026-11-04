# Structural review

The Code job **Vet, compile and CI tooling** attaches a structural report to
code-changing pull requests. Start with its job summary or the single
**Structural review** PR comment, follow the reported source locations, then
read the affected implementation and tests. Check the head and measured base
SHAs before using a report. A base branch can advance after measurement; this
report is a comparison of the two named snapshots, not a fresh analysis of a
later merge candidate.

The report is triage. It does not approve a design, prove behavior or establish
native compatibility. Do not repeat broad source scans merely to recount the
same methods, fields or diagnostics. Use source inspection for ownership,
responsibility boundaries, lock order, lifecycle and behavioral changes.

## Signals and their limits

| Concern | Tool and measurement | Interpretation and exclusions |
| --- | --- | --- |
| Large concrete owners | Existing `structmetrics`: declared exported/private receiver methods, direct fields, declaration multiplicity and package totals | Lexical Go AST, all build profiles combined. No tests, generated files, aliases, promoted methods/fields or non-struct receivers. Multiple declarations can combine mutually exclusive profiles. A count is not a responsibility boundary. |
| Control-flow branches | golangci-lint `cyclop` | Finds branch-heavy functions. Necessary input validation can be branch-heavy. It cannot find a type with many small methods. |
| Nesting/readability | golangci-lint `gocognit` | Corroborating calibration signal, often overlaps cyclop. Inspect overlapping messages together; do not count them as independent defects. |
| Long functions | golangci-lint `funlen` | Lines and statements are separate findings. A long native report decoder or test scenario can be coherent. |
| Repeated blocks | golangci-lint `dupl` | Candidates for inspection, not mandatory abstraction. Lock tests for `Mutex` and `RWMutex`, for example, can look alike while checking different contracts. Column zero is unspecified. Blocks have no function identity and remain unmatched. |
| Package boundaries | Existing import/source checks and Go compilation | Compilation already rejects cycles; no additional cycle checker is needed. An architectural gate needs a specific forbidden dependency, not a generic quality score. |

These reuse the existing Go parsers and the Code job's pinned golangci-lint
binary. No extra lint framework or dependency is installed. All four lint
signals remain report-only calibration, with the collection thresholds in
`internal/tools/lintcalibration/golangci.yml`; none is enabled as an additional
ordinary lint gate. In particular, overlapping complexity checks are not two
votes for refactoring. Any future threshold or failure policy needs a reviewed
proposal, useful examples and false-positive evaluation first.

Keep configuration-specific exceptions local and justified. Do not introduce a
shared suppression ledger or mechanically move methods into package functions,
facades or meaningless helpers to improve a number. A smaller owner is useful
only when responsibility, dependencies and ownership become clearer.

## Scope and ordering

The report compares the PR merge base and exact head, using one build of the
analysis tools and one embedded lint configuration for both. GitHub's compare
API names the merge base; the target SHA at the PR event is recorded separately.
Advances on the target branch therefore do not appear as removals by the PR. It does not
substitute the synthetic merge tree for the PR head. The root module is
collected with `./...` on Linux amd64, using Code's Go toolchain and the tag set
in the embedded calibration configuration. Test diagnostics are explicitly
labeled; generated files follow golangci-lint's exclusion policy. Nested Go
modules, fixtures, other platforms and other tag combinations are not measured.
The receipt records the tool source commit, Go version/environment, run attempt,
merge base/head, target base, timing and digests. `environments.json` records each snapshot's
Go version, GOOS, GOARCH and CGO policy; differing environments produce an
unavailable comparison. The raw lint reports record the actual linter
version, configuration digest and package patterns.

Concrete-owner counts show both sides and package totals. An introduced or
removed owner may be a move; an absent package is unmeasured, never an automatic
zero. Same-package file relocation without count changes produces no type delta.
Complexity identity is package/receiver/function: renames, cross-package moves
and ambiguous tagged declarations stay unmatched. A diagnostic disappearing
below a threshold or behind `nolint` is not a measured zero or improvement.

The type summary puts method-count growth first and labels the largest current
owners as existing context. The lint summary focuses on files changed on either
side, so both ends of file moves remain eligible; changes to global analysis
inputs expand focus to the whole module. Within that focus, newly reported
findings and changed messages come before unmatched blocks and threshold
absences. Identical messages and rows beyond the limit are counted explicitly.
This is inspection order, not a severity ranking across incomparable metrics.
The complete delta retains all rows outside the summary.

## Delivery and cost

Analysis reuses the existing Code job's installed linter and caches. It starts
no Docker or native jobs and adds no required check. Metric collection and
upload are advisory steps: collection failure reports **unavailable**, never
"clean", and cannot turn a metric value into a failing quality gate. No-Go
changes report **not_requested**; documentation-only PRs retain the existing
lightweight Code policy. Normal Code tests, source checks and native selection
are unchanged.

The job summary is available as soon as collection completes. Artifact
`structural-review-RUN_ID-ATTEMPT` retains `receipt.json`, both raw lint reports,
the full lint delta, both concrete-owner inventories, the structure delta and
Markdown summaries and snapshot environments for seven days. Nothing is committed or maintained as a
shared baseline. The report records wall time including analysis-tool builds
and both collections, and maximum child-process RSS on Linux (checkout/fetch and upload are outside
the measurement timer). Cache state and
scope affect those numbers; neither is a token, billing or review-time saving.

After Code checks completes on a branch other than `main`,
`structural-review.yml` runs a trusted publisher from the default branch.
The source-branch filter prevents main CI completions from creating publisher
runs; the job also requires a pull-request source event. GitHub can still
show main as the publisher's branch because it executes trusted default-branch
code. The filter also excludes PRs whose source branch is named `main`,
including fork branches with that name; their reports remain in Code artifacts
and can be published with the manual delivery inputs below.

The publisher verifies GitHub's run/PR association,
workflow identity, latest attempt, exact head/base, artifact digests and the
analysis source. A CI synthetic merge may use a newer target than the event:
its other parent must descend from the event target and belong to the current
target branch ancestry. An unrelated parent is refused. The privileged job executes no PR code and restores no PR
cache. Report headings, tables and inline code render as Markdown in the comment
and job summary. Cell entities stay escaped; raw HTML, notification mentions,
link/image syntax from report data and unexpected block-code fences are neutralized. A stale head, closed PR, duplicate artifact, invalid
receipt or missing report is never posted as a fresh clean result. A newer
report cannot be overwritten by an older run. The source attempt, completed
state and PR association are re-read immediately before creating or updating
the comment; a rerun observed during artifact processing prevents the write.
Updating the one marker-owned
comment avoids notification spam; API writes are not blindly retried. Human
comments are never adopted as bot reports, and the personal CLI can only
preview with `--dry-run`, not publish.

The publisher workflow becomes active when it is on the default branch. It
cannot automatically handle completion events that occurred before that point.
For retained reports, a maintainer can use the CI command workflow's
**structural_report_run** and **structural_report_attempt** dispatch inputs to
republish from the selected trusted engine commit. That authenticated,
write-enabled job runs on GitHub Actions and posts as `github-actions[bot]`;
PR events cannot enter it. It runs no Go analysis, tests or native jobs. Native `/ci` comment authorization and required Code checks are unchanged.
The publisher keeps all source-run identity, freshness and idempotence checks.
This is also how a new publisher can be exercised before it reaches the default
branch, without treating a maintainer-authored comment as bot validation.

## Local use

Build once, then collect the same snapshots and patterns with the same binary.
The guide in [Testing](testing.md#structural-metric-triage) gives the individual
commands. The combined collector accepts full commit SHAs already available in
the checkout and refuses to overwrite an existing output directory:

```sh
python3 .github/scripts/structural_review.py \
  --base BASE_SHA --head HEAD_SHA --output REPORT_DIRECTORY \
  --golangci-lint GOLANGCI_LINT_BINARY
```

Keep the output outside the checkout and uncommitted. Inspect `summary.md` first
and retain the raw evidence with it. The command returns a failure on collection
failure while writing an unavailable receipt when possible. Run substantial
measurements on the authorized Linux execution environment.
