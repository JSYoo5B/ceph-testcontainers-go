# Continuous integration

This page explains what each GitHub Actions workflow checks, how the runtime
batches are planned and run, and how to investigate a failing batch. How to
run tests locally and how to add them is described in [Testing](testing.md).

## Workflows

| Workflow | File | Triggers | What it checks |
| --- | --- | --- | --- |
| Code checks | `code.yml` | push to `main`, pull request, manual; weekly (Monday 05:17 UTC) for the vulnerability check only | Unit tests, race detector, vet, lint, tag compilation, CI tooling tests, compiled tag coverage, Go vulnerability check |
| Docker checks | `docker.yml` | push to `main`, pull request, manual | The `environment` category: Docker bridge behaviour without Ceph |
| Ceph short | `ceph-short.yml` | push to `main`, pull request, manual | The `short` category on Tentacle 20.2.4 |
| Ceph topology | `ceph-topology.yml` | pull request, manual; push to `main` in a release phase | The `topology` category on Tentacle 20.2.4 |
| Ceph multicluster | `ceph-multicluster.yml` | pull request, manual; push to `main` in a release phase | The `multicluster` category on Tentacle 20.2.4 |
| Ceph recovery | `ceph-recovery.yml` | pull request, manual; push to `main` in a release phase | The `recovery` category on Tentacle 20.2.4 |
| Ceph periodic | `ceph-periodic.yml` | every 3 hours, gated (see [Periodic runs](#periodic-runs)); daily at 04:23 UTC for the [representative campaign](#representative-campaign); manual | Every runtime category on every [supported release](#supported-releases), on `main` |
| Ceph release matrix | `ceph-release-matrix.yml` | push to `main` and pull request while `CI_RELEASE_PHASE` is `true`; manual | Every runtime category on each supported release other than the current one |
| Native regressions | `native-regressions.yml` | manual only | The optional `native_rgw_translation` batch, or the deliberately failing `failure_archive` batch |
| CI command | `ci-command.yml` | pull request comment `/ci run ...` | The named runtime batches on the pull request's head commit (see [Running chosen batches from a comment](#running-chosen-batches-from-a-comment)) |

The runtime workflows call the reusable template `tagged-runtime.yml`
(`workflow_call` only). The workflows are independent: none waits for
another, and each can be re-run on its own.

Displayed check names are layered as `<workflow> / <caller job> / <batch>`,
for example `Ceph multicluster / Scenarios / mirror_initial_daemons` or
`Ceph periodic / Squid topology / empty_bootstrap`. Each runtime workflow
also has a `Plan` job.

### Pull request scope

A pull request runs, on Tentacle 20.2.4 role images and in each batch's
default [network mode](#network-modes):
- Code checks, for every change;
- Docker checks and every `short` batch. With the `ci:selected-batches`
  label, Ceph short runs only the batches that declaration selection selects
  (see [Opting in to declaration selection](#opting-in-to-declaration-selection));
- of the `topology`, `multicluster` and `recovery` categories, only the
  batches whose own test files the pull request changes. A change to a shared
  helper or a public package selects none of them; Ceph periodic runs them on
  `main`.

The `ci:full` label (adding it starts a run) and a release phase run every
batch of those categories instead. Each `Plan` job's `tag-plan.json` artifact
lists the changed files and the selected batches.

Own-test-file selection compares proven commits. The caller passes the
event's base commit and the requested head; the planner records both, the
checked-out commit and its parents, and the compared range in
`changed_selection`. A two-parent test merge is accepted only when its second
parent is the requested head and the base is an ancestor of its first parent.
Missing commits, ambiguous merge bases or unrelated history fail planning
instead of selecting nothing. To inspect a selection manually:

```sh
python3 .github/scripts/tag_scenarios.py plan --category recovery \
  --changed-since BASE_SHA --changed-head PR_HEAD_SHA --output plan.json
```

Without `--changed-head` the planner uses a plain merge-base comparison
(recorded as `legacy-merge-base`). `--changed-files FILE` is a separate
manual input.

Pushes to `main` run Code checks, Docker checks and Ceph short. Every other
batch, and every batch on Squid 19.2.5, runs in Ceph periodic within 3 hours
of a new commit on `main`. Pushes to other branches trigger nothing. Pull
requests run on `opened`, `synchronize` and `reopened`; `labeled` (Ceph short
and the scoped categories) and `unlabeled` (Ceph short) start real runs only
for `ci:full` and `ci:selected-batches`.

To run any other batch on a pull request, such as one batch of another
category or a batch on Squid, use `/ci run <batch>`. The command adds a run
and never changes the checks above.

The workflows set `GOPROXY=https://proxy.golang.org|direct`, so a failed
proxy request falls back to the module's origin. The Go jobs share one module
cache keyed by `go.sum`, and the runtime jobs share one build cache keyed by
the installed Go version and `go.sum`. Each runtime `Plan` job fills a missing
module cache with `tag_scenarios.py modules` and a missing build cache with
`tag_scenarios.py buildcache` (one batch per package); runtime jobs and Code
checks only restore them. `setup-go`'s own cache is off. A cache only speeds
up a build: each job still compiles, records and verifies its own binary.

### Documentation-only changes

Runtime workflows (Docker checks and the Ceph category workflows) use
`paths-ignore` for `docs/**`, top-level `*.md`, `LICENSE` and
`internal/integration/goceph/**`, so a change to only those paths runs no
runtime batch. A Markdown file anywhere else, for example under a package's
`testdata/`, can be test input and runs the batches.

Code checks always starts, because its checks are required status checks of
the `main` ruleset. Its `Detect documentation-only changes` job runs
`.github/scripts/docs_only.py`. When that reports `docs_only=true`, Unit
tests, Race detector, `Vet, compile and CI tooling` and Compiled tag coverage
and helpers are skipped as whole jobs (a skipped job reports success), and the
Vulnerability check matrix jobs run with every step skipped so that each keeps
its expanded check name. The skip is by policy, not a pass of those checks.

A pull request is documentation-only when every changed file is on this
allowlist:
- `README.md`, any other top-level `*.md` file and `LICENSE`;
- Markdown files directly inside `docs/contributing/`, `docs/design/` and
  `docs/guides/`, except the files a check reads:
  `docs/design/api-naming*` (the API naming note and the per-package
  catalogues that `internal/apiinventory` compares with the code),
  `docs/design/value-configuration-composition.md`,
  `docs/guides/messenger-client-fixtures.md`,
  `docs/guides/bug-compatibility-tags.md` and
  `docs/contributing/test-double-inventory.md`.

Every other path runs the full Code checks, including a package's
`README.md` (the API inventory test reads it), any non-Markdown or nested file
under `docs/`, and a changed path whose old or new Git mode is not an ordinary
file. `test_docs_only.py` fails when a Go source or `tag_scenarios.py` names a
documentation path that is on the allowlist. Add a newly read file to
`READ_DOC_FILES` or `READ_DOC_PREFIXES` in `docs_only.py`.

The detection fails open. Pushes to `main`, the weekly schedule, manual runs,
pull requests from forks, and any failure to determine the change set (no
base, failed fetch or diff, empty list, script error) run every check. The
dependent jobs use `!cancelled()`, so a failed detection job runs them. The
script compares the pull request's merge commit with the event's base and
lists renames as a deletion and an addition.

## Concurrency and cancellation

| Workflows | Concurrency group | Effect |
| --- | --- | --- |
| Code checks, Docker checks, Ceph short/topology/multicluster/recovery, Ceph release matrix | workflow name, event, then the PR number (pull requests), the ref (pushes) or the run ID (manual runs) | A new commit on a PR cancels that PR's previous run of the same workflow. A new push to `main` cancels the previous `main` push run. Manual runs are never cancelled. |
| Ceph periodic | workflow name and ref, plus `-campaign` for the representative campaign | A new run waits for the run in progress and never cancels it. GitHub keeps one waiting run per group. |
| Ceph topology/multicluster/recovery | as above, plus the run ID for a label other than `ci:full` | An unrelated label starts a run that skips its jobs and cancels nothing. |
| Ceph short | as above, plus the run ID for a label other than `ci:selected-batches` | Adding or removing `ci:selected-batches` replaces the run in progress. |
| CI command | per caller job: PR number, head commit, selected profiles and release | An identical command waits for the one in progress; a third identical command replaces the waiting one, which ends cancelled. Different selections run independently. |
| Native regressions | none | Runs are independent. |

Consequences:
- Different PRs never cancel each other. In a stack, base each PR on the head
  branch of the previous one.
- Changing a PR's base branch starts no checks (`edited` is not a trigger).
  Rebase and push the child after the parent moves.
- A push to `main` discards the `main` run in progress. Push feature work when
  `main` CI is idle (`gh run list --branch main --limit 10`); push a fix for a
  confirmed failure immediately.

## Code checks

| Job | Timeout | Runs |
| --- | --- | --- |
| Unit tests | 15 min | `make test` |
| Race detector | 15 min | `make race` |
| Vet, compile and CI tooling | 25 min | `make vet`, golangci-lint, `make tag-compile`, the Python tests in `.github/scripts` (`test_*.py`), actionlint and the go-ceph harness test |
| Compiled tag coverage and helpers | 25 min | `tag_scenarios.py plan --verify`, then `tag_scenarios.py code` (the Docker-free `code` batch); uploads `code-tag-coverage` |
| Vulnerability check (Go 1.26.x and 1.27.x) | 20 min | `make vulncheck`; uploads `vulncheck-go-<version>` |

The Python tests include `test_tag_scenarios.py` for the planner and
`test_scenario_shards.py`, which checks the workflow files: one workflow per
category, no test-name lists in workflows, budgets taken from the source,
compile before Docker, cleanup on the same runner, and failure reporting that
never hides the test step's status.

### Vulnerability check

`make vulncheck` builds govulncheck v1.9.0 and scans the module with `-test`
for linux/amd64 with no tags, `all` and `all,goceph,native_regression`, and
for linux/arm64 and darwin/arm64 with no tags. The nested go-ceph probe module
needs cgo, so it gets a module scan only.

`.github/scripts/check_vulns.py` decides the result, because govulncheck's
JSON mode exits 0 on findings. Per vulnerability it takes the most precise
level: symbol and package findings fail the check, module-only findings are
listed and pass, and a malformed or incomplete stream fails as a broken scan.

The job runs with the module's minimum Go (`1.26.x`) and the current release
(`1.27.x`), because standard-library findings depend on the toolchain. It
restores the shared module cache and the build cache for its Go version; a
successful default-branch scan saves a missing build cache. Caches never skip
a scan or retain the vulnerability database. Every `setup-go` step sets
`check-latest: true`, and `go.mod` names a `toolchain` line, so an older
patch release with known standard-library vulnerabilities is not used. The
weekly scheduled run catches new advisories; the other Code checks jobs skip
it. `.github/dependabot.yml` proposes Go module updates daily and GitHub
Actions updates weekly.

## How batches are planned

`.github/scripts/tag_scenarios.py` builds the CI matrix from the test source.
Nothing in the workflows names a test.

1. **Catalog.** `internal/tools/tagcatalog` parses every `*_test.go` file
   (skipping hidden directories, `artifacts`, `vendor` and `testdata`) and
   records its `//go:build` expression, tags, `Test...` functions and `//ci:`
   metadata.
2. **Discovery.** Every file that declares tests and has a constraint belongs
   to exactly one category (`ci_<category>`) and one batch
   (`ci_batch_<name>`), compiles under `all`, and carries one valid `//ci:`
   line. One `(category, batch, package)` triple is one profile; its files
   agree on budget and image roles. Optional (`ci_optional`) and SDK
   (`ci_sdk`) files stay out of the required set and need a tag beyond `all`.
3. **Profile tags.** A batch compiles with every capability tag found in the
   source plus `ci`, `ci_<category>`, `ci_batch` and `ci_batch_<name>`. No file
   of another batch may compile into the profile.
4. **Selection.** `plan --category <category>` writes that category's
   profiles as the matrix; `plan --optional --batch <name>` selects one
   optional batch; `plan --representative` selects the campaign set.
5. **Verification.** `plan --verify` compiles the `all`, category and
   single-batch builds with `go test -list` and compares the test names with
   the catalog. Code checks runs it; the runtime workflows do not.

Each profile has a stable ID, `<category>-<batch>-<8 hex digits>`, such as
`short-default-a488da21`, used in artifact names, summaries and annotations.

The planner, compiler and runner always use `CGO_ENABLED=0`, `GOWORK=off` and
`GOFLAGS=-mod=readonly`, and pass only build tags and the batch timeout to
`go test` (no `-run`, `-short` or name filters). To move a test between
batches, change its build constraint.

Checker batches (CephFS process quiescence and process recovery per receipt
kind, and RBD receiver readiness) declare `checker=` and `case=` in `//ci:`.
They run in one network mode per entry, and afterwards
`check_scenario_quiescence.py`, `check_scenario_recovery.py` or
`check_scenario_receivers.py` verifies that mode's evidence in the log. The
planner requires the batches of a checker to cover every mode and kind.

### Bug compatibility selection

A `ceph_bug_<id>` tag selects the compatibility path of one Ceph bug (see the
[policy](bug-compatibility.md)); it never selects tests. The planner never
collects it as a capability, and `all` or `native_regression` never enable
it. Only `--bug-compatibility TAGS`, accepted by `plan`, `verify`, `compile`,
`run`, `code`, `optional` and `buildcache`, selects one:

```sh
python3 .github/scripts/tag_scenarios.py plan --bug-compatibility ceph_bug_<id>
```

The selection is refused when a tag is not registered in
[`bug-compatibility-tags.json`](../guides/bug-compatibility-tags.json), is
retired, is repeated or conflicts with another selected tag. The planner also
refuses a test file that names an unregistered or retired bug tag, and a
registered tag that no source file names. With no selection every ordinary
path compiles.

Both paths of a bug must keep the same tests in the same batch: for every
profile and each package's `all` build the planner toggles each bug tag the
package names and requires identical test names. Toggles that would activate
conflicting tags are skipped. The selected tags apply to every profile of the
plan, appear in its `tags` and `bug_compatibility`, and so change the build
key, compile receipt and history configuration, but not the profile ID.
`run --binary` refuses a binary compiled under another selection.

`tagged-runtime.yml` takes the selection as the `bug_compatibility` input
(empty by default). `Plan` passes it to `plan` and `buildcache`, so an invalid
selection fails before any runtime job. Each runtime job passes it to
`compile`, `run` or `optional` with `--profile-tags` set to its matrix
entry's tags, and fails before building when the recomputed profile differs.
The selection reaches commands only as an argument; `tag_scenarios.py` reads
none from the environment. No caller sets the input, so every workflow runs
the empty selection.

### Advisory declaration selection

`internal/tools/batchselect` narrows `internal/integration` at the level of
declarations, because its `*_helpers_test.go` files compile into every batch.
By default it is only reported:

```sh
python3 .github/scripts/tag_scenarios.py plan --category short \
  --select-declarations origin/main --apply-declarations \
  --summary /dev/stdout --output artifacts/tag-plan.json
```

Without `--apply-declarations`, the plan gains a `declaration_selection`
object (`selected` and `excluded` reasons per profile, `fallback`,
`changed_declarations`) and keeps the full matrix. With it, the matrix keeps
only the selected profiles and `selected_scope` is `declarations`.
`--summary FILE` appends the decisions as a Markdown table.

The tool diffs the checkout against its merge base with the base, type-checks
the package's `all` build (and each batch build not covered by it) with
`go/types`, and builds a graph over top-level declarations. Edges are resolved
identifiers, interface method calls to every implementing type of the package,
and each type to all its methods. A batch is selected when one of its roots
(`Test`, `Fuzz` and `Example` functions, `init` functions and package-level
var initializers it compiles) reaches a changed declaration. An added file
counts as a change to each of its declarations.

Every batch is selected, with the reason, when the change touches anything
the graph cannot attribute: a line outside every declaration (file header,
`//go:` directive), an import of a package that no other file of the build
imports, a deleted, untracked or non-Go file in the package, `.github/`,
`go.mod`, `go.sum`, any path outside `internal/integration` other than
documentation, a removed `init` or initializer, or anything that fails to
parse or type-check. Profiles of other packages are always selected, and a
tool failure selects every profile.

#### Opting in to declaration selection

Ceph short is the only workflow that narrows by declarations, and only for a
pull request with the `ci:selected-batches` label. The caller then passes the
base commit as the `select_declarations` input of `tagged-runtime.yml`, and
`Plan` runs `plan --select-declarations BASE --apply-declarations --summary
"$GITHUB_STEP_SUMMARY"`. The `Plan` summary lists every batch with `yes` or
`no` and its reason.

Adding or removing the label replaces the run in progress; later commits run
selected while the label stays. Every other trigger and workflow runs all its
batches, and the `main` push after a merge runs every Ceph short batch, so a
selection miss fails there. Record a miss with its plan, failed job and
selection evidence in a new issue. The label only helps for changes inside
`internal/integration`.

### Running chosen batches from a comment

A collaborator can run chosen runtime batches on a pull request by commenting:

```text
/ci run <batch|profile>... [on <release|codename|all>...]
```

For example, `/ci run rbd_mirror_bandwidth network_delay on squid` runs two
batches on Squid 19.2.5 role images. The run is added beside the usual checks.

- **Names.** A batch (`network_delay`), category and batch
  (`short/network_delay`), profile name
  (`short/network_delay/internal-integration`) or profile ID, separated by
  blanks or commas. Any runtime category and the optional batches are
  allowed; `code` batches are refused. Duplicates run once.
- **Releases.** After `on`: a release (`20.2.4`), a codename (`tentacle`,
  `squid`) or `all`. Without `on`, the current release. Each batch runs in its
  default network mode.
- **Validation.** Names are checked against the head commit's planner
  (`tag_scenarios.py vocabulary`). One unknown name refuses the whole command,
  and the reply lists every unknown name.
- **Format.** The command must start its line and appear once per comment;
  quoted or fenced lines are ignored. Editing a comment does not run it.
- **Authorization.** Only authors with association `OWNER`, `MEMBER` or
  `COLLABORATOR` run commands; others get a refusal reply. Fork and closed
  pull requests are refused; issue comments and bots are ignored.

The workflow records the pull request's head commit when it reads the pull
request, and every job runs that commit. `issue_comment` runs use the
workflow and scripts from `main`:

| Job | Permissions | What it does |
| --- | --- | --- |
| `Command` | `contents: read`, `pull-requests: read` | Runs `ci_command.py authorize` on `main`'s checkout. No pull request code runs. |
| `Validate` | `contents: read` | Checks out `main` and the recorded head side by side without credentials; the head's planner lists batches and releases, `main`'s `ci_command.py select` resolves the names. |
| `Acknowledge` | `statuses: write` | Sets a pending commit status on the head. |
| `Scenarios <release>` | `contents: read` | Calls `tagged-runtime.yml` with category `selected` and the resolved profile IDs, once per release. |
| `Report` | `contents: read`, `pull-requests: write`, `statuses: write` | Sets the final status and replies with the result, run link or refusal. |

The result appears on the pull request as the commit status
`CI command <comment-id>: <names> on <releases>` (`success`, `failure`, or
`error` for a cancelled run). A context longer than 100 characters is cut and
ends with a 12-character digest of the selection. Refused commands set no
status. A cancelled run is never re-run automatically; comment again or
re-run the workflow run.

To check a command locally on saved event and pull request JSON:

```sh
python3 .github/scripts/tag_scenarios.py vocabulary --output artifacts/vocabulary.json
python3 .github/scripts/ci_command.py authorize --event event.json --pull pull.json
python3 .github/scripts/ci_command.py select \
  --request '{"names": ["network_delay"], "selectors": ["squid"], "duplicates": [], "sha": "<head>", "pull": 1, "comment_id": 1}' \
  --vocabulary artifacts/vocabulary.json
```

## How a batch runs

Each matrix entry is one job on one `ubuntu-24.04` runner:

| Step | Phase in summary | What happens |
| --- | --- | --- |
| Restore the Go module and build caches | n/a | Restores what a `Plan` job saved. A build-cache miss only slows compilation. |
| Prepare Go modules | Go module preparation | `tag_scenarios.py modules` runs `go mod download` within 300 seconds. |
| Compile selected tagged suite | Compilation | `tag_scenarios.py compile` runs `go test -c` within 180 seconds with `GOPROXY=off`, writes `compile/<package>.test`, and records in `compile-report.json` the binary's sha256, its build key (package, tags, Go environment, version, `GOOS`, `GOARCH`), the sha256 of every source file read, and the listed tests. It first refuses a recomputed profile whose tags differ from the matrix entry's. |
| Docker baseline and engine identity | Docker baseline | Records the engine identity and every resource labelled `org.testcontainers=true`. |
| Resolve native published Ceph roles | Published image preparation | Ceph batches only. Pulls the declared immutable role images from `ghcr.io/jsyoo5b/ceph-testcontainers-images` for `linux/amd64`, freezes local image IDs and exports the selected `CEPH_TEST_*_IMAGE` variables. Never builds. |
| Native assertions and completion proof | Native assertions and completion | `tag_scenarios.py run` (or `optional`) with `--binary` runs the compiled binary and applies the pass rules below. |
| Verify owned Docker resource cleanup | Docker resource cleanup | Runs after any failure once the baseline succeeded. Waits up to 30 seconds for Ryuk, then fails if new labelled resources remain. Never deletes anything. |
| Show actual failure phases | n/a | Writes the phase table to the summary and `phases.json`, and annotates failed or cancelled phases. |
| Normalize the scenario history row | n/a | Writes `history-row.json` (see [Scenario history](#scenario-history)). Its failure never changes the job result. |
| Save compile, native and phase results | n/a | Uploads `tag-scenario-<id>`. |
| Identify completed failing tests | n/a | On failure, annotates each failed test or subtest found in `native.log`. |

The job sets `CGO_ENABLED=0`, `TESTCONTAINERS_HOST_OVERRIDE=127.0.0.1` and
`CEPH_TEST_PARALLEL=2`, and uses the latest Go `1.26.x`.

### Reusing the compiled binary

`run --binary <compile-directory>` (and `optional --binary`) executes the
binary that `compile` wrote. Before starting anything it requires a
successful receipt for the same profile and tags, a matching binary sha256, a
matching recomputed build key, unchanged source hashes, and a test inventory
(recorded and from `-test.list=.`) equal to the batch's declared tests. A
failed check never falls back to `go test`. The binary runs in the package
directory with `-test.v -test.count=1` and the batch's `-test.timeout`; a
network-mode entry adds `-test.run` for its `--tests`. `native/report.json`
records `execution: test-binary`, the sha256 and the build key.

Without `--binary`, `run` builds and runs with `go test`. To follow the CI
path locally:

```sh
python3 .github/scripts/tag_scenarios.py compile --category code --batch pure --directory artifacts/c/compile
python3 .github/scripts/tag_scenarios.py run --category code --batch pure --directory artifacts/c/run --binary artifacts/c/compile
```

### Image roles

Ceph batches declare their roles in `//ci:` metadata: `essential`, `client`,
`cephfs`, `rbd-aux` and `rgw`. Ordinary RADOS and RBD use `essential,client`;
CephFS adds `cephfs`, RBD mirroring and cache add `rbd-aux`, and gateways add
`rgw`. An undeclared Ceph profile gets all five; non-Ceph profiles get none.
Only declared roles are prepared, and a missing role is never substituted:
`integrationImages` fails when a used feature's role is missing. The optional
`role_routing` and `role_mirror_refresh` batches cover constrained routing
and can be requested with `/ci run`.

### Network modes

A test that can run on the Docker bridge and on the host network declares the
modes with `networkModes`, its default first, and runs one subtest per mode:

```go
for _, mode := range networkModes(t, "host", "bridge") {
	host, name := mode.Host, mode.Name
	t.Run(name, func(t *testing.T) { /* ceph.WithHostNetwork() when host */ })
}
```

The default is `host` for the RADOS products (`ceph`, `rbd`, `cephfs`),
whose native clients must reach the advertised daemon addresses, and `bridge`
for RGW. A test using a bridge-only feature
(`NetworkConfig.WithSeparateClusterNetwork`, `InterruptNetwork`,
`ApplyNetworkProfile`) declares `bridge` alone or keeps that part in the
bridge subtest.

The planner reads the modes from the call, so they are string literals in the
`Test` function itself, and the files of one batch declare the same modes. A
matrix entry sets `CEPH_TEST_NETWORK`:

| Scope | Entries of a batch |
| --- | --- |
| Pull requests, `main` pushes, CI command (`networks: default`) | One, in the default mode, with all tests |
| Ceph periodic and the release matrix (`networks: all`) | The default entry, plus one per other mode that runs only the mode-declaring tests (`--tests`), named `<batch> (<mode>)` |

A test without `networkModes` runs once and ignores `CEPH_TEST_NETWORK`. Run
locally with an undeclared mode, a test skips; with the variable unset, it
runs every declared mode.

### How a batch passes

The native phase succeeds only if `native.log` shows:

- exit status 0;
- exactly one `=== RUN` and one `--- PASS` for every compiled top-level test,
  and no other top-level test;
- exactly one `--- PASS` for every started subtest;
- no `--- FAIL`, `FAIL`, `panic:` or `--- SKIP` line at any level;
- exactly one `PASS` line after all results, followed by one `ok <package>`
  line under `go test`, or as the last line under `--binary`;
- for checker batches, a passing strict checker.

A skipped test therefore fails CI. Tests assert release differences instead
of skipping (see [Writing the test body](testing.md#writing-the-test-body)).

## Runner time budgets

| Job | Limit |
| --- | --- |
| Code checks jobs | 15 to 25 minutes, fixed in `code.yml` |
| Plan job of each runtime workflow | 10 minutes |
| Each batch job | `job-timeout` from the batch's `//ci:` line |
| The `go test` process of a batch | `timeout` from the batch's `//ci:` line |

The planner accepts a process timeout up to 6 hours and a job timeout of 1 to
360 minutes, and requires the job timeout to exceed the process timeout;
batches leave 10 to 15 minutes for setup and cleanup. Run
`tag_scenarios.py plan` to see every budget. Runners are GitHub-hosted
`ubuntu-24.04` machines with 16 GB of memory. `CEPH_TEST_PARALLEL=2` lets two
opted-in tests run at once in a batch; the rest run sequentially within the
process timeout.

### Runner budget

The account runs at most 40 hosted jobs at once. The constants in
`tag_scenarios.py` divide them:

| Constant | Value | Meaning |
| --- | --- | --- |
| `RUNNER_LIMIT` | 40 | The measured limit. |
| `PULL_REQUEST_RUNNERS` | 16 | Runners that Ceph periodic leaves free for pull requests. |
| `RELEASE_MAX_PARALLEL` | short 4, topology 2, multicluster 2, recovery 2 | `max-parallel` of each release caller in Ceph periodic and Ceph release matrix. |
| `ENVIRONMENT_MAX_PARALLEL` | 1 | `max-parallel` of the periodic environment caller. |
| `CAMPAIGN_MAX_PARALLEL` | 3 | `max-parallel` of the representative campaign. |

A full matrix run holds at most 21 runners and a campaign at most 3; both can
run at once because their concurrency groups differ.
`tag_scenarios.py releases --scope all` fails when the two together could
exceed `RUNNER_LIMIT - PULL_REQUEST_RUNNERS`, and `--scope others` fails when
the release callers alone could. Pull request workflows are unbounded. A bound
only delays jobs: every entry still runs with `fail-fast: false`. Batches are
not packed into shared jobs.

To measure runner use, fill a cache from the GitHub API and report offline:

```sh
python3 .github/scripts/ci_measurements.py fetch --cache artifacts/ci-cache --repo <owner/repo> \
  --since <date> --marker <name>=<commit> [--cache-logs N]
python3 .github/scripts/ci_measurements.py report --cache artifacts/ci-cache --marker <name>=<commit>
python3 .github/scripts/ci_measurements.py cohorts --cache artifacts/ci-cache \
  --history <scenario-history checkout> --repo <owner/repo> --output cohorts.json
python3 .github/scripts/ci_measurements.py caches --cache artifacts/ci-cache --repo <owner/repo> --output caches.json
```

`report` groups executions into cohorts by the newest marker commit their
head contains and gives job counts, runner minutes, wall time, runner
acquisition delay, phase times and simulated makespans. `cohorts` joins jobs
to history rows by repository, run, attempt and job ID and groups them by
configuration (see [Durable history and classification](#durable-history-and-classification));
rows that disagree about one job are listed as conflicts, never merged.
`caches` lists each cache restore, save, import or export with an outcome
taken only from log lines (`--cache-logs` fetches them); a green step is never
read as a hit or a write, and a missing log is `unknown`.

## Periodic runs

`Ceph periodic` runs every runtime category on every
[supported release](#supported-releases), on `main`, in every
[network mode](#network-modes) a batch declares. Its schedule fires every 3
hours, and its `Gate` job (`periodic_gate.py`) decides whether the matrix
runs:

| Situation | Matrix runs |
| --- | --- |
| `main` has a commit that no earlier periodic run executed | yes |
| The last execution of the current commit is 7 days old or older | yes, as a repeat |
| `CI_RELEASE_PHASE` is `true` | yes, on every firing |
| A manual dispatch | yes |
| Otherwise | no; the run ends after `Gate` |

Only a batch job of a runtime caller that ran to an end counts as an
execution; a run that ended in `Gate`, `Releases`, `Pin` or `History` did
not execute the commit.

Dispatch a run to check a branch on both releases before merging:

```sh
gh workflow run "Ceph periodic" --ref <branch>
```

A profile that fails and passes in different runs of the same commit is a
flaky candidate. Record its first failure (run link, commit, image IDs) in an
issue and fix the cause; do not re-run to get a pass.

### Supported releases

`SUPPORTED_RELEASES` in `.github/scripts/run_image_matrix.py` lists each
supported release with its codename and pinned official image (Tentacle
20.2.4 and Squid 19.2.5). The `Releases` jobs generate the callers:

```sh
python3 .github/scripts/tag_scenarios.py releases --scope all --check-prefix 'Ceph periodic'
python3 .github/scripts/tag_scenarios.py releases --scope others --check-prefix 'Ceph release matrix'
```

Each release and category becomes a `<Codename> <category>` call of
`tagged-runtime.yml`; `environment` needs no release and stays a separate
periodic job. The command fails unless every required profile runs on every
release of the scope exactly once, and `test_scenario_shards.py` checks the
same offline.

To add a release, add its row once ceph-testcontainers-images publishes its
role images, run the CI tooling tests, and for a new major release add it to
every release-limited row of the test expectation table (see
[Release differences](testing.md#release-differences)). A release that is not
a row is refused. The default release (`RELEASE`, the `release` defaults of
`tagged-runtime.yml` and the scenario-images action, and `ceph.DefaultImage`)
changes only with the current release.

### Release phase

```sh
gh variable set CI_RELEASE_PHASE --body true    # entering a release phase
gh variable delete CI_RELEASE_PHASE              # after the release
```

While it is `true`, Ceph periodic runs the full matrix on every firing, every
pull request and push to `main` runs every batch on Tentacle, and Ceph
release matrix runs every category on Squid.

### Representative campaign

The campaign repeats a source-owned set of batches on one pinned
configuration to build same-configuration pass and fail records. The set is
every batch whose `//ci:` line declares `representative=<theme>` (see
[Testing](testing.md#batches-and-the-ci-line)); `plan --representative` fails
if a theme (`topology`, `auth`, `mirroring`, `recovery`, `flaky`) has no
batch.

It runs in Ceph periodic on the daily `23 4 * * *` firing or a dispatch with
mode `representative`:

1. `Pin` resolves the commit (the `ref` input or the run's commit) and the
   registry digest of every role image (`periodic_campaign.py pin`).
2. `Representative` calls the template with `category: representative`. Each
   entry repeats `repetitions` times (default 3, at most 10) as separate jobs
   named `<batch> #<n>`, artifacts ending in `-r<n>`, each checking out the
   pinned commit and pulling `tag@digest`. History rows carry
   `repetition_id` `<n>`.
3. Every attempt keeps the normal completion, no-skip and cleanup proofs;
   nothing is retried.

A campaign is bounded at 100 jobs and 3000 summed `job-timeout` minutes, does
not count as a matrix execution for the gate, and has its own concurrency
group.

```sh
gh workflow run "Ceph periodic" -f mode=representative -f repetitions=5 -f release=19.2.5
gh workflow run "Ceph periodic" -f mode=representative -f ref=<commit>
```

`ref` must contain the campaign tooling, because every job runs the pinned
commit's scripts.

## Native regressions

Optional (`ci_optional`) batches are outside every required check. Run one
with `tag_scenarios.py optional --batch <name>`, with `/ci run <name>`, or for
two of them through `Native regressions`.

`native_rgw_translation` holds strict RGW sync-policy cases (numeric pipe
priority, ordinary-user source authorization) that unpatched Ceph 20.2.4 does
not satisfy, so it is expected to fail on the published images. Use it to
test a patched RGW image; an empty `rgw_image` uses the published role image,
and the workflow never builds one:

```sh
gh workflow run "Native regressions" --ref main -f rgw_image=ghcr.io/example/ceph-rgw:patched
```

`cephfs_fuse` (`TestCephFSFuseMount`) mounts CephFS with `ceph-fuse` in
client containers given `/dev/fuse`, `CAP_SYS_ADMIN` and
`apparmor=unconfined`. It needs a native, rootful Linux Docker Engine; on an
engine that refuses the mount it fails naming the missing requirement and
never skips. Outside the tag runner, build it with
`go test -tags all,cephfs_fuse`.

`failure_archive` is described in
[Controlled failed-native download proof](#controlled-failed-native-download-proof).

## Reading a failing batch

1. **Find the batch.** The check name gives the category and batch, for
   example `Ceph recovery / Scenarios / container_pause`.
2. **Read the annotations.** A phase annotation names the failed step and the
   profile ID; `Go test result` annotations list each failed test as
   `<profile id>: TestName/subtest`, or say that none could be identified
   (for example after a kill).

   ```sh
   gh run view <run-id> --log-failed
   gh api 'repos/{owner}/{repo}/check-runs/<job-id>/annotations'
   ```

3. **Check the job summary.** It shows each phase as `success`, `failure`,
   `cancelled`, `skipped`, `not run` or `not required`. A phase shows where
   execution stopped, not the root cause.
4. **Download the artifacts.**

   ```sh
   gh run download <run-id> -n tag-scenario-<profile-id> -D artifacts/ci-<run-id>
   ```

| Artifact | Contents |
| --- | --- |
| `tag-scenario-<id>` | `modules/` and `compile/` logs and reports (the binary is not uploaded); `native/native.log`; `native/report.json`; `native/test-artifacts/` (retained test artifacts such as [failure archives](#failure-archives)); `phases.json`; `history-row.json` |
| `runtime-cleanup-<id>` | `before.json` and `after.json`: engine identity and labelled resources before and after |
| `runtime-cleanup-<id>-baseline` | Only when the baseline itself failed |
| `scenario-images-<id>` | `summary.json` with resolved role image IDs, digests and platform |
| `plan-<category>-<release>` | `tag-plan.json` (selected profiles) and `tag-dispatch.json` (the expanded matrix with network scope, repetitions, run, attempt, release and category) |
| `code-tag-coverage` | `tag-plan.json` with compiled coverage and the `code` batch results |

Uploads are retained for 7 days, or 14 days when the job (or the uploading
composite action) has already failed at upload time.

`native/report.json`:
- `classification`: `passed`; `native-test-failure` (a test failed, panicked
  or hit the Go timeout); `incomplete-skipped-fixture` (a skip);
  `process-or-build-failure` (non-zero exit without a failure line);
  `incomplete-native-proof` (exit 0 but a pass rule or strict checker failed);
- `completion.errors`, `completion.failed_tests`, `completion.skipped_tests`;
- `compiled_tests`, `command`, `elapsed_seconds`, `exit_code`,
  `strict_completion`;
- `test_artifacts`: files left in `native/test-artifacts/` with sizes and the
  number of failure archive manifests.

`modules-report.json` and `compile-report.json` record `exit_code`,
`timed_out`, `timeout_seconds`, `elapsed_seconds` and a `classification` of
`prepared`/`compiled`, `modules-failure`/`compile-failure`,
`modules-timeout`/`compile-timeout`, or `compile-receipt-failure` (the build
succeeded but the receipt was not recorded, so the binary is not reused). A
timed-out command is killed with its process group and has `exit_code: null`.

### Typical causes by phase

| Failed phase | Look at | Common causes |
| --- | --- | --- |
| Go module preparation | `modules/modules.log`, `modules/modules-report.json` | Proxy and origin both failed, or `go.sum` lacks an entry. |
| Compilation | `compile/compile.log`, `compile/compile-report.json` | A test calls a helper that only another batch compiles; run `plan --verify` (see [Testing](testing.md#shared-helpers-must-compile-in-every-batch)). |
| Docker baseline | the step log, `runtime-cleanup-<id>-baseline` | Docker was not ready within 60 seconds. |
| Published image preparation | `scenario-images-<id>` | A role image is missing from the registry or could not be pulled. |
| Native assertions and completion | `native/native.log`, `native/report.json` | An assertion failed, a Ceph wait or the batch timeout expired, or a test skipped. A `panic: test timed out` block lists the tests still running. |
| Docker resource cleanup | `runtime-cleanup-<id>/after.json` | A test left a container, network or volume behind, for example by not registering cleanup when `Run` returned an error. |

Failed integration tests print the last 16 KB of every daemon's log into
`native.log` before the cluster is removed; search for `container ... logs:`.

### Dispatch join

`dispatch_join.py` joins a downloaded `tag-dispatch.json` offline to a saved
jobs-API response of the same run and attempt, optionally with
`history-row.json` receipts:

```sh
python3 .github/scripts/dispatch_join.py --dispatch tag-dispatch.json \
  --jobs jobs.json [--receipts downloaded-artifacts] [--caller "Tentacle short"]
```

Jobs match entries by name. Every entry, job and receipt ends in exactly one
bucket (matched, conflict, ambiguous, unmatched, invalid or out of scope);
exit 3 means the accounting failed and exit 2 that the input was incomplete.
A match proves neither runner allocation nor native execution, and the tool
is not wired into any workflow.

### Failure archives

The runner passes Go's artifact flags in both execution modes
(`-artifacts -outputdir=<native>/test-artifacts`, or the `-test.` forms for
the binary). A test that finalizes its cluster with
[`TerminateOnCleanup`](../../ceph/README.md#terminateoncleanup) and `Archive`
writes its failure archive under
`native/test-artifacts/_artifacts/<package>/<test>/<random>/ceph-<cluster>-<execution>/`
with `diagnostics.json`, `crash-metadata.json` and `manifest.json`. A passing
test leaves no directory.

Creation, upload and availability are separate facts. The test log and the
manifest record what each collector wrote, and `test_artifacts` lists the
files present after the run. For a failed native step, the job summary's
`Failure archive` section records the manifest count, the upload outcome and
the artifact ID. Only downloading the artifact and reading its manifest shows
that the archive was retained. An archive is a snapshot taken before
termination; nothing is archived when the runner is lost or the process is
killed or panics.

`archive_audit.py` audits a downloaded archive directory without changing
any verdict:

```sh
python3 .github/scripts/archive_audit.py <archive-directory> --output audit.json
```

It reports per file and per archive the containment, recorded write outcome,
JSON readability and size agreement, and joins each `capture_files` record to
its file entry. `evidence_complete` is true only when the producer declared
the archive complete, the manifest is final with a termination, and every
file and capture copy is verified. A producer-declared failure is never
reported as download corruption.

#### Controlled failed-native download proof

The optional `failure_archive` batch runs two deliberately failing native
subtests (an ordinary assertion and a finalization check) through the real
`testing.T`, each producing an archive with its trigger, snapshot and debug
restore receipts. Run it only on explicit request:

```sh
gh workflow run native-regressions.yml --ref <branch> -f failure_archive=true
```

The native step and the workflow must fail. Then download the attempt's
artifacts by their exact names and audit them from a checkout of that head:

```sh
gh run download <run-id> -n <tag-scenario-artifact> -D artifacts/archive-proof/scenario
gh run download <run-id> -n <scenario-images-artifact> -D artifacts/archive-proof/images
gh run download <run-id> -n <runtime-cleanup-artifact> -D artifacts/archive-proof/cleanup
python3 .github/scripts/failed_archive_proof.py \
  --scenario artifacts/archive-proof/scenario \
  --images artifacts/archive-proof/images --cleanup artifacts/archive-proof/cleanup \
  --root . --revision <full-checked-out-commit> --run-id <run-id> --attempt <attempt> \
  --output artifacts/archive-proof/audit.json
```

Exit 0 means `archive_verification: verified` with `native_outcome: failed`:
both failed cases, matching binary and source hashes, image identities, both
archives with their listed files, debug receipts, successful restoration and
termination, no leaked resources, and the history row's identity. Do not mix
artifacts of different attempts. The audit does not prove the upload itself;
keep the upload step and artifact ID as separate evidence.

### Scenario history

`scenario_history.py` turns one job's receipts into one row of schema
`ceph-scenario-history/v1`. It reads files only and never infers a cause. The
workflow writes it as `history-row.json`; to build one from downloads:

```sh
python3 .github/scripts/scenario_history.py normalize \
  --scenario a/scenario --cleanup a/cleanup --images a/images \
  --repository <owner/repo> --run-id <run-id> --run-attempt <attempt> --job-id <job-id> \
  --output row.json
```

A row holds:
- `execution_key`: repository, workflow, run, attempt, job, repetition,
  profile ID, checker, case and network mode;
- `source`: the requested head and the checked-out commit and tree;
- `build`: build key, binary sha256, a digest of the source hashes, and the
  native phase's binary identity (`native_binary` says whether they match);
- `profile`: profile identity, tags and `bug_compatibility`. Receipts that
  disagree on tags, or whose selection differs from `--bug-compatibility`,
  write no row;
- `runtime`: release, layout, platform and every role image ID and digest, or
  `not_applicable`;
- `phases`: per phase the step outcome, the receipt and their join. A pass
  needs both; a missing receipt is `missing`, never `passed`;
- `outcome.status` (`failed`, `cancelled`, `passed` or `incomplete`) and
  `first_failure`;
- `tests`: every RUN, PASS, FAIL and SKIP line with its duration;
- `comparability`: `complete`, or `incomplete` with what is missing;
- `evidence`: the sha256 of every input and `row_sha256`.

```sh
python3 .github/scripts/scenario_history.py ingest --history history.jsonl row.json
python3 .github/scripts/scenario_history.py summary --history history.jsonl
```

`ingest` skips identical rows and refuses a different row under the same
execution key or a row without run identity. `summary` counts outcomes per
profile and release, failures by phase and classification, and p50/p90 native
durations; cancelled rows are left out of the failure rate.

#### Durable history and classification

The `History` job of every Ceph periodic run that executes the matrix or a
campaign runs `scenario_history.py collect` on a checkout of the
`scenario-history` branch. It appends the run's rows to
`rows/<run-id>-<run-attempt>.jsonl`, rewrites `summary.json` and pushes. A
conflicting row fails the job and changes nothing.

`summary.json` (schema `ceph-scenario-history/v1/classification`) has, per
profile and release, the last 20 attempts and the 10 most recent
configurations. A configuration is the profile, network mode, release,
checked-out tree, build key (which includes the bug compatibility selection),
platform and every role image ID; rows without a complete identity count as
`incomparable_rows`. Each configuration records passes, failures, excluded
attempts by kind, the first failure, per-test `failed`/`ran` counts (only
attempts whose log has the test's result) and a classification.

`passed`, `assertion_failure`, `timeout` and `cleanup_failure` are scenario
outcomes. `runner_failure`, `image_failure`, `cancelled` and `incomplete` are
excluded.

| Classification | Scenario outcomes of the configuration |
| --- | --- |
| `insufficient_samples` | fewer than two |
| `confirmed_pass` | two or more, all passed |
| `deterministic_failure` | two or more, all failed with the same phase, test and classification |
| `confirmed_fail` | two or more, all failed, in different ways |
| `suspected_intermittent` | at least one pass and one failure |

```sh
git fetch origin scenario-history
git worktree add artifacts/history origin/scenario-history
python3 .github/scripts/scenario_history.py classify --history-dir artifacts/history
```

When a runtime job fails, `report_test_failures.py` reads `summary.json` and
adds `Scenario history` notices beside the failed tests, such as
`TestClientRevocation/aes failed 2/10 earlier same-config attempts that ran
it`, with the configuration's classification. The notices never decide the
job's result.

### Reproducing and re-running

Re-run only the failed jobs when the failure looks unrelated to the change:

```sh
gh run rerun <run-id> --failed
```

To reproduce a batch locally with the same tags, budget and pass rules, run
the tag runner; for identical images, first follow
[Reproducing CI's role images](testing.md#reproducing-cis-role-images):

```sh
python3 .github/scripts/tag_scenarios.py run \
  --category recovery --batch container_pause --package ./internal/integration \
  --directory artifacts/repro-container-pause
```

Local runs are sequential unless `CEPH_TEST_PARALLEL` is set. To isolate one
test, use `go test -tags=all -run` (see [Testing](testing.md#one-test)).
