# Continuous integration

This page explains what each GitHub Actions workflow checks, how the runtime
batches are planned and run, and how to investigate a failing batch. How to
run tests locally and how to add them is described in [Testing](testing.md).

## Workflows

| Workflow | File | Triggers | What it checks |
| --- | --- | --- | --- |
| Code checks | `code.yml` | push to `main`, pull request, manual; weekly (Monday 05:17 UTC) for the vulnerability check only | Unit tests, race detector, vet, tag compilation, CI tooling tests, compiled tag coverage, Go vulnerability check |
| Docker checks | `docker.yml` | push to `main`, pull request, manual; not for documentation-only changes | The `environment` category: Docker bridge behaviour without Ceph |
| Ceph short | `ceph-short.yml` | push to `main`, pull request, manual; not for documentation-only changes | The `short` category on Tentacle 20.2.4 |
| Ceph topology | `ceph-topology.yml` | pull request, manual; push to `main` in a release phase; not for documentation-only changes | The `topology` category on Tentacle 20.2.4: on a pull request only the changed batches (see [Pull request scope](#pull-request-scope)) |
| Ceph multicluster | `ceph-multicluster.yml` | pull request, manual; push to `main` in a release phase; not for documentation-only changes | The `multicluster` category on Tentacle 20.2.4: on a pull request only the changed batches (see [Pull request scope](#pull-request-scope)) |
| Ceph recovery | `ceph-recovery.yml` | pull request, manual; push to `main` in a release phase; not for documentation-only changes | The `recovery` category on Tentacle 20.2.4: on a pull request only the changed batches (see [Pull request scope](#pull-request-scope)) |
| Ceph periodic | `ceph-periodic.yml` | every 3 hours, gated (see [Periodic runs](#periodic-runs)); manual | Every runtime category on Tentacle 20.2.4 and Squid 19.2.5, on `main` |
| Ceph release matrix | `ceph-release-matrix.yml` | push to `main` and pull request while `CI_RELEASE_PHASE` is `true`; manual | Every runtime category on Squid 19.2.5 |
| Native regressions | `native-regressions.yml` | manual only | The optional `native_rgw_translation` batch, with an optional RGW image override |

Every runtime workflow calls the reusable template `tagged-runtime.yml`
(`workflow_call` only; it has no triggers of its own). The workflows are
independent: none waits for another, and each can be re-run on its own. A
failed check name therefore tells you the kind of failure before you open it.

### Pull request scope

A pull request runs, on Tentacle 20.2.4 role images and in each batch's
default [network mode](#network-modes):
- Code checks, for every change;
- Docker checks and every `short` batch, which includes the netem helper's
  `network_delay` batch;
- of the `topology`, `multicluster` and `recovery` categories, only the
  batches whose own test files (the files that declare the batch's tests) the
  pull request changes. A change to a shared helper or a public package
  selects none of them; [Ceph periodic](#periodic-runs) runs them on `main`.

Two things run every batch of those categories on the pull request instead:
the `ci:full` label (adding it starts the run), and a release phase. A change
that touches nothing but `docs/`, Markdown files, `LICENSE` or the go-ceph
probe under `internal/integration/goceph/` runs only Code checks: no runtime
batch reads those paths, and Code checks still runs the API inventory test
against `docs/`. Each `Plan` job's `tag-plan.json` artifact lists the changed
files and the selected batches.

Pushes to `main` run Code checks, Docker checks and Ceph short. Every other
batch, and every batch on Squid 19.2.5, runs in Ceph periodic within 3 hours
of a new commit on `main`.

Pushes to branches other than `main` trigger nothing. Pull requests are
checked through the `pull_request` event (`opened`, `synchronize`,
`reopened`), so a commit on a PR branch is checked once, not once as a push
and again as a pull request.

Both workflow files set `GOPROXY=https://proxy.golang.org|direct`. With `|`,
the go command fetches a module from its origin after any failed proxy
request, such as a reset HTTP/2 stream; with the default `,` it gives up on
everything but a 404 or 410, which once failed a whole batch at compilation.
`go.sum` verifies a module from either source.

The Go jobs share one module cache, keyed by `go.sum`. The `Plan` job of each
runtime workflow restores it and, when it is missing, downloads every module
with `tag_scenarios.py modules` and saves it. Runtime jobs start only after
`Plan`, and Code checks jobs only restore it. `setup-go`'s own cache is off:
it keeps whatever the first job to finish under a key had, which once was a
`Plan` job with almost no modules, so every later compile downloaded its
modules again.

### Check names

Displayed names are layered as `<workflow> / <caller job> / <batch>`. For
example, `Ceph multicluster / Scenarios / mirror_initial_daemons` is the
`mirror_initial_daemons` batch of the multicluster category, and
`Ceph periodic / Squid topology / empty_bootstrap` is a Squid topology batch.
Each runtime workflow also has a `Plan` job.

## Concurrency and cancellation

| Workflows | Concurrency group | Effect |
| --- | --- | --- |
| Code checks, Docker checks, Ceph short/topology/multicluster/recovery, Ceph release matrix | workflow name, event, then the PR number (pull requests), the ref (pushes) or the run ID (manual runs) | A new commit on a PR cancels that PR's previous run of the same workflow. A new push to `main` cancels the previous `main` push run of the same workflow. Manual runs are never cancelled and cancel nothing. |
| Ceph periodic | workflow name and ref | A new run waits for the run in progress on the same ref and never cancels it, so every started run finishes. GitHub keeps one waiting run per group. |
| Ceph topology/multicluster/recovery | as above, plus the run ID for a label other than `ci:full` | Adding an unrelated label starts a run that skips its jobs; its own group keeps it from cancelling the pull request's run. |
| Native regressions | none | Runs are independent. |

Practical consequences:

- Different PRs never cancel each other, because the group contains the PR
  number. In a stack of PRs, base each PR on the head branch of the PR before
  it; opening a child PR does not cancel the parent's checks.
- Changing a PR's base branch does not start new checks, because `edited` is
  not a trigger. After the parent is merged or rebased, rebase the child and
  push it.
- A push to `main` throws away the results of the `main` run in progress. Push
  new feature work when `main` CI is idle
  (`gh run list --branch main --limit 10`). Push a fix for a confirmed failure
  immediately: cancelling the superseded run is then what you want.

## Code checks

| Job | Timeout | Runs |
| --- | --- | --- |
| Unit tests | 15 min | `make test` |
| Race detector | 15 min | `make race` |
| Vet, compile and CI tooling | 25 min | `make vet`, golangci-lint (`make lint`), `make tag-compile`, the Python tests in `.github/scripts` (`test_*.py`) and the go-ceph harness test |
| Compiled tag coverage and helpers | 25 min | `tag_scenarios.py plan --verify`, then `tag_scenarios.py code` (the Docker-free `code` batch) |
| Vulnerability check (Go 1.26.x and 1.27.x) | 20 min | `make vulncheck` |

The tag coverage job uploads the `code-tag-coverage` artifact with
`tag-plan.json` and the `code` batch results.

### Vulnerability check

`make vulncheck` builds govulncheck v1.9.0 for the host and scans the module
with `-test` for linux/amd64 with no tags, `all` and
`all,goceph,native_regression`, and for linux/arm64 and darwin/arm64 with no
tags. The nested go-ceph probe module needs cgo, so it gets a module scan,
which checks its required module versions but not its call paths.

govulncheck's JSON mode exits 0 even when it reports vulnerabilities, so
`.github/scripts/check_vulns.py` decides. For each vulnerability it takes the
most precise level reported:

- symbol (the code calls a vulnerable function) and package (the code
  imports a vulnerable package) fail the check;
- module (only a required module is affected) is listed and passes, for
  example the unfixed `golang.org/x/crypto/openpgp` advisory GO-2026-5932,
  whose package this module does not import;
- a stream without the scanner's config, malformed JSON or an unknown
  finding fails as a broken scan.

The job runs with the module's minimum Go (`1.26.x`) and the current release
(`1.27.x`), because the standard library's findings depend on the toolchain,
and uploads every scan as `vulncheck-go-<version>`. Every `setup-go` step of
the workflows sets `check-latest: true`, so a runner's cached older patch is
not used: Go 1.26.8, for example, has eleven standard-library vulnerabilities
that the check reports and that 1.26.9 fixes. `go.mod` also names
`toolchain go1.26.9`, so a `go` command of an older 1.26 patch switches to it
in this repository (with the default `GOTOOLCHAIN=auto`). The `toolchain`
line does not apply to modules that use this one. A weekly scheduled run
catches new advisories without a source change; the other Code checks jobs
skip it. `.github/dependabot.yml` proposes Go module updates daily, with
Testcontainers for Go and its modules in one group, and GitHub Actions
updates weekly.

The Python tests include `test_tag_scenarios.py` for the planner and
`test_scenario_shards.py`, which checks the workflow files themselves: one
workflow per category, no test-name lists in workflows, budgets taken from the
source, compile before Docker, cleanup on the same runner, and failure
reporting that never hides the test step's status. Edit a workflow and these
tests tell you if the edit breaks those rules.

## How batches are planned

`.github/scripts/tag_scenarios.py` builds the CI matrix from the test source.
Nothing in the workflows names a test.

1. **Catalog.** `tools/tagcatalog` parses every `*_test.go` file in the
   module (skipping hidden directories, `artifacts` and `vendor`) without
   importing or running it. For each file it records the `//go:build`
   expression, its tags, the `Test...` functions and the `//ci:` metadata.
2. **Discovery.** Every file that declares tests and has a constraint must
   belong to exactly one category (`ci_<category>`) and one batch
   (`ci_batch_<name>`), compile under `all`, and carry one valid `//ci:` line.
   Files without tests must not carry `//ci:`. One `(category, batch,
   package)` triple is one profile; all its files must agree on the budget
   and the image roles.
   Optional (`ci_optional`) and SDK (`ci_sdk`) files are kept out of the
   required set and must need an extra tag beyond `all`.
3. **Profile tags.** A batch compiles with every capability tag found in the
   source (`integration`, `auth`, `diagnostics`, `features`, `hostnetwork`,
   `multicluster`, `topology`, and any new one) plus `ci`, `ci_<category>`,
   `ci_batch` and `ci_batch_<name>`. The planner checks that no file of
   another batch compiles into the profile.
4. **Selection.** `plan --category <category>` keeps that category's profiles
   and writes them as the job matrix. `plan --optional --batch <name>` selects
   one optional batch.
5. **Verification.** `plan --verify` compiles the `all` build, each category
   build and each single-batch build with `go test -list` and compares the
   compiled test names with the catalog. Code checks runs it for every pull
   request and every push to `main`; the runtime workflows do not repeat it.

Each profile has a stable ID, `<category>-<batch>-<8 hex digits>`, such as
`short-default-a488da21`. The ID appears in artifact names, the job summary
and annotations.

The planner, compiler and runner always use `CGO_ENABLED=0`, `GOWORK=off` and
`GOFLAGS=-mod=readonly`, so overlays, extra tags or workspace replacements
from the environment cannot change what runs. The runner passes only build
tags and the batch timeout to `go test`: no `-run`, no `-short`, no name
filters. To move a test between batches, change its build constraint.

Multi-cluster CephFS process-quiescence and process-recovery scenarios are
split into one batch per receipt kind (`peer`, `directory`), and RBD receiver
readiness has a batch of its own. Those batches declare `checker=`, and the
kind as `case=`, in `//ci:`; their test declares its network modes like any
other (see [Network modes](#network-modes)). A checker validates one mode's
run, so the runner runs such a batch in the matrix entry's mode, or in its
default mode when no entry selects one, and after the run a dedicated checker
(`check_scenario_quiescence.py`, `check_scenario_recovery.py`,
`check_scenario_receivers.py`) verifies that mode's leaf-specific evidence in
the log. The planner rejects these batches unless together they cover every
mode and kind of the checker. Locally, the `all` build still runs the original
parent test with every leaf.

## How a batch runs

For each matrix entry, one job on one `ubuntu-24.04` runner performs these
steps in order:

| Step | Phase in summary | What happens |
| --- | --- | --- |
| Restore the Go module cache | n/a | Restores the module cache that the `Plan` job saved. |
| Prepare Go modules | Go module preparation | `tag_scenarios.py modules` runs `go mod download` within 300 seconds. With a restored cache it finds every module at once; otherwise this is the only step that downloads. |
| Compile selected tagged suite | Compilation | `tag_scenarios.py compile` builds the batch's test binary with `go test -c` within 180 seconds, with `GOPROXY=off`, so it reads only the module cache. It writes the binary as `compile/<package>.test` (for example `internal-integration.test`) and records in `compile-report.json` its sha256, its build key (package, tags, the fixed Go environment, Go version, `GOOS` and `GOARCH`), the sha256 of every source file that the build reads, and the tests that the binary lists. No test runs. |
| Docker baseline and engine identity | Docker baseline | Records the engine identity and every container, network and named volume labelled `org.testcontainers=true`. |
| Resolve native published Ceph roles | Published image preparation | Only for Ceph batches. Resolves the batch's roles of `official-<release>-{control,osd,rgw,mds}` from `ghcr.io/jsyoo5b/ceph-testcontainers-images` for `linux/amd64` to immutable image IDs and exports them as `CEPH_TEST_IMAGE`, `CEPH_TEST_OSD_IMAGE`, `CEPH_TEST_RGW_IMAGE`, `CEPH_TEST_MDS_IMAGE`, with `SCENARIO_IMAGE_LAYOUT=roles` and `SCENARIO_IMAGE_ROLES`. A batch gets all four roles unless its `//ci:` line declares `image-roles` (see [Image roles](#image-roles)). Pulls only, never builds. |
| Native assertions and completion proof | Native assertions and completion | `tag_scenarios.py run` (or `optional`) with `--binary artifacts/scenario/compile` runs the binary of the compile step instead of building again (see [Reusing the compiled binary](#reusing-the-compiled-binary)), and applies the pass rules below. |
| Verify owned Docker resource cleanup | Docker resource cleanup | Runs even after a failure, if the baseline succeeded. Waits up to 30 seconds for Ryuk, then fails if new labelled resources remain. It never deletes anything. |
| Show actual failure phases | n/a | Writes the phase table to the job summary and `phases.json`, and an annotation for each failed or cancelled phase. |
| Normalize the scenario history row | n/a | Writes `history-row.json` from the saved receipts (see [Scenario history](#scenario-history)). Its own failure never changes the job's result. |
| Save compile, native and phase results | n/a | Uploads `tag-scenario-<id>`. |
| Identify completed failing tests | n/a | On failure, adds one annotation per failed test or subtest name found in `native.log`. |

The job environment sets `CGO_ENABLED=0`, `TESTCONTAINERS_HOST_OVERRIDE=127.0.0.1`
and `CEPH_TEST_PARALLEL=2`. Go is the latest `1.26.x`. Tentacle workflows use
role images for release 20.2.4; the Squid workflows pass release 19.2.5.

### Reusing the compiled binary

`run --binary <compile-directory>` (and `optional --binary`) executes the
binary that `compile` wrote to that directory. Before it creates the native
directory or starts anything, it checks the receipt in `compile-report.json`
and stops with an error when any check fails:

- the receipt exists, records a successful compile and belongs to the same
  profile and tags;
- the binary's sha256 matches the receipt;
- the build key computed now matches the recorded one, so a different Go
  version or platform is rejected;
- every source file that the build reads still has its recorded sha256;
- the recorded inventory, and the binary's own `-test.list=.` output, equal
  the tests that the source files declare for the batch.

A failed check never falls back to `go test`. The binary runs in the package
directory, as `go test` runs it, with `-test.v -test.count=1` and
`-test.timeout` from the batch's `//ci:` line. A network-mode entry adds
`-test.run` for the tests of `--tests`. The environment is the same as with
`go test`. `native/report.json` records `execution: test-binary` and the
binary's sha256 and build key.

Without `--binary`, `run` builds and runs the batch with `go test`, which
suits local use. To follow the CI path locally, compile first and pass the
compile directory:

```sh
python3 .github/scripts/tag_scenarios.py compile --category code --batch pure --directory /tmp/c/compile
python3 .github/scripts/tag_scenarios.py run --category code --batch pure --directory /tmp/c/run --binary /tmp/c/compile
```

### Image roles

A Ceph batch that needs fewer than the four role images declares them in its
`//ci:` line. `supervised_command` declares `image-roles=control`: it starts
one idle control-image container and no cluster. The planner accepts
`control`, `osd`, `rgw` and `mds`, requires `control`, rejects the field on a
batch that needs no Ceph, and puts the roles in the matrix as `image_roles`.
Every other Ceph batch gets all four. The preparation step inspects and pulls
only the listed roles, so an image that the batch does not use cannot fail
it. With `SCENARIO_IMAGE_LAYOUT=roles`, `integrationImages` fails for a role
that was not prepared instead of using the control image. The image
compatibility matrix always uses all four roles.

### Network modes

A test that can run its cluster on the Docker bridge and on the host network
declares the modes with `networkModes`, its default first, and runs one
subtest per returned mode:

```go
for _, mode := range networkModes(t, "host", "bridge") {
	host, name := mode.Host, mode.Name
	t.Run(name, func(t *testing.T) { /* ceph.WithHostNetwork() when host */ })
}
```

The default is `host` for the RADOS products (`ceph`, `rbd`, `cephfs`): their
clients reach every daemon at the address that the maps advertise, which a
native client in the test process can use only on the host network. It is
`bridge` for RGW, whose one HTTP endpoint a port mapping serves. A test that
uses a bridge-only feature (`WithSeparateClusterNetwork`, `InterruptNetwork`,
`TemporaryNetworkDelay`) declares `bridge` alone, or keeps that part in the
bridge subtest.

The planner reads the modes from the call (`tools/tagcatalog`), so they are
string literals, the call is in the `Test` function itself, and the test files
of one batch that declare modes declare the same ones. A matrix entry sets
`CEPH_TEST_NETWORK`:

| Scope | Entries of a batch |
| --- | --- |
| Pull requests and `main` pushes (`networks: default`) | One, in the batch's default mode, with all its tests |
| Ceph periodic and the release matrix (`networks: all`) | The default entry, and one more per other mode that runs only the tests that declare modes (`--tests`); its job is named `<batch> (<mode>)` |

A test without `networkModes` runs once, in the default entry, and ignores
`CEPH_TEST_NETWORK`. A mode that a test does not declare is never scheduled;
run locally with such a `CEPH_TEST_NETWORK`, the test skips. Unset, a test runs
in every mode it declares.

### How a batch passes

The native phase succeeds only if all of these hold in `native.log`:

- `go test` exits with status 0;
- every top-level test compiled into the profile has exactly one `=== RUN` and
  one `--- PASS` line, and no other top-level test ran;
- every started subtest has exactly one `--- PASS`;
- there is no `--- FAIL`, `FAIL` or `panic:` line;
- there is no `--- SKIP` line at any level;
- exactly one `PASS` line follows all test results. After `go test`, exactly
  one `ok <package>` line follows it. A test binary run with `--binary` prints
  no `ok` line, so its `PASS` must be the last line of the log, and its exit
  status and verified receipt bind the run to the package;
- for checker batches, the strict checker also passes.

A skipped test therefore fails CI. Tests must assert release differences
instead of skipping (see
[Writing the test body](testing.md#writing-the-test-body)).

## Runner time budgets

| Job | Limit |
| --- | --- |
| Code checks jobs | 15 or 25 minutes, fixed in `code.yml` |
| Plan job of each runtime workflow | 10 minutes |
| Each batch job | `job-timeout` from the batch's `//ci:` line |
| The `go test` process of a batch | `timeout` from the batch's `//ci:` line |

The planner accepts a process timeout of up to 6 hours and a job timeout of 1
to 360 minutes (the GitHub-hosted runner limit), and requires the job timeout
to exceed the process timeout. Existing batches leave 10 to 15 minutes between
them for checkout, Go setup, compilation, image preparation and the cleanup
check. Current budgets range from `15m`/25 minutes for small batches to
`150m`/160 minutes for the longest multi-cluster batch; run
`tag_scenarios.py plan` to see them all.

Runners are standard GitHub-hosted `ubuntu-24.04` machines with 16 GB of
memory. `CEPH_TEST_PARALLEL=2` lets two opted-in tests, each with its own
cluster, run at once inside a batch; all other tests in the batch run
sequentially and share the process timeout. Splitting a long test into its own
batch shortens the critical path when runners are free, but repeats checkout,
image preparation and the cleanup check, so total runner time grows.

## Periodic runs

`Ceph periodic` runs every runtime category on Tentacle 20.2.4 and Squid
19.2.5 role images, on `main`, in every [network mode](#network-modes) that a
batch's tests declare. Its schedule fires every 3 hours, and its
`Gate` job (`.github/scripts/periodic_gate.py`) decides whether the matrix
runs:

| Situation | Matrix runs |
| --- | --- |
| `main` has a commit that no earlier periodic run executed | yes |
| The last execution of the current commit is 7 days old or older | yes, as a repeat |
| The repository variable `CI_RELEASE_PHASE` is `true` | yes, on every firing |
| A manual dispatch | yes |
| Otherwise | no; the run ends after `Gate` |

While development is active, a run therefore checks the commits merged since
the previous one, and a failure points at those commits. When `main` stays
still, as in a release phase, the same commit runs again and again.

### Release phase

Set the repository variable for the days before a release:

```sh
gh variable set CI_RELEASE_PHASE --body true    # entering a release phase
gh variable delete CI_RELEASE_PHASE              # after the release
```

While it is `true`:
- Ceph periodic runs the full matrix every 3 hours, even on an unchanged
  commit;
- every pull request and push to `main` runs every batch of every category
  on Tentacle, and Ceph release matrix runs every category on Squid.

### Flaky profiles

Runs of an unchanged commit are how flaky profiles are found. A profile that
fails in one periodic run and passes in another on the same commit is a flaky
candidate: keep its first failure, with the run link, the commit and the image
IDs, in an issue, and fix the cause. Do not re-run the job to get a pass. The
gate never repeats a failed execution of a commit on its own; a repeat comes
from a release phase, the weekly repeat or a dispatch.

Dispatch a run to check a branch on both releases before it merges:

```sh
gh workflow run "Ceph periodic" --ref <branch>
gh run list --workflow "Ceph periodic" --limit 3
gh run watch <run-id>
```

`--ref` may name any branch on GitHub that contains the workflow. A dispatch
always runs the matrix.

## Native regressions

`Native regressions` runs the optional `native_rgw_translation` batch: strict
RGW sync-policy cases (numeric pipe priority, ordinary-user source
authorization) that unpatched Ceph 20.2.4 does not satisfy. The batch is not
part of any required check and keeps its assertions strict, so on the
published images it is expected to fail. Use it to test a patched RGW image:

```sh
gh workflow run "Native regressions" --ref main -f rgw_image=ghcr.io/example/ceph-rgw:patched
```

An empty `rgw_image` uses the published official RGW role image. The workflow
never builds an image. The other optional batch, `native_shuffle`, has no
workflow and is run locally with `tag_scenarios.py optional`.

## Reading a failing batch

1. **Find the failing workflow and batch.** The check name gives the category
   and batch, for example `Ceph recovery / Scenarios / container_pause`.
2. **Read the annotations.** A phase annotation such as
   `Ceph recovery / Native assertions and completion` says which step failed
   and gives the profile ID. `Go test result` annotations list each failed
   test or subtest as `<profile id>: TestName/subtest`. If the log has no
   completed failure (for example the process was killed), a notice says that
   the failed test could not be identified. Annotations are also available
   through the API:

   ```sh
   gh run view <run-id>                     # jobs and their status
   gh run view <run-id> --log-failed        # logs of the failed steps
   gh api 'repos/{owner}/{repo}/check-runs/<job-id>/annotations'
   ```

3. **Check the job summary.** It shows the outcome of each phase:
   `success`, `failure`, `cancelled`, `skipped`, `not run`, or `not required`
   for image preparation in batches without Ceph. A phase outcome shows where
   execution stopped; it does not establish the root cause. A Ceph timeout,
   for instance, is not automatically an infrastructure problem.
4. **Download the artifacts.**

   ```sh
   gh run download <run-id> -n tag-scenario-<profile-id> -D artifacts/ci-<run-id>
   ```

| Artifact | Contents |
| --- | --- |
| `tag-scenario-<id>` | `modules/modules.log` and `modules/modules-report.json` (download output and outcome); `compile/compile.log` and `compile/compile-report.json` (compiler output, outcome and binary receipt; the binary itself is not uploaded); `native/native.log` (complete verbose test output); `native/report.json` (verdict); `phases.json` (phase outcomes); `history-row.json` (the job's [history row](#scenario-history)) |
| `runtime-cleanup-<id>` | `before.json` and `after.json`: engine identity, labelled resources before and after, and anything left behind |
| `runtime-cleanup-<id>-baseline` | Uploaded only when the baseline itself failed |
| `scenario-images-<id>` | `summary.json` with the resolved role image IDs, digests and platform |
| `plan-<category>` | `tag-plan.json` from the `Plan` job, and `modules/` when it downloaded the modules |
| `code-tag-coverage` | `tag-plan.json` with compiled coverage, and the `code` batch results (Code checks) |

`native/report.json` has these fields of interest:

- `classification`:
  - `passed`;
  - `native-test-failure`: a test failed, panicked or hit the Go timeout;
  - `incomplete-skipped-fixture`: a test or subtest was skipped;
  - `process-or-build-failure`: `go test` or the test binary exited non-zero
    without any failure line, for example because the process was killed;
  - `incomplete-native-proof`: the exit status was 0 but a pass rule failed,
    such as a missing `--- PASS` or a failed strict checker.
- `completion.errors`: which pass rules failed.
- `completion.failed_tests` and `completion.skipped_tests`.
- `compiled_tests`: the tests the batch was expected to run.
- `command`, `elapsed_seconds`, `exit_code`.
- `strict_completion`: the checker result for checker batches.

`modules-report.json` and `compile-report.json` record `exit_code`,
`timed_out`, `timeout_seconds` and `elapsed_seconds`. Their `classification`
is `prepared` or `compiled`, `modules-failure` or `compile-failure` for a
nonzero exit, and `modules-timeout` or `compile-timeout` when the bound
expired. `compile-receipt-failure` means the build succeeded but its receipt
could not be recorded, so the binary is not reused. A timed-out command is
killed with its whole process group, its `exit_code` is `null`, and its log
keeps the output written until then.

`phases.json` holds the same phase outcomes as the job summary, with
`failed_phases` and `cancelled_phases` lists.

### Scenario history

`scenario_history.py` turns one job's receipts into one JSON row with schema
`ceph-scenario-history/v1`, and keeps such rows in a JSONL file. It reads
files only: it never contacts Docker or GitHub and never infers a cause.

The workflow writes the row as `history-row.json` in `tag-scenario-<id>`. To
build one from downloaded artifacts:

```sh
gh run download <run-id> -n tag-scenario-<id> -D a/scenario
gh run download <run-id> -n runtime-cleanup-<id> -D a/cleanup
gh run download <run-id> -n scenario-images-<id> -D a/images
python3 .github/scripts/scenario_history.py normalize \
  --scenario a/scenario --cleanup a/cleanup --images a/images \
  --repository <owner/repo> --run-id <run-id> --run-attempt <attempt> --job-id <job-id> \
  --output row.json
```

A row holds:

- `execution_key`: repository, workflow, run ID, run attempt, job ID,
  repetition ID, profile ID (the matrix entry ID), checker, case and network
  mode. Each attempt has its own key.
- `source`: the requested pull request head and the commit and tree that the
  job checked out.
- `build`: the compile receipt's build key, binary sha256, a digest of its
  source hashes, and whether the native step ran that binary.
- `runtime`: the resolved release, layout, platform and every role's image
  ID and registry digests, or `not_applicable` for a batch without Ceph.
- `phases`: for each workflow phase, the step outcome (`workflow`), the
  phase's own receipt (`evidence`) and their join (`status`). A failure on
  either side is `failed`. A pass needs a successful step and a passing
  receipt, so a cleanup failure after a native pass stays a failure, and a
  missing receipt or `phases.json` is `missing`, never `passed`. The native
  phase also records its classification and the completion and strict
  checker results.
- `outcome.status`: `failed` if any phase failed, else `cancelled`, else
  `passed` when every phase passed or was not required, else `incomplete`.
  `first_failure` names the first failed phase, its classification and first
  error, and for the native phase the first failed test.
- `tests`: every RUN, PASS, FAIL and SKIP line of `native.log` with its
  duration. A test without a result line stays `RUN`.
- `build`: the compile receipt's binary sha256 and build key, and, for a
  native phase that ran a compiled binary, that binary's own sha256 and build
  key (`native_binary_sha256`, `native_build_key`). `native_binary` says
  whether the two match.
- `comparability`: `complete`, or `incomplete` with the missing checkout,
  build or image identity, or `native_binary_identity_mismatch` when the
  native phase ran another binary than the compile receipt records. The
  phase outcomes stay as they were.
- `evidence`: the sha256 of every file read, and `row_sha256` over the row.

Append rows to a history file, and count it:

```sh
python3 .github/scripts/scenario_history.py ingest --history history.jsonl row.json
python3 .github/scripts/scenario_history.py summary --history history.jsonl
```

`ingest` skips a row identical to one already stored. A different row under
the same execution key is an error that prints both rows' `row_sha256` and
evidence hashes and leaves the file unchanged. A row without repository, run
ID or run attempt, such as one built from a local run, is rejected.

`summary` groups rows by profile ID and release. It reports the rows, the
passed, failed, cancelled and incomplete counts, failures by first failed
phase and classification, and the nearest-rank p50 and p90 of the native
step's `elapsed_seconds` over rows with a native receipt. Cancelled rows are excluded from the failure
denominator and from the durations; incomplete rows stay in the denominator.

### Typical causes by phase

| Failed phase | Look at | Common causes |
| --- | --- | --- |
| Go module preparation | `modules/modules.log`, `modules/modules-report.json` | The module proxy and the module's origin did not answer, or `go.sum` lacks an entry. |
| Compilation | `compile/compile.log`, `compile/compile-report.json` | A test calls a helper defined in a file that only another batch compiles. Run `plan --verify` locally (see [Testing](testing.md#shared-helpers-must-compile-in-every-batch)). A `compile-timeout` with a complete module cache is the build itself. |
| Docker baseline | the step log, `runtime-cleanup-<id>-baseline` | Docker did not become ready within 60 seconds. |
| Published image preparation | `scenario-images-<id>` | A role image for the release is missing from the registry or could not be pulled. |
| Native assertions and completion | `native/native.log`, `native/report.json` | An assertion failed, a Ceph wait hit its deadline, the batch timeout expired, or a test was skipped. For a Go timeout, the `panic: test timed out` block lists the tests still running and for how long. |
| Docker resource cleanup | `runtime-cleanup-<id>/after.json` | A test left a container, network or named volume behind, for example by not registering cleanup when `Run` returned an error. |

Failed integration tests that use the shared cluster helpers print the last
16 KB of every daemon's log into `native.log` before the cluster is removed.
Search for `container ... logs:` near the failing test.

### Reproducing and re-running

Re-run only the failed jobs when the failure looks unrelated to the change:

```sh
gh run rerun <run-id> --failed
```

To reproduce a batch locally with the same tags, budget and pass rules, run
the tag runner with the same category and batch. For identical images, first
resolve CI's role images as described in
[Reproducing CI's role images](testing.md#reproducing-cis-role-images):

```sh
python3 .github/scripts/tag_scenarios.py run \
  --category recovery --batch container_pause --package ./internal/integration \
  --directory artifacts/repro-container-pause
```

Local runs are sequential unless you set `CEPH_TEST_PARALLEL`, so timing can
differ from CI. To isolate one test, use `go test -tags=all -run` as described
in [Testing](testing.md#one-test).
