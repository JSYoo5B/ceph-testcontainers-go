# Continuous integration

This page explains what each GitHub Actions workflow checks, how the runtime
batches are planned and run, and how to investigate a failing batch. How to
run tests locally and how to add them is described in [Testing](testing.md).

## Workflows

| Workflow | File | Triggers | What it checks |
| --- | --- | --- | --- |
| Code checks | `code.yml` | push to `main`, pull request, manual; weekly (Monday 05:17 UTC) for the vulnerability check only | Unit tests, race detector, vet, tag compilation, CI tooling tests, compiled tag coverage, Go vulnerability check |
| Docker checks | `docker.yml` | push to `main`, pull request, manual | The `environment` category: Docker bridge behaviour without Ceph |
| Ceph short | `ceph-short.yml` | push to `main`, pull request, manual | The `short` category on Tentacle 20.2.4 |
| Ceph topology | `ceph-topology.yml` | push to `main`, pull request, manual | The `topology` category on Tentacle 20.2.4 |
| Ceph multicluster | `ceph-multicluster.yml` | push to `main`, pull request, manual | The `multicluster` category on Tentacle 20.2.4 |
| Ceph recovery | `ceph-recovery.yml` | push to `main`, pull request, manual | The `recovery` category on Tentacle 20.2.4 |
| Ceph squid | `ceph-squid.yml` | push to `main`, pull request, manual | The `short` category on Squid 19.2.5 |
| Ceph squid extended | `ceph-squid-extended.yml` | weekly (Tuesday 04:41 UTC), manual | The `topology`, `multicluster` and `recovery` categories on Squid 19.2.5 |
| Native regressions | `native-regressions.yml` | manual only | The optional `native_rgw_translation` batch, with an optional RGW image override |

Every runtime workflow calls the reusable template `tagged-runtime.yml`
(`workflow_call` only; it has no triggers of its own). The workflows are
independent: none waits for another, and each can be re-run on its own. A
failed check name therefore tells you the kind of failure before you open it.

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
`Ceph squid extended / Topology / empty_bootstrap` is a Squid topology batch.
Each runtime workflow also has a `Plan` job.

## Concurrency and cancellation

| Workflows | Concurrency group | Effect |
| --- | --- | --- |
| Code checks, Docker checks, Ceph short/topology/multicluster/recovery, Ceph squid | workflow name, event, then the PR number (pull requests), the ref (pushes) or the run ID (manual runs) | A new commit on a PR cancels that PR's previous run of the same workflow. A new push to `main` cancels the previous `main` push run of the same workflow. Manual runs are never cancelled and cancel nothing. |
| Ceph squid extended | workflow name and ref | A manual run on `main` cancels a running scheduled run on `main`, and the other way round. |
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

Multi-cluster CephFS process-quiescence and process-recovery scenarios, and
RBD receiver readiness, are split into one leaf batch per network mode and
case. Those batches declare `checker=` and `case=` in `//ci:`, and after the
run a dedicated checker (`check_scenario_quiescence.py`,
`check_scenario_recovery.py`, `check_scenario_receivers.py`) verifies the
leaf-specific evidence in the log. Locally, the `all` build still runs the
original parent test with every leaf.

## How a batch runs

For each matrix entry, one job on one `ubuntu-24.04` runner performs these
steps in order:

| Step | Phase in summary | What happens |
| --- | --- | --- |
| Restore the Go module cache | n/a | Restores the module cache that the `Plan` job saved. |
| Prepare Go modules | Go module preparation | `tag_scenarios.py modules` runs `go mod download` within 300 seconds. With a restored cache it finds every module at once; otherwise this is the only step that downloads. |
| Compile selected tagged suite | Compilation | `tag_scenarios.py compile` builds the batch's test binary with `go test -c` within 180 seconds, with `GOPROXY=off`, so it reads only the module cache. Nothing runs. |
| Docker baseline and engine identity | Docker baseline | Records the engine identity and every container, network and named volume labelled `org.testcontainers=true`. |
| Resolve native published Ceph roles | Published image preparation | Only for Ceph batches. Resolves the batch's roles of `official-<release>-{control,osd,rgw,mds}` from `ghcr.io/jsyoo5b/ceph-testcontainers-images` for `linux/amd64` to immutable image IDs and exports them as `CEPH_TEST_IMAGE`, `CEPH_TEST_OSD_IMAGE`, `CEPH_TEST_RGW_IMAGE`, `CEPH_TEST_MDS_IMAGE`, with `SCENARIO_IMAGE_LAYOUT=roles` and `SCENARIO_IMAGE_ROLES`. A batch gets all four roles unless its `//ci:` line declares `image-roles` (see [Image roles](#image-roles)). Pulls only, never builds. |
| Native assertions and completion proof | Native assertions and completion | `tag_scenarios.py run` (or `optional`) runs the batch and applies the pass rules below. |
| Verify owned Docker resource cleanup | Docker resource cleanup | Runs even after a failure, if the baseline succeeded. Waits up to 30 seconds for Ryuk, then fails if new labelled resources remain. It never deletes anything. |
| Show actual failure phases | n/a | Writes the phase table to the job summary and `phases.json`, and an annotation for each failed or cancelled phase. |
| Save compile, native and phase results | n/a | Uploads `tag-scenario-<id>`. |
| Identify completed failing tests | n/a | On failure, adds one annotation per failed test or subtest name found in `native.log`. |

The job environment sets `CGO_ENABLED=0`, `TESTCONTAINERS_HOST_OVERRIDE=127.0.0.1`
and `CEPH_TEST_PARALLEL=2`. Go is the latest `1.26.x`. Tentacle workflows use
role images for release 20.2.4; the Squid workflows pass release 19.2.5.

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

### How a batch passes

The native phase succeeds only if all of these hold in `native.log`:

- `go test` exits with status 0;
- every top-level test compiled into the profile has exactly one `=== RUN` and
  one `--- PASS` line, and no other top-level test ran;
- every started subtest has exactly one `--- PASS`;
- there is no `--- FAIL`, `FAIL` or `panic:` line;
- there is no `--- SKIP` line at any level;
- exactly one `PASS` line and one `ok <package>` line follow all test results;
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

## Squid workflows

`Ceph squid` runs the same `short` batches as `Ceph short`, on Squid 19.2.5
role images, for every push to `main` and every pull request. A test added to
the `short` category is therefore verified on both releases.

`Ceph squid extended` runs the `topology`, `multicluster` and `recovery`
categories on Squid. Running them on every push would add a large amount of
CI time, so they run weekly and on demand. Dispatch a run after changing
release-dependent behaviour:

```sh
gh workflow run "Ceph squid extended" --ref main
gh run list --workflow "Ceph squid extended" --limit 3
gh run watch <run-id>
```

`--ref` may name any branch on GitHub that contains the workflow. A manual run
cancels a scheduled run in progress on the same ref.

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
| `tag-scenario-<id>` | `modules/modules.log` and `modules/modules-report.json` (download output and outcome); `compile/compile.log` and `compile/compile-report.json` (compiler output and outcome); `native/native.log` (complete `go test -v` output); `native/report.json` (verdict); `phases.json` (phase outcomes) |
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
  - `process-or-build-failure`: `go test` exited non-zero without any failure
    line, for example because the process was killed;
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
expired. A timed-out command is killed with its whole process group, its
`exit_code` is `null`, and its log keeps the output written until then.

`phases.json` holds the same phase outcomes as the job summary, with
`failed_phases` and `cancelled_phases` lists.

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
