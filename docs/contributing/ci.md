# Continuous integration

This page explains what each GitHub Actions workflow checks, how the runtime
batches are planned and run, and how to investigate a failing batch. How to
run tests locally and how to add them is described in [Testing](testing.md).

## Workflows

| Workflow | File | Triggers | What it checks |
| --- | --- | --- | --- |
| Code checks | `code.yml` | push to `main`, pull request, manual; weekly (Monday 05:17 UTC) for the vulnerability check only | Unit tests, race detector, vet, tag compilation, CI tooling tests, compiled tag coverage, Go vulnerability check; skipped for a [documentation-only change](#documentation-only-changes) |
| Docker checks | `docker.yml` | push to `main`, pull request, manual; not for documentation-only changes | The `environment` category: Docker bridge behaviour without Ceph |
| Ceph short | `ceph-short.yml` | push to `main`, pull request, manual; not for documentation-only changes | The `short` category on Tentacle 20.2.4 |
| Ceph topology | `ceph-topology.yml` | pull request, manual; push to `main` in a release phase; not for documentation-only changes | The `topology` category on Tentacle 20.2.4: on a pull request only the changed batches (see [Pull request scope](#pull-request-scope)) |
| Ceph multicluster | `ceph-multicluster.yml` | pull request, manual; push to `main` in a release phase; not for documentation-only changes | The `multicluster` category on Tentacle 20.2.4: on a pull request only the changed batches (see [Pull request scope](#pull-request-scope)) |
| Ceph recovery | `ceph-recovery.yml` | pull request, manual; push to `main` in a release phase; not for documentation-only changes | The `recovery` category on Tentacle 20.2.4: on a pull request only the changed batches (see [Pull request scope](#pull-request-scope)) |
| Ceph periodic | `ceph-periodic.yml` | every 3 hours, gated (see [Periodic runs](#periodic-runs)); daily at 04:23 UTC for the [representative campaign](#representative-campaign); manual | Every runtime category on every [supported release](#supported-releases) (Tentacle 20.2.4 and Squid 19.2.5), on `main`; the representative batches repeated on a pinned commit and pinned images |
| Ceph release matrix | `ceph-release-matrix.yml` | push to `main` and pull request while `CI_RELEASE_PHASE` is `true`; manual | Every runtime category on each supported release other than the current one (Squid 19.2.5) |
| Native regressions | `native-regressions.yml` | manual only | The optional `native_rgw_translation` batch by default, or the explicitly requested, deliberately failing `failure_archive` proof |
| CI command | `ci-command.yml` | a pull request comment `/ci run ...` (see [Running chosen batches from a comment](#running-chosen-batches-from-a-comment)) | The named runtime batches, of any category or optional, on the pull request's head commit and the chosen releases |

The category runtime workflows call the reusable template `tagged-runtime.yml`
(`workflow_call` only; it has no triggers of its own). The workflows are
independent: none waits for another, and each can be re-run on its own. A
failed check name therefore tells you the kind of failure before you open it.

### Pull request scope

A pull request runs, on Tentacle 20.2.4 role images and in each batch's
default [network mode](#network-modes):
- Code checks, for every change;
- Docker checks and every `short` batch, which includes the netem helper's
  `network_delay`, `network_bandwidth`, `network_packet_faults`,
  `osd_peer_path` and `cephfs_mds_path_fault` batches. With
  the `ci:selected-batches` label, Ceph short runs only the batches that the
  declaration selection selects (see
  [Opting in to declaration selection](#opting-in-to-declaration-selection));
- of the `topology`, `multicluster` and `recovery` categories, only the
  batches whose own test files (the files that declare the batch's tests) the
  pull request changes. A change to a shared helper or a public package
  selects none of them; [Ceph periodic](#periodic-runs) runs them on `main`.

Two things run every batch of those categories on the pull request instead:
the `ci:full` label (adding it starts the run), and a release phase. A change
that touches nothing but `docs/`, the top-level Markdown files (`README.md`,
`CHANGELOG.md`), `LICENSE` or the go-ceph probe under
`internal/integration/goceph/` runs only Code checks: no runtime batch reads
those paths. Code checks skips most of its jobs for a smaller set of
documentation (see [Documentation-only changes](#documentation-only-changes)).
A Markdown file anywhere else, for example under a package's
`testdata/`, can be a test's embedded or run-time input, so changing it runs
the batches as usual. Each `Plan` job's `tag-plan.json` artifact lists the changed
files and the selected batches.

For this own-test-file selection, the caller supplies both the event's base
commit (`changed_since`) and requested PR head (`changed_head`). The planner
records them alongside the actual checkout commit/parents and the exact
comparison range in `changed_selection`. A two-parent test merge is accepted
only when its second parent is the requested head and the supplied base is an
ancestor of its first parent. Selection then compares that actual base parent
to the tested merge result. Independently advanced `main` files are not
attributed to the PR merely because the event supplied an older base.

A checkout of the requested PR head itself uses its unique merge base with the
supplied base. Missing commits, ambiguous merge bases, unproven parent identity
or unrelated base history fail planning instead of producing an empty selection.
This comparison uses proven commit identities, not an assumed `HEAD^`.

To inspect the same comparison manually, run `tag_scenarios.py plan --category
recovery --changed-since BASE_SHA --changed-head PR_HEAD_SHA --output plan.json`.
Omitting `--changed-head` preserves the legacy manual merge-base comparison and
records `legacy-merge-base`; it does not infer a PR identity. Explicit
`--changed-files` remains a separate manual input. These inputs do not change
`ci:full`, release-phase, Short, periodic or compiled-coverage policy. The
selection provenance establishes which execution set was requested, not a
measured cost saving.

Pushes to `main` run Code checks, Docker checks and Ceph short. Every other
batch, and every batch on Squid 19.2.5, runs in Ceph periodic within 3 hours
of a new commit on `main`.

To run any other batch on a pull request, such as one batch of another
category or a batch on Squid, comment `/ci run <batch>` (see
[Running chosen batches from a comment](#running-chosen-batches-from-a-comment)).
The command adds a run; it never changes the checks above.

Pushes to branches other than `main` trigger nothing. Pull requests are
checked through the `pull_request` event (`opened`, `synchronize`,
`reopened`), so a commit on a PR branch is checked once, not once as a push
and again as a pull request. The label events `labeled` (Ceph short and the
scoped categories) and `unlabeled` (Ceph short) start runs only for the
`ci:full` and `ci:selected-batches` labels.

Both workflow files set `GOPROXY=https://proxy.golang.org|direct`. With `|`,
the go command fetches a module from its origin after any failed proxy
request, such as a reset HTTP/2 stream; with the default `,` it gives up on
everything but a 404 or 410, which once failed a whole batch at compilation.
`go.sum` verifies a module from either source.

The Go jobs share one module cache, keyed by `go.sum`. The `Plan` job of each
runtime workflow restores it and, when it is missing, downloads every module
with `tag_scenarios.py modules` and saves it. Runtime jobs start only after
`Plan`, and Code checks jobs only restore it, including both Go versions of
the vulnerability check. `setup-go`'s own cache is off:
it keeps whatever the first job to finish under a key had, which once was a
`Plan` job with almost no modules, so every later compile downloaded its
modules again.

The runtime jobs also share one Go build cache, keyed by the exact Go version
that `setup-go` installed and `go.sum`. When `Plan` finds none, it compiles
one runtime batch of each package with `tag_scenarios.py buildcache` and
saves the cache; each runtime job restores it before its compile step. Every
batch of a package builds the same dependencies, so the batch compile then
builds only the package's own tests: on four CPUs, `./internal/integration`
compiled in 18 seconds with an empty cache and in 3.4 seconds with the cache
of another batch (about 55 MB compressed). The cache only speeds the build
up. Go reuses an entry only when its content hash matches, a stale entry is
merely unused, a failed `buildcache` leaves the cache unsaved without failing
`Plan`, and each job still compiles, records and verifies its own binary.

### Documentation-only changes

Code checks runs on every pull request, because its six checks are required
status checks of the `main` protection ruleset. A workflow-level
`paths-ignore` would never start the workflow for a documentation-only
change and leave those checks pending, which blocks merging. The workflow
therefore always starts, and its `Detect documentation-only changes` job runs
`.github/scripts/docs_only.py`. When that reports `docs_only=true`, three
checks skip themselves and report success; GitHub documents that a job
skipped by a condition reports success. The Vulnerability check matrix jobs
and `Vet, compile and CI tooling` skip their steps instead of the whole job:
the matrix so that each keeps its expanded check name, and the vet job so
that the fragment validator below still runs. The job names are unchanged.

A pull request is documentation-only when every changed file is on this
allowlist, and the allowlist contains only files that no check reads:
- `README.md`, `CHANGELOG.md`, any other top-level `*.md` file and `LICENSE`;
- Markdown files directly inside `docs/changes/` (release-note fragments),
  `docs/contributing/`, `docs/design/`, `docs/guides/` and `docs/research/`,
  except the files below.

These documentation files are read by a check and always run the full Code
checks, as does every other path (including any non-Markdown file under
`docs/` or `docs/changes/`, a new top-level `docs/` page, a nested path such
as `docs/changes/sub/x.md` and a top-level `changes/` directory). A changed
path whose old or new Git mode is not an ordinary file (a symbolic link, an
executable or a submodule) also runs them, because the path name alone cannot
show that `docs/changes/x.md` is a link to a code file:
- The API reference page of every public package (`docs/ceph.md`,
  `docs/fault.md`, `docs/override.md`, `docs/rbd.md`, `docs/rbd-mirror.md`, `docs/cephfs.md`,
  `docs/rgw.md`, `docs/rgw-multisite.md`, `docs/capture.md`, and the page a new
  package's receipt names): the API inventory test requires every exported name
  on its package page;
- `docs/design/api-naming*`: the API naming audit and review notes, and the
  package-scoped `docs/design/api-naming/<package>/declarations.json`,
  `members.json` and `review.json` catalogues and receipts, which the
  `internal/apiinventory` tests compare with the code. The catalogue JSON files
  are nested non-Markdown files, so they are never on the allowlist;
- `docs/guides/messenger-client-fixtures.md`: cross-checked against the
  messenger coverage manifest by a test in `internal/integration`;
- `docs/guides/bug-compatibility-tags.md` and `.json`: the tag registry and
  its catalogue, read by `tag_scenarios.py` and its tests.

`test_docs_only.py` scans the Go sources and `tag_scenarios.py` for documentation
paths they name and fails when one of them is on the allowlist, so a new
reader of a documentation file has to remove that file from it. Add the
file to `READ_DOC_FILES` or `READ_DOC_PREFIXES` in `docs_only.py`.

A head that changes only fragments is documentation-only, but it is still
validated. `Vet, compile and CI tooling` runs on every head instead of
skipping the whole job: its checkout and its last four steps run regardless
of `docs_only`. They are `Validate release-note fragments`
(`.github/scripts/release_fragments.py`, see
[Release-note fragments](release-fragments.md)) and `Check the released
changelog range` (`.github/scripts/release_assemble.py check`, see
[Release assembly](release-fragments.md#release-assembly)) and `Check the image
role vocabulary` (`.github/scripts/check_image_vocabulary.py`, which fails when a
tracked file reintroduces the retired single-broad-role image vocabulary outside
its list of historical records) and `Check the retired override names`
(`.github/scripts/check_retired_override_names.py`, which fails when a tracked
file names a removed `ceph.Container` override method, `ceph` handle type or
observation method receiver (these are `ceph.Observer` methods now) outside its list of historical records). The changelog step needs no network; the shallow checkout has no release tags, so it prints
`tag comparison: SKIPPED` and checks only the identifiers recorded in
`CHANGELOG.md` against `docs/changes`. Vet, lint, tag compilation, the Python tooling tests,
actionlint and the go-ceph harness test skip themselves on a
documentation-only head. The job name is unchanged and no required check is
added. The job's checkout fetches two commits (`fetch-depth: 2`), and a pull
request head also runs `Check move records against the merge base`
(`go run ./internal/tools/apinaming moves`, see
[Testing](testing.md#package-layout-and-the-export-inventory)): the checkout of a pull request is its
merge commit, whose first parent is the base, so the step passes `HEAD^1` and
compares the pull request's net change. A checkout that is not a merge commit
passes no base, and the step prints `SKIPPED` and a warning annotation instead
of `OK`; the record checks still run, and a skip is never a pass. The step does
not run on a documentation-only head, which cannot touch a catalogue or a move
record, and it does not run on pushes or scheduled runs. The step runs under
`shell: bash` (pipefail) and exits with the checker's status; a workflow control
in `.github/scripts/test_moves_step.py` runs the step as written. On such a head a green `Vet, compile and CI tooling` means that the
validator and the identifier check passed (not the tag comparison); vet and tag compilation did not run, and the skip is by
policy, not a pass of those steps. Unit tests, Race detector and Compiled tag
coverage and helpers remain skipped jobs, and the Vulnerability check jobs
execute with every step skipped.

The detection fails open. Every push to `main`, the weekly schedule and
manual runs always run every check. A pull request from a fork, a missing
base commit, a base that cannot be fetched, a failed `git diff`, an empty
change list, an unusable argument, a failure of the detection job itself and
any exception in the script run every check too. Only an explicit
`docs_only=true` skips, and the dependent jobs use `!cancelled()` so that a
failed detection job runs them instead of skipping them (a job whose
dependency failed is otherwise skipped, and a skipped job counts as a
pass). The script compares the pull request's merge commit with the event's
base commit, lists renames as a deletion and an addition, and so also sees a
code file that is moved into `docs/`.

### Role image checks

Runtime batches prepare only their source-declared roles from the checked-in
immutable digest map. There is no separate automatic role consumer campaign.
Normal Code, Docker and category CI retain their existing scopes.

Request additional role routing through the existing review command:

```text
/ci run role_routing role_mirror_refresh
/ci run role_routing role_mirror_refresh on all
```

These optional profiles cover constrained ordinary RBD, RGW without a generic
client, and stopped-link mirror monitor refresh. Other cache, scheduling,
shared/native and aggregate compatibility checks remain in their ordinary
source-owned batches; reviewers can select those batches by the same command.

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
| Ceph short | as above, plus the run ID when a label other than `ci:selected-batches` is added or removed | Adding or removing `ci:selected-batches` replaces the run in progress. Any other label starts a run that skips its job and cancels nothing. |
| CI command | per caller job: the pull request number, head commit, selected profiles and release; no group for the run itself | A repeated command for the same head commit, batches and release waits for the run in progress and never cancels it. Each command still has its own commit status. GitHub keeps one waiting caller per group, so a third identical command replaces the waiting one, which ends as cancelled. A different selection, release or head commit runs independently. |
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
| Vet, compile and CI tooling | 25 min | `make vet`, golangci-lint (`make lint`), `make tag-compile`, the Python tests in `.github/scripts` (`test_*.py`), actionlint and the go-ceph harness test; the release-note fragment validator on every head; on pull requests, `apinaming moves` against the merge base |
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
and uploads every scan as `vulncheck-go-<version>`. Both versions restore the
same module cache as the runtime jobs, rather than saving the dependencies
again per toolchain. They also restore the runtime build-cache key for their
exact Go version. A successful default-branch scan saves a missing build
cache, including for the newer toolchain; pull requests only restore it.
Cache-save failures do not change the scan's result. The build cache
contains no module sources. Scanner or nested-probe modules
absent from the shared module cache are downloaded on demand. Cache hits
never skip a scan, cache its verdict or retain the vulnerability database:
every run invokes govulncheck against the current advisories.

Every `setup-go` step of the workflows sets `check-latest: true`, so a
runner's cached older patch is not used: Go 1.26.8, for example, has eleven standard-library vulnerabilities
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

1. **Catalog.** `internal/tools/tagcatalog` parses every `*_test.go` file in the
   module (skipping hidden directories, `artifacts`, `vendor` and
   `testdata`) without importing or running it. For each file it records the
   `//go:build` expression, its tags, the `Test...` functions and the `//ci:`
   metadata.
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

Bug compatibility tags never take part in steps 2 and 3: see
[Bug compatibility selection](#bug-compatibility-selection).

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

### Bug compatibility selection

A `ceph_bug_<id>` tag selects the compatibility path of one Ceph bug (see the
[policy](bug-compatibility.md)); it never selects tests. The planner therefore
never collects such a tag as a capability, and `all` or `native_regression`
never enable one. Only `--bug-compatibility TAGS`, accepted by `plan`,
`verify`, `compile`, `run`, `code`, `optional` and `buildcache`, selects them:

```sh
python3 .github/scripts/tag_scenarios.py plan --bug-compatibility ceph_bug_<id>
```

The selection is refused before the catalog is read when a tag is not in
[`bug-compatibility-tags.json`](../guides/bug-compatibility-tags.json), is
retired, appears twice, or conflicts with another selected tag. The planner
also refuses a test file whose constraint names an unregistered or retired
bug tag, and a registered tag whose source files do not name it in
`//go:build`. With no selection every ordinary path compiles.

A test file guarded by a bug tag is one path of that bug. The file that the
selection excludes stays in its batch's `sources`; the other path owns its
tests. For every profile and for each package's `all` build, the planner
toggles each bug tag that a test file of the package names and requires the
same test names: both paths must keep the same tests in the same batch. A bug
tag that adds, removes, renames or moves a test refuses the plan. A toggle
that would make two conflicting tags active together is skipped, because the
selection refuses that combination: the comparison covers only selections
that can be planned.

The selection applies to the whole test binary, so every profile of the plan
gets the selected tags. They are part of the profile's `tags`, and so of its
build key, its compile receipt and its history configuration identity, and
the plan and each profile list them in `bug_compatibility`. The profile ID
does not change. `run --binary` refuses a binary compiled under another
selection.

#### Selecting paths in a workflow run

`tagged-runtime.yml` takes the selection as one `workflow_call` input,
`bug_compatibility`, empty by default. Its Plan job passes the input to
`plan` and `buildcache` as `--bug-compatibility`, so an invalid selection
fails the Plan job and no runtime job starts. Each runtime job passes the
same input to `compile`, `run` or `optional`, together with
`--profile-tags`, its matrix entry's `tags`. The job recomputes its profile
from source and its own selection; when those tags differ from the matrix
entry's, the command fails before it builds or runs anything. The compile
receipt check then refuses a binary of another selection, as above.

The input reaches each command only as an argument, through a quoted step
variable. `tag_scenarios.py` reads no selection from the environment, and the
Go environment it builds with still replaces an inherited `GOFLAGS`, so
neither `-tags` nor `-overlay` from the runner can select a path. The history
step passes the input to `scenario_history.py normalize`, which writes no row
when the receipts were built with another selection (see
[Scenario history](#scenario-history)).

No caller passes the input. Pull requests, `ci:full`, the comment command
and the periodic runs therefore keep the empty selection, which is also the
only valid one while the catalogue registers no tag.

### Advisory declaration selection

File-level selection cannot narrow `internal/integration`: its
`*_helpers_test.go` files compile into every batch. `internal/tools/batchselect`
selects at the level of declarations instead. It is advisory. By default no
workflow narrows by it. A pull request can opt in for Ceph short (see
[Opting in to declaration selection](#opting-in-to-declaration-selection)).

```sh
python3 .github/scripts/tag_scenarios.py plan --category topology \
  --select-declarations origin/main --output artifacts/tag-plan.json
```

The plan then has a `declaration_selection` object. `selected` and
`excluded` map each profile ID to a reason, such as
`func TestX reaches func helperY (file)` or
`none of 25 roots reaches a changed declaration`. `fallback` lists why every
batch was selected, and `changed_declarations` lists what the diff touched.
The matrix and the `include` list are unchanged.

With `--apply-declarations`, the plan keeps only the selected profiles in
`include` and in the matrix, and sets `selected_scope` to `declarations`.
`--summary FILE` appends the selected and excluded batches with their reasons
as a Markdown table, for example to `$GITHUB_STEP_SUMMARY`. To see locally
what a pull request would run:

```sh
python3 .github/scripts/tag_scenarios.py plan --category short \
  --select-declarations origin/main --apply-declarations \
  --summary /dev/stdout --output artifacts/tag-plan.json
```

The tool works as follows:
1. It diffs the checkout against its merge base with the given base, with
   `git diff -U0 --no-renames`.
2. It type-checks the `all` build of the package with `go/types`, for
   `linux/amd64` and with `CGO_ENABLED=0`, from the export data of
   `go list -export`. A batch whose files are not all in that build gets its
   own type check with its own tags.
3. It builds a graph over the package's top-level declarations: functions,
   methods, and each type, var and const spec. An identifier that `go/types`
   resolves to another declaration of the package is an edge, so
   `cluster.Ceph(...)` reaches `(*ceph.Container).Ceph` and never a fake's
   method of the same name. Two further kinds of edge are added:
   - a call through an interface method reaches that method of every type in
     the package that implements the interface (class hierarchy analysis);
   - a type reaches all of its methods, because code in other packages, such
     as `fmt` or testcontainers, can call any of them.
4. It maps each hunk to the declarations whose lines, doc comment included,
   it touches. A deleted line is mapped through the base version to the
   declaration of the same name in the head.
5. It selects a batch when one of its roots reaches a changed declaration.
   The roots are the batch's `Test`, `Fuzz` and `Example` functions, and
   every `init` function and package-level var initializer that the batch
   compiles.

An added Go file of the package counts as a change to each of its
declarations. Its build constraint and package clause only decide which
batches compile it. A new test in it therefore selects its own batch, a new
helper selects the batches that call it, and a new method selects the
batches that reach its type, because the method can change which interfaces
the type satisfies.

An import counts by the package it links. When a modified file adds or
removes an import, or an added file imports a package, the change is fine
grained as long as another file of the same build imports that package too:
the package is in the test binary either way, and only the declarations
that use it change. Otherwise the package's `init` functions start or stop
running in the binary, so every batch is selected.

Every batch is selected, with the reason, when the change touches any of:
- a line outside every declaration: the file header (build constraint,
  `//ci:` line, package clause) of a modified file, a directive comment
  (`//go:`), or any other code. Blank lines, plain comments between
  declarations and import lines of packages that the build links anyway are
  ignored;
- an import of a package that no other file of the build imports;
- a deleted or untracked file in the package;
- a non-Go file in the package directory or below it, such as an embedded
  file or the go-ceph probe in `internal/integration/goceph/`;
- `.github/`, whose scripts, workflows and actions are read at run time;
- `go.mod` or `go.sum`;
- any other path outside `internal/integration`. The public packages are not
  analysed yet;
- a removed `init` function or package-level var initializer;
- a diff, base file or package that cannot be parsed or type-checked.

Every file under the analysed package's directory is classified first, so
a Markdown file that a test embeds or reads there selects every batch. Other
Markdown files, `docs/` and `LICENSE` are ignored. A profile of another
package, such as the `environment` batch, is always selected. If the tool
fails, every profile is recorded as selected, with the error.

#### Validating the selection against CI history

`replay_declaration_selection.py` replays the selection over merged pull
requests. For each of the last pull requests on `origin/main`, merged or
squashed, it takes the first parent as the base and the pull request head
(the second parent, or the squash commit) as the head. It exports the head's
tree, plans it with the head's own planner, and prints a table of selected
batches per pull request: all runtime profiles, and the `short` ones on
their own, since Ceph short runs every batch on every pull request.

```sh
python3 .github/scripts/replay_declaration_selection.py --merges 15
```

With `--github`, it also reads each pull request's CI history through `gh`:
every attempt of every runtime run on the pull request's branch while it was
open, and the push runs of its merge commit. It then checks each failed
batch job against the selection of the diff that the run tested. A pull
request run is checked against the diff up to the head it tested, and a push
run against the pull request's diff. Cancelled jobs are ignored. A failed
job that the selection excludes is reported as:
- `explained`, when `batch_selection_explained.json` records its job ID;
- otherwise an unresolved miss, and the script exits with status 1.

A later attempt of the same run that passed the job is shown next to the
miss as `(passed on rerun)`, but it does not clear the miss. A regression
that the diff introduced and that fails only sometimes passes a retry of the
same commit too, so the retry proves only that the failure is intermittent.

Each entry of `batch_selection_explained.json` records the pull request,
the batch (`category/batch`, which must match the failed job), a short
quote of the failure's signature, the evidence that the diff did not cause
it, and a reference. The evidence is one of:
- an infrastructure failure before the tests ran, such as an action that
  could not be downloaded or an artifact upload error;
- a known intermittent failure whose code path the diff does not touch,
  with the issue or pull request that investigated or fixed it.

Add an entry only after reading the job's log. A failure that cannot be
explained this way stays a miss.

```sh
python3 .github/scripts/replay_declaration_selection.py --merges 60 \
  --github JSYoo5B/ceph-testcontainers-go --cache gh-cache.json \
  --output validation.json
```

`--cache` keeps every API response in a JSON file, so a second run reads
GitHub only for new pull requests. The replay builds every head's plan and
type-checks the package once per head, which takes a few minutes for 60
pull requests. To run it on a host without `gh`, fill the cache on a machine
that has it, copy the cache and a clone with the pull request heads
(`git fetch origin '+refs/pull/*/head:refs/remotes/origin/pr/*'`), and pass
`--offline` and the same `--ref`.

Changes outside `internal/integration`, such as the public packages and
`.github/`, still select every batch.

#### Opting in to declaration selection

Ceph short is the only workflow that can narrow by declarations, and only on
request. It is not the default gate. Add the `ci:selected-batches` label to a
pull request to opt in. Without the label, Ceph short runs every batch.

- With the label, the caller passes the pull request's base commit as the
  `select_declarations` input of `tagged-runtime.yml`. `Plan` then fetches
  the full history and runs `tag_scenarios.py plan --select-declarations
  BASE --apply-declarations --summary "$GITHUB_STEP_SUMMARY"`, so the
  matrix holds only the selected batches. The job summary of `Plan` lists
  every batch with `yes` or `no` and its reason, and says that the run is
  narrowed by opt-in.
- Adding the label starts a selected run that replaces the run in progress.
  Removing it starts a full run in the same way. A later commit runs selected
  as long as the label stays.
- Pushes to `main`, manual runs and pull requests without the label run every
  batch. No other workflow passes `select_declarations`. Docker checks, the
  `topology`, `multicluster` and `recovery` pull request scope, Ceph periodic
  and the release matrix are unchanged.
- The `main` push after a merge still runs every Ceph short batch, so a
  batch that the selection missed fails there. Record such a miss with its
  plan, failed job and selection evidence in a new tracking issue.
- Use the label for changes inside `internal/integration`. A change to
  `.github/`, `go.mod`, the public packages or any other fallback path
  selects every batch anyway.

Measured on 2026-10-18 with two measurement pull requests, each adding one
comment line. The selected run and the full run are of the same commit: the
label was removed after the selected run, which started a full Ceph short
run. Execution totals include Code checks (6 jobs), Docker checks (2 jobs) and
the `Plan` jobs of the three scoped categories (1 job each, no batch
selected). Job minutes are the sum of each job's start-to-completion time.
Wall time runs from the run's creation to its last job's completion. For an
execution it is the longest of its workflows.

| Change | Ceph short | Ceph short jobs | Ceph short job minutes | Ceph short wall time | Execution jobs | Execution job minutes | Execution wall time |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| one batch (`address_family`) | selected | 2 | 2.7 | 4.9 min | 13 | 20.5 | 11.2 min |
| one batch (`address_family`) | full | 49 | 184.7 | 24.5 min | 60 | 202.5 | 24.5 min |
| shared helper (`verifyRBDBytes`, 5 batches) | selected | 6 | 20.9 | 10.1 min | 17 | 35.3 | 10.1 min |
| shared helper (`verifyRBDBytes`, 5 batches) | full | 49 | 185.7 | 17.7 min | 60 | 200.1 | 17.7 min |

Earlier measurements without selection were 50 to 51 jobs, 209 to
227 job minutes and 17 to 27 minutes of wall time, the same for a one-batch
and a public-API change. In these measurements the selection cut an
execution to 13 jobs and 21 job minutes for a one-batch change (about 90%
fewer job minutes), and to 17 jobs and 35 job minutes for a shared helper
that five batches reach (about 82% fewer). The wall time became that of Code
checks (7 to 11 minutes) or of the longest selected batch. The `Plan` job
took 36 seconds with the analysis included. The two full runs started
together, and from their eighth minute a third pull request's full Ceph
short run competed with them for runners. This contention may explain why
the one-batch full run took longer than the shared-helper full run. Each figure is a single
run.

### Running chosen batches from a comment

A collaborator can run chosen runtime batches on a pull request by
commenting a command line:

```text
/ci run <batch|profile>... [on <release|codename|all>...]
```

For example, `/ci run rbd_mirror_bandwidth network_delay on squid` runs the
two batches on Squid 19.2.5 role images. The command adds a run beside the
pull request's usual checks, which stay as they are, and so do pushes to
`main` and the `ci:full` and `ci:selected-batches` labels.

- **Names.** Each name is a batch (`network_delay`), a category and batch
  (`short/network_delay`), a profile name
  (`short/network_delay/internal-integration`) or a profile ID
  (`short-network_delay-<hash>`), separated by blanks or commas. Any runtime
  category and the optional batches are allowed. Code batches run in Code
  checks and are refused here. A name given twice runs once, and the reply
  says so.
- **Releases.** After `on`, give a release (`20.2.4`), its codename
  (`tentacle`, `squid`) or `all` for every
  [supported release](#supported-releases). Without `on`, the batches run on
  the current release, Tentacle 20.2.4. Each batch runs in its default
  [network mode](#network-modes).
- **Validation.** Every name is checked against the planner of the head
  commit (`tag_scenarios.py vocabulary`). One unknown name refuses the whole
  command and the reply lists every unknown name, so a typo never runs a
  partial selection. A head that predates this command has no vocabulary,
  so rebase it onto `main` first.
- **Format.** The command must start its line: text before `/ci` on the
  same line, a quote (`> /ci`) or a fenced code block makes the line an
  ordinary comment. Other lines of the comment may hold any text. A comment
  may hold only one command line. Editing a comment does not run its command.
- **Who and which pull requests.** Only comments whose author association
  is `OWNER`, `MEMBER` or `COLLABORATOR` run. Any other author gets a refusal
  reply and nothing runs. Pull requests from forks are refused as well, and
  so are closed ones. Comments on issues and by bots are ignored.

The command records the pull request's head commit when the workflow reads
the pull request, a few seconds after the comment, and every job runs that
commit even if the branch moves on. The reply names it.

The `CI command` workflow (`ci-command.yml`) runs on `issue_comment`, so
GitHub runs the workflow file and its scripts as they are on `main`. Its
jobs and their token permissions are:

| Job | Permissions | What it does |
| --- | --- | --- |
| `Command` | `contents: read`, `pull-requests: read` | Checks out `main`, reads the pull request and runs `ci_command.py authorize`: the author association, the fork and state checks, and the command syntax. No code of the pull request runs here. |
| `Validate` | `contents: read` | Checks out `main` and the recorded head commit side by side, without persisted credentials. The head's planner lists its batches and releases, and `main`'s `ci_command.py select` resolves the names. |
| `Acknowledge` | `statuses: write` | Sets a pending commit status on the head commit. |
| `Scenarios <release>` | `contents: read` | Calls `tagged-runtime.yml` with category `selected`, the resolved profile IDs and the recorded commit, once per release. |
| `Report` | `contents: read`, `pull-requests: write`, `statuses: write` | Sets the final commit status and replies on the pull request with the result and the run link. It also posts every refusal. |

Runs of `issue_comment` belong to `main`'s commit, so their check runs do not
appear on the pull request. The result appears there as the commit status
`CI command <comment-id>: <names> on <releases>` on the head commit, linked to
the run: a passed run is `success`, a failed one `failure`, and a cancelled one
`error`. For example, the comment `/ci run environment/sdk_network on all`
with ID `4000000001` sets
`CI command 4000000001: environment/sdk_network on 20.2.4 19.2.5`. The comment
ID is the number in the comment's link (`#issuecomment-<comment-id>`). A
selection too long for GitHub's 100-character context limit is cut and ends
with a 12-character digest of the selection; the comment ID is never cut.
`Acknowledge` sets the context to `pending` and `Report` finalizes the same
context, so each command has its own status. Posting the same command twice
gives two statuses: the second one stays `pending` while its runtime jobs wait
for the first command's, and never shows the first command's result. The
duplicate still waits rather than cancels, because the concurrency group names
the selection and not the comment. A refused command sets no status. Every new comment starts a `CI command`
run. Without `/ci` in it, every job is skipped. A comment that mentions
`/ci` without a command line ends after `Command` and gets no reply.

A cancelled run is reported as cancelled and is never re-run
automatically. To run the batches again, comment the command again or
re-run the workflow run. See
[Concurrency and cancellation](#concurrency-and-cancellation) for repeated
commands.

To check a command before posting it, run the same steps locally on the
event payload and pull request JSON:

```sh
python3 .github/scripts/tag_scenarios.py vocabulary --output artifacts/vocabulary.json
python3 .github/scripts/ci_command.py authorize --event event.json --pull pull.json
python3 .github/scripts/ci_command.py select \
  --request '{"names": ["network_delay"], "selectors": ["squid"], "duplicates": [], "sha": "<head>", "pull": 1, "comment_id": 1}' \
  --vocabulary artifacts/vocabulary.json
```

## How a batch runs

For each matrix entry, one job on one `ubuntu-24.04` runner performs these
steps in order:

| Step | Phase in summary | What happens |
| --- | --- | --- |
| Restore the Go module cache | n/a | Restores the module cache that the `Plan` job saved. |
| Restore the Go build cache | n/a | Restores the build cache that a `Plan` job saved for this Go version and `go.sum`. A miss only makes the compile build every dependency. |
| Prepare Go modules | Go module preparation | `tag_scenarios.py modules` runs `go mod download` within 300 seconds. With a restored cache it finds every module at once; otherwise this is the only step that downloads. |
| Compile selected tagged suite | Compilation | `tag_scenarios.py compile` builds the batch's test binary with `go test -c` within 180 seconds, with `GOPROXY=off`, so it reads only the module cache. It writes the binary as `compile/<package>.test` (for example `internal-integration.test`) and records in `compile-report.json` its sha256, its build key (package, tags, the fixed Go environment, Go version, `GOOS` and `GOARCH`), the sha256 of every source file that the build reads, and the tests that the binary lists. No test runs. It first refuses a recomputed profile whose tags differ from the matrix entry's (see [Selecting paths in a workflow run](#selecting-paths-in-a-workflow-run)). |
| Docker baseline and engine identity | Docker baseline | Records the engine identity and every container, network and named volume labelled `org.testcontainers=true`. |
| Resolve native published Ceph roles | Published image preparation | Only for Ceph batches. Pulls the declared immutable role indexes from `ghcr.io/jsyoo5b/ceph-testcontainers-images` for native `linux/amd64`, freezes local image IDs and exports the selected `CEPH_TEST_ESSENTIAL_IMAGE`, `CEPH_TEST_CLIENT_IMAGE`, `CEPH_TEST_CEPHFS_IMAGE`, `CEPH_TEST_RBD_AUX_IMAGE`, `CEPH_TEST_RGW_IMAGE`. Source-owned `image-roles` selects the required subset. Pulls only, never builds. |
| Native assertions and completion proof | Native assertions and completion | `tag_scenarios.py run` (or `optional`) with `--binary artifacts/scenario/compile` runs the binary of the compile step instead of building again (see [Reusing the compiled binary](#reusing-the-compiled-binary)), and applies the pass rules below. |
| Verify owned Docker resource cleanup | Docker resource cleanup | Runs even after a failure, if the baseline succeeded. Waits up to 30 seconds for Ryuk, then fails if new labelled resources remain. It never deletes anything. |
| Show actual failure phases | n/a | Writes the phase table to the job summary and `phases.json`, and an annotation for each failed or cancelled phase. |
| Normalize the scenario history row | n/a | Writes `history-row.json` from the saved receipts (see [Scenario history](#scenario-history)), and appends its bug compatibility selection, release, platform and role image IDs to the job summary. Its own failure never changes the job's result. |
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

Ceph batches declare their required dependency roles in `//ci:` metadata:
`essential`, `client`, `cephfs`, `rbd-aux` and `rgw`. The planner requires the
core role and prepares only that subset. Ordinary RADOS/RBD paths use
`essential,client`; CephFS adds `cephfs`, RBD mirrors/cache add `rbd-aux`,
and gateway paths add `rgw`. Mixed-service and broadly shared batches keep
all five when required. An undeclared Ceph profile conservatively gets all
five; non-Ceph profiles get none. `supervised_command` uses an idle
`essential` runtime; `native_input` uses essential + client and starts a
cluster without OSDs for ownership and attached-input proofs.

The composite action exports selected image IDs only after complete
preparation. There is no implicit substitution for a missing role. `integrationImages` constructs
`ceph.ImageSet` from those identities and fails when a used feature's role
is missing. Userspace native oracles run in the client toolkit; daemon-local
admin sockets and process probes stay in their owning daemon. Two-client
network tests keep separate client containers rather than collapsing both
sides onto one executor. Periodic campaign pinning resolves the same five
published roles to immutable registry digests before repeated attempts.

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
`ApplyNetworkProfile`) declares `bridge` alone, or keeps that part in the
bridge subtest.

The planner reads the modes from the call (`internal/tools/tagcatalog`), so they are
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

### Runner budget

GitHub runs at most 40 jobs of this account's hosted runners at once. The
limit is measured, not configured: of the jobs that waited more than two
minutes for a runner, the largest share started when exactly 39 others were
running, and the limit also covers the account's other repositories. The
constants in `tag_scenarios.py` divide it:

| Constant | Value | Meaning |
| --- | --- | --- |
| `RUNNER_LIMIT` | 40 | The measured limit. |
| `PULL_REQUEST_RUNNERS` | 16 | Runners that the periodic matrix leaves free. A pull request execution (Ceph short plus Code and Docker checks, about 55 jobs and 185 runner minutes) finishes in about the time of its longest batch with them. |
| `RELEASE_MAX_PARALLEL` | short 4, topology 2, multicluster 2, recovery 2 | The `max-parallel` of each release caller's runtime matrix, in Ceph periodic and Ceph release matrix. Short has about twice the runner minutes of each other category. |
| `ENVIRONMENT_MAX_PARALLEL` | 1 | The `max-parallel` of the periodic environment caller. |
| `CAMPAIGN_MAX_PARALLEL` | 3 | The `max-parallel` of the representative campaign: what a full matrix run leaves of the periodic share. |

A full matrix run holds at most 21 runners (two releases of 4 + 2 + 2 + 2,
and 1 for environment) and a campaign run at most 3, together the 24 that
pull requests leave. Both are needed because the two modes have separate
concurrency groups (`Ceph periodic-<ref>` and `Ceph periodic-<ref>-campaign`,
see [Representative campaign](#representative-campaign)), so a run of each
can be in progress at once. `tag_scenarios.py releases --scope all` fails
when the two groups together could hold more than
`RUNNER_LIMIT - PULL_REQUEST_RUNNERS` runners
(`periodic_runner_budget`), and `--scope others` fails when the release
callers alone could, so a new release, category or campaign bound cannot
take the pull request share unnoticed. The pull request entry points stay
unbounded and use whatever is free.

A bound only delays jobs: every matrix entry and every campaign attempt still
runs, with `fail-fast: false`, and a started run always finishes. Simulated
on the periodic runs of 2026-10-16 and 2026-10-17, the bounded matrix takes
74 to 81 minutes instead of about 40, well within its 3-hour schedule. With
the representative batches' measured durations, the default campaign (seven
batches, three repetitions) takes about 27 minutes on 3 runners.

Runtime batches are not packed into shared jobs, and a compiled binary is not
passed between jobs. In a pull request execution each batch compiles exactly
once, so there is nothing to share between jobs. Packing small batches would
save under 2 % of its runner time while serializing them and removing their
separate runners. The build cache above removes most of the compile time
instead.

To measure again, fill a cache directory from the GitHub API and report from
it offline:

```sh
python3 .github/scripts/ci_measurements.py fetch --cache /tmp/ci-cache --repo JSYoo5B/ceph-testcontainers-go \
  --since 2026-10-14 --marker pr-scope=<commit> --marker binary-reuse=<commit>
python3 .github/scripts/ci_measurements.py report --cache /tmp/ci-cache \
  --marker pr-scope=<commit> --marker binary-reuse=<commit> --pack-size 2 --pack-size 4
```

A marker names a CI change by its commit; each execution (the runs of one
head commit and event) belongs to the cohort of the newest marker that its
head contains. Cancelled runs and reruns are left out. The report gives per
cohort the job count, runner minutes, wall time from the first run's creation
to the last job's end, runner acquisition delay (a job's creation to its
start), per-phase step times and per-batch duration distributions. It also
gives the measured runner limit, pull request wall time with and without an
overlapping periodic run, and simulated periodic and pull request makespans
under `RELEASE_MAX_PARALLEL` (or `--limit category=N` and `--reserve N`).

Marker cohorts group executions by the CI change their head contains, not by
what each job ran. To compare repetitions of exactly one configuration, join
the measured jobs to their history rows:

```sh
git worktree add /tmp/ci-history origin/scenario-history
python3 .github/scripts/ci_measurements.py cohorts --cache /tmp/ci-cache \
  --history /tmp/ci-history --repo JSYoo5B/ceph-testcontainers-go --output /tmp/cohorts.json
```

`--history` takes a `scenario-history` checkout (`rows/*.jsonl`), downloaded
`history-row.json` artifacts, or both; a row is read once. A row joins the
cached job with its repository, run ID, run attempt and job ID. Joined
attempts are grouped by `scenario_history.config_key`: profile, network
mode, release, checked-out tree, build key, platform and every role image
ID. Each configuration has an outcome ledger with the attempts and runner
seconds of every kind (passed, assertion failure, timeout, cancelled and the
others of `scenario_history.attempt_kind`). Its sample count and min, median
and max of job, acquisition and phase seconds come from passed attempts only.
Rows that do not join (another repository, no job ID, a job missing from the
cache, a job that held no runner), measured runtime jobs without a row, and
rows without a complete configuration identity are listed with the reason.

Job ownership is resolved before configurations are grouped, so neither the
order of the rows nor the artifact path they came from decides a job's
bucket. All distinct rows (receipts) that name one job are collected first.
Receipts that agree on repetition, profile, network mode, release, outcome
kind and configuration key join the job once; the sample lists every
receipt's `row_sha256` as provenance and its runner time counts once.
Receipts that disagree, such as one with complete checkout metadata and one
without, make the job a receipt conflict. The job stays out of every
configuration and out of the incomparable list, and `conflicts` lists it with
its job seconds and each receipt's kind, configuration key and comparability
reasons; `conflict_runner_seconds` totals them. No receipt is preferred by
input order, and a conflict stays unresolved until its receipts are corrected.
Rows of two checked-out trees are two configurations, also when only the CI
policy differs; the key is never relaxed to merge them.

`fetch` stores the jobs of every completed run, cancelled runs included, so a
cancelled attempt that held a runner joins its row and counts in the
`cancelled` ledger, and the other jobs of a cancelled workflow join their
rows as well. The report still leaves cancelled runs out, so its results do
not depend on whether the cache holds their jobs. A cache filled before
this change has no job lists for cancelled runs. The next `fetch` over the
same days requests only those missing lists; the stored run lists of
finished days and the stored job lists are not fetched again.

Which cache a job restored decides what its setup and compile time mean, so
a cost comparison also needs the cache state of every sample. `fetch` reads
no job log by default. With `--cache-logs N` it reads at most N job logs per
call, oldest run first because logs expire, cancelled runs included, of the
jobs that held a runner and have a cache
step (`Restore ... cache`, `Save ... cache` or an unnamed `actions/cache`
step). `--cache-log-jobs REGEX` adds jobs by name, such as BuildKit image
builds in `ceph-testcontainers-images`. GitHub serves only a whole job log,
so only its cache lines are stored, without timestamps, in
`cachelogs/<job id>.json`: every step header, each `actions/cache` step
section, the `go version` line, the runner's `Cache mode`, lines naming a
`cache-to` destination, the continuation targets of a multiline `cache-to:`
input and BuildKit cache lines, at most 400 lines (the excerpt records
whether it was truncated). A `cache-to` line cut at the 500-character line
width is marked as cut. An
expired log is stored as unavailable and not read again; any other failed
request is counted and read on the next call.

```sh
python3 .github/scripts/ci_measurements.py fetch --cache /tmp/ci-cache --repo JSYoo5B/ceph-testcontainers-go \
  --since 2026-10-20 --cache-logs 50
python3 .github/scripts/ci_measurements.py caches --cache /tmp/ci-cache \
  --repo JSYoo5B/ceph-testcontainers-go --output /tmp/caches.json
```

`caches` lists one row per cache operation of every job that held a runner,
keyed by repository, run ID, run attempt and job ID, with the workflow, job,
event (`pull_request`, `push`, ...) and head branch, the cache kind
(`go-module`, `go-build`, `registry` or `other`), the operation (`restore`,
`save`, BuildKit `import` or `export`), the exact key or cache reference, the
Go toolchain and platform of the job, whether the operation was requested and
its outcome. Every cache step that ran keeps a row. The excerpt's
`actions/cache` sections are matched to those steps in order, by operation
and by kind where the step name and the key both give one. A step without a
section is `unknown` with the reason: no log, an unavailable log, or no
section in the excerpt (`excerpt_truncated` says whether the excerpt was
cut). An empty excerpt therefore leaves the steps `unknown`, never
`no cache restore`. A section without a matching step keeps its own row.

BuildKit's `exporting cache to registry` names no destination, and one build
may import several references and export to another. An export's key is the
literal `ref=` of its own step's cache-to selection when that selection is
exactly one destination. The selection gathers every `--cache-to=<target>`,
separate `--cache-to <target>` argument (as `build-push-action` emits them),
`cache-to:` input and its continuation lines. A reference through a
variable, a destination without a ref, two destinations, none, or a cut
`cache-to` line leave the key empty; it is never taken from an import.

Only a log line sets an outcome:

| Outcome | Evidence |
| --- | --- |
| `hit` | `Cache restored from key` naming the input key; a BuildKit cache manifest import that reached `done` |
| `partial-restore` | `Cache restored from key` naming another key (a `restore-keys` prefix) |
| `miss` | `Cache not found for input keys`; a BuildKit import `ERROR` saying `not found` |
| `restore-failed` | `Failed to restore`, an `##[error]` in a restore step, or any other BuildKit import `ERROR` |
| `saved` | `Cache saved with key`; a BuildKit `writing cache ... manifest ... done` |
| `save-skipped` | the save step's own conclusion is `skipped` (requested is false) |
| `save-failed` | `Failed to save`, a refused cache reservation, an `##[error]` in a save step, or a BuildKit `ERROR` or `WARNING` of the export |
| `unknown` | a requested operation whose log states none of these, or a cache step whose log was not fetched or has expired |

A green job or a green save step is never read as a write.
`continue-on-error: true` and BuildKit's `ignore-error=true` keep a job green
after a failed save, and such a save is `save-failed` when its log says so and
`unknown` when the log is missing. A registry `hit` is the import of a cache
manifest; which stages it served is a separate question that the ledger does
not answer.

`cohorts` reads the same ledger from the cache directory. Each sample carries
the cache condition of its job: every restore or import as `kind=outcome`,
such as `go-build=hit,go-module=hit`, or `no cache restore`. Each
configuration also lists `by_cache`, the same outcome ledger and passed-only
ranges per condition. The configuration's own ranges stay as they are, and a
sample without a fetched log is in an `unknown` condition rather than in a
guessed one.

## Periodic runs

`Ceph periodic` runs every runtime category on the role images of every
[supported release](#supported-releases), on `main`, in every
[network mode](#network-modes) that a batch's tests declare. Its schedule
fires every 3 hours, and its
`Gate` job (`.github/scripts/periodic_gate.py`) decides whether the matrix
runs:

| Situation | Matrix runs |
| --- | --- |
| `main` has a commit that no earlier periodic run executed | yes |
| The last execution of the current commit is 7 days old or older | yes, as a repeat |
| The repository variable `CI_RELEASE_PHASE` is `true` | yes, on every firing |
| A manual dispatch | yes |
| Otherwise | no; the run ends after `Gate` |

An earlier run counts as an execution only when a batch job ran to an end:
a job of a runtime caller, named `<caller> / <batch>`, other than its `Plan`
job. Jobs that the periodic workflow defines itself (`Gate`, `Releases`,
`Pin`, `History`) only plan or record, so a run that ended in them executed
nothing and the next firing runs the commit again.

While development is active, a run therefore checks the commits merged since
the previous one, and a failure points at those commits. When `main` stays
still, as in a release phase, the same commit runs again and again.

### Supported releases

`SUPPORTED_RELEASES` in `.github/scripts/run_image_matrix.py` lists each
supported release with its codename and pinned official image. Ceph periodic
and Ceph release matrix do not list releases themselves: their `Releases`
job runs

```sh
python3 .github/scripts/tag_scenarios.py releases --scope all --check-prefix 'Ceph periodic'
python3 .github/scripts/tag_scenarios.py releases --scope others --check-prefix 'Ceph release matrix'
```

and calls `tagged-runtime.yml` once per release and category of the printed
matrix, as `<Codename> <category>` with the check name
`<prefix> <codename> <category>`. The `environment` category needs no Ceph
release and stays a separate Ceph periodic job.

The command also proves coverage before it prints the matrix. It expands the
callers into `(release, profile)` cells and fails, naming the missing or
extra cells, unless every required profile runs on every release of the scope
exactly once. `test_scenario_shards.py` checks the same rule offline, with
the release and caller set the workflows had before they were generated, so
a consolidation cannot drop a required cell.

To add a release, add its row once ceph-testcontainers-images publishes its
role images, then run the CI tooling tests. A release or patch that is not a
row is refused (`run_image_matrix.py --release 20.2.5` is a usage error), and
the role images of one cell always come from one release. For a new major
release, also add its entry to every release-limited row of the test
expectation table (see
[Release differences](testing.md#release-differences)). The default release
(`RELEASE`, the `release` input default of `tagged-runtime.yml` and of the
scenario-images action, and `ceph.DefaultImage`) changes only when the
current release changes.

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
in the default `full` mode always runs the matrix.

### Representative campaign

The campaign repeats a small, source-owned set of batches many times on one
configuration, so that a same-configuration pass or fail record exists for
them. The set is every batch whose `//ci:` line declares
`representative=<theme>` (see [Testing](testing.md#batches-and-the-ci-line));
`tag_scenarios.py plan --representative` selects it and fails if a theme
(`topology`, `auth`, `mirroring`, `recovery`, `flaky`) has no batch. The
matrix and the category workflows keep running every batch; the campaign
adds repetitions only.

The campaign runs in Ceph periodic, on the daily `23 4 * * *` firing or on a
dispatch with mode `representative`. Its `Gate` decision is
`campaign=true`, and then:

1. `Pin` resolves the commit (the `ref` input, or the run's commit) and the
   registry digest of every role image tag of the release
   (`periodic_campaign.py pin`, through `docker buildx imagetools inspect`).
2. `Representative` calls the reusable template with
   `category: representative`. Each matrix entry is repeated `repetitions`
   times (default 3, at most 10), and each attempt is its own job on a fresh
   runner, named `<batch> #<n>`, with artifact names ending in `-r<n>`. Every
   attempt checks out the pinned commit and pulls `tag@digest`, so a moved
   tag cannot change a later attempt. The attempt's history row carries
   `repetition_id` `<n>` under the same profile ID.
3. Each attempt keeps the normal native completion, no-skip and cleanup
   proofs; nothing is retried.

The planner bounds a campaign at 100 jobs and 3000 summed `job-timeout`
minutes. A campaign run never counts as an execution of the matrix for the
gate, and has its own concurrency group, so a matrix firing never replaces a
waiting campaign. A campaign can therefore run beside a full matrix run, and
its `max-parallel` is `CAMPAIGN_MAX_PARALLEL` (see
[Runner budget](#runner-budget)).

```sh
gh workflow run "Ceph periodic" -f mode=representative -f repetitions=5 -f release=19.2.5
gh workflow run "Ceph periodic" -f mode=representative -f ref=<commit>
```

`ref` must be a commit that already contains the campaign tooling, because
every job runs the scripts of the pinned commit.

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
never builds an image. The other optional batches have no workflow and are run
locally with `tag_scenarios.py optional`: `native_shuffle`, and
`cephfs_fuse`.

`cephfs_fuse` (`TestCephFSFuseMount`) mounts CephFS with `ceph-fuse` in
client containers that get `/dev/fuse`, `CAP_SYS_ADMIN` and
`apparmor=unconfined`. It is optional because those settings are proven
only on native, rootful Linux Docker Engine; GitHub-hosted runners are
untested, and no required check should depend on them. On an engine that
refuses the mount the test fails with the missing requirement (device,
capability, security profile or engine device mapping); it never skips or
falls back to libcephfs. Outside the tag runner, build it with the
`cephfs_fuse` tag (`go test -tags all,cephfs_fuse`). See
[the feasibility report](../research/ceph-fuse-mount.md).

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
| `tag-scenario-<id>` | `modules/modules.log` and `modules/modules-report.json` (download output and outcome); `compile/compile.log` and `compile/compile-report.json` (compiler output, outcome and binary receipt; the binary itself is not uploaded); `native/native.log` (complete verbose test output); `native/report.json` (verdict); `native/test-artifacts/` (the tests' retained artifact directories, such as [failure archives](#failure-archives)); `phases.json` (phase outcomes); `history-row.json` (the job's [history row](#scenario-history)) |
| `runtime-cleanup-<id>` | `before.json` and `after.json`: engine identity, labelled resources before and after, and anything left behind |
| `runtime-cleanup-<id>-baseline` | Uploaded only when the baseline itself failed |
| `scenario-images-<id>` | `summary.json` with the resolved role image IDs, digests and platform |
| `plan-<category>-<release>` | `tag-plan.json` (the selected profiles, before network and repetition expansion) and `tag-dispatch.json` (the expanded matrix that was dispatched, with the network scope, repetitions and run, attempt, release and category; absent when expansion failed, `count` 0 when the selection is empty) from the `Plan` job, and `modules/` when it downloaded the modules |
| `code-tag-coverage` | `tag-plan.json` with compiled coverage, and the `code` batch results (Code checks) |

Every upload explicitly retains its artifact for 7 days, or 14 days if the
job or its current composite action has already failed when the upload runs.
Image identities and plans
uploaded before a later test failure keep their original 7-day expiry;
the final native and cleanup diagnostics use the failure retention.
The periodic `History` job still downloads the current run's artifacts
before storing its rows on the durable `scenario-history` branch. Retention
applies to new uploads only; existing artifacts keep their recorded expiry.

### Dispatch join

`dispatch_join.py` joins a downloaded `tag-dispatch.json` to the jobs of the
same repository, run and attempt. It is offline: the jobs are a saved
jobs-API response (repeat `--jobs` for pages), and `--receipts` optionally
reads `history-row.json` files.

```sh
python3 .github/scripts/dispatch_join.py --dispatch tag-dispatch.json \
  --jobs jobs.json [--receipts downloaded-artifacts] [--caller "Tentacle short"]
```

Every dispatch entry and every listed job ends in exactly one bucket, and the
report checks that itself (exit 3 if not): `matched`, `conflicts`,
`ambiguous`, `unmatched_expected` and `invalid_expected` for entries;
`matched`, `conflicts`, `ambiguous`, `unmatched_actual`, `invalid_actual` and
`out_of_scope` for jobs. A job matches an entry by its own name (the part
after `<caller> / `); names shared by several entries or jobs stay
`ambiguous`. A job of another run, attempt or repository, a repeated job ID or
a receipt that contradicts the entry is a conflict. Missing, null or
wrong-typed fields are invalid or inconclusive, never matched, and a truncated
job list (`total_count`) or an incomplete dispatch envelope makes the whole
input inconclusive (exit 2). The `Plan` job and other jobs of the caller are
reported as `unmatched_actual`; jobs of other callers are `out_of_scope`.

Receipts are accounted for as well: every `history-row.json` read ends in
exactly one of attached to a matched job, `receipts_without_a_matched_job`,
`receipts_other_run` or `invalid_receipts`. A receipt without a usable job key
is invalid and is never associated with a job by guesswork, and the history
format's `missing` marker in a compared field is a gap (inconclusive), never
agreement or contradiction.

`context.sha` is reported as `declared_workflow_sha`. The checkout identity
comes only from receipts and is null when none states it.

A match proves neither runner allocation nor native execution. The report
computes no cost, billing, native outcome or empirical cohort and changes no
scheduling or selection; it is not wired into any workflow.

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
- `test_artifacts`: the files that the tests left in `native/test-artifacts/`,
  with their sizes, and the number of failure archive manifests among them.

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

### Failure archives

The runner passes Go's artifact flags in both execution modes: `-artifacts
-outputdir=<native>/test-artifacts` to `go test`, and `-test.artifacts
-test.outputdir=<native>/test-artifacts` to the compiled test binary. A test
that finalizes its cluster with
[`TerminateOnCleanup`](../ceph.md#terminateoncleanup) and `Archive` writes
its failure archive into its `ArtifactDir`, so the archive lands under
`native/test-artifacts/_artifacts/<package>/<test>/<random>/ceph-<cluster>-<execution>/`
with `diagnostics.json`, `crash-metadata.json` and `manifest.json`. Parallel
tests, repeated executions and several clusters of one test get separate
directories. A passing test calls nothing and leaves no directory.

Three outcomes are recorded separately, and none implies the next:

1. **Local file creation.** The test log names the archive directory and
   what each collector and file write did; the manifest records the same
   per collector and file. `test_artifacts` in `native/report.json` lists the
   files that exist after the run.
2. **Upload.** `native/test-artifacts/` is inside the uploaded
   `tag-scenario-<id>` artifact. For a failed native step, the job summary's
   `Failure archive` section records the number of local manifests, the
   upload step's outcome and the artifact's URL and ID.
3. **Availability.** Only downloading the artifact of a failed run and
   reading its manifest shows that the archive is retained. Unit and fixture
   tests of the archive do not show it, and neither does a listing or a
   successful upload step.

An archive holds a snapshot taken before the cluster's termination, not
continuous history. Nothing is archived when the runner is lost or the test
process is killed or panics, for example at the job timeout or the Go test timeout.

#### Controlled failed-native download proof

The optional `failure_archive` batch runs two genuinely failed native subtests:
an ordinary assertion deliberately expects two OSDs after independently checking
the one-OSD native control, and a finalization verification requires a crash that
has not happened. Both use the real `testing.T`, not a recorder that turns an
expected failure into a passing test. Both end the workload context before
finalization; each archive records the original trigger, the before-cleanup
snapshot, and later debug restoration and termination outcomes. A single owned
OSD debug window is tracked with the existing local receipt API, so the initial
`applied-verified` copy and later `restored-verified` copy remain distinct.

This batch is outside `all` and all required runtime profiles. Run it only by
an explicit request. The existing manual workflow accepts a boolean input;
its default remains the RGW regression batch:

```sh
gh workflow run native-regressions.yml --ref <branch> -f failure_archive=true
```

The native step and workflow **must remain failed**. A failed compile, image
preparation, cleanup, missing archive or unrelated assertion is not a successful
proof of failure-archive delivery. The ordinary tagged runner, phase reports,
resource checker and always-run upload handle this batch without a separate
failure framework or a native retry. The workflow's `Failure archive` summary
records upload outcome and artifact ID; retain that independently of the files.

After the attempt finishes, download its scenario, image preparation and cleanup
artifacts using their exact names from that attempt. Do not combine artifacts
from different attempts or derive the expected image cohort from the archive:

```sh
gh run view <run-id> --json headSha,attempt,jobs
gh api repos/<owner>/<repo>/actions/runs/<run-id>/artifacts
gh run download <run-id> -n <tag-scenario-artifact> -D artifacts/archive-proof/scenario
gh run download <run-id> -n <scenario-images-artifact> -D artifacts/archive-proof/images
gh run download <run-id> -n <runtime-cleanup-artifact> -D artifacts/archive-proof/cleanup

# Run against the source checkout of that exact head.
python3 .github/scripts/failed_archive_proof.py \
  --scenario artifacts/archive-proof/scenario \
  --images artifacts/archive-proof/images --cleanup artifacts/archive-proof/cleanup \
  --root . --revision <full-checked-out-commit> --run-id <run-id> --attempt <attempt> \
  --output artifacts/archive-proof/audit.json
```

The offline audit requires both complete failed native cases, matching compiled
binary/source hashes, independent image-preparation IDs and platform, both
retained archives and their manifest-listed file sizes, original failure
triggers, separate debug receipt copies, successful restoration/termination and
zero new resources on the same engine. It also checks the history row's source
and run identity. Missing manifest provenance stays explicitly missing; the
independently retained compile/history records do not rewrite the manifest.
Its zero exit means `archive_verification: verified`; the result still says
`native_outcome: failed` and does not claim a passing workflow. These records
are not cryptographic attestation, and an offline directory alone does not
establish upload: preserve the actual upload step/API identity and successful
download as separate evidence.

This proof covers one healthy cluster's assertion/post-check failure path and
the tracked debug restore. It does not establish every collector failure,
partial bootstrap, disk failure, forced process exit or runner-loss path under
#49/#51, and it does not close either issue by itself.

### Retained archive audit

`archive_audit.py` audits one already downloaded finalization archive
directory (`manifest.json` and the files it lists). It only reads files,
recollects nothing and never changes a test verdict:

```sh
python3 .github/scripts/archive_audit.py <archive-directory> --output audit.json
```

It reports these facts separately for each file and for the archive:
availability inside the archive root (no link or path escape), the producer's
recorded write outcome, JSON readability, size agreement, the producer's
`archive.complete` declaration, and `final` plus recorded termination.
`evidence_complete` is true only when the producer declared the archive
complete, the manifest is final with a termination, and every listed file and
capture copy is verified. `final=true` or a successful copy never upgrades an
archive the producer declared incomplete.

A write the producer recorded as failed has `bytes: 0` (or the staged count)
regardless of what was kept. A retained fragment is hashed and reported as
producer-declared partial, never as download corruption, and an absent file is
reported as a producer-declared failed absence.

Each `capture_files` record (a raw packet copy registered with
`CaptureWithFile`) is joined to the `files` entry of the same name:

| Producer `copied` | Downloaded file | Capture state |
|---|---|---|
| true | present, copied size = expected size = file size, full SHA256 agrees three ways | `verified` |
| true | absent | `missing` |
| true | size or digest disagrees | `size_mismatch` or `digest_mismatch` |
| false with producer error | absent | `producer_declared_failed_absent` (explained omission) |
| false with producer error | present | `failed_copy_file_present` (unexpected retained file) |

The raw copy is atomic, so a failed copy publishes no final file even when it
staged bytes (`bytes` is nonzero); that absence is not evidence of upload loss.
Raw captures are not parsed as JSON. Missing, null or wrong-typed fields, a
contradictory error field, a `files` entry that disagrees with the record, an
unsafe name, a duplicate name or a record with no `files` entry are not
matches (`inconclusive`, `duplicate_name` or `not_in_files`). Extra `.pcap`
or `.capture-*` files with no record are listed in `unrecorded_capture_files`,
apart from the records and from `unlisted_files`. The producer outcome
(`producer_copy`), downloaded availability (`downloaded`) and the archive
fields stay independent keys, never one boolean.

The fixtures under `.github/scripts/fixtures/` are real finalization producer
output (a failed JSON write, and four raw copies: valid identity, same-size
wrong digest, read error after bytes, limit exceeded with 0 bytes). The audit
does not establish a real packet capture, an upload or download, a filesystem
under pressure, or any native run: it checks the stated producer contract on
bytes that were already downloaded.

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
- `profile`: the receipts' profile identity and tags, and
  `bug_compatibility`, the selected bug tags (empty for every ordinary path,
  `missing` without a receipt). Compile and native receipts with different
  tags or selections, or receipts of another selection than
  `--bug-compatibility`, write no row. With `--summary`, `normalize` also
  appends the selection, tags, checkout tree, release, platform and every
  role image ID to the job summary.
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

#### Durable history and classification

The `History` job of every Ceph periodic run that executes the matrix or a
campaign downloads the run's `tag-scenario-*` artifacts and runs
`scenario_history.py collect` on a checkout of the `scenario-history`
branch. It stores the rows in `rows/<run-id>-<run-attempt>.jsonl`, one file
per run attempt that is only ever appended to, rewrites `summary.json`, and
pushes the commit. A branch keeps the history without expiry (artifacts
expire after 7 or 14 days and caches are evicted), needs no service
outside the repository, and makes every update a reviewable commit. A row
without a run identity is skipped with a warning; a row that conflicts with
a stored one fails the job and changes nothing.

`summary.json` (schema `ceph-scenario-history/v1/classification`) has, per
profile ID and release:

- `recent`: the last 20 attempts, their attempt kinds, and how many of the
  scenario outcomes among them failed.
- `configs`: the 10 most recent configurations. A configuration is the
  profile, network mode, release, checked-out tree, build key, platform and
  every role image ID. The build key holds the profile's tags, so another
  bug compatibility selection is another configuration; the selection has
  no field of its own in the identity, which keeps the keys of earlier rows.
  Rows without complete identities are counted in
  `incomparable_rows` and never joined to one. Each configuration has its
  `passed` and `failed` counts, the excluded attempts by kind, the first
  failure (run, attempt, job, repetition, phase and test; a later pass does
  not remove it), per-test `failed`/`ran` counts and a classification. A
  test's counts cover only the attempts whose native log has its `PASS` or
  `FAIL` line, so an attempt that stopped before the test ran (a timeout,
  panic or earlier abort) or left it without a result is not counted for it.

Attempt kinds separate what an outcome is evidence of. `passed`,
`assertion_failure`, `timeout` (the native receipt timed out) and
`cleanup_failure` are scenario outcomes. `runner_failure` (modules, compile
or Docker baseline), `image_failure`, `cancelled` and `incomplete` are
excluded from the scenario record.

| Classification | Scenario outcomes of the configuration |
| --- | --- |
| `insufficient_samples` | fewer than two |
| `confirmed_pass` | two or more, all passed |
| `deterministic_failure` | two or more, all failed with the same phase, test and classification |
| `confirmed_fail` | two or more, all failed, in different ways |
| `suspected_intermittent` | at least one pass and one failure |

Print the classification of a local checkout of the branch:

```sh
git fetch origin scenario-history
git worktree add /tmp/history origin/scenario-history
python3 .github/scripts/scenario_history.py classify --history-dir /tmp/history
```

When a runtime job fails, it reads `summary.json` from the branch and
`report_test_failures.py` adds `Scenario history` notices beside the failed
test names, for example `TestClientRevocation/aes failed 2/10 earlier
same-config attempts that ran it`, the same configuration's record and
classification, and the profile's recent failure count. A test with no
observed outcome in the earlier attempts gets `has no observed outcome in N
earlier same-config attempts` instead of a count, and a job whose
configuration has no earlier attempt says so. The notes describe history; they do not decide the
job's result.

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
