# Testing

This page is for contributors. It explains how the test suite is organised,
how to run it locally against the image you choose, and how to add tests that
CI picks up automatically. What the GitHub Actions workflows do with these
tests is described in [Continuous integration](ci.md).

## How the suite is organised

| Layer | Location | Needs Docker | How it is selected |
| --- | --- | --- | --- |
| Unit tests | Next to the code they test, without build tags | No | `go test ./...` |
| Package examples | `examples_test.go` in `ceph`, `rbd`, `cephfs`, `rgw` | No (Docker examples are only compiled) | `go test ./...` |
| Ceph integration tests | `internal/integration` | Yes, plus Ceph images | Build tags |
| Docker environment tests | `internal/dockerbridge`, `*_integration_test.go` | Docker only, no Ceph | Build tags |
| CI tooling tests | `.github/scripts/test_*.py` | No | `python3 -m unittest` |
| go-ceph consumer check | `internal/integration/goceph` | Yes, prepared Linux images | `goceph` tag and a harness |

Integration tests are excluded by build tags, never by `t.Skip`. A plain
`go test ./...` therefore never starts a container, and a tagged run does not
quietly pass when Docker is missing: it fails.

`Example...` functions that start a cluster have no `// Output:` comment.
`go test` compiles them, so they must keep building, but does not execute
them. An example with an `// Output:` comment runs in every `go test ./...`
and therefore must not need Docker.

## Prerequisites

For the opt-in warm startup/container/resource comparison of dependency roles,
see [Measuring dependency-role overhead](role-overhead.md).
To count the methods and fields of struct types and compare two snapshots, see
[Structural metrics](structural-metrics.md).

- Go 1.26 or later. In this repository, `go.mod`'s `toolchain go1.26.9` line
  makes an older 1.26 patch switch to 1.26.9, which `make vulncheck` needs to
  pass.
- A Docker engine that runs Linux containers and that testcontainers-go can
  reach, with pull access to `quay.io/ceph/ceph` and the Ryuk image.
- A C toolchain for `make race` only. The race detector needs cgo; every other
  target builds with `CGO_ENABLED=0`.
- Python 3.9 or later for the scripts under `.github/scripts`.
- Memory: the suite is sized for a 4 GiB Docker VM. Tests run one at a time
  unless you set `CEPH_TEST_PARALLEL` (see
  [Running tests in parallel](#running-tests-in-parallel)).

If testcontainers-go cannot find the Docker Desktop socket, point it at the
current Docker context. Ryuk may need the socket path inside the VM:

```sh
export DOCKER_HOST="unix://${HOME}/.docker/run/docker.sock"
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
```

Many batches contain `TestHostNetwork...` variants of a scenario. These need
the test process to reach daemons on the Docker host network: a Linux Docker
engine, or Docker Desktop 4.34 or later with host networking enabled.
Otherwise `Run` returns an error that wraps `ceph.ErrHostNetworkUnavailable`
and the test fails. When the test process itself runs inside a container, set
`TESTCONTAINERS_HOST_OVERRIDE` to the Docker host address; CI sets it to
`127.0.0.1`. See [Host network](../guides/host-network.md).

## Host checks: unit tests, race detector, vet

```sh
make test         # CGO_ENABLED=0 go test -mod=readonly ./...
make race         # CGO_ENABLED=1 go test -mod=readonly -race ./...
make vet          # CGO_ENABLED=0 go vet -mod=readonly ./...
make tag-compile  # test-only-code boundary check, then compile every tagged test file
make check        # all four of the above; never starts Docker
make lint         # golangci-lint v2.14.0 with .golangci.yml
make vulncheck    # govulncheck v1.9.0 on every scanned platform and tag set
```

`make vulncheck` downloads and builds govulncheck itself; see
[Continuous integration](ci.md#vulnerability-check) for the scans and the
failure policy. `make lint` needs golangci-lint installed. Its configuration enables every
build tag, so it also checks the integration and go-ceph files, and the Code
checks workflow runs the same version.

`make test`, `make race` and `make vet` see only untagged files.
`make tag-compile` first runs the [test-only-code check](#test-only-code), then
compiles the module with only `integration`,
so shared helpers cannot accidentally require an unrelated capability tag.
It also builds the module's `all` union, its consumer/native-regression/role
union and its `all,cephfs_fuse` union. Every command runs nothing (`-run '^$'`):
compilation does not establish native execution. These projections still cannot
check every individual CI batch's helper dependencies. Use
`plan --verify` for that (see
[Verify batch profiles before pushing](#verify-batch-profiles-before-pushing)).

The CI tooling has its own Docker-free tests:

```sh
python3 -m unittest discover -s .github/scripts -p 'test_*.py' -v
python3 -m unittest discover -s internal/integration/goceph -p test_run.py -v
```

## Build tags

Every integration test file starts with a `//go:build` line. The tags are:

| Tag | Selects |
| --- | --- |
| `integration` | Base tag of every Ceph integration test file. Alone, it selects the tests that need no capability tag. |
| `features` | Server fixtures and client recipes: pools, Cephx, subvolumes, RGW features, mirror scope. |
| `auth` | Client identity and Cephx capability tests. |
| `topology` | MON/MGR/MDS/RGW composition and node lifecycle. |
| `multicluster` | Independent clusters connected by RBD/CephFS mirroring or RGW multisite. |
| `hostnetwork` | Host-network-only scenarios. |
| `diagnostics` | Diagnostics collection. |
| `all` | The whole normal suite, regardless of capability or CI tags. |
| `native_regression` | Strict tests for known upstream Ceph defects. Never part of `all`. |
| `goceph` | The Linux go-ceph consumer test. Never part of `all`; needs prepared images. |
| `ci`, `ci_<category>`, `ci_batch`, `ci_batch_<name>`, `ci_optional`, `ci_sdk` | Set only by the CI runner to select one batch. Do not pass them by hand. |

`all` is the local entry point for the full suite. Every constraint begins
with `all || (...)`, so `all` wins over capability and CI tags. That is why
`-tags=all,ci,ci_short` does not filter anything; to run one category or batch
use the [tag runner](#one-ci-batch-exactly-as-ci-runs-it). The `all` build tag
has nothing to do with the Ceph `all` role image.

[Bug-ID compatibility selection](bug-compatibility.md) is a separate
implementation choice from these test selectors. `all` must not implicitly
select bug workarounds, and the planner never collects a `ceph_bug_` tag as a
capability. The CI runner takes an explicit `--bug-compatibility` selection
that must name registered tags (see
[Bug compatibility selection](ci.md#bug-compatibility-selection)). The
[supported-tag catalogue](../guides/bug-compatibility-tags.md) currently has no
registered tags, so the only accepted selection is empty. Future pairs must
retain the same test contracts and prove which path was selected, alongside
the actual image/build identity.

## Test-only code

Test-only code is kept inside a checked import, seam and declaration boundary,
with no build tag and no extra test command flag. The shared helper packages
and the seam are ordinary untagged source, so `go build ./...` still compiles
them; the boundary limits who can use them:

- Anything that only tests of the same package use is a `_test.go` file of that
  package.
- A `_test.go` file of package A is invisible to the tests of package B, so
  what tests of several packages share (a synthetic `ceph.Container`, access
  to its locks and registry, fake CLIs, accessor wrappers) lives in helper
  packages under `internal/`: `internal/testhelper`, `internal/clustertest` and
  `internal/clusteraccess`. Only `_test.go` files import them.
- A helper cannot read unexported `ceph` state. It reaches it through one
  registered seam: `ceph/unit_access.go` sets `hookreg.Test` to an unexported
  `unitAccess` value whose `Unit*` methods take and return plain values, and
  `internal/testhelper` is its only caller. The seam is not one of the hook
  values that production packages assert, and no public package has a
  `Test*` function or method or a test-config or test-state struct in a
  non-test file.

`make tag-compile` runs `go run ./internal/tools/testsupportcheck` first. For
every ordinary tag set of that target it requires, from `go list` metadata and
the syntax of the selected files, that:

- no non-test file of the module imports a package of the manifest's
  `test_only_packages` list;
- no selected non-test file outside those packages declares a function or
  method whose name starts with `Test`, or a type of the manifest's forbidden
  list;
- `hookreg.Test` is read only in the manifest's seam files. The import is
  resolved by path to its local binding, so a renamed import is caught, a
  local variable or another package called `hookreg` is not, and a dot import
  of the seam package is rejected outright in other files.

Add a new shared test helper package to
`internal/tools/testsupportcheck/manifest.json` in the same change. The
command's own tests keep a fixture module with a negative control for each rule.
`go run ./internal/tools/testdoublecheck`, the next line of `make tag-compile`,
then requires the package's exported declarations to appear in the
[test-double inventory](test-double-inventory.md), as described there.

The `internal/` path adds import visibility from outside the module: a
downstream module that imports `internal/testhelper`, `internal/clustertest`,
`internal/clusteraccess` or `internal/hookreg` fails with Go's "use of internal
package ... not allowed". That is a property of the import path, not something
the check measures.

What this does not prove: the helper packages and the seam stay compiled by
`go build ./...` and `go vet ./...` (they have no tag), so an ordinary build of
the module still compiles them. The boundary limits who imports them and who
reads the seam. It makes no claim about which symbols a linked binary contains,
because that was not measured. The check reads source and `go list` metadata,
not a symbol listing of a linked binary, and it does not look at packages
outside the module.

## Running integration tests

Always pass `-count=1` to Docker-backed runs, so that Go never reports a cached
result.

### One test

`-tags=all` compiles every normal test, so it works for any test name:

```sh
CGO_ENABLED=0 go test -tags=all -count=1 -v -timeout=30m \
  -run '^TestRGWS3$' ./internal/integration
```

Select a subtest by anchoring each level:

```sh
CGO_ENABLED=0 go test -tags=all -count=1 -v -timeout=90m \
  -run '^TestMultiClusterRBDReceiverReadiness$/^bridge$' ./internal/integration
```

Optional tests also need their opt-in tag, for example
`-tags=all,native_regression`.

A test that declares network modes runs in each of them. `CEPH_TEST_NETWORK`
selects one, as a CI matrix entry does (see
[Network modes](ci.md#network-modes)):

```sh
CEPH_TEST_NETWORK=bridge CGO_ENABLED=0 go test -tags=all -count=1 -v -timeout=30m \
  -run '^TestOSDFullRatios$' ./internal/integration
```

### The whole suite

```sh
make test-all                       # -tags=all -count=1 -v -timeout=180m ./...
ALL_TEST_TIMEOUT=6h make test-all   # a longer process budget
```

The full suite runs every scenario sequentially and takes hours on a laptop.
Prefer one test, a capability group (see [Make targets](#make-targets)) or one
CI batch.

### One CI batch, exactly as CI runs it

CI groups tests into batches by build tags. List them, with their budgets,
without starting Docker:

```sh
python3 .github/scripts/tag_scenarios.py plan --output artifacts/tag-plan.json
```

Each entry of `include` has a `category`, `batch`, `package`, `timeout` and
`job_timeout`. Run one entry:

```sh
python3 .github/scripts/tag_scenarios.py run \
  --category short --batch default --package ./internal/integration \
  --directory artifacts/local-short-default
```

`run` compiles the batch's tag profile, checks the compiled test list against
the source, runs `go test -tags=<profile> -count=1 -v -timeout=<batch timeout>`
and writes `native.log` and `report.json` into `--directory`, which must not
exist yet. It applies the same pass rules as CI (see
[How a batch passes](ci.md#how-a-batch-passes)). The runner fixes
`CGO_ENABLED=0`, `GOWORK=off` and `GOFLAGS=-mod=readonly` and unsets
`CEPH_TEST_RBD_CLIENT_IMAGE`. Every other variable, including the image
variables below, is passed through. `artifacts/` is git-ignored and is the
usual place for local logs.

Optional batches run through the `optional` subcommand:

```sh
python3 .github/scripts/tag_scenarios.py optional \
  --batch native_rgw_translation --package ./internal/integration \
  --directory artifacts/local-native-rgw
python3 .github/scripts/tag_scenarios.py optional \
  --batch native_shuffle --package ./internal/integration \
  --directory artifacts/local-native-shuffle
python3 .github/scripts/tag_scenarios.py optional \
  --batch cephfs_fuse --package ./internal/integration \
  --directory artifacts/local-cephfs-fuse
```

`cephfs_fuse` is optional for another reason: its client containers need
`/dev/fuse`, `CAP_SYS_ADMIN` and `apparmor=unconfined`, which only native,
rootful Linux Docker Engine is proven to allow (see
[ci.md](ci.md#native-regressions)). It fails, rather than skips, on an
engine that refuses the mount.

These batches assert behaviour that the unpatched default image is known not
to provide, so they are expected to fail there. They exist to verify a patched
image; they never convert a failure into a pass.

### Running tests in parallel

Tests that call `parallelWhenEnabled(t)` as their first statement run
concurrently when `CEPH_TEST_PARALLEL` is set to an integer of 2 or more. At
most that many opted-in tests run at once, and each keeps its slot until its
own cluster has been terminated. Unset, or `1`, keeps every test sequential,
which is what a 4 GiB Docker VM needs. Any other value fails the test. CI uses
`2` on 16 GB runners.

With parallel runs, `-v` output interleaves `=== PAUSE`, `=== CONT` and
`=== NAME` lines. The CI pass rules still apply unchanged.

## Choosing images

CI supplies immutable image IDs through `CEPH_TEST_ESSENTIAL_IMAGE`,
`CEPH_TEST_CLIENT_IMAGE`, `CEPH_TEST_CEPHFS_IMAGE`, `CEPH_TEST_RBD_AUX_IMAGE`
and `CEPH_TEST_RGW_IMAGE`. Only roles declared by the batch are prepared;
missing selected roles never fall back to essential.

A local run with no explicit role variables uses `ceph.DefaultImage` for
all roles. `CEPH_TEST_IMAGE` can select another aggregate image. For split
images set essential and the feature roles your selected test requires:

```sh
CEPH_TEST_ESSENTIAL_IMAGE=registry.example.com/ceph:essential \
CEPH_TEST_CLIENT_IMAGE=registry.example.com/ceph:client \
  CGO_ENABLED=0 go test -tags=all -count=1 -v -run '^TestRBDLifecycle$' ./internal/integration
```

The supplied-image runner reads every supported variant/release from the
checked-in digest map and never builds or publishes images. Additional
routing checks are optional source-owned profiles, available to the same
`/ci run role_routing role_mirror_refresh [on all]` review command as other
runtime batches. Normal category checks run the existing required coverage.

### Squid runs

Tentacle 20.2.x is the default. To run a test on Squid, select the Squid image:

```sh
CEPH_TEST_IMAGE=quay.io/ceph/ceph:v19.2.5 CGO_ENABLED=0 \
  go test -tags=all -count=1 -v -timeout=30m -run '^TestRGWS3$' ./internal/integration

CEPH_TEST_IMAGE=quay.io/ceph/ceph:v19.2.5 \
  python3 .github/scripts/tag_scenarios.py run --category short --batch default \
  --package ./internal/integration --directory artifacts/squid-short-default
```

Use 19.2.5. In 19.2.6 the secondary zone's `radosgw-admin realm pull` is
rejected by the master gateway with HTTP 403 (an upstream regression), so RGW
multisite scenarios cannot start there. CI runs Squid with the role images
extracted from the same release; to use exactly those images, see
[Reproducing CI's role images](#reproducing-cis-role-images). Behaviour that
differs between releases is described in
[Releases and images](../guides/releases-and-images.md).

### `make scenario-*` targets and image variables

The `scenario-*` targets default to `SCENARIO_IMAGE_LAYOUT=all`, which unsets
the five role variables so the target runs `ceph.DefaultImage` even if you
exported `CEPH_TEST_IMAGE`. To run a scenario target with your own images, set
all five role variables and `SCENARIO_IMAGE_LAYOUT=roles`. A missing variable stops
Make before Go starts.

```sh
CEPH_TEST_ESSENTIAL_IMAGE=... CEPH_TEST_CLIENT_IMAGE=... CEPH_TEST_CEPHFS_IMAGE=... \
CEPH_TEST_RBD_AUX_IMAGE=... CEPH_TEST_RGW_IMAGE=... \
  make scenario-topology SCENARIO_IMAGE_LAYOUT=roles
```

### Reproducing CI's role images

CI runs Ceph batches with published role image digests from
`.github/ceph-role-images.json`. Resolve them
locally without running any test:

```sh
python3 .github/scripts/run_image_matrix.py --variant official --layout roles \
  --release 20.2.4 --prepare-only --output-dir artifacts/role-preparation
```

Use `--release 19.2.5` for Squid. The script pulls an image only when it is
missing, never builds one, and refuses an existing `--output-dir`. Its
`summary.json` lists the selected `CEPH_TEST_*` values under
`prepared_environment` as immutable local image IDs; export them and run a
test, the tag runner, or a `scenario-*` target with
`SCENARIO_IMAGE_LAYOUT=roles`. `--roles essential`, as for a batch with
`image-roles=essential`, resolves only the listed roles.

### Image compatibility checks

`make image-compatibility` runs nine representative tests with the selected
images: `TestClusterLifecycle`, `TestManagerLifecycle`, `TestRBDLifecycle`,
`TestCephFSFilesystem`, `TestRGWS3`, `TestMultiClusterRBDBackup`,
`TestMultiClusterRBDSnapshotMirror`,
`TestMultiClusterCephFSSnapshotMirrorAndBackup` and
`TestMultiClusterRGWMultisite`.

`make image-matrix` picks one published combination, pins the image IDs, and
runs the same nine tests:

```sh
make image-matrix                                    # official, all, native platform
make image-matrix IMAGE_VARIANT=ubuntu IMAGE_LAYOUT=all
make image-matrix IMAGE_VARIANT=debian IMAGE_LAYOUT=roles IMAGE_PLATFORM=linux/arm64
```

`IMAGE_VARIANT` is `official`, `debian` or `ubuntu`; `IMAGE_LAYOUT` is `all`
or `roles`. `IMAGE_PLATFORM` must match the Docker engine's native
architecture; an emulated run is not accepted as evidence for that platform.
The target always uses release 20.2.4; call `run_image_matrix.py` directly
with `--release 19.2.5` for Squid. Checking that an image satisfies the role
contract (files, libraries, object classes) belongs to the separate
ceph-testcontainers-images project, not to this suite.

## Make targets

Every Docker target accepts a timeout override, for example
`TOPOLOGY_TIMEOUT=90m make topology`.

| Target | What it runs | Timeout variable (default) |
| --- | --- | --- |
| `test`, `race`, `vet`, `tag-compile`, `check` | Host checks; see above | none |
| `test-all` | `-tags=all ./...`: every unit and normal integration test | `ALL_TEST_TIMEOUT` (180m) |
| `integration` | `-tags=integration`: tests that need no capability tag | `INTEGRATION_TIMEOUT` (20m) |
| `topology` | MON/MGR/MDS/RGW topology selection | `TOPOLOGY_TIMEOUT` (40m) |
| `topology-extensions` | Network separation, five MONs, mirror daemon and zonegroup topologies | `TOPOLOGY_EXTENSIONS_TIMEOUT` (90m) |
| `multicluster` | `^TestMultiCluster` with `integration,multicluster` | `MULTICLUSTER_TIMEOUT` (60m) |
| `hostnetwork`, `hostnetwork-multicluster` | `^TestHostNetwork` scenarios | `HOSTNETWORK_TIMEOUT` (40m) |
| `cluster-features`, `cluster-feature-extensions` | Pool, identity, subvolume, configuration and placement fixtures | `CLUSTER_FEATURES_TIMEOUT`, `CLUSTER_FEATURE_EXTENSIONS_TIMEOUT` (40m) |
| `client-fixtures`, `rgw-s3-fixtures`, `rgw-protocol-fixtures`, `rgw-admin-fixtures`, `rgw-sync-fixtures` | Client recipes. `client-fixtures` and `rgw-sync-fixtures` include known upstream limitations, so they do not fully pass on the default image. | `CLIENT_FIXTURES_TIMEOUT` (120m), `RGW_CLIENT_FIXTURES_TIMEOUT` (40m), `MULTICLUSTER_TIMEOUT` (60m) for `rgw-sync-fixtures` |
| `rgw-sync-native-regressions` | Strict RGW sync regressions (`native_regression`) | `MULTICLUSTER_TIMEOUT` |
| `scenario-*` | Named selections kept for manual diagnosis; see below | one per target |
| `image-compatibility`, `image-matrix` | The nine representative tests with chosen images | `IMAGE_COMPATIBILITY_TIMEOUT` (40m) |
| `goceph-linux`, `scenario-goceph-linux` | go-ceph consumer check with prepared images | set by the harness |

The `scenario-*` targets run fixed `-run` selectors. CI does not use them; it
discovers batches from build tags. Several targets take a case or group
variable to narrow the selection, for example:

```sh
make scenario-multicluster-topology SCENARIO_MULTICLUSTER_GROUP=rgw
make scenario-cephfs-removal SCENARIO_CEPHFS_REMOVAL_CASE=process-quiescence-bridge-peer
make scenario-cephfs-fixtures SCENARIO_CEPHFS_FIXTURE_CASE=pins
make scenario-rbd-fixtures SCENARIO_RBD_FIXTURE_CASE=failback
make scenario-topology-extensions SCENARIO_TOPOLOGY_EXTENSION_CASE=cephfs-mirror-host
make scenario-rgw-sync-fixtures SCENARIO_RGW_SYNC_GROUP=policy-owned-host
make scenario-rbd-receivers SCENARIO_RBD_RECEIVERS_CASE=host
```

The accepted values are the `*_TESTS_<value>` variables in the Makefile; an
unknown value stops Make with an error.

The go-ceph check needs two images that you prepare on the local engine (a
client with the probe under `/usr/local/bin/go-ceph-probe` and the native
libraries, and a runner with the `integration,goceph` test binary). The harness
never builds or pulls them, and it needs a Linux Docker engine or Docker
Desktop with host networking:

```sh
CEPH_TEST_GOCEPH_CLIENT_IMAGE=ceph-testcontainers-goceph:20.2.4-client \
CEPH_TEST_GOCEPH_RUNNER_IMAGE=ceph-testcontainers-goceph:20.2.4-runner \
make goceph-linux
```

The probe reports the go-ceph version that it was linked with, from its own
build information, and the harness reads the expected version from the probe
module's `go.mod`. A client image with a probe built from an older pin
therefore fails the identity check: rebuild the client image after a go-ceph
update.

## Writing a new integration test

### Files

A scenario normally lives in two files in `internal/integration`, both in
package `integration_test`.

1. A helper file, `<topic>_integration_helpers_test.go`, holds the scenario
   body as an ordinary function, such as
   `testMyFeature(t *testing.T, opts ...testcontainers.ContainerCustomizer)`,
   and any helpers it needs. Its constraint has no CI terms: use
   `//go:build all || integration`, or narrow it with a capability tag only,
   such as `//go:build all || (integration && multicluster)`. It has no
   `//ci:` line.
2. A wrapper file, `<topic>_integration_test.go`, contains only the `Test...`
   functions, the CI category and batch in its constraint, and exactly one
   `//ci:` line before the `package` clause.

```golang
//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_default))))

//ci: timeout=20m job-timeout=35

package integration_test

import (
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestMyFeature(t *testing.T) {
	parallelWhenEnabled(t)
	testMyFeature(t)
}

func TestHostNetworkMyFeature(t *testing.T) {
	parallelWhenEnabled(t)
	testMyFeature(t, ceph.WithHostNetwork())
}
```

The parts of the constraint have these roles:

- `all ||` puts the test into the normal suite. The planner rejects a required
  test that `all` does not compile.
- `integration` plus any capability tags (`features`, `topology`,
  `multicluster`, ...) select the test in capability runs such as
  `make integration` or `-tags=integration,features`.
- `!ci ||` keeps the file in those capability runs. Under `ci` the file
  compiles only for its own category tag `ci_<category>`.
- `(!ci_batch || ci_batch_<name>)` lets a category-wide compile include the
  file, while a batch run compiles it only for its own batch.

### Categories

| Category | Tag | Runs in workflow | Use it for |
| --- | --- | --- | --- |
| `code` | `ci_code` | Code checks | Docker-free helper tests in a Docker test package (for example the SigV4 signer) |
| `environment` | `ci_environment` | Docker checks, and Ceph periodic | Docker behaviour without Ceph |
| `short` | `ci_short` | Ceph short, and Ceph periodic on both releases | Basic bootstrap, services and client fixtures |
| `topology` | `ci_topology` | Ceph topology (changed batches on a pull request), and Ceph periodic on both releases | Cluster composition, node lifecycle, diagnostics |
| `multicluster` | `ci_multicluster` | Ceph multicluster (changed batches on a pull request), and Ceph periodic on both releases | Independent clusters, zones and peers |
| `recovery` | `ci_recovery` | Ceph recovery (changed batches on a pull request), and Ceph periodic on both releases | Faults, partitions, removal and later recovery |

`short` names a selection, not a time limit, and has nothing to do with Go's
`-short` flag. `short` runs on Tentacle for every pull request and push to
`main`. A pull request runs a batch of the other categories when it changes
that batch's own test files. Every category runs on both Tentacle and Squid
in Ceph periodic, so a test must pass on both releases (see
[ci.md](ci.md#pull-request-scope)).

### Batches and the `//ci:` line

- To join an existing batch, copy that batch's constraint and its `//ci:` line
  exactly. All files of one batch must declare the same budget; a mismatch is
  a planner error. The new test shares the batch's process timeout with the
  tests already there.
- To create a batch, choose a new `ci_batch_<name>` with a lowercase
  identifier (`[a-z][a-z0-9_]*`) and give the file a `//ci:` line. CI
  discovers the batch from the source. Do not edit workflows, matrices or the
  Makefile.
- The `//ci:` line has the form
  `//ci: timeout=<duration> job-timeout=<minutes>`. `timeout` is the Go
  `-timeout` for the whole batch process (`ms`, `s`, `m` or `h`, at most 6h).
  `job-timeout` is the GitHub job limit in minutes (1 to 360) and must be
  greater than `timeout`. Leave at least ten minutes between them: the job also
  checks out the code, sets up Go, compiles, pulls images and verifies
  cleanup.
- `checker=` and `case=` are reserved for the strict completion checkers of the
  CephFS process quiescence/recovery and RBD receiver batches. `case=` names
  the receipt kind (`peer` or `directory`); the receiver batch has none. The
  network mode comes from the test's `networkModes`.
- `image-roles=` lists the published role images that a Ceph batch needs,
  such as `image-roles=essential` for a batch that only starts core-image
  containers. Roles are `essential`, `client`, `cephfs`, `rbd-aux` and `rgw`,
  comma-separated, with `essential` required. Without a declaration CI
  conservatively prepares all five. Missing feature roles fail explicitly.
  Ordinary RADOS/RBD uses `essential,client`, CephFS adds `cephfs`, mirrors/cache
  add `rbd-aux`, and gateway paths add `rgw`. Keep native client commands in
  the toolkit and daemon-local sockets in their owning daemon.
- `representative=<theme>` puts the batch into the representative set that
  Ceph periodic's campaign repeats (see
  [ci.md](ci.md#representative-campaign)). The theme is one of `topology`,
  `auth`, `mirroring`, `recovery` and `flaky`, and the set must cover every
  theme. Like the budget, every file of the batch declares the same value.
  Choose short batches: each attempt is a separate job. Use `flaky` for a
  batch with a known intermittent failure that the campaign should keep
  measuring.
- The process timeout covers every test in the batch, each with its own
  cluster bootstrap and cleanup. When a batch approaches its budget, move a
  long, independent test into a batch of its own instead of raising the limit.
  Each test already creates its own cluster, so a split adds no bootstrap, but
  each extra batch repeats runner setup and image preparation. Keep short
  related tests together.

### Shared helpers must compile in every batch

CI compiles each batch alone, with only that batch's `ci_batch_<name>` tag. A
function defined in a file that belongs to batch A does not exist when batch B
is compiled, so a test in batch B that calls it breaks batch B's compilation.
`go vet`, `make tag-compile` and `-tags=all` builds compile every file at once
and do not notice.

Put any helper used from more than one file into a helper file whose
constraint has no CI terms, `//go:build all || integration` (narrowed with
capability tags at most). Every CI profile sets all capability tags, so such a
file compiles in every batch.

### Verify batch profiles before pushing

Run this before pushing any change that adds, moves or renames an integration
test file, or that makes a test call a helper from another file:

```sh
python3 .github/scripts/tag_scenarios.py plan --verify --output artifacts/tag-plan.json
```

It needs no Docker but compiles the test packages once per profile, so it
takes several minutes. It fails when:

- a test file is not in the `all` suite, has no category, has more than one
  category or batch, or belongs to two batches;
- a runtime test file has no `//ci:` line, more than one, or a malformed one,
  or a helper file has one;
- two files of the same batch declare different budgets;
- the `all` build, a category build or any single batch build does not
  compile, or its compiled test list differs from what the source declares.

The `code` subcommand of the same script runs only the `code` batch and does
not catch these problems. Code checks runs `plan --verify` for every pull
request and every push to `main`.

### Writing the test body

- Start clusters through `newServiceCluster(t, opts...)` or, for a custom
  first image, `integrationImages(t)` plus `newServiceClusterWithOptions`.
  These honour the image variables, start a client container connected with
  `cluster.WithClient()`, and register cleanup that prints the last 16 KB of
  every daemon's log when the test failed.
- When you call `Run` yourself, register cleanup as soon as it returns a
  non-nil container, even together with an error.
- Bound each phase with `context.WithTimeout(t.Context(), ...)`. Inside
  `t.Cleanup`, derive a fresh context from `context.Background()`:
  `t.Context()` is already cancelled when cleanup functions run. See
  [Contexts and cleanup](../guides/contexts-and-cleanup.md).
- Prove the effect with a real client: bytes read back, a request that is
  denied, a replica that arrives. A CLI exit code alone is not evidence.
  `execCommand(t, ctx, client, args...)` runs a command in the client
  container and fails the test on a non-zero exit. See
  [Native commands](../guides/native-commands.md).
- Add a `TestHostNetwork...` variant when the feature touches addresses,
  ports or client connectivity.
- Never call `t.Skip`. CI counts any skipped test or subtest as a failure.
  When a release does not support something, select the expectation from
  the shared table (see [Release differences](#release-differences)) and
  assert the classified refusal, never a release prefix of your own:

  ```golang
  if !capabilitySupported(t, ctx, cluster, capabilityRBDNamespaceMapping) {
  	requireCapabilityRefused(t, err, cluster, capabilityRBDNamespaceMapping, detail)
  	return
  }
  ```

- Opt into parallel runs with `parallelWhenEnabled(t)` as the first statement
  only if the test touches nothing but the clusters and containers it
  created. Do not opt in tests that list or count engine-wide Docker
  containers, networks, volumes or events, tests that need fixed host ports,
  or tests that call `t.Setenv`.
- To collect evidence on failure, call `cluster.CollectDiagnostics` from
  cleanup with a short background context.

### Optional tests

A test for a known upstream defect, which must fail on the unpatched default
image, is optional. It requires `native_regression` in its constraint, never
compiles under plain `all`, and uses the `ci_optional` category:

```golang
//go:build (all || (integration && multicluster)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_my_regression)))
```

It keeps strict assertions; there is no expected-failure mode. Run it with the
`optional` subcommand of the tag runner.

### Release differences

The same contract runs on every supported release (CI runs it on real
Tentacle 20.2.4 and Squid 19.2.5 images). Where the expected outcome differs,
the test selects it from one table, `releaseCapabilities` in
`internal/integration/release_capabilities_integration_helpers_test.go`, and
never from a release prefix of its own. `capabilitySupported` first checks
`ceph versions`: every daemon must run the release of the essential image.
It then answers from the row:

| Capability | Decided by | Where unsupported |
| --- | --- | --- |
| `rbd mirror pool enable --remote-namespace` | Probe: `rbd help mirror pool enable` in the client toolkit | The fixture refuses a differently named mapping with `ErrUnsupported` naming the cluster; there is no init-only mode |
| `messenger dump` | Probe: `ceph tell mon.<leader> help` | The native probes prove encryption from secure-only service modes and client `debug_ms` READY lines instead |
| RGW account principals in user-mode sync pipes | Release row: 19 unsupported, 20 supported. No tool reveals it, because Ceph 19 accepts the pipe and replicates nothing | The fixture refuses the pipe with `ErrUnsupported` naming the zone |

Unsupported cases assert the classified error with
`requireCapabilityRefused` and return; they do not skip. These outcomes are
explicit failures of the test configuration rather than guesses:

- A cluster whose daemons run several releases, for example a Squid RGW role
  image beside a Tentacle essential image, fails with `mixed-release cluster`.
- A version that does not parse fails with `unrecognized Ceph release`.
- A release-limited row has no entry for a new major release until someone
  observes it and adds the entry; until then it fails with
  `no expectation for Ceph <version>`. An unlisted patch of a listed major
  release, such as 20.2.9, uses that release's entry.

`TestReleaseCapabilityTable` (code category, `pure` batch) checks these rules
without a cluster.

Other release differences, kept out of the table on purpose:

- CephX key types: `ceph-authtool --key-type` is probed by the product
  (`detectKeyTypeOption`); the auth cipher tests pin the 20.2.3 image that
  lacks it, so their expectation follows the image they start.
- Product command forms: `ceph.CephBefore`/`multicluster.CephBefore` choose
  how to issue a command that both releases support, such as
  `fs new ... set max_mds` (CephFS), the empty user of a system-mode sync pipe
  and the legacy `bucket sync status` JSON (RGW), and the receiver discovery
  format of `rbd mirror status` (RBD). The fixture's own tests cover them;
  they select no test expectation.
- Release-tolerant assertions accept every observed form without a branch:
  the rotating-key log lines of daemon key rotation, the MON session after a
  rejected renewal (client key replacement) and when a CephFS client gets
  its GlobalID (session admission).

## Debugging a failing local run

- Read the `-v` output from the top. Tests log the phase they reached, and
  failed tests that use the shared cluster helpers print daemon log tails.
- `TestMultiClusterMonitorBootstrapRefresh` replaces every MON as named
  phases (`add d`, `remove a`, ...). A passing replacement logs one
  `MON_REPLACEMENT_RECEIPT` line per cluster: each phase's operation and
  quorum time, the monmap epoch the MONs report afterwards, and which MON held
  each warm MGR, OSD and MDS session (`mgr.x:a>b>d`, with `-` when no MON
  listed one). A failed phase is not retried. Before the test fails it logs a
  `MON_REPLACEMENT_BUNDLE` JSON line: the phase and partial MON handle, every
  MON's own `mon_status`, the warm daemons' MON sessions, status and MON client
  settings, the MON client lines (hunting, authentication, beacons, mounts) of
  every warm daemon, client and mirror container, and `CollectDiagnostics`
  output. The warm MGRs, OSDs and MDS daemons run with `debug_monc 10/10` from
  the start of the replacement to the end of the test. The MDS socket is
  reached with the `ceph` CLI in the MDS's own container; an MDS image without
  the CLI leaves the MDS observations marked incomplete. The recorder lists
  the owned MONs, MGRs and OSDs with `Monitors`, `Managers` and
  `OSDs`, each within one query bound. While another operation holds
  the topology, the bundle records `list monitors: ...` (or managers, osds)
  as incomplete instead of waiting, keeps the observations that need no
  membership, and leaves a debug window whose daemon it cannot identify
  `skipped: identity unavailable`. When a later step
  fails, for example the CephFS observation after the mirror restarts, the
  cleanup logs a bundle for the phase `after replacement` before the fixture
  is removed. Otherwise the cleanup restores the exact prior level on the same
  container generation and logs `MON_REPLACEMENT_DEBUG` with the result. The
  clients and both mirror daemons are stopped during the replacement, so they
  have no debug window.
- A Go timeout prints `panic: test timed out after ...` followed by the tests
  that were still running and for how long. Raise the `-timeout` or the Make
  timeout variable only after checking that the test was making progress.
- Ceph reports health and placement changes with a delay. Wait with the
  fixture's `Wait...` methods instead of reading state once (see
  [Ceph](../ceph.md)).
- To check that a run leaked nothing, record the labelled resources before and
  compare after. The checker never deletes anything and assumes that no other
  testcontainers-go session uses the same engine:

  ```sh
  python3 .github/scripts/check_runtime_cleanup.py snapshot --directory artifacts/cleanup-1
  # ... run the test ...
  python3 .github/scripts/check_runtime_cleanup.py check --directory artifacts/cleanup-1
  ```

  `check` waits up to 30 seconds for Ryuk, then fails if containers, networks
  or named volumes labelled `org.testcontainers=true` remain that were not in
  the snapshot. `docker ps -a --filter label=org.testcontainers=true` lists
  them.
- A CI failure is reproduced with the tag runner and the same images; see
  [Reading a failing batch](ci.md#reading-a-failing-batch).

## What "done" means for a feature

A server-side fixture feature counts as done only when all four hold:

1. A public typed API exists, or a documented, runnable combination of public
   APIs achieves it.
2. Tests check native server state and identity, not only that a command
   succeeded.
3. Tests check the real client effect: success, denial, failure and recovery,
   or replicated data, observed through a client.
4. Tests check that the original state is restored, or that owned resources
   and disposable clusters are cleaned up.

Some things do not close an item on their own: passing unit tests or tag
compilation, or the fact that `Ceph`, the `rbd`/`radosgw-admin` helpers or
`Exec` could run the commands. A combination of existing APIs is acceptable,
but it still needs a recipe and a real-cluster test. An implementation that
cannot survive a partial failure or a lost reply, or that adopts or deletes
resources it did not create, is not done.

Covering every client CRUD call with a Go wrapper is not a goal. The goal is
that a test can prepare the authentication, pools, namespaces, daemons,
modules and policies that a client needs, and then exercise the client
itself. Responsibilities are split by package: `ceph` owns cluster-wide
pools, principals, configuration and modules; `rbd`, `cephfs` and `rgw` own
service state inside a cluster and the peers, mappings, realms and replication
between clusters.

It is not necessary to rerun every Docker scenario after each change. Verify
the changed public path with a representative bridge and host-network run, and
rely on unit tests, the race detector, vet and `plan --verify` for the rest; CI
then runs the full set.

## Unit tests with fake containers

The product is disposable Ceph clusters for integration tests, so a
real-cluster test in `internal/integration` is the primary verification. If a
path can be reproduced on a real cluster, test it there.

Fake-container unit tests are optional (see [Test doubles](#test-doubles) for how to
choose a substitute). Use them for deterministic checks of
branches that a real cluster cannot produce on demand: the second of two
commands failing, a lost reply, a filesystem ID replaced under the same name,
a concurrent caller. When you write one:

- Model a small piece of Ceph state in the fake. For example, store what
  `fs set` changes and answer `fs get` from it, and refuse what Ceph refuses.
- Inject faults by condition, such as "the next run of this command fails",
  not by position in a script.
- Assert the final state and the kind of error. Do not assert the exact
  command strings or their order; such tests break on harmless refactors and
  still pass when real Ceph behaves differently.
- Parse tests use output captured from a real cluster, not hand-written JSON.

## Test doubles

This section is the policy for choosing a substitute in a test. The
[test-double inventory](test-double-inventory.md) lists what exists today. The
rules for the test-only import, seam and declaration boundary are in
[Test-only code](#test-only-code), and they apply to every kind of substitute.

Most of this project is written by AI and reviewed by a person. Write a test so
that the reader can see what it exercises and what it proves. A substitute that
saves the writer effort but hides the behavior under test is the wrong choice.

### Choosing a substitute

1. Prefer the real implementation, or a real cluster in `internal/integration`,
   when the behavior can be reproduced there.
2. Otherwise prefer a small fake: a type whose limited behavior and state
   changes are visible in the test or in one short declaration next to it.
3. Use a mock (a substitute that is configured with expected calls and canned
   replies, by hand or generated) only when the substituted dependency, the
   configured behavior and the purpose of the check are short and explicit at
   the reading site of the test, and the result is easier to review than the
   real implementation or a small fake. Mocks are allowed under that
   condition. They are not the default.

A fake is an option, not an automatic replacement for a mock. Do not grow a
parallel implementation of Ceph or Docker semantics. If understanding a fake
means studying a large shared simulation, change the boundary between the code
and its dependency instead. A fake's behavior is a claim made by the test, not
a fact about Ceph.

### What a good test double check looks like

- Good: inject one known external-call failure and check how the error
  propagates. The substituted call and the expected result are apparent.
- Good: check that a refused operation issued no native command, or that an
  uncertain write was not replayed. Here the interaction is the contract, so
  asserting it is right.
- Avoid: a long sequence of expected calls, argument matching and canned replies
  that rebuilds a whole normal scenario. It repeats the implementation's
  assumptions and hides whether observable behavior is tested.
- Avoid: asserting call order or count when it is only an implementation
  detail. Assert the observable outcome instead.
- Do not set a line-count limit and do not move a mock's setup into distant
  helpers to make it look short. The reader must understand what is substituted
  without chasing hidden expectation machinery.

### Evidence

A passing test with a fake or mock is unit or control evidence for the logic
under test. It does not show that real Ceph, Docker or a native client behaves
the same way. Do not word a result, a pull request or a document so that it
does. Native interoperability is shown only by a run on a real cluster.

### Names and availability

A concrete double is named for its role (a fake models state, a recorder
records calls, a counter counts them), not like the real thing it replaces. A
double that more than one package shares lives in a test-only package under
`internal/`, and a double of one package is declared in a `_test.go` file. No
double or test accessor is exported from a public package. Add the declaration
to the [inventory](test-double-inventory.md) in the same change when it is in a
test-only package or when a runtime file gets a new injection seam whose
comment says it is replaced in tests; `go run ./internal/tools/testdoublecheck`
(part of `make tag-compile`) fails otherwise.

## What features may rely on in images

Features may rely only on what the official `quay.io/ceph/ceph` image ships
for the release in use, split into the role contract that the
ceph-testcontainers-images project defines (`docs/ROLES.md` there is the
authority):

| Role | Must provide |
| --- | --- |
| `essential` | MON, MGR, OSD, the `ceph` CLI, keyring and monmap bootstrap, process inspection, the always-on MGR modules and `volumes`, `rbd_support`, `mirroring` and `snap_schedule`, the OSD classes, compressor and erasure-code plugins |
| `client` | `ceph`, `rados`, `rbd`, offline CRUSH and OSD map tools, the Python `rados`/`rbd`/`cephfs` bindings, `libradosstriper`, librbd encryption with `cryptsetup` |
| `cephfs` | MDS and `cephfs-mirror` |
| `rbd-aux` | `rbd-mirror`, the immutable object cache and the librbd parent cache plugin |
| `rgw` | `radosgw`, `radosgw-admin`, `readlink` and a `ceph`/Python runtime for per-zone setup containers |
| `all` | The union of the five, selected explicitly |

The control container is not a role. It is an executor created from the
essential image, and it runs the `ceph` CLI; `rados` and `rbd` run in the client
toolkit and `radosgw-admin` in the rgw role. No optional role falls back to
essential.

Consequences for contributors:

- Do not require tools the official image lacks. It has no `tc`, `ip`,
  `iptables` or `nft`, so network faults are injected with Ceph's own
  messenger options or Docker network operations, not with netem or packet
  filters (see [Fault injection](../guides/fault-injection.md)). There is no
  reliance on an external `timeout` executable either; command time limits
  are enforced from Python in the control container.
- Server images must not need compilers, development headers, go-ceph, kernel
  mount tools or a Vault server. Those belong to separate client or service
  containers that a test starts itself.
- The module supplies the FSID, keys, configuration, daemon IDs, bootstrap
  scripts and entrypoints at run time. Images carry no cluster setup, and the
  code must not depend on a distribution, package names or image labels.
- The rgw role carries the runtime that RGW setup needs; the gateway process
  itself needs only the shell and core utilities. Configuration that needs the
  `ceph` CLI or Python runs in the control container or a client-role container.

## Module conventions

Keep the conventions of the upstream testcontainers-go modules:

- Each package exposes a public type named `Container` that embeds
  `testcontainers.Container`. `ceph.Container` overrides `Terminate` so that
  terminating it removes the whole cluster.
- The entry point is
  `Run(ctx context.Context, config ceph.RunOptions, customizers ...testcontainers.ContainerCustomizer) (*Container, error)`
  (`RunOptions` in the `ceph` package itself).
  The service packages' `Run` returns a `Container` that embeds
  `*ceph.Container`. `Run` applies the module defaults first, then the
  caller's options.
- When `Run` fails after creating containers, it returns the partly created
  `Container` together with the error, so the caller can terminate it.
- Option functions are named `With...` and return a concrete type: the
  package's `Option` for settings outside the container request, or
  `testcontainers.CustomizeRequestOption` for request changes. They do not
  return the `ContainerCustomizer` interface. Options report invalid input as
  an error, and `Run` returns that error; they never panic.
- Runnable examples are `Example...` functions in the external `<pkg>_test`
  package, in `examples_test.go` as in the upstream modules.
- Production code imports no client SDK: no go-ceph, no cgo, no host librados,
  no AWS SDK. SDKs and their native libraries are allowed only in tests,
  examples and separate consumer modules, such as the go-ceph probe, which has
  its own `go.mod`.

## Design rationale

These decisions explain the shape of the code and should guide new features.

- **Container-local CLIs instead of host librados.** The module controls Ceph
  by running the official CLIs (`ceph`, `rbd`, `rados`, `radosgw-admin`) and
  reading files inside containers through the Docker API. This keeps the Go
  module free of cgo and native Ceph libraries, so it builds with
  `CGO_ENABLED=0` on any host, including macOS and Windows. It also matches
  how Ceph clients work: a client learns OSD addresses from the MON and then
  connects to every OSD directly, so a single published MON port is not
  enough for a host process anyway. Native clients run in Linux containers on
  the cluster network (`WithClient`), or on the host network.
- **Daemons started directly, one per container.** MON, MGR, OSD, MDS and RGW
  run in the foreground from the official image, each in its own container.
  cephadm is not used, because it assumes systemd, LVM, time synchronisation
  and host management that a small test fixture should not need. This also
  makes daemon-level faults (stop, pause, remove, replace) natural.
- **A stable control container.** Administrative commands run in the control
  container. In the default single-MON cluster that is the MON itself; with
  several MONs it is a separate CLI container, so the fixture stays
  manageable while individual MONs are stopped, removed or replaced.
- **File-backed BlueStore.** Each OSD stores data in a sparse file inside its
  container. Daemons need no privileged mode, host devices, bind mounts or
  Docker socket. Disk failures, real failure domains, LVM and production
  performance or durability are outside what this fixture can show.
- **Operations and checks, not an operations SDK.** The public API prepares,
  changes and restores test conditions (operations) and reads native state for
  assertions (checks). Object, image and file I/O belongs to the client under
  test. Kernel RBD mapping, kernel or FUSE CephFS mounts, NFS, SMB, NVMe-oF and
  cephadm/systemd orchestration are out of scope.
- **Small-cluster defaults are test settings.** Placement-group counts,
  disabled autoscaling and small replica counts make tiny clusters reach
  `active+clean` quickly. They are not recommendations for production.

## Package layout and the export inventory

Each public package holds its own implementation. The rbd, cephfs and rgw
packages reach the parts of a cluster that are not public API, such as its
mutex, owned service containers and Run-time provisioning, through
`internal/servicehook`, and keep their per-cluster state in an extension
registry on the cluster. Helpers that both sides share live in
`internal/clusterutil`, and the CLI fakes and lock assertions that their unit
tests share live in `internal/clustertest`. Cross-cluster fixtures (RBD
mirroring, CephFS mirroring, RGW multisite) share resource tracking from
`internal/multicluster`.

`internal/apiinventory` checks that each package page in `docs` names every
exported identifier and method of its package. Mention a new export on the
package's page, usually under its own heading, in the same change.

The same tests check the package-scoped API catalogues in
`docs/design/api-naming/<package>/`. A public package is discovered from the
source tree and needs `declarations.json`, `members.json` and `review.json`
there; no shared index, count or hash file exists. After a public API change in
a package, do two separate things and review each diff on its own:

```sh
go run ./internal/tools/apinaming regenerate <package>   # writes declarations.json and members.json only
go run ./internal/tools/apinaming approve <package>      # writes review.json only
```

Regenerating never approves: a package whose catalogue changed but whose
receipt was not approved fails `TestNamingReviewBindsCurrentSurface`. Approval
refuses a stale catalogue or a pending rename proposal. A change in one package
makes only that package's receipt stale, so pull requests on different packages
touch disjoint files. `go run ./internal/tools/apinaming aggregate` prints the
per-package counts and totals on demand; they are not committed. CI runs
neither command, only the tests and the move check below.

#### Merging `main` into a branch that conflicts in these files

Two pull requests that change the same package's API both rewrite its
`declarations.json` and `review.json` (and `members.json` for members), so Git
reports a content conflict even when their API changes are independent. After
`git merge origin/main`, resolve the Go sources first. Then, instead of editing
the three JSON files by hand:

```sh
go run ./internal/tools/apinaming resolve            # packages that both parents changed
go run ./internal/tools/apinaming resolve <package>  # or name them
```

`resolve` reads the three committed states from Git (the merge base and the two
parents, `HEAD` and `MERGE_HEAD` by default; `-base`, `-ours` and `-theirs`
override them), so conflict markers in the working tree do not matter. It
rewrites the three files of a package, receipt included, only when all of these
hold:

- each parent's receipt pins that parent's own committed catalogue bytes and
  counts, so both parents are approved states;
- the three-way merge of the catalogue rows (one row per declaration, keyed by its
  name, and one per member, keyed by its type and member path with the full
  row value, so a removal against an edit of the same member conflicts) has no row that both parents changed differently, and no
  receipt field (`review`, `page`) that both changed differently;
- the catalogue derived from the merged source consists of exactly those merged
  rows: no declaration or member that neither approved parent contains, and none
  that both keep missing; no pending rename proposal remains.

That is the whole claim: the merge introduced no API row that a reviewer has not
already approved on one of the two sides. It does not review whether the two
changes work together; the merged source still has to compile and pass the
tests. When any condition fails, `resolve` writes nothing, names the rows, and
the usual `regenerate` followed by a separate `approve` (a human review of the
new combination) remains the path. Commit the result with the merge.

A change that moves public API from one package to another (or deletes or
introduces a declaration row) also adds a move record,
`docs/design/api-naming/moves/<change-id>.json`, in the same pull request. The
file name is `<change-id>.json` with the lowercase `a-z0-9-` identifier of the
change; `declarations`, `members` and `review` are reserved.

```json
{
  "schema": "ceph-api-move/v1",
  "change": "156-example-move",
  "review": "api-naming-audit.md",
  "moves": [{"from": "cephfs.MirrorConfig", "to": "cephfs/mirror.Config"}],
  "removed": ["pkg.DroppedName"],
  "added": ["pkg.NewName"]
}
```

`moves` pairs a declaration row (its catalogue `current` value) with the row
that replaces it in another package. `removed` and `added` list rows that were
deleted or introduced on purpose. `review` is a note below `docs/design`; it
must mention the change identifier, and the receipts of every package the
record names must carry it as their `review`. Run the check:

```sh
go run ./internal/tools/apinaming moves -base origin/main
```

It prints `OK`, `FAILED` with one line per problem, or `SKIPPED`. Without
`-base`, or when the base is not in the clone, or when HEAD has no merge base
with it, the comparison cannot run: the result is `SKIPPED` with the reason,
never `OK`, and only the record checks have run (`-require-base` makes it a
failure). The base is resolved to its merge base with `HEAD` and compared
with the working tree. Every row removed from a package catalogue since then
must be the `from` of a move or listed in `removed`, and every added row the
`to` of a move or listed in `added`. It fails on:

- an unaccounted removal or addition, and a record entry that names a row that
  was not removed (or still exists), was not in the base, already existed, or is
  missing from the head catalogue;
- a partial move: members of a moved type that did not follow it, a different
  member count, a different declaration kind, or a move inside one package;
- a name claimed by two records or twice in one record, a record with an
  unknown field, trailing data, a wrong `change`, or a review note that does
  not mention it;
- a changed package whose receipt equals the base's, does not bind the head
  catalogue bytes, is missing, or names another note than the record;
- a changed catalogue with no new or changed release-note fragment under
  `docs/changes` (a fragment of kind `none` with a reason is enough);
- a record or fragment path that is not exactly the expected name (a trailing
  newline, a symbolic link) and a base revision that is not a plain revision
  name.

Only records that are new or changed since the base are compared with the
catalogue difference. A merged record is history and is not judged against
later API changes. Member rows inside a type that stays in its package are
covered by the receipt review, not by this check. In CI the check runs on pull
requests only, in `Vet, compile and CI tooling`; see
[CI](ci.md#documentation-only-changes). Its tests build temporary Git
repositories and need no Docker:
`go test ./internal/apiinventory/moves/`.
