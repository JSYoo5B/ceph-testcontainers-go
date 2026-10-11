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

- Go 1.25 or later.
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
make tag-compile  # compile every tagged test file without running it
make check        # all four of the above; never starts Docker
make lint         # golangci-lint v2.14.0 with .golangci.yml
```

`make lint` needs golangci-lint installed. Its configuration enables every
build tag, so it also checks the integration and go-ceph files, and the Code
checks workflow runs the same version.

`make test`, `make race` and `make vet` see only untagged files.
`make tag-compile` builds the test packages with `-tags=all` and with
`-tags=all,goceph,native_regression` and runs nothing (`-run '^$'`), so every
integration file is at least compiled. It compiles all files together, so it
cannot catch a helper that only another CI batch compiles. Use
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
```

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

The integration tests read these variables:

| Variable | Default | Effect |
| --- | --- | --- |
| `CEPH_TEST_IMAGE` | `ceph.DefaultImage` | Control image: MON, MGR, the fixture's CLI container, mirror daemons, RGW multisite setup clients, and the test's own client container. Also the default for every role below. |
| `CEPH_TEST_OSD_IMAGE` | `CEPH_TEST_IMAGE` | OSD daemons (`ceph.WithOSDImage`). |
| `CEPH_TEST_RGW_IMAGE` | `CEPH_TEST_IMAGE` | RGW gateways (`rgw.WithImage`, `rgw.RunMultisite`, `rgw.RunTopology`). |
| `CEPH_TEST_MDS_IMAGE` | `CEPH_TEST_IMAGE` | MDS daemons (`cephfs.WithMDSImage`). |
| `CEPH_TEST_RBD_CLIENT_IMAGE` | control image | Alternative native RBD client for `TestRBDClientFeatures`. The tag runner and the fixture Make targets unset it, so the control image itself is verified. |
| `CEPH_TEST_VAULT_IMAGE` | `hashicorp/vault:1.21.4` | Vault server for the RGW SSE-KMS backend tests. |
| `CEPH_TEST_PARALLEL` | unset (sequential) | Concurrency limit for opted-in tests. |
| `CEPH_TEST_HOST_TCP_REQUIRED` | unset | `1` makes the host-network tests require direct TCP reachability of every advertised MON/MGR/OSD port from the test process. |
| `CEPH_TEST_HOST_HTTP_REQUIRED` | unset | `1` makes `TestHostNetworkRGWEndpoints` fail, instead of logging, when the test process cannot reach the host-network RGW over HTTP. |
| `CEPH_TEST_GOCEPH_CLIENT_IMAGE` | none | Prepared Linux image with the go-ceph probe, for `TestGoCephLinux`. |
| `CEPH_TEST_GOCEPH_RUNNER_IMAGE` | none | Prepared runner image for `make goceph-linux` (read by the harness, not by Go). |

Because the control image also runs the client container, it must provide the
Python `rados`, `rbd` and `cephfs` bindings and the `rbd`/`rados` CLIs. A
control-only image therefore needs the role overrides as well. Tests never
build images or install packages; testcontainers-go pulls a reference that is
not present locally.

```sh
# One image for every role
CEPH_TEST_IMAGE=registry.example.com/ceph:dev CGO_ENABLED=0 \
  go test -tags=all -count=1 -v -timeout=30m -run '^TestRBDLifecycle$' ./internal/integration

# Separate role images
CEPH_TEST_IMAGE=registry.example.com/ceph-control:dev \
CEPH_TEST_OSD_IMAGE=registry.example.com/ceph-osd:dev \
CEPH_TEST_RGW_IMAGE=registry.example.com/ceph-rgw:dev \
CEPH_TEST_MDS_IMAGE=registry.example.com/ceph-mds:dev \
  make image-compatibility
```

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
the four role variables so the target runs `ceph.DefaultImage` even if you
exported `CEPH_TEST_IMAGE`. To run a scenario target with your own images, set
all four variables and `SCENARIO_IMAGE_LAYOUT=roles`. A missing variable stops
Make before Go starts.

```sh
CEPH_TEST_IMAGE=... CEPH_TEST_OSD_IMAGE=... CEPH_TEST_RGW_IMAGE=... CEPH_TEST_MDS_IMAGE=... \
  make scenario-topology SCENARIO_IMAGE_LAYOUT=roles
```

### Reproducing CI's role images

CI runs Ceph batches with the published `official-<release>-{control,osd,rgw,mds}`
role images from `ghcr.io/jsyoo5b/ceph-testcontainers-images`. Resolve them
locally without running any test:

```sh
python3 .github/scripts/run_image_matrix.py --variant official --layout roles \
  --release 20.2.4 --prepare-only --output-dir artifacts/role-preparation
```

Use `--release 19.2.5` for Squid. The script pulls an image only when it is
missing, never builds one, and refuses an existing `--output-dir`. Its
`summary.json` lists the four `CEPH_TEST_*` values under
`prepared_environment` as immutable local image IDs; export them and run a
test, the tag runner, or a `scenario-*` target with
`SCENARIO_IMAGE_LAYOUT=roles`.

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
| `environment` | `ci_environment` | Docker checks | Docker behaviour without Ceph |
| `short` | `ci_short` | Ceph short and Ceph squid | Basic bootstrap, services and client fixtures |
| `topology` | `ci_topology` | Ceph topology | Cluster composition, node lifecycle, diagnostics |
| `multicluster` | `ci_multicluster` | Ceph multicluster | Independent clusters, zones and peers |
| `recovery` | `ci_recovery` | Ceph recovery | Faults, partitions, removal and later recovery |

`short` names a selection, not a time limit, and has nothing to do with Go's
`-short` flag. It runs on every push for both Tentacle and Squid, so a test
placed there must pass on both releases. Topology, multicluster and recovery
batches run on Squid weekly (see [ci.md](ci.md#squid-workflows)).

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
  CephFS process quiescence/recovery and RBD receiver leaf batches.
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
  When a release does not support something, branch on `cluster.CephVersion()`
  and assert the documented behaviour, typically that the fixture refuses the
  request before it changes anything:

  ```golang
  if strings.HasPrefix(cluster.CephVersion(), "19.") {
  	if err == nil {
  		t.Fatal("Squid accepted a mapping that needs Ceph 20")
  	}
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

## Debugging a failing local run

- Read the `-v` output from the top. Tests log the phase they reached, and
  failed tests that use the shared cluster helpers print daemon log tails.
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

Fake-container unit tests are optional. Use them for deterministic checks of
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

## What features may rely on in images

Features may rely only on what the official `quay.io/ceph/ceph` image ships
for the release in use, split into the role contract that the
ceph-testcontainers-images project defines:

| Role | Must provide |
| --- | --- |
| `control` | MON, MGR, the `ceph`, `rbd`, `rados` and `radosgw-admin` CLIs, Python `rados`/`rbd`/`cephfs` bindings, `rbd-mirror` and `cephfs-mirror`, librbd encryption with `cryptsetup`, `rados --striper` |
| `osd` | OSD daemon and object classes including `hello` and `lock` |
| `rgw` | `radosgw`, `radosgw-admin`, `readlink` |
| `mds` | MDS daemon |
| `all` | The union of the four |

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
- RGW images need no Python or `ceph` CLI. Configuration that needs those runs
  in a control-image container.

## Module conventions

Keep the conventions of the upstream testcontainers-go modules:

- Each package exposes a public type named `Container` that embeds
  `testcontainers.Container`. `ceph.Container` overrides `Terminate` so that
  terminating it removes the whole cluster.
- The entry point is
  `Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Container, error)`.
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
