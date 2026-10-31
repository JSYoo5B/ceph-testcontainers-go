# Testing

This page explains how the test suite is organised, how to run it locally and
how to add tests that CI picks up. What the workflows do with these tests is
described in [Continuous integration](ci.md).

## How the suite is organised

| Layer | Location | Needs Docker | Selected by |
| --- | --- | --- | --- |
| Unit tests | Next to the code, no build tags | No | `go test ./...` |
| Package examples | `examples_test.go` in `ceph`, `rbd`, `cephfs`, `rgw` | No (Docker examples are only compiled) | `go test ./...` |
| Ceph integration tests | `internal/integration` and its `rbd`, `cephfs` and `rgw` test packages | Yes, plus Ceph images | Build tags |
| Docker environment tests | `internal/dockerbridge`, `*_integration_test.go` | Docker only | Build tags |
| CI tooling tests | `.github/scripts/test_*.py` | No | `python3 -m unittest` |
| go-ceph consumer check | `internal/integration/goceph` | Yes, prepared Linux images | `goceph` tag and a harness |

Integration tests are excluded by build tags, never by `t.Skip`. A plain
`go test ./...` never starts a container, and a tagged run without Docker
fails instead of passing. An `Example...` that starts a cluster has no
`// Output:` comment, so it is compiled but not run.

## Prerequisites

- Go 1.26 or later. `go.mod`'s `toolchain go1.26.9` line switches older 1.26
  patches to 1.26.9, which `make vulncheck` needs.
- A Docker engine for Linux containers that testcontainers-go can reach, with
  pull access to `quay.io/ceph/ceph` and the Ryuk image.
- A C toolchain for `make race` only. Every other target builds with
  `CGO_ENABLED=0`.
- Python 3.9 or later for `.github/scripts`.
- The suite is sized for a 4 GiB Docker VM. Tests run one at a time unless
  `CEPH_TEST_PARALLEL` is set (see
  [Running tests in parallel](#running-tests-in-parallel)).

If testcontainers-go cannot find the Docker Desktop socket:

```sh
export DOCKER_HOST="unix://${HOME}/.docker/run/docker.sock"
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
```

`TestHostNetwork...` variants need a Linux Docker engine, or Docker Desktop
4.34 or later with host networking enabled. Otherwise `Run` returns an error
wrapping `ceph.ErrHostNetworkUnavailable` and the test fails. When the test
process runs inside a container, set `TESTCONTAINERS_HOST_OVERRIDE` to the
Docker host address (CI uses `127.0.0.1`). See
[Host network](../guides/host-network.md).

The Docker Desktop VM kernel decides a few more tests. It is the kernel of the
Linux VM, not of macOS, and its configuration is readable from any container
in `/proc/config.gz`. Observed with kernel 7.0.14-linuxkit (arm64):

| Need | In the Desktop kernel | Effect on the suite |
| --- | --- | --- |
| netem, `prio`, u32 | built in | network fault tests run |
| loop, device-mapper `linear` and `error` | built in | `TestBlockDeviceFault` runs its attachment, error and recovery windows; `TestBlockDeviceControlSupervision` runs entirely |
| device-mapper `delay` and `flakey` | not built | the delay window of `TestBlockDeviceFault` is recorded as not eligible; it runs on GitHub-hosted runners |
| FUSE | built in | `TestCephFSFuseMount` runs |
| NBD | built in | `TestDesktopRBDOverNBD` (optional batch `desktop_capabilities`) maps an RBD image through `rbd-nbd` |
| kernel RBD and kernel CephFS clients | not built | the module offers neither; clients use librbd and libcephfs |

No test or fixture operation loads a kernel module or changes a Desktop
setting. Building a module for the Desktop kernel needs matching kernel headers,
which are not published for every Desktop release; it is not a supported
setup.

## Host checks: unit tests, race detector, vet

```sh
make test         # CGO_ENABLED=0 go test -mod=readonly ./...
make race         # CGO_ENABLED=1 go test -mod=readonly -race ./...
make vet          # CGO_ENABLED=0 go vet -mod=readonly ./...
make tag-compile  # test-only-code and test-double checks, then compile every tagged test file
make check        # all four of the above; never starts Docker
make lint         # golangci-lint with .golangci.yml (all build tags enabled)
make vulncheck    # govulncheck on every scanned platform and tag set
```

`make test`, `make race` and `make vet` see only untagged files.
`make tag-compile` compiles (`-run '^$'`, nothing executes) the module with
`integration` alone, so shared helpers cannot require an unrelated capability
tag, and then the `all` union and the optional unions. It cannot check every
CI batch's helper dependencies; use
[`plan --verify`](#verify-batch-profiles-before-pushing) for that. See
[Vulnerability check](ci.md#vulnerability-check) for the `vulncheck` policy.

The CI tooling tests:

```sh
python3 -m unittest discover -s .github/scripts -p 'test_*.py' -v
python3 -m unittest discover -s internal/integration/goceph -p test_run.py -v
```

## Build tags

| Tag | Selects |
| --- | --- |
| `integration` | Base tag of every Ceph integration file. Alone, the tests that need no capability tag. |
| `features` | Server fixtures and client recipes. |
| `auth` | Client identity and Cephx capability tests. |
| `topology` | MON/MGR/MDS/RGW composition and node lifecycle. |
| `multicluster` | RBD/CephFS mirroring and RGW multisite between clusters. |
| `hostnetwork` | Host-network-only scenarios. |
| `diagnostics` | Diagnostics collection. |
| `all` | The whole normal suite, regardless of capability or CI tags. |
| `native_regression` | Strict tests for known upstream Ceph defects. Not part of `all`. |
| `goceph` | The Linux go-ceph consumer test. Not part of `all`. |
| `ci`, `ci_<category>`, `ci_batch`, `ci_batch_<name>`, `ci_optional`, `ci_sdk` | Set only by the CI runner. Do not pass them by hand. |

Every constraint begins with `all || (...)`, so `all` overrides capability and
CI tags: `-tags=all,ci,ci_short` filters nothing. To run one batch, use the
[tag runner](#one-ci-batch-exactly-as-ci-runs-it). The `all` tag is unrelated
to the Ceph `all` role image.

`ceph_bug_` tags for [bug-ID compatibility](../guides/bug-compatibility.md) are not test
selectors: `all` never selects them, and the CI runner takes them only through
an explicit `--bug-compatibility` selection (see
[Bug compatibility selection](ci.md#bug-compatibility-selection)).

## Test-only code

Test-only code is bounded by imports, one seam and declaration rules, not by a
build tag. It stays ordinary untagged source that `go build ./...` compiles.

- Code that only one package's tests use is a `_test.go` file of that package.
- Code that tests of several packages share (a synthetic `ceph.Container`,
  lock and registry access, fake CLIs, accessor wrappers) lives in
  `internal/testhelper`, `internal/clustertest` or `internal/clusteraccess`.
  Only `_test.go` files import them.
- Unexported `ceph` state is reachable only through `ceph/unit_access.go`,
  which sets `hookreg.Test` to an unexported `unitAccess` value.
  `internal/testhelper` is its only caller.
- No public package has a `Test*` function or method, or a test-config or
  test-state type, in a non-test file.

`make tag-compile` enforces this with
`go run ./internal/tools/testsupportcheck`, driven by
`internal/tools/testsupportcheck/manifest.json`. Add a new shared test helper
package to that manifest in the same change. The next step,
`go run ./internal/tools/testdoublecheck`, requires each exported declaration
of the package to carry a `//testdouble:<class>` directive (see the
[test-double inventory](test-double-inventory.md)).

The check reads source and `go list` metadata only. It limits who imports the
helpers and who reads the seam; it makes no claim about which symbols a linked
binary contains.

## Running integration tests

Always pass `-count=1` to Docker-backed runs.

### One test

`-tags=all` compiles every normal test:

```sh
CGO_ENABLED=0 go test -tags=all -count=1 -v -timeout=30m \
  -run '^TestRGWS3$' ./internal/integration/rgw
CGO_ENABLED=0 go test -tags=all -count=1 -v -timeout=90m \
  -run '^TestMultiClusterRBDReceiverReadiness$/^bridge$' ./internal/integration/rbd
```

Optional tests also need their opt-in tag, for example
`-tags=all,native_regression`. A test that declares network modes runs in each
of them; `CEPH_TEST_NETWORK=bridge` (or another mode) selects one (see
[Network modes](ci.md#network-modes)).

### The whole suite

```sh
make test-all                       # -tags=all -count=1 -v -timeout=180m ./...
ALL_TEST_TIMEOUT=6h make test-all
```

It runs every scenario sequentially and takes hours. Prefer one test, a
[Make target](#make-targets) or one CI batch.

### One CI batch, exactly as CI runs it

List the batches and their budgets without Docker, then run one entry:

```sh
python3 .github/scripts/tag_scenarios.py plan --output artifacts/tag-plan.json
python3 .github/scripts/tag_scenarios.py run \
  --category short --batch default --package ./internal/integration \
  --directory artifacts/local-short-default
```

`run` compiles the batch profile, checks the compiled test list against the
source, runs the batch with its timeout and writes `native.log` and
`report.json` into `--directory`, which must not exist yet. It applies the CI
pass rules ([How a batch passes](ci.md#how-a-batch-passes)), fixes
`CGO_ENABLED=0`, `GOWORK=off` and `GOFLAGS=-mod=readonly`, unsets
`CEPH_TEST_RBD_CLIENT_IMAGE` and passes every other variable through.
`artifacts/` is git-ignored.

Optional batches use the `optional` subcommand:

```sh
python3 .github/scripts/tag_scenarios.py optional \
  --batch native_rgw_translation --package ./internal/integration/rgw \
  --directory artifacts/local-native-rgw
```

Other optional batches include `native_shuffle` and `cephfs_fuse`. The native
batches assert behaviour that the unpatched default image lacks, so they fail
there; they exist to verify a patched image. `cephfs_fuse` needs `/dev/fuse`,
`CAP_SYS_ADMIN` and `apparmor=unconfined`, which only rootful native Linux
Docker Engine allows (see [Native regressions](ci.md#native-regressions)).

### Running tests in parallel

Tests whose first statement is `harness.ParallelWhenEnabled(t)` run concurrently when
`CEPH_TEST_PARALLEL` is an integer of 2 or more; at most that many run at once,
each holding its slot until its cluster is terminated. Unset or `1` keeps every
test sequential. Any other value fails the test. CI uses `2`.

## Choosing images

CI supplies image IDs through `CEPH_TEST_ESSENTIAL_IMAGE`,
`CEPH_TEST_CLIENT_IMAGE`, `CEPH_TEST_CEPHFS_IMAGE`, `CEPH_TEST_RBD_AUX_IMAGE`
and `CEPH_TEST_RGW_IMAGE`, only for the roles the batch declares. A missing
selected role never falls back to essential.

Without role variables a local run uses `ceph.DefaultImage` for every role, or
the aggregate image in `CEPH_TEST_IMAGE`. For split images, set essential and
the roles the test needs:

```sh
CEPH_TEST_ESSENTIAL_IMAGE=registry.example.com/ceph:essential \
CEPH_TEST_CLIENT_IMAGE=registry.example.com/ceph:client \
  CGO_ENABLED=0 go test -tags=all -count=1 -v -run '^TestRBDLifecycle$' ./internal/integration/rbd
```

Tentacle 20.2.x is the default. For Squid, set
`CEPH_TEST_IMAGE=quay.io/ceph/ceph:v19.2.5`. Do not use 19.2.6: its master
gateway rejects the secondary zone's `radosgw-admin realm pull` with HTTP 403,
so RGW multisite scenarios cannot start. See
[Releases and images](../guides/releases-and-images.md).

`scenario-default` and `scenario` default to `SCENARIO_IMAGE_LAYOUT=all`, which unsets
the role variables and runs `ceph.DefaultImage` even if `CEPH_TEST_IMAGE` is
exported. To use your own images, set all five role variables and
`SCENARIO_IMAGE_LAYOUT=roles`; a missing variable stops Make.

### Reproducing CI's role images

CI uses the role image digests in `.github/ceph-role-images.json`. Resolve
them locally without running a test:

```sh
python3 .github/scripts/run_image_matrix.py --variant official --layout roles \
  --release 20.2.4 --prepare-only --output-dir artifacts/role-preparation
```

Use `--release 19.2.5` for Squid, and `--roles essential` (or another list) to
resolve only some roles. The script pulls only missing images, never builds,
and refuses an existing `--output-dir`. `summary.json` lists the `CEPH_TEST_*`
values under `prepared_environment`; export them for a test, the tag runner or
`make scenario` with `SCENARIO_IMAGE_LAYOUT=roles`.

### Image compatibility checks

`make image-compatibility` runs nine representative lifecycle, service and
multicluster tests (the `-run` list in the Makefile) with the selected images.
`make image-matrix` pins one published combination and runs the same tests:

```sh
make image-matrix IMAGE_VARIANT=debian IMAGE_LAYOUT=roles IMAGE_PLATFORM=linux/arm64
```

`IMAGE_VARIANT` is `official`, `debian` or `ubuntu`; `IMAGE_LAYOUT` is `all`
or `roles`; `IMAGE_PLATFORM` must be the engine's native architecture. Call
`run_image_matrix.py` directly with `--release 19.2.5` for Squid. Whether an
image satisfies the role contract is checked by ceph-testcontainers-images,
not by this suite.

### What features may rely on in images

Features may rely only on what the official `quay.io/ceph/ceph` image ships,
split into the roles that ceph-testcontainers-images defines (its
`docs/ROLES.md` is the authority): `essential` (MON, MGR, OSD, `ceph` CLI),
`client` (`rados`, `rbd`, Python bindings), `cephfs` (MDS, `cephfs-mirror`),
`rbd-aux` (`rbd-mirror`, caches) and `rgw` (`radosgw`, `radosgw-admin`). The
control container runs the `ceph` CLI from the essential image; `rados` and
`rbd` run in the client toolkit and `radosgw-admin` in the rgw role.

- The image has no `tc`, `ip`, `iptables`, `nft` or external `timeout`.
  Inject network faults with Ceph messenger options or Docker network
  operations (see [Fault injection](../guides/fault-injection.md)).
- Server images need no compilers, headers, go-ceph, kernel mount tools or
  Vault. Those go in client or service containers that the test starts.
- The module supplies FSID, keys, configuration, daemon IDs and entrypoints at
  run time. Do not depend on a distribution, package names or image labels.

## Make targets

Every Docker target takes a timeout override, for example
`MULTICLUSTER_TIMEOUT=90m make multicluster`. The targets run
`./internal/integration/...` with `-p 1`, one test package at a time, because
each package starts its own clusters.

| Target | What it runs | Timeout variable (default) |
| --- | --- | --- |
| `test-all` | `-tags=all ./...` | `ALL_TEST_TIMEOUT` (180m) |
| `integration`, `scenario-default` | `-tags=integration` | `INTEGRATION_TIMEOUT` (20m) |
| `scenario` | One CI batch through the tag runner | the batch's own budget |
| `multicluster` | `^TestMultiCluster` | `MULTICLUSTER_TIMEOUT` (60m) |
| `hostnetwork` | `^TestHostNetwork` | `HOSTNETWORK_TIMEOUT` (40m) |
| `image-compatibility`, `image-matrix` | Representative tests with chosen images | `IMAGE_COMPATIBILITY_TIMEOUT` (40m) |
| `goceph-linux` | go-ceph consumer check | set by the harness |

`make scenario` runs one batch with the tags, budget and pass rules that CI
uses. The tag runner takes the batch's tests from the build tags and metadata
of the test sources, so the Makefile keeps no list of test names and a new
test joins its batch without a Makefile change:

```sh
python3 .github/scripts/tag_scenarios.py plan --output artifacts/plan.json
make scenario SCENARIO_CATEGORY=recovery SCENARIO_BATCH=container_pause
```

`plan.json` lists every category, batch and package. A batch that several
integration packages have (such as `short`/`default`) also needs
`SCENARIO_PACKAGE`, for example `./internal/integration/rbd`. The run's logs
and report go to
`SCENARIO_DIRECTORY` (default `artifacts/scenario-<category>-<batch>`), which
must not exist yet. To run one test, use `go test -tags=all -run` (see
[One test](#one-test)).

`make goceph-linux` needs two images prepared on the local engine: a client
with `/usr/local/bin/go-ceph-probe` and the native libraries, and a runner
with the `integration,goceph` test binary. The harness never builds or pulls
them and needs host networking:

```sh
CEPH_TEST_GOCEPH_CLIENT_IMAGE=ceph-testcontainers-goceph:20.2.4-client \
CEPH_TEST_GOCEPH_RUNNER_IMAGE=ceph-testcontainers-goceph:20.2.4-runner \
make goceph-linux
```

The harness compares the probe's linked go-ceph version with the probe
module's `go.mod`, so rebuild the client image after a go-ceph update.

## Writing a new integration test

### Files

A scenario lives in the test package of its area: `internal/integration/rbd`,
`internal/integration/cephfs` or `internal/integration/rgw` when it is about
that service, and `internal/integration` (package `integration_test`) for the
cluster and everything else. Each package compiles and runs alone, so a change
in one area does not edit or rebuild the others.

Code that tests of more than one of these packages call lives in
`internal/integration/harness`. Go lets a test of another package see only
non-test files, so the harness is a non-test package that only test files may
import (`internal/tools/testsupportcheck` enforces that), and what other
packages use is exported. Keep a helper in its test package while only that
package uses it, and move it into the harness when a second package needs it.

In the test package, the scenario takes two files:

1. `<topic>_integration_helpers_test.go` holds the scenario body as a plain
   function such as
   `testMyFeature(t *testing.T, opts ...testcontainers.ContainerCustomizer)`.
   Its constraint has no CI terms (`//go:build all || integration`, optionally
   narrowed by capability tags) and it has no `//ci:` line.
2. `<topic>_integration_test.go` holds only the `Test...` functions, the CI
   category and batch in its constraint, and exactly one `//ci:` line before
   `package`.

```golang
//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_default))))

//ci: timeout=20m job-timeout=35

package integration_test

func TestMyFeature(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testMyFeature(t)
}

func TestHostNetworkMyFeature(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testMyFeature(t, ceph.WithHostNetwork())
}
```

`all ||` puts the test in the normal suite (the planner rejects a required
test that `all` does not compile). `integration` and capability tags select it
in capability runs. `!ci ||` keeps it there, while under `ci` it compiles only
for `ci_<category>`, and `(!ci_batch || ci_batch_<name>)` limits a batch run
to its own files.

### Categories

| Category | Use it for |
| --- | --- |
| `code` | Docker-free helper tests in a Docker test package |
| `environment` | Docker behaviour without Ceph |
| `short` | Basic bootstrap, services and client fixtures |
| `topology` | Cluster composition, node lifecycle, diagnostics |
| `multicluster` | Independent clusters, zones and peers |
| `recovery` | Faults, partitions, removal and recovery |

The tag is `ci_<category>`. `short` is a selection, not a time limit: it runs
for every pull request. Other categories run on a pull request only for
batches whose files changed. Ceph periodic runs every category on Tentacle and
Squid, so a test must pass on both (see
[Pull request scope](ci.md#pull-request-scope)).

### Batches and the `//ci:` line

- To join a batch, copy its constraint and `//ci:` line exactly. All files of
  a batch declare the same values, and the new test shares the batch timeout.
- To create a batch, pick a new `ci_batch_<name>` (`[a-z][a-z0-9_]*`). CI
  discovers it from source; do not edit workflows, matrices or the Makefile.
- `//ci: timeout=<duration> job-timeout=<minutes>`: `timeout` is the Go
  `-timeout` for the batch process (at most 6h); `job-timeout` is the job limit
  in minutes (1 to 360) and must exceed `timeout`. Leave at least ten minutes
  between them for checkout, compilation, image pulls and cleanup checks.
- `checker=` and `case=` are reserved for the strict completion checkers of
  the CephFS quiescence/recovery and RBD receiver batches.
- `image-roles=` lists the role images the batch needs (`essential`, `client`,
  `cephfs`, `rbd-aux`, `rgw`; `essential` required). Without it CI prepares
  all five. RADOS/RBD uses `essential,client`, CephFS adds `cephfs`,
  mirrors and caches add `rbd-aux`, gateways add `rgw`.
- `representative=<theme>` (`topology`, `auth`, `mirroring`, `recovery`,
  `flaky`) adds the batch to the periodic campaign (see
  [Representative campaign](ci.md#representative-campaign)). Choose short
  batches; use `flaky` for a known intermittent failure to keep measuring.
- When a batch approaches its budget, move a long independent test into its
  own batch instead of raising the limit. Keep short related tests together.
- A batch is one CI job, and each job pays about two minutes before and after
  its tests (compilation, freeing runner storage, image pulls, the cleanup
  check) plus the wait for a runner. So add a short test to an existing batch
  of its package and image roles rather than creating a batch for it. The
  short category groups its quick tests this way: `daemon_basics`,
  `daemon_operations`, `network_faults`, `address_families`, `cephfs_clients`,
  `cephfs_fixtures_admin`, `cephfs_fixtures_data`, `rbd_fixtures_basic` and
  `rbd_fixtures_mirror` each run several tests that keep their own clusters
  in one job, about ten minutes of tests each.

### Suites that share one cluster

A batch shares a job, not a cluster: every test starts and terminates its own.
A `testify/suite` may serve several cases from one cluster when they need the
same topology, create and name their own resources, change no daemon and
restore what they override. `TestRADOSPoolCases`
(`rados_pool_suite_integration_test.go`) is the example: one single-OSD
cluster per network mode for the pool-level RADOS cases.

- `SetupSuite` starts the cluster under the suite's test, so its cleanup ends
  the cluster after the last case. Each case uses `s.T()` and its own context.
- The suite's `sharedClusterGate` checks the cluster before every case and
  after a failed one. Once the cluster is not clean, every later case fails at
  once, naming the case that broke it; nothing is reset or retried.
- No case may depend on another having run or passed. Run one case alone with
  `-run '^TestRADOSPoolCases$/^bridge$/^TestObjectReceipts$'`.
- Cases that stop, kill or pause daemons, change topology or fault the network
  keep their own clusters.
- `testify/suite` runs its cases one at a time; do not move tests that use
  `harness.ParallelWhenEnabled` into a suite without accepting that.
- Methods may live in other files of the batch. The catalog records each
  `Type.Method` and the types each test passes to `suite.Run`. The planner
  counts a methods-only file as a source of the batch, and the completion proof
  requires every method to run and pass as a leaf of the test that runs its
  suite.

### Shared helpers must compile in every batch

CI compiles each batch alone, with only its own `ci_batch_<name>` tag, so a
helper defined in batch A's file does not exist when batch B compiles. `go
vet`, `make tag-compile` and `-tags=all` compile everything at once and miss
this. Put any helper used from more than one file into a helper file without
CI terms.

### Verify batch profiles before pushing

Run this after adding, moving or renaming an integration test file, or making
a test call a helper from another file:

```sh
python3 .github/scripts/tag_scenarios.py plan --verify --output artifacts/tag-plan.json
```

It needs no Docker but compiles each profile, so it takes several minutes. It
fails on a file outside `all`, a missing or duplicate category or batch, a
missing, duplicate or malformed `//ci:` line (or one in a helper file),
differing budgets within a batch, and any build whose compiled test list
differs from the source. Code checks runs it for every pull request.

### Writing the test body

- Start clusters with `newServiceCluster(t, opts...)`, or
  `integrationImages(t)` plus `newServiceClusterWithOptions` for a custom
  first image. These honour the image variables, attach a client container
  and register cleanup that prints daemon log tails on failure.
- When calling `Run` yourself, register cleanup as soon as it returns a
  non-nil container, even with an error.
- Bound each phase with `context.WithTimeout(t.Context(), ...)`. In
  `t.Cleanup`, derive from `context.Background()`, because `t.Context()` is
  already cancelled. See [Contexts and cleanup](../guides/contexts-and-cleanup.md).
- Prove the effect with a real client: bytes read back, a denied request, an
  arrived replica. A CLI exit code alone is not evidence.
  `execCommand(t, ctx, client, args...)` fails the test on a non-zero exit. See
  [Native commands](../guides/native-commands.md).
- Add a `TestHostNetwork...` variant when the feature touches addresses, ports
  or client connectivity.
- Never call `t.Skip`; CI counts skips as failures. When a release lacks a
  capability, assert the classified refusal from the shared table (see
  [Release differences](#release-differences)):

  ```golang
  if !harness.CapabilitySupported(t, ctx, cluster, harness.CapabilityRBDNamespaceMapping) {
  	harness.RequireCapabilityRefused(t, err, cluster, harness.CapabilityRBDNamespaceMapping, detail)
  	return
  }
  ```

- Call `harness.ParallelWhenEnabled(t)` only if the test touches nothing but its own
  clusters and containers: not for tests that inspect engine-wide Docker
  resources or events, need fixed host ports, or call `t.Setenv`.
- To collect evidence on failure, call `cluster.CollectDiagnostics` from
  cleanup with a short background context.

### Optional tests

A test for a known upstream defect must fail on the unpatched default image.
It requires `native_regression`, never compiles under plain `all`, uses the
`ci_optional` category and keeps strict assertions:

```golang
//go:build (all || (integration && multicluster)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_my_regression)))
```

### Release differences

Where the expected outcome differs between releases, the test selects it from
`ReleaseCapabilities` in
`internal/integration/harness/release_capabilities_integration_helpers.go`,
never from a release prefix of its own. `CapabilitySupported` first requires every
daemon to run the essential image's release, then answers from a probe or the
release row. Unsupported cases assert the error with
`RequireCapabilityRefused` and return. A mixed-release cluster, an unparsable
version, or a new major release without a row fails the test; an unlisted
patch uses its major release's row. `TestReleaseCapabilityTable` checks these
rules without a cluster.

Command-form differences that both releases support (`ceph.CephBefore`,
`multicluster.CephBefore`) belong to the fixtures and their own tests, not to
this table.

## Debugging a failing local run

- Read the `-v` output from the top: tests log the phase they reached, and the
  fixture helpers print daemon log tails on failure.
- A Go timeout prints `panic: test timed out after ...` with the running
  tests. Raise the timeout only after checking that the test was progressing.
- Ceph reports health and placement changes with a delay. Use the fixture's
  `Wait...` methods (see [Ceph](../../ceph/README.md)).
- To check that a run leaked nothing (the checker deletes nothing and assumes
  no other testcontainers-go session on the engine):

  ```sh
  python3 .github/scripts/check_runtime_cleanup.py snapshot --directory artifacts/cleanup-1
  # ... run the test ...
  python3 .github/scripts/check_runtime_cleanup.py check --directory artifacts/cleanup-1
  ```

  `check` waits up to 30 seconds for Ryuk, then fails on containers, networks
  or volumes labelled `org.testcontainers=true` that were not in the snapshot.
- Reproduce a CI failure with the tag runner and the same images; see
  [Reading a failing batch](ci.md#reading-a-failing-batch).

## What "done" means for a feature

A server-side fixture feature is done only when:

1. a public typed API, or a documented runnable combination of public APIs,
   achieves it;
2. tests check native server state and identity, not only command success;
3. tests check the real client effect (success, denial, failure and recovery,
   or replicated data) through a client;
4. tests check that the original state is restored or that owned resources and
   disposable clusters are cleaned up.

Unit tests, tag compilation, or the ability to run the commands through `Exec`
do not close an item. An implementation that cannot survive a partial failure
or lost reply, or that adopts or deletes resources it did not create, is not
done. Wrapping every client CRUD call is not a goal: the API prepares what a
client needs, and the test exercises the client.

Verify the changed public path with a representative bridge and host-network
run, plus unit tests, race, vet and `plan --verify`. CI runs the full set.

## Unit tests with fake containers

A real-cluster test is the primary verification. Use fake-container unit tests
only for branches a real cluster cannot produce on demand: the second of two
commands failing, a lost reply, an ID replaced under the same name, a
concurrent caller.

- Model a small piece of Ceph state (store what `fs set` changes, answer
  `fs get` from it) and refuse what Ceph refuses.
- Inject faults by condition ("the next run of this command fails"), not by
  position in a script.
- Assert the final state and the kind of error, not exact command strings or
  their order.
- Parse tests use output captured from a real cluster.

## Test doubles

The [test-double inventory](test-double-inventory.md) defines the classes; the
boundary rules are in [Test-only code](#test-only-code). Write tests so that
the reader sees what is substituted and what is proved.

1. Prefer the real implementation, or a real cluster in `internal/integration`.
2. Otherwise prefer a small fake whose behaviour and state are visible in the
   test or one short declaration next to it.
3. Use a mock (configured expected calls and canned replies) only when the
   substituted dependency, its configured behaviour and the purpose of the
   check are short and explicit at the reading site, and easier to review than
   a real implementation or a small fake.

Do not grow a parallel implementation of Ceph or Docker semantics; change the
boundary instead. Asserting an interaction is right when the interaction is
the contract (a refused operation issued no native command, an uncertain write
was not replayed). Avoid long expectation scripts that rebuild a normal
scenario, call-order or call-count assertions on implementation details, and
expectations hidden in distant helpers.

A fake or mock passing is unit evidence only. Do not word a result, pull
request or document as if it showed native Ceph, Docker or client behaviour;
only a real-cluster run shows that.

Name a double for its role (fake, recorder, counter), not like the real thing.
A double shared across packages lives in a test-only package under
`internal/`; a single-package double is in a `_test.go` file; none is exported
from a public package. Add it to the inventory in the same change when it is
in a test-only package or when a runtime file gets a new injection seam
documented as replaced in tests; `testdoublecheck` fails otherwise.

## Package layout and the export inventory

Each public package holds its own implementation. `rbd`, `cephfs` and `rgw`
reach non-public cluster parts (mutex, owned service containers, Run-time
provisioning) through `internal/servicehook` and keep per-cluster state in an
extension registry on the cluster. Shared helpers live in
`internal/clusterutil`, shared CLI fakes and lock assertions in
`internal/clustertest`, and cross-cluster resource tracking in
`internal/multicluster`. No Go file of the module, test files included,
imports a client library (go-ceph, an S3 or Swift SDK) or cgo. A client
belongs to a separate Go module, such as `internal/integration/goceph/probe`,
or to a consumer repository; see
[Client dependencies](../guides/api-purpose.md#client-dependencies).

`internal/apiinventory` checks that every exported function, type, method,
constant and variable of a public package has a godoc comment, and that each
package keeps its usage page, `<pkg>/README.md` (for example
[ceph/README.md](../../ceph/README.md)). The README describes what the package
is for, the usual order of use and its concepts and limits; the API itself is
documented in godoc, which `go doc` and pkg.go.dev show. Document a new export
in its doc comment in the same change.

The same tests check the per-package catalogues in
`internal/apiinventory/catalogues/<package>/` (`declarations.json`, `members.json`,
`review.json`). Packages are discovered from the source tree; there is no
shared index, count or hash file. Never edit these files by hand. After a
public API change, run two separate steps and review each diff:

```sh
go run ./internal/tools/apinaming regenerate <package>   # declarations.json and members.json only
go run ./internal/tools/apinaming approve <package>      # review.json only
```

Regenerating never approves: a changed catalogue without a new receipt fails
`TestNamingReviewBindsCurrentSurface`. `approve` refuses a stale catalogue or
a pending rename proposal. `go run ./internal/tools/apinaming check` verifies
layout, catalogues and receipts as the tests do, and `aggregate` prints totals
without committing them. CI runs only the tests.

#### Merging `main` into a branch that conflicts in these files

Two pull requests that change the same package's API conflict in its JSON
files even when the changes are independent. Resolve the Go sources first,
then run:

```sh
go run ./internal/tools/apinaming resolve            # packages that both parents changed
go run ./internal/tools/apinaming resolve <package>
```

`resolve` reads the merge base and both parents from Git (`HEAD` and
`MERGE_HEAD` by default; `-base`, `-ours`, `-theirs` override), so conflict
markers in the working tree do not matter. It rewrites a package's three files
only when both parents' receipts approve their own catalogues, the three-way
row merge has no row or receipt field that both parents changed differently,
the catalogue derived from the merged source equals the merged rows, and no
rename proposal is pending. It does not check that the two changes work
together; the merged source must still compile and pass the tests. Otherwise
it writes nothing and names the rows, and `regenerate` followed by a separate
`approve` is required. Commit the result with the merge.
