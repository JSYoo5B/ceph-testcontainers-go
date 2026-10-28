# Test-double inventory

This page lists the test doubles and test-only access points of the module,
what each one substitutes, and whether its name says so. The policy for
choosing a double is in
[Test doubles](testing.md#test-doubles). The compilation rules for test-only
code are in [Test-only code](testing.md#test-only-code).

The inventory was taken from source at the merge of the test-only code
isolation (`internal/testhelper`, the `ceph/unit_access.go` seam and
`internal/tools/testsupportcheck`). It is source inspection, not a type-resolved
analysis and not a linked-binary symbol listing. Counts are of files at that
point and are context, not a contract.

`go run ./internal/tools/testdoublecheck` (part of `make tag-compile`) keeps
the two tables below in sync with the source, in both directions:

- every exported top-level declaration of a package in the `test_only_packages`
  list of `internal/tools/testsupportcheck/manifest.json` has a row in the
  first table;
- every non-test declaration outside those packages whose doc or trailing
  comment says it exists for tests (for example "replaced in tests", "tests
  replace it" or "test-only") has a row in the second table;
- every symbol a row names is still declared in the file the row names;
- no non-test file outside the test-only packages declares a type or function
  whose name contains `fake`, `mock`, `stub` or `spy`.

A row names a symbol as `path/to/file.go#Symbol`, `Type.field` or
`Type.Method`. Add the row in the change that adds the declaration.

What the check cannot see: a seam whose comment does not use one of those
phrases, a double declared in a `_test.go` file (they are not listed one by
one, see [Package-local doubles](#package-local-doubles)), and whether a
description in a row is still true. The role and naming columns are reviewed by
people.

## Classes

| Class | Meaning |
| --- | --- |
| `shared double` | A concrete substitute that tests of several packages share. Lives in a test-only package. |
| `shared assertion` | A helper that takes `testing.TB` and fails a test. Not a substitute, but test-only. |
| `unit seam` | The one registered access from test helpers to unexported cluster state. |
| `dependency port` | A runtime interface with a real implementation chosen at construction. A test supplies another implementation. This is legitimate dependency injection. |
| `nil-default hook` | An unexported function-valued field that production code leaves nil (or sets to the real function) and that only same-package tests assign. |
| `wired operation` | A field that production code sets to the real operation at construction and that tests replace afterwards. |
| `test mutator` | A production-file method that exists so a test can change state. |
| `test accessor in contract` | A method of a runtime hook contract that only tests call. |

## Shared test-support packages

Only `_test.go` files import these packages. `internal/clustertest` is imported
by 36 test files, `internal/clusteraccess` by 114 and `internal/testhelper` by
55. None is imported by a non-test file.

| Declarations | Class and role | Substitutes | Name assessment |
| --- | --- | --- | --- |
| `internal/clustertest/clustertest.go#CannedCommandContainer` | shared double. Answers each Ceph command from a table keyed by the command words, records the calls, and fails one named command. | The control container's `Exec` (the `ceph` CLI). | Accurate. Renamed from `CLI`, which read as a real CLI. States that the answers are canned. |
| `internal/clustertest/clustertest.go#StateFake`, `internal/clustertest/clustertest.go#NewStateFake`, `internal/clustertest/clustertest.go#FakePool`, `internal/clustertest/clustertest.go#FakeFilesystem` | shared double. A small model of the OSDMap, CRUSH rules, erasure-code profiles, FSMap and `mon_allow_pool_delete`. Reads render from the state and mutations change it. | The control container's `Exec` for pool and filesystem commands. | Accurate. Keep. |
| `internal/clustertest/clustertest.go#Quorum`, `internal/clustertest/clustertest.go#ModuleArgs`, `internal/clustertest/clustertest.go#QuorumEnvelope`, `internal/clustertest/clustertest.go#Reply` | shared double support. Recognize the supervised quorum read and encode a command reply in the Docker multiplexed or supervisor envelope format. | The wire format of a command result. | Accurate. Keep. |
| `internal/clustertest/deadline.go#SignalDeadline`, `internal/clustertest/deadline.go#NewSignalDeadline` | shared double. A context whose deadline passes when the test calls `Expire`. | A wall-clock deadline context. | Accurate enough: the type reports no `Deadline`. Keep. |
| `internal/clustertest/locks.go#NativeCallCounter` | shared double. Counts `Exec`, `Stop` and `Terminate` calls and fails any `Exec`, so a queued operation provably made no native call. | A running container. | Accurate. Renamed from `ContextLockRuntime`, which suggested a real runtime. |
| `internal/clustertest/locks.go#ControlQueryCounter` | shared double. Counts container-ID queries that an admission refusal must not reach. | The control container. | Accurate. Renamed from `AdmissionContainer`, which read as a real container. |
| `internal/clustertest/locks.go#TerminateRecorder` | shared double. Records `Terminate` calls and returns a chosen error. | A container being cleaned up. | Accurate. Renamed from `CleanupContainer`, which read as a real container. |
| `internal/clustertest/locks.go#AssertContextLockQueued` | shared assertion. Holds a mutex and checks that an operation returns its own deadline without taking the gate. | none | Accurate. Keep. |
| `internal/clusteraccess/clusteraccess.go#OSDs`, `internal/clusteraccess/clusteraccess.go#Monitors`, `internal/clusteraccess/clusteraccess.go#Managers`, `internal/clusteraccess/clusteraccess.go#ControlContainer`, `internal/clusteraccess/clusteraccess.go#Gateways`, `internal/clusteraccess/clusteraccess.go#RGWGateways` | shared assertion. One-line topology accessors that fail the test on an error, so a refused call is not read as an empty topology. | none | Accurate. Keep. |
| `internal/testhelper/testhelper.go#TestConfig`, `internal/testhelper/testhelper.go#TestStateView`, `internal/testhelper/testhelper.go#TestCluster`, `internal/testhelper/testhelper.go#TestState`, `internal/testhelper/testhelper.go#TestClientState`, `internal/testhelper/testhelper.go#TestApply`, `internal/testhelper/testhelper.go#TestPrepare`, `internal/testhelper/testhelper.go#TestFaults`, `internal/testhelper/testhelper.go#TestHeld` | unit seam client. Build a synthetic `ceph.Container` and read its lock identities, client state, prepared pools and registered fault or override handles. | A bootstrapped cluster. | Accurate in a test-only package. The `Test` prefix is allowed only there. |
| `internal/testhelper/testhelper.go#TestSetClosed`, `internal/testhelper/testhelper.go#TestSetControlPlane`, `internal/testhelper/testhelper.go#TestSetConfig`, `internal/testhelper/testhelper.go#TestSetKeyring`, `internal/testhelper/testhelper.go#TestSetCephVersion`, `internal/testhelper/testhelper.go#TestSetNetworkRemoved`, `internal/testhelper/testhelper.go#TestNetworkRemoved`, `internal/testhelper/testhelper.go#TestSetInitialOSDs`, `internal/testhelper/testhelper.go#TestSetPoolDefaults`, `internal/testhelper/testhelper.go#TestSetStartupTimeout`, `internal/testhelper/testhelper.go#TestSetCephFSImage`, `internal/testhelper/testhelper.go#TestReplaceOSDs`, `internal/testhelper/testhelper.go#TestOSDCount` | unit seam client. Set or read one field of a synthetic cluster through plain values. | Cluster state that a real bootstrap produces. | Accurate. Keep. |

## Runtime injection seams

These declarations are in files that production builds compile. "Test
writers" says who assigns or calls them. None is reachable from the public API
of a package.

| Declarations | Class and role | Substituted dependency | Notes |
| --- | --- | --- | --- |
| `ceph/unit_access.go#unitAccess`, `ceph/unit_access.go#unitAccess.UnitCluster`, `ceph/unit_access.go#unitAccess.UnitState`, `ceph/unit_access.go#unitAccess.UnitFaults`, `ceph/unit_access.go#unitAccess.UnitHeld`, `ceph/unit_access.go#unitAccess.UnitApply`, `ceph/unit_access.go#unitAccess.UnitReplaceOSDs`, `ceph/unit_access.go#unitAccess.UnitSetNetworkRemoved`, `internal/hookreg/hookreg.go#Test` | unit seam. Registered once in `hookreg.Test`; only `internal/testhelper` reads it. Constructs a cluster value and reads or writes unexported fields. | Cluster construction and state. | Compiled into every build of `ceph`. Isolated by `testsupportcheck` (who may read it), not removed from the build. |
| `ceph/osd_registry_state.go#osdRegistry.replace` | test mutator. Swaps the owned OSD descriptors. | OSD registry content. | Only `unitAccess.UnitReplaceOSDs` calls it. |
| `ceph/ceph.go#Container.keyRotationDocker` | nil-default hook. Docker client of `RotateKey`. | Docker engine client. | Same-package tests assign it. Production leaves it nil. |
| `ceph/ceph.go#Container.startup`, `ceph/startup_identity.go#startupIdentity.ownedContainerDocker` | Runtime identity owner containing a nil-default cleanup hook. The owner holds startup facts; the hook supplies the Docker client for removal of labeled containers in `Terminate`. | Docker engine client (the hook only). | Same-package tests assign the hook. The owner is real runtime state, not a fake. |
| `ceph/ceph.go#Container.daemonRunner` | nil-default hook. Container start for `AddMonitor` and `AddManager`. | `runContainer`. | Same-package tests assign it. |
| `ceph/ceph.go#Container.stores`, `ceph/osd_storage_state.go#osdStorageState`, `ceph/osd_storage_state.go#osdStorageState.runner`, `ceph/osd_storage_state.go#osdStorageState.volumeDocker`, `ceph/osd_storage_state.go#osdStorageState.volumeRunner` | nil-default hook. OSD container start and owned-volume allocation. | `runContainer` and the Docker volume client. | Same-package tests assign them. |
| `ceph/shared.go#sharedSettings.start`, `ceph/shared.go#sharedSettings.settle` | wired operation. `NewShared` sets the real starter and the settle timeout; tests replace them. | `Run` of the shared cluster and the settle timeout. | `start` is a function type with a real default. |
| `ceph/shared_backend.go#sharedBackend` | dependency port. The container backend implements it; unit tests supply a model. | One shared cluster's pool, readiness, invariants and resources. | Legitimate injection. Not a fake by itself. |
| `ceph/shared_ops.go#clusterOps.raw` | dependency port member. Returns the gated cluster. | The tenant's cluster operations. | The comment also names a test-only hook; `SharedCluster.Exclusive` is a runtime caller, so the method stays. |
| `ceph/verification.go#Verification.admit`, `ceph/verification.go#Verification.snapshot`, `ceph/verification.go#Verification.docker`, `ceph/verification.go#Verification.now` | wired operation. `Finalization.Verify` wires the cluster's own operations; tests replace them. | Admission, diagnostics snapshot, Docker dial and clock. | |
| `ceph/finalization.go#Finalization.cluster`, `ceph/finalization.go#Finalization.collectDiagnostics`, `ceph/finalization.go#Finalization.collectCrash`, `ceph/finalization.go#Finalization.terminate` | wired operation. `TerminateOnCleanup` wires the cluster's methods; tests replace them. | Diagnostics, crash metadata and termination. | |
| `capture/docker.go#backend` | dependency port. The Docker backend implements it; tests supply a model. | Docker operations of a packet capture. | Legitimate injection. |
| `fault/network_delay.go#netemBackend` | dependency port. The Docker and helper-container backend implements it; tests replace it with a model of namespaces and qdiscs. | Docker and netem helper operations. | Legitimate injection. |
| `fault/pause.go#pauseContainerInCluster` | dependency port as a parameter. `Pause` passes the real Docker client factory. | Docker pause client. | Admission, locks and registry use the real implementation. |
| `internal/servicehook/servicehook.go#TenantCluster` | test accessor in contract. Returns the shared cluster of a tenant. | none | Open finding, see below. |

## Package-local doubles

Doubles declared in a `_test.go` file are compiled only by tests of their own
package and are not listed one by one. A list of names would only restate the
source and would conflict between concurrent changes. This section describes
the families. By the name pattern `fake`, `mock`, `stub`, `spy`, `recording`,
`recorder` or `scripted`, 77 type declarations in test files were found, in
`ceph` (24), `internal/integration` (23), `fault` (10), `cephfs/mirror` (10),
`rbd`, `override`, `cephfs` and `capture` (two each), `internal/supervised` and
`internal/dockerbridge` (one each). The module has no generated mocks and no
mock framework; the only `testify` reference is an indirect dependency.

| Family | What it substitutes | Typical shape |
| --- | --- | --- |
| Docker client and container fakes (`ceph`, `fault`, `cephfs/mirror`, `rbd`) | `testcontainers.Container`, the Docker engine client, volume and network operations | A struct that embeds the interface, implements the few methods the code under test calls and records or fails them. |
| CLI and `rados` fakes (`ceph`, `internal/integration`) | The control container's `Exec`, `rados` runs, supervised commands | A command table, or a small model of the state the command reads. |
| Cluster models in fixtures (`internal/integration`) | A cluster for the Docker-free control tests of native scenarios (ledgers, oracles, classifiers) | A `*Fake` type that feeds synthetic Ceph output to the logic under test. |
| Network and process models (`fault`, `internal/supervised`, `internal/dockerbridge`) | Namespaces, qdiscs, links, a supervised process, the bridge allocator | A model whose state changes are visible in the test. |
| Recorders (`ceph`, `internal/integration`) | A reporter, a finalization step list, shared operations | Appends what it is told and exposes it for an assertion. |

Passing a unit test with one of these doubles is control evidence for the logic
under test. It is not evidence that Ceph or Docker behaves the same way.

## Findings and uncertainties

Confirmed by source inspection:

- No non-test file imports a test-only package and none declares a
  `fake`, `mock`, `stub` or `spy` type or function. The `testdoublecheck` command
  enforces the second statement.
- `servicehook.TenantCluster` and `serviceHooks.TenantCluster` are part of the
  runtime hook contract. Their callers are `_test.go` files only: one unit test
  in `ceph` and the native integration tests of the shared cluster. This is a
  test-only accessor in a runtime contract, the same kind of mixing that the
  hook-contract split removed for the `Test*` methods. It is listed here and
  not changed in this inventory.
- The nil-default hooks and wired operations are fields of runtime structs that
  production never sets to anything but the real operation. They are a
  deliberate, narrow injection style (unexported, assigned by same-package
  tests). The inventory does not treat them as fakes.

Not established:

- Whether linker dead-code removal drops the compiled seams from a final binary.
  Ordinary source inclusion is the claim, and the rest is unmeasured.
- Seams without one of the comment phrases above are not found by the check.
  Reviewers of a new injectable dependency check that its comment says so.
- Package-local doubles in `_test.go` files are described by family only.

## Naming

The "Name assessment" column marks accurate names and, where a name was
misleading, records the rename. The four shared doubles `CLI`,
`ContextLockRuntime`, `AdmissionContainer` and `CleanupContainer` were renamed
to `CannedCommandContainer`, `NativeCallCounter`, `ControlQueryCounter` and
`TerminateRecorder`, each chosen from what its callers use it for. Every caller
was updated and no alias was kept (pre-1.0, old names are removed outright). A
future misleading name is renamed in its own change in the same way.
