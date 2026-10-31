# Test-double inventory

This page defines the classes of test doubles and test-only access points of
the module. Each declaration states its own class in a directive at the end of
its doc comment, so changes that add doubles in different packages do not edit
a shared list. The policy for choosing a double is in
[Test doubles](testing.md#test-doubles). The compilation rules for test-only
code are in [Test-only code](testing.md#test-only-code).

```go
// docker replaces the Docker client in tests; nil uses the engine.
//
//testdouble:nil-default-hook
docker func(context.Context) (keyRotationExecClient, error)
```

go doc hides the directive, and gofmt keeps it after a blank comment line. The
doc comment above it says what the declaration substitutes; that sentence is
the description a reviewer checks.

`go run ./internal/tools/testdoublecheck` (part of `make tag-compile`) checks
the directives with Go syntax only, not with type information:

- every exported top-level declaration of a package in the `test_only_packages`
  list of `internal/tools/testsupportcheck/manifest.json` has one;
- every non-test declaration outside those packages whose doc or trailing
  comment says it exists for tests (for example "replaced in tests", "tests
  replace it" or "test-only") has one;
- every directive names one class of the list below, once, in the doc comment
  of a declaration;
- no non-test file outside the test-only packages declares a type or function
  whose name contains `fake`, `mock`, `stub` or `spy`.

What the check cannot see: a seam whose comment uses none of those phrases and
that has no directive, a double declared in a `_test.go` file (they are
described by family, see [Package-local doubles](#package-local-doubles)), and
whether a doc comment is still true.

## Classes

| Directive | Meaning |
| --- | --- |
| `shared-double` | A concrete substitute, or its support code, that tests of several packages share. Lives in a test-only package. |
| `shared-assertion` | A helper that takes `testing.TB` and fails a test. Not a substitute, but test-only. |
| `unit-seam` | The one registered access from test helpers to unexported cluster state. |
| `unit-seam-client` | A test-only helper that builds a synthetic cluster or reads or writes its state through the unit seam. |
| `dependency-port` | A runtime interface, or a parameter of one, with a real implementation chosen at construction. A test supplies another implementation. This is legitimate dependency injection. |
| `nil-default-hook` | An unexported function-valued field that production code leaves nil (or sets to the real function) and that only same-package tests assign, or the owner field that carries one. |
| `wired-operation` | A field that production code sets to the real operation at construction and that tests replace afterwards. |
| `test-mutator` | A production-file method that exists so a test can change state. |

To list the declarations of one class:

```sh
git grep -n -B1 '^\s*//testdouble:nil-default-hook'
```

## Package-local doubles

Doubles declared in a `_test.go` file are compiled only by tests of their own
package and are not listed one by one. A list of names would only restate the
source and would conflict between concurrent changes. This section describes
the families. The module has no generated mocks and no
mock framework. `testify` is used only for `testify/suite` in integration
tests (see [Suites that share one cluster](testing.md#suites-that-share-one-cluster));
its `mock` and `assert` packages are not used.

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
- The nil-default hooks and wired operations are fields of runtime structs that
  production never sets to anything but the real operation. They are a
  deliberate, narrow injection style (unexported, assigned by same-package
  tests). The inventory does not treat them as fakes.

Not established:

- Whether linker dead-code removal drops the compiled seams from a final binary.
  Ordinary source inclusion is the claim, and the rest is unmeasured.
- Seams without one of the comment phrases above and without a directive are
  not found by the check. Reviewers of a new injectable dependency check that
  it has a directive.
- Package-local doubles in `_test.go` files are described by family only.

## Naming

A double's name says what it substitutes. Rename a misleading double in its
own change.
