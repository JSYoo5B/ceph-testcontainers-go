# Contributing

This repository holds Testcontainers for Go modules that start disposable Ceph
clusters for integration tests of services that use Ceph. This page is the
short path from a problem or an idea to a change that a reviewer can verify.
The detailed commands and policies live in the guides it links to.

## Reporting a problem

Open a [bug report](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/new?template=bug_report.yml).
The form asks for the module version, the Ceph release and image, the platform,
the network mode, a minimal reproduction and the expected and actual results.
Answer "unknown" or "not applicable" when a detail does not apply; do not guess.
Redact hostnames, private addresses, account paths and credentials from logs
before pasting them, and keep only the lines that show the problem.

A compatibility experiment or a research question about native Ceph behavior
does not fit the form. Choose "Blank issue" on the
[new issue page](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/new/choose)
and describe its trigger, its controls and the releases it compares.

## Proposing a change

Open or find an issue first for anything beyond a small fix, so the scope and
the acceptance can be agreed before the work. A pull request follows the
[pull request template](.github/pull_request_template.md): what changes for a
consumer of the modules, how it was verified, and what was not.

Before writing code, read:

- [AGENTS.md](AGENTS.md): the module conventions every package keeps (one
  `Run` entry point, configuration through the variadic options) and the
  purpose a public API must serve.
- [API naming](docs/contributing/api-naming.md) and
  [API purpose and purpose tags](docs/guides/api-purpose.md) for a change to
  the public API.
- [Bug compatibility tags](docs/contributing/bug-compatibility.md) for a
  change that depends on a Ceph defect of particular releases.

## First checks, without Docker

These start no container. They need:

- GNU make (or a compatible `make`);
- Go 1.21 or newer, which downloads the `toolchain` version that
  [go.mod](go.mod) names when the installed one is older (unless
  `GOTOOLCHAIN=local` is set);
- a C toolchain that cgo can use (`gcc` or `clang` as `CC`) on a platform the
  race detector supports, such as linux/amd64, linux/arm64, darwin/amd64 or
  darwin/arm64: `make check` runs `make race`, which sets `CGO_ENABLED=1` and
  fails to build `runtime/cgo` without it;
- Python 3;
- [golangci-lint](https://golangci-lint.run/) v2, at the version the Code
  checks workflow pins (`.github/workflows/code.yml`).

```sh
make check   # unit tests, race detector, vet and tagged compilation
make lint
golangci-lint run --build-tags all ./...
python3 -m unittest discover -s .github/scripts -p 'test_*.py'
python3 .github/scripts/tag_scenarios.py plan --verify
```

A change to the public API also regenerates and approves its catalogue and
passes `go run ./internal/tools/apinaming check`; see
[Testing](docs/contributing/testing.md#package-layout-and-the-export-inventory).
The exact list CI enforces is in [CI](docs/contributing/ci.md).

These checks do not show that a fixture works against a real Ceph cluster.

## When native evidence is needed

A change to what a fixture does to a cluster, or to what it observes, needs an
integration run against the native daemons: the affected tests on a Linux
Docker engine, with the release and the role image layout CI uses for them.
A documentation-only change, or a change covered entirely by unit tests of
pure code, does not. Report fake or unit results separately from native ones,
and name the release, image and network mode of every native run.

- [Testing](docs/contributing/testing.md): running one test or one CI batch,
  choosing images, and writing a new integration test.
- [What "done" means for a feature](docs/contributing/testing.md#what-done-means-for-a-feature):
  the evidence a fixture feature needs before its issue closes.
- [CI](docs/contributing/ci.md): what the workflows run on a pull request and
  how to read a failing batch.
