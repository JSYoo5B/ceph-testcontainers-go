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

With Go, make, a C toolchain for the race detector and Python 3 installed:

```sh
make check   # unit tests, race detector, vet and tagged compilation
```

This starts no container. Before pushing, run the complete list in
[Local checks without Docker](docs/contributing/testing.md#local-checks-without-docker),
which also names the prerequisites: lint (with the `.golangci.yml` tags and
with `all` alone), the CI tooling tests and the batch profile verification.
A public API change also regenerates and approves its catalogue, and a
workflow change runs actionlint; the same section says how.

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
