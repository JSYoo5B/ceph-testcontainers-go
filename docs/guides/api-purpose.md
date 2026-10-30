# API purpose and purpose tags

This guide states what the public API is for and what each purpose tag means.
It is a design guide: the tag of each function and method lives next to it, in
a `//purpose:` directive (see [Tagging an API](#tagging-an-api)), and the go doc
of each identifier remains its contract.

## Purpose boundary

This repository is a test tool. Every public API is judged against two rules.

1. **It serves repeatable integration tests of services that use Ceph.** It
   does not aim to implement or operate a complete Ceph cluster. It reproduces
   as many operational scenarios as a disposable test cluster can, but
   large-scale experiments, performance measurement and tuning are out of
   scope. Fault injection exists only to reproduce the environment or condition
   a test asks for.
2. **It builds and checks the collaborator, not the system under test.** An API
   either brings Ceph into the fixture state a test wants, independently of the
   SUT and the test code, or lets the test check after the run that Ceph and
   the integration environment reached the expected state. An API that goes
   beyond this, such as a convenience that only shortens what the SUT or a
   native command already does, or an operation aimed at running a real
   service, does not belong here.

APIs that make a condition easier to build or a state easier to assert may keep
being added; the boundary is the test purpose, not a method count.

## Client dependencies

The fixture and the SUT reach Ceph from opposite sides, and the dependency
graph keeps them apart.

- **The fixture uses the server's own tools.** It builds and checks state
  through the Ceph CLIs, radosgw-admin and admin sockets inside the containers
  it owns. The module imports no Ceph or object-storage client library
  (go-ceph, librados bindings, an S3 or Swift SDK) and no cgo, and it builds
  with `CGO_ENABLED=0`. A fixture API that needs a client library to work is
  doing the SUT's side of the job.
- **The SUT brings its own client.** The application under test connects with
  whatever client it ships with, using what a **Hand-off** API gives it. The
  fixture does not wrap, choose or pin that client.
- **A test that plays the SUT uses a client from its own module.** A test may
  need a real client to drive the fixture the way the SUT would, for example
  to prove that a go-ceph consumer can use a hand-off. That test, or the probe
  it runs, lives in a separate Go module (such as
  `internal/integration/goceph/probe`) or in a consumer repository. It does
  not go into this module's tests: go.mod lists test dependencies too, so the
  client would join every consumer's module graph.

`TestModuleRequiresNoClientLibrary` and `TestModuleImportsNoClientLibraryOrCgo`
in `internal/apiinventory` enforce the module side of these rules.

## Purpose tags

A purpose tag names one test purpose an API serves. One API often serves
several, so every public function and method carries one or more tags. Types and
constants follow the API that uses them. The directive spelling of each tag is
given in parentheses.

Purpose tags are documentation and review aids. They are unrelated to the Go
build tags and the CI scenario tags that `internal/tools/tagcatalog` manages.

**Boundary rule:** an API must pass the "belongs" test of at least one of its
tags. An API that passes none is outside the boundary.

| Tag | Definition | Belongs when | Does not belong when |
| --- | --- | --- | --- |
| **Construction** (`construction`) | Creates the disposable cluster, a service on it, or a resource in it, in the state the test needs before the SUT runs. | It establishes a precondition the test cannot get from the SUT, and the fixture owns what it creates. | It adopts arbitrary existing state as if the fixture owned it, or provisions something only a production deployment needs. |
| **Condition** (`condition`) | Moves an existing fixture into an abnormal, transitional or reconfigured state (a fault, a failover, a policy change) that the test wants the SUT to meet. | The state is a scenario a test asserts the SUT's reaction to, and the API records what it changed when undo is possible. | It is an operational procedure with no test scenario, or it changes state without recording how to undo it when undo is possible. |
| **Observation** (`observation`) | Reads native state, or waits for it, so the test can assert the expected result. It never changes Ceph state. | It reports exactly what Ceph says, with the identity checks needed to trust it, or waits for a stated native condition. | It reinterprets or summarizes beyond what an assertion needs, or mutates while observing. |
| **Hand-off** (`handoff`) | Gives the SUT or a test client what it needs to talk to the fixture: configuration, keyrings, endpoints, credentials, a prepared client container. | The SUT would otherwise have no way to reach the fixture as a real client. | It performs the SUT's work for it (a client library wrapper, a data-path helper). |
| **Native access** (`native`) | Runs a native Ceph, RBD, radosgw-admin or admin-socket operation inside an owned container, for what the typed API does not cover. | It keeps the fixture's scope (realm, zone, identity, container ownership) and its timeout and output limits. | It lets the caller override the fixture's scope or connection, or it becomes the typed API's replacement. |
| **Evidence** (`evidence`) | Collects material for investigating a failed test: diagnostics, crash metadata, packet captures, redaction. | It is bounded, redacts secrets, and does not change what the test observed. | It is a monitoring or export feature for a running service. |
| **Lifetime** (`lifetime`) | Ends or releases what the fixture owns: termination, test-cleanup registration, removal of fixture-created resources, restoration of a changed state. | It removes or restores only what the fixture created or changed, in an order that keeps the cluster consistent. | It deletes caller or native state the fixture did not create. |

Two tagging notes:

- A handle's `Restore`, `Resume` or `Stop` carries the tags of the call that
  returned the handle, plus **Lifetime**.
- A `Remove` of a member (MON, MGR, OSD, MDS, gateway, mirror daemon) is
  **Condition** + **Lifetime**: the test may use it to cause a member loss, and
  it also releases what the fixture owns. A `Remove` of a plain
  fixture-created resource (pool, subvolume, user, namespace) is **Lifetime**.

## Judging borderline APIs

These rules decide the cases that the tag tests alone leave open. The reason a
particular API is kept belongs in that API's own doc comment, not here.

- A method that only forwards to another public API, or a shortcut that works
  for one configuration only, is refused; keep the one general form.
- A check that only another package of this module needs is internal (a hook
  under `internal/`), not public API.
- An observation of internal state is acceptable when a test needs it to tell
  apart two outcomes it must judge.
- An operation that looks operational (backup and restore, schedules, period
  pulls) belongs when it seeds a precondition, performs a condition step, or
  reads back what the SUT wrote, and only with the restrictions a fixture
  needs.
- Two near-identical waits or reads may both exist only when their acceptance
  conditions differ, and each one's doc comment states the difference.

## Tagging an API

Each exported function, and each exported method of an exported type, ends its
doc comment with one directive:

```go
// AddOSD registers, formats and starts a new OSD ...
//
//purpose:construction,condition
func (c *Container) AddOSD(ctx context.Context, opts ...OSDOptions) (*OSDContainer, error)
```

- Write `//purpose:` with no space after `//`, then the tags in lowercase,
  separated by commas without spaces, in the order of the table above
  (construction, condition, observation, handoff, native, evidence,
  lifetime).
- go doc and pkg.go.dev do not show directive lines, and gofmt keeps a
  directive at the end of the doc comment, so the tag does not change the
  rendered documentation.
- Methods whose contract is an interface they implement carry no directive:
  `String`, `GoString`, `Error`, `Unwrap`, `Is`, `As`, and `Customize`, which
  makes a configuration value a `testcontainers.ContainerCustomizer`.

`TestExportsDeclarePurposeTags` in `internal/apiinventory` enforces this: one
directive per exported function or method in scope, as the last doc line, with
known tags, each once, in that order, and no purpose directive anywhere else.
Because the tag sits with its declaration, two changes to different APIs never
edit the same line.

## Adding or changing an API

A new or changed public function or method states its purpose tags in its
`//purpose:` directive and in the pull request. It must pass the "belongs" test
of at least one of its tags. A proposal that passes none, such as a
service-operation API or a convenience that only repeats what the SUT, the test
or a `native` API already does, is refused. An existing API that passes none
is removed, unless one of the rules in "Judging borderline APIs" applies; the
reason then goes in that API's doc comment.
