# Preflight

Import `github.com/JSYoo5B/ceph-testcontainers-go/ceph/preflight` to check,
before a fixture starts, whether the Docker engine that the fixture would use
meets its prerequisites. The report is safe to share: keep it in the test
output, a CI artifact or an issue. It covers the engine itself and the block
I/O faults of [`fault/blockio`](../../fault/blockio/README.md). Preflight is
something the test asks for: no fixture runs it, and a fixture's own
operation-time checks still decide.

## Order of use

1. **Observe** with `preflight.Observe` and the capabilities the test needs.
2. **Decide** from `Report.Verdict`: go on, skip the case on this engine, or
   fail. Do not parse the text.
3. **Keep** the report: `Report.String()` is the summary and the JSON
   encoding of `Report` is the same snapshot.
4. **Start the fixtures.**

The [compiled example](example_test.go) does all four for the delay and EIO
profiles of `fault/blockio`.

## Levels

| Level | What it does | What stays unknown |
| --- | --- | --- |
| `Inspect` (the default) | Reads the engine's information. Starts no container, pulls no image, changes nothing. | Every check that needs the helper: image tools, drivers, control devices and targets are `not-run`, so a block I/O verdict is at most `unknown`. |
| `Helper` | Also starts one temporary container from `Request.HelperImage`, pulling it when the engine lacks it, with the block I/O keeper's privileges (root, privileged, the engine host's `/dev`, no network). It runs a read-only probe in it and removes it. | Whatever the probe could not read. |

The helper probe is the one `blockio.Run` and `Container.Apply` run in their
keeper: the image tools, the loop and device-mapper drivers, read and write
access to their control devices, and the targets `dmsetup targets` lists.
Neither level applies a fault, creates a loop device or mapping, or loads a
kernel module. testcontainers can start its reaper for the helper, as it does
for any fixture.

The helper is removed on success, failure, timeout and cancellation, with a
fresh 30-second context when the caller's has ended. `Report.Helper` says
whether its removal was confirmed; when it was not, it names the container
and `Observe` returns an error along with the report.

## Reading a report

Each check has a stable key, a state, the source that observed it, and for an
unavailable one its kind and guidance:

| State | Meaning |
| --- | --- |
| `observed` | The prerequisite was observed. |
| `unavailable` | An observation proved it missing. Kind `environment` is an engine, kernel or privilege limit, which operations report as `ceph.ErrEnvironmentUnavailable`; kind `image` is the helper image, reported as `ceph.ErrImageCapability`. |
| `unknown` | The observation failed or was inconclusive. It proves nothing either way. |
| `not-run` | The check was not run at this level, or a gate before it was not observed. |

`Observe` returns an error with the report for every `unknown` check that a
failed observation left, and for a helper whose removal was not confirmed.
`unavailable` and `not-run` checks come without an error.

`Report.Verdict` is `unavailable` when any check of a capability is,
otherwise `unknown` when one is unknown or not run, otherwise `observed`.
The common gates (engine, Linux engine, helper start, tools, drivers,
control devices, `linear`) belong to both block I/O capabilities; the `delay`
target belongs to `BlockIODelay` and the `error` target to `BlockIOEIO`, so
a missing delay target leaves EIO eligible.

Two cases are easy to read wrongly:

- A successful `dmsetup targets` without `delay` proves the target missing.
  A `dmsetup targets` that fails proves nothing: the targets are `unknown`.
- An engine that refuses the helper's privileged configuration makes
  `blockio.helper-start` unavailable, because the keeper needs the same
  configuration, and leaves the kernel checks `not-run`.

`observed` never certifies that a device can be created, that I/O is delayed,
that a client sees EIO or that Ceph recovers. The native block I/O scenarios
prove those effects; see [`fault/blockio`](../../fault/blockio/README.md).

## Engine facts

`Report.Process` is the platform of the Go test process and
`Report.Engine` that of the engine, which differ for Docker Desktop and for
a remote engine. The engine facts are an allowlist: server version, OS type,
architecture, kernel version, operating system, cgroup version, Docker Desktop
and a WSL 2 backend when the engine reports them, rootless and user namespace
security options, and the transport scheme of the endpoint (`unix`, `npipe`,
`tcp`, `ssh`, `http`, `https`). A fact the engine did not report is unknown.
The report never holds the endpoint, a host name, a path, a credential, the
environment or a raw engine response, and `Observe`'s errors carry fixed texts
and the context error only.

The engine is the one testcontainers selects for this process, through the
testcontainers configuration, `DOCKER_HOST` or the Docker context.
testcontainers caches that selection for the process, so a fixture started
next uses the same engine, and an engine it found unreachable stays
unreachable for the process. The engine is `unavailable` only when every
candidate endpoint failed to connect. An endpoint that answered with an
error, a selection that found no candidate and a runtime provider that
failed are `unknown`, with an error. Every `Observe` reads the engine again.

## Changing the engine

The library changes no kernel, Docker Desktop or daemon setting. To provide
what a report finds missing:

- On Docker Desktop, [Prepare the engine kernel](../../docs/guides/desktop-setup.md#prepare-the-engine-kernel)
  inspects and prepares the `loop`, `dm` and `dm-delay` features of the
  engine VM; the same guide covers WSL 2.
- On a Linux engine, the host administrator loads `loop`, `dm_mod` and
  `dm_delay`.
- A rootless engine or an authorization plugin can refuse privileged
  containers with the host's `/dev`; use an engine that allows them.

See [Fault injection](../../docs/guides/fault-injection.md#delaying-or-failing-an-osds-device)
for the faults themselves and [Host networking](../../docs/guides/host-network.md)
for the other engine prerequisites of the fixtures, which preflight does not
check yet.
