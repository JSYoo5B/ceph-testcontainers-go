# Preparing for v0.3.0

This guide describes the v0.3.0 candidate relative to v0.2.0. The release has
not been published; `@latest` still selects v0.2.0. For candidate testing,
pin a reviewed commit instead of assuming that `@latest` includes these
changes. All four packages share one root module version.

For example, after setting `CEPH_CANDIDATE_SHA` to the reviewed full commit:

```sh
go get "github.com/JSYoo5B/ceph-testcontainers-go/ceph@${CEPH_CANDIDATE_SHA}"
```

The [v0.3.0 milestone](https://github.com/JSYoo5B/ceph-testcontainers-go/milestone/2)
has no remaining open items: supervised native input, an MDS unsafe-to-safe
request window, and bounded authentication first-failure diagnostics.
Additional native receipts merged since v0.2.0 are listed in
[CHANGELOG.md](../../CHANGELOG.md). Completion of this milestone describes
the producer fixtures; a consumer must still run its own protocol tests.

## Compatibility

| Area | Change from v0.2.0 |
| --- | --- |
| Go minimum and repository toolchain | Unchanged: Go 1.26.0, repository toolchain `go1.26.9`; consumers select their own patched toolchain |
| Module dependencies | `go.mod` and `go.sum` are unchanged; Testcontainers for Go remains v0.44.0 |
| Client platforms | Unchanged: the public module builds with `CGO_ENABLED=0`; native libraries and tools run inside Linux containers |
| Bootstrap, image and Messenger defaults | Unchanged; existing image selection and extension opt-in contracts still apply |
| Exported API | Native input types, fields, result counters and `ErrNativeInputClosed` added in `ceph`; no new exported observer or service-package API |
| Existing commands without input | Leave `Input` nil and `MaxInput` zero; the existing no-input execution path and result/error contracts remain |

Use keyed literals for `ceph.NativeCommand` and `ceph.NativeCommandResult`.
Their new fields can require source changes in unkeyed literals. The input
type is created with `ceph.NewNativeInput()`, not a zero value or struct
literal, and can serve only one command. It has `Write(ctx, data) error`,
not the `io.Writer` method signature.

For the earlier Go/Docker dependency and error changes from v0.1.0, see
[Upgrading to v0.2.0](upgrading-to-v0.2.md).

## Supplying data to a running native command

`RunNativeCommand` now accepts `NativeCommand.Input` and `MaxInput`. Create
the input, start the supervised call, wait for the worker's ready receipt,
then send records with `Write` and finish with `Close`. The
[standard-input recipe](native-commands.md#standard-input) shows cancellation
and joining the call when an assertion fails.

| Contract | What to check in a consumer |
| --- | --- |
| Bounds | Set a positive `MaxInput`, at most `MaxNativeCommandInput` (64 MiB total). Without input, the limit must be zero. A write crossing the limit is refused whole with `ErrInvalidRequest`; input stays open. |
| Ownership | An input belongs to one command. The call joins its input/output work before returning; the caller must also join the goroutine it started. `Close` is idempotent and ends the stream after earlier writes. |
| Backpressure | `Write` waits for the container input stream, within its context, rather than for the child to read. The supervisor can hold up to `MaxInput` bytes in memory. |
| Uncertain delivery | A context error from `Write` can follow a transfer. `InputWritten` counts bytes accepted by the stream, `InputDelivered` bytes written into the child's pipe, and `InputEOF` the end of that stream. None proves application execution or durability. |
| Completion | `Complete()` still checks time and output limits only. Check exit code, delivery counters, EOF and the worker's own receipt separately. |
| Errors and cleanup | Further writes after close or command completion fail with `ErrNativeInputClosed`. Command cancellation retains `NativeCommandUnstoppedError.Stop` and cluster `Terminate` obligations. Never infer rollback from a stopped process. |
| Secrets | The transport does not place input in argv, environment, files or diagnostics. Output remains raw and is not automatically redacted; workers handling secrets must not echo them. |
| Target | Input uses the cluster's Docker engine attach API. Its target must belong to that engine and meet the existing native-command requirements, including `python3`. Role OSD, RGW and MDS images need not provide Python; use a suitable control/client container. |

`TestNativeCommandInput` supplies two binary records in separate phases and
verifies a worker receipt, byte counts, EOF and supervisor cleanup.
`TestNativeCommandInputCancellation` verifies cancellation while the command
does not read. Both belong to the control-only `native_input` batch. The
implementation PR records focused official and role-image runs on Tentacle
and Squid: [#85](https://github.com/JSYoo5B/ceph-testcontainers-go/pull/85).
Those implementation runs are separate from final release-source CI.

## Protocol receipts and their limits

These recipes live in `internal/integration` test helpers and the
[Messenger fixture guide](messenger-client-fixtures.md). They are not
importable public observer APIs. Reuse their observation and classification
logic in a consumer without assuming the helper names or receipt schemas
are stable package interfaces.

| Receipt | Established observation | Boundary |
| --- | --- | --- |
| [MDS unsafe-to-safe window](messenger-client-fixtures.md#mds-sessions), [#86](https://github.com/JSYoo5B/ceph-testcontainers-go/pull/86) | With `mds_log_pause` applied on the running MDS, the client's and MDS's pending requests hold the same rename TID after its early unsafe reply. Restoring the pause permits journal completion; another client verifies the resulting inode and namespace. | The MDS stays alive. This does not establish replay across a crash, arbitrary dirty-data recovery or lease recovery; [#48](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/48) remains separate. |
| [Authentication first-failure bundle](messenger-client-fixtures.md#authentication-first-failure-bundle), [#87](https://github.com/JSYoo5B/ceph-testcontainers-go/pull/87) | Within bounded collection and cleanup allowances, preserve the first rejection, MON observations, MGR identities, query outcomes and fresh native controls, including partial results. Offline fixtures preserve the retained failure and classify missing or changed observations. | Complete collection or a successful control is not proof of issuer/verifier key-generation equality. That field remains `unavailable/unverified`; the intermittent failure in [#71](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/71) is not fixed by this recipe. |
| [MGR transition](messenger-client-fixtures.md#mgr-transition-receipt), [#88](https://github.com/JSYoo5B/ceph-testcontainers-go/pull/88) | Stop the active MGR, observe the captured standby promoted without replacing its process, then restart the old container and observe its new registration. Bracket maps and correlate beacon sessions with GIDs. | Tested with the default Messenger policy. This is not secure/CRC matrix coverage, connection-mode evidence or OSD/MDS/RGW/mirror continuity across that transition. |

The native tests also distinguish Squid and Tentacle observations: Squid's
pending MDS requests use repeated `request` members rather than Tentacle's
`requests` array; its MONs do not support the rotating-key wipe used by the
Tentacle diagnostic scenario. Collection without that wipe is not wipe
coverage. Missing, truncated or unsupported observations remain explicit
outcomes rather than successful absence checks.

Other receipts added since v0.2.0 cover retained MDS sessions, standby and
rank-1 takeover, late reconnect eviction, capability handoff, live rank
migration, and replicated/EC log recovery versus backfill. Each has its own
observation boundary in the fixture guide; together they do not certify every
daemon path or a downstream Messenger implementation.

## Adoption and release gates

For a legacy Messenger client without `OSDHASHPSPOOL`, disable `hashpspool`
before *any* pool is created, including the MGR's `.mgr` pool. The updated
[profile recipe](messenger-client-fixtures.md#profiles) uses
`WithConfigFile` with `osd_pool_default_flag_hashpspool = false`;
`NoHashPSPool` on only the application's pool is insufficient. Monmap-only
membership changes also require waiting for the election to settle before
starting fresh clients; see the same guide's
[daemon controls](messenger-client-fixtures.md#daemon-controls).

Before publication, record required CI at the exact final source, its
attempts and any unresolved failures as described in
[Preparing a release](../contributing/releasing.md). A closed milestone,
focused native run or successful retry does not replace that gate. Selective
peer faults and the remaining parent investigations are not silently covered
by this release. No new image capability is required by the three milestone
items, and image policy remains owned by ceph-testcontainers-images.
