# Upgrading to v0.2.0

This guide describes the v0.2.0 changes from v0.1.0. Select the release tag
explicitly when upgrading an integration module. The complete change list is
in [CHANGELOG.md](../../CHANGELOG.md).

The release prepares server-side fixtures for Messenger client tests:
configurable v2-only, secure and CRC profiles, Cephx keys and ticket lifetimes,
daemon controls and native mutation receipts. The
[fixture-readiness milestone](https://github.com/JSYoo5B/ceph-testcontainers-go/milestone/1)
is complete. A downstream project must still migrate its adapter and run its
own client tests; the milestone does not certify a Go Messenger implementation.

## Go and client platforms

| Requirement | v0.1.0 | v0.2.0 |
| --- | --- | --- |
| Go module minimum | Go 1.25 | Go 1.26.0 |
| Repository toolchain | None (no `toolchain` line) | `go1.26.9` in `go.mod` |
| Testcontainers for Go | v0.44.0 | v0.44.0 |
| Docker client modules (`github.com/moby/moby/client`, `github.com/moby/moby/api`) | v0.5.0, v1.55.0 | v0.6.2, v1.56.1 |
| Native dependencies in the public module | None | None; builds with `CGO_ENABLED=0` |

When upgrading, check the consuming module's `go` and `toolchain` lines and
use a patched Go release. The dependency's `toolchain go1.26.9` line selects
this repository's toolchain; it does not set the toolchain of a consuming
module. Minimum version selection raises the consuming module's Docker client
modules to at least these versions, which matters if it calls the Docker API
directly. If fixtures live in a separate integration module, upgrade that
module without adding Ceph native dependencies to the product module.

The Go fixture library runs from macOS with Docker Desktop or from Linux,
while all Ceph daemons and native probes run on a Linux Docker engine. These
are separate requirements:

- An S3 SDK can use the mapped RGW HTTP endpoint from the host.
- A client that speaks Messenger to MON/MGR/OSD needs every advertised
  address reachable, not just the mapped MON port. Use a client container on
  the cluster bridge, or host networking when the engine supports it.
- The repository's go-ceph tests are Linux-only and need a prepared client
  image, cgo and Ceph development libraries. Enabling Docker Desktop host
  networking does not supply those libraries to a macOS-native process.
  Host TCP reachability on macOS is not proof of authenticated native I/O.

See [Host network](host-network.md) for Docker Desktop requirements and the
verified host/client-container paths.

## API compatibility

Ordinary calls to these methods remain valid without options. Interfaces,
mocks and function variables that require their old exact signatures must
include the new variadic parameter:

| Method | New trailing parameter |
| --- | --- |
| `ceph.Container.CreateClient(ctx, name, caps, ...)` | `...ceph.ClientOption` |
| `ceph.Container.WithClient(...)` | `...ceph.ClientContainerOption` |
| `ceph.Container.WithClientIdentity(client, ...)` | `...ceph.ClientContainerOption` |

Other changes that may affect assertions or cleanup:

| Old assumption | What to do now |
| --- | --- |
| Native errors contain stdout/stderr and can be matched as strings | Match categories with `errors.Is` and read `*ceph.NativeFailure` with `errors.As`. Raw output is not included in the error; some sensitive operations also omit `Reason`. |
| Every failed service start returns a handle | A failure before container creation returns nil. Register nil-safe cleanup before checking the error; preserve any non-nil partial result. |
| `WithConfigFile` can set `crash_dir` | It is reserved at `/var/lib/ceph/crash`, where `CollectCrashMetadata` reads bounded, redacted metadata. |
| RBD namespace mapping support follows only the version number | The relevant setup and control clients' `rbd --remote-namespace` capability is checked. Backported images may work; an unsupported target returns `ErrUnsupported` before setup changes. |

The five error categories distinguish a refused request, unavailable Ceph
feature, missing image capability, environment limit and failed native command.
Unclassified parse/readback failures and unknown outcomes still exist.
Context errors keep their own contract. Follow
[Classified errors](errors.md) rather than treating every error as retryable.

## Messenger bootstrap and connection policy

The default mode is unchanged. V2-only protocol selection and encrypted
connection selection are separate choices:

| Fixture choice | Protocols | Connection policy |
| --- | --- | --- |
| `MessengerDefault` | v1 and v2 | Ceph defaults, with configurable v2 service/client policies |
| `MessengerV2Only` | v2 only | CRC, secure or a per-daemon mix selected with `WithMessengerPolicy` |
| `MessengerV2Secure` | v2 only | All six daemon/client/cluster mode settings fixed to secure |

Both v2-only modes remove the v1 listener and address. In bridge mode,
`MappedPort(ctx, "6789/tcp")` fails; read the bootstrap addresses or the mapped
`3300/tcp` port instead. A mapped MON port still does not expose MGRs or OSDs.

`WithMessengerPolicy` controls what MONs, MGRs and OSDs accept from clients.
It leaves daemon-to-daemon modes unchanged and cannot be combined with
`MessengerV2Secure`. `TemporaryMessengerPolicy` affects new connections;
established connections keep their negotiated mode. Its non-nil handle must
be kept even when application partly fails, then restored with a fresh
context. An empty policy, an unknown connection mode and any use under
`MessengerV2Secure` are refused before any change with `ErrInvalidRequest`.
`WithClientMessengerPolicy` configures only the attached client container,
including intentional mismatch tests; it does not change the fixture's control
CLI or a host client's configuration.

`TemporaryConfig` refuses `ms_bind_msgr1` and `ms_bind_msgr2` in both v2-only
modes. Secure-only also refuses the six mode settings; these refusals match
`ErrInvalidRequest`. For field definitions, precedence and native checks,
see [WithMessengerMode](../ceph.md#withmessengermode).

## Authentication and storage choices

Use `WithAuth` to make key type, MonMap service/allowed/preferred ciphers and
ticket lifetimes explicit. `aes256k` needs the target tools' key-type and
cipher support (present in the default Tentacle 20.2.4 image); Squid 19.2.5
and unmodified Tentacle 20.2.3 support only `aes`. The fixture checks
capabilities and refuses an unavailable type rather than falling back.
Without explicit settings, the image's defaults apply.

Ticket lifetimes set by `WithAuth` are written to local configuration files;
a later central `TemporaryConfig` does not change them. Keep MON ticket
lifetimes at least ten seconds for ordinary tests. Shorter lifetimes can
strand native daemon MON sessions; the 1.5-second regression profile is
optional. Longer lifetimes do not protect a daemon from a MON pause that
outlasts them; see [Remaining native limits](#remaining-native-limits). Read
[WithAuth](../ceph.md#withauth) before designing expiry tests. Key
replacement/revocation affects fresh authentication; existing sessions
can keep tickets. Keep the descriptor returned by `ReplaceClientKey`;
containers attached with the old descriptor retain the old keyring.

BlueStore remains the default. MemStore is useful for the fast replicated
mutation profile and keeps data across a clean stop, but loses it on a kill.
Memory-backed BlueStore (`WithOSDInMemoryStorage`) is a separate option that
keeps the BlueStore store on a Docker tmpfs volume. Crash-durability
assertions need the appropriate BlueStore fixture; see
[OSD storage](../ceph.md#withosdobjectstore).

## Native commands, outcomes and images

`RunNativeCommand` is new in this release. It reports a nonzero exit in
`NativeCommandResult.ExitCode`, not as a Go error; a timeout or truncated
output returns `ErrNativeCommandIncomplete`.

Cancellation stops a supervised command's process group; it does not undo a
MON command or an OSD mutation that was already submitted. Preserve partial
results, `NativeCommandUnstoppedError` and its `Stop` obligation. Cephx
identity changes settle submitted MON work and report the observed outcome;
check the operation's result before retrying. `Ceph` reconnects only after a
CLI connection timeout before sending a command, bounded by the context and
a minute; it does not replay a mutation after an uncertain send.

Run native oracles in a `control`/`all` client image. A supervised command in
an OSD, MDS or RGW role container also needs `python3`, which those role
contracts do not require. The baseline still uses only official-image tools
and installs nothing. Netem packet delay is an explicit helper-image
extension requiring `NET_ADMIN` on the helper and kernel netem support;
selecting it does not alter the Ceph role images or require privileged mode.

See [Native commands](native-commands.md),
[Contexts and cleanup](contexts-and-cleanup.md) and
[Releases and images](releases-and-images.md) for the contracts.

## Remaining native limits

- CephFS rebalance now serializes removals on an idle policy. External
  directory changes or mirror daemons joining/leaving can still trigger the
  upstream MGR hang; see [CephFS limitations](../cephfs.md#limitations-and-release-differences).
- A MON paused for longer than the MON ticket lifetime can strand a daemon
  whose MON session was on it: its reconnect is refused with `EACCES`, and it
  stops authenticating until it is restarted. With 12-second tickets and a
  25-second pause, this happened to the active MGR in two of four native
  runs. See [Daemon controls](messenger-client-fixtures.md#daemon-controls).
- Intermittent MON join timeout and long-lived session loss after replacing
  every MON remain under investigation in [#61](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/61).
  A later green retry does not establish their cause or a fix.
- Preparation, runner and storage/health observation investigations remain
  in [#31](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/31).
  Closing the historical [#41 audit](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/41)
  retained these owners and the original failures.

The [Messenger fixture guide](messenger-client-fixtures.md) records each
profile and native receipt. Full Messenger/MDS/daemon-path coverage is a
separate roadmap, not a claim of this release.

## Installing and adopting the release

Select the root module tag in the consumer's integration module:

```sh
go get github.com/JSYoo5B/ceph-testcontainers-go@v0.2.0
```

All four packages share this tag. Remove test-only workspace or `replace`
wiring when validating the published dependency, so the checks exercise the
release rather than a local checkout. Compile the consuming module and run
its actual client profiles, including restricted auth, mode mismatches,
address reachability and restart/readback assertions. Retain the old adapter
until those tests pass. For development candidates of a later release, see
[Preparing a release](../contributing/releasing.md).
