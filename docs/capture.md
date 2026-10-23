# Packet capture

Import `github.com/JSYoo5B/ceph-testcontainers-go/capture` to record the
packets of one container endpoint around selected test traffic. The package
starts an opt-in capture helper in the target's network namespace, retrieves
one classic pcap file and checks its structure. It never decodes, decrypts or
judges the packets: Messenger interpretation belongs to consumers. A capture
observes one endpoint, not a cluster.

This is the first increment of
[#52](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/52). It
supports one explicitly selected endpoint on a user-defined bridge, one file,
no rotation and no host-network capture. A capture never follows its target
across a restart. See the [design note](design/packet-capture.md) for what is
covered and what is left.

## Start

```golang
func Start(ctx context.Context, target testcontainers.Container, req capture.Request) (*capture.Session, error)
```

Starts `tcpdump` in `target`'s network namespace and returns once it listens
there. `req.HelperImage` names an image that meets
[packet-capture contract 1](https://github.com/JSYoo5B/ceph-testcontainers-images/blob/main/docs/EXTENSIONS.md).
No helper image is published yet, so there is no default and nothing selects
one implicitly. The helper joins the namespace (Docker network mode
`container:<target>`) with `NET_RAW` as its only capability, without
`no-new-privileges`, and as the image's own non-root user. `target` keeps its
image, capabilities and networking. It can be an owned Ceph daemon or a
caller-owned container attached to the bridge; it must be running, not paused,
with its own network namespace.

| `capture.Request` field | Meaning |
| --- | --- |
| `HelperImage` | The helper image. Required. |
| `Network` | The user-defined bridge network of the endpoint. `bridge`, `host` and `none` are refused. |
| `Interface` | The endpoint's interface inside the target's namespace, such as `eth0`. Its MAC address must be that of the endpoint on `Network`. `any` and `lo` are refused. |
| `Filter` | A pcap-filter expression, at most `MaxFilterBytes` (4096) bytes. Required: select the endpoints and ports of interest instead of assuming one Messenger port covers every role. |
| `SnapLen` | The bytes kept of each packet, 1 to `MaxSnapLen` (262144). |
| `MaxPackets` | Ends the capture after this many packets. The file is bounded by 24 + `MaxPackets` × (16 + `SnapLen`) bytes, which must not exceed `MaxFileBytes` (1 GiB). |
| `Output` | The file that `Stop` writes the capture to. It must not exist; `Start` creates it, so parallel or repeated tests need distinct paths. |
| `ReadyTimeout` | The readiness wait; zero is `DefaultReadyTimeout` (30 s), at most `MaxTimeout` (5 min). |
| `StopGrace` | The wait for `tcpdump` to end after `SIGTERM`; zero is `DefaultStopGrace` (10 s), at most `MaxTimeout`. |

Before the capture starts, a short-lived probe from the same image in the same
namespace checks the contract's executables and user, runs `tcpdump --version`,
compiles the filter with `tcpdump -y EN10MB -d` and reads the interface's MAC
address. The capture helper then runs the image ID that the probe ran, not the
tag:

```text
tcpdump -p -i <Interface> -s <SnapLen> -c <MaxPackets> --immediate-mode -U -w <file> -- <Filter>
```

Readiness is `tcpdump`'s `listening on <Interface>, link-type EN10MB` line
together with its output file. A running helper or an existing file alone is
not readiness. Once ready, `Start` checks that the target still has the same
container instance and network sandbox, and binds the session to them. Start
the workload of interest only after `Start` returns.

| Failure | Category | Narrower error |
| --- | --- | --- |
| No `HelperImage`: the extension is not selected | `ErrImageCapability` | |
| A missing `sh`, `sleep`, `tcpdump` or `python3`, a `tcpdump` that does not run or compile, or a helper that runs as root | `ErrImageCapability` | `capture.ErrImageUnsupported` |
| The engine refused to run `tcpdump` with `NET_RAW` or to open its device | `ErrEnvironmentUnavailable` | `capture.ErrPermissionDenied` |
| A malformed request, an existing `Output`, a filter that does not compile, an interface that is not the endpoint's, or a refused target | `ErrInvalidRequest` | |
| `tcpdump` exited before readiness, was not ready in time, or the target changed while it started | `ErrNativeFailure` | `capture.ErrFailed` |

After a failure `Start` removes the helper and the output it created. If it
cannot remove the helper, it returns the session as well; call `Stop` to retry
the removal.

## Session.Stop

```golang
func (s *Session) Stop(ctx context.Context) (capture.Report, error)
```

Sends `SIGTERM` and waits up to `StopGrace` for `tcpdump` to flush and close its
file; after that it kills the helper. It then compares the target with the
generation it joined, copies the file into `Output`, checks its structure with
bounded reads, and removes the helper. Its Docker work uses a fresh bounded
context, so a canceled or expired workload context does not leave the helper
behind. The caller owns the session; no cluster `Terminate` knows it. Call
`Stop` also after a failed workload, and keep the workload's own error
alongside Stop's.

`Stop` returns the report also with an error. The error joins
`capture.ErrFailed` for a capture that failed, was killed, lost its target or
delivered a file that is not valid classic pcap, and any removal error. A
capture that ended at `MaxPackets` is not an error, but its `End` says that
later traffic is missing. After a removal error `Stop` can be called again: it
retries the removal and returns the same report.

## Report

`capture.Report` keeps the process, target, retrieval and file outcomes
separate. A structurally valid file does not show that the intended traffic was
captured, that nothing was dropped, or that any protocol message is valid.

| Field | Meaning |
| --- | --- |
| `ContainerID`, `Network`, `Interface`, `MACAddress` | The captured endpoint. |
| `SandboxID`, `TargetStartedAt`, `Namespace` | The network namespace generation that the helper joined. |
| `HelperImage`, `HelperImageID`, `HelperPlatform`, `ToolVersion` | The requested image, the image that ran, its os/architecture and `tcpdump --version`. |
| `Filter`, `SnapLen`, `MaxPackets`, `MaxBytes` | The selection and bounds; `MaxBytes` is derived from the other two. |
| `StartedAt`, `ReadyAt`, `StopRequestedAt`, `EndedAt` | Helper start, readiness, the start of `Stop`, and the end of `tcpdump`. |
| `End`, `ExitCode` | How `tcpdump` ended, a `capture.EndReason`: `EndStopped` (status 0 after `SIGTERM`), `EndPacketLimit` (status 0 before `Stop`, with a valid file of `MaxPackets` packets and a captured counter, when printed, that agrees), `EndPremature` (status 0 before `Stop` without reaching `MaxPackets`, as after a signal from elsewhere; an error), `EndKilled` (no end within the grace), `EndFailed` (nonzero status) or `EndUnknown` (not observed, or an end before `Stop` without a valid file to decide it). |
| `Target` | A `capture.TargetState`: `TargetUnchanged`, `TargetChanged` (restarted, stopped or another sandbox), `TargetGone` or `TargetUnknown`. |
| `Statistics` | `tcpdump`'s counters as a `capture.Statistics`: `Captured`, `ReceivedByFilter`, `DroppedByKernel` and `DroppedByInterface`. A nil counter was not printed and is unknown, not zero. |
| `Stderr` | `tcpdump`'s standard error, up to 64 KiB. |
| `Output`, `Retrieved`, `RetrievedBytes` | The delivered file and whether, and how much of, the helper's file was copied into it. At most `MaxBytes` are kept. |
| `File` | A `capture.FileReport` of `Output`; its `Status` is empty when nothing was retrieved. |
| `HelperRemoved` | The helper container was removed. |

`FileReport.Status` is a `capture.FileStatus`: `FileValid` (every record header
and its data fit the file; zero packets is valid), `FileUnsupported` (a
recognized format that is not checked, such as pcapng), `FileMalformed` (not
classic pcap, or a broken or cut-off structure) or `FileLimitExceeded` (the
helper's file is larger than `MaxBytes`). `Reason` explains any other status
than `FileValid`. `ByteOrder`, `TimestampUnit`, `VersionMajor`, `VersionMinor`,
`SnapLen` and `LinkType` are the header's. `Packets` counts the complete records
and `TruncatedPackets` those captured shorter than their original length,
which is snaplen truncation, not a malformed file. `SHA256` covers exactly
`BytesRead`; only when `Complete` is true does it identify the whole file.

The report carries JSON field tags for delivery. A pcap holds payloads,
addresses and identifiers; text and JSON redaction do not sanitize it, so
keep capture files only where the caller deliberately selects them.
