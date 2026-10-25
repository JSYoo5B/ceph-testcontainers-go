# Packet capture: session and minimal file validation

This note records the increments of #52. The module collects packet captures
from a capture helper image whose contract is defined with
[ceph-testcontainers-images#8](https://github.com/JSYoo5B/ceph-testcontainers-images/issues/8)
and fixed as packet-capture contract 1 in that repository's
`docs/EXTENSIONS.md`. Collection, retrieval, minimal structural validation and
delivery belong here. Messenger decoding, decryption, protocol assertions and
dissectors belong to consumers.

The first increment added the classic pcap structure check in
`internal/pcapfile`. The second adds the public [`capture`](../capture.md)
package: one capture session on one explicitly selected endpoint.

## Capture session

`capture.Start` takes a caller-selected helper image, target container,
user-defined bridge network, interface, filter, snaplen and packet bound. It
never selects an image, interface or filter by itself and never changes the
target. The lifecycle follows contract 1:

1. Validate the request. The file bound is the contract's
   24 + packets × (16 + snaplen) bytes, at most 1 GiB.
2. Admit the target: running, unpaused, in its own network namespace and
   attached to a bridge-driver network. Its container instance (`StartedAt`),
   sandbox and endpoint MAC address are recorded as its generation.
3. Run a short-lived probe from the requested image in the target's namespace
   (`container:<target>`), as the image's own user with `NET_RAW` only and
   without `no-new-privileges`. It reports the contract's executables, the
   user, `tcpdump --version`, the filter compiled with
   `tcpdump -y EN10MB -d -- <filter>`, the namespace and the interface's MAC
   address. The probe's image ID, not the tag, is used from then on.
4. Start the helper with `tcpdump` as its process, with the contract's capture
   options and `--` before the filter. Readiness is the
   `listening on <interface>, link-type EN10MB` line on standard error
   together with the output file; a running container is not readiness. The
   target's generation is checked again after readiness.
5. `Session.Stop` sends `SIGTERM` and waits a bounded grace for exit status 0.
   Without an end it sends `SIGKILL` and reports an emergency stop. Since
   `tcpdump` is the helper's process, the container's exit status is the
   tool's, and its statistics are read from the container's standard error.
   An unprinted counter stays unknown.
6. Compare the target with its generation, copy the file out of the stopped
   helper (at most the bound; one more byte only shows that it is larger),
   check it with `internal/pcapfile`, and remove the helper. Stop works from a
   fresh bounded context, keeps its report, and retries only the removal when
   called again.

The generic command supervisor is not used for the capture itself: its
cancellation kills the process group, which is not the contract's graceful
stop. Ending the helper's own process with a signal and observing the
container's exit keeps stop and completion separate from cancellation.

Error categories are distinct. A missing helper image is
`ceph.ErrImageCapability`. An image that breaks the contract is
`capture.ErrImageUnsupported` (`ErrImageCapability`), and a refused
`NET_RAW` exec or device open is `capture.ErrPermissionDenied`
(`ErrEnvironmentUnavailable`). A bad request, filter, interface or target is
`ceph.ErrInvalidRequest`. A capture that did not become ready, did not end
cleanly, lost its target or delivered a file that is not valid classic pcap is
`capture.ErrFailed` (`ErrNativeFailure`).

## What the file check establishes

`pcapfile.Validate` reads a stream with two bounds: the file size and one
record's captured length. It returns a report with one status.

| Status | Meaning |
| --- | --- |
| `valid` | The classic pcap 2.4 header is recognized and every record header and its declared data fit the file. Zero records is valid. |
| `unsupported` | A recognized format or variant that is not checked: pcapng or another classic pcap version. |
| `malformed` | Not classic pcap, or its structure breaks: a file or record header cut off, record data cut off, a captured length above snaplen, the record bound or the original length, or a timestamp fraction out of range. |
| `limit_exceeded` | The file is larger than the bound. The part read is reported; the rest is unchecked. An early end caused by the bound is not called a cut-off file. |

The report keeps the byte order (`little`, `big`), timestamp unit
(`microsecond`, `nanosecond`), version, snaplen, link type, packet count, and
the count of records captured shorter than their original length. That count
is snaplen truncation of those packets, not a malformed file. It also keeps
the bytes read and the SHA-256 of exactly those bytes. `complete` says that
reading reached the end of the input within the bound; only then is the
digest a whole-file identity. An early verdict such as an unknown magic
number, a read error or the bound leaves it false, and the digest covers the
prefix that was read.

The byte read past the file bound only shows that the input is larger; it is
neither hashed nor decoded. The largest representable bound needs no such
byte.

A record's declared length is compared with the bounds before anything is
read, and record data is streamed through the digest without being held, so
a hostile length can neither allocate nor read its declared size. Read errors
are returned as errors, never as a verdict.

## What it does not establish

- That the intended traffic was captured, that nothing was dropped, or that
  the filter selected the right flows.
- That any packet holds a valid protocol message. Packets are not parsed.
- That capture completed: a readable file can come from a partial capture.
  The collection report must keep process and stop outcomes separately from
  file validity.

## Validation

### File check

Unit tests build captures in both byte orders and both timestamp units. They
cover valid, empty and snaplen-truncated captures; every malformed and
unsupported case above with its reason and read count; the record bound; the
file bound at several offsets with the digest of exactly the bytes read; read
errors; and reading from disk. `FuzzValidate` checks that any input yields a
verdict within its bounds. These are offline checks of the parser, not capture
evidence.

### Capture session

Unit tests in `capture` replace Docker with a model of one target and one
helper. They cover request and target refusals, an existing output, every
probe classification with negative controls against the other error kinds,
readiness that needs both the line for the selected interface and the file,
target change during start, packet-limit, killed, failed, target-gone,
cut-off, pcapng and over-bound ends, unknown statistics, an expired workload
context, and a removal that is retried. These are offline checks of the
lifecycle, not capture evidence.

`TestCaptureTargetTraffic` is the native scenario, in the `environment`
category's `packet_capture` batch, which the Docker checks workflow runs on
its Linux runner. No helper is published yet, so it builds the reference helper
from the ceph-testcontainers-images commit that merged contract 1 (the
engine builds that repository's Dockerfile; this module produces no image),
unless `CEPH_TEST_CAPTURE_IMAGE` names another image. On a private bridge it
captures HTTP traffic between a caller-owned busybox target and a client,
selected by port and client address, while a second flow on another port must
be excluded. It requires a graceful stop, a valid file with both directions and
the response payload, statistics that match the file, a file still readable
after helper removal, an unchanged target, a valid empty capture, and refused
filter and interface requests that leave no output.

The same scenario then covers the negative and partial cases:

- Packet limit: with `MaxPackets` 4, `tcpdump` must end on its own before
  `Stop`, which reports `EndPacketLimit` without an error and a valid file of
  exactly four packets of the selected flow.
- Canceled workload: `Stop` receives the workload's canceled context and still
  stops gracefully, delivers a valid file and removes the helper.
- Denied `NET_RAW`: the public API always grants `NET_RAW`, so a test-only hook
  in the package starts the capture helper (not the probe) without it. The
  engine must refuse it before readiness as `capture.ErrPermissionDenied`,
  with no session, helper or output left. This shows the classification of a
  withheld capability, not a particular engine's policy.
- Target removed or restarted during a capture: each uses its own target.
  `Stop` must join `capture.ErrFailed`, report `TargetGone` or `TargetChanged`,
  deliver a valid file of the traffic before the loss and remove the helper.
  When `tcpdump` ends on its own after the loss, its end must be `EndFailed`
  with a nonzero status; otherwise `Stop` ends it.

A pass is evidence only for the runner's engine and architecture
(`linux/amd64`). `EndKilled` (the stop grace expired and `Stop` killed the
helper) and a cut-off final record stay offline checks in this scenario: the
reference helper always ends within the grace on `SIGTERM`.

### Killed helper observation

`TestCaptureKilledHelperObservation` (`environment` category,
`packet_capture_killed` batch, in `capture`) records what a real
capture holds when its helper is killed from outside. It captures the
requests of a client loop to a caller-owned target, waits for readiness, a
completed request and 32 KiB of written file, freezes the helper to copy a
snapshot of the file, unfreezes it and sends `SIGKILL` to that helper while
the client keeps requesting. It then calls `Stop`, which finds a helper that
already ended, and runs the existing parser over exactly the delivered bytes
again, independently of the report. The outcome (end reason, exit status,
tail shape, record count, byte count, SHA256, statistics with an absent
counter kept "unknown") is recorded as observed. It asserts only invariants:
the helper ran before and stopped after the kill with a nonzero engine
status, the client completed requests after the kill, the snapshot holds
whole records and is a byte prefix of the delivered file, the delivered bytes
are a prefix of a valid classic pcap file (a clean end or a cut final record
header or record data), the report describes those same bytes, the capture is
not reported as stopped or at the packet limit, the helper is removed and the
target is unchanged. Docker-free fixtures cover the tail classification and
every invariant with negative controls.

This is an external `SIGKILL` of the helper, not an `EndKilled` capture. It
does not show where the cut falls (a clean end is the likely result of
`tcpdump -U`), that a cut final record occurs, stability across runs,
`linux/arm64`, other engines or a published helper.

## Next steps

1. Each declared architecture separately, `linux/arm64` included, once the
   helper is published.
2. Integration with #49's failure bundle, keeping raw pcap retention and upload
   deliberate and capture usable on its own.
