# Packet capture: minimal file validation

This is the first increment of #52. The module will collect packet captures
from a capture helper image whose contract is defined with
[ceph-testcontainers-images#8](https://github.com/JSYoo5B/ceph-testcontainers-images/issues/8).
Collection, retrieval, minimal structural validation and delivery belong
here. Messenger decoding, decryption, protocol assertions and dissectors
belong to consumers.

The capture lifecycle (helper start, readiness, graceful stop and flush,
limits, statistics, target generation and cleanup) waits for that contract.
This increment adds only the part that does not depend on it: the classic
pcap structure check in `internal/pcapfile`. No public API is added yet.

## What the check establishes

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

Unit tests build captures in both byte orders and both timestamp units. They
cover valid, empty and snaplen-truncated captures; every malformed and
unsupported case above with its reason and read count; the record bound; the
file bound at several offsets with the digest of exactly the bytes read; read
errors; and reading from disk. `FuzzValidate` checks that any input yields a
verdict within its bounds. These are offline checks of the parser, not capture
evidence.

## Next steps

1. Agree on the helper contract with images#8: readiness signal, graceful stop
   and flush, output bounds and statistics.
2. Add the capture session: explicit target and filter, readiness before the
   workload, graceful stop, retrieval, validation with this package, and a
   structured report that keeps unknown statistics unknown.
3. Prove it natively per the #52 acceptance list, separately per architecture.
