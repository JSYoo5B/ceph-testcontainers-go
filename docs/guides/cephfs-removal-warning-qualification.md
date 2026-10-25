# CephFS removal with MDS health warnings

[#125](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/125) first
qualified a real difference in ordinary `fs fail` behavior, then chose a
general graceful-removal algorithm instead of registering a compatibility tag.
The public precondition remains: stop target clients, mirrors and subvolume
operations before requesting destructive removal.

## Ordinary graceful removal

The module sets only target B down, waits for that same filesystem ID to
report `max_mds=0` and an explicitly present, empty `mdsmap.up`, then runs the
confirmed filesystem removal. It does not fail the target ranks or retry
with a stronger confirmation flag. Missing/incomplete map evidence, changed
filesystem identity, cancellation or a drain timeout prevent filesystem and
pool deletion; the owned descriptor remains available after an error.

A target warning is handled by completion of a graceful drain, rather than
ignoring the warning and forcing a rank failure. If an MDS cannot stop or
flush, the drain cannot establish the removal predicate. This is an explicit
contract decision, not a promise that every warning will permit removal.

## Native contract controls

`TestCephFSRemovalWarningQualification` uses separate owned fixtures for three
conditions. Every final control expects ordinary public removal to succeed
on both qualified builds, without release-string dispatch or bug selection:

- `unrelated`: only A's MDS has the relevant native warning. B also has an
  owned standby-replay daemon and a pool attached after its initial creation.
- `target`: only B's MDS has the warning; its clients have already closed.
  Successful graceful drain and explicit target cleanup are required.
- `none`: neither MDS has `MDS_CACHE_OVERSIZED` or `MDS_TRIM`.

Both filesystems receive independent bytes before warning induction. The
fixture records the selected owned daemon's actual runtime cache limit, sets
that daemon to one byte once and reads it back. A low value alone is not the
warning oracle: native health detail must attribute a relevant warning
exclusively to the selected MDS within 90 seconds. MDS identity and filesystem
association are recorded independently through the native filesystem status.

The MON configuration database alone is insufficient when the fixture's
MDS command-line limit has higher priority. The scenario uses a supervised
runtime config command, not a host setting or a global MDS override.
An unconfirmed invocation is stopped before restoration. Fresh bounded cleanup
checks the original filesystem ID, native MDS name/GID, container start
generation and current value before restoring; an outside value is preserved as a conflict. This is a
read-before-write guard, not atomic compare-and-swap.

For A's warning, the prior runtime limit is restored, native warning absence
is observed, and a fresh client checks A's exact bytes. For target B's warning,
its ephemeral override is recorded as disposed with the owned daemon only
after container, auth-key and pool cleanup is confirmed; it is not called a
restored setting on a live daemon. If removal fails while that daemon is
still owned and running, the fresh-context restoration path remains active.

All controls verify A's filesystem ID, MDS name/GID, pools and complete file
contents. B must disappear from the native FS list and module ownership;
every captured B MDS container and Cephx key must be absent, and B's configured
pools must be gone. The later-attached pool must survive. Runtime teardown
checking is separate from these assertions and from the public result.

Run the final ordinary contract on an eligible Linux Docker engine with
essential/client/cephfs images resolved to immutable identities:

```sh
CGO_ENABLED=0 go test -mod=readonly -tags=all,native_regression \
  -run '^TestCephFSRemovalWarningQualification$' -count=1 -timeout=20m -v \
  ./internal/integration
```

The optional source-owned batch is `cephfs_removal_warning`. Attempt receipts
retain ordinary failures even when subsequent assertions fail. A final receipt
requires all scenario assertions and configuration restoration/disposal.
No successful receipt follows from a skip or incomplete cleanup.

## Observed initial cohorts

Both observations used implementation source
`1c6d9847e2ffde6adf25bc8fdb75cb31705f5d88`, Linux/amd64, no bug selection,
and the official essential/client/cephfs image digests in the
[checked-in image map](../../.github/ceph-role-images.json). The receipt keeps
all three resolved image IDs and the actual native daemon build inventory.

| Native build | Actual warning on A alone | Ordinary B removal | Qualification run |
| --- | --- | --- | --- |
| 19.2.5, `abc7aa7f2701e5d46878fd5e6bb7e2955f1a395a` | `MDS_CACHE_OVERSIZED`, 57 kB / 1 B | Refused at `fs fail`, native `EPERM` requesting confirmation | Expected-refusal case passed, 68.00 s; cleanup checker passed |
| 20.2.4, `7f793731f1b39eb4f465e960113d2363c311b964` | `MDS_CACHE_OVERSIZED`, 54 kB / 1 B | Removed B | Success case passed, 66.52 s; cleanup checker passed |

A's active generation, pools and independent file read were preserved in both
cohorts. The actual runtime limit returned from `134217728` to `1` and back to
`134217728`. These two image/build observations do not establish all patch
boundaries or the behavior of a same-release distribution/custom backport.

## Evidence boundaries

The table records the historical `1c6d984` fixture and original `fs fail`
implementation. That diagnostic baseline required an explicit caller cohort
expectation (`refused` or `removed`); the maintained final contract no longer
uses that environment input and expects graceful removal for all controls.
Historical expected-refusal PASS means the actual public operation refused,
not that it removed B. Original failures and their source/image provenance
remain separate from the final algorithm's qualification results.

These observations do not qualify every patch release, all warning classes,
malicious/active target clients or a same-reported-release fixed custom or
distribution-backport image. No such custom build has been exercised here.

The general algorithm and its target-warning contract require qualification
on both build cohorts before native acceptance is reported. No bug tag is registered or selected. Consequently this work
does not supply #24's separate real ordinary/compatibility implementation-pair
criterion; a justified real tag case would need independent qualification.
