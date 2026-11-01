# Diagnostics

Import `github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics` alongside `ceph`
to collect read-only evidence from a cluster for a failed test to save or
report: a diagnostics report of its containers, networks, log tails and fixed
Ceph queries, the crash metadata that its daemons wrote, and the redaction of
the secrets that the cluster knows. Collection never starts, stops,
reconfigures or adopts a container. A cluster needs nothing from this package
to run. See [API purpose and purpose tags](../../docs/guides/api-purpose.md) for
where this fits.

## Order of use

1. **Start a cluster** with `ceph.Run`.
2. **Run the test.** Nothing is recorded in advance.
3. **Collect** from a cleanup, with a fresh context of its own:

   | Evidence | Function | Report |
   | --- | --- | --- |
   | Container state, network addresses, log tails, fixed Ceph queries | `Collect` | `Report` |
   | Crash metadata files in `ceph.CrashDir` of the daemon containers | `CollectCrashMetadata` | `CrashMetadataReport` |
   | Output with the cluster's secrets replaced by `[REDACTED]` | `Redact` | the masked bytes |

```golang
t.Cleanup(func() {
	if !t.Failed() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	report, err := diagnostics.Collect(ctx, cluster, diagnostics.Config{})
	if report != nil {
		data, _ := json.MarshalIndent(report, "", "  ")
		t.Logf("ceph diagnostics:\n%s", data)
	}
	if err != nil {
		t.Logf("diagnostics incomplete: %v", err)
	}
})
```

To collect both reports into a failure archive on one ordered cleanup path,
use the [finalize](../finalize/README.md) package instead.

## Concepts and limits

- Collection is standalone and read-only. It works on a partially started
  cluster, after `Terminate`, and on stopped or removed containers. It runs
  no arbitrary command and reads no file, environment, keyring, auth database
  or mount source; `CollectCrashMetadata` reads only crash `meta` files
  through Docker's archive API.
- A partial report is returned together with the aggregate error when some
  observations fail: keep the report. Truncation alone is not an error.
  `Report.Complete` means every observation succeeded untruncated, not that
  Ceph is healthy.
- A canceled test context cannot collect: `t.Context()` has ended when
  cleanups run.
- `Config` and `CrashMetadataConfig` bound time, bytes, records and
  concurrency; zero fields select defaults, and invalid limits are refused
  with an error that matches `ceph.ErrInvalidRequest`, without a report.
  Caller-owned clients and mirror daemons are added through
  `AdditionalContainers` (`Target`) and are never adopted.
- Redaction is best effort: known admin and service credentials, every client
  key the cluster generated, `RedactValues` and recognized secret patterns are
  masked. Log contents and crash metadata can still hold host names, paths
  and application secrets; review a report before sharing it. An oracle
  compares the raw bytes, not the output of `Redact`.
- No crash records is not proof that no daemon crashed: a container removed
  earlier took its records along.

## Reference

The full contract of every type and function is in go doc:
[pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics)
or `go doc -all github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics`.
Collection from cleanup is in
[Contexts and cleanup](../../docs/guides/contexts-and-cleanup.md#collect-diagnostics-before-cleanup),
the collected queries in
[Running native commands](../../docs/guides/native-commands.md#diagnostics), and
the failure archive layout in
[Ceph operations](../../docs/guides/ceph-operations.md#the-failure-archive).
