# Finalization

Import `github.com/JSYoo5B/ceph-testcontainers-go/ceph/finalize` alongside `ceph` to
end a test's dedicated cluster on one ordered cleanup path: post-test checks,
verification of the daemons' log lines and new crashes against expected
events, a failure archive of diagnostics and crash metadata when the test has
failed, caller cleanups, then `Terminate`. Nothing in this package is needed to
run a cluster: a cluster terminated with `testcontainers.CleanupContainer`
needs nothing from finalize. See
[API purpose and purpose tags](../../docs/guides/api-purpose.md) for where this
fits.

## Order of use

1. **Start a cluster** with `ceph.Run`.
2. **Register finalization** with `finalize.TerminateOnCleanup(t, cluster, config)`
   right after `Run`, instead of another cleanup that terminates the cluster.
   A partially started cluster returned with an error is still finalized.
3. **Add steps** to the returned `Finalization`:

   | Step | Method |
   | --- | --- |
   | A post-test check | `Check` |
   | Log and crash expectations for the test window | `Verify` |
   | A packet capture to settle (metadata, or with its raw file) | `Capture`, `CaptureWithFile` |
   | An `override.SetDebugLevel` window to record in the manifest | `TrackDebugLevel` |
   | The removal of a caller-owned client the archive should still see | `Cleanup` |

4. **Run the SUT.** The registered cleanup does the rest.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage)
finalization, ferr := finalize.TerminateOnCleanup(t, cluster, finalize.Config{Archive: true})
if err != nil { // a partially started cluster is still finalized
	t.Fatal(err)
}
if ferr != nil {
	t.Fatal(ferr)
}
finalization.Check("pool still clean", func(ctx context.Context) error {
	return cluster.WaitForClean(ctx)
})
```

## Concepts and limits

- The cleanup runs, in this order: the checks in registration order, the
  settlement of registered captures, the failure archive (only when the test
  has failed by then), the cleanups in reverse registration order, then
  `Terminate`. A check or a capture settlement that fails the test triggers
  the archive too; a cleanup that fails when nothing had failed before
  triggers an archive of what still runs.
- Test cleanups registered after `TerminateOnCleanup` run before all of this,
  as Go runs cleanups last-in first-out. Route the removal of clients the
  archive should see through `Cleanup`.
- Every phase uses a fresh context bounded by its own timeout in `Config`
  (`CheckTimeout`, `ArchiveTimeout`, `CleanupTimeout`), because the test's
  context has ended. Check, collection and storage errors never skip the
  cleanups or `Terminate`. A check's error and `Terminate`'s error fail the
  test; collection and storage errors are logged and recorded in the
  manifest.
- The archive holds a manifest and the reports of `diagnostics.Collect` and
  `diagnostics.CollectCrashMetadata`, configured through `Config.Diagnostics`
  and `Config.CrashMetadata`, plus verification and capture reports. A passing
  run collects nothing. By default it goes into the test's `ArtifactDir`,
  which `go test` keeps only with `-artifacts`.
- A verification that is unexpected or incomplete fails the test. It never
  changes the cluster, and clean is evidence for the reported sources and
  window only.
- Adding a step after finalization has started fails the test. Nothing runs
  after a forced process exit or a lost runner.

## Reference

The full contract of every type and function is in go doc:
[pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph/finalize)
or `go doc -all github.com/JSYoo5B/ceph-testcontainers-go/ceph/finalize`. The
archive layout, the manifest and the verification model are in
[Ceph operations](../../docs/guides/ceph-operations.md#failure-archive-and-post-test-verification);
contexts in cleanup code in
[Contexts and cleanup](../../docs/guides/contexts-and-cleanup.md); standalone
collection in [diagnostics](../diagnostics/README.md).
