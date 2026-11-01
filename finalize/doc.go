// Package finalize ends a test's dedicated Ceph cluster on one ordered
// cleanup path. [TerminateOnCleanup] registers a single t.Cleanup that runs
// the post-test checks, settles registered packet captures, writes a failure
// archive when the test has failed, runs the finalization cleanups and then
// terminates the cluster. The returned [Finalization] collects the checks,
// cleanups, captures and debug level windows, and [Finalization.Verify]
// activates a post-test verification of the daemons' log lines and new
// crashes against expected events.
//
// The failure archive holds a manifest and the reports of
// diagnostics.Collect and diagnostics.CollectCrashMetadata, configured
// through [Config]; a passing run collects nothing. Nothing in this package is
// needed to run a cluster: a test that terminates its cluster with
// testcontainers.CleanupContainer does not import it.
//
// The error categories are those of package ceph: a refused configuration or
// registration matches ceph.ErrInvalidRequest.
package finalize
