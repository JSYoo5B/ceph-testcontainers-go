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
// through [Config]; the finalization of a passing run collects nothing, and
// only a before-deadline archive taken while the test still ran remains.
// Nothing in this package is
// needed to run a cluster: a test that terminates its cluster with
// testcontainers.CleanupContainer does not import it.
//
// [TerminateOnCleanup] and [Finalization.Verify] return an error, and a
// refused configuration matches ceph.ErrInvalidRequest, as in package ceph.
// [Finalization.Check], [Finalization.Cleanup], [Finalization.Capture],
// [Finalization.CaptureWithFile] and [Finalization.TrackDebugLevel] return
// nothing: a refused registration fails the test.
package finalize
