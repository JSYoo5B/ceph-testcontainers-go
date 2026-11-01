# Packet capture

Import `github.com/JSYoo5B/ceph-testcontainers-go/capture` to record the
packets of one container endpoint around selected test traffic, so that a test
can check afterwards what went over the wire. The package starts an opt-in
`tcpdump` helper in the target's network namespace, retrieves one classic pcap
file and checks its structure. It never decodes or judges the packets;
interpreting Messenger traffic is the test's job. See
[API purpose and purpose tags](../docs/guides/api-purpose.md) for where this
fits.

## Order of use

1. **Start a target.** Any running container on a user-defined bridge: an owned
   Ceph daemon (for example a MON of a `ceph.Run` cluster) or a container the
   test owns.
2. **Start the capture** with `capture.Start(ctx, target, capture.Request{...})`.
   It returns once `tcpdump` listens; start the traffic of interest only after
   that.
3. **Run the SUT** so that it sends the traffic to observe.
4. **Stop and check** with `Session.Stop`, which returns a `capture.Report`
   (how the capture ended, whether the target changed, the file's structural
   verdict). Call `Stop` also after a failed workload; it is retryable.
   Alternatively register the session with `finalize.Finalization.Capture`
   (metadata only) or `finalize.Finalization.CaptureWithFile` (raw file into a
   failure archive), which settles it at the end of the test.

```golang
session, err := capture.Start(ctx, monitor, capture.Request{
	HelperImage: helperImage, // an image that meets packet-capture contract 1
	Network:     networkName,
	Interface:   "eth0",
	Filter:      "tcp port 3300",
	SnapLen:     65535,
	MaxPackets:  10000,
	Output:      filepath.Join(t.TempDir(), "mon.pcap"),
})
if err != nil {
	t.Fatal(err)
}
runWorkload(t)
report, err := session.Stop(ctx)
if err != nil {
	t.Fatal(err)
}
if report.End != capture.EndStopped || report.File.Status != capture.FileValid {
	t.Fatalf("capture did not cover the workload: %+v", report)
}
```

## Concepts and limits

- One endpoint, one file: a capture observes one container's interface on one
  user-defined bridge. `bridge`, `host` and `none` networks, `any`/`lo`
  interfaces, host-network targets and targets that share another container's
  namespace are refused. There is no rotation and no host-network capture.
- The helper image is the caller's choice. It must meet
  [packet-capture contract 1](https://github.com/JSYoo5B/ceph-testcontainers-images/blob/main/docs/EXTENSIONS.md);
  no helper image is published yet, so nothing selects one implicitly. The
  helper runs as the image's non-root user with `NET_RAW` as its only
  capability.
- A capture is bound to the target's container instance and network sandbox
  at readiness and never follows a restart; `Report.Target` says whether the
  target changed.
- Process, target, retrieval and file outcomes are reported separately. A
  structurally valid file does not show that the intended traffic was captured,
  that nothing was dropped, or that any message is valid. A capture that ended
  at `MaxPackets` is not an error, but later traffic is missing.
- The caller owns the session and its output file: no cluster `Terminate`
  stops it. A pcap holds payloads and identifiers that redaction does not
  sanitize; keep it only where the test deliberately retains it.

## Reference

The full contract of every type and function is in go doc:
[pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/capture)
or `go doc -all github.com/JSYoo5B/ceph-testcontainers-go/capture`. Error
categories are described in [Errors](../docs/guides/errors.md); failure
archives in [finalize](../ceph/finalize/README.md).
