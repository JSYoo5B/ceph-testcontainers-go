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
if session != nil {
	t.Cleanup(func() {
		if _, err := session.Stop(context.Background()); err != nil {
			t.Errorf("stop capture: %v", err)
		}
	})
}
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
  select the reference helper described there or another conforming image
  explicitly. The helper runs as the image's non-root user with `NET_RAW` as
  its only capability.
- A capture is bound to the target's container instance and network sandbox
  at readiness and never follows a restart; `Report.Target` says whether the
  target changed.
- Process, target, retrieval and file outcomes are reported separately. A
  structurally valid file does not show that the intended traffic was captured,
  that nothing was dropped, or that any message is valid. A capture that ended
  at `MaxPackets` is not an error, but later traffic is missing.
- Start creates the output file exclusively after helper readiness. If another
  writer claims the name first, Start leaves that file in place. A failed
  start can return a session for retrying probe or helper cleanup with `Stop`.
- The caller owns the session and its output file: no cluster `Terminate`
  stops it. A pcap holds payloads and identifiers that redaction does not
  sanitize; keep it only where the test deliberately retains it.

## Restarting the target

A session never follows a restart, so evidence around a deliberate restart of
the captured peer is a sequence of sessions that the caller composes, one per
target generation. `TestCaptureAcrossTargetRestart`
(`capture_restart_integration_test.go`) is the runnable recipe, with a client
in the engine host's network namespace.

- Keep the first session running through the restart. It ends with its
  generation, usually as `EndFailed` once tcpdump reports that the interface
  disappeared, with `Report.Target` `TargetChanged` and an `ErrFailed` error
  from `Stop`. Its file still holds, and the report still checks, the traffic
  captured before the restart. Then `Start` a new session on the same
  container.
- Nothing the new generation sends or accepts before the next `Start`
  returns is captured. When the handshake after the restart matters, keep it
  from happening earlier: let the target's entrypoint wait at a gate that every
  start re-arms (the recipe uses a FIFO in a tmpfs) and open it after `Start`
  returns, or hold the client until then. A target whose entrypoint the caller
  does not own, such as a daemon of `ceph.Run`, can only be gated at the
  client. Without a gate the handshake may be in no segment.
- Keep every segment's order, `Report` and error, also those of a segment that
  ended with its generation. `ContainerID` names the target, and
  `TargetStartedAt` with `SandboxID` the generation: the next generation can
  reuse the namespace inode, so `Namespace` alone does not tell them apart.
  The report also keeps the timing, filter, interface, bounds and tcpdump's
  counters (a missing counter means unknown drops, not zero). Record the time
  from the restart request to the next segment's `ReadyAt` as uncovered.
  Segments are separate files; concatenating them does not make an
  uninterrupted capture.
- A containerized client on the same bridge is a stable observer: one capture
  of the client's namespace spans the peer's restart in one file and one
  generation. It observes the client's endpoint, so it fits only when the
  client may be a container. A client on the engine host has no namespace that
  this package captures, and capturing the host bridge is outside it.

## Reference

The full contract of every type and function is in go doc:
[pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/capture)
or `go doc -all github.com/JSYoo5B/ceph-testcontainers-go/capture`. Error
categories are described in [Errors](../docs/guides/errors.md); failure
archives in [finalize](../ceph/finalize/README.md).
