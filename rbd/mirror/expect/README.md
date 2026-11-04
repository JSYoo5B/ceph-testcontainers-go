# Mirror expect

Import `github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror/expect` alongside
`rbd/mirror` when a test wants a precise verdict on a receiver or image replay
observation it already captured, with an explanation of each mismatch. A link
needs nothing from this package to run, and a test that only runs, observes
or watches a link can ignore it. See
[API purpose and purpose tags](../../../docs/guides/api-purpose.md) for where
this fits.

Every expectation has one `Evaluate` method that reads only its arguments: no
query, wait, retry or `testing.T`. A mismatch is a result, listed in a
deterministic order and summarized by `String` with a bounded number of
entries. An error means there is no verdict: an invalid expectation is
`ceph.ErrInvalidRequest`, and a malformed observation is an error of its own,
never a mismatch. The result copies what it evaluated, so its diagnostics
describe that observation even after the caller reuses the input. Polling,
retries and assertion failures stay with the test. A match never proves that a
write or a checkpoint arrived: compare the destination data separately.

## Receiver cohort

`expect.Receivers` states what selected owned receivers must report in one
`ReceiverStatus` observation, from `ReceiverStatus`, `WaitReceiverReady` or a
`WatchReceivers` event. Each `ReceiverDaemon` is running (the process runs,
reports the pool running and has discovered the namespace mapping) or
stopped. A `Baseline` from an earlier observation pins the container, so a
same-name replacement does not pass, and `NewInstance` requires a native
instance other than the baseline's, as after a same-container restart. `Leader`
names the daemon that must be the agreed owned leader, `Ready` requires the
observation's own verdict and lists each daemon problem that prevents it, and
`Exact` refuses unselected cohort members and foreign instances.

```golang
baseline, err := link.WaitReceiverReady(ctx)
if err != nil {
	t.Fatal(err)
}
// ... stop leader, let the survivor take over, start leader again ...
want := expect.Receivers{
	Select: []expect.ReceiverDaemon{{Name: leader, Baseline: baseline.Daemons[leader], Running: true, NewInstance: true}},
	Ready:  true,
}
status, err := link.ReceiverStatus(ctx)
if err != nil {
	t.Fatal(err) // a failed query is never a mismatch
}
result, err := want.Evaluate(status)
if err != nil {
	t.Fatal(err)
}
if !result.Matched {
	t.Fatal(result) // receivers [a(new instance)] ready in cohort [a b]: a instance: want other than 4101, observed 4101
}
```

Unlike `WaitReceiverReady`, which waits for one readiness verdict, an
expectation also states non-ready conditions, such as a survivor that leads
while the stopped member stays in the cohort.

## Image replay

`expect.Image` states what one `ImageStatus` observation, from `ImageStatus`,
`WaitReplayReady` or a `WatchImage` event, must report for an image. `Select`
names the image and its namespaces; an earlier observation as `Select` also
pins the mode and the global, source and destination image IDs, so a
re-created image of the same name does not pass. `Replaying` requires an
owned receiver to replay the source-primary, destination-secondary pair
(`ReplayReady`, native `up+replaying`, both sides enabled); false requires only
that `ReplayReady` is false, which allows a stale native report. `Daemon` names
the receiver that must replay the image, and `NewInstance` requires a
receiving instance other than the selected one.

```golang
result, err := expect.Image{Select: initial, Replaying: true, NewInstance: true}.Evaluate(restarted)
if err != nil {
	t.Fatal(err)
}
if !result.Matched {
	t.Fatal(result) // image volumes/:/volume want replaying=true new instance: instance: want other than 4101, observed 4101
}
```
