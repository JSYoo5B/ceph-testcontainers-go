# RBD expect

Import `github.com/JSYoo5B/ceph-testcontainers-go/rbd/expect` alongside
`rbd` or `rbd/mirror` when a test wants a precise verdict on an image client,
receiver or image replay observation it already captured, with an explanation
of each mismatch. An image or a link needs nothing from this package, and a
test that only observes an image or runs, observes or watches a link can
ignore it. Neither `rbd` nor `rbd/mirror` imports it, so a test that uses only
them does not depend on it. It imports both for the observation types. See
[API purpose and purpose tags](../../docs/guides/api-purpose.md) for where
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

## Image clients

`expect.ImageClients` states what one `rbd.ImageClients` observation must
report for an image's managed exclusive lock and watchers. `Lock` requires
the lock held or free (advisory locks are not read), `Owner` names the entity
that must hold it, and `OwnerWatches` requires the holder's address among
the watchers. `Departed` lists addresses, such as a former owner's, that must
no longer watch, and `Unwatched` requires no watcher at all. Clients the
expectation does not name are not compared, and a wrong owner suppresses its
watch comparison.

The watcher and lock lists come from separate native reads, so during a
handoff an observation can name the new owner before its watch appears. An
`owner-watch` mismatch is then a possible observed state; the test decides
whether to read again.

```golang
status, err := rbd.ImageClients(ctx, cluster, pool, "", image)
if err != nil {
	t.Fatal(err) // a failed query is never a mismatch
}
result, err := expect.ImageClients{
	Lock: expect.ExclusiveLockHeld, Owner: "client.4102", OwnerWatches: true, Departed: []string{former.Address},
}.Evaluate(*status)
if err != nil {
	t.Fatal(err)
}
if !result.Matched {
	t.Fatal(result) // image clients want lock=held owner=client.4102 owner-watches departed=[10.0.0.1:0/1001]: departed: 10.0.0.1:0/1001 still watches
}
```

## Receiver cohort

`expect.Receivers` states what selected owned receivers must report in one
`ReceiverStatus` observation, from `ReceiverStatus`, `WaitReceiverReady` or a
`WatchReceivers` event. Each `ReceiverDaemon` is running (the process runs,
reports the pool running and has discovered the namespace mapping) or
stopped. A `Baseline` from an earlier observation pins the container, so a
same-name replacement does not pass, and `NewInstance` requires a native
instance other than the baseline's, as after a same-container restart. `Leader`
names the daemon that must claim its own instance as leader and whose instance
every selected running receiver must report as leader, `Ready` requires the
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

`expect.MirrorImage` states what one `ImageStatus` observation, from `ImageStatus`,
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
result, err := expect.MirrorImage{Select: initial, Replaying: true, NewInstance: true}.Evaluate(restarted)
if err != nil {
	t.Fatal(err)
}
if !result.Matched {
	t.Fatal(result) // image volumes/:/volume want replaying=true new instance: instance: want other than 4101, observed 4101
}
```
