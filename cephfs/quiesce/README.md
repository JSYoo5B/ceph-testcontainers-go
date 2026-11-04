# CephFS quiesce

Import `github.com/JSYoo5B/ceph-testcontainers-go/cephfs/quiesce` to pause the
I/O of owned subvolumes as one native quiesce set, so that a test can take a
consistent checkpoint (for example a snapshot of each subvolume) while the
application under test keeps its clients connected. The subvolumes come from
[`cephfs/subvolume`](../subvolume/README.md); the package starts no container
and pauses no process. See
[API purpose and purpose tags](../../docs/guides/api-purpose.md).

## Order of use

1. **Create the subvolumes** with `subvolume.Create(ctx, fs, cfg)` and let the
   SUT write to them.
2. **Start the set** with `quiesce.Start(ctx, fs, volumes, quiesce.Config{...})`.
   It creates a fresh set exclusively and returns once the members are
   `QUIESCED`, within `Timeout`; `Expiration` is the native safety timer.
3. **Take the checkpoint** while paused, for example `Subvolume.CreateSnapshot`
   on each member; `Set.Status` reads the set without renewing its timer, and
   `Set.Watch` reports when it stops being `QUIESCED`.
4. **Release** with `Set.Release`, which resumes I/O only at the version it
   confirmed, so an expired or outside-edited set is reported as an error.

```golang
set, err := quiesce.Start(ctx, fs, []*subvolume.Subvolume{a, b}, quiesce.Config{Timeout: 20 * time.Second, Expiration: time.Minute})
if err != nil {
	t.Fatal(err)
}
for _, volume := range []*subvolume.Subvolume{a, b} {
	if _, err := volume.CreateSnapshot(ctx, "checkpoint"); err != nil {
		t.Fatal(err)
	}
}
if err := set.Release(ctx); err != nil {
	t.Fatal(err) // the checkpoint may not be consistent
}
```

## Watching the set

To notice that a checkpoint lost its pause while the SUT is still copying,
`set.Watch(ctx, cfg)` polls the set's native state in one goroutine and sends
changes of its state, version, timeout and expiration on one channel. The
baseline is buffered before it returns; receive it before starting the
copy. A read never renews the expiration and never adopts a version, so
`Release` still refuses a changed set. A failed read is a `ceph.WatchGap`
event and the next success is `ceph.WatchResumed`. A `RELEASED`, `EXPIRED`,
`TIMEDOUT`, `CANCELED` or `FAILED` state is the last event, and `Stop` then
returns nil: an expired set is an outcome in that event, not a watch error.
These end the watch with `ceph.ErrInvalidRequest` instead: a set that is
absent from the native quiesce database, changed or excluded members, a
version lower than one already seen (the set was recreated), a replaced
filesystem and cluster termination; a full buffer ends it with
`ceph.ErrWatchOverflow`. A `QUIESCED` event is a sample, not a lease: the set
can expire right after it, so a missing event never proves a consistent
checkpoint, and `Release` stays the consistency result. Call `Stop` before
the fixture's cleanup: it joins the watch and returns why it ended. See
[watching state](../../docs/guides/contexts-and-cleanup.md#watching-state) for
the lifecycle that every Watch shares.

## Concepts and limits

- Members must be confirmed subvolumes of `fs` that the subvolume package
  created; each one's native identity is checked before the set is created.
- An uncertain create can return a non-nil `Set` with an error: keep it to
  release, or let its expiration pass before terminating the cluster. Never
  edit the set ID outside the fixture.
- Native sets can overlap paths; releasing this set does not cancel another.
  Set records stay in the native quiesce database until the cluster is
  disposed of.

## Reference

The full contract of every type and function is in go doc:
[pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/cephfs/quiesce)
or `go doc -all github.com/JSYoo5B/ceph-testcontainers-go/cephfs/quiesce`.
See also [Contexts and cleanup](../../docs/guides/contexts-and-cleanup.md) and
[Errors](../../docs/guides/errors.md).
