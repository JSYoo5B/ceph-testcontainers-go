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
   on each member; `Set.Status` reads the set without renewing its timer.
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
