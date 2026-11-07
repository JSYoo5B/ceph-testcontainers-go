# CephFS snapshot schedules

Import `github.com/JSYoo5B/ceph-testcontainers-go/cephfs/snapschedule` to add a
snapshot schedule, with its retention, to a directory of an existing
filesystem through the MGR `snap_schedule` module, so that a test can check
how its application handles scheduled snapshots appearing and being pruned.
The filesystem stays in the [cephfs package](../README.md); the package starts
no container. See
[API purpose and purpose tags](../../docs/guides/api-purpose.md).

## Order of use

1. **Start a filesystem** and enable the module with
   `override.SetMGRModule(ctx, cluster, "snap_schedule", true)`. The active
   MGR restarts to load it, and `SetMGRModule` checks membership, not command
   readiness: wait until the MGR map reports an available manager and a
   `snap-schedule list` of the filesystem succeeds (below). For a schedule in
   minutes, also set `mgr/snap_schedule/allow_m_granularity` to `true` with
   `override.SetConfig`.
2. **Add the schedule** with `snapschedule.Add(ctx, fs, snapschedule.Config{Path:
   "/data", Repeat: "1m", Retention: map[string]int{"n": 3}})`. It returns a
   `Lease`.
3. **Run the SUT** while the module takes `scheduled-…` snapshots.
4. **Observe** with `Lease.Status` (the module's counts and times),
   `Lease.Watch` (their changes) and `Lease.Snapshots` (the scheduled
   snapshots in the directory's `.snap`).
5. **Restore** with `Lease.Restore`, which removes the retention rules and the
   schedule the lease added; the snapshots stay.

```golang
observer, err := ceph.Observe(cluster)
if err != nil {
	t.Fatal(err)
}
for {
	status, err := observer.ManagerStatus(ctx)
	if err == nil && status.Available {
		_, err = cluster.Ceph(ctx, "fs", "snap-schedule", "list", "/", "--fs", fs.FilesystemName, "--format", "json")
		if err == nil {
			break
		}
	}
	select {
	case <-ctx.Done():
		t.Fatalf("snap_schedule does not answer: %v", err)
	case <-time.After(time.Second):
	}
}
lease, err := snapschedule.Add(ctx, fs, snapschedule.Config{Path: "/", Repeat: "1m", Retention: map[string]int{"n": 3}})
if lease != nil {
	defer lease.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
snapshots, err := lease.Snapshots(ctx)
```

## Watching the schedule

`lease.Watch(ctx, cfg)` polls the module's status of the lease's schedule in
one goroutine and sends an event when its active state, its created or pruned
count, its first or last snapshot time or the directory's retention changes.
This is aggregate progress, emitted only on change, not a per-snapshot event
history: two runs between reads are one event, a read equal to the previous
one is none, and nothing says why the module deactivated a schedule. List the
snapshots themselves with `Lease.Snapshots`. The baseline is buffered before
it returns. A failed read is a `ceph.WatchGap` event and the next success is
`ceph.WatchResumed`. A deactivated schedule is a change, not the end of the
watch, since the module can activate it again. The watch has no terminal
state; these end it with `ceph.ErrInvalidRequest`: `Restore`, a schedule the
module no longer lists (even if added again with the same path and repeat),
another schedule of the directory with the lease's repeat, a removed or
replaced filesystem and cluster termination. A full buffer ends it with
`ceph.ErrWatchOverflow`. A lease whose add was never confirmed cannot be
watched; restore it. Call `Stop` before `Restore` and the fixture's cleanup: it
joins the watch and returns why it ended. See
[watching state](../../docs/guides/contexts-and-cleanup.md#watching-state) for
the lifecycle that every Watch shares.

## Concepts and limits

- `Add` never enables the module; a disabled module, a directory that already
  has a schedule and a second lease of the same directory are refused with
  `ceph.ErrInvalidRequest`.
- The module keeps retention per directory: every schedule of the directory
  shares it, and it stays after the last schedule is removed. Rules a new
  schedule would inherit are refused after the schedule was added, with a
  lease that removes it.
- Keep a non-nil lease returned with an error and restore it: the schedule may
  have been added before a reply was lost. `Restore` reads the directory back
  on every call and resumes where an earlier call stopped.
- A lease's `Path`, `Repeat`, `Start` and `Retention` fields are copies of the
  request. Its operations and `Restore` use the schedule and rules that `Add`
  recorded, so editing the fields retargets nothing.
- A lease is bound to the filesystem it was added on: after that filesystem
  is removed or replaced, even by one of the same name, every lease operation
  is refused. Restore the lease before removing the filesystem.
- `Snapshots` runs libcephfs in the client container, which needs the Python
  `cephfs` binding.

## Reference

The full contract of every type and function is in go doc:
[pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/cephfs/snapschedule)
or `go doc -all github.com/JSYoo5B/ceph-testcontainers-go/cephfs/snapschedule`.
See also [Contexts and cleanup](../../docs/guides/contexts-and-cleanup.md) and
[Errors](../../docs/guides/errors.md).
