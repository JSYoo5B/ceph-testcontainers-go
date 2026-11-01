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
   `override.SetMGRModule(ctx, cluster, "snap_schedule", true)`; wait until the
   active MGR, which restarts to load it, answers again. For a schedule in
   minutes, also set `mgr/snap_schedule/allow_m_granularity` to `true` with
   `override.SetConfig`.
2. **Add the schedule** with `snapschedule.Add(ctx, fs, snapschedule.Config{Path:
   "/data", Repeat: "1m", Retention: map[string]int{"n": 3}})`. It returns a
   `Lease`.
3. **Run the SUT** while the module takes `scheduled-…` snapshots.
4. **Observe** with `Lease.Status` (the module's counts and times) and
   `Lease.Snapshots` (the scheduled snapshots in the directory's `.snap`).
5. **Restore** with `Lease.Restore`, which removes the retention rules and the
   schedule the lease added; the snapshots stay.

```golang
lease, err := snapschedule.Add(ctx, fs, snapschedule.Config{Path: "/", Repeat: "1m", Retention: map[string]int{"n": 3}})
if lease != nil {
	defer lease.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
snapshots, err := lease.Snapshots(ctx)
```

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
- `Snapshots` runs libcephfs in the client container, which needs the Python
  `cephfs` binding.

## Reference

The full contract of every type and function is in go doc:
[pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/cephfs/snapschedule)
or `go doc -all github.com/JSYoo5B/ceph-testcontainers-go/cephfs/snapschedule`.
See also [Contexts and cleanup](../../docs/guides/contexts-and-cleanup.md) and
[Errors](../../docs/guides/errors.md).
