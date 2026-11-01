# CephFS sessions

Import `github.com/JSYoo5B/ceph-testcontainers-go/cephfs/session` to observe the
client sessions of an existing filesystem and to shorten its session limits, so
that a test can watch the MDS evict a silent or frozen client within seconds
instead of minutes. The filesystem stays in the [cephfs package](../README.md);
the package starts no container. See
[API purpose and purpose tags](../../docs/guides/api-purpose.md).

## Order of use

1. **Start a filesystem** with `cephfs.Run`, `cephfs.Start` or
   `cephfs.WithFilesystems`, and take its handle from
   `cephfs.Filesystems(cluster.Container)`.
2. **Shorten the limits** with `session.OverrideTimeouts(ctx, fs,
   session.Limits{Timeout: ..., Autoclose: ...})`. It returns a handle that
   keeps the previous values.
3. **Reproduce the condition**: freeze the client with `fault.Pause`, kill it,
   or let another client ask for its capabilities.
4. **Observe**: `session.List(ctx, fs)` lists the sessions of every active
   rank; `session.Timeouts(ctx, fs)` reads the current limits; the cluster's
   `BlocklistEntries` show the evicted address.
5. **Restore** with `TimeoutsOverrideHandle.Restore`, or let the cluster's
   termination discard the filesystem.

```golang
change, err := session.OverrideTimeouts(ctx, fs, session.Limits{Timeout: 30 * time.Second, Autoclose: time.Minute})
if change != nil {
	defer change.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
sessions, err := session.List(ctx, fs)
if err != nil {
	t.Fatal(err)
}
// Match the client by Session.ID or Address, then poll until it is gone.
```

## Concepts and limits

- Both limits are MDSMap settings of one filesystem, not central
  configuration, so `override.SetConfig` cannot change them. Each must be a
  whole number of seconds and at least 30 seconds.
- One override handle per filesystem may be active. The native CLI changes one
  value per command, so apply and restore are not atomic: keep a non-nil handle
  returned with an error, since a lost reply can leave either value changed.
- `Restore` refuses a value that is neither the applied nor the captured one,
  and a filesystem recreated under the same name.
- `List` reports a client once per rank and includes the fixture's own
  short-lived libcephfs probes. `Session.Address` has the form that
  blocklist entries use.
- Evicting the sessions of one subvolume principal is
  `subvolume.Authorization.EvictClients` in
  [`cephfs/subvolume`](../subvolume/README.md).

## Reference

The full contract of every type and function is in go doc:
[pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/cephfs/session)
or `go doc -all github.com/JSYoo5B/ceph-testcontainers-go/cephfs/session`.
See also [Fault injection](../../docs/guides/fault-injection.md#evicting-a-silent-cephfs-client),
[Contexts and cleanup](../../docs/guides/contexts-and-cleanup.md) and
[Errors](../../docs/guides/errors.md).
