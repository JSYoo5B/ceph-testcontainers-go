# CephFS sessions

Import `github.com/JSYoo5B/ceph-testcontainers-go/cephfs/session` to observe the
client sessions of an existing filesystem, to shorten its session limits, so
that a test can watch the MDS evict a silent or frozen client within seconds
instead of minutes, to make the MDS refuse new client sessions while open
ones keep working, and to require client features, so that only clients that
lack them are refused or evicted. The filesystem stays in the [cephfs package](../README.md);
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

To refuse new sessions, call `session.OverrideRefusal(ctx, fs)`. It sets the
filesystem's `refuse_client_session` flag: a client that mounts afterwards
fails with EPERM and gets no session, while a client mounted before keeps
working. `RefusalOverrideHandle.Restore` clears the flag again.

```golang
refusal, err := session.OverrideRefusal(ctx, fs)
if refusal != nil {
	defer refusal.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
// A new mount of the SUT now fails with EPERM; a session it opened before still works.
if err := refusal.Restore(ctx); err != nil {
	t.Fatal(err)
}
// New mounts succeed again.
```

To make the MDS refuse only clients that lack a client feature, call
`session.OverrideRequiredFeatures(ctx, fs, names...)` with names that the
image's `fs feature ls` lists. It adds each feature to the filesystem's
`required_client_features`: a client that does not advertise one of them
fails to mount with EPERM, an open session that lacks one is evicted and, with
Ceph's default `mds_session_blocklist_on_evict`, blocklisted, and clients that
advertise every required feature are admitted as before.
`session.RequiredFeatures(ctx, fs)` reads the required set back from the
MDSMap. `RequiredFeaturesOverrideHandle.Steps` reports, per requested
feature, whether it was already required, whether its add was sent and
whether a readback confirmed it; `Restore` removes only the bits this handle
added.

```golang
required, err := session.OverrideRequiredFeatures(ctx, fs, "metric_collect")
if required != nil {
	defer required.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
// A client that does not advertise metric_collect now fails to mount with
// EPERM; its open session is evicted. Clients that advertise it mount.
if err := required.Restore(ctx); err != nil {
	t.Fatal(err)
}
// The client that lacks the feature mounts again; the evicted session does
// not come back.
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
- `OverrideRefusal` owns only the change from admitting to refusing: on a
  filesystem that already refuses new sessions it returns an error and no
  handle. One refusal per filesystem may be active, independent of the
  timeouts override. Its `Restore` accepts a flag that was cleared outside it.
- While new sessions are refused, every new session fails, including the
  fixture's own libcephfs probes (pin reads, clone checks, snapshot schedule
  listings) and a session the MGR `volumes` module opens; restore the flag
  before those operations.
- Do not restart, fail or replace an MDS during the refusal unless the test
  wants to lose its clients: an open session that must reconnect is refused
  like a new one, the MDS drops it and blocklists the client, and the client's
  I/O does not come back after `Restore`.
- The flag exists on every supported release; a cluster whose MDSMap does
  not report it returns `ceph.ErrUnsupported` before any change.
- `OverrideRequiredFeatures` sets the admission policy of one filesystem
  and proves only that its MDSMap requires the features. Which features a
  client advertises is the client's own state: the fixture neither reads nor
  changes it, so observe the client's mount and session outcome separately.
  A feature name that the image does not list, and its placeholder
  `reserved`, returns `ceph.ErrUnsupported`;
  an empty list, a blank or a repeated name returns `ceph.ErrInvalidRequest`,
  both before any change.
- A feature that was already required is recorded as `AlreadyRequired`: the
  handle owns nothing for it and `Restore` leaves it required. Bits the
  handle did not add, including ones the release does not name, are never
  changed. If every requested feature was already required, the handle owns
  nothing but still holds the filesystem's slot until `Restore`.
- The native command adds or removes one feature at a time without a
  precondition, so the override is a sequence: a failure stops it and returns
  the handle with the error, owning the steps that were sent. `Restore`
  refuses, with `ceph.ErrInvalidRequest` and before any change, an owned
  feature that a readback had confirmed but that is no longer required, and a
  filesystem recreated under the same name. An outside remove and re-add of an
  owned feature between two reads cannot be detected.
- Removing a requirement restores the policy only: a session that it evicted
  stays closed and its blocklist entry stays.
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
