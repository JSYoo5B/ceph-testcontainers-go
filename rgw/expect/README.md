# RGW expect

Import `github.com/JSYoo5B/ceph-testcontainers-go/rgw/expect` alongside `rgw`
or `rgw/multisite` when a test wants a precise verdict on a storage
accounting or multisite data sync observation it already captured, with an
explanation of each mismatch. Gateways need nothing from this package to run,
and a test that only runs or observes them can ignore it. Packages `rgw` and
`rgw/multisite` do not import it, so a test that uses only them does not
depend on it. It imports both for the observation types.
See [API purpose and purpose tags](../../docs/guides/api-purpose.md) for where
this fits.

Every expectation has one `Evaluate` method that reads only its arguments: no
query, stats synchronization, wait, retry or `testing.T`. A mismatch is a
result, listed in a deterministic order and summarized by `String` with a
bounded number of entries. An error means there is no verdict: an invalid
expectation is `ceph.ErrInvalidRequest`, and a malformed observation is an
error of its own, never a mismatch. The result copies what it evaluated, so
its diagnostics describe that observation even after the caller reuses the
input. Polling, retries and assertion failures stay with the test.

## Storage accounting

`expect.UserUsage` states what one `rgw.UserUsage` observation, from
`User.Usage`, must report. `UserID` selects the user, `Tenant` is compared
when set, and `Scope` says whose counters they are: `UsageScopeUser` for an
ordinary user, who is its own owner, or `UsageScopeAccount` for a user of an
account, whose `OwnerID` is the account ID. Two users of one account
therefore state the same `OwnerID` with their own `UserID`, and a wrong owner
or scope is a mismatch even when the counters match. `Objects` and
`SizeBytes` are always compared, so zero objects and zero bytes are
expectations of their own. `SizeBytes` is the native logical size; the
rounded accounting size (`SizeActualBytes`) is not compared.

```golang
// Native accounting is asynchronous: synchronize the stats first.
if _, err := gateway.Admin(ctx, "user", "stats", "--uid", user.ID(), "--sync-stats"); err != nil {
	t.Fatal(err)
}
usage, err := user.Usage(ctx)
if err != nil {
	t.Fatal(err) // a failed query is never a mismatch
}
result, err := expect.UserUsage{UserID: user.ID(), Scope: expect.UsageScopeUser, Objects: 4, SizeBytes: 4096}.Evaluate(usage)
if err != nil {
	t.Fatal(err)
}
if !result.Matched {
	t.Fatal(result) // usage of user tc-user want scope=user owner=tc-user objects=4 size=4096: objects: want 4, observed 3; size: want 4096, observed 3072
}
```

A match proves only what that one stats read reported. Quota enforcement and
object content are separate assertions.

## Multisite data sync

`expect.MultisiteDataSync` states what one `multisite.SyncStatus` observation,
from `Fixture.SyncStatus`, `WaitSyncReady` or a `WatchSync` event, must
report for one directional data sync stream: the data that `Zone`, the zone
the status was read in, syncs from `Source`. `MultisiteDataSyncCaughtUp`
requires the stream's caught-up verdict. `MultisiteDataSyncBehind` requires a
stream that is not caught up and has a positive number of shards behind its
source's log: a stream that is only not caught up, such as one in full sync,
is not evidence of a backlog. An observation read in another zone, as when
the direction is swapped, and one that lists no stream from `Source` are
mismatches of their own; a stream without an identified source is a
malformed observation.

Each zone reports only what it receives, so a lag in one direction and none
in the other are two expectations evaluated against the two zones'
observations:

```golang
statusB, err := fixture.SyncStatus(ctx, "b")
if err != nil {
	t.Fatal(err)
}
fromA, err := expect.MultisiteDataSync{Zone: "b", Source: "a", State: expect.MultisiteDataSyncBehind}.Evaluate(statusB)
if err != nil {
	t.Fatal(err)
}
statusA, err := fixture.SyncStatus(ctx, "a")
if err != nil {
	t.Fatal(err)
}
fromB, err := expect.MultisiteDataSync{Zone: "a", Source: "b", State: expect.MultisiteDataSyncCaughtUp}.Evaluate(statusA)
if err != nil {
	t.Fatal(err)
}
if !fromA.Matched || !fromB.Matched {
	t.Fatal(fromA, fromB) // data sync of zone b from a want behind (observed 128 shards: 128 incremental, 0 full, 0 behind, 0 recovering): caught-up: want false, observed true; behind-shards: want >0, observed 0
}
```

The two reads are taken one after the other, so they are not one atomic state
of both zones. Metadata sync is not compared; assert `SyncStatus.Metadata`
separately. Neither state proves that a particular object arrived or is
missing: compare the S3 payloads.
