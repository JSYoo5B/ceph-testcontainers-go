# RGW expect

Import `github.com/JSYoo5B/ceph-testcontainers-go/rgw/expect` alongside `rgw`
when a test wants a precise verdict on a storage accounting observation it
already captured, with an explanation of each mismatch. A gateway needs
nothing from this package to run, and a test that only runs or observes a
gateway can ignore it. Package `rgw` does not import it, so a test that uses
only `rgw` does not depend on it. It imports `rgw` for the observation types.
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
