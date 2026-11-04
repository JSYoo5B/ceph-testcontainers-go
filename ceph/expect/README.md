# Expect

Import `github.com/JSYoo5B/ceph-testcontainers-go/ceph/expect` alongside `ceph`
when a test wants a precise verdict on an observation it already captured,
with an explanation of each mismatch. A cluster needs nothing from this
package to run, and a test that only constructs, queries or watches a
cluster can ignore it. See
[API purpose and purpose tags](../../docs/guides/api-purpose.md) for where
this fits.

Every expectation has one `Evaluate` method that reads only its arguments: no
query, wait, retry or `testing.T`. A mismatch is a result, listed in a
deterministic order and summarized by `String` with a bounded number of
entries. An error means there is no verdict: an invalid expectation is
`ceph.ErrInvalidRequest`, and a malformed observation is an error of its own,
never a mismatch. The result copies what it evaluated, so its diagnostics
describe that observation even after the caller reuses the input. Polling,
retries and assertion failures stay with the test.

## OSD membership

`expect.OSDs` states the up and in flags that selected owned OSDs must report
in one `ceph.Observer.OSDStates` observation. Select the OSDs from an earlier
`OSDStates` baseline to pin their native UUIDs, so an ID that a replacement
re-registered does not pass, and set `Exact` when no other owned OSD may
exist. `OSDEvaluation` lists each absent, replaced, down, out or unselected
OSD.

```golang
baseline, err := observer.OSDStates(ctx)
if err != nil {
	t.Fatal(err)
}
// ... stop and restart an OSD while the SUT runs ...
want := expect.OSDs{Select: baseline, Up: true, In: true, Exact: true}
states, err := observer.OSDStates(ctx)
if err != nil {
	t.Fatal(err) // a failed query is never a mismatch
}
result, err := want.Evaluate(states)
if err != nil {
	t.Fatal(err)
}
if !result.Matched {
	t.Fatal(result) // osds [0 1] want up=true in=true exact in 2 observed: osd.1 up: want true, observed false
}
```

Unlike `ceph.Observer.WaitForClean`, which compares native OSD counts from
the cluster status, an expectation checks individual identities.
