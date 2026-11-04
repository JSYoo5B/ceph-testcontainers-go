# CephFS expect

Import `github.com/JSYoo5B/ceph-testcontainers-go/cephfs/expect` alongside
`cephfs` when a test wants a precise verdict on an MDS observation it already
captured, with an explanation of each mismatch. A filesystem needs nothing
from this package to run, and a test that only constructs, queries or
watches a filesystem can ignore it. See
[API purpose and purpose tags](../../docs/guides/api-purpose.md) for where
this fits, and [`ceph/expect`](../../ceph/expect/README.md) for evaluations
of cluster observations.

Every expectation has one `Evaluate` method that reads only its arguments: no
query, wait, retry or `testing.T`. A mismatch is a result, listed in a
deterministic order and summarized by `String` with a bounded number of
entries. An error means there is no verdict: an invalid expectation is
`ceph.ErrInvalidRequest`, and a malformed observation is an error of its own,
never a mismatch. The result copies what it evaluated, so its diagnostics
describe that observation even after the caller reuses the input. Polling,
retries and assertion failures stay with the test.

## MDS ranks and standbys

`expect.MDSs` states what one `fs.MDSStatus` read, or the `Status` of one
`WatchMDSs` event, must report:

- `Ranks`: ranks that must be up:active, each with an optional holder `Name`
  and `GID` and a `NotGID` that the holder must no longer have. A takeover is
  `NotGID` set to the killed daemon's GID; pinning the baseline `GID` makes a
  restarted or replaced daemon fail as `gid`.
- `Owned`: every holder in `Ranks` is one of this fixture's daemons.
- `AllRanksActive`: every rank below the reported max_mds is up:active.
- `Exact`: no up:active rank outside `Ranks`.
- `FilesystemID` and `MaxMDS`: the native filesystem ID, so a filesystem
  recreated under the same name fails, and max_mds.
- `Standby` and `StandbyReplay`: the exact number of owned ordinary or
  replay standbys and the daemons, by name or GID, that must be among them.
  A nil list is not compared; `&expect.MDSStandbys{}` requires it empty.

`MDSEvaluation` lists each changed filesystem ID or max_mds, each inactive,
renamed, replaced, unchanged, unowned or unselected rank, and each standby
count or standby that differs.

```golang
baseline, err := fs.MDSStatus(ctx)
if err != nil {
	t.Fatal(err)
}
old := baseline.Active[0]
// ... SIGKILL the active MDS, fail it, and restart its container ...
want := expect.MDSs{
	FilesystemID: baseline.FilesystemID,
	Ranks:        []expect.MDSRank{{Rank: 0, NotGID: old.GID}},
	Owned:        true,
	Exact:        true,
	Standby:      &expect.MDSStandbys{Count: 1, Daemons: []expect.MDSDaemon{{Name: old.Name, NotGID: old.GID}}},
}
status, err := fs.MDSStatus(ctx)
if err != nil {
	t.Fatal(err) // a failed query is never a mismatch
}
result, err := want.Evaluate(*status)
if err != nil {
	t.Fatal(err)
}
if !result.Matched {
	t.Fatal(result) // ... standby count: want 1, observed 0; standby mds.tc-cephfs-0 absent
}
```

A match proves what that one FSMap reported. A rank still in replay,
reconnect or rejoin is in no list and fails as `inactive`; up:active does not
prove that clients reconnected. Unlike `fs.WaitReady`, which waits for the
configured counts, an expectation checks the identities the test names.
