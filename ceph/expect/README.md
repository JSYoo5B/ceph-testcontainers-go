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

## Health checks and mutes

`expect.Health` states the checks and stored mutes one
`ceph.Observer.HealthDetails` observation must report. `Require` lists checks
by native code, each with an optional severity, summary count and muted or
unmuted state; `Forbid` lists codes that must be absent. `Mutes` and `NoMutes`
list codes that must or must not have a stored mute, which a sticky mute can
keep after its check clears. `Status` compares the overall status, which
Ceph derives from the worst unmuted check, `FSID` pins the cluster, and
`Exact` allows no other check or mute. Messages are never compared, since
they carry countdowns and application data. `HealthEvaluation` lists each
absent, forbidden or unexpected check and each severity, count, mute flag or
stored mute that differs.

```golang
want := expect.Health{
	Require: []expect.HealthCheck{{Code: "OSD_DOWN", Severity: "HEALTH_WARN", Muted: expect.HealthUnmuted, Count: 1}},
	NoMutes: []string{"OSD_DOWN"},
}
health, err := observer.HealthDetails(ctx)
if err != nil {
	t.Fatal(err) // a failed query is never a mismatch
}
result, err := want.Evaluate(health)
if err != nil {
	t.Fatal(err)
}
if !result.Matched {
	t.Fatal(result) // health want require=[OSD_DOWN] nomutes=[OSD_DOWN] in HEALTH_OK with 0 checks, 0 mutes: OSD_DOWN absent
}
```

Ceph raises many checks only after a delay and clears them at a later
monitor tick, so a test polls with its own deadline. A cleared check does
not prove that the fault behind it is gone.

## Pool placement groups

`expect.PGs` states the PGs one `ceph.Pool.PGs` observation must report.
`Count` or `Select` names the PGs to check, so an empty report never matches
by having nothing to check. Each checked PG must report the exact `State`
(such as `active+clean`), carry the whole state tokens of `Require` and none
of `Forbid`, report the exact ordered `Up` and `Acting` vectors or an
`ActingSize`, contain the OSDs of `InUp` and `InActing` and none of `NotInUp`
and `NotInActing`, and have one of the `ActingPrimary` values. Take `Pool`
from an earlier observation, such as a baseline's `PoolAfter`, to pin the
native pool ID, so a pool recreated under the same name does not pass.
`PGEvaluation` lists each PG and condition that differs.

```golang
baseline, err := pool.PGs(ctx)
if err != nil {
	t.Fatal(err)
}
// ... stop and restart osd.2 while the SUT runs ...
want := expect.PGs{Pool: baseline.PoolAfter, Count: len(baseline.PGs), State: "active+clean", InActing: []int{2}}
snapshot, err := pool.PGs(ctx)
if err != nil {
	t.Fatal(err) // a failed query is never a mismatch
}
result, err := want.Evaluate(snapshot)
if err != nil {
	t.Fatal(err)
}
if !result.Matched {
	t.Fatal(result) // ... pg 3.1 state: want active+clean, observed active+undersized+degraded; pg 3.1 acting missing osd.2: observed [0 1]
}
```

Unlike `ceph.Observer.WaitForClean`, which waits for every PG of the cluster
to report active+clean, an expectation checks one pool's PGs once. The PG
reports arrive from the MGR asynchronously, so a match is no proof of write
visibility or durability, and the native `PGReady` flag is not compared.

## MON quorum

`expect.Monitors` states the monmap members and the quorum of one
`ceph.Observer.QuorumStatus` observation. `Members` and `Quorum` are exact
name sets; `MemberCount` and `QuorumSize` count them for a test whose MONs
get new names, such as a rolling replacement. `FSID` pins the cluster, so a
rebuilt cluster that reuses the MON names does not pass. Every set field is a
condition and a zero field is not checked. `MonitorEvaluation` names each
member that is absent, unexpected, out of the quorum or in it unexpectedly.

```golang
// ... stop mon.c while the SUT runs ...
want := expect.Monitors{Members: []string{"a", "b", "c"}, Quorum: []string{"a", "b"}}
status, err := observer.QuorumStatus(ctx)
if err != nil {
	t.Fatal(err) // a failed query is never a mismatch
}
result, err := want.Evaluate(status)
if err != nil {
	t.Fatal(err)
}
if !result.Matched {
	t.Fatal(result) // mons want members [a b c] quorum [a b] in monmap [a b c] quorum [a b c]: mon.c in quorum
}
```

A quorum that is not a majority of the expected members is an invalid
request, since a `QuorumStatus` answer always comes from a majority. A match
does not say which MON leads or that a member out of the quorum has stopped.
Unlike `ceph.Observer.WaitForQuorum`, which accepts any majority, an
expectation names the members the test requires.

## MGR registrations

`expect.Managers` states the MGRMap of one `ceph.Observer.ManagerStatus`
observation, or of the `Status` of a `ceph.ManagerWatchEvent`. `Available`
is always compared. `Active` names the active MGR, `ActiveGID` pins its
registration so a restart under the same name does not pass, and
`FormerGID` requires an active MGR with any GID other than an earlier one,
which states a failover. `Standbys` is the exact set of standby names, with
an empty non-nil slice for none, and `StandbyCount` counts them.
`ManagerEvaluation` names the field that differs and each absent or
unexpected standby.

```golang
before, err := observer.ManagerStatus(ctx)
if err != nil {
	t.Fatal(err)
}
// ... fail the active MGR while the SUT runs ...
want := expect.Managers{Available: true, Active: before.Standbys[0].Name, FormerGID: before.ActiveGID}
status, err := observer.ManagerStatus(ctx)
if err != nil {
	t.Fatal(err)
}
result, err := want.Evaluate(status)
if err != nil {
	t.Fatal(err)
}
if !result.Matched {
	t.Fatal(result) // ...: active: want mgr.b, observed mgr.a; active gid: want other than 4100, observed 4100
}
```

A match does not prove that a module serves requests or that a stopped MGR
has exited. Unlike `ceph.Observer.WaitForClean`, which requires only an
available MGR, an expectation names the registrations the test requires.
