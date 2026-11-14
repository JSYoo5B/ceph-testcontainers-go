# A PG merge never starts while its target has a primary override

Owner: MGR. Kind: upstream bug reproducer (`native_regression`).
Test: `TestPGMergeWithPrimaryOverride`, optional batch
`mgr_pg_merge_primary_override`. Its reader of the OSDMap, its
classification of a sample and its judgement have Docker-free tests in the
same package (`code/pure`).

## Use case and native contract

A primary override (`ceph osd pg-upmap-primary <pg> <osd>`) makes another
member of a PG's acting set its primary; the read balancer writes such
entries, and so does `Pool.OverridePGPrimary`. When the PG count of a pool
is reduced, the manager merges the last PG (the merge source) into its
parent (the merge target), one pair at a time. It starts a merge only when
both PGs are active and clean and their acting sets are equal, primary
included, so an entry that reorders one of them has to go first. A reduction
of the PG count must finish whatever overrides the pool has, and must leave
the overrides of other pools alone.

The defect: in `DaemonServer::adjust_pgs` (`src/mgr/DaemonServer.cc`) the
manager asks `OSDMap::have_pg_upmaps` whether a merge participant has an
entry to clear. In Tentacle 20.2.4 that function knows `pg_upmap` and
`pg_upmap_items` only, so a primary override is not cleared, the acting sets
of source and target stay different, and the manager, whose source reports
`acting does not match` at debug level 10 for such a pair, never asks for
the merge. The pool
stays at its old `pg_num` with `pg_num_target` below it until somebody
removes the override. The monitor's own cleanup (`OSDMap::check_pg_upmaps`)
removes the overrides of a PG that is gone or pending a merge, which this
pool never reaches.

Squid 19.2.5 has the same code as Tentacle 20.2.4. Tentacle 20.2.5 and
Umbrella 21.2.0 make `have_pg_upmaps` count primary overrides and have the
manager send `osd rm-pg-upmap-primary-all <pool>` for every pool with such a
participant; the command got its pool argument with the same change. The
cleanup removes every primary override of that pool, and none of another
pool.

Upstream references:

- [Tracker 76731](https://tracker.ceph.com/issues/76731), fixed in
  [Tentacle 20.2.5](https://docs.ceph.com/en/latest/releases/tentacle/).
- `DaemonServer::adjust_pgs` in
  [`src/mgr/DaemonServer.cc`](https://github.com/ceph/ceph/blob/v20.2.5/src/mgr/DaemonServer.cc),
  `OSDMap::have_pg_upmaps` in
  [`src/osd/OSDMap.h`](https://github.com/ceph/ceph/blob/v20.2.5/src/osd/OSDMap.h),
  and the `osd rm-pg-upmap-primary-all` command in
  [`src/mon/OSDMonitor.cc`](https://github.com/ceph/ceph/blob/v20.2.5/src/mon/OSDMonitor.cc).

## Requirements

- One cluster with three OSDs (`ceph.Run`) and its client toolkit. Every
  container runs the package's `image`, by default the Tentacle 20.2.4
  aggregate. Image roles `essential` and `client`.
- Three replicated pools of size 3 with two PGs each, so every PG is on all
  three OSDs and any OSD is an admissible primary.
- The overrides are made with `Pool.OverridePGPrimary` and
  `RaiseMinCompatClient`: the case raises `require_min_compat_client` to
  the release the monitor names, and the restoration of the last override
  lowers it again.
- The test bounds itself to 19 minutes; the batch timeout is 20 minutes.

## Tier and triggers

Standalone reproducer: the test runs on its package-level `image`, Tentacle
20.2.4 by default, which reproduces the defect. Its given and when steps and
its controls fail the test; the then step does not. It prints one line,
`EXPERIMENT_RESULT case=mgr_pg_merge_primary_override result=<REPRODUCED|NOT_REPRODUCED>`,
and passes either way. No automatic run includes it, and it never gates
anything.

- Manual: `gh workflow run "Experiments" -f case=mgr_pg_merge_primary_override`
  requires `REPRODUCED`; adding
  `-f image=quay.io/ceph/ceph@sha256:4337ac8ac81d40df136e5eac92ca53bb52360a5e039f3d1f7935e729eb5ef42c`
  requires `NOT_REPRODUCED` on Tentacle 20.2.5 afterwards.
  `gh workflow run "Native regressions" -f batch=mgr_pg_merge_primary_override`
  takes its pass or failure only.
- Comment: `/ci run mgr_pg_merge_primary_override`.

## Running it

On the default image, which must print `result=REPRODUCED`:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestPGMergeWithPrimaryOverride$' \
  ./internal/integration/experiments/mgr_pg_merge_primary_override
```

On another image, injected into the package's `image` variable, for example
the fixed Tentacle 20.2.5, which must print `result=NOT_REPRODUCED`:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/mgr_pg_merge_primary_override_test.image=quay.io/ceph/ceph@sha256:4337ac8ac81d40df136e5eac92ca53bb52360a5e039f3d1f7935e729eb5ef42c" \
  -run '^TestPGMergeWithPrimaryOverride$' \
  ./internal/integration/experiments/mgr_pg_merge_primary_override
```

The cluster and its client run `image`; the role image variables
(`CEPH_TEST_IMAGE`, `CEPH_TEST_ESSENTIAL_IMAGE`) do not select it. The test
logs the client's `ceph --version` and the daemons' `ceph versions`.

## Observations and controls

| Pool | PG count | Primary override |
| --- | --- | --- |
| `tc-merge-target` | reduced from 2 to 1 | on the parent PG `<pool>.0`, the merge target |
| `tc-merge-control` | stays 2 | on `<pool>.0` |
| `tc-merge-eligible` | reduced from 2 to 1 | none |

Each override names a member of the PG's acting set that is the primary of
neither PG of its pool. The parent's primary then differs from the source's,
also once the reduced placement count has placed both PGs alike, and the
entry stays valid: the monitor cancels an entry that names the PG's own
CRUSH primary.

Every decision reads two reports. `ceph osd dump --format json` gives, from
one OSDMap, each pool's `pg_num`, `pg_num_target`, `pg_placement_num` and
`pg_num_pending` and every `pg_upmap_primaries` entry. `Pool.PGs` gives the
state, the acting primary and the acting set the manager reports for each
PG of the pool, and the manager's word that it has the PGs' reports. A dump
that lacks its epoch, its pool list, its override list, a count of a pool or
the PG or OSD of an entry fails the test, and so does an entry whose OSD is
negative or the placeholder of a missing OSD (2147483647), so an absent
field is never read as a count of zero, as `osd.0` or as a map without
overrides.

A PG counts as served only when it is reported, its state names `active`
and `clean`, it has a primary, the primary is a member of its acting set
and the acting set holds OSDs only. The manager reports -1 for a PG without
a primary and may report an empty acting set or the placeholder while a PG
is placed anew; a PG the reports do not hold has no primary at all. The
primary of a PG is always the one the manager reports, never the first
member of the acting set, and it is only read from a served PG.

1. Conditions before any override: the pools are clean and hold 16 objects
   of known bytes each, which read back; the OSDMap has no primary override
   at all; each pool has 2 in every PG count; the manager has the reports,
   and both PGs of the two pools that get an override are served, the
   parent on three OSDs.
2. The overrides are made. The OSDMap must then hold exactly these two
   entries, and the manager must report both PGs of the target pool served,
   with the override's OSD as the primary of the parent PG and not of the
   source PG. Applying an override created an entry.
3. Trigger: `Pool.SetPGCount(1)` on `tc-merge-eligible` and on
   `tc-merge-target`.
4. Eligibility control: `tc-merge-eligible` must reach one PG in every
   count within five minutes. The manager merges an eligible pool on this
   cluster, so a pool that does not merge is not waiting for the manager.
5. From that moment `tc-merge-target` is sampled every five seconds, until
   a sample is merged, or the samples end in two minutes of blocked samples,
   or six minutes have passed. Every sample must hold the control pool's
   entry exactly as it was made and no entry the case did not make. Nothing
   is restored or removed while the pool is sampled.
6. The bytes of all three pools are read back.
7. The pool is merged when its last sample is merged, and blocked when
   its samples end in at least two blocked samples that span two minutes.
   Samples before that run may be in another state: the reduced placement
   count places the PGs anew, and they are not clean while they peer.
   Anything else fails the test here. Only for a blocked pool the test
   removes the target pool's override itself
   (`PGPrimaryOverrideHandle.Restore`), waits up to five minutes for the
   pool to reach one PG and takes one more sample. This tells a merge that
   waited for the override from one that does not happen at all.
8. The samples are judged. Then both overrides are restored, the OSDMap
   must hold no primary override, and the bytes of all three pools are read
   back once more.

## Reading the result

- **REPRODUCED:** every sample of the last two minutes is blocked: the
  manager has the reports, the pool has `pg_num` 2 and `pg_num_pending` 2
  with `pg_num_target` 1 and `pg_placement_num` 1, the parent's entry is
  still in the OSDMap, and both PGs are served on the same OSDs with
  different reported primaries, the parent's being the OSD of its override. And the pool merged once the test
  removed the entry. The symptoms give the counts, the entry and the two
  acting sets.
- **NOT_REPRODUCED:** a sample is merged: the pool has 1 in every PG count,
  the manager has the reports and reports its one PG served, and the OSDMap
  has no entry for the parent although the test did not remove one. With
  the control pool's entry unchanged in the same sample, that is the
  pool-scoped cleanup of Tentacle 20.2.5.
- **Test failure (no result):**
  - `condition not established`: a pool does not start as described, the
    OSDMap already has a primary override, no admissible OSD exists for an
    override, the entries after the overrides are not exactly the two, the
    override does not change the parent's primary, or the pool without an
    override does not merge within five minutes.
  - `control not established`: the control pool's entry is missing from a
    sample.
  - `another fault:`: the OSDMap holds an entry the case did not make
    (another PG, or one of its PGs with another OSD), or the target pool
    does not merge within five minutes after the test removed its override.
  - `outcome not observed`: within six minutes the pool neither merged nor
    stayed blocked for two minutes, for example a PG is not served, the
    manager does not have the reports, a merge is under way at the end, the
    placement count was not reduced, the acting sets are equal, or the entry
    is gone and the pool still has two PGs. Samples that are not one series,
    because a sample's time does not follow its predecessor's or its OSDMap
    is older, fail the same way.
  - The OSD dump lacks a field (see above), an object does not read back,
    a restoration fails or leaves an entry, or any other command fails.
  - The cluster or its client does not start.

Native results (Linux amd64 host, Docker 29.9.1, one run each):

| Image | Result | Time |
| --- | --- | --- |
| Tentacle 20.2.4, `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7…` (default) | `REPRODUCED`: 21 blocked samples over 2 m 4 s with `pg_num` 2, `pg_num_pending` 2, `pg_placement_num` 1, the entry `2.0 -> osd.1` in place, the primaries osd.1 and osd.0 and the acting sets `[1 2 0]` and `[0 2 1]`; merged after the test removed the entry | 203 s |
| Squid 19.2.5, `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:d0fcbea14a2a…` | `REPRODUCED`, the same | 210 s |
| Tentacle 20.2.5, `quay.io/ceph/ceph@sha256:4337ac8ac81d…` | `NOT_REPRODUCED`: the first sample has two PGs with equal acting sets and no entry for the parent, the second, five seconds later, is merged; the control pool's entry is unchanged | 87 s |
| Umbrella 21.2.0, `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:7fce1d2de623…` | `NOT_REPRODUCED`, the same | 80 s |

In every run the pool without an override finished its merge within five
seconds of the reduction, the manager had the PGs' reports in every sample,
and the first sample of a blocked pool was already blocked. A run with the reduction of `tc-merge-target` left out, so the
trigger was never sent, was sampled for the six minutes, failed with
`outcome not observed` and printed no result.

## Cleanup and limits

The overrides are restored by the test and again by its cleanup, which is
idempotent; `require_min_compat_client` returns to its prior level with the
last one. The pools end with the cluster; nothing outside it is changed.

Not covered: arm64, an override on the merge source instead of the target,
reductions over more than one merge, erasure-coded
pools, overrides written by the read balancer, the manager's log line, and
how long a blocked pool stays blocked beyond the two minutes the test
requires.
