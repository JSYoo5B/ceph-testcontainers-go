# A late COMPLETE update brings a removed RBD group snapshot back

Owner: RBD. Kind: upstream bug reproducer (`native_regression`).
Test: `TestRBDGroupSnapshotLateCompleteAfterRemoval`, optional batch
`rbd_group_snap_late_complete`. Its judgement of what the group shows after
the late update has Docker-free tests in the same package (`code/pure`).

## Use case and native contract

A group snapshot is created in two steps. librbd first stores its record as
INCOMPLETE with the class method `rbd.group_snap_set`, which also allocates
the snapshot's order entry, then snapshots the member images and stores the
record again as COMPLETE. `rbd.group_snap_remove` deletes the record
(`snapshot_<id>`) and the order entry (`snap_order_<id>`) from the group
header object. Once a snapshot is removed, the group's listing must not name
it any more, whatever update for it still arrives.

`group_snap_set` checks that the record does not exist only for an
INCOMPLETE update. For any other state it writes `snapshot_<id>`
unconditionally (`src/cls/rbd/cls_rbd.cc`). A COMPLETE update that reaches
the object after the removal, such as the second step of a creation that
raced with it, therefore stores the record again, without an order entry.
The listing of the `rbd` command and of the Python binding sorts by order
when it can and otherwise returns the entries unsorted, so it succeeds and
names the removed snapshot again, as `complete`, although its member image
snapshots are gone. Only librbd's internal listing with
`fail_if_not_sorted` answers `EINVAL`; no public entry point passes it.

Tentacle 20.2.4 and Umbrella 21.2.0 have the same class code, and no release
contains a correction. The correction on the upstream development branch
makes a non-INCOMPLETE update of a missing record return `-EINVAL`.
Upstream reference: [tracker 81590](https://tracker.ceph.com/issues/81590).

## Requirements

- One cluster with one OSD (`ceph.Run`) and its client toolkit, whose
  `python3` has the `rados` module. Image roles `essential` and `client`, of
  one release.
- The test bounds itself to 15 minutes; the batch timeout is 20 minutes.

## Tier and triggers

Standalone reproducer: the test runs on its package-level `image`, Tentacle
20.2.4 by default, which reproduces the defect. Its given and when steps fail
the test; the then step does not. It prints one line,
`EXPERIMENT_RESULT case=rbd_group_snap_late_complete result=<REPRODUCED|NOT_REPRODUCED>`,
and passes either way. No automatic run includes it (the default checks of a
pull request, pushes to `main`, periodic samples), and it never gates
anything. A maintainer can run it on demand, also on a pull request's head
with the comment below.

- Manual: `gh workflow run "Experiments" -f case=rbd_group_snap_late_complete`,
  or `gh workflow run "Native regressions" -f batch=rbd_group_snap_late_complete`
  for its pass or failure only.
- Comment: `/ci run rbd_group_snap_late_complete` or
  `/ci run rbd_group_snap_late_complete on umbrella`.

## Running it

On the default image, which must print `result=REPRODUCED`:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestRBDGroupSnapshotLateCompleteAfterRemoval$' \
  ./internal/integration/experiments/rbd_group_snap_late_complete
```

On another image, injected into the package's `image` variable; a fixed
image must print `result=NOT_REPRODUCED`. No release fixes the defect yet,
so the example names Umbrella 21.2.0, which reproduces it too:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rbd_group_snap_late_complete_test.image=ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:7fce1d2de623a5e6c9f7dc7dd13b356dcb1f71b0b4d74bf7cd2c175cd802735f" \
  -run '^TestRBDGroupSnapshotLateCompleteAfterRemoval$' \
  ./internal/integration/experiments/rbd_group_snap_late_complete
```

The cluster and its client run `image`; the role image variables
(`CEPH_TEST_IMAGE`, `CEPH_TEST_ESSENTIAL_IMAGE`) do not select it. The test
logs the client's `ceph --version`.

## Observations and controls

The pool holds two groups, `late` and `control`, set up the same way with
the ordinary commands: one 4 MiB image, `rbd group snap create` for
`healthy` and then for `victim`. Each snapshot therefore went through
INCOMPLETE and COMPLETE. A Python process in the client container reads the
group header object (`rbd_group_header.<group id>`) through the `rados`
binding: its omap keys and, decoded from each `snapshot_<id>` value, the ID,
name and state of the record. The listing is always
`rbd group snap ls --format json`.

The late update is not produced by a race. The payload of
`rbd.group_snap_set` is an encoded group snapshot, which is also the value
the method stores, so the test keeps the stored COMPLETE record of `victim`
from before its removal and sends it again with
`Ioctx.execute(<header object>, "rbd", "group_snap_set", <record>)`.

1. Conditions in both groups after creation: the listing names `healthy`
   and `victim`, and the header object has a complete record and an order
   entry for each.
2. Mechanism control, in `late`: the stored record of `healthy`, an
   existing snapshot, is sent with the same call. The server must accept it
   (return 0) and the header object and the listing must not change. The
   client, the object and the method therefore work before the trigger.
3. `rbd group snap rm` removes `victim` from both groups. Both listings
   must name only `healthy`, and neither header object may keep the
   record or the order entry of its `victim`.
4. Trigger, in `late` only: the stored record of the removed `victim` is
   sent again. Its result is recorded.
5. Control group: `control`, which got no late update, must still list only
   `healthy`.
6. `late` is judged by its listing and its header object.

## Reading the result

- **REPRODUCED:** the server accepted the late update, the header object of
  `late` has `snapshot_<victim id>` again without `snap_order_<victim id>`,
  and the listing names `healthy` and `victim`. The symptoms give the
  listing, the result of the update and the omap keys. A listing that fails
  while the header object holds that orphan record is reported as
  `REPRODUCED` too; no tested image does that.
- **NOT_REPRODUCED:** the listing of `late` succeeds and names only
  `healthy`. That is the result when the server refuses the late update
  with `EINVAL`, which is what the upstream correction does: the mechanism
  control showed that the same call on the same object is accepted for an
  existing record, so the refusal is the server's answer to an update of a
  removed snapshot. It is also the result when a server accepts the update
  and the listing does not show the snapshot. The receipt
  (`RBD_GROUP_SNAP_LATE_COMPLETE_RECEIPT`) gives the result of the update
  and the omap keys in either case.
- **Test failure (no result):**
  - `condition not established`: a group does not start with the two
    complete snapshots, the mechanism control is refused or changes the
    group, the removal leaves the record or the order entry, or the control
    group does not list only `healthy` at the end.
  - `trigger not delivered`: the late update ended with a result other than
    0 or `EINVAL`, such as a timeout.
  - `another fault:`: the listing of `late` is wrong in a way the orphan
    record does not explain, for example it names `victim` after a refused
    update, lacks `healthy`, or fails without the orphan record.
  - The cluster or its client does not start, or the client's `python3`
    lacks the `rados` module.

Native results (Linux amd64 host, Docker 29.9.1, aggregate images, one run
each):

| Image | Result | Time |
| --- | --- | --- |
| Tentacle 20.2.4, `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7…` (default) | `REPRODUCED`: update returned 0, `snapshot_<victim id>` back without `snap_order_<victim id>`, listing `[healthy victim]` | 29 s |
| Umbrella 21.2.0, `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:7fce1d2de623…` | `REPRODUCED`, the same | 29 s |

No fixed image exists to run, so `NOT_REPRODUCED` has not been observed
natively; the refusal and the clean listing are covered only by the
judgement tests.

## Cleanup and limits

The pool, its groups and images end with the cluster; nothing outside it is
changed.

Not covered: arm64, a late update produced by a real race between a slow
`rbd group snap create` and a concurrent removal, Squid, the Python
binding's `Group.list_snaps`, `rbd group snap info` and the reuse of the
name `victim`, groups with several images, and librbd's internal listing
that fails with `EINVAL`.
