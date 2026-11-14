# Python RBD trash purge reads its cutoff in the client's timezone

Owner: RBD. Kind: upstream bug reproducer (`native_regression`).
Test: `TestRBDTrashPurgeCutoffAcrossClientTimezones`, optional batch
`rbd_trash_purge_timezone`. Its judgement of what a purge retained has
Docker-free tests in the same package (`code/pure`).

## Use case and native contract

A tool purges a pool's RBD trash through the Python binding with a cutoff:
`RBD().trash_purge(ioctx, expire_ts)` removes the images whose deferment
ended by `expire_ts`, a `datetime` that the binding documents as UTC. A
timezone-aware datetime names one instant, so the images removed must not
depend on the timezone of the client process.

The binding converts the cutoff with `time.mktime(expire_ts.timetuple())`
(`src/pybind/rbd/rbd.pyx`). `timetuple()` keeps the datetime's own
calendar fields and drops its offset, and `mktime` reads those fields as
local time of the process. A UTC cutoff therefore lands at the instant
minus the client's UTC offset: a client ahead of UTC purges too little, and
a client behind UTC removes images whose deferment has not ended at the
cutoff. Only a client whose timezone equals the datetime's gets the instant
it passed.

Squid 19.2.5, Tentacle 20.2.4 and Umbrella 21.2.0 have the same conversion.
Upstream reference: [tracker 80938](https://tracker.ceph.com/issues/80938).

Every published image reproduces it in the two subtests away from UTC. The
`rbd trash purge --expires-before` command does not use this conversion and
is not covered.

## Requirements

- One cluster with one OSD (`ceph.Run`) and its client toolkit, whose
  `python3` has the `rados` and `rbd` modules. Image roles `essential` and
  `client`, of one release.
- No zone database: the client timezones are POSIX strings in `TZ`
  (`UTC0`, `TST-9` for nine hours ahead of UTC, `TST9` for nine behind).
- The test bounds itself to 15 minutes; the batch timeout is 20 minutes.

## Tier and triggers

Standalone reproducer: the test runs on its package-level `image`, Tentacle
20.2.4 by default, which reproduces the defect. Its given and when steps fail
the test; the then step does not. It prints one line,
`EXPERIMENT_RESULT case=rbd_trash_purge_timezone result=<REPRODUCED|NOT_REPRODUCED>`,
for both subtests away from UTC together, and passes either way. No
automatic run includes it (the default checks of a pull request, pushes to
`main`, periodic samples), and it never gates anything. A maintainer can run
it on demand, also on a pull request's head with the comment below.

- Manual: `gh workflow run "Native regressions" -f batch=rbd_trash_purge_timezone`.
- Comment: `/ci run rbd_trash_purge_timezone` or
  `/ci run rbd_trash_purge_timezone on squid`.

## Running it

On the default image, which must print `result=REPRODUCED`:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestRBDTrashPurgeCutoffAcrossClientTimezones$' \
  ./internal/integration/experiments/rbd_trash_purge_timezone
```

On another image, such as a patched build, injected into the package's
`image` variable; a fixed image must print `result=NOT_REPRODUCED`. No
release fixes the defect yet.

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rbd_trash_purge_timezone_test.image=<image>" \
  -run '^TestRBDTrashPurgeCutoffAcrossClientTimezones$' \
  ./internal/integration/experiments/rbd_trash_purge_timezone
```

The cluster runs `image`; the role image variables (`CEPH_TEST_IMAGE`,
`CEPH_TEST_ESSENTIAL_IMAGE`) do not select it. The test logs the client's
`ceph --version`.

## Observations and controls

Each client timezone has a pool of its own. Before any purge, one Python
process per pool, in UTC, creates four images and moves them to the trash
with deferment delays of 10 minutes, 6, 15 and 30 hours, and reports each
trash entry's ID, name and recorded deadline. The test then fixes one
cutoff for all three purges: 10 hours after the last move, in seconds since
the epoch. Each subtest runs one Python process with its `TZ`, which reads
the trash, purges with the UTC-aware datetime of that cutoff and lists what
the trash retains. Entries are compared by name and ID.

1. Conditions before the first purge, in every pool: every recorded
   deadline is the move plus its delay within two minutes; no deadline is
   within 30 minutes of the cutoff or of the cutoff nine hours earlier or
   later; two deadlines end by the cutoff, one by the earlier instant and
   three by the later one.
2. Conditions in every subtest: the process reports the UTC offset of its
   timezone; the instant of its datetime is the one cutoff; the trash it
   reads before the purge has the IDs, names and deadlines the moves
   reported, so a deadline reads the same in each timezone; the cutoff's
   fields read as the process's local time (`time.mktime`) name the instant
   its UTC offset before the cutoff.
3. Control `utc`: the trash retains the 15-hour and 30-hour images. The
   binding, the purge and the listing work as the contract says. It runs
   first; when it fails, the other pools are not purged.
4. `ahead-9h` and `behind-9h`: the trash must retain the same two entries.

What a purge retained is compared with two sets predicted from the trash
before it: the entries whose deadline is after the cutoff, and the entries
whose deadline is after the instant the cutoff's fields name as local time.
Only a purge away from UTC that retained the second set is reported as the
defect. A purge that retained neither set, such as one that removed nothing
or everything, is another fault.

The cutoff sits about four and five hours from the nearest deadlines, and a
nine-hour shift moves it across exactly one of them in either direction, so
the seconds between the moves of the pools do not change an outcome. The
10-minute image is removed and the 30-hour image retained in every
timezone.

## Reading the result

- **REPRODUCED:** a subtest away from UTC retained what the cutoff's fields
  read as local time keep; the symptom starts with the subtest name and
  `observed defect:`. Nine hours ahead of UTC the 6-hour image is retained as
  well; nine hours behind, only the 30-hour image is, so an image whose
  deferment ends five hours after the cutoff was removed. The receipt
  (`RBD_TRASH_PURGE_TIMEZONE_RECEIPT`) gives the shift of the cutoff.
- **NOT_REPRODUCED:** all three subtests retained the 15-hour and 30-hour
  images. Check the receipts: each must name its UTC offset.
- **Test failure (no result):**
  - `condition not established`: a deadline is not where the cutoff needs
    it, the process did not run in the timezone or did not purge with the
    one cutoff, the trash changed between the moves and the purge, or the
    cutoff's fields read as local time are not the client's UTC offset from
    it.
  - `control failed`: the purge from UTC did not retain exactly the 15-hour
    and 30-hour images. The other subtests fail with the same prefix without
    purging.
  - `another fault:`: a purge away from UTC retained neither the entries the
    cutoff keeps nor the entries its local reading keeps. It is not the
    defect of this case.
  - The cluster or its client does not start, or the client's `python3`
    lacks the `rbd` module.

Native results (Linux amd64 host, Docker, aggregate image, one run):

| Image | Result | Time |
| --- | --- | --- |
| Tentacle 20.2.4, `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7…` (the default image when recorded) | `REPRODUCED`: ahead-9h retains `due-6h` too; behind-9h retains only `due-30h` | 38 s |

No fixed image exists to run. Earlier, failing-test runs on role images
reproduced on Tentacle 20.2.4 (38 s), Umbrella 21.2.0 (37 s) and Squid
19.2.5 (45 s).

## Cleanup and limits

The pools and their trash end with the cluster; nothing outside it is
changed, and `TZ` is set only in the environment of each Python process.

Not covered: arm64, a naive (timezone-free) cutoff, timezones with
daylight saving, the `rbd` command line, and the trash purge schedules of
the `rbd_support` module.
