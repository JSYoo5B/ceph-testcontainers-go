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

Every published image reproduces it in the two subtests away from UTC.
That is the expected result for an unpatched defect; it is not a fixture
failure. The `rbd trash purge --expires-before` command does not use this
conversion and is not covered.

## Requirements

- One cluster with one OSD (`ceph.Run`) and its client toolkit, whose
  `python3` has the `rados` and `rbd` modules. Image roles `essential` and
  `client`, of one release.
- No zone database: the client timezones are POSIX strings in `TZ`
  (`UTC0`, `TST-9` for nine hours ahead of UTC, `TST9` for nine behind).
- The test bounds itself to 15 minutes; the batch timeout is 20 minutes.

## Tier and triggers

Deliberately failing reproducer (without an expectation): one run takes
about 40 seconds (38 s on Tentacle 20.2.4, 37 s on Umbrella 21.2.0, 45 s on
Squid 19.2.5) and fails in the `ahead-9h` and `behind-9h` subtests on every
published image. No
automatic run includes it (the default checks of a pull request, pushes to
`main`, periodic samples), and it never gates anything. A maintainer can
run it on demand, also on a pull request's head with the comment below.

- Manual: `gh workflow run "Native regressions" -f batch=rbd_trash_purge_timezone`.
- Comment: `/ci run rbd_trash_purge_timezone` or
  `/ci run rbd_trash_purge_timezone on squid`.

## Running it

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestRBDTrashPurgeCutoffAcrossClientTimezones$' \
  ./internal/integration/experiments/rbd_trash_purge_timezone
```

or, exactly as CI runs the batch:

```sh
python3 .github/scripts/tag_scenarios.py optional --batch rbd_trash_purge_timezone \
  --package ./internal/integration/experiments/rbd_trash_purge_timezone \
  --directory "artifacts/rbd-trash-purge-timezone-$(date -u +%Y%m%dT%H%M%SZ)"
```

To check the reproducer against an affected build, declare the expected
verdict; the run then passes only on that verdict:

```sh
CEPH_TEST_EXPECTED_VERDICT=rbd_trash_purge_timezone=REPRODUCED \
python3 .github/scripts/tag_scenarios.py optional --batch rbd_trash_purge_timezone \
  --package ./internal/integration/experiments/rbd_trash_purge_timezone \
  --directory "artifacts/rbd-trash-purge-timezone-$(date -u +%Y%m%dT%H%M%SZ)"
```

The runner refuses an existing `--directory`, so the timestamp gives each
run, such as each release of a comparison, its own.

The test logs the client's `ceph --version`; record the image digests with
the result.

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

## Verdict

The test reports one unit, `rbd_trash_purge_timezone`, in one
`REPRODUCTION_VERDICT` line, with the observations `ahead-9h` and
`behind-9h`. Both subtests evaluate the same conversion with the same
trigger and the same UTC control; they differ only in the sign of the
shift, and a build that fixes the conversion fixes both directions, so they
are one unit and a row expects one verdict for the case. `utc` is the
control, not an observation.

- **REPRODUCED:** a purge away from UTC retained what the cutoff's fields
  read as local time keep (the `ahead-9h` or `behind-9h` subtest fails with
  `observed defect:`). Nine hours ahead of UTC the 6-hour image is retained
  as well; nine hours behind, only the 30-hour image is, so an image whose
  deferment ends five hours after the cutoff was removed. The receipt
  (`RBD_TRASH_PURGE_TIMEZONE_RECEIPT`) gives the shift of the cutoff. Every
  published image gives this verdict.
- **NOT_REPRODUCED:** the UTC control held and both purges away from UTC
  retained the 15-hour and 30-hour images. Check the receipts: each must
  name its UTC offset.
- **INCONCLUSIVE:** the reason names the category. Unavailable
  prerequisite: the cluster or its client does not start, or the client's
  `python3` lacks the `rbd` module (the test ends before a verdict).
  Condition not established: a deadline is not where the cutoff needs it,
  the process did not run in the timezone or did not purge with the one
  cutoff, the trash changed between the moves and the purge, or the
  cutoff's fields read as local time are not the client's UTC offset from
  it. Control failed: the purge from UTC did not retain exactly the 15-hour
  and 30-hour images; the other subtests are not purged. Another fault: a
  purge away from UTC retained neither the entries the cutoff keeps nor the
  entries its local reading keeps.

Without `CEPH_TEST_EXPECTED_VERDICT` a reproduction fails the observing
subtest, absence passes it and an inconclusive run fails it. With
`CEPH_TEST_EXPECTED_VERDICT=rbd_trash_purge_timezone=<REPRODUCED|NOT_REPRODUCED>`
only that verdict passes.

## Cleanup and limits

The pools and their trash end with the cluster; nothing outside it is
changed, and `TZ` is set only in the environment of each Python process.

Not covered: arm64, a naive (timezone-free) cutoff, timezones with
daylight saving, the `rbd` command line, and the trash purge schedules of
the `rbd_support` module.
