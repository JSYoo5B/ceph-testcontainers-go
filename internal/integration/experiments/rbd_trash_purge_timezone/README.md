# Python RBD trash purge reads its cutoff in the client's timezone

Owner: RBD. Kind: upstream bug reproducer (`native_regression`).
Test: `TestRBDTrashPurgeCutoffAcrossClientTimezones`, optional batch
`rbd_trash_purge_timezone`.

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

The test fails on every published image in the two subtests away from UTC.
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

Deliberately failing reproducer: one run takes about 40 seconds (38 s on
Tentacle 20.2.4, 37 s on Umbrella 21.2.0, 45 s on Squid 19.2.5) and fails in
the `ahead-9h` and `behind-9h` subtests on every published image. No
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

The runner refuses an existing `--directory`, so the timestamp gives each
run, such as each release of a comparison, its own.

The test logs the client's `ceph --version`; record the image digests with
the result.

## Observations and controls

Each subtest has a pool of its own and runs one Python process with its
`TZ`. The process creates four images, moves them to the trash with
deferment delays of 10 minutes, 6, 15 and 30 hours, reads the deadlines the
cluster recorded, purges with a UTC-aware cutoff 10 hours after the move,
and lists what the trash retains.

1. Conditions, in every subtest: the process reports the UTC offset of its
   timezone; the cutoff is the move plus 10 hours as an instant; every
   recorded deadline is the move plus its delay within two minutes, read
   the same in each timezone.
2. Control `utc`: the trash retains the 15-hour and 30-hour images. The
   binding, the purge and the listing work as the contract says.
3. `ahead-9h` and `behind-9h`: the trash must retain the same two images.

The cutoff sits four and five hours from the nearest deadlines, and a
nine-hour shift moves it across exactly one of them in either direction, so
clock differences of minutes do not change an outcome. The 10-minute image
is removed and the 30-hour image retained in every timezone.

## Reading the result

- **Pass:** all three subtests retained the 15-hour and 30-hour images. No
  published image passes today.
- **Observed bug:** the failure starts with `observed defect:`. Nine hours
  ahead of UTC the 6-hour image is retained as well; nine hours behind,
  only the 30-hour image is, so an image whose deferment ends five hours
  after the cutoff was removed. The receipt
  (`RBD_TRASH_PURGE_TIMEZONE_RECEIPT`) gives the shift of the cutoff.
- **Not reproduced:** a pass on a build claimed to be affected. Check the
  receipts: each must name its UTC offset.
- **Unavailable prerequisite:** the cluster or its client does not start,
  or the client's `python3` lacks the `rbd` module. The test or the script
  fails before a receipt.
- **Condition not established:** the failure starts with
  `condition not established`: the process did not run in the timezone, or
  a deadline or the cutoff is not where the round needs it.

## Cleanup and limits

The pools and their trash end with the cluster; nothing outside it is
changed, and `TZ` is set only in the environment of each Python process.

Not covered: arm64, a naive (timezone-free) cutoff, timezones with
daylight saving, the `rbd` command line, and the trash purge schedules of
the `rbd_support` module.
