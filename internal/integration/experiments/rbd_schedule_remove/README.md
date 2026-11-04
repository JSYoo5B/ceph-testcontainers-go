# RBD schedule removal with malformed input

Owner: RBD. Kind: cross-build characterization.
Test: `TestDiscoveryTrashScheduleBoundary`, optional batch `discovery_schedule`.

## Use case and native contract

`rbd trash purge schedule remove` and `rbd mirror snapshot schedule remove`
take an interval and an optional start time. Both handlers of the `rbd_support`
MGR module share one removal implementation (`Schedules.remove` in
`src/pybind/mgr/rbd_support/schedule.py`). A malformed interval or start time
is invalid input and should be refused with `EINVAL`.

Trigger: remove with a malformed interval (`dummy`) or a valid interval and a
malformed start time (`1d dummy`) at the global level, once with no schedule
and once with a `2d` schedule present.

Up to Squid 19.2.2 the strings are parsed only after a schedule is found, so
with no schedule the command exits 0. From 19.2.3, and in 20.2.4 and 21.2.0,
both are parsed first and every case exits 22.

Upstream references:

- [tracker 70640](https://tracker.ceph.com/issues/70640), fix
  [ceph/ceph#62484](https://github.com/ceph/ceph/pull/62484), Squid backport
  [ceph/ceph#62965](https://github.com/ceph/ceph/pull/62965)
  ([tracker 70675](https://tracker.ceph.com/issues/70675)).

## Requirements

- One cluster: 1 MON, 1 MGR, 1 OSD, pool defaults 1/1. Image roles
  `essential` and `client`. One release for every role.
- `python3` in the client role (the official image ships it).
- The test bounds itself to 5 minutes; the batch timeout is 7 minutes.
- The fixture exposes no schedule API. The test enables `rbd_support`
  explicitly and never waits for a schedule to fire.

## Running it

The build line needs the batch's CI tags, so run it through the tag runner
from the module root:

```sh
CEPH_TEST_IMAGE=quay.io/ceph/ceph:v19.2.2 \
python3 .github/scripts/tag_scenarios.py optional \
  --batch discovery_schedule \
  --package ./internal/integration/experiments/rbd_schedule_remove \
  --directory artifacts/rbd-schedule-remove-19.2.2
```

Use a pinned digest (`quay.io/ceph/ceph:v19.2.2@sha256:...`) or the published
role aggregate when the result is evidence. The periodic `discovery` mode adds
this test on 19.2.2 and 19.2.3 automatically and freezes the image digests in
its plan; `periodic_execute.py` copies every JSON case line into the durable
receipt as `characterization`.

## Observations and controls

Each native command prints one JSON line: `case`, `argv`, `code`, `out`,
`err`. The `version` case records `ceph --version` from the client role.

| Case | Meaning |
| --- | --- |
| `empty-before`, `empty-after` | Control: the policy is empty before and after the empty-policy removals (asserted). |
| `empty-invalid-interval`, `empty-invalid-start` | The characterized outcome: recorded, never asserted. |
| `add`, `present-before` | Control: a `2d` schedule exists (asserted). |
| `present-invalid-interval`, `present-invalid-start` | Must exit nonzero on every build (asserted). |
| `present-after` | The policy is unchanged by the refused removals (asserted). |
| `cleanup`, `restored` | The `2d` schedule is removed and the policy is empty again (asserted). |

Every case runs for the trash purge handler and, prefixed `mirror-`, for the
mirror snapshot handler.

## Reading the result

- **Pass:** every asserted control and the populated refusals held. A pass is
  not compatibility success; read the `empty-invalid-*` codes.
- **Observed old behavior:** a pass whose `empty-invalid-*` cases exit 0
  (19.2.2). This is characterized acceptance of invalid input, not a supported
  operation.
- **Not reproduced:** a pass whose `empty-invalid-*` cases exit 22.
- **Unavailable prerequisite:** the cluster does not start, `rbd_support`
  cannot be enabled or the image lacks `python3`. The test fails before any
  case line.
- **Condition not established:** the policy is not empty at the start or the
  `2d` schedule cannot be added. The script's assertion fails and the test
  fails; there is no characterization to read.

## Cleanup and limits

The script removes the `2d` schedule in a `finally` block and checks the empty
policy; the harness terminates the cluster. Only the global level was
characterized. Pool- and image-level schedules, 19.2.0/19.2.1 and arm64 were
not run.
