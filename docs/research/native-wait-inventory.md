# Native test wait inventory

Refs issue 181. This is the first Docker-free unit of that research: a source
inventory of the fixed waits, timers and time budgets in the Go sources, with a
scanner that reproduces it. It measures nothing at runtime. It does not show how
much real time any call site costs, so it cannot rank savings, and it does not
establish that any wait can be shortened.

## How to reproduce

```sh
python3 .github/scripts/wait_inventory.py summary --top 5
python3 .github/scripts/wait_inventory.py scan --output wait-inventory.json
python3 -m unittest discover -s .github/scripts -p 'test_wait_inventory.py'
```

The figures below were regenerated after the owner review fixes (unit handling
and shadowed names), from the head of the pull request that added this page. Run the
scanner again for current numbers.

## What the scanner counts

| Category | Call sites | Meaning |
| --- | --- | --- |
| sleep | `time.Sleep` | An unconditional pause. |
| poll_interval | `time.NewTicker`, `time.Tick` | The period of a polling loop. |
| timer | `time.After`, `time.NewTimer`, `time.AfterFunc` | A fixed timer. In the tests it is mostly the upper-bound arm of a `select`, not a wait that a successful run sits through. |
| budget | `context.WithTimeout`, `context.WithDeadline` | An upper bound that a passing run does not wait out. |
| config_timing | Ceph option literals with a numeric value and a timer-like name | A configured delay, interval, grace, lease or similar. The value is in the option's own unit. |

Rules that keep the numbers honest:

- Comments and the contents of string and rune literals are blanked before call
  sites are matched, so a quoted or commented `time.Sleep(` is not counted.
- Durations are evaluated in Go duration units (integer nanoseconds) and
  converted to seconds only at the end, so a bare `time.Sleep(5)` is 5 ns and
  `time.Second + 1` is 1.000000001 s. Conversions that are not integral or do
  not fit a Duration stay unresolved.
- A duration is resolved only from literals, `time.` units and constants whose
  single definition in the same directory can be evaluated. A name defined more
  than once with different values is ambiguous. The scanner has no scopes, so a
  name that is also a function or closure parameter, result or receiver, a
  short-variable or range declaration, a `var` without a value, or the target of
  a compound assignment anywhere in the directory stops resolving there.
- A duration that cannot be resolved stays null and is counted in the
  `unresolved` column. It is never summed as zero.
- Sleeps and timers get a placement label. `poll_loop` means a `for` and a
  deadline or context check appear in the 14 preceding code lines, so the
  sleep is probably the retry delay of a bounded condition poll. `bare` means no
  such pair was found. This is a heuristic, for example a sleep in a `for`
  post-statement is labelled `bare`. Treat it as a triage hint and read the
  code before acting.

Counts are call sites, not executions. A sleep inside a loop runs many times and
a helper shared by several scenarios runs once per use.

## Inventory at this commit

Call sites: 1591 total, 1043 in native scenarios.

| Area | Scope | sleep | poll_interval | timer | budget | config_timing | unresolved |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| capture | non-test | 0 | 0 | 3 | 5 | 0 | 2 |
| capture | test | 8 | 0 | 0 | 9 | 0 | 0 |
| ceph | non-test | 0 | 5 | 10 | 96 | 0 | 84 |
| ceph | test | 12 | 0 | 45 | 50 | 3 | 7 |
| cephfs | non-test | 0 | 2 | 10 | 42 | 0 | 20 |
| cephfs | test | 0 | 0 | 50 | 33 | 0 | 3 |
| fault | non-test | 0 | 0 | 0 | 2 | 0 | 1 |
| fault | test | 0 | 0 | 4 | 1 | 0 | 0 |
| internal/capturearchive | test | 0 | 0 | 0 | 5 | 0 | 0 |
| internal/clustertest | non-test | 0 | 0 | 2 | 1 | 0 | 0 |
| internal/clusterutil | non-test | 0 | 1 | 2 | 0 | 0 | 0 |
| internal/debugoverhead | test | 2 | 0 | 0 | 6 | 0 | 0 |
| internal/dockerbridge | non-test | 0 | 0 | 0 | 1 | 0 | 0 |
| internal/dockerbridge | test | 0 | 0 | 1 | 10 | 0 | 0 |
| internal/multicluster | non-test | 0 | 0 | 1 | 0 | 0 | 0 |
| internal/multicluster | test | 0 | 0 | 0 | 3 | 0 | 0 |
| internal/supervised | non-test | 0 | 0 | 0 | 5 | 0 | 2 |
| internal/supervised | test | 3 | 0 | 4 | 0 | 0 | 2 |
| internal/tools | non-test | 0 | 0 | 1 | 2 | 0 | 1 |
| native-scenarios | non-test | 0 | 0 | 1 | 0 | 0 | 0 |
| native-scenarios | test | 137 | 6 | 159 | 724 | 16 | 61 |
| rbd | non-test | 0 | 0 | 2 | 17 | 0 | 8 |
| rbd | test | 0 | 0 | 5 | 19 | 0 | 1 |
| rgw | non-test | 0 | 2 | 6 | 30 | 0 | 5 |
| rgw | test | 1 | 0 | 5 | 21 | 0 | 1 |

Resolved durations per category (all areas; per call site, not per execution):

| Category | Sites | Resolved | Min | Median | Max |
| --- | ---: | ---: | ---: | ---: | ---: |
| sleep | 163 | 155 | 5ms | 500ms | 1m |
| poll_interval | 16 | 15 | 5ms | 500ms | 2s |
| timer | 311 | 284 | 5ms | 1s | 3m |
| budget | 1082 | 920 | 1ms | 105s | 35m |
| config_timing | 19 | 19 | 0ms | 2s | 120m |

Sleep and timer placement in native scenarios (heuristic, see wait_context):

| Category | poll_loop sites | bare sites |
| --- | ---: | ---: |
| sleep | 67 | 70 |
| timer | 72 | 88 |

Longest resolved sleep (bare) sites in native scenarios:

- 1m internal/integration/client_revocation_integration_helpers_test.go:132 `3 * clientKeyMonTicketTTL`
- 10s internal/integration/cephfs_retained_client_integration_helpers_test.go:875 `10 * time.Second`
- 5s internal/integration/cephfs_mds_path_fault_integration_helpers_test.go:384 `5 * time.Second`
- 5s internal/integration/cephfs_mirror_path_fault_integration_test.go:150 `5 * time.Second`
- 5s internal/integration/messenger_compression_integration_test.go:289 `5 * time.Second`

Longest resolved sleep (poll_loop) sites in native scenarios:

- 10s internal/integration/cephfs_snapshot_schedule_integration_test.go:110 `10 * time.Second`
- 3s internal/integration/cephfs_retained_client_integration_helpers_test.go:2414 `3 * time.Second`
- 3s internal/integration/rgw_notifications_integration_test.go:115 `3 * time.Second`
- 2s internal/integration/address_family_integration_helpers_test.go:184 `2 * time.Second`
- 2s internal/integration/cephfs_session_integration_test.go:244 `2 * time.Second`

Longest resolved timer (bare) sites in native scenarios:

- 3m internal/integration/messenger_backoff_integration_helpers_test.go:669 `3 * time.Minute`
- 3m internal/integration/mon_majority_loss_client_integration_test.go:376 `3 * time.Minute`
- 3m internal/integration/two_domain_outage_integration_test.go:376 `3 * time.Minute`
- 2m internal/integration/cephfs_fuse_mount_integration_test.go:237 `2 * time.Minute`
- 2m internal/integration/cephfs_retained_client_integration_helpers_test.go:213 `2 * time.Minute`

Longest resolved poll_interval sites in native scenarios:

- 2s internal/integration/rbd_multicluster_integration_helpers_test.go:166 `2 * time.Second`
- 2s internal/integration/rbd_multicluster_integration_helpers_test.go:207 `2 * time.Second`
- 1s internal/integration/placement_integration_helpers_test.go:18 `time.Second`
- 1s internal/integration/rbd_integration_helpers_test.go:180 `time.Second`
- 250ms internal/integration/rgw_admin_integration_helpers_test.go:306 `250 * time.Millisecond`

Timing options set by tests:

| Option | Sites | Values |
| --- | ---: | --- |
| `mds_reconnect_timeout` | 1 | `10` |
| `mgr/volumes/snapshot_clone_delay` | 3 | `30`, `60` |
| `mon_osd_down_out_interval` | 1 | `3` |
| `ms_inject_delay_max` | 1 | `2` |
| `ms_inject_delay_probability` | 1 | `1` |
| `osd_debug_inject_dispatch_delay_duration` | 2 | `30` |
| `osd_debug_inject_dispatch_delay_probability` | 3 | `1` |
| `osd_recovery_sleep` | 3 | `0`, `0.5`, `1.000` |
| `osd_recovery_sleep_degraded` | 1 | `0.5` |
| `osd_scrub_min_interval` | 1 | `7200.0000` |
| `rgw_lc_debug_interval` | 1 | `2` |
| `rgw_usage_log_tick_interval` | 1 | `1` |

## Reading the inventory

Most of the volume is `budget`: 724 of the 1,043 call sites in the native
scenarios are context bounds. A passing run does not wait them out, so they are
not savings candidates. They matter for failure latency and for the
ceilings of long scenarios, not for passing-run duration.

The remaining sleeps and poll intervals fall into three groups.

1. **Retry delay of a bounded condition poll** (`poll_loop`). The test waits for
   a daemon state, and the sleep is only how often it looks. Shortening the
   sleep reduces detection lag, at most by one period per wait, and does not make
   the cluster reach the state sooner. These are the necessary condition waits.
2. **Observation windows that are the test contract.** Examples from the
   longest `bare` sleeps: the 5-second pause that re-queries a PG while a
   recovery flag is held (OSD recovery receipt, OSD second fault, OSD min_size
   boundary) so that "no progress while held" is observed rather than assumed;
   the 5-second sampling period during the MDS path cut; the 10-second pause
   that shows the CephFS cap state is steady and does not depend on catching a
   revocation in progress. Shortening these weakens the evidence they exist to
   produce.
3. **Waits derived from a configured lifetime.** `3 * clientKeyMonTicketTTL`
   in the client revocation helper outlasts the rotating secrets of the
   session's last auth ticket. It is a nominal 60 seconds that moves with the
   ticket TTL, so it is a TTL-relative boundary and not an independent knob. The
   issue's CI timing comment already records this.

The long `timer` sites (2 to 3 minutes) are `select` arms that bound a wait for
a result, for example the held read after a replica returns. They are budgets in
practice even though the scanner files them as timers.

## Timing options the tests set

The `config_timing` table above is what the tests themselves configure.
Checked against the pinned Ceph source (Tentacle 20.2.4, commit
7f793731f1b39eb4f465e960113d2363c311b964):

- `mgr/volumes/snapshot_clone_delay` (30 and 60): the clone worker in
  `src/pybind/mgr/volumes/fs/async_cloner.py` calls
  `time.sleep(snapshot_clone_delay)` when it starts a clone, and the value is
  passed into the worker call, so a later configuration change does not shorten
  a sleep already entered. This is a test-injected delay, not a production
  default. Shortening it is a bounded candidate only if the pending, cancel and
  collision windows the scenarios rely on survive.
- `osd_recovery_sleep` and `osd_recovery_sleep_degraded` (0.5 in the
  second-fault fixtures): `OSD::get_osd_recovery_sleep` in `src/osd/OSD.cc`
  returns `osd_recovery_sleep` when it is non-zero and only otherwise the
  device-class variant. These values are deliberately injected to hold a
  recovery overlap window. They are the opposite of a saving; removing them
  removes the fault window.
- `rgw_data_sync_poll_interval` and `rgw_meta_sync_poll_interval` (20 by default;
  the placement test only reads them back): per `src/common/options/rgw.yaml.in`
  they are the wait after a shard has caught up with its source before polling
  for more. The scanner lists an option only where a test writes a numeric literal, so
  these two do not appear in the table.

## What this does not show

- No wall-clock contribution. A source count cannot say which of these waits
  dominates a job; that needs the phase-level timings the issue's last comment
  asks for.
- No equivalence of any shortened setting to production-default timing.
- No coverage of waits outside Go call sites: sleeps inside embedded Python or
  shell run in a container, daemon-side timers, container start and image pull.
- The heuristic labels and the same-directory constant resolution can misclassify
  a site. The scanner prefers a null duration to a guessed one.

## Decisions the owner needs to make

1. Whether the next unit is phase-level timing of the two CephFS clone scenarios
   (needs a native run on a Linux runner) before any delay value is changed.
2. Whether `bare` sleeps that exist only to hold an observation window should be
   marked in source so future changes cannot shorten them by accident.
3. Whether a per-scenario nominal-wait report should be added to CI artifacts, or
   the scanner stays an on-demand research tool.
4. Whether TTL-relative waits should be expressed as a multiple of one named
   constant everywhere, so a reduced TTL profile cannot desynchronize them.
