# Measuring OSD debug gathering and output

`TestOSDDebugOverhead` is an opt-in measurement of one surviving owned OSD's
`debug_osd` runtime setting. It reuses `OverrideDebugLevel`, including uncertain
apply ownership and same-generation restoration. It does not expand that API's
supported daemon roles, choose recommended levels, or diagnose an intermittent
failure. The first profile uses one OSD, one single-replica pool and the existing
native Python librados client in the selected essential/client images.

Run through the source-owned optional batch on an authorized Linux Docker engine:

```sh
python3 .github/scripts/tag_scenarios.py optional \
  --batch debug_overhead --package ./internal/debugoverhead \
  --directory artifacts/debug-overhead
```

Alternatively, run only the named test with local opt-in tags:

```sh
CGO_ENABLED=0 go test -mod=readonly -tags=integration,debug_overhead \
  -run '^TestOSDDebugOverhead$' -count=1 -v -timeout=20m ./internal/debugoverhead
```

Select immutable role images as described in [Testing](../contributing/testing.md#choosing-images).
The test is absent from `all` and required CI execution. Its optional profile is
discovered from source; it does not add a workflow or change PR gating policy.

## Three controlled arms

The same OSD/container start generation runs three trials, each with three arms.
Before measurement, one fixed unmeasured 128-pair workload creates and exercises
the object, followed by a one-second wait. Every measured write thus replaces an
existing object under the same API semantics.
The warm-up's own receipt is retained, and it does not reset all caches or logs.
The fixed cyclic orders are `0/0, 0/20, 20/20`, `0/20, 20/20, 0/0`, and
`20/20, 0/0, 0/20`. Each level thus occupies each position once. Only the explicitly selected option changes; other subsystems retain their
existing settings. The `debug_overhead` profile selects `debug_osd`; the separate
`debug_ms_window` profile below selects `debug_ms`.

| Arm | Selected log/gather levels | Interpretation |
| --- | --- | --- |
| `0/0` | Output 0, gather 0 | A low-threshold control; level 0 and other subsystems still run. |
| `0/20` | Output 0, gather 20 | Separates the selected gathering threshold from normal output. |
| `20/20` | Output 20, gather 20 | Adds the selected normal-output threshold to the gathering arm. |

These controls follow the pinned Tentacle logging boundary described in
[issue #50](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/50): collected
entries can consume formatting/queue/ring work without appearing in normal
output. Normal-output bytes alone therefore do not measure collection cost.
The helper's single-number form would set both thresholds and cannot isolate
that distinction.

Every arm verifies the normalized effective value and generation, performs the
same 128 acknowledged `write_full` and 128 read/byte-check operations on one
32-KiB known object, then restores and independently reads back the exact prior
debug setting before the next arm. A non-nil handle is registered for failure
receipts and fresh-context cleanup even when apply reports an error. An
unconfirmed application/restoration fails the measurement; the next arm is not
started. A fresh native process checks the final object after all windows.

The `DEBUG_OVERHEAD_RECEIPT` JSON records all nine arms, every pair latency,
the applied/restored handle receipts, FSID, frozen container/start generation,
and inspected immutable target/client image IDs. Keep it with the runner's
source/build/image and cleanup records; a receipt line before a later test or
cleanup failure is not a successful completed measurement.

## Independent measurements and limits

| Field | Actual scope and units |
| --- | --- |
| `workload.write_read_pair_ns`, `elapsed_ns` | Python monotonic time for acknowledged write + same-process verified read; connection setup is excluded. |
| `cpu_delta_ns` | Difference of Docker `cpu_stats.cpu_usage.total_usage`, container CPU nanoseconds. Not CPU percent, client CPU, or total engine cost. |
| `before/after.memory_usage_bytes`, `memory_usage_delta_bytes` | Two Docker memory-usage endpoints, including cache/accounting effects. Signed delta; not peak memory, heap/RSS, or a recent-ring-only measurement. |
| `docker_log_payload_bytes` | Stdout/stderr payload bytes selected by engine-clock start/end, without Docker framing/timestamps, at most 16 MiB per arm. Other backends are not included. |

The target must use unrotated `json-file` logs. The pinned client requests one-shot
non-streaming Docker stats from the selected container ID. The sampler inspects
that endpoint's ID, start time and running/unpaused state before and after the
stats call. A response `id`, when supplied, must match; a one-shot response that
omits it remains bound by those endpoint/generation inspections. The sample's
`stats_identity` records whether a matching response ID was also supplied.
Moby 28.0.4's [direct one-shot path](https://github.com/moby/moby/blob/v28.0.4/daemon/stats.go#L39-L53)
encodes the [Linux stats constructor](https://github.com/moby/moby/blob/v28.0.4/daemon/stats_unix.go#L47-L49)
without the ID assignment performed by its other collection path.
An explicit null, empty, mistyped or contradictory response ID is refused.
Invalid/truncated/over-limit streams, missing counters, CPU rollback,
nonadvancing sample time, restart or replacement refuse comparison. An observed
zero remains distinct from missing input. Parse refusals report bounded,
allowlisted field states for JSON, generation, response ID, read time, CPU and
memory, without retaining the raw body, IDs or invalid field text. The receipt
records the effective file/stderr/syslog/journal backend
switches and `log_max_new`/`log_max_recent` values. This reports the current
bounded recent-ring policy; it does not modify or clear it.

Each OSD accounting interval includes the client connection, instrumentation,
workload, a fixed one-second post-workload observation, and background daemon
work. The log window is inside the accounting samples. The fixed wait is not
proof that the asynchronous logging pipeline drained completely. File,
syslog and journal bytes, future delayed flushes, and host I/O are unmeasured.

No `log dump`/`dump_recent` runs: such a dump is an additional output-producing
phase and can bypass the normal threshold. Restoring thresholds does not clear
the recent ring, so earlier high-gather arms can retain memory into later ones.
The same generation deliberately preserves that carryover. The cyclic order
reduces one fixed ordering confound but is not randomization, independence or a
way to subtract the carryover. Three trials per arm are descriptive samples,
not a confidence bound or proof of negligible overhead. Cache warming, PG/MON
background work, engine scheduling and observation itself can perturb timing;
do not treat differences as additive causal components or a later pass as a
repair of an earlier race. Broader roles/images/platforms, repeated independent
clusters and stronger memory attribution are possible later experiments, not
claims supplied by either current OSD cohort.


## Explicit Messenger operation-window cohort

`TestOSDMessengerDebugOverhead` uses the same workload, cyclic controls,
restoration ownership, output cap and endpoint-generation accounting as the
original OSD cohort. It changes only `debug_ms`, and has its own explicit
optional profile; `debug_osd` measurements do not substitute for this option.

```sh
python3 .github/scripts/tag_scenarios.py optional \
  --batch debug_ms_window --package ./internal/debugoverhead \
  --directory artifacts/debug-ms-window
```

For a direct named execution, select `-tags=integration,debug_ms_window` and
`-run '^TestOSDMessengerDebugOverhead$'`. Neither measurement executes under
`all` alone or required CI. Prepare the same immutable essential/client images
and retain the exact source/build/image/native/cleanup receipts with the JSON.

Each arm records `operation_interval.started_at`, `finished_at` and `budget_ns`:
the controller's UTC envelope immediately around the client exec request, with
an explicit one-minute request budget. Zero/reversed/nonadvancing or over-budget
intervals refuse acceptance. It includes Python startup, connection and
shutdown; Python's monotonic pair/loop latencies exclude connection setup.
The wider OSD CPU/memory interval also includes instrumentation and the fixed
post-workload wait. Applied/restored handle transitions delimit the full
raised-level interval, including sampling and bounded log collection. These
are separate observation intervals; none is silently reported as another.
The one-minute request budget and one-second observation are fixed experiment
inputs, not application timeout recommendations or a process-quiescence claim
for a lost exec response. Failed operations are not successful measurements;
registered restoration and owned resource cleanup still run.

## Level and interval policy

The supported helper policy is **caller-selected level and bounded operation
interval, with no generic logging default**. Choose one owned supported daemon
and the option relevant to a concrete operation, open the window before that
operation, and end it immediately after the selected operation returns. The
caller supplies a workload timeout appropriate to that operation, registers any
non-nil handle (also with an apply error), and restores using a fresh bounded
context. The existing finalization collects a known failure before restoration
and retains the later restoration receipt. Never use an expired workload
context as the only restoration context.

This decision follows the evidence boundary, rather than assuming a successful
verbose run proves safety. The accepted `debug_osd` cohort's
[analysis](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/50#issuecomment-6398628334)
already shows that nearly quiet normal output can coincide with different
container CPU and workload observations under high gathering. A separate
`debug_ms` cohort supplies that option's actual windows and cost observations,
not a transport-independent optimum. Both contain only three cyclic samples
on one surviving generation, persistent recent-ring/cache history and one
selected workload. They do not compare every useful diagnostic event/level or
independent cluster conditions, so neither `20/20`, `0/20` nor a fixed duration
is selected as a universal diagnostic default. `0/0` is also a control rather
than a silence guarantee. Callers must weigh the selected event coverage,
observed output/cost and timing perturbation for their operation, and retain
its original success criteria.

This completes the policy decision for the explicitly supported surviving-OSD
admin-socket model: explicit selection and verified restoration, rather than
auto-enabling logging on failure. It does not claim a negligible-cost threshold,
causal cost decomposition, race repair, or replacement-spanning policy. Such
claims need their own workload and evidence; broader roles are not required to
use the current helper's declared model.
