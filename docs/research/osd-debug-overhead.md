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
`20/20, 0/0, 0/20`. Each level thus occupies each position once. Only
`debug_osd` changes; other subsystems retain their existing settings.

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
repair of an earlier race. `debug_ms`, other roles/images/platforms, repeated
independent-cluster campaigns and stronger memory attribution remain #50 work.
