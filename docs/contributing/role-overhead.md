# Measuring dependency-role overhead

The integrated role split addresses **unnecessary image payload**: workloads
select the capabilities they use and omit optional service/tool dependencies.
The stored-layer measurements below establish that improvement. This follow-up
measures the lifecycle/resource tradeoff of using a separate lazy client
container after the images are already available locally.

`internal/tools/roleoverhead` compares an explicit aggregate selection with
essential + client through the real Go consumer. It starts one MON, one MGR
and one file-backed BlueStore OSD, without optional services. No product
defaults or publication tags change.

Run on the native Linux Docker host with a local Unix socket and the three
images already available. Hold the runner's exclusive Docker workload lock.
Build the binary from a clean checkout so its VCS revision is recorded:

```sh
mkdir -p artifacts
go build -o artifacts/roleoverhead ./internal/tools/roleoverhead
artifacts/roleoverhead \
  -all ceph-testcontainers:roles-v2-official-19.2.5-all \
  -essential ceph-testcontainers:roles-v2-official-19.2.5-essential \
  -client ceph-testcontainers:roles-v2-official-19.2.5-client \
  -image-source IMAGE_ASSEMBLY_COMMIT \
  -output artifacts/role-overhead.json
```

The tool resolves all three local image IDs before allocation and uses those
IDs for every trial. It rejects emulated image architectures. The supplied
image-source revision is operator provenance, not inferred from an image ID.
Retain the original assembly reports to substantiate it. Output deliberately
omits engine names, addresses, filesystem roots and container configuration.
Review image references, supplied provenance and the output filename before
sharing, since these are operator inputs.

One unreported warmup pair precedes five measured pairs by default. The JSON
retains the warmups with `Warmup=true`; exclude them from summary statistics.
Pair order alternates all/split then split/all to reduce ordering bias. Each
trial records:

- `StartSeconds`: `RunWithImages` through its normal core readiness return.
- `Core`: five seconds settling followed by eleven one-shot cgroup snapshots
  at least one second apart, before any client request.
- `FirstClientSeconds`: the first `ClientContainerContext` call. Split mode
  creates an owned idle toolkit and checks connectivity/version; aggregate
  mode reuses the existing core executor.
- `RBDProofSeconds`: pool setup, RBD initialization, clean barrier and a native
  Python RBD create/write/read/remove proof. This is correctness evidence and
  elapsed setup time, not a throughput benchmark.
- `WithClient`: the same settling/sampling protocol after the proof, with no
  deliberate application traffic.
- `CleanupPassed`: successful `Terminate` plus an empty all-state container
  query for that trial's unique cluster ownership label, including warmups.

Every snapshot retains immutable container/image IDs and CPU/memory counters.
The expected three core containers and zero/one additional toolkit are checked
against resolved image IDs. Inventory changes, missing/stopped containers,
counter resets and nonadvancing clocks fail the measurement. The report is
saved after each trial, including a failed trial; only a fully successful
campaign sets `Complete=true`.

CPU is the sum of each container's CPU-nanosecond delta divided by its actual
sampling time delta, expressed as percent of **one CPU**. Memory is the mean
over snapshots of the sum of raw cgroup usage. `MeanWorkingSet` subtracts
`inactive_file` (cgroup v2) or `total_inactive_file` (v1), clamped to zero;
the engine's recorded cgroup version selects the key even when both exist,
as distinguished by [Docker's memory accounting](https://docs.docker.com/reference/cli/docker/container/stats/#description).
Unknown cgroup versions are rejected before cluster allocation. Every adjacent
sample must advance its timestamp and must not decrease its CPU counter;
unchanged CPU is valid idle time. Working set is not process RSS. Retain both memory measures. Shared page accounting
and cache ownership can affect cross-trial results.

This is a **warm, local, idle-container** comparison. It does not flush caches,
pull images, prune shared resources or change host configuration. It excludes
Docker/containerd/Ryuk/host overhead, bootstrap CPU totals, loaded throughput,
network distribution, peak memory and cold-start effects. Core sampling is
before the test pool exists; post-client sampling is after the same RBD proof
in both modes. Compare paired modes on the same host; differences between
variants on different hosts do not establish distribution performance rankings.
Report medians, ranges, replicate counts, exact sources and cleanup separately
from earlier storage or functionality results.

## Recorded experiment, 2026-10-22

### Primary result: less unnecessary image payload

The earlier final-image measurements count actual gzip Docker-save layer
payloads, with shared layers counted once, in decimal MB. Essential + client
supports ordinary RADOS/RBD without pulling in the optional daemon/tool
capabilities retained by all. The selected image IDs in this follow-up match
those size-study images.

| Role-image selection | Official-derived 19.2.5 | Debian 19.2.5 | Ubuntu 19.2.5 | Alpine 19.2.4 experimental |
| --- | ---: | ---: | ---: | ---: |
| all | 185.83 | 188.68 | 166.29 | 114.23 |
| essential + client, shared-layer union | 162.18 | 156.90 | 140.93 | 59.80 |
| Reduction relative to all | 12.7% | 16.8% | 15.3% | 47.7% |

These reductions isolate role selection relative to the new pruned all image.
Larger reductions against the installed source also include file pruning.
The compressed payload reduction is measured; actual pull, unpack and
deployment-ready time improvements require separate cold-distribution timing.

### Warm-container experiment: cost of the additional executor

The [per-trial dataset](role-overhead-results.json) contains five measured
pairs and one excluded warmup pair per variant: **40 measured trials plus
8 warmups**. All 48 native RBD proofs and 48 owned-container cleanup checks
passed. A separate final inventory found no remaining containers on the four
measurement runners. These are repetitions of this small workload, not 48
distinct features or a replacement for full image qualification.

The clean measurement binary was built at Go revision
`111d5ba9d86906381a0e3b651577b08eaf515b84`, based on merged consumer
`15c95a504963a20bb73e3401c1b1f18ad1bf42c1`. Runtime images were assembled
at image revision `7ef514c475d6c789620f7bb6f63318a154ded742`; all twelve
selected local image IDs match the earlier stored-layer measurement reports.
The diff from that image source to merged image revision
`6129d8c60e18f49b19b1f6bbc1e68f715c911d32` changes only the measurement
tool and its tests, not assembly. This experiment still records its actual
older image identities; it is not an exact-merge-image rebuild or a registry
pull. Official-derived/Debian/Ubuntu use 19.2.5; Alpine is the separate
19.2.4 experiment. No ARM64/Tentacle runtime-performance claim is made.

Warm core startup, seconds, **median [minimum, maximum]**. Each variant's pair
runs on the same Linux/amd64 host. First-client time in all mode is below
0.001 seconds because it reuses the core executor.

| Variant | Core, all | Core, split | First client, split |
| --- | ---: | ---: | ---: |
| Official-derived | 13.367 [13.349, 14.399] | 14.409 [12.865, 14.448] | 0.811 [0.798, 0.819] |
| Debian | 13.613 [13.402, 14.629] | 14.356 [13.558, 14.596] | 0.960 [0.934, 0.969] |
| Ubuntu | 12.765 [12.699, 13.248] | 13.236 [12.714, 13.247] | 0.894 [0.882, 0.908] |
| Alpine experimental | 12.894 [12.878, 13.459] | 13.364 [12.877, 13.436] | 1.013 [0.989, 1.025] |

The median **within-round split minus all** startup differences are +0.025,
+0.422, +0.476 and +0.006 seconds, respectively. Every variant's observed
paired range includes both signs; no consistent startup reduction is shown.
Subtracting the two marginal medians above is not a paired effect estimate.
The phases are timed separately with an idle sampling window between them;
these numbers are not an end-to-end application-ready latency measurement.

Post-client idle medians, **all → split**. Memory uses decimal MB; working set
is the documented cgroup estimate, not RSS. CPU is percent of one CPU.

| Variant | Raw memory, MB | Working set, MB | CPU, % |
| --- | ---: | ---: | ---: |
| Official-derived | 386.12 → 388.11 | 357.34 → 357.93 | 1.012 → 1.019 |
| Debian | 437.04 → 440.64 | 400.44 → 400.57 | 0.992 → 1.017 |
| Ubuntu | 398.03 → 401.83 | 368.39 → 369.83 | 0.437 → 0.461 |
| Alpine experimental | 502.56 → 506.60 | 464.92 → 466.63 | 0.565 → 0.620 |

Both modes use three core containers before the client call. All stays at
three afterward; split adds one idle toolkit. That toolkit's own median raw
cgroup memory is 2.62/4.79/3.69/4.13 MB and its working set is
1.23/1.35/1.33/1.70 MB, respectively. Its sampled idle CPU is below 0.005%
of one CPU in every variant median. This does not measure its transient
startup/command CPU. The dataset also retains the pre-client phase, ranges,
original report hashes, immutable IDs and cleanup results. Raw counters were
independently recomputed and checked against the summary before publication.

This experiment characterizes the added executor's lifecycle and idle cost.
The first split-client call creates/starts a container and checks Ceph
connectivity/version; the idle phase then includes that additional process.
The small aggregate differences also include normal run variation and cache
accounting. Docker daemon resource use was not measured separately, so the
entire difference cannot be assigned to Docker itself.

Both selections execute the same core daemons and RBD workload. Omitting
unused image files does not itself remove that running work. Consequently,
the absence of a CPU/memory reduction in this warm experiment is not evidence
that image lightweighting failed, or a verdict on its overall performance.
The established improvement is **smaller required image payload**, accompanied
by the measured cost of a lazy client container when that client is used.
Keep split selection for workloads that benefit from omitted capabilities;
all remains useful when broad features are wanted. A separate public client
lifecycle type is not needed to obtain this behavior. Pull/unpack latency,
loaded throughput and Docker/host overhead remain outside this measurement;
registry publication remains a separate operation.

### Measurement-tool review corrections

The [review of e4efc45](https://github.com/JSYoo5B/ceph-testcontainers-go/pull/29#pullrequestreview-5606518338)
identified two defects in the tool: it could select the local cache counter
instead of the hierarchy counter on cgroup v1 when both keys were present,
and endpoint-only validation could miss intermediate counter/time reversals.
The implementation now uses the engine's explicit cgroup version and checks
every adjacent pair. Regression tests cover both keys present on v1/v2,
underflow, unknown versions, intermediate resets, duplicate/backward clocks,
and valid idle intervals with equal CPU counters.

The retained experiment used cgroup v2 throughout, and its intermediate
observations are monotonic. Its published counters and summary values remain
unchanged. These corrections strengthen future measurement validation; the
retained native results keep their original `111d5ba`/`7ef514c` provenance
and are not relabeled as a corrected-head native execution. The smaller
required image payload is independently established by the stored-layer study.
