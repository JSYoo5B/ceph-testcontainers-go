# Measuring dependency-role overhead

`internal/tools/roleoverhead` compares an explicit aggregate selection with
essential + client through the real Go consumer. It starts one MON, one MGR
and one file-backed BlueStore OSD, without optional services. No product
defaults or publication tags change.

Run on the native Linux Docker host with a local Unix socket and the three
images already available. Hold the runner's exclusive Docker workload lock.
Build the binary from a clean checkout so its VCS revision is recorded:

```sh
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
it is not process RSS. Retain both memory measures. Shared page accounting
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
