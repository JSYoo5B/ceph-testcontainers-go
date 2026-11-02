# OSD startup calibration

Small functional fixtures can explicitly skip mClock's automatic capacity
benchmark without switching schedulers:

```golang
storage := ceph.OSDStorage{}.
    WithBlockSize(256 << 20).
    WithStartupBenchmark(ceph.OSDBenchmarkSkip)
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithOSDStorage(storage))
if cluster != nil {
    testcontainers.CleanupContainer(t, cluster)
}
if err != nil {
    t.Fatal(err)
}
```

The ordinary fixture keeps native calibration policy. Use
`OSDBenchmarkNative` when explicitly selecting Ceph's native decision, or omit
the field to retain normal configuration precedence. Native is not a force
switch: a non-default configured capacity can avoid another calibration, and
MemStore or a different scheduler does not enter this mClock path. A benchmark
that rejects its measured capacity can leave compiled defaults in effect and
run again on a later restart.

## Resource and scheduling semantics

The inspected Squid 19.2.5 and Tentacle 20.2.4 automatic workload prefills one
hundred 4 MiB objects, then performs 12,288,000 bytes of 4 KiB random writes and
cleans up. Prefill, cache flush and cleanup are additional to the timed write
phase. The 400 MiB prefill is object payload, not an exact prediction of block
allocation, resident memory or elapsed time.

A sparse file's logical size does not reserve physical space. Writes can
materialize backing blocks, and object deletion need not immediately release
those blocks or tmpfs pages. Capacity calibration on shared file-backed or tmpfs
storage measures that environment, rather than an isolated physical drive.
Skipping it can reduce fixture I/O and allocation without a reliable fixed
wall-time saving on every backend.

`OSDBenchmarkSkip` sets only `osd_mclock_skip_benchmark=true`. It leaves the
selected queue, force flag, HDD/SSD IOPS capacities and sequential-bandwidth
assumptions unchanged. mClock remains available with the capacities from normal
configuration; those numbers are QoS assumptions, not newly measured storage
performance. Select native calibration for capacity/QoS or backing-device
experiments where calibration is part of the intended behavior.

| Storage | Automatic mClock calibration |
| --- | --- |
| Container-local sparse BlueStore | Applies under native mClock conditions; explicit Skip bypasses it |
| `WithInMemory` BlueStore on a Docker tmpfs volume | Also applies; this is not MemStore |
| `WithVolume` BlueStore or an attached block device | Explicit policy follows the owned OSD's startup; use Native when studying that backing store |
| `WithObjectStore(OSDMemStore)` | Already excluded by native Ceph; explicit Native/Skip has no calibration effect |

The tmpfs ceiling is cluster-wide filesystem allocation, not reserved RAM or an
OSD RSS limit. Include replicas, metadata, recovery and any native calibration
when sizing it. RAM/RSS samples and allocated-file/volume usage are separate
observables; neither is an exact physical-I/O or peak-memory measurement.

## Lifecycle and precedence

The policy is a copied value in the OSD storage group, passed through its
existing `WithOSDStorage` option. It reaches initial OSDs, later `AddOSD`, the
same container's `Stop`/`Start`, and a retained store's `DetachedOSD.Reopen`.
The immutable launch recipe captures the selection. Restart does not reset the
store or claim that an earlier calibration will be cached.

An explicit Native/Skip becomes a command-line false/true value on the owned
OSD before its initialization. It takes precedence over `WithConfigFile` and
the MON configuration database. A later `override.SetConfig` cannot change a
pinned command-line policy. Omission adds no benchmark flag, so existing local
files and central overrides continue to work. Repeated selection in one
`OSDStorage` replaces its earlier value; conflicting carriers and invalid values
are refused before resource allocation.

The startup script checks that the binary understands the exact selected
option/value before preparing a store. This local input check does not prove a
live daemon ran or skipped calibration. Use the running OSD's admin socket and
calibration logs for that evidence. A non-nil partial cluster returned with an
error remains owned and must be terminated.

`osd_mclock_force_run_benchmark_on_init=false` alone does not bypass the first
normal calibration. Skip takes precedence over force. Changing capacity to an
arbitrary non-default value is not a substitute for a skip policy. Reducing
`osd_bench_duration` can reject the fixed automatic workload in its admissibility
precheck rather than make it proportionately shorter. Switching to `wpq`
changes scheduler semantics and is not this policy.

## Internal functional tests

The internal integration harness sets `osd_mclock_skip_benchmark = true` in its
bootstrap configuration before the initial OSDs start. The same configuration
reaches later OSDs and retained starts. Functional tests avoid calibration I/O
without changing the library default. Explicit storage policy takes precedence;
a later configuration file can replace the harness default key by key.

Calibration controls explicitly select Native or provide their comparison
configuration. Tests of unselected-policy precedence use image selection without
the harness default, so native calibration and central configuration remain
covered independently.

## Native verification

The opt-in benchmark qualifier compares native/skip on fresh disk-backed and
tmpfs-backed BlueStore fixtures and keeps MemStore as an applicability control.
It records initial startup, later OSD addition and same-container restart
separately, with effective scheduler/skip/force/capacity settings, calibration
receipts, allocated bytes, tmpfs usage and daemon RSS where observable. Client
writes, recovery and owned cleanup remain assertions in both cohorts.

The startup-policy integration test separately exercises the public typed API,
contradictory local/central settings, initial/add/restart/retained-reopen paths
and exact-byte I/O. Native controls retain calibration; unselected policy still
honors existing central configuration. These tests require Linux and the
appropriate native build tags, as described in [Testing](../contributing/testing.md).
Compilation, live settings, observed calibration and full CI are different
validation layers.
