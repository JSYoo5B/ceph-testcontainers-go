# Container-local MON memory qualification

This opt-in research fixture separates finite-limit restoration, a possible
Ceph daemon OOM, native quorum recovery and resource cleanup. It adds no public
fault API or supported memory defaults. CPU throttling, other daemon roles,
other engines and data durability are separate experiments.

## Admission and budget

`TestDaemonMemoryQualification` requires a local Unix Docker endpoint on Linux,
cgroup v2 memory/swap support, readable host process and target-local cgroup
identities/counters, and at least 4 GiB currently available runner memory.
Eligibility refusal is not native qualification. The target is the first MON
of a new three-MON cluster with no MGR or OSD; a separate client observes quorum.

The owned target starts with a finite 1 GiB `Memory` and equal `MemorySwap`
(no container swap), and no automatic restart. These are experiment ceilings,
not recommended Ceph defaults or a host reservation guarantee. The controller
never changes host configuration, disables the OOM killer, runs a host stressor,
or allocates a competing helper inside the victim cgroup.

Before any update the observer binds container ID/start, host PID/process start,
PID1 `ceph-mon` executable, and the exact container-local cgroup identity. It
requires that the cgroup contains only that process. No exec command is run in
the victim during pressure; the recorder runs outside it.

## Independent gates

1. Establish three-member quorum through the separate native admin client.
2. Inspect the finite baseline and independently read `memory.max`,
   `memory.swap.max`, `memory.current`, and `memory.events.local`.
3. With current usage below half the benign ceiling, change to 768 MiB, observe
   actual effective limits and native quorum, then restore the original 1 GiB
   ceiling on the same process/cgroup generation. No local OOM event may occur.
   HTTP success or saved inspect settings alone do not satisfy this gate.
4. Only after that gate, request one predeclared 32 MiB ceiling. Observe for at
   most ten seconds. Do not keep reducing the limit or use an allocator until
   something dies. An uncertain update reply is retained and followed by
   inspection rather than replaying the pressure mutation.
5. Keep `max`, `oom` and `oom_kill` deltas from the target-local event handle,
   Docker's OOMKilled/stopped state, and the same captured daemon generation.
   Generic exit 137, API memory failcnt, a helper victim, an unavailable counter,
   a new generation or an unobserved event cannot qualify the daemon OOM.
6. Restore the finite limit configuration before any explicit restart. A saved
   update while stopped is configuration restoration only. Independently observe
   the new process generation's actual 1 GiB/no-swap limits and fresh native
   three-member quorum after restart. Cleanup has an independent deadline.

Docker may remove the cgroup when PID1 dies. The local event file is opened
before pressure and retained outside the victim; an event window lost before
its required deltas are observed remains incomplete. A later recovery does not
turn missing OOM evidence into success. The fixture reports/fails incomplete
qualification rather than skipping an eligible but unobserved OOM.

The experiment does not prove a native pause/network comparison, heap-pressure
behavior without a kill, host-OOM immunity, an OSD data oracle, or universal
resource-update/reset semantics. Arbitrary unlimited baselines are deliberately
excluded. The first-MON-only Run customizer does not set a cluster-wide limit.

## Execution and evidence

```sh
go test -tags integration,ci_optional,ci_batch_daemon_oom ./internal/integration \
  -run '^TestDaemonMemoryQualification$' -count=1 -timeout=15m -v
```

Run Docker/native checks on an authorized Linux runner with the shared run lock;
keep substantial compilation/fixtures under its check lock. The optional source
owns `ci_batch_daemon_oom`; ordinary `all` does not execute pressure. Retain the
exact source SHA, image IDs/platform/build, engine version, requested transition,
observed limits/counters, process generations, native results and owned cleanup.
Do not publish personal host paths or raw unrelated process/environment data.

Docker-free controls exercise missing/decreasing counters, wrong generation,
non-daemon/helper attribution, generic SIGKILL and a still-running daemon. These
are classifier controls, not native OOM, pressure or recovery evidence.

References: [Docker memory/swap semantics](https://docs.docker.com/engine/containers/resource_constraints/),
[kernel cgroup-v2 memory events](https://docs.kernel.org/admin-guide/cgroup-v2.html),
and [existing resource accounting adapter](../../internal/integration/container_accounting_helpers_test.go).
