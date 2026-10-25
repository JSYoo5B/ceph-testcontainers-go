# Container-local MON memory qualification

This opt-in research fixture separates finite-limit restoration, a possible
Ceph daemon OOM, native quorum recovery and resource cleanup. It adds no public
fault API or supported memory defaults. CPU throttling, other daemon roles,
other engines and data durability are separate experiments.

## Admission and budget

`TestDaemonMemoryFiniteRoundtrip` and `TestDaemonMemoryQualification` require a local Unix endpoint for a rootful Docker engine on Linux,
cgroup v2 memory/swap support, readable host process and target-local cgroup
identities/counters, permission to read the target PID1 executable, and at least
4 GiB currently available runner memory.
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
go test -tags integration,ci_optional,ci_batch_daemon_memory_roundtrip ./internal/integration \
  -run '^TestDaemonMemoryFiniteRoundtrip$' -count=1 -timeout=15m -v
```

The finite-roundtrip entry stops after the verified baseline is restored. The
separate `ci_batch_daemon_oom` / `TestDaemonMemoryQualification` entry additionally requires current
usage above the fixed 32 MiB pressure ceiling before its single pressure step.
It fails admission if that condition is absent; a finite-roundtrip pass never
qualifies the additional OOM experiment.

A controller without permission to read the target process is ineligible; a
local Docker socket alone does not grant that host-process read permission.
The receipt records the controller EUID. An authorized runner may execute the
already compiled, checksum-verified test binary with the
required process-read permissions, without changing host security settings.

Run Docker/native checks on an authorized Linux runner with the shared run lock;
keep substantial compilation/fixtures under its check lock. The optional source
owns separate `ci_batch_daemon_memory_roundtrip` and `ci_batch_daemon_oom`
profiles; ordinary `all` executes neither native experiment. Retain the
exact source SHA, image IDs/platform/build, engine version, requested transition,
observed limits/counters, process generations, native results and owned cleanup.
Do not publish personal host paths or raw unrelated process/environment data.

Docker-free controls exercise missing/decreasing counters, wrong generation,
non-daemon/helper attribution, generic SIGKILL and a still-running daemon. The tests also exercise the actual file observer against a replaced cgroup
identity, the actual recorder against an extra process, and the real inspect
fence against a changed generation. Plain fixture-file handle behavior does not
prove that a removed native cgroup remains readable. These are controls, not
native OOM, pressure or recovery evidence.

References: [Docker memory/swap semantics](https://docs.docker.com/engine/containers/resource_constraints/),
[kernel cgroup-v2 memory events](https://docs.kernel.org/admin-guide/cgroup-v2.html),
and [existing resource accounting adapter](../../internal/integration/container_accounting_helpers_test.go).

## Initial observed boundary

An unprivileged controller attempt at `7037e497b9eef7d19fc9e6a9cdd1c3e4a3fff020`
failed PID1-executable admission before any resource update. A separate,
checksum-verified privileged attempt at `0f0f20590c8c318f8ddbf59768f0727e028f2733`
on Linux amd64 / Docker 29.9.1 / the official Ceph 20.2.4 image observed the
finite same-generation round trip and three-member native quorum. The complete
OOM test still failed: current MON usage was at or below the predeclared 32 MiB
pressure ceiling, so no pressure update was dispatched. Owned containers were
removed. No limit was progressively reduced to obtain a kill.

Those observations qualify the finite controller prerequisite only. Actual OOM,
Ceph-victim attribution, the event-file behavior after a real cgroup teardown,
and OOM recovery remain unqualified. The separately selectable finite-roundtrip
entry lets that useful prerequisite run without disguising the experimental
OOM admission failure as a passing native OOM test. Exact final-source checks
and immutable image/build receipts belong with each execution report.
