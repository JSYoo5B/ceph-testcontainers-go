# Feasibility: block I/O faults beneath BlueStore

This report is the first increment of issue #7. It decides whether an
optional, test-owned block-device fault backend (a sparse backing file, a
loop device and a device-mapper mapping with `delay`, `error` and `flakey`
tables) can produce bounded delays and I/O errors on a disposable Linux host,
before any BlueStore OSD is attached to it. It records the alternatives,
eligibility, privileges, ownership and cleanup, and what a bounded probe
actually observed. This study commits no public API and runs no Ceph daemon.

Two kinds of statement appear below, and they are kept apart:

- **Runtime evidence**: observed by
  [`.github/scripts/block_fault_probe.py`](../../.github/scripts/block_fault_probe.py),
  run by the
  [Block fault probe](../../.github/workflows/block-fault-probe.yml) workflow
  on a GitHub-hosted `ubuntu-latest` runner, and read from the run's
  `block-fault-report` artifact. Runner details that do not bear on the
  result are omitted.
- **Source evidence**: read from Linux, util-linux or Ceph source and
  documentation, mostly in the audits recorded on issue #7. It was not
  observed in a running kernel or OSD.

## Summary

| Question | Outcome | Basis |
| --- | --- | --- |
| Hosted runner eligible without installing anything | **Yes.** `losetup` and `dmsetup` present, DM driver 4.50.0; `delay` and `flakey` were not registered but their modules were installed and loaded on the first table load | Runtime evidence |
| Baseline aligned direct I/O, exact bytes | 64 KiB write, flush and read; read hash equals the written pattern | Runtime evidence |
| `delay` with separate read/write/flush classes | 300/600/900 ms requested; 0.301/0.610/0.905 s and 0.306/0.630/0.908 s observed in two runs, each class identified by its own value | Runtime evidence |
| `error` window | read and write fail with `EIO` | Runtime evidence |
| `flakey` `error_writes` / `error_reads` | the selected direction fails with `EIO`, the other succeeds with exact bytes | Runtime evidence |
| Restore of the original linear table | verified table and exact baseline readback after every fault | Runtime evidence |
| Owned cleanup | mapping observed absent, loop association observed unbound, file removed | Runtime evidence |
| Whole-device `delay` on a host with udev | **Control uncertainty.** The kernel installed the table, but `dmsetup resume` timed out after 15 s while another process held the mapping open | Runtime evidence (first run) |
| OSD attachment, native workload effect | Not attempted | Out of scope |
| Docker Desktop, arm64, rootless engines | Not run | Unverified |

Recommendation: **conditional go** for the next step, a separately reviewed
experiment that proves an OSD opened the owned mapping and that a native
workload reaches it. The conditions are listed under
[Go/no-go for the next step](#gono-go-for-the-next-step). Nothing here
supports a public extension API or a general host-storage manager.

## Backend alternatives

| Backend | Device fault | Owned and disposable | Assessment |
| --- | --- | --- | --- |
| Test-owned sparse file → loop → device-mapper (`linear`, `delay`, `error`, `flakey`) | Yes, at the block layer below BlueStore | Yes: file, loop association and mapping are created and removed by the test | **Selected** for the probe; shown feasible on a scratch mapping |
| Host LV or a dedicated physical/virtual disk under device-mapper | Yes | Only if an operator dedicates it | Alternative, not a prerequisite. The project must not discover, format or reconfigure host disks |
| `scsi_debug`, `null_blk`, `nbd` or `ublk` devices | Yes, with their own fault options | Needs a module load with parameters, a server or a newer kernel | Host configuration change; outside the "no automatic module or host change" constraint |
| Ceph-level injection (`bluestore_debug_inject_read_err`, debug object errors, `objectstore_blackhole`) | No, it fails above the device | Yes | Useful for other contracts, but does not prove device I/O delay or errors |
| Temporary fullness, network shaping | No | Yes | Separate contracts; already covered elsewhere |
| Syscall interception inside the OSD container (preload or seccomp) | No, it changes the daemon's own calls | Yes | Exercises a different layer and alters the daemon under test |

Physical host devices, a separate block.db/WAL layout and capacity
exhaustion need their own scope and evidence, as issue #7 states.

## What the probe does

The probe is a single Python 3 script with no third-party dependencies. It
is meant only for a disposable host and refuses to start when not root. It
uses only the tools it finds and never installs anything. Every command runs
with a timeout (15 s for control commands, at most 20.9 s for one I/O),
and a killed child that cannot be reaped is recorded rather than waited for.

1. **Eligibility.** It records the kernel release and machine, the effective
   UID, `losetup --version`, `dmsetup version` and `dmsetup targets`. Linux,
   root, both tools and a DM driver are required. When `delay` or `flakey` is
   not registered, `modinfo -n` checks (read-only) whether the installed
   kernel ships the module. Without `--allow-target-autoload` an
   unregistered required target is ineligibility. With the flag, which the
   workflow passes on the disposable runner, the first table load may make
   the kernel load an installed module on demand; the probe itself never
   loads a module, and the targets before and after are both recorded. A
   missing optional target (`flakey`) skips only its phases.
2. **Owned chain.** It creates a work directory and a 64 MiB sparse file with
   `O_EXCL`, records its device and inode, binds it with
   `losetup --find --show`, and verifies through `losetup --json --list`
   that exactly that loop device is associated with that inode, with no
   offset or size limit. It then creates a linear mapping by loop
   major:minor (not by path) with a random name `ctc-blockfault-…` and UUID
   `CTC-BLOCKFAULT-…`, and uses `/dev/mapper/<name>` only when its device
   number equals the mapping's.
3. **Baseline.** A child process opens the mapping, checks it is the
   expected block device number, and performs one 64 KiB `O_DIRECT` write
   of a deterministic pattern, an `fsync` and one `O_DIRECT` read at a
   4 KiB-aligned offset, recording elapsed time, `errno` and the SHA-256 of
   the bytes.
4. **Faults.** Each phase loads a table (an inactive table), suspends and
   resumes, and observes `dmsetup info`, the live table and the inactive table
   after each of the three transitions. The fault covers a 1 MiB window at
   32 MiB with linear segments around it. Bounded direct I/O then observes
   the effect, and only after that I/O completes does restoration start, so
   a suspend cannot shorten a delay being measured. The phases are:
   - `delay` with the nine-argument form, read 300 ms, write 600 ms and
     flush 900 ms. The write repeats the baseline bytes, so its outcome
     cannot change the expected content.
   - `error`, with a read and a write of different bytes, both expected to
     fail with `EIO`.
   - `flakey` with `up_interval` 0 and `down_interval` 3600, so every
     operation in the window falls into the down interval regardless of
     when the table was loaded, with `error_writes` and then `error_reads`.
     `drop_writes` and corruption features are not used.
5. **Restore.** It reloads the original linear table the same way, verifies
   the live table equals it exactly with no inactive table and no
   suspension, and reads the window back. A hash other than the baseline
   is a failure.
6. **Cleanup.** See [Ownership and cleanup](#ownership-and-cleanup).

The report keeps four states apart for each phase: the requested table,
the installed live and inactive tables with the mapping's identity after each
transition, the observed I/O effect, and (only in a later step) the native
OSD effect. A timed-out load, suspend or resume, an identity change or an
incomplete observation halts every later control action; the probe does not
retry, and the verdict becomes `uncertain`. The verdict is `complete`,
`ineligible`, `failed` or `uncertain`; the workflow fails for the latter two.
`complete` means that nothing failed and nothing is uncertain. It does not
mean that every effect was observed: an operation can be `unverified` (for
example a flush when the mapping has no write-back cache) and its phase
`partly-unverified` while the verdict stays `complete`. Each observation's
own `effect` field is the result to read.

The decisions are covered by Docker-free unit tests with a fake kernel
([`test_block_fault_probe.py`](../../.github/scripts/test_block_fault_probe.py)):
missing tools, privileges, targets and modules; a suspend that times out with
the mapping suspended; a successful `dmsetup remove` that leaves the mapping
present; a successful detach that leaves the association bound; a lingering
opener; a rejected optional feature; a delay that is not observed; an error
table whose write still mutates data; a foreign UUID after resume; an I/O
helper that times out without being reaped; a `dmsetup create` or
`losetup --find` child that times out without being reaped; and a device node
with a foreign device number.

## What the probe observed

The runs below used the hosted `ubuntu-latest` image on x86_64 with
util-linux 2.39.3, device-mapper library 1.02.185 and DM driver 4.50.0. The
first two ran on kernel `6.17.0-1022-azure`; by the third run the image had
moved to kernel `7.0.0-1017-azure`. `dmsetup targets` listed `linear` 1.5.0, `error` 1.7.0, `striped`
and `multipath` before the probe, and additionally `delay` 1.5.0 and `flakey`
1.5.0 after it. The mapping's queue reported `write back`, so the kernel
issues flushes to it. The runner was destroyed after each job.

### First run: whole-device delay

[Run 39794773707, job 119879220939, attempt 1](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/39794773707/job/119879220939),
head `d71de25`, verdict `uncertain`. This version applied every fault to the
whole mapping.

Setup and the exact baseline succeeded. For the delay table, `load` and
`suspend` returned within 6 ms and the observations showed the inactive table
and then `SUSPENDED`. `dmsetup resume` was killed at its 15 s timeout. The
fresh observation right after showed the state the timeout did not:

| After | State | Tables | Open count | Live table |
| --- | --- | --- | --- | --- |
| load | ACTIVE | live, inactive | 0 | linear |
| suspend | SUSPENDED | live, inactive | 0 | linear |
| resume (timed out) | ACTIVE | live | **1** | **delay** |

So the kernel had swapped in the delay table and resumed; the timeout did not
mean rollback. The command was still waiting, almost certainly for udev to
finish processing the change event: dmsetup waits for udev by default, and a
udev probe that reads the device becomes slow when every read is delayed by
300 ms. The opener was not identified in this run, so that explanation is an
inference. The probe did what the contract requires: it recorded the
reconciled state, performed no further control, and kept the mapping, the
loop association and the backing file
(`cleanup-retained:dm-mapping,loop,backing-file`) because the state was
uncertain and the mapping was open.

### Second run: faults in a window

[Run 39797464460, job 119888368624, attempt 1](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/39797464460/job/119888368624),
head `ec62fba`, verdict `complete`, artifact `block-fault-report`. The probe
then confined faults to the window at 32 MiB and recorded the processes
holding the mapping open whenever its open count was not zero. For example,
the installed delay table was:

```text
0 65536 linear 7:0 0
65536 2048 delay 7:0 65536 300 7:0 65536 600 7:0 65536 900
67584 63488 linear 7:0 67584
```

| Phase | Operation | Expected | Observed | Restore |
| --- | --- | --- | --- | --- |
| baseline | write, `fsync`, read | exact | 0.0001 s, 0.0008 s, 0.0001 s; read SHA-256 `ee4c6608…121fb1e1` equals the pattern | n/a |
| delay | read | ≥ 0.24 s | 0.3014 s, exact bytes | restored, exact |
| delay | write | ≥ 0.48 s | 0.6099 s | |
| delay | `fsync` (flush) | ≥ 0.72 s | 0.9051 s | |
| error | read, write | `EIO` | `EIO`, `EIO` | restored, exact |
| flakey `error_writes` | read, write | exact, `EIO` | exact, `EIO` | restored, exact |
| flakey `error_reads` | read, write | `EIO`, success | `EIO`, success | restored, exact |

The thresholds are 80% of each configured delay, and the baseline had to be
below 20% of it for a delay to count. Every load, suspend and resume returned
within 66 ms, every observation showed the expected state, the open count
stayed 0 throughout, and `dmsetup status` showed zero queued bios in each
class after the I/O. Zero queued bios only shows nothing was queued at that
moment; it is not used as evidence of completion. The writes during the error
phases used different bytes, and the readback after restore still matched
the baseline, so the failed writes did not reach the backing file.

Cleanup in this run: `dmsetup remove` (not forced, not deferred) returned 0
and a full `dmsetup info` scan found neither the name nor the UUID;
`losetup --detach` returned 0 and a full `losetup --json --list` scan found
no association with the file's inode; then the table files, the backing file
and the work directory were removed. `dmsetup udevcookies` listed no leftover
cookie.

### Third run: the current probe

[Run 39826858735, job 119992308722, attempt 1](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/39826858735/job/119992308722),
head `1135c77`, verdict `complete`, artifact `block-fault-report`. This head
adds the retained chain for an unreaped child (see
[Ownership and cleanup](#ownership-and-cleanup)) and merges `main`; the
fault phases are unchanged. On kernel `7.0.0-1017-azure`, with the same
target versions and a `write back` queue, every observation of the second
run was repeated with `effect: observed`: delays of 0.3057, 0.6303 and
0.9082 s; `EIO` for the error read and write and for each selected `flakey`
direction; exact bytes otherwise; `restored` with the exact baseline after
every phase. Every control command returned within 59 ms. No child was
unreaped, so cleanup ran normally: mapping absent, association unbound, file
removed, nothing retained and no leftover udev cookie.

The commits after `1135c77` change only this report, so the probe and the
workflow it executed are the ones on the PR head.

## Eligibility

A host is eligible for this backend only when all of these hold, and each is
checked rather than assumed:

- Linux with a device-mapper driver, `losetup` and `dmsetup` already present.
- `linear`, `delay` and `error` registered, or their modules installed and an
  explicit decision that an on-demand kernel load is acceptable on that host.
  On a user's workstation the default must be ineligibility.
- `flakey` with `error_reads` for read errors (accepted by dm-flakey 1.5.0
  here). Older kernels reject that feature; the phase is then reported as not
  installed, not as a pass.
- Free loop devices and permission to create them.
- A write-back queue on the mapping if the flush class is to be exercised;
  otherwise the kernel sends no flush and the flush delay is unverified.
- A dedicated host or VM. The probe gives no protection against another
  administrator or process changing the same resources concurrently.

## Minimum privileges

Setup and control are host operations: binding a loop device and every
device-mapper ioctl (create, load, suspend, resume, remove) need
`CAP_SYS_ADMIN` and access to `/dev/loop-control` and `/dev/mapper/control`.
The probe runs them as root through `sudo` on the runner. Its direct I/O also
runs as root; that is a property of the probe, not of the backend.

The OSD would need much less, but this is unverified: the mapping's block
node passed into its container (for example `--device`) with read and write
permission for the OSD's user, and no capability for the fault itself.
Fault control must stay with the host-side controller so that the daemon
cannot change its own fault. Whether Docker's device cgroup and the image's
`ceph` user need extra settings is part of the next step.

## Ownership and cleanup

One lease owns the backing file identity (device and inode), the loop
device name, number and backing association, and the mapping name, UUID,
major:minor and table. A later step would join the Docker engine and the
OSD FSID and UUID to it. The probe uses no host disk, never forces a
removal and never removes a resource it cannot identify as its own.

Cleanup runs in a fixed order and each step records the command outcome
separately from the observed state:

1. If any control or I/O child timed out and could not be reaped after
   SIGKILL, keep the whole chain (mapping, loop and file) and run no removal
   command. Observing the mapping or association absent at that moment does
   not stop the child from creating or binding it later.
2. Otherwise, a complete `dmsetup info` scan that shows the mapping absent
   lets cleanup continue with the loop. If the mapping is present and an
   earlier control was uncertain or an I/O may still be in flight, keep the
   mapping and everything under it.
3. Observe the mapping. Wait up to 10 s for its open count to reach 0,
   recording the openers. Remove it only when it is our UUID and device
   number, active, not suspended and not open, then require a complete
   `dmsetup info` scan to show it gone. Exit status 0 alone is not enough,
   since a deferred removal also returns 0 while the mapping remains.
4. Detach the loop device only after the mapping is observed gone and the
   association is still exactly ours, then require a complete scan to show
   no association with the backing inode. A detach that returns 0 can leave
   a lazily detached association bound while other openers remain.
5. Remove the backing file only with no association to its inode and the
   same device and inode as created.

Anything not released is listed as retained and makes the verdict
`uncertain`; it is never deleted to tidy up.

## Scope of a whole-device fault

Without a separate block.db, BlueStore keeps its BlueFS (RocksDB metadata and
WAL) on the main block device, so a fault over that whole device also
affects metadata and the WAL, not just object data. It must be labelled a
shared BlueStore/BlueFS fault. Data-only, separate DB/WAL, silent write loss
or corruption, and capacity exhaustion need different layouts and evidence.

The OSD's reaction is not necessarily a clean client `EIO`. In
[`KernelDevice`](https://github.com/ceph/ceph/blob/v20.2.4/src/blk/kernel/KernelDevice.cc),
an aio error becomes `EIO` for the caller only when the I/O context sets
`allow_eio`; any other failed completion aborts the OSD
([L703-L731](https://github.com/ceph/ceph/blob/v20.2.4/src/blk/kernel/KernelDevice.cc#L703-L731)),
and a failed `fdatasync` in `flush` aborts it too
([L533-L539](https://github.com/ceph/ceph/blob/v20.2.4/src/blk/kernel/KernelDevice.cc#L533-L539)).
With `bdev_debug_aio` and `bdev_debug_aio_suicide_timeout` set, a long
enough delay also aborts it as a stalled aio
([L759-L773](https://github.com/ceph/ceph/blob/v20.2.4/src/blk/kernel/KernelDevice.cc#L759-L773)).
This is source evidence from Tentacle 20.2.4, not observed here. A
native abort is therefore a possible, source-supported outcome. The next step
has to keep operation, error and offset, the OSD's signal or exit, the
client's result and the uncertain state of any new mutation separate, and
must not replay a mutation whose outcome is unknown.

The first run also shows that a whole-device fault reaches every opener of
the mapping, including host tooling such as udev, not only the OSD.

## Limitations

- Three runs on the hosted image, on two kernels (6.17 and 7.0 azure). Kernel and target versions,
  vendor patches and udev rules differ elsewhere and must be rechecked.
- Docker Desktop (macOS, Windows), rootless engines, Podman and arm64 were
  not run. Docker Desktop's VM kernel and its device-mapper and loop support
  are unverified.
- Only one 64 KiB operation per class and phase. Delay fidelity under
  concurrent I/O, merging, readahead or a scheduler, and the PREFLUSH versus
  FUA-only routing difference recorded on issue #7, were not measured.
- The windowed fault avoids host probes of the mapping. A BlueStore fault
  cannot pick a window this way without knowing BlueFS and data placement, so
  the udev interaction of the first run is still open for whole-device faults.
- The opener during the first run was inferred, not identified.
- Cleanup is checked against a model of one owner. It cannot exclude an
  external actor that changes the resources between an observation and the
  next command.
- A killed `dmsetup` can leave a udev cookie semaphore. The second run left
  none; the first run did not yet record cookies.

## Go/no-go for the next step

**Go**, under conditions, for one separately reviewed experiment: attach an
OSD to an owned mapping prepared before OSD startup and prove that the OSD
opened that mapping and that a known native workload reaches it. That step is
not part of this report. It should proceed only when all of these hold:

- It runs on an eligible, disposable native Linux host (the hosted
  `ubuntu-latest` runner qualifies on the evidence above), and repeats this
  scratch probe there first, on a separate mapping. The verdict `complete`
  alone is not enough. The report must show, each with `effect: observed`:
  the exact baseline; the delay phase's read, write and flush (the flush
  needs a `write back` queue, otherwise the flush class is not claimed for
  the OSD step); `EIO` for both the read and the write of the error phase;
  for the `flakey` phases, only when the OSD step uses direction-specific
  errors, `EIO` in the selected direction and success in the other. Every
  restore must be `restored` with the exact baseline readback, and cleanup
  must retain nothing.
- It identifies or neutralizes host openers before a whole-device fault, for
  example by recording openers and udev completion around each transition and
  treating a non-OSD opener as an uncertain state. `--noudevsync` alone only
  stops `dmsetup` from waiting; it does not stop udev from opening the device.
- It proves attachment with identity, not a path: the OSD's block device is
  the owned mapping's device number, the OSD FSID and UUID are joined to the
  lease, and the mapping sees I/O during a known workload window (for example
  through `dmsetup status` or statistics) before any fault is claimed.
- It keeps requested fault, installed table and identity, observed device
  effect and native OSD, client and PG effect as four separate results, and
  accepts an OSD abort as a possible outcome.
- It separates fault removal from OSD restart, scrub and replica recovery,
  and never counts a read served by another replica as recovery of the
  affected OSD.
- It never runs a destructive raw probe on a mapping the OSD owns, and
  closes the OSD before releasing the mapping, loop and file.

**No-go** for now on Docker Desktop, rootless engines and arm64 until this
probe has run there, on any host where the targets are not registered and
module loading has not been explicitly accepted, and for any public API
before the OSD experiment and the fake/control coverage of issue #7 exist.
