# Docker Desktop setup for Ceph fixtures

## Introduction

On macOS and Windows the Go test and the Ceph containers run on different
kernels: the test is a host process, and the containers run in a Linux VM that
Docker Desktop manages. The size of that VM, its kernel and the way the host
reaches it decide which fixture scenarios work. This guide walks from finding
the engine your tests use, through sizing it for a workload and preparing its
kernel, to applying, verifying and undoing the settings.

It comes with an opt-in script, `scripts/desktop/desktop_setup.py`. The script
is a host tool for a person or a setup job. No fixture, `Run` call or test
invokes it, and the Go module does not depend on it.

```sh
python3 scripts/desktop/desktop_setup.py            # plan: reads only
```

It needs Python 3.9 or later and the `docker` CLI, and nothing else.

## Find the engine your tests use

Three things are easy to confuse:

- the **client machine**, where `go test` runs;
- the **selected Docker daemon**, which testcontainers-go resolves from, in
  this order, `tc.host` in `~/.testcontainers.properties`, the `DOCKER_HOST`
  environment variable and the current Docker context;
- the **engine kernel**, which is the kernel of the VM or host that daemon runs
  on. Containers see that kernel, never the macOS or Windows one.

A Mac whose `DOCKER_HOST` points at a Linux server is not a Docker Desktop
case, and a setting changed in the local Docker Desktop does nothing for it.
The script resolves the daemon in the same order and prints what it found:

```text
Client: darwin/arm64
Engine selected by: DOCKER_HOST; transport unix (local socket)
Engine: Docker Desktop 4.99.0 (245822), engine 29.9.3, linux/aarch64, kernel 7.0.14-linuxkit, 8 CPUs, 7.75 GiB memory
Backend: desktop-macos (Docker Desktop on macOS)
```

It names one of these backends, from the client platform, the transport of the
endpoint and what the engine reports about itself:

| Backend | Recognized by | Resource settings |
| --- | --- | --- |
| `desktop-macos` | macOS client, local socket, engine reports Docker Desktop | [manual](#macos-and-other-desktop-backends) |
| `desktop-windows-wsl2` | Windows client (or a WSL distribution), Desktop engine on a WSL 2 kernel | [`.wslconfig`, automated from Windows](#windows-with-the-wsl-2-backend) |
| `desktop-windows-hyperv` | Windows client, Desktop engine on a LinuxKit kernel | [manual](#macos-and-other-desktop-backends) |
| `desktop-windows-containers` | Desktop engine in Windows container mode | none: the fixtures need Linux containers; switch Docker Desktop to Linux containers |
| `desktop-linux` | Linux client, engine reports Docker Desktop | [manual](#macos-and-other-desktop-backends) |
| `linux-engine` | Linux client, local socket, not Docker Desktop | none: the engine has the host's resources and kernel |
| `remote` | `tcp`, `ssh`, `http` or `https` endpoint, wherever it leads | set on the machine that runs the daemon |
| `unrecognized` | a local socket of another product, or an endpoint the script cannot read | set with the tool that manages that engine |
| `unreachable` | the selected daemon did not answer | start it or fix the selection first |

For a remote or unrecognized daemon the script still reports the engine's CPU
count, memory and kernel, and still compares them with the profile. Those
observations describe what the tests will get. Only the instructions for
changing them are out of its reach, and it refuses to write any local
configuration, because a local file would not change that engine. A TCP
endpoint on `127.0.0.1` counts as remote too: the script cannot tell a
forwarded port from a local daemon.

## Size the VM for the workload

| Resource | What uses it | Notes |
| --- | --- | --- |
| CPUs | every daemon of every running cluster, plus the test process on the host | Tests run one at a time unless `CEPH_TEST_PARALLEL` is 2 or more ([Testing](../contributing/testing.md#running-tests-in-parallel)). |
| Memory | MON, MGR, OSD, MDS and RGW processes, and every tmpfs page | The engine reports a little less than the VM is given. |
| Swap | relief when memory runs short | Not a substitute for memory: a daemon that swaps answers slowly, and Ceph reads slow heartbeats as failures. |
| Disk | images, container layers, named volumes | All of it lives on the VM's virtual disk, not on a shared host folder. |

OSD stores decide most of the difference between workloads:

- The default store is a 1 GiB sparse BlueStore file in the OSD container's
  writable layer. It takes VM disk as it fills, and no memory beyond the page
  cache.
- `OSDStorage.WithVolume()` puts the stores in a Docker named volume, also on
  the VM disk.
- `OSDStorage.WithInMemory()` puts them on a tmpfs volume. Every allocated
  byte of every store is VM memory, in addition to the daemons' own memory,
  and the tmpfs ceiling is a filesystem limit, not a reservation
  ([OSD startup](osd-startup-benchmark.md)). Add the stores' full size to the
  memory you give the VM. Swap can absorb tmpfs pages, at the price above.
- `WithObjectStore(OSDMemStore)` keeps objects in the OSD process, again in VM
  memory.

### Example workloads

The script ships these profiles in `scripts/desktop/profiles.json`. A value is
the least the workload needs: an engine that has more passes.

| Profile | CPUs | Memory | Free disk | Workload | Where the numbers come from |
| --- | --- | --- | --- | --- | --- |
| `single-cluster` | 4 | 4 GiB | 12 GiB | one cluster at a time on the default store, tests sequential | memory: the VM size the suite is sized for ([Testing](../contributing/testing.md#prerequisites)); CPUs: the hosted CI runner; disk: the 8 GiB image-preparation and 4 GiB runtime allowances of the smallest CI storage profile ([CI](../contributing/ci.md)) |
| `in-memory-osds` | 4 | 8 GiB | 12 GiB | one cluster of up to three 1 GiB `WithInMemory` OSDs | `single-cluster` plus 3 GiB for three fully allocated stores, rounded up; an arithmetic bound, not a measured peak |
| `ci-parity` | 4 | 16 GiB | 16 GiB | a CI batch as CI runs it: `CEPH_TEST_PARALLEL=2`, multicluster fixtures included | the hosted CI runner and the largest CI storage profile (8 GiB image preparation, 8 GiB runtime) |

The other shipped profiles have the resources of `single-cluster` and add the
kernel features of one kind of test; [Prepare the engine kernel](#prepare-the-engine-kernel)
lists them.

None of the profiles requests swap. They are starting points taken from the
sizes the suite already runs at on Linux CI runners, not Docker Desktop
measurements: no profile was qualified by running the suite on a Desktop VM
set to exactly its limits. [What was tested](#what-was-tested) lists what was.
Fewer than 4 CPUs were not measured either.

Edit the values for your workload on the command line, or keep your own
profile file:

```sh
python3 scripts/desktop/desktop_setup.py --profile in-memory-osds --memory-gib 12 --swap-gib 2
python3 scripts/desktop/desktop_setup.py --profile-file my-profiles.json --profile mine
```

A profile file has the shape of the shipped one: `cpus`, `memory_gib`,
`swap_gib`, `disk_free_gib` (0 requests nothing), an optional
`kernel_features` list and an optional `kernel_parameters` object of sysctl
names and whole numbers.

## Files, clients and networks

**File sharing and VM-local volumes.** The fixtures bind no host directory.
Cluster data lives in container layers, Docker named volumes and tmpfs, all
inside the VM, and files reach containers through the Docker API. Docker
Desktop's file sharing settings therefore do not affect a fixture; they matter
only for bind mounts your own test adds. Prefer a named volume or a copy
through the Testcontainers API over a bind mount of a host folder: a shared
folder crosses the host/VM boundary on every file operation. The one bind the module makes, `/dev` for the
[block fault keeper](fault-injection.md#delaying-or-failing-an-osds-device), is
the VM's `/dev`, not the host's.

**Client location.** A native Ceph client connects to every daemon at the
address the cluster maps advertise. A client in a container next to the
cluster works in the default bridge network on every backend. A client in the
test process on macOS or Windows cannot reach bridge addresses, which exist
only inside the VM. It needs host networking, which is a Docker Desktop
setting that is off by default and that the engine API does not expose; the
script cannot read or change it. Enable it by hand as
[Host network](host-network.md#docker-desktop) describes; `Run` then verifies
the path itself. An S3 client is the exception and works from the host in both
modes.

**Isolation.** Docker Desktop's Enhanced Container Isolation and its Settings
Management are administrator controls. Network shaping needs `NET_ADMIN` in a
helper, and the FUSE, NBD and block fault fixtures need privileged helpers
with VM devices; Enhanced Container Isolation may refuse these
([Fault injection](fault-injection.md)), and none of them was run with it
enabled. The script never writes Docker Desktop's settings, so it cannot
weaken a managed setting. It reports when an administrator settings file is
present, because a locked setting cannot be changed in the dashboard either.

## What the script automates

| Setting | WSL 2 backend | macOS, Hyper-V and Linux Desktop | Remote or unrecognized |
| --- | --- | --- | --- |
| CPUs, memory, swap | `apply` writes `[wsl2]` keys of `%UserProfile%\.wslconfig` | manual steps printed | manual, on the engine's machine |
| Free disk | reported with `--probe-image`; raising it is manual | same | same |
| Host networking | manual ([Host network](host-network.md#docker-desktop)) | manual | not applicable |
| File sharing | manual, and not needed by the fixtures | same | not applicable |
| Kernel modules the engine VM ships | `apply-kernel` loads them until the VM restarts; `apply --persist-kernel-modules` lists them in `loadKernelModules` | `apply-kernel` loads them until the VM restarts | commands printed for the engine's operator |
| Kernel parameters | `apply-kernel` raises them until the VM restarts | same | commands printed for the engine's operator |
| Kernel features the engine VM does not ship | reported; cannot be added | same | same |
| Restart of Docker Desktop or WSL | never; the operator does it | never | never |

Docker documents its resource settings as dashboard settings and names the
file that stores them without documenting it as an interface. The `docker
desktop` CLI has no command that reads or sets them. The script therefore
treats Docker Desktop's settings store as private and never writes it. The
`.wslconfig` file is different: Microsoft documents its location, sections and
keys, and Docker Desktop's WSL 2 backend takes its CPU, memory and swap from
it.

## Plan, apply, verify, undo

The command is the first argument; without one the script plans.

| Command | Writes | Exit status |
| --- | --- | --- |
| `plan` (default) | nothing | 0 |
| `verify` | nothing | 0 when every setting is `applied` or `available`, otherwise 1 |
| `apply` | the `[wsl2]` keys that are too low, a state file and a backup next to `.wslconfig` | 0 written or nothing to do, 2 refused, 3 a write failed |
| `restore` | puts the script's own keys back | 0 restored, 2 refused, 3 a write failed |
| `apply-kernel` | loads modules and raises kernel parameters in the engine VM, and records them in a Docker volume on that engine | 0 done or nothing to do, 2 refused, 3 an action failed |
| `restore-kernel` | puts the parameters back, unloads the modules and removes the volume | 0 undone, 1 something had to be kept, 2 refused, 3 failed |

Every run prints the selected engine and backend, the profile, one line per
setting with the requested value, the effective value on the engine and an
outcome, the keys it wrote, whether a restart is pending and numbered manual
steps. `--format json` prints the same report as one JSON object
(`schema_version` 1) for automation. Neither form contains a Docker endpoint,
a socket or file path, a context name other than Docker's built-in ones, a
host name or text copied from an engine error or from your config file, so the
report can be attached to an issue as it is.

| Outcome | Meaning |
| --- | --- |
| `applied` | the engine the tests use has at least the requested value |
| `planned` | `apply` would write the value to `.wslconfig`, or `apply-kernel` would load the module or raise the parameter |
| `pending-restart` | the config has the value and the running engine does not yet |
| `manual` | the value is too low and only the printed manual step changes it |
| `unverifiable` | the script could not observe it on this engine |
| `available`, `unavailable` | a kernel feature is built in or already loaded, or the engine VM does not ship it |
| `failed` | `apply-kernel` tried the change and the engine kernel refused it |

Swap, free disk and the kernel are not in the engine metadata. They stay
`unverifiable` unless you name an image for one-shot containers:

- `--probe-image <image>` reads swap, free disk and the kernel's build options
  from three unprivileged containers (`cat /proc/meminfo`, `df -Pk /`,
  `cat /proc/config.gz`). The image needs `cat` and `df`.
- `--kernel-helper-image <image>` reads the modules and parameters of the
  engine VM from a privileged container in the engine's PID namespace; see
  [Prepare the engine kernel](#prepare-the-engine-kernel).

Either image must already be on the engine, because nothing is pulled. The
containers have no network and are removed when they exit. Without these
flags the script starts no container at all.

### Windows with the WSL 2 backend

Run the script with Python for Windows, as the user who runs Docker Desktop.
From inside a WSL distribution it recognizes the backend and prints the manual
lines, but does not write: the Windows profile that holds the file is not
established there. The Windows listings below show the output format with
made-up engine values; this path was not run on a Windows host
([What was tested](#what-was-tested)).

```text
> python scripts\desktop\desktop_setup.py --profile in-memory-osds
...
Config writes: %UserProfile%\.wslconfig [wsl2]
Scope: %UserProfile%\.wslconfig is global: its [wsl2] values apply to every WSL 2 distribution of this Windows user, not only to Docker Desktop
Settings:
  cpus                   requested 4          effective 16                               applied
  memory                 requested 8.00 GiB   effective 3.83 GiB                         planned
  disk-free              requested 12.00 GiB  effective unknown                          unverifiable
Planned [wsl2] changes (written only by `apply --confirm-wsl-global`): memory=8GB
```

`.wslconfig` is not a Docker Desktop file. Its `[wsl2]` values size the one
VM that all WSL 2 distributions of the user share, so a change also changes
what your other distributions get. `apply` writes only after you confirm that
scope:

```text
> python scripts\desktop\desktop_setup.py apply --profile in-memory-osds --confirm-wsl-global
```

What `apply` does and does not do:

- It raises `memory`, `processors` and `swap` only where the engine has less
  than the profile asks and the file does not already set at least that much.
  A value that is already enough is left alone, and a missing key whose WSL
  default is enough is not added.
- It keeps every other byte: other keys and sections, comments, the spelling
  and spacing of the keys it changes, the encoding and byte order mark (UTF-8
  or BOM-marked UTF-16) and each line's ending. A missing file is created with
  only a `[wsl2]` section.
- Before the first change it copies the file to
  `.wslconfig.ceph-testcontainers.bak`, and it records the lines it wrote in
  `.wslconfig.ceph-testcontainers.state.json`, both next to the config. The
  new content replaces the file in one rename.
- A second `apply` of the same profile writes nothing.
- It refuses, and writes nothing, when the file is not UTF-8 or BOM-marked
  UTF-16, has a line that is neither a section, a `key=value` pair nor a
  comment, has two `[wsl2]` sections or sets a managed key twice, sets a
  managed key to a value it cannot read, is a symbolic link or read-only, or
  changed while the script ran. It then prints the lines to set by hand.
- It does not restart anything.

The new values take effect when the WSL 2 VM restarts. Until then the report
shows `pending-restart` and `Restart pending: yes`:

1. Quit Docker Desktop.
2. Run `wsl --shutdown`. It stops every running WSL 2 distribution, not only
   Docker Desktop's.
3. Start Docker Desktop.
4. Run `verify` with the same profile. It exits 0 once the engine the tests
   use reports the values.

```text
> python scripts\desktop\desktop_setup.py verify --profile in-memory-osds --probe-image busybox:1.37.0
```

To undo, run `restore`. It puts back the previous line of each key the script
wrote, or removes the key and the section it added, and deletes its state and
backup files; a config file the script created and that holds nothing else is
removed. Lines you added or changed afterwards stay. If you edited a key the
script owns after `apply`, `restore` refuses rather than overwrite your edit;
set that line by hand and delete the two script files. The restored values
again need the restart above.

To set the keys by hand instead, add them to the one `[wsl2]` section of
`%UserProfile%\.wslconfig`, or use the WSL Settings app that Microsoft
recommends for these values:

```ini
[wsl2]
memory=8GB
processors=4
swap=2GB
```

### macOS and other Desktop backends

Docker Desktop on macOS, on Windows with Hyper-V and on Linux has no supported
write interface for these settings, so `apply` writes nothing there. The script
prints the steps and verifies the result on the engine:

1. Run `plan` with your profile. Each setting below the profile is `manual`.
2. In Docker Desktop open **Settings > Resources > Advanced** and raise
   **CPU limit**, **Memory limit**, **Swap** and **Disk usage limit** to at
   least the printed values.
3. Apply the change. Docker Desktop restarts its VM, which stops every
   running container; images and volumes stay.
4. Run `verify`. The script reads the values back from the engine, not from a
   settings file, so `applied` means the VM really has them.

There is nothing to undo in a file: set the previous values in the same
dialog. If an administrator settings file is present the script says so; a
setting it locks has to be changed by the administrator.

## Prepare the engine kernel

Some scenarios need a piece of the engine kernel, not more of a resource. The
Ceph software itself is in the images; only the kernel side lives in the
engine VM, and that is the part this step prepares.

| Path | In the image (user space) | In the engine kernel | Feature names |
| --- | --- | --- | --- |
| RBD through librbd (`rbd` CLI, the fixtures' RBD operations) | librbd, in the client role image and the aggregate image | nothing | none |
| RBD as a block device through `rbd-nbd` | librbd and `rbd-nbd`; the official `quay.io/ceph/ceph` image has `rbd-nbd`, the role images do not | the NBD driver and `/dev/nbd*` | `nbd` |
| RBD through the kernel client (krbd, `rbd map`) | the `rbd` CLI only starts the mapping | `rbd` and `libceph` | `krbd`, `libceph` |
| CephFS through libcephfs | libcephfs, in the images | nothing | none |
| CephFS through `ceph-fuse` | `ceph-fuse`, in the official image | FUSE and `/dev/fuse` | `fuse` |
| CephFS through the kernel client (`mount -t ceph`) | nothing beyond `mount` | `ceph` and `libceph` | `cephfs-kernel`, `libceph` |
| Network delay, loss and rate faults | `tc` in the netem helper image | the netem qdisc, and with peers the prio qdisc and the u32 classifier | `netem`, `prio`, `u32` |
| Packet capture | `tcpdump` in the capture helper image | packet sockets | `packet` |
| Block faults on an OSD device | `dmsetup` and `losetup` in the block fault helper image (`blockio.HelperImage`) | loop devices and device-mapper with its `linear`, `error` and `delay` targets | `loop`, `dm`, `dm-delay` |
| OSD on a LUKS2 store | `cryptsetup` in the client role image | loop devices and the device-mapper `crypt` target | `loop`, `dm`, `dm-crypt` |

No fixture maps an image through krbd, and only the kernel subtest of the
`cephfs_fscrypt` experiment uses the kernel CephFS client; the features exist
so that an engine that ships those modules can be prepared for your own tests.

The profiles that carry these features, all with the resources of
`single-cluster`:

| Profile | Kernel features | Used by |
| --- | --- | --- |
| `network-faults` | `netem`, `prio`, `u32` | `fault.ApplyNetworkProfile` and the network fault tests |
| `block-fault-delay` | `loop`, `dm`, `dm-delay` | `fault/blockio`, `TestBlockDeviceFault` with its delay window |
| `encrypted-osd` | `loop`, `dm`, `dm-crypt` | the `osd_encrypted_store` batch |
| `cephfs-fuse` | `fuse` | the `cephfs_fuse` batch |
| `rbd-nbd` | `nbd` | `TestDesktopRBDOverNBD` |
| `kernel-clients` | `libceph`, `krbd`, `cephfs-kernel` | the kernel subtest of the `cephfs_fscrypt` experiment; krbd for your own tests |
| `ci-parity` | `netem`, `prio`, `u32`, `packet`, `loop`, `dm` | an ordinary CI batch |

No shipped profile sets a kernel parameter: nothing in the fixtures, tests or
documentation of this repository depends on a global sysctl. The routers of
the zone boundary fixture set `net.ipv4.ip_forward` in their own network
namespace through Docker, which needs no host change. Add a parameter your
workload needs with `--kernel-parameter name=value` or in your profile file;
the value is a least value, like the resources.

### Inspect

```sh
python3 scripts/desktop/desktop_setup.py --profile kernel-clients --kernel-feature nbd \
  --kernel-feature dm-delay --kernel-feature fuse --kernel-parameter fs.aio-max-nr=1048576 \
  --kernel-helper-image busybox:1.37.0
```

```text
  kernel:cephfs-kernel   requested present    effective not built, no module file in the engine VM unavailable
  kernel:dm-delay        requested present    effective not built, no module file in the engine VM unavailable
  kernel:fuse            requested present    effective built in                         available (persists)
  kernel:krbd            requested present    effective not built, no module file in the engine VM unavailable
  kernel:libceph         requested present    effective not built, no module file in the engine VM unavailable
  kernel:nbd             requested present    effective built in                         available (persists)
  sysctl:fs.aio-max-nr   requested 1048576    effective 65536 (below the requested value) planned (until the engine VM restarts)
```

The helper image must already be on the engine and contain `sh`, `nsenter`,
`zcat`, `grep`, `cut` and `tr`; `busybox` is enough. The script runs it
privileged, in the engine's PID namespace and without a network, and enters
the namespaces of the engine VM's PID 1 to read `/proc/config.gz`,
`/proc/modules`, the VM's `modules.dep` and the requested `/proc/sys` values.
Inspection changes nothing.

For each feature it reports one of:

| Report | Meaning | What `apply-kernel` does |
| --- | --- | --- |
| `available`, built in | compiled into the engine kernel; persists | nothing |
| `available`, module already loaded | loaded by the VM or by someone else | nothing |
| `planned` | the VM has the module file and it is not loaded | `modprobe` in the VM |
| `applied` | this script loaded it; lost when the VM restarts | nothing |
| `unavailable` | not built and no module file in the VM, or the kernel refuses module loading | nothing; see below |
| `failed` | `modprobe` was tried and failed | reports it, exit status 3 |

### Apply and undo

```sh
python3 scripts/desktop/desktop_setup.py apply-kernel --profile <profile> \
  --kernel-helper-image busybox:1.37.0 --confirm-engine-kernel
```

`apply-kernel` loads each `planned` module with the VM's own `modprobe` and
writes each `planned` parameter to `/proc/sys` in the VM's namespaces. It never
lowers a parameter, loads nothing that is already there, and does nothing on a
second run. A later run with more features or parameters adds to what an
earlier run did, and a parameter write counts only when the value reads back. It needs `--confirm-engine-kernel`, because the change is not
scoped to your tests: it affects every container on the engine, and on the
WSL 2 backend the one kernel that all WSL 2 distributions share.

The script changes the kernel only of a VM that Docker Desktop manages on this
machine (the macOS, WSL 2, Hyper-V and Linux Desktop backends). On a plain
Linux engine the kernel is the host's own, and a remote engine's belongs to
another machine and possibly to other users; for those it prints the
`sudo modprobe` and `sudo sysctl -w` commands for the operator and changes
nothing.

What it did is recorded in a Docker volume on the same engine,
`ceph-testcontainers-desktop-setup-state`, together with the VM's boot
identity. `restore-kernel` reads it, writes back the previous value of each
parameter that still holds the value the script wrote, unloads each module the
script loaded that is no longer in use, and removes the volume. It keeps, and
reports, a parameter that was changed afterwards, a module that is in use and
a module on a kernel built without module unloading; its exit status is then
1. If the VM restarted in between, the changes are already gone and the
record is only removed. Both commands stop before changing anything when the
engine's volumes cannot be listed or inspected, or when a volume of that name
exists without the script's label: a failed lookup is not taken for a missing
record.

**These changes do not survive a restart of the engine VM**: quitting Docker
Desktop, `wsl --shutdown`, a Docker Desktop update or a reboot undo them. Run
`apply-kernel` again afterwards. A supported persistent route exists for one
case only:

- WSL 2 backend, modules: `[wsl2] loadKernelModules` in
  `%UserProfile%\.wslconfig` lists modules that WSL loads when its VM starts.
  `apply --confirm-wsl-global --persist-kernel-modules --kernel-helper-image <image>`
  adds the profile's loadable modules to that list, keeps the entries it
  already has, and records and restores the line like the resource keys.
- Kernel parameters, and modules on every other backend: none. Docker
  Desktop's VM image and boot configuration are private, so the script does
  not edit them.

### What the engine VM does not ship

A feature that is neither built into the engine kernel nor present as a
module file in the VM is `unavailable`. No profile, VM size, restart or Docker
Desktop setting adds it, and the script does not build modules, download them
or boot another kernel. On the Docker Desktop kernel listed in
[What was tested](#what-was-tested) that is the case for `dm-delay`, `krbd`,
`libceph` and `cephfs-kernel`: that VM ships only Docker's own modules, so
`apply-kernel` has no module to load there, and every other feature in the
table above is built in. Preparing that kernel therefore makes no additional
fixture test runnable; the `rbd-nbd` path already works because NBD is built
in, and the krbd and kernel CephFS paths stay out of reach.

To check a build option by hand:

```sh
docker run --rm --network none busybox:1.37.0 sh -c 'zcat /proc/config.gz | grep -E "CONFIG_(DM_DELAY|BLK_DEV_NBD|BLK_DEV_RBD|CEPH_FS|FUSE_FS|NET_SCH_NETEM)[= ]"'
```

`=y` is built in, `=m` is a module that the VM may or may not ship, and a
missing line is not built. The script checks the kernel, not the helper
image, the privileges or the fixture path: a fixture's own preflight (for
example `dmsetup targets` in the block fault keeper) decides at run time. A
pre-run capability report in the Go API is tracked in
[issue 911](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/911).

For a scenario this engine's kernel cannot run, use a different engine
instead of changing this one:

- a Linux VM you manage (any hypervisor or cloud instance) with Docker Engine
  and a distribution kernel that has the module, selected through
  `DOCKER_HOST` or a Docker context;
- a remote Linux engine, such as a CI runner.

The script then reports that engine as `remote` and verifies it the same way.
With a remote engine the test process is not on the Docker host, so
[host mode](host-network.md) needs the engine's address, not `127.0.0.1`.

## What was tested

| Path | Status |
| --- | --- |
| macOS, Docker Desktop 4.99.0, engine 29.9.3, linux/arm64, kernel 7.0.14-linuxkit | `plan`, `verify` and the probe were run against the real engine: backend detection, CPU and memory comparison, free disk and the refusal of `apply` and `restore`. The manual steps were not clicked through, and no resource value was changed on it. |
| The kernel step on that engine | Run for real. Inspection found `netem`, `prio`, `u32`, `packet`, `loop`, `dm`, `dm-crypt`, `fuse` and `nbd` built in, `dm-delay`, `libceph`, `krbd` and `cephfs-kernel` not built, an empty `modules.dep`, and a kernel built without module unloading. `apply-kernel` raised `fs.aio-max-nr` from 65536 to 1048576, an independent read in the VM confirmed it, a second run changed nothing, and `restore-kernel` put 65536 back and removed the state volume. No module was loaded, because that VM has none to load; module loading and unloading are covered by unit tests only. |
| Windows, WSL 2 backend | **Not run on a Windows host.** Backend detection, the `.wslconfig` edit, backup, idempotent second apply, restore, every refusal and the restart-pending report are covered only by unit tests with synthetic engine facts and files in temporary directories. Whether Docker Desktop's WSL 2 engine reports the new limits after `wsl --shutdown`, whether the helper can enter the WSL 2 VM and load its modules, and whether `loadKernelModules` loads them at start were not observed. |
| Windows Hyper-V backend, Docker Desktop on Linux, Windows containers | Detection and manual steps covered by unit tests only. |
| Remote, unrecognized and unreachable engines | Selection order and refusal covered by unit tests. An unreachable `tc.host` endpoint was also selected ahead of a local Desktop context, and refused, in a real run. |
| Example profiles | Not qualified on Docker Desktop at their exact limits; see [Example workloads](#example-workloads). |

The unit tests are `scripts/desktop/test_*.py`:

```sh
python3 -m unittest discover -s scripts/desktop -p 'test_*.py'
```
