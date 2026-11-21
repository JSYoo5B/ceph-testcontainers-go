# Docker Desktop setup for Ceph fixtures

## Introduction

On macOS and Windows the Go test and the Ceph containers run on different
kernels: the test is a host process, and the containers run in a Linux VM that
Docker Desktop manages. The size of that VM, its kernel and the way the host
reaches it decide which fixture scenarios work. This guide walks from finding
the engine your tests use, through sizing it for a workload, to applying,
verifying and undoing the settings.

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
| `block-fault-delay` | 4 | 4 GiB | 12 GiB | `single-cluster` plus the device-mapper `delay` target | see [Kernel features are not limits](#kernel-features-are-not-limits) |

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
`swap_gib`, `disk_free_gib` (0 requests nothing) and an optional
`kernel_features` list.

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
| Kernel features | reported with `--probe-image`; cannot be set | same | same |
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
| `planned` | `apply` would write the value to `.wslconfig` |
| `pending-restart` | the config has the value and the running engine does not yet |
| `manual` | the value is too low and only the printed manual step changes it |
| `unverifiable` | the script could not observe it on this engine |
| `available`, `unavailable` | a kernel feature is built into the engine kernel, or is not |

Swap, free disk and kernel features are not in the engine metadata. They stay
`unverifiable` unless you pass `--probe-image <image>`, which reads them from
three one-shot containers (`cat /proc/meminfo`, `df -Pk /`,
`cat /proc/config.gz`). The image must already be on the engine, because
nothing is pulled, and must contain `cat` and `df`; the containers have no
network and are removed when they exit. Without the flag the script starts no
container at all.

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
  cpus             requested 4          effective 16                               applied
  memory           requested 8.00 GiB   effective 3.83 GiB                         planned
  disk-free        requested 12.00 GiB  effective unknown                          unverifiable
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

## Kernel features are not limits

Some scenarios need a feature of the engine kernel, not more of a resource.
The delay window of the block fault test needs the device-mapper `delay`
target, and the Docker Desktop kernel listed in
[Testing](../contributing/testing.md#prerequisites) does not build it. No
profile, VM size, restart or Docker Desktop setting adds it, and the script
reports it apart from the limits:

```text
$ python3 scripts/desktop/desktop_setup.py verify --profile block-fault-delay --probe-image busybox:1.37.0
...
  cpus             requested 4          effective 8                                applied
  memory           requested 4.00 GiB   effective 7.75 GiB                         applied
  disk-free        requested 12.00 GiB  effective 44.93 GiB                        applied
  kernel:dm-delay  requested built in   effective not set                          unavailable
```

The check reads `/proc/config.gz` of the engine kernel, which any container
can do. By hand:

```sh
docker run --rm --network none busybox:1.37.0 sh -c 'zcat /proc/config.gz | grep -E "CONFIG_(DM_DELAY|BLK_DEV_NBD|FUSE_FS|NET_SCH_NETEM)[= ]"'
```

`=y` is built in. `=m` is a module that may or may not be loaded, which the
script reports as `unverifiable`; a fixture's own preflight (for example
`dmsetup targets` in the block fault keeper) is what decides at run time. The
script knows the features `dm-delay`, `dm-flakey`, `dm-crypt`, `fuse`, `nbd`
and `netem`. It is not a fixture capability report: it checks a kernel build
option, not the helper image, the privileges or the fixture path. A pre-run
capability report in the Go API is tracked in
[issue 911](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/911).

For a scenario the current backend's kernel cannot run, use a different
engine instead of changing this one:

- a Linux VM you manage (any hypervisor or cloud instance) with Docker Engine
  and a distribution kernel that has the target, selected through
  `DOCKER_HOST` or a Docker context;
- a remote Linux engine, such as a CI runner.

The script then reports that engine as `remote` and verifies it the same way.
With a remote engine the test process is not on the Docker host, so
[host mode](host-network.md) needs the engine's address, not `127.0.0.1`.
Building a module for the Docker Desktop kernel or booting a custom kernel is
not part of this setup and is not supported by it.

## What was tested

| Path | Status |
| --- | --- |
| macOS, Docker Desktop 4.99.0, engine 29.9.3, linux/arm64, kernel 7.0.14-linuxkit | `plan`, `verify` and the probe were run against the real engine: backend detection, CPU and memory comparison, free disk, the absent `dm-delay` option and the refusal of `apply` and `restore`. The manual steps were not clicked through, and no resource value was changed on it. |
| Windows, WSL 2 backend | **Not run on a Windows host.** Backend detection, the `.wslconfig` edit, backup, idempotent second apply, restore, every refusal and the restart-pending report are covered only by unit tests with synthetic engine facts and files in temporary directories. Whether Docker Desktop's WSL 2 engine reports the new limits after `wsl --shutdown` was not observed. |
| Windows Hyper-V backend, Docker Desktop on Linux, Windows containers | Detection and manual steps covered by unit tests only. |
| Remote, unrecognized and unreachable engines | Selection order and refusal covered by unit tests. An unreachable `tc.host` endpoint was also selected ahead of a local Desktop context, and refused, in a real run. |
| Example profiles | Not qualified on Docker Desktop at their exact limits; see [Example workloads](#example-workloads). |

The unit tests are `scripts/desktop/test_*.py`:

```sh
python3 -m unittest discover -s scripts/desktop -p 'test_*.py'
```
