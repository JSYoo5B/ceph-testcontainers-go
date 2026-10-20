# Feasibility: a Crimson OSD fixture with CyanStore

This report decides whether ceph-testcontainers-go can offer an opt-in OSD
that runs Crimson (`crimson-osd`) with its CyanStore backend, next to the
existing classic OSD fixtures. It records where Crimson builds are
published, what was actually run, what failed and why, and what an API and
a companion image would need. This feasibility study commits no public API.

Two kinds of statement appear below, and they are kept apart:

- **Runtime evidence**: observed in a bounded manual run on a native Linux
  Docker Engine host (x86_64, cgroup v2). Every container, volume,
  network and image of the run was removed afterwards.
- **Source or registry evidence**: read from Ceph's source tree, Ceph's
  documentation, the Shaman/Chacra build APIs or the quay.ceph.io tag API
  on 2026-10-20, not observed in a running daemon.

## Summary

| Source | Crimson OSD binary | Architectures | Matches a release | Basis |
| --- | --- | --- | --- | --- |
| `quay.io/ceph/ceph:v20.2.4`, `v19.2.5` and the role images built from them | **Absent** | amd64 inspected | Yes | Runtime evidence (issue comment of 2026-10-19) |
| Tentacle and Squid branch CI builds (`ceph-ci/ceph:tentacle`, `:squid`, Chacra `default` flavor RPMs) | **Absent**; off by default in `ceph.spec.in`, by a different switch per branch: Tentacle [`%bcond_with crimson`](https://github.com/ceph/ceph/blob/9465ad89a4e4139d1c43a86ab98eafb1a4d67554/ceph.spec.in#L94), Squid [`%bcond_with seastar`](https://github.com/ceph/ceph/blob/fe9eba0b6ebd09ec811354c8b80308acb19c600c/ceph.spec.in#L94) | x86_64, arm64 | Branch heads (20.2.4-626, 19.2.7-28), not releases | Registry and source evidence |
| Shaman `crimson-debug` / `crimson-release` flavors and `<sha1>-crimson-release` tags | Present, but the newest is from 2025-09-16 (`main`, 20.3.0-3037) | x86_64 only | No | Registry evidence |
| `main` branch CI images and packages (21.3.0 "umbrella" dev) | **Present**: package `ceph-osd-crimson`, binary `/usr/bin/ceph-osd-crimson`, next to `ceph-osd-classic` | x86_64 (run); aarch64 RPMs and an arm64 image are published but were not run | No | Runtime evidence (x86_64), registry evidence (arm64) |

| Combination on the `main` image | Outcome | Basis |
| --- | --- | --- |
| Crimson + CyanStore | **Fails.** mkfs succeeds, the OSD boots and is marked `up`, then dies with `SIGSEGV` on the first client operation (the MGR's write to `.mgr`). Reproduced on 7 fresh clusters; no privilege, memory or reactor setting changes it. | Runtime evidence; cause located in source |
| Crimson + MemStore (alien, non-native) | Works unprivileged. Binary write/read/delete with exact bytes, xattr, OMAP, pool snapshot read, watch/notify | Runtime evidence |
| Crimson + SeaStore | mkfs aborts (`SIGABRT`) with a file-backed device; not pursued | Runtime evidence, not diagnosed |

Recommendation: **no-go for a CyanStore fixture now, and no-go for any
Crimson fixture built on release images.** Crimson ships only in
development builds of an unreleased Ceph, CyanStore crashes in the current
one, and the builds rotate daily. See [Recommendation](#recommendation) for
the conditions under which the question should be reopened.

## Where Crimson builds are published

### Release images

The official Tentacle 20.2.4 and Squid 19.2.5 images contain no Crimson
executable (see the issue's inspection of 2026-10-19). This is a build
choice, not an omission in the image: both release branches build Crimson
only when a `ceph.spec.in` build condition that is off by default is turned
on, and the switch differs per branch. Tentacle uses
[`%bcond_with crimson`](https://github.com/ceph/ceph/blob/9465ad89a4e4139d1c43a86ab98eafb1a4d67554/ceph.spec.in#L94) and passes `-DWITH_CRIMSON:BOOL=ON`
([L1518](https://github.com/ceph/ceph/blob/9465ad89a4e4139d1c43a86ab98eafb1a4d67554/ceph.spec.in#L1518)) when it is on. Squid uses
[`%bcond_with seastar`](https://github.com/ceph/ceph/blob/fe9eba0b6ebd09ec811354c8b80308acb19c600c/ceph.spec.in#L94): its `crimson-osd` subpackage is gated by
`%if 0%{with seastar}` ([L904-L905](https://github.com/ceph/ceph/blob/fe9eba0b6ebd09ec811354c8b80308acb19c600c/ceph.spec.in#L904-L905)) and the build passes
`-DWITH_SEASTAR:BOOL=ON` ([L1473-L1475](https://github.com/ceph/ceph/blob/fe9eba0b6ebd09ec811354c8b80308acb19c600c/ceph.spec.in#L1473-L1475)). Either way the
default build, and so the release RPMs, produce no Crimson OSD package.

The current Tentacle and Squid CI builds confirm this. Shaman's newest
`centos/9` `default` repositories for the two branches
(`tentacle` at `9465ad89a4e4139d1c43a86ab98eafb1a4d67554`, 20.2.4-626, and
`squid` at `fe9eba0b6ebd09ec811354c8b80308acb19c600c`, 19.2.7-28) list
`ceph-osd` and `ceph-osd-debuginfo` only. Shaman has no Crimson flavor build
for either branch.

### Dedicated Crimson flavors

The Tentacle documentation tells users to deploy
`quay.ceph.io/ceph-ci/ceph:<sha1>-crimson-release`. Shaman's API returns one
`crimson-release` build and five `crimson-debug` builds, all from
2025-09-16 and 2025-09-17 and all 20.3.0 development builds of `main` or wip
branches, x86_64 only. The matching quay.ceph.io tags still exist, for
example
`5917c0610636d57894e7cb5f41611c528f3115f9-crimson-release`
(`sha256:07d22c57095ebfc99c9b07b2aebc99ed96fe43860af4d7bd34472084c452035f`,
2025-09-16). No newer build of these flavors exists; the older `crimson`
flavor's last build is from 2024-03. A tag search for `crimson` on
quay.ceph.io returns 184 active tags, but apart from these 2025-09 builds
they are wip branches whose names contain the word (for example
`wip-perezjos-crimson-only-...`), not Crimson flavors.

### `main` builds

On `main`, Crimson became part of the default build: `ceph.spec.in` has
`%bcond_without crimson`, splits the daemon into `ceph-osd-classic` and
`ceph-osd-crimson`, and `ceph-osd` requires both. `update-alternatives`
points `/usr/bin/ceph-osd` at the classic binary (priority 100 against 50),
so the default `ceph-osd` stays classic. The current documentation on `main`
says accordingly that CI containers carry both binaries.

Every `main` build therefore publishes Crimson. For the build used here,
`main` at `4927956a73bcf6e09eb861bcb00ed2f426a50a6e` (21.3.0-4511):

| Artifact | Identity |
| --- | --- |
| Image `quay.ceph.io/ceph-ci/ceph:4927956a…-centos-stream9` (also `main-centos-stream9` until the next day's build) | `sha256:0e0a7bd9cd96367cedfdc69faec115e53a703a21df7e53f4a81dd7cf238f3af2`, config `sha256:d9c8ecf78fc26d55e55889e3ddf06d3f42f32b409c01e9606eb3f140c5e0244a`, linux/amd64, labels `CEPH_REF=main`, `CEPH_SHA1=4927956a…`, `OSD_FLAVOR=default`, base CentOS Stream 9 |
| Same commit, Rocky Linux 10 x86_64 image (tag `4927956a…`) | `sha256:930c934eda9a2585440d799820c352248d7c6cfa4d994d7f906e44729d372c17` |
| Same commit, CentOS Stream 9 arm64 image | `sha256:63accaf1881a19dc4c3c3901219bff5d0c25c05e503d925eb7b6cf444e1d44fa` (not run) |
| RPMs | `https://2.chacra.ceph.com/r/ceph/main/4927956a…/centos/9/flavors/default/` has `ceph-osd-crimson-21.3.0-4511.g4927956a.el9` for `x86_64` and `aarch64`; Rocky 10 and Ubuntu jammy/noble/resolute (`ceph-osd-crimson_…_amd64.deb`) repositories have it too |

Inside the image, `ceph-osd-crimson --version` prints
`ceph version 21.3.0-4511-g4927956a (…) umbrella (dev - RelWithDebInfo) (crimson)`.
`ldd` resolves all 29 shared libraries; none is missing. The image adds a
1.1 GB compressed layer to CentOS Stream 9, about twice the compressed size
of the Tentacle CI image (545 MB).

### Retention

- The moving tags (`main`, `main-centos-stream9`, `tentacle`, …) are
  replaced by every build; `main-centos-stream9` moved twice on
  2026-10-19 alone. Its tag history on quay.ceph.io reaches back only to
  2026-10-05.
- The `<sha1>`-named tags are what can be pinned. quay.ceph.io documents no
  retention policy that this study could find; tags from 2025-09 are still
  active, so they are not removed after a fixed short period, but nothing
  promises that they stay.
- Package retention is unverified. Two separate mechanisms were found, and
  neither establishes how long a Chacra commit repository of `main`,
  `tentacle` or `squid` stays available:
  - Chacra itself documents a configurable purge age, not the production
    deployment's setting
    ([chacra README](https://github.com/ceph/chacra/blob/main/README.rst)).
  - Ceph's
    [purge policy](https://github.com/ceph/ceph-build/blob/main/scripts/purge-policy.yaml)
    is for Pulp: `default.days: 14` is the age cutoff that `pulp-cleanup`
    applies to refs that are not listed, while a listed ref's
    `keep_minimum` (10 for `main`, `tentacle` and `squid`) is the number of
    repository versions that
    [`pulp_upload.sh`](https://github.com/ceph/ceph-build/blob/main/scripts/pulp_upload.sh)
    retains on upload (`--retain-repo-versions`). This is not a 14-day
    expiry of those branches' commit repositories.

  A consumer that needs a given commit's packages later should mirror the
  immutable artifacts rather than rely on them staying published.

Download speed from quay.ceph.io was a practical obstacle: a plain
`docker pull` of the 1.1 GB layer ran at about 340 KB/s and timed out
after 15 minutes. The image was fetched instead with 16 parallel HTTP range
requests, each blob checked against its sha256 digest, and loaded with
`docker load`; the loaded image ID equals the pinned manifest digest.

## What was run

Everything used the one pinned `main` image above; the official release
images cannot host a Crimson OSD, and a classic Tentacle or Squid MON would
mix releases with a 21.3 development OSD. No package was installed into
any image, and the host was not reconfigured.

1. A MON and an MGR in one container on a user-defined bridge network,
   bootstrapped with `ceph-mon --mkfs`, one MON on msgr2 only, cephx, pool
   size 1, PG autoscaling off, and the three settings that Crimson needs
   before any OSD exists:
   `enable_experimental_unrecoverable_data_corrupting_features = crimson`,
   `osd_pool_default_crimson = true` and
   `ceph osd set-allow-crimson --yes-i-really-mean-it`, plus
   `ceph config set osd crimson_cpu_num 1`. The MON was ready in about
   1.2 s.
2. One OSD container from the same image, with the default Docker settings
   (no added capability, default seccomp and AppArmor profiles, no CPU or
   memory limit): `ceph osd new`, then
   `ceph-osd-crimson --mkfs --osd-objectstore <store> --key … --osd-uuid …`
   and `ceph-osd-crimson -f`. The OSD was `up` about 4.2 s after its
   container started.
3. A separate client container from the same image running the `rados`
   command line client (librados) for the I/O checks.

### Crimson with CyanStore

mkfs succeeds: the data directory gets `type = cyanstore`, `collections0`,
`meta0`, `fsid`, `ceph_fsid`, `whoami` and the keyring. The OSD boots and
the MON marks it `up` with msgr2 addresses on ports 6800 and 6801 of the
container's bridge address. As soon as the MGR creates `.mgr` and
sends its first write, the OSD aborts:

```text
Aborting Got SIGSEGV on shard 0 - Stopping all shards
Backtrace:
  0# crimson::osd::OperationThrottler::acquire_throttle(crimson::osd::scheduler::params_t)
  ...
  2# crimson::osd::ClientRequest::process_op(...)
```

The container exits with code 1, `.mgr`'s PG stays `unknown`, and the OSD
stays `up` in the OSDMap, because one OSD has no peer to report it down. A
pool created afterwards never becomes active. The same crash happened with
each of these changes, every time on a fresh cluster:

| OSD container / daemon setting | Result |
| --- | --- |
| defaults | `SIGSEGV` on first client op |
| `--memory 2G` (Seastar memory) | same |
| `--reactor-backend epoll` (default is `linux-aio` here) | same |
| `--cap-add SYS_NICE` | same (`mbind` warning gone) |
| `--security-opt seccomp=unconfined` | same |

The cause is visible in the source of the same commit. `OSD::start` creates
the I/O scheduler only when the store reports a device class:
`_add_device_class()` sets `is_rotational` from
`store.get_default_device_class()`, and `initialize_scheduler` runs only if
`is_rotational` has a value. `CyanStore::get_default_device_class()` returns
an empty string, so the start path takes its fallback branch (a warning
"no device class set, defaulting is_rotational to false"), sets the flag,
and never creates the scheduler.
The first `acquire_throttle` then calls `scheduler->enqueue` on a null
scheduler. The empty-class fallback came with
[ceph/ceph#69331](https://github.com/ceph/ceph/pull/69331) ("fix CyanStore
boot with crimson OSD", July 2026), which made CyanStore boot but did not
initialize the scheduler on that path; `main` still has the same code on
2026-10-20. No configuration option reaches the missing call, so a fixture
cannot work around it. It needs a Ceph fix and a new build.

Starting a new OSD container on the crashed OSD's data volume did not
recover the cluster either. The OSD stayed up and the PGs of a pool that
the MON still listed as `creating` became active+clean, but `.mgr`'s PG
stayed `unknown` and the MGR's write was reported as a slow op. After one
more forced restart (`docker rm -f`), the OSD loaded a single PG
(`num_pgs: 1` in `ceph tell osd.0 status`) and the pool's PGs went stale.

Because of this, nothing about CyanStore's data path, Stop/Start or
abrupt-exit behaviour could be observed. The source reading in the issue
(CyanStore saves its collections on a clean `umount` and loads them on
`mount`) remains unverified at runtime.

### Crimson with MemStore, for comparison

To separate the CyanStore defect from the image, the bootstrap and the
container settings, the same procedure ran with `--osd-objectstore
memstore`, which Crimson runs through its alien thread pool. With the
default, unprivileged container settings:

| Check | Result |
| --- | --- |
| `.mgr` and an 8-PG pool active+clean | after 6 s |
| `rados put` / `get` of 1,048,583 random bytes | exact bytes back (sha256 `2b8ff636…`) |
| `stat`, `setxattr`/`getxattr`, `setomapval`/`getomapval`/`listomapkeys` | OK |
| Pool snapshot: `mksnap`, overwrite, read at the snapshot | snapshot read returns the original bytes exactly |
| `watch` / `listwatchers` / `notify` | watcher listed, notify acknowledged by the watcher |
| `rm`, then `stat` | object absent |
| OSD metadata | `osd_type: crimson`, `osd_objectstore: memstore`, version string ending in `(crimson)` |
| `ceph tell osd.0 status` | `cluster_fsid` and `osd_fsid` match the cluster and `ceph osd new` |
| Memory after the run (`docker stats`) | OSD 156 MiB, MON+MGR 166 MiB |
| `docker stop` (SIGTERM), new container on the same data volume | stop took 2 s; PGs active+clean again; object read back with the same digest |
| `docker kill` after a new write, new container | PGs active+clean, the object written before the last clean stop is there, the object written after it is gone (`ENOENT`) |

So on this build the image, the bootstrap, the three Crimson settings and
an unprivileged container are enough for a Crimson OSD to serve native
client I/O. The kill result also shows a trap for any in-memory backend: the
cluster reports `active+clean` after silently losing every write since the
last clean stop.

This is not a CyanStore result and is not offered as a substitute fixture:
alien MemStore is a classic store run in threads outside Seastar, and the
existing `OSDMemStore` classic fixture already covers the same store.

### Crimson with SeaStore

With a file-backed device (`--seastore-device-size 4G`), mkfs aborted with
`SIGABRT`. SeaStore is outside this issue's scope and the abort was not
diagnosed.

## Resources, privileges and identity

Observed on the `main` image, x86_64:

- **Privileges**: none beyond Docker's defaults. Seastar warns
  `unable to mbind shard memory` (needs `CAP_SYS_NICE`) and falls back from
  the `perf_event` stall detector to a POSIX timer; both are performance
  warnings only.
- **CPU**: `crimson_cpu_num` has no default and must be set before the OSD
  starts, otherwise the early configuration step refuses to start. With
  `crimson_cpu_num = 1` the OSD derives `--smp 1 --thread-affinity 0`; no
  CPU pinning or cpuset is needed. `crimson_cpu_set` would add `--cpuset`
  and pinning, which a shared CI runner should avoid.
- **Reactor backend**: the documentation names io_uring as the default, but
  Seastar chose `linux-aio` in a default Docker container; io_uring was not
  tried. `epoll` also starts. The host's `fs.aio-max-nr` was 65536 and was
  not changed.
- **Memory**: without `--memory`, Seastar sizes itself from the host's
  memory (`mem_total_kb` in the metadata is the host's, not a container
  limit). A fixture should pass `--memory` or set `crimson_memory`
  explicitly instead of relying on that.
- **Startup**: the OSD process starts twice, once to read its early
  configuration from the MON in a child process and once for real.
  From the OSD container's start (including `ceph osd new` and mkfs) to
  `up` took about 4.2 s. This is one observation, not a comparison with the
  classic fixtures.
- **Identity**: `ceph osd metadata` reports `osd_type: crimson`; it reports
  `osd_objectstore` for MemStore but **not** for CyanStore, so a fixture
  cannot verify the backend from metadata alone and would need the data
  directory's `type` file. `ceph tell osd.N status` reports `osd_fsid` and
  `cluster_fsid` as on classic OSDs.
- **Mon admission**: without `allow_crimson` the MON refuses the Crimson OSD
  boot (documented); pools need the `crimson` flag, which
  `osd_pool_default_crimson` sets on new pools. Crimson OSDs do not
  instantiate PGs of pools without the flag, so classic and Crimson OSDs
  cannot share pools.

## What was not tested

- Any CyanStore data path, restart or abrupt-exit behaviour (blocked by the
  crash).
- arm64 and Docker Desktop. The arm64 image and aarch64 RPMs are published;
  nothing was run on them.
- The Rocky Linux 10 and `-debug` image variants, and any other `main`
  commit.
- The module's own fixture code: the run was manual and used a single
  MON/MGR container rather than the module's role images.
- RBD, CephFS, erasure coding, mixed classic/Crimson OSDs and fault
  injection.

## API alternatives

These remain design options; none is proposed for implementation now.

1. **OSD implementation as its own option.** A cluster option such as
   `WithOSDImplementation(OSDCrimson)` kept separate from
   `WithOSDObjectStore`, with admission that rejects unsupported pairs
   (classic + CyanStore, Crimson + `WithOSDInMemoryStorage`, Crimson with
   fault injection) and rejects images whose probe finds no Crimson binary.
   The cluster-wide choice fits Crimson's pool flag: Crimson needs
   `osd_pool_default_crimson` and `allow_crimson` set on the MON before the
   first OSD, so a per-OSD choice would also have to manage pool flags. The
   fixture would set `crimson_cpu_num` and the memory size itself, write the
   choice into the readiness check, and verify the backend from the data
   directory's `type` file, since CyanStore metadata lacks
   `osd_objectstore`.
2. **Capability detection, not version gates.** Probe the selected image
   for `ceph-osd-crimson` (or `crimson-osd`) and run
   `--version | grep '(crimson)'`, following the project's rule of
   detecting tools rather than releases. Release images will fail the probe
   until a release ships Crimson.
3. **A companion extension image in ceph-testcontainers-images.** Such an
   image would need: a pinned Ceph commit that ships Crimson and whose
   CyanStore boots and serves I/O, identified by digest, with the same
   commit for MON, MGR and OSD; both `ceph-osd-crimson` and
   `ceph-osd-classic` so that a classic fixture keeps working; a per-image
   record of architecture (amd64 first, arm64 only after a run), the
   `ceph --version` string and the `type` file a mkfs produces; a smoke test
   that boots one Crimson OSD and performs a client write/read before
   publishing; and a mirror of the pinned digest in a registry that the
   project controls, because quay.ceph.io serves slowly and documents no
   retention.
4. **Keep it research-only.** Track upstream until a Ceph release ships
   Crimson and CyanStore serves I/O, and revisit then.

## Recommendation

No-go now; option 4, with the evidence above recorded. The reasons:

- **No release ships Crimson.** Tentacle and Squid, the supported releases,
  build without it. Only `main` (21.3, unreleased) has it, so a fixture
  would depend on daily development builds that the project cannot pin for
  long and that would be a third Ceph version next to the two supported
  ones.
- **CyanStore does not work in the current build.** The crash is
  deterministic, independent of privileges and resources, and its cause is
  a missing scheduler initialization in the OSD start path for stores
  without a device class. Nothing in the fixture can work around it.
- **The alternative store proves the plumbing but not the goal.** Crimson
  with alien MemStore works unprivileged, which shows that the bootstrap,
  the MON settings and the container settings are not the obstacle. It does
  not justify a fixture, because it exercises the classic MemStore code in
  threads, which `OSDMemStore` already covers.

Reopen the question when either holds, and start with option 3:

- A `main` build after an upstream fix for the CyanStore scheduler path
  (watch `src/crimson/osd/osd.cc` near `_add_device_class` and
  `CyanStore::get_default_device_class`) boots a CyanStore OSD that serves
  the client checks above. Then also measure clean Stop/Start and kill on
  CyanStore.
- A Ceph release image ships `ceph-osd-crimson`, so the supported image
  line can be probed directly.
