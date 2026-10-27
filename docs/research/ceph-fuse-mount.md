# Feasibility: ceph-fuse mounts in client containers

This report decides whether ceph-testcontainers-go can offer an opt-in CephFS
mount through `ceph-fuse` inside a client container, so that a test can use
ordinary POSIX file I/O, including `fcntl` record locks, on a real CephFS
mount. It records what each container engine needs, what was observed, and
what an API would have to do. This feasibility study commits no public API.

Two kinds of statement appear below, and they are kept apart:

- **Runtime evidence**: observed in a run of `TestCephFSFuseMount`
  (`internal/integration/cephfs_fuse_mount_integration_test.go`, the
  optional batch `cephfs_fuse`), whose log lines are quoted.
- **Documentation**: what engine and kernel documentation states, not yet
  observed by this project.

## Summary

| Environment | Outcome | Minimum settings | Basis |
| --- | --- | --- | --- |
| Native Linux Docker Engine, rootful, AppArmor `docker-default`, default seccomp (FUSE built into the kernel) | **Works** on Tentacle 20.2.4 and Squid 19.2.5, with the official image and with the official role images | `--device /dev/fuse`, `--cap-add SYS_ADMIN`, `--security-opt apparmor=unconfined` | Runtime evidence |
| GitHub-hosted runners (`ubuntu-24.04`) | Untested; expected to behave like the row above | Same as above | Documentation: the runner is a rootful Docker Engine on Ubuntu with AppArmor enabled |
| Docker Desktop (macOS, Windows) | Untested | Documentation suggests `--device /dev/fuse` and `--cap-add SYS_ADMIN`; the VM applies no AppArmor profile | Documentation |
| Rootless Docker | Untested | `--device /dev/fuse` and `CAP_SYS_ADMIN` inside the user namespace; kernels since 4.18 allow FUSE mounts in a user namespace | Documentation |
| Podman | Untested | Rootful: as Docker, with SELinux labels instead of AppArmor; SELinux hosts may need `label=disable` | Documentation |

Recommendation: **go for an opt-in, admission-checked client option limited
to engines that pass the admission; no-go for any default.** See
[Recommendation](#recommendation).

## Minimum settings on native Linux Docker Engine

The host check was read-only: `/dev/fuse` exists (`crw-rw-rw-`, 10:229), the
kernel lists `fuse` in `/proc/filesystems`, and `CONFIG_FUSE_FS=y`, so the
driver is built in rather than a module (`/proc/modules` has no `fuse`).
Docker reports `apparmor` (profile `default`), `seccomp` (profile `builtin`)
and `cgroupns`, with cgroup v2. Nothing on the host was changed.

Each setting was tried on a fresh client container of the official image,
first with an admission probe (see [Admission](#admission)) and then with
`ceph-fuse -f` itself. Every smaller setting fails in `fuse_mount`, before
the client opens an MDS session, and `ceph-fuse` exits 5. The results were
the same on Tentacle 20.2.4 and Squid 19.2.5:

| Settings | Admission | `ceph-fuse` |
| --- | --- | --- |
| none | `/dev/fuse device`: `open /dev/fuse` returns `ENOENT` | `fuse: device not found, try 'modprobe fuse' first` |
| `--device /dev/fuse` | `CAP_SYS_ADMIN`: `mount(2)` of type `fuse` returns `EPERM`; `CapEff` lacks bit 21 | `fusermount: mount failed: Operation not permitted` (libfuse 2 falls back to the setuid `fusermount`, which the capability bounding set also stops) |
| `--device /dev/fuse --cap-add SYS_ADMIN` | `security profile`: `mount(2)` returns `EACCES` under `docker-default (enforce)` | `fuse: mount failed: Permission denied` |
| the above with `--security-opt seccomp=unconfined` | `security profile`, as above | `fuse: mount failed: Permission denied` |
| the above with `--security-opt apparmor=unconfined` instead of seccomp | admitted | mounts |

A probe without Ceph also showed that `--cap-add SYS_ADMIN` without the
device fails with `ENOENT`, and `--device /dev/fuse --security-opt
apparmor=unconfined` without the capability with `EPERM`.

So the minimum is the device, `CAP_SYS_ADMIN` and an AppArmor allowance.
Docker's default seccomp profile already allows `mount(2)` to a container
that holds `CAP_SYS_ADMIN`, so seccomp needs no change. The AppArmor
allowance is the coarse part: `apparmor=unconfined` removes the whole
`docker-default` profile from that one container. A narrower profile that
only adds `mount fstype=fuse.*` would have to be loaded on the host, which
the fixture must not do. `--privileged` also works but grants every device
and capability, and is not needed.

The settings can be limited to the mounting client container. In the test,
the cluster's daemons and the fixture's own client container keep their
default privileges; only the FUSE client containers get the three settings.

## Images

The official image `quay.io/ceph/ceph` ships `/usr/bin/ceph-fuse`,
`/usr/bin/fusermount` (setuid) and `fuse-libs` 2.9.9 on both releases. The
official role images (`official-<release>-control` and the others; the `-control`
tag belonged to the retired role layout, whose current replacements are
essential, client, cephfs, rbd-aux, rgw and all) do not
ship `ceph-fuse` or `fusermount`. Under the role layout the test reports
that refusal for the control image and starts its FUSE client containers
from the official image of the cluster's release, `quay.io/ceph/ceph:v<version>`:

```
ceph-fuse mount refused: ceph-fuse binary: control image ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-control has no executable /usr/bin/ceph-fuse; the FUSE client containers use quay.io/ceph/ceph:v20.2.4
```

No package is installed at run time.

## Admission

The admission decides before anything changes in the cluster or in the
container's mount table. It runs one Python script in the client container
and classifies the result into a typed refusal (`fuseRefusal` in the test,
with a `Requirement` and a `Detail`):

1. `/usr/bin/ceph-fuse` is executable, or the refusal is `ceph-fuse binary`.
2. `/dev/fuse` opens read-write, or the refusal is `/dev/fuse device` with
   the errno.
3. One `mount(2)` of type `fuse` with that descriptor on a scratch directory,
   detached at once with `umount2(MNT_DETACH)`. Nobody serves the
   connection, so the probe neither blocks nor reaches Ceph. `EPERM` becomes
   `CAP_SYS_ADMIN` (with the effective capability bit), `EACCES` becomes
   `security profile` (with the label from `/proc/self/attr/current`), and
   any other errno `fuse mount`.

An engine that cannot map `/dev/fuse` at all fails the container creation;
that error becomes the refusal `engine device mapping`.

The probe is the deciding step. Reading capabilities and the AppArmor label
alone would guess; the kernel's answer to the same `mount(2)` that libfuse
makes is the evidence. In every run each refused setting was refused by both
the admission and `ceph-fuse`, with the libfuse message that matches the
admission's requirement, and the setting that the admission accepted
mounted.

## Lifecycle findings

All of these are runtime evidence from the four native runs.

- **Mount and readiness.** `ceph-fuse -f --id fuse-mount --client_fs <fs>
  -r /fuse /mnt/cephfs` runs in the foreground under `RunNativeCommand`.
  Readiness is two observations: the container's `/proc/mounts` lists
  `/mnt/cephfs` with type `fuse.ceph-fuse`, and the MDS's session list holds
  an `open` session whose metadata names the identity (`entity_id`), the
  container's hostname, the root `/fuse` and the PID of the one `ceph-fuse`
  process for that mount point. That session's ID is the mount's client ID.
  Readiness took 1.5 to 1.7 s.
- **Identity.** The mount uses a dedicated CephX client with `mon 'allow r'`,
  `mds 'allow rw path=/fuse'` and `osd 'allow rw tag cephfs data=<fs>'`. A
  mount of `/` with the same identity exits 1 and leaves no mount, process
  or session.
- **I/O.** 4113 random bytes written and fsynced through one mount are read
  back unchanged, with the same inode, through libcephfs (`python3-cephfs`)
  in the cluster's control container, and through a second mount in another
  container. Inode numbers through FUSE equal the CephFS inode numbers.
- **Clean unmount.** `umount /mnt/cephfs` in the container ends `ceph-fuse`
  with exit 0; the MDS closes the session and does not blocklist the
  address.
- **Killed `ceph-fuse`.** After `SIGKILL` the mount stays in the container's
  mount table and every access returns `ENOTCONN` (107); the MDS keeps the
  session `open`, and the dead client's locks keep refusing other clients.
  Recovery takes two steps: `umount` in the container, then `ceph tell
  mds.<name> client evict id=<id>`. The eviction blocklists the address
  (Ceph's default `mds_session_blocklist_on_evict`) and releases the locks.
- **Cancellation.** Cancelling the context of the supervised `ceph-fuse`
  makes the supervisor kill its process group with `SIGKILL` (within 80 to
  131 ms). The outcome is the same as a killed `ceph-fuse`: the dead mount
  and the open session remain until `umount` and eviction. A fixture
  therefore cannot rely on cancellation for cleanup; its cleanup must unmount
  and evict.
- **Partial startup.** A `ceph-fuse` that fails in `fuse_mount` exits 5 and
  leaves no mount, no process and no session.
- **Containers.** After the cleanup the client containers are terminated and
  no longer exist. A mount lives only in its container's mount namespace, so
  removing the container removes the mount; it does not close the MDS
  session, which is why eviction belongs in the cleanup.

Not covered by these runs: MDS failover and client eviction while a FUSE
client keeps working (the libcephfs tests cover the client library's
behaviour), and `ceph-fuse` reconnect behaviour.

## fcntl record-lock receipt

Two mounts of the same directory, in two containers, contend for a write
lock on bytes 0 to 4095 of one file. The receipt is identical on Tentacle
20.2.4 and Squid 19.2.5, with and without role images:

- The holder's `F_SETLK` with `F_WRLCK` returns 0; the contender's returns
  `EAGAIN` (11), and its `F_GETLK` reports `F_WRLCK` on the same range (with
  `l_pid` 0: Ceph does not report another client's PID).
- The contender's `F_SETLKW` does not return. Twice, 3 s apart, the MDS's
  `dump_ops_in_flight` holds the same request, for example
  `client_request(client.4289:11 setfilelock rule 1, type 2, owner ..., pid
  207, start 0, length 4096, wait 1 #0x10000000003 ...)`, at `failed to add
  lock, waiting`. Rule 1 is an fcntl lock (rule 2 is flock); type 2 is
  exclusive.
- After the holder's `F_UNLCK` the waiting call returns 0 (3.4 to 4.0 s after
  it started, about 0.65 s after the unlock) and the request leaves the MDS;
  the former holder's `F_SETLK` now returns `EAGAIN`.
- The holder takes the lock again and its `ceph-fuse` is killed. The
  contender's `F_SETLK` still returns `EAGAIN` while the dead client's
  session is open, and returns 0 once that session is evicted.

The test logs this as `CEPHFS_FCNTL_LOCK_RECEIPT`, and the Messenger coverage
manifest cites it for `client-mds/file-locks`. The lock state of `fcntl`
locks across an MDS failover is not covered.

Recorded native durations of `TestCephFSFuseMount` (cluster start included):
Tentacle 20.2.4 62.6 s, Squid 19.2.5 69.6 s, Tentacle role images 58.8 s,
Squid role images 66.8 s.

## Other environments

These are documentation, not evidence. The test is the optional batch
`cephfs_fuse` (build tag `cephfs_fuse`), which no required CI check runs, so
GitHub-hosted runners remain untested. Run it explicitly with
`tag_scenarios.py optional --batch cephfs_fuse`; on a refusing engine it
fails with the admission's typed reason instead of skipping.

- **GitHub-hosted runners.** `ubuntu-24.04` runners run a rootful Docker
  Engine on Ubuntu with AppArmor enabled, so the expected minimum is the same
  three settings. The Azure kernel of the runner provides FUSE.
- **Docker Desktop.** Containers run in a Linux VM whose kernel has FUSE, and
  `--device /dev/fuse` maps the VM's device. Docker Desktop applies no
  AppArmor profile, so `--device /dev/fuse --cap-add SYS_ADMIN` may be
  enough; whether the engine accepts `apparmor=unconfined` without AppArmor
  is not established. The FUSE mount lives in the VM and is not visible on
  the macOS or Windows host.
- **Rootless Docker.** The container's root holds `CAP_SYS_ADMIN` only in its
  user namespace. FUSE allows mounts in a user namespace since Linux 4.18, and
  `/dev/fuse` is usually mode 0666, so the mount may work with the device and
  the capability. Rootless Docker does not apply AppArmor profiles.
- **Podman.** Rootful Podman behaves like Docker for the device and the
  capability. On SELinux hosts the container label may deny the mount, and
  the documented remedy is `--security-opt label=disable`, a broader
  allowance comparable to `apparmor=unconfined`.

## Risks

- `apparmor=unconfined` (or `label=disable`) lifts the whole mandatory
  access control profile from the client container, not only the mount
  rule. The container also holds `CAP_SYS_ADMIN`. Both are limited to the
  one client container, but a test that runs untrusted code there gets a
  much weaker sandbox.
- Cancellation kills `ceph-fuse` without unmounting, and a dead mount keeps
  its MDS session and locks until eviction. A fixture whose cleanup misses
  the eviction leaves sessions that hold capabilities and locks, which can
  delay other clients for up to `session_autoclose`.
- An eviction blocklists the client's address. Tests that count blocklist
  entries must expect it.
- The official role images do not ship `ceph-fuse`, so a role-image cluster
  needs a second image for the FUSE client.
- Docker Desktop, rootless engines and SELinux hosts are untested. Support
  claims must stay limited to the engines the admission has admitted in CI.
- The admission probe itself needs `python3` and `ctypes` in the image; both
  are in the official image.

## API alternatives

1. **An opt-in client option with admission (recommended).** A
   `ClientContainerOption` or customizer, such as `cephfs.WithFuseClient()`,
   adds the device, `CAP_SYS_ADMIN` and the AppArmor allowance to that one
   container. A `Mount(ctx, container, identity, root, point)` call runs the
   admission first and returns a typed error (for example
   `ErrFuseUnsupported` with the missing requirement) before any change.
   It then starts `ceph-fuse -f` under `RunNativeCommand`, waits for the
   mount table entry and the MDS session, and returns a handle with the
   client ID. `Unmount` runs `umount` and waits for exit 0 and the closed
   session. The handle's cleanup, also after cancellation or a killed
   process, runs `umount`, evicts the session and waits until both are gone.
2. **`--privileged` client containers.** Simpler, and also works, but grants
   every device and capability. Rejected.
3. **A host AppArmor profile that allows only FUSE mounts.** Narrower than
   `unconfined`, but needs a profile loaded on the host, which the fixture
   must not do. Can be documented for users who manage their hosts.
4. **Keep it test-internal.** Use the PoC only as the source of the `fcntl`
   receipt, with no public API.

## Recommendation

Go, limited to an opt-in API with admission, for native Linux Docker Engine,
and keep it out of every default fixture:

- The mount works on native rootful Docker Engine with three settings on one
  client container, on both supported releases, with the official image.
- The admission predicts the outcome exactly and refuses with a typed reason
  before anything changes, so unsupported engines fail fast and explicitly.
- The lifecycle is controllable, with the caveat that cleanup must unmount
  and evict; a public handle has to own that.
- Any CI profile stays opt-in and limited to environments where the
  admission has admitted the mount. `cephfs_fuse` is therefore an optional
  batch, outside every required check: the settings it needs are proven only
  on native rootful Docker Engine. Where it runs and the engine refuses, it
  fails with the requirement it lacks, never with a skip or a fallback to
  libcephfs.
