# Optional in-memory OSD storage

`WithOSDInMemoryStorage(maxBytes)` places the cluster's OSD backing files in
one Docker-managed tmpfs volume. It is an opt-in fixture setting for callers
who want to avoid storing OSD data in the container's writable layer. The
default remains a sparse BlueStore file in each OSD container.

```go
cluster, err := ceph.Run(ctx, image,
    ceph.WithOSDCount(2),
    ceph.WithOSDInMemoryStorage(2<<30),
)
// A nonnil cluster owns partial resources even when Run returns an error.
testcontainers.CleanupContainer(t, cluster)
if err != nil {
    t.Fatal(err)
}
```

`maxBytes` is a positive allocation ceiling for the entire cluster's OSD
volume, including the files in its OSD directories. It is not a per-OSD
reservation, a daemon memory limit, or an amount allocated eagerly. Values
at or below zero are rejected before Docker allocation. The logical
`WithOSDBlockSize` remains independent: two default OSDs each advertise a
1 GiB sparse block file, while the example caps their shared allocated backing
storage at 2 GiB. The cap can be smaller than the sum of the logical block
sizes. Filling the volume produces native filesystem/Ceph errors and can abort
an OSD; it does not expand the cap or fall back to disk.

Size the cap for replicas, BlueStore/BlueFS metadata and temporary writes,
including startup and recovery work. With the default mClock scheduler,
Ceph 20.2.4's startup capacity calibration can prefill 100 objects of 4 MiB
per OSD before removing them.
[Ceph's startup benchmark](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSD.cc#L9644-L9693)
and [prefill implementation](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSD.cc#L11036-L11063)
show this temporary footprint. Removing a Ceph object does not necessarily
release the allocated tmpfs pages in its sparse BlueStore backing file.
Later writes or another startup can therefore need more backing capacity than
the current live payload suggests. There is no universal minimum cap: size it
for the chosen OSD count and workload, and leave headroom. This option does not
disable the startup benchmark or change the scheduler implicitly.

The volume uses the Docker daemon's Linux memory. On macOS with Docker Desktop,
this is the Docker Linux VM's memory. A remote Docker daemon uses that remote
host's memory, without requiring a path on the Go client's machine. A daemon
that cannot mount a local-driver tmpfs returns an error. Support depends on
the Docker daemon and its kernel, including restrictions in rootless or managed
environments; selecting the option does not prove that a given daemon supports it.

Tmpfs pages compete with MON, MGR, OSD, MDS, RGW and client processes for RAM.
The cap bounds backing-file allocations, not total fixture memory. Tmpfs can
use swap, so this option does not promise that every page stays in physical
RAM. Account for each cluster's cap when running several clusters or tests
concurrently. [Linux tmpfs documentation](https://docs.kernel.org/filesystems/tmpfs.html)
explains allocation, swap and capacity limits.

## Ownership and restart lifetime

The module lazily creates a unique owned named volume and one idle keeper
container from the selected control image before the first OSD allocation.
`WithNoInitialOSDs()` therefore creates neither resource until the first
`AddOSD` call. The keeper runs `sleep infinity` without a network connection.
It holds the volume mounted while OSD containers stop and start.

OSDs use separate `ceph-ID` directories on the shared volume. An ordinary
same-container OSD `Stop`/`Start` retains its store while the original keeper
remains running. Successfully purging and removing an OSD also discards that
original OSD directory, allowing its numeric ID and storage capacity to be
reused by a later OSD. A plain per-container Docker tmpfs would disappear on
container stop and does not provide this lifetime.
[Docker tmpfs mounts](https://docs.docker.com/engine/storage/tmpfs/) and
[Docker volumes](https://docs.docker.com/engine/storage/volumes/) describe the
different mount lifetimes.

This is temporary storage for one running fixture. Keeper loss, Docker daemon
or VM restart, and machine reboot are outside its data-retention contract.
The module does not silently replace a lost keeper or accept a restarted keeper
with a potentially empty store. Independent clusters have independent owned
volumes and keepers.

Use whole-cluster `Terminate` for successful and partial startup cleanup.
It removes OSDs before releasing the keeper and named volume. Failed cleanup
retains ownership for retry with a fresh context; the keeper remains owned
while OSD removal is pending. The volume carries Testcontainers labels for
Ryuk cleanup when enabled. Normal container removal alone does not establish
that an owned named volume was removed.

## Image and test scope

The selected images keep their existing role contracts. OSDs still initialize
sparse BlueStore files with `ceph-osd --mkfs`. Docker manages the volume mount;
the image does not need a `mount` executable, block device, LVM, privileged mode,
or extra package. The idle keeper uses the control image's existing `sleep`
command. Image construction and requirements remain owned by
`ceph-testcontainers-images`.

This option supports disposable topology and client I/O tests. It does not
establish physical-disk durability or improve every scenario's execution time.
Mirror election, native convergence, required negative observation windows,
CLI calls and container cleanup can still dominate a fixture. The option does
not select a scheduler or shorten assertion windows; use the
[OSD scheduler measurements](OSD_SCHEDULER.md)
to distinguish actual data movement from those other waits.
