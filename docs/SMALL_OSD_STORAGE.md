# Small OSD fixtures

`WithOSDBlockSize(size)` sets each sparse BlueStore file's logical capacity.
The default remains 1 GiB; the module accepts values starting at 64 MiB.
This is a fixture admission floor, not an absolute minimum supported by every
Ceph version or a promise that an arbitrary workload fits.

For a small fixture, start the control plane without OSDs and explicitly skip
mClock's startup capacity calibration before adding the first OSD. The following
fragment runs inside a Go test with `ctx` and a selected Ceph `image`:

```go
cluster, err := ceph.Run(ctx, image,
    ceph.WithNoInitialOSDs(),
    ceph.WithOSDBlockSize(64<<20),
    // Optional: omit this option to use container-local sparse files.
    ceph.WithOSDInMemoryStorage(256<<20),
)
// Register cleanup before handling an error; Run can return partial resources.
if cluster != nil {
    testcontainers.CleanupContainer(t, cluster)
}
if err != nil {
    t.Fatal(err)
}

// Keep this setting until whole-cluster cleanup, including any OSD restarts.
// Terminate disposes of the configuration database, so no Restore is needed.
if _, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{
    Section: "osd", Name: "osd_mclock_skip_benchmark", Value: "true",
}); err != nil {
    t.Fatal(err)
}
for range 2 {
    if _, err := cluster.AddOSD(ctx); err != nil {
        t.Fatal(err)
    }
}

// Create the test's pool and exercise a bounded client workload here.
```

Apply the setting before `AddOSD`, and keep it for the entire fixture lifetime.
Restoring it while small OSDs remain can enable calibration on their next boot.
`TemporaryConfig` remains tracked even when application returns an error;
whole-cluster cleanup also disposes of that partial preparation.
Local files or custom daemon arguments can override central settings, so check
the daemon's effective `osd_mclock_skip_benchmark` value when customizing them.

The setting only skips capacity calibration; it leaves `osd_op_queue` and the
mClock profile unchanged. No scheduler change, privileged container or new
image dependency is required. Without this explicit preparation, the module
retains Ceph's default benchmark behavior.
[Ceph 20.2.4's option definition](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/common/options/osd.yaml.in#L1277-L1289)
and [startup gate](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/OSD.cc#L10211-L10270)
show the default and its scope.

## Capacity and memory bounds

These three settings describe different resources:

| Resource | Bound | What it includes |
| --- | --- | --- |
| Per-OSD logical storage | `WithOSDBlockSize` | BlueStore object data, metadata and temporary writes within each sparse file. |
| Shared allocated RAM backing | `WithOSDInMemoryStorage` | Allocated tmpfs pages and files across all OSD directories in the cluster. |
| Process RAM | Docker host/VM limits and daemon configuration | MON/MGR/OSD and client memory; neither storage option limits their RSS. |

The default mClock calibration can temporarily prefill 100 objects of 4 MiB
per OSD before cleanup. That 400 MiB footprint alone exceeds a 64, 128 or
256 MiB logical file. Filling BlueStore can abort the OSD rather than yield a
successful bootstrap with less free space.
[The prefill implementation](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/OSD.cc#L11687-L11714)
and [BlueStore's ENOSPC handling](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/os/bluestore/BlueStore.cc#L16235-L16251)
explain why calibration is an explicit part of this recipe.

Skipping calibration leaves metadata, replicas, BlueFS log growth, recovery
and live client data to account for. A larger logical file does not reserve
its entire size in RAM. Conversely, removing a Ceph object does not necessarily
release the sparse backing file's allocated tmpfs pages. Choose both bounds
for the actual workload and leave headroom. See
[in-memory OSD storage](OSD_MEMORY_STORAGE.md) for keeper lifetime, Docker
host/VM memory, allocation-cap failures and cleanup behavior.

## Functional validation scope

`TestSmallOSDBlockSizeTopology` uses independent serial bridge clusters with
Ceph 20.2.4. Each case starts two OSDs, writes 16 objects totaling 8 MiB into
a two-replica pool, restarts the same OSD containers, adds a third OSD and
backfills, then drains and removes an original OSD. Every data phase uses an
independent full reader and checks the complete payload hash. The test also
checks native device capacity, effective scheduler settings, retained cluster
and OSD identities, and removal of owned resources.

All ten cases passed locally on Docker Desktop's Linux ARM64 engine with
4 CPUs and approximately 4 GiB of VM memory, using the prepared Ceph 20.2.4
control and OSD role images. The complete parent took 550.96 seconds;
the Go package took 551.409 seconds. Each case verified all four data phases,
topology changes and owned cleanup. This is functional evidence, not a
disk-versus-RAM performance comparison or a result for the full AMD64 CI.

| Per-OSD logical size | `skip_benchmark` | Container-local files | RAM backing | Shared RAM cap |
| --- | --- | --- | --- | --- |
| 64 MiB | `true` | PASS | PASS | 256 MiB |
| 128 MiB | `true` | PASS | PASS | 512 MiB |
| 256 MiB | `true` | PASS | PASS | 1 GiB |
| 512 MiB | `true` | PASS | PASS | 2 GiB |
| 512 MiB control | `false` | PASS | PASS | 2 GiB |

The 512 MiB controls leave the benchmark flag at its native default `false`.
That establishes the configured policy; it does not, by itself, prove that
calibration ran on every boot, because Ceph can reuse capacity information.
The matrix covers this bounded 8 MiB workload, not every existing test scenario
or production-sized data set. In particular, it does not establish that a
64 MiB OSD can hold the separate 128 MiB RAM-storage fixture's payload.

Run this focused matrix with `make scenario-small-osds` after selecting the
prepared images. It also belongs to the existing `scenario-empty-bootstrap`
CI group. The local raw log, frozen execution-source hashes and independent
resource cleanup report are retained in
`artifacts/small-osd-storage-20261009/`.
