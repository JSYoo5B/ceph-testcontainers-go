# Bootstrap MON/MGR before adding storage

`ceph.WithNoInitialOSDs()` constructs the selected MON/MGR topology without
registering, formatting or starting an initial OSD. Use it for MON commands,
authentication, quorum/manager tests and applications that must tolerate a
cluster before storage is provisioned. Add storage explicitly with the existing
`AddOSD` or `AddOSDWithConfig` methods, then provision pools or data services.

```go
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithNoInitialOSDs())
if cluster != nil {
    defer cluster.Terminate(context.Background())
}
if err != nil {
    return err
}
// The original defaults remain size=2, min_size=1, even with no initial storage.
for range 2 {
    if _, err := cluster.AddOSD(ctx); err != nil {
        return err // The cluster also owns a non-nil partial OSD descriptor.
    }
}
if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{
    Name: "application", Application: "rados",
}); err != nil {
    return err
}
if err := cluster.WaitForClean(ctx); err != nil {
    return err
}
// Attach the caller's prepared consumer container with cluster.WithClient()
// or a restricted identity and test native I/O with that consumer's library.
```

The cluster owns its MON/MGR, any independent CLI, network attachments, port
allocators and subsequently added OSDs. There are zero initial OSDs, rather
than zero Docker resources. Host mode and the existing separate public/backend
bridge option remain available; a separate backend bridge can be created before
any OSD joins it. No image build, new role or go-ceph/cgo dependency is added.

The fixed image contract still requires the `control` and `osd` role payloads.
`WithOSDImage` retains the prepared OSD image for later Add calls; a zero-storage
Run does not certify a control-only payload as a valid OSD image or weaken the
role requirements. Storage absence may cause HEALTH_WARN, which is separate from
a missing executable, module, library or other image requirement.

## Option admission and defaults

Ordinary `Run` still starts two OSDs. `WithOSDCount(0)`, negative counts and
`WithInitialOSDs()` keep their existing errors. The new option cannot be
combined with an explicit positive `WithOSDCount` or nonempty `WithInitialOSDs`,
regardless of order. Repeating `WithNoInitialOSDs()` is harmless.

The final initial user pool, filesystem and gateway lists must be empty. Thus
`WithPools(nonempty...)`, `WithCephFS(...)` and `WithRGW(...)` are rejected before
network/port/container allocation or request customizers run. Their ordinary
positive-storage validation and readiness contracts remain unchanged. Existing
list replacement semantics still apply; a final `WithPools()` may clear an
earlier list. The zero option does not silently defer or discard user services.

`WithPoolDefaults` keeps its existing positive validation and controls later
pools. Without an explicit override, prospective defaults remain size 2 and
min_size 1. Resolve them before suppressing the effective initial OSD count;
never write size 0 into the MON configuration. To use one OSD later, explicitly
request `WithPoolDefaults(1, 1)`. Adding OSDs does not automatically change pool
replicas. Custom CRUSH roots and future OSD block/image options remain usable.
The initial root-population check is skipped only in this explicit zero mode;
root/hierarchy syntax and all ordinary placement checks remain in force.

A valid canceled constructor returns the canonical context error and nil
cluster before runtime allocation. A non-nil `Run` result on any later failure
still needs whole-cluster cleanup. The constructor customizer continues to apply
to its MON request; suppressing OSDs does not suppress or apply it to other roles.

## Bootstrap readiness and lifecycle

Ordinary Run, including `WithNoInitialOSDs()` alone, still requires an answering
MON, the selected native MON topology and an available active MGR. The explicit
`WithNoInitialManagers()` option instead waits for MON quorum and owned initial
OSD states; combining both zero options constructs a MON-only phase. See the
[no-initial-manager contract](NO_INITIAL_MANAGERS.md). Neither cold constructor
requires `HEALTH_OK` or `WaitForClean`. Native
`.mgr` pools or pending PGs may exist without OSDs. No pool/data readiness is
implied. `WaitForClean` retains its existing active+clean PG and up/in contract,
and `CreatePool` retains its eligible owned-domain preflight.

Explicit Add uses the original image, networking, CRUSH default, UUID capture
and partial ownership. Removing the last registered owned OSD is still rejected,
even when the fixture originally started without storage. This option is a
construction mode, not permission to purge the last OSD or to adopt an external
OSD. Native registration transport loss and external edits are not new retry
contracts; a failed Add may require terminating the disposable cluster.

An omitted logical host resolves to `osd-ID` after native registration. If this
name conflicts with a selected root/rack, the existing Add path returns the
tracked registered descriptor before launching its container. The acceptance
scenario uses this public configuration as a bounded partial-startup fixture;
it disposes of the whole cluster rather than inventing a registration retry or
reconfiguring the partial OSD.

## Validation

`TestNoInitialOSDTopology` has independent bridge/host normal-first and
partial-first fixtures. The normal case uses a separate backend bridge in
bridge mode. Native assertions require original canonical FSID, MON quorum,
positive active MGR GID, an explicitly present empty OSD map and zero owned OSD
inventory. A fresh raw engine/list/full-CID oracle admits exactly the original
MON and MGR containers, plus at most one newly created standard session Ryuk;
it rejects hidden OSDs, unexpected containers and allocator residue.
Native health detail is logged separately. Before storage only the explicit
storage-related codes `TOO_FEW_OSDS`, `PG_AVAILABILITY`, `PG_DEGRADED` and
`POOL_NO_REDUNDANCY` are admitted. `MGR_MODULE*` checks or any other unexpected
check immediately fail, and a failed leaf stops the parent from constructing
another fixture. After two Add calls and PG clean, the normal fixture requires
`HEALTH_OK`, with one explicit 90-second context bounding both polling and
each native health CLI request. Missing modules or dependencies are never treated as an acceptable storage warning or repaired
with a new image. Health-code definitions follow the
[Ceph health checks](https://docs.ceph.com/en/tentacle/rados/operations/health-checks/).

The normal fixture verifies defaults 2/1, rejected pre-storage pool creation,
authenticated native MON access, canceled first Add, non-clean storage, two
explicit up/in OSD UUIDs, unchanged last-OSD protection and a later native pool
with positive ID and defaults 2/1. Fresh librados processes write and read back
128 KiB nonce bytes, preserving original FSID and MGR GID. The partial fixture
uses an empty custom root `osd-0` and explicit defaults 1/1; its first generated
host conflicts only after registration, preserving native ID/UUID and a nil
container descriptor. It verifies no OSD launch and retryable whole-cluster
cleanup. Both paths freshly inspect original container/network absence after
cleanup; outer runtime source/image/session/cleanup attestation remains separate.

The MON's effective pool defaults are read from its original container's admin
socket, with a bounded native subprocess and strict one-key string JSON. Central
`ceph config get` is logged separately: it reads the central database or compiled
default, which can differ from local `ceph.conf` settings. It is not the runtime
default oracle. See [Ceph configuration sources and runtime settings](https://docs.ceph.com/en/tentacle/rados/configuration/ceph-conf/).

The first native attempt failed at that incorrect central-database oracle before
storage was added. Its returned value was not logged, so the exact original
value is unknown. The failed source and log remain preserved. A separate fresh,
read-only diagnostic captured central defaults 3/0 and effective MON defaults
2/1 through both runtime show and the original admin socket. Only the native
test's readback was corrected; production defaults, images and policy stayed
unchanged.

On 2026-10-07 the corrected `make scenario-storage-bootstrap` passed on the
unmodified pinned Quay Ceph 20.2.4 image, Linux ARM64 Docker engine, for bridge
and host networking. All seven named tests passed without skips: the parent,
two network parents and four independent leaves. Package time was 89.354 seconds;
the parent took 89.07 seconds, bridge 44.90 and host 44.17. Four zero-storage
observations, six raw resource checks, eight strict runtime default reads,
four exact 128 KiB byte/hash records and two partial registrations passed.
After storage, both normal fixtures reached `HEALTH_OK` with empty checks.
All four owned cleanup checks and the separate outer cleanup passed with no
new containers or networks remaining. The same 248 runtime source inputs,
engine and fixed image-requirement hash were retained before and after execution.

The existing `TestInitialClusterComposition` independently passed in
45.706 seconds with one OSD, an initial pool and named filesystem. Fresh native
RADOS/CephFS sessions retained bytes, and its separate cleanup left no new
resources. Full unit/race/vet checks passed before the native-only oracle repair;
fresh minimal/all-tag compilation and the actual required CI selection of
114 names passed afterward. Logs, source manifests and independent reviews are
under `artifacts/no-initial-osds-20261007/` in the validating checkout.

Operational ceilings are 20 minutes for normal-first and eight minutes for
partial-first per network; cleanup has separate bounded contexts. The dedicated
CI profile uses Go 80 minutes and job 90 minutes, depends on `scenario-default`
and stops after the first failed fixture. These ceilings are not runtime
promises. This focused validation does not certify a new whole CI/image matrix,
all Ceph APIs, production upgrades, hardware performance, global process absence
or every image/platform combination. No image-requirement violation was observed;
a substantiated violation must stop dependent work and preserve the exact
policy item, image digest, reproducing command and native failure evidence.

## No-manager composition follow-up

`WithNoInitialManagers()` can also be selected with this option to construct a
MON-only phase. The [no-initial-manager validation](NO_INITIAL_MANAGERS.md#validation)
passed four independent bridge/host fixtures on the later 250-input source,
including first MGR startup before OSDs and subsequent exact data checks. This
does not replace the historical 248-input storage-bootstrap measurements above.
The existing `scenario-storage-bootstrap` independently passed on that new
source in 89.109 seconds, with all seven named tests and dedicated cleanup leaving
no new containers or networks. Its parent took 88.71 seconds, bridge 45.03 and
host 43.69. Its [own provenance](../artifacts/no-initial-managers-20261007/storage-bootstrap-regression/provenance.json)
and [cleanup](../artifacts/no-initial-managers-20261007/storage-bootstrap-regression/cleanup/after.json)
retain the separate episode and unchanged 250 source inputs/fixed policy. This
focused regression does not certify a new full CI/image matrix or replace the
historical M measurement of 89.354 seconds above.
