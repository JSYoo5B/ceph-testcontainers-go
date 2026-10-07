# Bootstrap before the first manager

`ceph.WithNoInitialManagers()` constructs the selected MON and OSD topology
without creating a manager identity or starting an initial MGR container. It is
useful for first management provisioning, bootstrap ordering, and clients that
must use MON/OSD operations before manager services become available. For an
ordinary outage against an already configured cluster, the existing MGR handle's
`Stop`/`Start` remains sufficient; a stopped daemon retains its owned identity and
prior cluster state.

```go
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
    ceph.WithNoInitialManagers(),
    ceph.WithPools(ceph.PoolConfig{Name: "application", Application: "rados"}),
)
if cluster != nil {
    defer cluster.Terminate(context.Background())
}
if err != nil {
    return err
}
// MON quorum and the two default OSDs are up/in. MGR-backed services and
// clean PG statistics are unavailable; the initial pool has been created.
// A caller can test direct data access with its own bounded native client.
manager, err := cluster.AddManager(ctx, "a")
if err != nil {
    return err // Any non-nil partial descriptor is owned by the cluster.
}
_ = manager
if err := cluster.WaitForClean(ctx); err != nil {
    return err
}
```

The ordinary default remains one MGR. `WithManagerCount(0)` and negative counts
retain their errors. An explicit positive manager count conflicts with the new
option in either order; repeating `WithNoInitialManagers()` is harmless. The
final initial CephFS and RGW service lists must be empty and are rejected before
allocation. Provision them explicitly after `AddManager`. This is the supported
initial scope, rather than a claim that every native MDS/RGW setup needs MGR.

Initial `WithPools` is supported with positive initial OSDs, preserving placement
validation, final list replacement, defaults and MON creation semantics. Pool
creation does not imply clean PGs or readiness for application I/O. In this
deliberately incomplete fixture, callers can test the direct MON/OSD data path
using bounded native operations; the ordinary clean barrier remains available
after MGR provisioning.

Combine the option with `WithNoInitialOSDs()` for a MON-only phase. The existing
[zero-storage contract](NO_INITIAL_OSDS.md) still rejects initial user pools and
data services, preserves prospective defaults 2/1, and requires storage to be
added before pools are provisioned. Either order is supported. A first manager
can be added while storage is still absent; then add OSDs and pools explicitly.

## Readiness and ownership

The explicit no-manager constructor waits for MON majority quorum and checks
owned OSD UUIDs and up/in state in the MON OSDMap. It does not wait for MGR
availability, module initialization, PG statistics or health. The ordinary
constructor retains its active-manager availability barrier. `WaitForClean` and
`WaitForPGClean` still require an available MGR and at least one clean PG; missing
or stale statistics do not become ready in this mode.

`AddManager` keeps its existing identity preflight, owner gate, timeout,
active/standby registration and partial ownership. Successful first addition
does not automatically wait for every intended MGR command or pool to become
ready. Use the relevant command/module and data barriers afterward. Its original
`ManagerContainer` compatibility pointer represents name `a`; adding a first
manager named `b` leaves this pointer nil. Use `Managers` and `ManagerStatus` for
the actual candidate list and active identity. Removing the last manager still
requires adding a replacement first.

Auth creation can commit despite a lost response. This option adds no adoption,
blind retry or cleanup receipt: existing foreign-identity preflight still rejects
that identity on a new Add. After a later Docker launch failure, the existing
non-nil descriptor remains owned for whole-cluster cleanup. A non-nil Run result
with an error also needs `Terminate`. Valid canceled construction returns before
network, port or container allocation and request customization. Constructor
customizers retain their MON-only role.

There are zero initial MGR daemons, rather than zero Docker resources. MONs,
OSDs, a multi-MON CLI, bridge/host resources and later additions remain owned by
the cluster. The fixed image contract still requires control and OSD payloads,
including `ceph-mgr`, its modules and dependencies even when startup is deferred.
The original control image is retained for `AddManager`; a prepared OSD image
is retained for storage startup. Skipping a daemon never certifies missing image
components. No image build, new role, setup entrypoint or go-ceph/cgo dependency
is introduced.

Ceph can raise `MGR_DOWN` while no manager exists, and that specific condition
may progress from warning to error despite a usable client data path. Without
OSDs it may be suppressed. No particular health status or missing-check presence
is required for cold construction. [Ceph's MGR_DOWN guidance](https://docs.ceph.com/en/tentacle/rados/operations/health-checks/#mgr-down)
distinguishes client I/O from management commands. This is a deliberately
incomplete fixture, rather than a normally healthy cluster.

## Validation

`TestNoInitialManagerTopology` uses independent bridge/host cold-MGR and combined
MON-only fixtures under minimal `integration,topology` tags. The normal case
creates a pool with two OSDs, pins the original FSID, pool ID, OSD UUIDs and full
CIDs, then writes and reads 128 KiB nonce bytes with fresh native clients before
the first manager. It adds name `a`, verifies the exact owned descriptor/CID and
positive native GID, executes the required `rbd_support` task command, waits clean
and for `HEALTH_OK`, and reads the retained bytes on the same pool/cluster. Last
manager removal is rejected. The combined case authenticates to MON with no
manager/storage, adds the first manager while OSD inventory remains empty, then
adds two OSDs, creates a pool and checks independent native bytes.

Raw native MGR fields are explicitly present, non-null and empty/false/zero before
addition. Auth entries require present non-null nonempty unique entity names;
only parsed MGR entity names/counts are logged, never keys or raw auth listings.
A fresh raw engine/full-CID oracle admits only this owner's selected daemons,
the known client and at most one standard session Ryuk. It rejects hidden workers
and allocation residue and separately verifies original resource disappearance.
This proves the owner's launches, rather than global absence of management.

Native health detail allows only `MGR_DOWN` and the explicitly expected storage
checks. Aggregate `HEALTH_ERR` is permitted only with an actual severe MGR_DOWN
detail; it is never successful post-provision closure. Unexpected checks or
`MGR_MODULE*` errors fail immediately, retaining actual detail for diagnosis.
After management/storage provision, require `HEALTH_OK` within an explicit
90-second context shared by polling and each native query. If actual evidence
proves a fixed image requirement failure, halt dependent work and report it;
never alter policy, build an image or hide it as an absence warning.

On 2026-10-07 `make scenario-manager-bootstrap` passed on the unmodified
pinned Quay Ceph 20.2.4 image, Linux ARM64 Docker engine, for bridge and host
networking. All seven named tests passed without skips: the parent, two network
parents and four independent leaves. Package time was 141.986 seconds; the parent
took 141.52 seconds, bridge 70.31 and host 71.21. The source assertions and saved
records cover four zero-manager and authenticated client observations, 18 MGR
map reads, 14 parsed auth-entity inventories, ten raw resource checks, ten exact
128 KiB byte/hash records, four owned cleanup checks and four completed fixtures.
Both positive-storage fixtures read the same bytes before and after management
provisioning; the MON-only fixtures authenticate before management/storage and
read new bytes after both are provisioned. All ten health detail reads actually
observed `HEALTH_OK` with empty checks. The permitted severe `MGR_DOWN` branch was
not observed in this run.

The separate outer cleanup also passed with no new containers or networks. The
same 250 runtime source inputs, raw engine and fixed image-requirement hash were
retained before and after execution. Full root unit/race/vet/all-tag compilation
passed before native execution. Fresh completed six-tag all, integration default,
integration SDK and minimal exact-parent lists confirmed 115 required names
(113 internal plus two SDK); only this new parent was added to the historical
storage-bootstrap inventory of 114. Compilation and selection are separate from
the focused runtime result and the historical whole CI of 101.

The [native log](../artifacts/no-initial-managers-20261007/native-runtime.log),
[command and terminal provenance](../artifacts/no-initial-managers-20261007/checks-provenance.json),
[actual compiled selection](../artifacts/no-initial-managers-20261007/compiled-selection.json),
and [outer cleanup](../artifacts/no-initial-managers-20261007/cleanup/after.json)
are saved in the validating checkout. Existing `scenario-storage-bootstrap`
independently passed on this source in 89.109 seconds, with all seven named tests
and separate cleanup leaving no new containers or networks. Its parent took
88.71 seconds, bridge 45.03 and host 43.69. Its
[own provenance](../artifacts/no-initial-managers-20261007/storage-bootstrap-regression/provenance.json)
and [cleanup](../artifacts/no-initial-managers-20261007/storage-bootstrap-regression/cleanup/after.json)
remain separate from the no-manager episode. Both runs retain the same 250 source
inputs and fixed image policy. Historical storage-bootstrap measurements and the
earlier whole CI remain attached to their original source.

Operational ceilings are 15 minutes for cold-positive and ten minutes for
combined MON-only per network, with separate three-minute whole-cluster cleanup;
a failed leaf stops later fixtures. The dedicated CI profile uses Go 80 minutes
and job 90 minutes, depends on `scenario-default`, clears the existing role
overrides and retains its own baseline/always-cleanup artifact. These ceilings
are not runtime promises. This focused validation does not certify a new whole
CI/image matrix, all APIs, global process absence, every image/platform
combination or recovery from an auth response loss. No image-requirement violation
was observed; any substantiated violation must stop dependent work and preserve
the exact policy item, image digest, reproducing command and native failure
source evidence.
