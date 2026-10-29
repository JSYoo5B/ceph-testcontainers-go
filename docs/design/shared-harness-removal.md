# Shared-cluster harness removal

The owner retired the shared-cluster/tenant test harness because its reuse
benefit did not justify lifetime, admission, isolation and recycling complexity.
This decision supersedes extraction and hardening of the framework. Tests own
independent `ceph.Run` fixtures and explicitly create and clean their resources.
There is no replacement sharing facade, deprecated alias or retired mode.
See the [migration guide](../guides/remove-shared-harness.md).

## Removal boundary

| Area | Disposition |
| --- | --- |
| `ceph/harness`, `rbd/harness`, `cephfs/harness`, `rgw/harness` | Remove all public orchestration and service lease adapters. |
| Core shared admission, exclusive mode, tenant pool/observer provenance | Remove the framework-specific runtime state and its callers. |
| `internal/harnesshook`, `internal/sharedinvariants` | Remove the framework-only contracts and invariants. |
| Harness slots in `internal/hookreg`, tenant/support ports in service/test helpers | Remove the harness routes; retain ordinary service and unit-access seams. |
| `internal/namedregistry`, `internal/servicekind` | Restore ordinary gateway state and bootstrap constants to their owning service packages; remove cross-package harness access. |
| Fault/override admission | Remove only harness admission; retain active-token ownership, restoration, finalization and uncertain-result cleanup. |
| Host-port, OSD device/store and network-delay ownership | Retain ordinary resource ownership and retryable cleanup. These are not tenant leases. |
| RGW tenants/accounts | Retain the actual Ceph feature; it is unrelated to test-harness tenancy. |

## Native scenario disposition

| Removed scenario or assertion | Reason or retained independent coverage |
| --- | --- |
| `TestSharedCluster` namespace/Cephx assertions | Preserve in `TestClientIdentities`: own namespace positive operations, foreign/default/all-namespace and administrative denials, read-only identity. |
| `TestSharedCluster` gate, sequential reuse and dirty-tenant recycling | Framework-only authority, reuse, poisoning and statistics; remove. |
| `TestSharedClusterExclusive` | Framework-only lease arbitration, exclusivity and retirement; remove. Ordinary cluster operations, fault restoration and lifecycle tests remain. |
| `TestSharedClusterServices` RBD operations | Preserve in `TestRBDNamespaces` and `TestRBDLifecycle`: namespace permissions, snapshot/protect/clone/trash, explicit resource cleanup and killed image-client handling. |
| `TestSharedClusterServices` CephFS operations | Preserve in `TestCephFSSubvolumes` and `TestCephFSSubvolumeClientAuthorization`: group/subvolume ownership, path/data namespace permissions and killed mounted-client cleanup. |
| `TestSharedClusterServices` RGW operations | Preserve in `TestRGWTenantsAndAccounts`: distinct actual Ceph tenants, same bucket names, cross-tenant access denial and owned user/bucket cleanup. |
| Service release absence checks | Preserve as explicit owned cleanup assertions in independent service scenarios. |
| Dirty service tenant poisons/recycles the fixture | Framework-only orchestration; remove. |
| `TestSharedClusterBenchmark` | Measures the retired sharing framework; remove instead of retaining a benchmark campaign. |

The source-derived planner no longer produces the `shared_cluster` or optional
`shared_benchmark` batches. Generic dispatch allocation/cost tooling and native
scenario coverage policy remain applicable to independent fixtures.

## Public API and historical material

The package-scoped catalogues and deliberate-removal record account for all
65 removed declaration rows: 30 in `ceph/harness`, 7 in `rbd/harness`, 11 in
`cephfs/harness`, 9 in `rgw/harness` and 8 in `ceph` (the shared-policy error
and seven formerly gated wrappers now inherited from Testcontainers). No
declaration is added. Regeneration and approval are separate operations. The
breaking-change fragment links the migration guide. Published release history
and former design/evidence remain historical records and do not establish
current API support.

## Validation boundary

Compilation, unit/tooling checks and native behavior are separate evidence.
Focused standalone native cases must establish the retained RADOS/service
behavior and owned cleanup on the final source. Earlier shared-harness passes
cannot qualify the replacement fixtures. Independent review checks the complete
combined tree, including private hooks and scenario disposition, before merge.
