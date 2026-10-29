---
change: 332-remove-shared-harness
kind: api-break
migration: docs/guides/remove-shared-harness.md
supersedes: [155-harness-package-move, 155-observe-tenant-seam, 146-shared-backend-port-narrowing, 155-shared-cluster-ops-port, 155-shared-pool-single-definition]
---
Remove the shared-cluster and tenant harness completely, including the public
`ceph/harness`, `rbd/harness`, `cephfs/harness` and `rgw/harness` packages,
shared/tenant/exclusive entry points and options, service support and lease
adapters, and the core harness-only admission and lifetime plumbing. There are
no aliases or replacement sharing framework. Tests now own independent `Run`
fixtures and explicitly create and clean up their resources and identities.
Ordinary lifecycle, uncertain-outcome cleanup, fault restoration and the native
RBD, CephFS and RGW operations remain supported, including actual RGW tenants
and accounts.
