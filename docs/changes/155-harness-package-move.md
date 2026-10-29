---
change: 155-harness-package-move
kind: api-break
migration: docs/guides/upgrading-harness-packages.md
---
The shared-cluster and tenant harness moved from `ceph` and the service packages to harness packages under their domains, with no aliases or forwarding declarations: `ceph/harness` has `SharedCluster`, `NewShared`, `RunTests`, `Tenant`, `TenantAccess`, the shared and lease options, `SharedStats`, `SharedPool` and `ObserveTenant`, and `rbd/harness`, `cephfs/harness` and `rgw/harness` have the tenant `Lease` entry points and their types. Service support is explicit: importing a service package no longer registers it, `cephharness.WithServices` takes the opaque `ServiceSupport` that each service harness returns from `Support()`, and a service `Lease` on a cluster that was not given its support fails with `ceph.ErrInvalidRequest` before it creates anything. The behavior of leases, releases, poisoning and Exclusive is unchanged.
