---
change: 146-observer-only-queries
kind: api-break
migration: docs/guides/upgrading-observer-queries.md
supersedes: [146-observer-control-queries, 146-observer-locked-queries, 146-observer-rados-metrics-queries]
---
`ceph.Observe(cluster)` and `harness.ObserveTenant(tenant)` of `ceph/harness` return a `*ceph.Observer` that has all sixteen read-only observations of a cluster or of a tenant lease; the tenant form validates its lease on every query. The observations (`Status`, `ManagerStatus`, `DaemonAddresses`, `Configuration`, `BlocklistEntries`, `MGRModules`, `OSDFlags`, `OSDStates`, `ManagerServices`, `QuorumStatus`, `MonitorBootstrapAddresses`, `HealthDetails`, `FullRatios`, `PGInconsistencies`, `ObjectWatchers` and `ManagerMetrics`) are methods of `ceph.Observer` only. They are removed from `ceph.Container`, and the `Tenant` forwarders `Status`, `HealthDetails`, `Pools`, `PoolStatus`, `PoolUsage`, `ObjectWatchers`, `OSDFlags` and `BlocklistEntries` are removed. `Container.Pools` is replaced by `Observer.Pools`, which lists `*ceph.Pool` objects; the per-pool reads are the `Status`, `Usage` and `PGs` methods of `Pool`. There are no aliases.
