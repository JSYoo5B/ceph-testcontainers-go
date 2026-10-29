---
change: 146-observer-control-queries
kind: feature
---
`ceph.Observer` gains `QuorumStatus`, `MonitorBootstrapAddresses`, `HealthDetails` and `FullRatios`, for a cluster (`ceph.Observe`) and for a tenant lease (`harness.ObserveTenant` of `ceph/harness`). Each query reads the cluster when it is called and keeps the locking, control-handle and identity checks of the `Container` method of the same name; a tenant-backed observer validates its lease on every query. The `Container` and `Tenant` methods are unchanged.
