---
change: 146-observer-rados-metrics-queries
kind: feature
---
`ceph.Observer` gains `PGInconsistencies`, `ObjectWatchers` and `ManagerMetrics`, for a cluster (`ceph.Observe`) and for a tenant lease (`harness.ObserveTenant` of `ceph/harness`). Each query reads the cluster when it is called and keeps the argument validation, locking, client or control container selection and bounds of the `Container` method of the same name; a tenant-backed observer validates its lease on every query. With these, all sixteen observations are available through an `Observer`. The `Container` and `Tenant` methods are unchanged.
