---
change: 146-observer-locked-queries
kind: feature
---
`ceph.Observer` gains `Configuration`, `BlocklistEntries`, `MGRModules`, `OSDFlags`, `OSDStates` and `ManagerServices`, available for a cluster (`ceph.Observe`) and for a tenant lease (`harness.ObserveTenant` of `ceph/harness`). Each query reads the cluster when it is called, takes the topology lock as the `Container` method of the same name does, and a tenant-backed observer validates its lease on every query. The `Container` and `Tenant` methods are unchanged.
