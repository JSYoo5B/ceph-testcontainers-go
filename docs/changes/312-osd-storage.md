---
change: 312-osd-storage
kind: api-break
migration: docs/guides/upgrading-osd-storage.md
---
`ceph.WithOSDBlockSize`, `ceph.WithOSDObjectStore`, `ceph.WithOSDInMemoryStorage` and `ceph.WithOSDVolumeStorage` are removed. The OSD storage settings are one plain chainable value, `ceph.OSDStorage` (`WithBlockSize`, `WithObjectStore`, `WithInMemory`, `WithVolume`), passed to `Run`, `rbd.Run`, `cephfs.Run` and `rgw.Run` through one option function, `ceph.WithOSDStorage`; the `Run` signatures are unchanged. A field set by two `WithOSDStorage` options is refused even when equal (the removed functions let the last one win); explicit zero or empty settings stay refused and an unspecified field keeps its default. The combination rules, defaults and runtime behavior are unchanged. There is no deprecated alias or forwarding layer.
