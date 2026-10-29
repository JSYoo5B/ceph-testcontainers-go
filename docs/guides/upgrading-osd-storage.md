# Upgrading: OSD storage settings

The four OSD storage options are replaced by one plain value, `ceph.OSDStorage`,
carried to `Run` by `ceph.WithOSDStorage`. The functions are removed without
aliases; the `Run` signatures are unchanged.

| Before | After |
| --- | --- |
| `ceph.WithOSDBlockSize(256 << 20)` | `ceph.WithOSDStorage(ceph.OSDStorage{}.WithBlockSize(256 << 20))` |
| `ceph.WithOSDObjectStore(ceph.OSDMemStore)` | `ceph.WithOSDStorage(ceph.OSDStorage{}.WithObjectStore(ceph.OSDMemStore))` |
| `ceph.WithOSDInMemoryStorage(2 << 30)` | `ceph.WithOSDStorage(ceph.OSDStorage{}.WithInMemory(2 << 30))` |
| `ceph.WithOSDVolumeStorage()` | `ceph.WithOSDStorage(ceph.OSDStorage{}.WithVolume())` |
| `WithOSDBlockSize(a), WithOSDObjectStore(b)` | `ceph.WithOSDStorage(ceph.OSDStorage{}.WithBlockSize(a).WithObjectStore(b))` |

```golang
storage := ceph.OSDStorage{}.WithBlockSize(256 << 20).WithObjectStore(ceph.OSDMemStore)
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithOSDStorage(storage), ceph.WithOSDCount(1))
```

What differs from the removed functions:

- Pass the settings of one `OSDStorage` through one `WithOSDStorage`. A field that
  two `WithOSDStorage` options both set is refused with `ErrInvalidRequest` even
  when the values agree; the removed functions let the last one win. Different
  fields in different `WithOSDStorage` options still compose.
- An explicit zero or empty setting is refused, not read as the default:
  `WithBlockSize(0)`, `WithObjectStore("")` and `WithInMemory(0)` fail before
  anything is created. Leave a field out to keep its default.
- Calling the same method twice on one value replaces the earlier call.
- The combination rules are unchanged: MemStore cannot be combined with
  `WithInMemory` or `WithVolume`, and `WithInMemory` cannot be combined with
  `WithVolume`.
- `OSDStorage` is not a customizer: passing it straight to `Run` does not
  compile. `WithOSDStorage` changes the cluster settings only and never reaches a
  container request.

Placement and per-call settings of one OSD stay in `OSDConfig` and `OSDOptions`;
storage is cluster-wide. See the
[composition rules](../design/value-configuration-composition.md).
