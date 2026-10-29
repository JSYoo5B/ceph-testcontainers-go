---
change: 312-run-options
kind: api-break
migration: docs/guides/upgrading-run-options.md
---
`ceph.Run`, `rbd.Run`, `cephfs.Run` and `rgw.Run` take `(ctx, config ceph.RunOptions, customizers ...testcontainers.ContainerCustomizer)` instead of `(ctx, image, opts...)`. The image is selected on the value (`RunOptions{}.WithImage(image)`, or `WithImageSet(set)` for role images, where Essential is no longer supplied twice); with no selection `ceph.DefaultImage` is used. Cluster and service settings (`ceph.WithOSDCount`, `rbd.WithPools`, `cephfs.WithFilesystems`, `rgw.WithGateways`, `cephfs.WithMDSImage`, ...) are passed with `RunOptions.WithOptions`. The trailing variadic keeps only testcontainers customizers, which reach the first MON; passing a `ceph.Option` there is refused instead of being dropped. `ceph.RunWithImages` and `ceph.WithImages` are removed, and `ceph.WithSharedRunOptions` takes `(config, customizers...)`. There is no deprecated alias or forwarding layer.
