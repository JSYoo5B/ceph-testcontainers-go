# Upgrading: Run configuration

`ceph.Run`, `rbd.Run`, `cephfs.Run` and `rgw.Run` take a `ceph.RunOptions` value
and then testcontainers customizers. `ceph.RunWithImages` and `ceph.WithImages`
are removed without aliases.

| Before | After |
| --- | --- |
| `ceph.Run(ctx, ceph.DefaultImage)` | `ceph.Run(ctx, ceph.RunOptions{})` |
| `ceph.Run(ctx, image)` | `ceph.Run(ctx, ceph.RunOptions{}.WithImage(image))` |
| `ceph.Run(ctx, image, ceph.WithOSDCount(1), rbd.WithPools(p))` | `ceph.Run(ctx, ceph.RunOptions{}.WithImage(image).WithOptions(ceph.WithOSDCount(1), rbd.WithPools(p)))` |
| `rbd.Run(ctx, image, opts...)` | `rbd.Run(ctx, ceph.RunOptions{}.WithImage(image).WithOptions(opts...))` |
| `ceph.RunWithImages(ctx, set, opts...)` | `ceph.Run(ctx, ceph.RunOptions{}.WithImageSet(set).WithOptions(opts...))` |
| `ceph.Run(ctx, set.Essential, ceph.WithImages(set))` | `ceph.Run(ctx, ceph.RunOptions{}.WithImageSet(set))` |
| `ceph.Run(ctx, image, testcontainers.WithEnv(env))` | `ceph.Run(ctx, ceph.RunOptions{}.WithImage(image), testcontainers.WithEnv(env))` |
| `ceph.WithSharedRunOptions(ceph.WithOSDCount(1))` | `ceph.WithSharedRunOptions(ceph.RunOptions{}.WithOptions(ceph.WithOSDCount(1)))` |

Where each argument goes:

- Cluster and service settings are `ceph.Option` values (every `ceph.WithX`
  cluster setting and the options of `rbd`, `cephfs` and `rgw`, including
  `cephfs.WithMDSImage` and `rgw.WithImage`). Pass them with
  `RunOptions.WithOptions`. They are never container customizers: passing one
  after the `RunOptions` is refused with `ErrInvalidRequest` instead of being
  silently dropped.
- Docker and container execution policies are testcontainers customizers
  (`testcontainers.WithEnv`, `WithEntrypoint`, `WithCmd`, ...). They stay
  trailing arguments and apply to the first MON's request only; see
  [Container customizers](../ceph.md#container-customizers) for the order.
- The image is selected once. A role image set no longer needs its Essential
  image as a separate argument, and a missing role image that a requested feature
  needs is still an error; there is no fallback to Essential. Selecting an empty
  image or image set is refused.
- `RunOptions{}.WithImage("")` is an explicit invalid selection, not the default.

`rbdmirror.RunWithImages` and `cephfsmirror.RunWithImages` are separate functions
and are not changed by this migration. See the
[composition rules](../design/value-configuration-composition.md).
