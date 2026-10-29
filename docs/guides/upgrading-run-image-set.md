# Upgrading: role images with RunOptions

`ceph.RunWithImages` and `ceph.WithImages` are removed without aliases. The
`Run` signatures are unchanged; the image set travels in a `ceph.RunOptions`
value passed among the existing customizers.

| Before | After |
| --- | --- |
| `ceph.RunWithImages(ctx, set, opts...)` | `ceph.Run(ctx, "", ceph.RunOptions{}.WithImageSet(set), opts...)` |
| `ceph.Run(ctx, set.Essential, ceph.WithImages(set), opts...)` | `ceph.Run(ctx, "", ceph.RunOptions{}.WithImageSet(set), opts...)` |
| `rbd.Run(ctx, set.Essential, ceph.WithImages(set))` | `rbd.Run(ctx, "", ceph.RunOptions{}.WithImageSet(set))` |
| `ceph.Run(ctx, image, opts...)` | unchanged |

- With an image set the image argument is empty or equal to the set's Essential
  (for an `All` set, the `All` image). Any other value is refused with
  `ErrInvalidRequest` before anything is created. Without a set the argument is
  the aggregate image, as before.
- Pass at most one `RunOptions` value that selects a set; a second one is refused
  even when equal. Within one value a later `WithImageSet` replaces the earlier
  one. An explicit empty set is refused.
- `RunOptions` is a module setting like `ceph.WithOSDCount`: it does not reach a
  container request. Raw testcontainers customizers still reach only the first
  MON, in argument order, after the module's own request options.
- `cephfs.WithMDSImage` and `rgw.WithImage` still replace their role of the set,
  and a role a requested feature needs but the set omits is still an error.
- `rbdmirror.RunWithImages` and `cephfsmirror.RunWithImages` are separate
  functions and are not changed.

See the [composition rules](../design/value-configuration-composition.md).
