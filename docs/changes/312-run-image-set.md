---
change: 312-run-image-set
kind: api-break
migration: docs/guides/upgrading-run-image-set.md
---
`ceph.RunWithImages` and `ceph.WithImages` are removed. Role images are selected with `ceph.RunOptions{}.WithImageSet(set)`, a configuration value that `Run` (and `rbd.Run`, `cephfs.Run`, `rgw.Run`) recognises among its existing customizers, the way it recognises a `ceph.Option`; the `Run` signatures are unchanged. When an image set is supplied the image argument is not a second input: it must be empty or equal to the set's essential image, and any other value is refused with `ErrInvalidRequest` before anything is created. At most one `RunOptions` value may select a set; a missing role that a requested feature needs is still refused with no fallback to Essential. There is no deprecated alias or forwarding layer.
