---
change: 312-image-options
kind: api-break
migration: docs/guides/upgrading-image-options.md
---
`ceph.RunWithImages` and `ceph.WithImages` are removed. Role images are selected with `ceph.ImageOptions{}.WithImageSet(set)`, a plain chainable value that enters `Run` (and `rbd.Run`, `cephfs.Run`, `rgw.Run`) through one grouped option function, `ceph.WithImageOptions`, which `Run` recognises like any `ceph.Option`; the `Run` signatures are unchanged. When an image set is supplied the image argument is not a second input: it must be empty or equal to the set's essential image, and any other value is refused with `ErrInvalidRequest` before anything is created. At most one `WithImageOptions` may select a set; a missing role that a requested feature needs is still refused with no fallback to Essential. There is no deprecated alias or forwarding layer.
