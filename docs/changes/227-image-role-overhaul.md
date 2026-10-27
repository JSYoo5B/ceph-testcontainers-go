---
change: 227-image-role-overhaul
kind: api-break
migration: docs/guides/upgrading-image-roles.md
---
`ClusterFacts` carries the essential image only as `Images.Essential`. Code that passed the essential image to a client, mirror daemon or gateway must select that role from `ClusterFacts.Images` (`Client`, `RBDAux`, `CephFS`, `RGW`). `cephfs.WithMDSImage` and `rgw.WithImage` now replace their role of a `WithImages` set whichever option comes first. `rbdmirror.Run`, `cephfsmirror.Run` and the multisite `Run` functions refuse an image that a split-role cluster selects for another role. Guides, API pages and comments describe the executor of each operation by its current role.
