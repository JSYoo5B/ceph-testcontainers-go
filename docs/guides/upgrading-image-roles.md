# Selecting images by role

The module selects images by the dependency roles of the image contract:
essential, client, cephfs, rbd-aux and rgw, plus the explicit aggregate all.
This page lists where each role is chosen. Old spellings are removed without
aliases; use the names below.

| You need | Use |
| --- | --- |
| The image that runs the MONs, MGRs and OSDs | `cluster.EssentialImage()` or `cluster.Images().Essential` |
| An image for your own container that runs `ceph`, `rados`, `rbd` or Python bindings | `cluster.Images().Client` |
| The `rbd-mirror` daemon image | `destination.Images().RBDAux`, or `rbdmirror.RunWithImages` |
| The `cephfs-mirror` daemon image | `source.Images().CephFS`, or `cephfsmirror.RunWithImages` |
| The RGW gateway image of `multisite.Run` and `multisite.RunTopology` | `cluster.Images().RGW` |
| `rados` or `rbd` inside the cluster | `cluster.ClientContainer(ctx)` |
| `ceph` and `ceph-authtool` inside the cluster | `cluster.Ceph(ctx, ...)` or `cluster.ControlContainer(ctx)` |

`ControlContainer` is the executor created from the essential image. It is not
a role. `cephfs.WithMDSImage` and `rgw.WithImage` set the cephfs and rgw roles
of the whole cluster and replace those roles of a `WithImages` set whichever
option comes first.

The mirror `Run` functions refuse an image that a cluster selects for another
role, for example its essential image as the `rbd-mirror` daemon image, when
that cluster has a separate rbd-aux image. An aggregate selection, where one
image serves every role, is accepted.

`ClusterFacts` no longer has a separate essential-image field: read
`Images.Essential`.
