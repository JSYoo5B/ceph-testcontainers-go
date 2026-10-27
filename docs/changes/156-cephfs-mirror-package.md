---
change: 156-cephfs-mirror-package
kind: api-break
migration: docs/guides/upgrading-cephfs-mirror-package.md
---
Move the CephFS mirror fixture from `cephfs` to the service-specific package
`cephfs/mirror`. `cephfs.RunMirror` and `cephfs.RunMirrorWithImages` become
`Run` and `RunWithImages`, `cephfs.Mirror` keeps its name, and the `Mirror`
prefix is dropped from the other exported types (`Config`, `Daemon`,
`DirectoryAddition`, `DirectoryRemoval`, `PeerRemoval`, `Snapshot`,
`DirectoryStatus` and the status types). Methods, embedded container, error
kinds, identity checks, locking, cancellation and cleanup are unchanged; there
is no deprecated alias or forwarding layer.
