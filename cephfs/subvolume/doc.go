// Package subvolume prepares the subvolume state of an existing CephFS
// filesystem: subvolume groups and subvolumes, their snapshots and clones,
// principals restricted to one subvolume, and directory pin policies of
// subvolumes and groups.
//
// Create and CreateGroup create owned resources in a *cephfs.Filesystem; the
// listing and metadata functions (Names, GroupNames, Info, GroupInfo,
// SnapshotInfo, AuthorizedClients) read native state by name, including
// resources created outside this package. An operation on an owned resource is
// a method of its handle, which records the native identity it created and
// refuses a replaced resource. These functions act on a filesystem that
// cephfs.Run, cephfs.Start or cephfs.WithFilesystems created; they start no
// container.
package subvolume
