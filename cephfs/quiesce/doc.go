// Package quiesce pauses the I/O of owned CephFS subvolumes as one native
// quiesce set, so that a test can take a consistent checkpoint while the
// application under test keeps its clients connected.
//
// Start creates the set over subvolumes that cephfs/subvolume created in an
// existing *cephfs.Filesystem; the returned Set observes and releases it. It
// starts no container and pauses no process.
package quiesce
