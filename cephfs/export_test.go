package cephfs

import (
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
)

// SyntheticFilesystemForTest returns the registered filesystem "fixture" of a
// synthetic cluster, with its native identity captured as FSMap ID 41, and an
// owned MDS handle of it, for the tests of package cephfs_test, which cannot
// build a Filesystem. remove drops it from the registry, as Remove does after
// the native removal; terminate marks the cluster terminated. The control
// container answers no command: an operation that reaches a native read
// fails with an error that is not ceph.ErrInvalidRequest.
func SyntheticFilesystemForTest(t testing.TB) (fs *Filesystem, mds *MDS, remove, terminate func()) {
	t.Helper()
	ctr := &clustertest.CannedCommandContainer{Output: map[string]string{}}
	cluster := testhelper.TestCluster(ctr, testhelper.TestConfig{StartupTimeout: time.Second, OSDs: 1})
	fs = newFilesystem(cluster, Config{Name: "fixture", ActiveMDS: 1, MetadataPool: ceph.PoolConfig{Name: "metadata"}, DataPool: ceph.PoolConfig{Name: "data"}}, nil)
	fs.base().ReplaceNative(&fsbase.NativeIdentity{ID: 41, MetadataPool: 1, DefaultPool: 2, Attachments: make(map[string]*fsbase.PoolAttachment)})
	filesystems(cluster).byName = map[string]*Filesystem{"fixture": fs}
	mds = &MDS{ID: "a", FilesystemName: "fixture"}
	mds.identity = &cephFSMDSIdentity{fleet: fs.fleet(), native: fs.base().Native(), descriptor: mds, name: "a"}
	remove = func() { delete(filesystems(cluster).byName, "fixture") }
	terminate = func() { testhelper.TestSetClosed(cluster, true) }
	return fs, mds, remove, terminate
}
