package cephfs

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fstest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/testcontainers/testcontainers-go"
)

// The shared CLI fakes under the names that the ceph package tests use.
type (
	poolFixtureContainer = clustertest.CannedCommandContainer
	cephStateFake        = clustertest.StateFake
)

func newCephStateFake() *cephStateFake { return clustertest.NewStateFake() }

// poolFixtureCluster builds a cluster value with count owned OSDs whose
// control and OSD containers are ctr.
func poolFixtureCluster(ctr testcontainers.Container, count int) *ceph.Container {
	return testhelper.TestCluster(ctr, testhelper.TestConfig{StartupTimeout: time.Second, OSDs: count})
}

func assertContextLockQueued(t *testing.T, mutex *sync.Mutex, operation func(context.Context) error) {
	t.Helper()
	clustertest.AssertContextLockQueued(t, mutex, operation)
}

// filesystemFixture returns the registered filesystem "fixture" of the shared
// synthetic output, with its native identity captured.
func filesystemFixture() (*Filesystem, *poolFixtureContainer) {
	ctr := &poolFixtureContainer{Output: fstest.Output()}
	cluster := poolFixtureCluster(ctr, 2)
	fs := newFilesystem(cluster, Config{Name: "fixture", MetadataPool: ceph.PoolConfig{Name: "metadata"}, DataPool: ceph.PoolConfig{Name: "data"}, AdditionalDataPools: []ceph.PoolConfig{{Name: "additional"}}}, nil)
	fs.base().ReplaceNative(&fsbase.NativeIdentity{ID: 41, MetadataPool: 1, DefaultPool: 2, Attachments: make(map[string]*fsbase.PoolAttachment)})
	filesystems(cluster).byName = map[string]*Filesystem{"fixture": fs}
	return fs, ctr
}

// setServices replaces the service containers the cluster owns.
func setServices(c *ceph.Container, services map[string]testcontainers.Container) {
	for name := range servicehook.Services(c) {
		servicehook.DeleteService(c, name)
	}
	for name, ctr := range services {
		servicehook.SetService(c, name, ctr)
	}
}

// preparedFilesystems returns the filesystem configurations that prepared
// service bootstraps would create.
func preparedFilesystems(services []servicehook.Bootstrap) []Config {
	for _, service := range services {
		if b, ok := service.(*cephFSBootstrap); ok {
			return b.configs
		}
	}
	return nil
}
