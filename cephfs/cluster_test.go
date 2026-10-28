package cephfs

import (
	"context"
	"errors"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/testcontainers/testcontainers-go"
)

// The shared CLI fakes under the names that the ceph package tests use.
type (
	poolFixtureContainer = clustertest.CLI
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

// assertSnapshotReadLockQueued holds mutex as a writer and checks that
// operation waits for it, returns the caller's deadline and leaves the gate
// to the writer.
func assertSnapshotReadLockQueued(t *testing.T, mutex *sync.RWMutex, operation func(context.Context) error) {
	t.Helper()
	mutex.Lock()
	locked := true
	defer func() {
		if locked {
			mutex.Unlock()
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- operation(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("queued snapshot lost caller deadline: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		mutex.Unlock()
		locked = false
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		t.Fatal("snapshot remained queued after caller deadline")
	}
	if mutex.TryRLock() {
		mutex.RUnlock()
		t.Fatal("snapshot unlocked the busy writer gate")
	}
	mutex.Unlock()
	locked = false
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

func clientOwner(client *ceph.ClientConfig) *ceph.Container {
	owner, _, _, _ := testhelper.TestClientState(client)
	return owner
}

func clientCreated(client *ceph.ClientConfig) bool {
	_, created, _, _ := testhelper.TestClientState(client)
	return created
}

func clientReady(client *ceph.ClientConfig) bool {
	_, _, ready, _ := testhelper.TestClientState(client)
	return ready
}

func clientRevoked(client *ceph.ClientConfig) bool { return servicehook.ClientRevoked(client) }

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
