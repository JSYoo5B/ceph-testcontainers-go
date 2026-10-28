package ceph

import "github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"

// The shared fakes keep their former names in this package's tests.
type (
	poolFixtureContainer = clustertest.CannedCommandContainer
	cephStateFake        = clustertest.StateFake
	FakePool             = clustertest.FakePool
	FakeFilesystem       = clustertest.FakeFilesystem
)

func newCephStateFake() *cephStateFake { return clustertest.NewStateFake() }
