package servicehook

import (
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
	"github.com/testcontainers/testcontainers-go"
)

// testHooks is the synthetic-cluster and test-access part of the registered
// hooks. It is asserted separately from hooks, and only when a Test* function
// runs, so an ordinary Lock or Admit call does not depend on it.
type testHooks interface {
	TestCluster(control testcontainers.Container, config TestConfig) *ceph.Container
	TestState(c *ceph.Container) TestStateView
	TestReplaceOSDs(c *ceph.Container, osds map[int]*ceph.OSDContainer)
	TestOSDCount(c *ceph.Container) int
	TestSetClosed(c *ceph.Container, closed bool)
	TestSetControlPlane(c *ceph.Container, control testcontainers.Container)
	TestSetConfig(c *ceph.Container, config []byte)
	TestSetKeyring(c *ceph.Container, keyring []byte)
	TestSetCephVersion(c *ceph.Container, version string)
	TestSetNetworkRemoved(c *ceph.Container, removed bool)
	TestNetworkRemoved(c *ceph.Container) bool
	TestSetInitialOSDs(c *ceph.Container, osds int)
	TestSetPoolDefaults(c *ceph.Container, replicas, minSize int)
	TestSetStartupTimeout(c *ceph.Container, timeout time.Duration)
	TestSetCephFSImage(c *ceph.Container, image string)
	TestClientState(client *ceph.ClientConfig) (owner *ceph.Container, created, ready, revoked bool)
	TestApply(opts ...ceph.Option) ([]Bootstrap, error)
	TestPrepare(osds int, noInitialOSDs bool, pools []ceph.PoolConfig, opts ...ceph.Option) ([]ceph.PoolConfig, []Bootstrap, error)
}

func getTest() testHooks {
	h, ok := hookreg.Value.(testHooks)
	if !ok {
		panic("servicehook: package ceph did not register its test-access hooks")
	}
	return h
}

// TestConfig selects the settings of a unit-test cluster value.
type TestConfig = struct {
	StartupTimeout time.Duration
	Closed         bool
	HostNetwork    bool
	PublicAddress  string
	NetworkName    string
	Shared         bool
	// OSDs adds owned OSD descriptors 0..OSDs-1 whose container is control
	// and whose CRUSH location is OSDPlacement.
	OSDs         int
	OSDPlacement ceph.OSDConfig
	// MemStore selects the OSDMemStore object store instead of BlueStore.
	MemStore bool
	// MessengerMode selects the bootstrap messenger mode.
	MessengerMode ceph.MessengerMode
	// Images selects the role images; nil means one aggregate image.
	Images *ceph.ImageSet
}

// TestStateView points at a cluster's locks for unit tests. PortLeases is a
// count taken when TestState is called. The closed flag, the control plane, the
// options, the bootstrap record and the bridge-removal flag are written through the TestSet* functions.
type TestStateView = struct {
	Mutex        *sync.Mutex
	ControlMutex *sync.RWMutex
	ConfigMutex  *sync.RWMutex
	PortLeases   int
}

// TestCluster builds a cluster value for unit tests: control runs the CLI.
func TestCluster(control testcontainers.Container, config TestConfig) *ceph.Container {
	return getTest().TestCluster(control, config)
}

// TestState exposes a cluster's internals to unit tests.
func TestState(c *ceph.Container) TestStateView { return getTest().TestState(c) }

// TestReplaceOSDs swaps the owned OSD descriptors of a cluster value for osds.
// It takes no lock: use it on a cluster no other goroutine uses, or while
// holding the mutex that TestState exposes.
func TestReplaceOSDs(c *ceph.Container, osds map[int]*ceph.OSDContainer) {
	getTest().TestReplaceOSDs(c, osds)
}

// TestOSDCount is the number of OSD descriptors the cluster value owns, read
// without a lock.
func TestOSDCount(c *ceph.Container) int { return getTest().TestOSDCount(c) }

// TestSetClosed sets or clears a cluster's closed flag. It takes no lock: hold
// the mutex that TestState exposes, or use a cluster no other goroutine uses.
func TestSetClosed(c *ceph.Container, closed bool) { getTest().TestSetClosed(c, closed) }

// TestSetControlPlane replaces a cluster's control-plane container. It takes no
// lock: hold ControlMutex, or use a cluster no other goroutine uses.
func TestSetControlPlane(c *ceph.Container, control testcontainers.Container) {
	getTest().TestSetControlPlane(c, control)
}

// TestSetConfig replaces a cluster's recorded bootstrap configuration. It takes
// no lock: hold ConfigMutex, or use a cluster no other goroutine uses.
func TestSetConfig(c *ceph.Container, config []byte) { getTest().TestSetConfig(c, config) }

// TestSetKeyring replaces a cluster's recorded admin keyring. It takes no lock:
// hold ConfigMutex, or use a cluster no other goroutine uses.
func TestSetKeyring(c *ceph.Container, keyring []byte) { getTest().TestSetKeyring(c, keyring) }

// TestSetCephVersion replaces a cluster's recorded Ceph version. It takes no
// lock: use a cluster no other goroutine uses, or hold the mutex that TestState
// exposes.
func TestSetCephVersion(c *ceph.Container, version string) { getTest().TestSetCephVersion(c, version) }

// TestSetNetworkRemoved marks a cluster's public bridge as removed or not. It
// takes no lock: hold the mutex that TestState exposes, or use a cluster no
// other goroutine uses.
func TestSetNetworkRemoved(c *ceph.Container, removed bool) {
	getTest().TestSetNetworkRemoved(c, removed)
}

// TestNetworkRemoved reports whether a cluster's public bridge is marked
// removed, read without a lock.
func TestNetworkRemoved(c *ceph.Container) bool { return getTest().TestNetworkRemoved(c) }

// TestSetInitialOSDs sets a cluster's initial OSD count. It takes no lock.
func TestSetInitialOSDs(c *ceph.Container, osds int) { getTest().TestSetInitialOSDs(c, osds) }

// TestSetPoolDefaults sets the replica count and minimum size that pool
// defaults resolve to. It takes no lock.
func TestSetPoolDefaults(c *ceph.Container, replicas, minSize int) {
	getTest().TestSetPoolDefaults(c, replicas, minSize)
}

// TestSetStartupTimeout sets a cluster's startup timeout. It takes no lock.
func TestSetStartupTimeout(c *ceph.Container, timeout time.Duration) {
	getTest().TestSetStartupTimeout(c, timeout)
}

// TestSetCephFSImage selects the image of the CephFS role. It takes no lock.
func TestSetCephFSImage(c *ceph.Container, image string) { getTest().TestSetCephFSImage(c, image) }

// TestClientState returns a client identity's owner and lifecycle flags.
func TestClientState(client *ceph.ClientConfig) (owner *ceph.Container, created, ready, revoked bool) {
	return getTest().TestClientState(client)
}

// TestApply applies options to empty settings and returns the service
// bootstraps they record.
func TestApply(opts ...ceph.Option) ([]Bootstrap, error) { return getTest().TestApply(opts...) }

// TestPrepare validates an initial composition of osds OSDs and pools with
// opts applied, as Run does, and returns the resulting initial pool list and
// the prepared service bootstraps.
func TestPrepare(osds int, noInitialOSDs bool, pools []ceph.PoolConfig, opts ...ceph.Option) ([]ceph.PoolConfig, []Bootstrap, error) {
	return getTest().TestPrepare(osds, noInitialOSDs, pools, opts...)
}
