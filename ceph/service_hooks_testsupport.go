//go:build test_support

package ceph

import (
	"sync"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// This file is compiled only with the test_support build tag. It holds the
// synthetic-cluster constructor, the privileged state accessors and the
// matching hook methods that the service packages' unit tests use. An ordinary
// build does not contain them; docs/contributing/testing.md describes the
// selection and the exclusion check.

// testClusterConfig selects the settings of a unit-test cluster value.
type testClusterConfig = struct {
	StartupTimeout time.Duration
	Closed         bool
	HostNetwork    bool
	PublicAddress  string
	NetworkName    string
	Shared         bool
	// OSDs adds owned OSD descriptors 0..OSDs-1 whose container is control
	// and whose CRUSH location is OSDPlacement.
	OSDs         int
	OSDPlacement OSDConfig
	// MemStore selects the OSDMemStore object store instead of BlueStore.
	MemStore bool
	// MessengerMode selects the bootstrap messenger mode.
	MessengerMode MessengerMode
	// Images selects the role images; nil means one aggregate image.
	Images *ImageSet
}

// TestCluster builds a cluster value for the service packages' unit tests.
func (serviceHooks) TestCluster(control testcontainers.Container, config testClusterConfig) *Container {
	c := &Container{Container: control, closed: config.Closed, settings: options{
		startupTimeout: config.StartupTimeout, hostNetwork: config.HostNetwork, publicAddress: config.PublicAddress,
		essentialImage: "unit-all", messengerMode: config.MessengerMode,
	}}
	if config.MemStore {
		c.settings.osdObjectStore = OSDMemStore
	}
	if config.Images != nil {
		images := *config.Images
		c.settings.imageSet = &images
	}
	if config.Shared {
		c.sharing.Store(int32(sharedWithTenants))
	}
	if config.NetworkName != "" {
		c.nets.public = &testcontainers.DockerNetwork{Name: config.NetworkName}
	}
	if config.OSDs > 0 {
		for id := range config.OSDs {
			c.osdHandles.register(id, &OSDContainer{ID: id, owner: c, Container: control, placement: config.OSDPlacement})
		}
	}
	return c
}

// testState points at a cluster's internals for the service packages' unit
// tests. PortLeases is a count taken when TestState is called. It publishes
// the three lock identities and no pointer to the closed flag, the control
// plane, the options or the bootstrap record: those are written through the
// TestSet* hooks below (see the membership/control section of
// docs/design/container-decomposition.md).
type testState = struct {
	Mutex          *sync.Mutex
	ControlMutex   *sync.RWMutex
	ConfigMutex    *sync.RWMutex
	PortLeases     int
	NetworkRemoved *bool
}

// TestState exposes a cluster's internals to unit tests. It takes no lock: the
// caller holds Mutex or the cluster value is not shared yet.
func (serviceHooks) TestState(c *Container) testState {
	return testState{
		Mutex: &c.mu, ControlMutex: c.controlPlane.mutex(), ConfigMutex: c.bootstrap.mutex(),
		PortLeases: c.portLeases.count(), NetworkRemoved: &c.nets.publicRemoved,
	}
}

// The TestSet* hooks write one membership or control value of a cluster for the
// service packages' unit tests. They replace the pointers that TestState used to
// publish. None takes a lock: the caller holds the mutex that TestState exposes
// (Mutex for the closed flag and the options, ControlMutex for the control
// plane) or the cluster value is not shared yet.

// TestSetClosed sets or clears the closed flag. The caller holds Mutex or the
// cluster value is not shared.
func (serviceHooks) TestSetClosed(c *Container, closed bool) { c.closed = closed }

// TestSetControlPlane replaces the control-plane container. The caller holds
// ControlMutex or the cluster value is not shared.
func (serviceHooks) TestSetControlPlane(c *Container, control testcontainers.Container) {
	c.controlPlane.handle = control
}

// TestSetConfig replaces the recorded bootstrap configuration. The caller holds
// ConfigMutex or the cluster value is not shared.
func (serviceHooks) TestSetConfig(c *Container, config []byte) { c.bootstrap.config = config }

// TestSetKeyring replaces the recorded admin keyring. The caller holds
// ConfigMutex or the cluster value is not shared.
func (serviceHooks) TestSetKeyring(c *Container, keyring []byte) { c.bootstrap.keyring = keyring }

// TestSetCephVersion replaces the recorded Ceph version. The caller holds
// Mutex or the cluster value is not shared.
func (serviceHooks) TestSetCephVersion(c *Container, version string) {
	c.bootstrap.cephVersion = version
}

// TestSetInitialOSDs sets the initial OSD count. The caller holds Mutex or the
// cluster value is not shared.
func (serviceHooks) TestSetInitialOSDs(c *Container, osds int) { c.settings.osds = osds }

// TestSetPoolDefaults sets the replica count and minimum size that pool
// defaults resolve to. The caller holds Mutex or the cluster value is not
// shared.
func (serviceHooks) TestSetPoolDefaults(c *Container, replicas, minSize int) {
	c.settings.poolReplicas, c.settings.poolMinSize = replicas, minSize
}

// TestSetStartupTimeout sets the startup timeout. The caller holds Mutex or the
// cluster value is not shared.
func (serviceHooks) TestSetStartupTimeout(c *Container, timeout time.Duration) {
	c.settings.startupTimeout = timeout
}

// TestSetCephFSImage selects the image for the CephFS role. The caller holds
// Mutex or the cluster value is not shared.
func (serviceHooks) TestSetCephFSImage(c *Container, image string) { c.settings.cephfsOverride = image }

// TestReplaceOSDs swaps the owned OSD descriptors of a cluster value for
// osds, for the service packages' unit tests. It takes no lock: the caller
// owns a cluster no other goroutine uses yet, or holds the mutex that
// TestState exposes.
func (serviceHooks) TestReplaceOSDs(c *Container, osds map[int]*OSDContainer) {
	c.osdHandles.replace(osds)
}

// TestOSDCount is the number of OSD descriptors a cluster value owns. The
// caller holds the mutex that TestState exposes, or owns a cluster no other
// goroutine uses.
func (serviceHooks) TestOSDCount(c *Container) int { return c.osdHandles.count() }

// TestClientState returns a client identity's owner and lifecycle flags.
func (serviceHooks) TestClientState(client *ClientConfig) (owner *Container, created, ready, revoked bool) {
	return client.owner, client.created, client.ready, client.revoked
}

// TestApply applies options to empty settings and returns the service
// bootstraps they record, for the service packages' unit tests.
func (serviceHooks) TestApply(opts ...Option) ([]serviceBootstrap, error) {
	var settings options
	for _, opt := range opts {
		if err := opt(&settings); err != nil {
			return nil, err
		}
	}
	return settings.services, nil
}

// TestPrepare validates an initial composition of osds OSDs and pools with
// opts applied, as Run does, and returns the resulting initial pool list and
// the prepared service bootstraps.
func (serviceHooks) TestPrepare(osds int, noInitialOSDs bool, pools []PoolConfig, opts ...Option) ([]PoolConfig, []serviceBootstrap, error) {
	settings := options{osds: osds, noInitialOSDs: noInitialOSDs, pools: pools}
	for _, opt := range opts {
		if err := opt(&settings); err != nil {
			return nil, nil, err
		}
	}
	if err := prepareInitialComposition(&settings); err != nil {
		return nil, nil, err
	}
	return settings.pools, settings.services, nil
}
