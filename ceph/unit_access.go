package ceph

import (
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
	"github.com/testcontainers/testcontainers-go"
)

// unitAccess is the one seam through which the unit-test helper package
// internal/testhelper reaches unexported cluster state. It is the only place in
// package ceph that links test code to production code: it is registered in
// hookreg.Test, it is not one of the hook values that the rbd, cephfs, rgw,
// fault and override packages assert, and it carries no test-config or
// test-state struct (the helper package defines those and passes plain values).
// Its methods are exported only so that internal/testhelper can call them
// through an interface; the type is unexported and appears in no public
// package. A same-package test needs none of it and uses the fields directly.
//
// Every method takes no lock. The caller holds the mutex that UnitState
// returns, or owns a cluster value that no other goroutine uses yet.
type unitAccess struct{}

func init() {
	hookreg.Test = unitAccess{}
}

// UnitCluster builds a cluster value for the service packages' unit tests.
func (unitAccess) UnitCluster(control testcontainers.Container, startupTimeout time.Duration, closed, hostNetwork bool,
	publicAddress, networkName string, shared bool, osds int, placement OSDConfig, memStore bool,
	mode MessengerMode, images *ImageSet) *Container {
	c := &Container{Container: control, closed: closed, settings: options{
		startupTimeout: startupTimeout, hostNetwork: hostNetwork, publicAddress: publicAddress,
		essentialImage: "unit-all", messengerMode: mode,
	}}
	if memStore {
		c.settings.osdObjectStore = OSDMemStore
	}
	if images != nil {
		set := *images
		c.settings.imageSet = &set
	}
	if shared {
		c.sharing.Store(int32(sharedWithTenants))
	}
	if networkName != "" {
		c.nets.public = &testcontainers.DockerNetwork{Name: networkName}
	}
	for id := range osds {
		c.osdHandles.register(id, &OSDContainer{ID: id, owner: c, Container: control, placement: placement})
	}
	return c
}

// UnitState exposes a cluster's internals to unit tests. It takes no lock: the
// caller holds Mutex or the cluster value is not shared yet.
func (unitAccess) UnitState(c *Container) (mu *sync.Mutex, control, config *sync.RWMutex, portLeases int) {
	return &c.mu, c.controlPlane.mutex(), c.bootstrap.mutex(), c.portLeases.count()
}

// UnitSetClosed sets or clears the closed flag. The caller holds Mutex or the
// cluster value is not shared.
func (unitAccess) UnitSetClosed(c *Container, closed bool) { c.closed = closed }

// UnitSetControlPlane replaces the control-plane container. The caller holds
// ControlMutex or the cluster value is not shared.
func (unitAccess) UnitSetControlPlane(c *Container, control testcontainers.Container) {
	c.controlPlane.handle = control
}

// UnitSetConfig replaces the recorded bootstrap configuration. The caller holds
// ConfigMutex or the cluster value is not shared.
func (unitAccess) UnitSetConfig(c *Container, config []byte) { c.bootstrap.config = config }

// UnitSetKeyring replaces the recorded admin keyring. The caller holds
// ConfigMutex or the cluster value is not shared.
func (unitAccess) UnitSetKeyring(c *Container, keyring []byte) { c.bootstrap.keyring = keyring }

// UnitSetCephVersion replaces the recorded Ceph version. The caller holds
// Mutex or the cluster value is not shared.
func (unitAccess) UnitSetCephVersion(c *Container, version string) {
	c.bootstrap.cephVersion = version
}

// UnitSetNetworkRemoved marks the public bridge as removed or not, so a unit
// test with a synthetic bridge skips the Docker removal. The caller holds Mutex
// or the cluster value is not shared.
func (unitAccess) UnitSetNetworkRemoved(c *Container, removed bool) {
	c.nets.publicRemoved = removed
}

// UnitNetworkRemoved reports whether the public bridge is marked removed. The
// caller holds Mutex or the cluster value is not shared.
func (unitAccess) UnitNetworkRemoved(c *Container) bool { return c.nets.publicRemoved }

// UnitSetInitialOSDs sets the initial OSD count. The caller holds Mutex or the
// cluster value is not shared.
func (unitAccess) UnitSetInitialOSDs(c *Container, osds int) { c.settings.osds = osds }

// UnitSetPoolDefaults sets the replica count and minimum size that pool
// defaults resolve to. The caller holds Mutex or the cluster value is not
// shared.
func (unitAccess) UnitSetPoolDefaults(c *Container, replicas, minSize int) {
	c.settings.poolReplicas, c.settings.poolMinSize = replicas, minSize
}

// UnitSetStartupTimeout sets the startup timeout. The caller holds Mutex or the
// cluster value is not shared.
func (unitAccess) UnitSetStartupTimeout(c *Container, timeout time.Duration) {
	c.settings.startupTimeout = timeout
}

// UnitSetCephFSImage selects the image for the CephFS role. The caller holds
// Mutex or the cluster value is not shared.
func (unitAccess) UnitSetCephFSImage(c *Container, image string) { c.settings.cephfsOverride = image }

// UnitReplaceOSDs swaps the owned OSD descriptors of a cluster value for
// osds, for the service packages' unit tests. It takes no lock: the caller
// owns a cluster no other goroutine uses yet, or holds the mutex that
// UnitState exposes.
func (unitAccess) UnitReplaceOSDs(c *Container, osds map[int]*OSDContainer) {
	c.osdHandles.replace(osds)
}

// UnitOSDCount is the number of OSD descriptors a cluster value owns. The
// caller holds the mutex that UnitState exposes, or owns a cluster no other
// goroutine uses.
func (unitAccess) UnitOSDCount(c *Container) int { return c.osdHandles.count() }

// UnitBackend returns the shared-harness backend value of c, as Start returns
// it, so that ceph/harness can prove that the real types satisfy the
// internal/harnesshook contract without starting Docker.
func (unitAccess) UnitBackend(c *Container) any { return containerBackend{c: c} }

// UnitNewClient builds a client descriptor for a model backend of the shared
// harness's unit tests, which cannot set the private fields.
func (unitAccess) UnitNewClient(name string, created, ready bool) *ClientConfig {
	return &ClientConfig{name: name, created: created, ready: ready}
}

// UnitClientState returns a client identity's owner and lifecycle flags.
func (unitAccess) UnitClientState(client *ClientConfig) (owner *Container, created, ready, revoked bool) {
	return client.owner, client.created, client.ready, client.revoked
}

// UnitApply applies options to empty settings and returns the service
// bootstraps they record, for the service packages' unit tests.
func (unitAccess) UnitApply(opts ...Option) ([]serviceBootstrap, error) {
	var settings options
	for _, opt := range opts {
		if err := opt(&settings); err != nil {
			return nil, err
		}
	}
	return settings.services, nil
}

// UnitPrepare validates an initial composition of osds OSDs and pools with
// opts applied, as Run does, and returns the resulting initial pool list and
// the prepared service bootstraps.
func (unitAccess) UnitPrepare(osds int, noInitialOSDs bool, pools []PoolConfig, opts ...Option) ([]PoolConfig, []serviceBootstrap, error) {
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

// UnitFaults snapshots one family for cross-package contract tests. The
// caller holds the mutex, or the cluster is not shared.
func (unitAccess) UnitFaults(c *Container, kind string) map[string]any {
	k, ok := faultKindNames[kind]
	if !ok {
		return make(map[string]any)
	}
	return c.faults.handles(k)
}

// UnitHeld snapshots the handles that one kind holds, for unit tests. The
// caller holds the mutex, or the cluster is not shared.
func (unitAccess) UnitHeld(c *Container, kind string) map[string]any {
	k, ok := overrideKindNames[kind]
	if !ok {
		return make(map[string]any)
	}
	return c.faults.handles(k)
}
