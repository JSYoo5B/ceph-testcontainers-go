// Package testhelper gives unit tests of the ceph, rbd, cephfs, rgw, fault,
// override and internal packages the synthetic cluster constructor, the
// privileged state accessors and the registry snapshots they need. Only
// _test.go files import it: internal/tools/testsupportcheck fails when a
// non-test file does. The rest of the module has no Test* function or method
// and no test-config or test-state struct in a non-test file.
//
// The package reaches unexported cluster state through one registered seam,
// hookreg.Test, which package ceph sets to its unitAccess value (ceph/unit_access.go).
// That value is not one of the hook values that production packages assert, and
// no other code links the helper to production internals.
package testhelper

import (
	"sync"
	"time"

	"github.com/testcontainers/testcontainers-go"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// unitHooks is the seam that package ceph registers in hookreg.Test.
type unitHooks interface {
	UnitCluster(control testcontainers.Container, startupTimeout time.Duration, closed, hostNetwork bool,
		publicAddress, networkName string, shared bool, osds int, placement ceph.OSDConfig, memStore bool,
		mode ceph.MessengerMode, images *ceph.ImageSet) *ceph.Container
	UnitState(c *ceph.Container) (mu *sync.Mutex, control, config *sync.RWMutex, portLeases int)
	UnitSetClosed(c *ceph.Container, closed bool)
	UnitSetControlPlane(c *ceph.Container, control testcontainers.Container)
	UnitSetConfig(c *ceph.Container, config []byte)
	UnitSetKeyring(c *ceph.Container, keyring []byte)
	UnitSetCephVersion(c *ceph.Container, version string)
	UnitSetNetworkRemoved(c *ceph.Container, removed bool)
	UnitNetworkRemoved(c *ceph.Container) bool
	UnitSetInitialOSDs(c *ceph.Container, osds int)
	UnitSetPoolDefaults(c *ceph.Container, replicas, minSize int)
	UnitSetStartupTimeout(c *ceph.Container, timeout time.Duration)
	UnitSetCephFSImage(c *ceph.Container, image string)
	UnitReplaceOSDs(c *ceph.Container, osds map[int]*ceph.OSDContainer)
	UnitOSDCount(c *ceph.Container) int
	UnitClientState(client *ceph.ClientConfig) (owner *ceph.Container, created, ready, revoked bool)
	UnitApply(opts ...ceph.Option) ([]servicehook.Bootstrap, error)
	UnitPrepare(osds int, noInitialOSDs bool, pools []ceph.PoolConfig, opts ...ceph.Option) ([]ceph.PoolConfig, []servicehook.Bootstrap, error)
	UnitFaults(c *ceph.Container, kind string) map[string]any
	UnitHeld(c *ceph.Container, kind string) map[string]any
}

func get() unitHooks {
	h, ok := hookreg.Test.(unitHooks)
	if !ok {
		panic("testhelper: package ceph did not register its unit-test access")
	}
	return h
}

// TestConfig selects the settings of a unit-test cluster value.
type TestConfig struct {
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
type TestStateView struct {
	Mutex        *sync.Mutex
	ControlMutex *sync.RWMutex
	ConfigMutex  *sync.RWMutex
	PortLeases   int
}

// TestCluster builds a cluster value for unit tests: control runs the CLI.
func TestCluster(control testcontainers.Container, c TestConfig) *ceph.Container {
	return get().UnitCluster(control, c.StartupTimeout, c.Closed, c.HostNetwork, c.PublicAddress, c.NetworkName,
		c.Shared, c.OSDs, c.OSDPlacement, c.MemStore, c.MessengerMode, c.Images)
}

// TestState exposes a cluster's lock identities to unit tests.
func TestState(c *ceph.Container) TestStateView {
	mu, control, config, leases := get().UnitState(c)
	return TestStateView{Mutex: mu, ControlMutex: control, ConfigMutex: config, PortLeases: leases}
}

// TestReplaceOSDs swaps the owned OSD descriptors of a cluster value for osds.
// It takes no lock: use it on a cluster no other goroutine uses, or while
// holding the mutex that TestState exposes.
func TestReplaceOSDs(c *ceph.Container, osds map[int]*ceph.OSDContainer) {
	get().UnitReplaceOSDs(c, osds)
}

// TestOSDCount is the number of OSD descriptors the cluster value owns, read
// without a lock.
func TestOSDCount(c *ceph.Container) int { return get().UnitOSDCount(c) }

// TestSetClosed sets or clears a cluster's closed flag. It takes no lock: hold
// the mutex that TestState exposes, or use a cluster no other goroutine uses.
func TestSetClosed(c *ceph.Container, closed bool) { get().UnitSetClosed(c, closed) }

// TestSetControlPlane replaces a cluster's control-plane container. It takes no
// lock: hold ControlMutex, or use a cluster no other goroutine uses.
func TestSetControlPlane(c *ceph.Container, control testcontainers.Container) {
	get().UnitSetControlPlane(c, control)
}

// TestSetConfig replaces a cluster's recorded bootstrap configuration. It takes
// no lock: hold ConfigMutex, or use a cluster no other goroutine uses.
func TestSetConfig(c *ceph.Container, config []byte) { get().UnitSetConfig(c, config) }

// TestSetKeyring replaces a cluster's recorded admin keyring. It takes no lock:
// hold ConfigMutex, or use a cluster no other goroutine uses.
func TestSetKeyring(c *ceph.Container, keyring []byte) { get().UnitSetKeyring(c, keyring) }

// TestSetCephVersion replaces a cluster's recorded Ceph version. It takes no
// lock: use a cluster no other goroutine uses, or hold the mutex that TestState
// exposes.
func TestSetCephVersion(c *ceph.Container, version string) { get().UnitSetCephVersion(c, version) }

// TestSetNetworkRemoved marks a cluster's public bridge as removed or not. It
// takes no lock: hold the mutex that TestState exposes, or use a cluster no
// other goroutine uses.
func TestSetNetworkRemoved(c *ceph.Container, removed bool) { get().UnitSetNetworkRemoved(c, removed) }

// TestNetworkRemoved reports whether a cluster's public bridge is marked
// removed, read without a lock.
func TestNetworkRemoved(c *ceph.Container) bool { return get().UnitNetworkRemoved(c) }

// TestSetInitialOSDs sets a cluster's initial OSD count. It takes no lock.
func TestSetInitialOSDs(c *ceph.Container, osds int) { get().UnitSetInitialOSDs(c, osds) }

// TestSetPoolDefaults sets the replica count and minimum size that pool
// defaults resolve to. It takes no lock.
func TestSetPoolDefaults(c *ceph.Container, replicas, minSize int) {
	get().UnitSetPoolDefaults(c, replicas, minSize)
}

// TestSetStartupTimeout sets a cluster's startup timeout. It takes no lock.
func TestSetStartupTimeout(c *ceph.Container, timeout time.Duration) {
	get().UnitSetStartupTimeout(c, timeout)
}

// TestSetCephFSImage selects the image of the CephFS role. It takes no lock.
func TestSetCephFSImage(c *ceph.Container, image string) { get().UnitSetCephFSImage(c, image) }

// TestClientState returns a client identity's owner and lifecycle flags.
func TestClientState(client *ceph.ClientConfig) (owner *ceph.Container, created, ready, revoked bool) {
	return get().UnitClientState(client)
}

// TestTenantCluster returns the shared cluster of a live tenant, or nil. Its
// gate refuses the methods that a tenant cannot use. The service adapters use
// servicehook.Tenant and never this.
func TestTenantCluster(t *harness.Tenant) *ceph.Container {
	h, ok := hookreg.HarnessTenant.(interface {
		TenantCluster(t *harness.Tenant) *ceph.Container
	})
	if !ok {
		panic("testhelper: package ceph/harness did not register its tenant hooks")
	}
	return h.TenantCluster(t)
}

// TestApply applies options to empty settings and returns the service
// bootstraps they record.
func TestApply(opts ...ceph.Option) ([]servicehook.Bootstrap, error) { return get().UnitApply(opts...) }

// TestPrepare validates an initial composition of osds OSDs and pools with
// opts applied, as Run does, and returns the resulting initial pool list and
// the prepared service bootstraps.
func TestPrepare(osds int, noInitialOSDs bool, pools []ceph.PoolConfig, opts ...ceph.Option) ([]ceph.PoolConfig, []servicehook.Bootstrap, error) {
	return get().UnitPrepare(osds, noInitialOSDs, pools, opts...)
}

// TestFaults snapshots the fault registry for unit tests. Call with the cluster
// mutex held.
func TestFaults(c *ceph.Container, kind faulthook.Kind) map[string]any {
	return get().UnitFaults(c, string(kind))
}

// TestHeld snapshots the handles that one override kind holds, for unit tests.
// Call with the cluster mutex held.
func TestHeld(c *ceph.Container, kind overridehook.Kind) map[string]any {
	return get().UnitHeld(c, string(kind))
}
