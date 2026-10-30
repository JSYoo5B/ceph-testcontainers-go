package ceph

import (
	"context"
	"strconv"

	"github.com/testcontainers/testcontainers-go"
)

// osdFleet runs the steps of an OSD's lifecycle: registration and storage,
// the launch recipe and start, the removal preflight, drain and purge, and
// the reopen preflight of a detached OSD. It owns the OSD state those steps
// change (osdFleetState: the handle registry, the memory and volume storage,
// the device claims), held in Container.fleet, and binds it to what the steps
// read: the command transport, the settings, the run directory, the startup
// identity, the cluster facts and the cluster-wide launch recipe.
//
// Every method needs Container.mu held for the whole call, the lock that
// guards the registry, the storage and the claims, except startOSD and
// startOSDs: they run while the cluster is still being assembled or after the
// launch was prepared under the lock, and read only the launch they are
// given and the storage runner, which is set before the cluster is shared.
// Container keeps what decides whether a lifecycle step may run at all: the
// lifetime flags, the topology lock and the fencing of native calls.
type osdFleet struct {
	*osdFleetState
	conn     cephConn
	runDir   *runDirState
	startup  *startupIdentity
	settings options
	// owner is the cluster that the handles registerOSD creates belong to;
	// the fleet records it in OSDContainer.owner and calls nothing on it.
	owner        *Container
	facts        func() ClusterFacts
	keyArgs      func(CephxCipher) ([]string, error)
	launchRecipe func(osd *OSDContainer, keyring []byte) *osdLaunchRecipe
	clearSocket  func(ctx context.Context, daemon string) error
}

// osdFleetState is the OSD state that osdFleet owns: the handle registry,
// the memory and volume storage and the device claims. Container holds it as
// the field fleet, whose whole value only osdFleet's constructor names
// (TestOwnerStatesAreNamedOnlyByTheirConstructors); Container.mu guards all
// of it.
type osdFleetState struct {
	// osdHandles is the OSD handle registry, owned and detached; see
	// osdRegistry.
	osdHandles osdRegistry
	// stores is the OSD memory and volume storage and its test seams; see
	// osdStorageState.
	stores osdStorageState
	// deviceClaims is the cluster-wide claim table of the host block devices
	// of OSDs; see osdDeviceClaims. Nothing publishes a claim yet.
	deviceClaims osdDeviceClaims
}

// osdFleet returns the fleet of the cluster's OSDs. The caller holds c.mu
// while it uses the fleet, except for the start steps described at osdFleet.
func (c *Container) osdFleet() osdFleet {
	return osdFleet{
		osdFleetState: &c.fleet, conn: c.conn(),
		runDir: &c.runDir, startup: &c.startup, settings: c.settings,
		owner: c, facts: c.staticFacts, keyArgs: c.generateKeyArgs, launchRecipe: c.newOSDLaunchRecipe, clearSocket: c.clearAdminSocket,
	}
}

// diagnosticOSDs passes each OSD container to add, for a diagnostics
// snapshot. The caller holds Container.mu.
func (f osdFleet) diagnosticOSDs(add func(role, name string, ctr testcontainers.Container)) {
	for id, osd := range f.osdHandles.each() {
		if osd != nil {
			add("osd", strconv.Itoa(id), osd.Container)
		}
	}
}
