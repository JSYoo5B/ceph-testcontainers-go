package ceph

import "context"

// osdFleet runs the steps of an OSD's lifecycle: registration and storage,
// the launch recipe and start, the removal preflight, drain and purge, and
// the reopen preflight of a detached OSD. It binds the OSD state it changes
// (the handle registry, the memory and volume storage, the device claims) to
// what those steps read: the command transport, the settings, the run
// directory, the startup identity, the cluster facts and the cluster-wide
// launch recipe. It holds no state
// of its own.
//
// Every method needs Container.mu held for the whole call, the lock that
// guards the registry, the storage and the claims, except startOSD and
// startOSDs: they run while the cluster is still being assembled or after the
// launch was prepared under the lock, and read only the launch they are
// given and the storage runner, which is set before the cluster is shared.
// Container keeps what decides whether a lifecycle step may run at all: the
// lifetime flags, the topology lock and the fencing of native calls.
type osdFleet struct {
	conn         cephConn
	osdHandles   *osdRegistry
	stores       *osdStorageState
	deviceClaims *osdDeviceClaims
	runDir       *runDirState
	startup      *startupIdentity
	settings     options
	// owner is the cluster that the handles registerOSD creates belong to;
	// the fleet records it in OSDContainer.owner and calls nothing on it.
	owner        *Container
	facts        func() ClusterFacts
	keyArgs      func(CephxCipher) ([]string, error)
	launchRecipe func(osd *OSDContainer, keyring []byte) *osdLaunchRecipe
	clearSocket  func(ctx context.Context, daemon string) error
}

// osdFleet returns the fleet of the cluster's OSDs. The caller holds c.mu
// while it uses the fleet, except for the start steps described at osdFleet.
func (c *Container) osdFleet() osdFleet {
	return osdFleet{
		conn: c.conn(), osdHandles: &c.osdHandles, stores: &c.stores, deviceClaims: &c.deviceClaims,
		runDir: &c.runDir, startup: &c.startup, settings: c.settings,
		owner: c, facts: c.staticFacts, keyArgs: c.generateKeyArgs, launchRecipe: c.newOSDLaunchRecipe, clearSocket: c.clearAdminSocket,
	}
}
