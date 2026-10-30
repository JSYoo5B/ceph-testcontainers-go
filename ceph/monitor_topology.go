package ceph

import (
	"context"

	"github.com/testcontainers/testcontainers-go"
)

// monitorTopology owns the cluster's MON and MGR membership: the embedded
// first MON and whether it was terminated, and the roster of the added MONs,
// the MGR candidates and the services. Its methods are the membership views
// (the current MONs, whether a daemon is owned, the targets of a monitor
// configuration refresh), the refresh itself, the start of a MGR, and the
// steps of removing a MON or a MGR. It binds that state to what those steps
// read: the command transport, the settings, the bootstrap record, the OSD
// fleet, the feature client, the service extensions' configuration targets
// and the daemon runner. It holds no state of its own.
//
// Every method needs Container.mu held for the whole call. Container keeps
// what decides whether a step may run at all: the cluster's closed flag, the
// topology lock and the admission of a handle's name.
type monitorTopology struct {
	conn cephConn
	// first is the embedded first MON, which does not change after start.
	first             testcontainers.Container
	monitorTerminated *bool
	roster            *memberRoster
	bootstrap         *bootstrapRecord
	osds              osdFleet
	settings          options
	// owner is the cluster that the handles the topology creates belong to;
	// the topology records it in their owner field and calls nothing on it.
	owner          *Container
	client         func(context.Context) (testcontainers.Container, error)
	serviceTargets func() []map[string]testcontainers.Container
	adminClient    func(...ClientContainerOptions) testcontainers.CustomizeRequestOption
	runDaemon      func(context.Context, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error)
	checkAddresses func(context.Context) error
}

// monitorTopology returns the cluster's MON and MGR membership. The caller
// holds c.mu while it uses the topology.
func (c *Container) monitorTopology() monitorTopology {
	return monitorTopology{
		conn: c.conn(), first: c.Container, monitorTerminated: &c.monitorTerminated, roster: &c.roster, bootstrap: &c.bootstrap,
		osds: c.osdFleet(), settings: c.settings, owner: c,
		client: func(ctx context.Context) (testcontainers.Container, error) { return c.toolkit.snapshot(ctx) },
		serviceTargets: func() []map[string]testcontainers.Container {
			var targets []map[string]testcontainers.Container
			for _, state := range extensionsWith[serviceConfigTargets](c) {
				targets = append(targets, state.ConfigTargets())
			}
			return targets
		},
		adminClient: c.adminClient, runDaemon: c.runDaemon, checkAddresses: c.checkDualStackAddresses,
	}
}

// firstMonitorStands reports whether the embedded first MON exists and was
// not terminated.
func (t monitorTopology) firstMonitorStands() bool { return t.first != nil && !*t.monitorTerminated }

// firstMonitorTerminated reports whether the embedded first MON was
// terminated.
func (t monitorTopology) firstMonitorTerminated() bool { return *t.monitorTerminated }

// markFirstMonitorTerminated records that the embedded first MON is gone.
func (t monitorTopology) markFirstMonitorTerminated() { *t.monitorTerminated = true }
