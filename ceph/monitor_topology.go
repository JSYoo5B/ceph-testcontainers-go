package ceph

import (
	"context"
	"strconv"
	"strings"

	"github.com/testcontainers/testcontainers-go"
)

// monitorTopology owns the cluster's MON and MGR membership: the embedded
// first MON and whether it was terminated, and the roster of the added MONs,
// the MGR candidates and the services. Its methods are the membership views
// (the current MONs, whether a daemon is owned, the targets of a monitor
// configuration refresh), the refresh itself, the start of a MGR, and the
// steps of removing a MON or a MGR. It owns the membership (membershipState,
// held in Container.members) and binds it to what those steps read: the
// command transport, the settings, the bootstrap record, the OSD fleet, the
// feature client, the service extensions' configuration targets and the
// daemon runner.
//
// Every method needs Container.mu held for the whole call. Container keeps
// what decides whether a step may run at all: the cluster's closed flag, the
// topology lock and the admission of a handle's name.
type monitorTopology struct {
	conn cephConn
	// first is the embedded first MON, which does not change after start.
	first testcontainers.Container
	*membershipState
	bootstrap *bootstrapRecord
	osds      osdFleet
	settings  options
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
// membershipState is the MON and MGR membership that monitorTopology owns.
// Container holds it as the field members, whose whole value only
// monitorTopology's constructor names; Container.mu guards all of it.
type membershipState struct {
	// roster is the added MONs, the MGR candidates, the first MGR's container
	// and the services; see memberRoster.
	roster memberRoster
	// monitorTerminated records that the embedded first MON was terminated.
	// It is read or written only through monitorTopology's accessors.
	monitorTerminated bool
}

func (c *Container) monitorTopology() monitorTopology {
	return monitorTopology{
		conn: c.conn(), first: c.Container, membershipState: &c.members, bootstrap: &c.bootstrap,
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
func (t monitorTopology) firstMonitorStands() bool { return t.first != nil && !t.monitorTerminated }

// firstMonitorTerminated reports whether the embedded first MON was
// terminated.
func (t monitorTopology) firstMonitorTerminated() bool { return t.monitorTerminated }

// markFirstMonitorTerminated records that the embedded first MON is gone.
func (t monitorTopology) markFirstMonitorTerminated() { t.monitorTerminated = true }

// ownsNativeTarget reports whether id is a container of this cluster: the
// first MON, a MON, a MGR, an OSD, a service or the feature client. The
// caller holds Container.mu.
func (t monitorTopology) ownsNativeTarget(ctx context.Context, id string) (bool, error) {
	matches := func(ctr testcontainers.Container) bool { return ctr != nil && ctr.GetContainerID() == id }
	if matches(t.first) || matches(t.roster.initialManager()) {
		return true, nil
	}
	for _, mon := range t.roster.eachMonitor() {
		if mon != nil && matches(mon.Container) {
			return true, nil
		}
	}
	for _, mgr := range t.roster.eachManager() {
		if mgr != nil && matches(mgr.Container) {
			return true, nil
		}
	}
	for _, osd := range t.osds.osdHandles.each() {
		if osd != nil && matches(osd.Container) {
			return true, nil
		}
	}
	for _, ctr := range t.roster.eachService() {
		if matches(ctr) {
			return true, nil
		}
	}
	client, err := t.client(ctx)
	if err != nil {
		return false, err
	}
	return matches(client), nil
}

// daemonKeyrings returns the container of an owned daemon and the keyring
// files from which it authenticates when it starts. The caller holds c.mu.
func (t monitorTopology) daemonKeyrings(kind, id string) (testcontainers.Container, []string) {
	if kind == "mgr" {
		return t.roster.manager(id).Container, []string{"/etc/ceph/mgr.keyring", "/var/lib/ceph/mgr/ceph-" + id + "/keyring"}
	}
	n, _ := strconv.Atoi(id)
	return t.osds.osdHandles.get(n).Container, []string{"/etc/ceph/osd.keyring", "/var/lib/ceph/osd/ceph-" + id + "/keyring"}
}

// diagnosticDaemons passes each MON and MGR container of the cluster to add,
// for a diagnostics snapshot: the first MON while it stands (or, once the
// cluster is closed, to report its remains), the added MONs, the MGR
// candidates and the first MGR. The caller holds Container.mu.
func (t monitorTopology) diagnosticDaemons(closed bool, add func(role, name string, ctr testcontainers.Container)) {
	if t.first != nil && (!t.firstMonitorTerminated() || closed) {
		add("mon", "a", t.first)
	}
	for name, mon := range t.roster.eachMonitor() {
		if mon != nil {
			add("mon", name, mon.Container)
		}
	}
	for name, mgr := range t.roster.eachManager() {
		if mgr != nil {
			add("mgr", name, mgr.Container)
		}
	}
	if first := t.roster.initialManager(); first != nil {
		add("mgr", "a", first)
	}
}

// diagnosticServices passes each service container (MDS, RGW or another
// service) to add, with its role, for a diagnostics snapshot. The caller
// holds Container.mu.
func (t monitorTopology) diagnosticServices(add func(role, name string, ctr testcontainers.Container)) {
	for name, ctr := range t.roster.eachService() {
		role := "service"
		if strings.HasPrefix(name, "mds.") {
			role = "mds"
		}
		if name == "rgw" || strings.HasPrefix(name, "rgw:") {
			role = "rgw"
		}
		add(role, name, ctr)
	}
}
