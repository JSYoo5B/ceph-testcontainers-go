package ceph

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/testcontainers/testcontainers-go"
)

// ConnectionConfig returns the independent bootstrap ceph.conf and admin
// keyring copies, waiting for topology and the configuration cache only while
// ctx remains active. On error it returns the zero ConnectionConfig.
//
// It is for a native client the caller starts, such as a test process using
// go-ceph in host mode. The configuration contains the addresses the cluster
// advertises: container addresses in bridge mode, host addresses with the
// final MON ports in host mode. ctx bounds the wait for a concurrent topology
// change or configuration update. It fails after termination or before
// bootstrap completed and then returns the zero ConnectionConfig. For a
// restricted identity use ClientConfig.ConnectionConfig.
//
//purpose:handoff
func (c *Container) ConnectionConfig(ctx context.Context) (ConnectionConfig, error) {
	if c == nil {
		return ConnectionConfig{}, errors.New("ceph cluster is unavailable")
	}
	if err := c.lockTopology(ctx); err != nil {
		return ConnectionConfig{}, err
	}
	defer c.mu.Unlock()
	if c.closed {
		return ConnectionConfig{}, errClusterTerminated
	}
	config, keyring, complete, err := c.bootstrap.connection(ctx)
	if err != nil {
		return ConnectionConfig{}, err
	}
	if !complete {
		return ConnectionConfig{}, errors.New("ceph cluster bootstrap is incomplete")
	}
	return ConnectionConfig{Conf: config, Keyring: keyring}, nil
}

// Managers returns sorted owned candidate descriptors. The slice is
// independent; descriptors and their containers retain fixture ownership.
//
// The returned slice is independent; descriptors and containers stay owned by
// the cluster. Stop the active manager's container (ManagerStatus names it) to
// make a standby take over, or the only manager to test behaviour without one.
//
//purpose:observation
func (c *Container) Managers(ctx context.Context) ([]*ManagerContainer, error) {
	if c == nil {
		return nil, errors.New("ceph cluster is unavailable")
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	result := make([]*ManagerContainer, 0, c.members.roster.managerCount())
	for _, manager := range c.members.roster.eachManager() {
		result = append(result, manager)
	}
	slices.SortFunc(result, func(a, b *ManagerContainer) int { return strings.Compare(a.DaemonName, b.DaemonName) })
	return result, nil
}

// Monitors returns the owned MON membership sorted by daemon name,
// like Monitors, waiting for the topology only while ctx remains active.
//
// The returned slice is independent; descriptors and containers stay owned by
// the cluster. Stop and Start on a handle inject failures and keep the
// monitor's store; MonitorContainer.Remove changes membership. When a majority
// is stopped, quorum is lost and new clients cannot authenticate; after the
// monitors start again, existing and new sessions work. On a bridge network a
// MON returns at the address the monmap records for it whatever the order of
// Start calls, because the fixture requests its original address from Docker
// again; a MON whose endpoint fault.InterruptNetwork interrupted does not
// start until it is restored. MON a waits for a quorum when it starts, so
// after stopping all MONs start the others first.
//
//purpose:observation
func (c *Container) Monitors(ctx context.Context) ([]*MonitorContainer, error) {
	if c == nil {
		return nil, errors.New("ceph cluster is unavailable")
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	return c.monitorTopology().monitorsLocked(), nil
}

// OSDs returns the owned OSD descriptors sorted by ID, like OSDs,
// waiting for the topology only while ctx remains active.
//
// The returned slice is independent; descriptors and containers stay owned by
// the cluster. Stop and Start on a handle inject daemon failures and keep the
// OSD's identity and data, also with in-memory storage; Ceph marks a stopped
// OSD down after the heartbeat grace and out after mon_osd_down_out_interval
// unless noout is set. OSD bootstrap initializes only an empty directory: it
// refuses a non-empty partial store without a ready marker, and a ready store
// must match the OSD's ID, UUID and cluster FSID; failed initialization is not
// replayed (see docs/internals/osd-storage.md).
//
//purpose:observation
func (c *Container) OSDs(ctx context.Context) ([]*OSDContainer, error) {
	if c == nil {
		return nil, errors.New("ceph cluster is unavailable")
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	return c.fleet.osdHandles.sorted(), nil
}

// ControlContainer returns the stable CLI handle while bounding a
// concurrent control-plane creation or termination publication wait by ctx.
// The cluster owns the handle; this snapshot does not promise it remains live.
// In a single-MON cluster it is MON a; with more than one monitor it is an
// independent CLI container created by the first AddMonitor (also during Run),
// so commands keep working while MON a is stopped or removed. It runs the
// essential image, so it has the ceph CLI and bootstrap tools such as
// ceph-authtool but not rados and rbd (use ClientContainer) or radosgw-admin
// (use rgw.Gateway.Admin). Do not pause or stop it; fault.Pause refuses it.
//
//purpose:handoff,native
func (c *Container) ControlContainer(ctx context.Context) (testcontainers.Container, error) {
	return c.conn().controlContainer(ctx)
}
