package ceph

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
)

// Replace starts this MON in a new container on the store and address it
// keeps, and waits for it to rejoin the quorum. The membership does not change:
// the MON keeps its name, rank and monmap entry. It needs
// MonitorStorage.WithVolume. DaemonOptions.WithImage starts the new container
// from another image, for example a newer release; with no option it uses the
// image this MON ran. The store is not checked against that image's release,
// and nothing makes the change reversible.
//
// Before it touches the running MON it refuses a replacement that would leave
// fewer than a majority of the monmap in quorum, a store that is not attached
// to this handle's container, and a store with no recorded address. It then
// removes the old container, confirms that nothing mounts the volume, and
// creates the new one with the recorded IPv4 address, in a mode that refuses to
// format a missing store. It returns the new handle; this handle names the
// removed container and is stale, also for MON a, whose embedded container of
// Container is then gone and whose fixture commands use the control container.
//
// When the old container is gone and the new one fails, the error comes with
// the new handle if a container exists: the store and its volume stay owned,
// nothing is formatted, and Replace on that handle or Terminate continues.
// A creation whose reply was lost is never replayed; only Terminate settles it.
//
//purpose:condition,lifetime
func (m *MonitorContainer) Replace(ctx context.Context, opts ...DaemonOptions) (*MonitorContainer, error) {
	c, err := m.cluster("MonitorContainer.Replace")
	if err != nil {
		return nil, err
	}
	return c.replaceMonitor(ctx, m, opts)
}

func (c *Container) replaceMonitor(ctx context.Context, handle *MonitorContainer, opts []DaemonOptions) (*MonitorContainer, error) {
	daemon, err := resolveDaemonOptions(opts)
	if err != nil {
		return nil, err
	}
	if !c.settings.monitorVolume {
		return nil, invalidRequest("MonitorContainer.Replace needs MonitorStorage.WithVolume")
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	if c.closed {
		return nil, errClusterTerminated
	}
	t := c.monitorTopology()
	cur, err := t.currentMonitor(handle)
	if err != nil {
		return nil, err
	}
	name := cur.DaemonName
	store := c.monStores.Store(name)
	if store == nil || store.Attachment == nil || cur.Container == nil || store.Attachment.ID != cur.GetContainerID() {
		return nil, invalid(fmt.Errorf("monitor %s: its store is not attached to this handle's container", name))
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := c.monitorReplacementQuorum(ctx, name); err != nil {
		return nil, err
	}
	key, monmap, err := c.monitorReplacementMaterial(ctx)
	if err != nil {
		return nil, err
	}
	join := monitorJoin{name: name, config: []byte(strings.ReplaceAll(string(c.bootstrap.rawConfig()), "mon initial members = a\n", "")), key: key, monmap: monmap}
	image := cur.image
	if daemon.imageSet {
		image = daemon.image
	}
	gens := c.monitorGenerations(name, func(ports []int) []testcontainers.ContainerCustomizer { return c.monitorJoinOptions(join, ports) }, monitorSocketReady(name))
	gens.beforeCreate = func(ctx context.Context) error { return c.clearAdminSocket(ctx, "mon."+name) }
	oldID := cur.GetContainerID()
	id, err := store.ReplaceGeneration(ctx, gens, image)
	if store.Attachment != nil && store.Attachment.ID == oldID {
		// Refused before the old container was touched: nothing changed.
		return nil, err
	}
	// The old container is gone. Its handle is stale from here on.
	if name == "a" && cur.Container == t.first {
		t.markFirstMonitorTerminated()
	}
	next := &MonitorContainer{DaemonName: name, owner: c, image: image, Container: gens.created[id]}
	if next.Container == nil {
		c.members.roster.dropMonitor(name)
		return nil, errors.Join(err, fmt.Errorf("monitor %s has no container after its replacement; Terminate settles the store", name))
	}
	c.members.roster.addMonitor(name, next)
	if err != nil {
		return next, err
	}
	if err := clusterutil.Poll(ctx, func() (bool, error) {
		status, err := daemonMaps{c.conn()}.quorum(ctx)
		return slices.Contains(status.QuorumNames, name), err
	}); err != nil {
		return next, fmt.Errorf("monitor %s did not rejoin the quorum: %w", name, err)
	}
	return next, t.refreshMonitorConfig(ctx)
}

// monitorReplacementQuorum refuses a replacement that would leave fewer than a
// majority of the monmap in quorum while name is down. The caller holds c.mu.
func (c *Container) monitorReplacementQuorum(ctx context.Context, name string) error {
	status, err := daemonMaps{c.conn()}.quorum(ctx)
	if err != nil {
		return fmt.Errorf("inspect monitor membership: %w", err)
	}
	others := 0
	for _, member := range status.QuorumNames {
		if member != name {
			others++
		}
	}
	if others*2 <= len(status.MonMap.Mons) {
		return invalid(fmt.Errorf("replacing monitor %s would leave %d of %d monitors in quorum; add a monitor first", name, others, len(status.MonMap.Mons)))
	}
	return nil
}

// monitorReplacementMaterial starts the independent CLI control, so that no
// fixture command depends on a MON being added or replaced, and reads the
// shared mon. key and the live monmap. The caller holds c.mu.
func (c *Container) monitorReplacementMaterial(ctx context.Context) (key, monmap []byte, err error) {
	if err := c.ensureControlPlane(ctx); err != nil {
		return nil, nil, fmt.Errorf("start independent CLI control: %w", err)
	}
	key, err = c.conn().ceph(ctx, "auth", "get", "mon.")
	if err != nil {
		return nil, nil, err
	}
	mapPath := "/tmp/tc-monmap-" + uuid.NewString()
	if _, err := c.conn().ceph(ctx, "mon", "getmap", "-o", mapPath); err != nil {
		return nil, nil, err
	}
	monmap, err = readFile(ctx, c.conn().cliContainer(), mapPath)
	return key, monmap, err
}
