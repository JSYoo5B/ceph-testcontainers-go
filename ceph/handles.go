package ceph

import (
	"context"
	"fmt"
	"strconv"
)

// daemonHandle is what AdminSocket and RotateKey need of the MON, MGR and OSD
// handles: the Ceph daemon name and whether the cluster still owns this very
// instance. ownedBy runs with c.mu held.
type daemonHandle interface {
	daemonRef() (kind, id string)
	ownedBy(c *Container) bool
}

var errHandleWithoutCluster = "the handle does not belong to a cluster; obtain it from the cluster that owns the daemon"

// currentOSD checks, with c.mu held, that handle is the descriptor that the
// cluster owns for its ID now. A descriptor of a removed, detached or
// reopened OSD is stale and never acts on whatever owns the ID instead.
func (c *Container) currentOSD(handle *OSDContainer) error {
	id := handle.ID
	if cur := c.osdHandles.get(id); cur == handle {
		return nil
	} else if cur != nil {
		return invalid(fmt.Errorf("this handle of osd.%d is stale: the cluster owns another instance of the OSD, which DetachedOSD.Reopen or a later handle returns", id))
	}
	if c.osdHandles.detachedOf(id) != nil {
		return invalid(fmt.Errorf("osd.%d is already detached; this handle is stale, reopen it with DetachedOSD.Reopen", id))
	}
	return invalid(fmt.Errorf("osd.%d is not owned by this cluster", id))
}

// currentMonitor checks, with c.mu held, that handle is the MON that the
// cluster owns under its name now and returns it. The primary MON "a" is
// rebuilt as a fresh descriptor around the cluster's own container on every
// listing, so it is matched by that container, not by the pointer.
func (c *Container) currentMonitor(handle *MonitorContainer) (*MonitorContainer, error) {
	name := handle.DaemonName
	if name == "a" && !c.monitorTerminated && c.Container != nil && handle.Container == c.Container {
		return handle, nil
	}
	if cur := c.roster.monitor(name); cur != nil && cur == handle {
		return cur, nil
	}
	return nil, invalid(fmt.Errorf("monitor %s is not owned by this cluster, or this handle is stale", name))
}

// currentManager checks, with c.mu held, that handle is the MGR that the
// cluster owns under its name now.
func (c *Container) currentManager(handle *ManagerContainer) (*ManagerContainer, error) {
	name := handle.DaemonName
	if cur := c.roster.manager(name); cur != nil && cur == handle {
		return cur, nil
	}
	return nil, invalid(fmt.Errorf("manager %s is not owned by this cluster, or this handle is stale", name))
}

func (o *OSDContainer) daemonRef() (string, string) { return "osd", strconv.Itoa(o.ID) }

// ownedBy reports, with c.mu held by the caller, whether c still owns o.
func (o *OSDContainer) ownedBy(c *Container) bool {
	return o != nil && c.osdHandles.get(o.ID) == o && o.Container != nil && !o.purged
}

func (m *MonitorContainer) daemonRef() (string, string) { return "mon", m.DaemonName }

func (m *MonitorContainer) ownedBy(c *Container) bool {
	if m == nil || m.Container == nil {
		return false
	}
	_, err := c.currentMonitor(m)
	return err == nil
}

func (m *ManagerContainer) daemonRef() (string, string) { return "mgr", m.DaemonName }

func (m *ManagerContainer) ownedBy(c *Container) bool {
	return m != nil && c.roster.manager(m.DaemonName) == m && m.Container != nil && !m.terminated
}

func (o *OSDContainer) cluster(op string) (*Container, error) {
	if o == nil || o.owner == nil {
		return nil, invalidRequest("%s: "+errHandleWithoutCluster, op)
	}

	return o.owner, nil
}

func (m *MonitorContainer) cluster(op string) (*Container, error) {
	if m == nil || m.owner == nil {
		return nil, invalidRequest("%s: "+errHandleWithoutCluster, op)
	}

	return m.owner, nil
}

func (m *ManagerContainer) cluster(op string) (*Container, error) {
	if m == nil || m.owner == nil {
		return nil, invalidRequest("%s: "+errHandleWithoutCluster, op)
	}

	return m.owner, nil
}

// Remove drains this owned OSD, waits for safe-to-destroy, stops it, waits for
// down, purges it from Ceph and removes its container. It refuses the last OSD.
// After identity admission, removal fences this Docker instance against new
// native commands and drains its tracked commands before native mutations. A
// failed drain retains ownership and its fence for retry; other targets remain
// admitted. This is permanent removal, not retained-store detach or reopen.
// Every removal phase verifies the captured registration UUID. External OSD
// replacement must not race removal: Ceph has no UUID compare-and-swap purge.
// An uncertain purge response remains tracked; a retry reconciles fresh native
// absence only after this handle issued purge. Completed purge retries only
// Docker cleanup. On error retry with a fresh context or terminate the cluster.
//
// Remove acts on this handle's own instance. A handle of a removed or detached
// OSD, or one that Reopen replaced, returns an error and never acts on the
// container that owns the ID now. Remove refuses a detaching or detached OSD.
func (o *OSDContainer) Remove(ctx context.Context) error {
	c, err := o.cluster("OSDContainer.Remove")
	if err != nil {
		return err
	}
	return c.removeOSD(ctx, o)
}

// SetIn changes native OSDMap membership of this owned OSD. It does not stop,
// purge or change CRUSH capacity weights. Readback verifies the owned UUID and
// desired state, since native in/out can succeed for nonexistent IDs.
// WaitForPGClean can wait for recovery while an OSD remains out. Ceph preserves
// a nonzero map reweight across out/in. External replacement must not race
// this method; Ceph has no compare-and-swap operation for OSD membership.
// A handle of a removed or detached OSD returns an error.
func (o *OSDContainer) SetIn(ctx context.Context, in bool) error {
	c, err := o.cluster("OSDContainer.SetIn")
	if err != nil {
		return err
	}
	return c.setOSDIn(ctx, o, in)
}

// Detach stops and removes this owned OSD's container but keeps its store in
// the owned volume and its MON registration, so that DetachedOSD.Reopen can
// start a new container on them. It needs WithOSDVolumeStorage. The OSD is
// down, not out or purged, while it is detached; hold noout to keep it in.
//
// Detach admits the native identity, refuses an OSD that a held network or
// pause fault targets, fences the instance against new native commands and
// drains its tracked ones, then stops it, waits for down and removes the
// container. Only after confirmed removal does it read the store's identity
// and publish the DetachedOSD; this *OSDContainer stays tied to the removed
// instance. On error the OSD stays owned with its fence: retry Detach with
// a fresh context or terminate the cluster. Remove refuses a detaching or
// detached OSD.
func (o *OSDContainer) Detach(ctx context.Context) (*DetachedOSD, error) {
	c, err := o.cluster("OSDContainer.Detach")
	if err != nil {
		return nil, err
	}
	return c.detachOSD(ctx, o)
}

// AdminSocket runs a command on the admin socket of this OSD, which its
// cluster must still own, and returns the command's output. args are the command
// words, such as "status", "version" or "config", "get", "osd_heartbeat_grace";
// "help" lists the daemon's commands.
//
// The command does not use the Messenger or Cephx: it reaches the daemon
// through its local socket, so it works while the daemon cannot reach a
// monitor and reports what the daemon itself has applied, for example the
// newest OSDMap epoch in an OSD's "status". It runs in the control-plane container,
// which shares the daemons' run directory, so it works whatever the daemon's
// own image contains. The daemon must be running; a stopped or removed
// daemon returns an error. A paused daemon accepts the connection but does not
// answer, so the call waits for ctx or the cluster's startup timeout, which
// also bounds every call. When the call ends early, AdminSocket stops its CLI
// in the control container before it returns, including one that Docker has
// not started yet. A nonzero exit returns the output with an error.
func (o *OSDContainer) AdminSocket(ctx context.Context, args ...string) ([]byte, error) {
	c, err := o.cluster("OSDContainer.AdminSocket")
	if err != nil {
		return nil, err
	}
	return c.adminSocket(ctx, o, args...)
}

// AdminSocket runs a command on the admin socket of this MON, which its
// cluster must still own, and returns the command's output. args are the command
// words, such as "status", "version" or "config", "get", "osd_heartbeat_grace";
// "help" lists the daemon's commands.
//
// The command does not use the Messenger or Cephx: it reaches the daemon
// through its local socket, so it works while the daemon cannot reach a
// monitor and reports what the daemon itself has applied, for example the
// newest OSDMap epoch in an OSD's "status". It runs in the control-plane container,
// which shares the daemons' run directory, so it works whatever the daemon's
// own image contains. The daemon must be running; a stopped or removed
// daemon returns an error. A paused daemon accepts the connection but does not
// answer, so the call waits for ctx or the cluster's startup timeout, which
// also bounds every call. When the call ends early, AdminSocket stops its CLI
// in the control container before it returns, including one that Docker has
// not started yet. A nonzero exit returns the output with an error.
func (m *MonitorContainer) AdminSocket(ctx context.Context, args ...string) ([]byte, error) {
	c, err := m.cluster("MonitorContainer.AdminSocket")
	if err != nil {
		return nil, err
	}
	return c.adminSocket(ctx, m, args...)
}

// AdminSocket runs a command on the admin socket of this MGR, which its
// cluster must still own, and returns the command's output. args are the command
// words, such as "status", "version" or "config", "get", "osd_heartbeat_grace";
// "help" lists the daemon's commands.
//
// The command does not use the Messenger or Cephx: it reaches the daemon
// through its local socket, so it works while the daemon cannot reach a
// monitor and reports what the daemon itself has applied, for example the
// newest OSDMap epoch in an OSD's "status". It runs in the control-plane container,
// which shares the daemons' run directory, so it works whatever the daemon's
// own image contains. The daemon must be running; a stopped or removed
// daemon returns an error. A paused daemon accepts the connection but does not
// answer, so the call waits for ctx or the cluster's startup timeout, which
// also bounds every call. When the call ends early, AdminSocket stops its CLI
// in the control container before it returns, including one that Docker has
// not started yet. A nonzero exit returns the output with an error.
func (m *ManagerContainer) AdminSocket(ctx context.Context, args ...string) ([]byte, error) {
	c, err := m.cluster("ManagerContainer.AdminSocket")
	if err != nil {
		return nil, err
	}
	return c.adminSocket(ctx, m, args...)
}

// RotateKey replaces the Cephx key of this running OSD without a restart,
// as cephadm's daemon key rotation does. The rotation is described at
// ManagerContainer.RotateKey; the same steps and failure semantics apply.
// A handle of a removed or detached OSD returns an error.
func (o *OSDContainer) RotateKey(ctx context.Context) error {
	c, err := o.cluster("OSDContainer.RotateKey")
	if err != nil {
		return err
	}
	return c.rotateDaemonKey(ctx, o)
}

// RotateKey replaces the Cephx key of this running MGR without a restart, as
// cephadm's daemon key rotation does:
//
//  1. auth get-or-create-pending creates the new key on the MONs, which then
//     accept both keys.
//  2. The daemon's keyring files in its container get the new key, so that a
//     later restart authenticates with it.
//  3. ceph tell <daemon> rotate-key hands the key to the running daemon.
//  4. auth commit-pending makes it the entity's only key.
//
// MONs share the mon. key, so *MonitorContainer has no RotateKey.
//
// The MONs accept both keys until step 4, and they also commit the pending key
// on their own when the daemon authenticates with it, after a restart or a
// handoff. A failure before step 4 therefore keeps both keys valid, and the
// next call finishes the rotation with the same key; a failed step 4 is
// reported with the outcome that the MONs show once settled. A call returns
// only once none of its commands can still change the keys or the keyring
// files, or after recording such commands for the daemon: the next call stops
// them, waits for them and settles the MONs for them before it changes
// anything, and fails while it cannot. After a handoff that may still reach
// the daemon, a later call finishes the rotation with the same key and
// generates another key only once the daemon's container has restarted.
// A handle of a removed MGR returns an error.
func (m *ManagerContainer) RotateKey(ctx context.Context) error {
	c, err := m.cluster("ManagerContainer.RotateKey")
	if err != nil {
		return err
	}
	return c.rotateDaemonKey(ctx, m)
}

// Remove deletes this owned monitor. It preserves a working quorum: add a
// replacement first when a stop would leave fewer than a majority of the
// current map running. A handle of a removed monitor returns an error.
func (m *MonitorContainer) Remove(ctx context.Context) error {
	c, err := m.cluster("MonitorContainer.Remove")
	if err != nil {
		return err
	}
	return c.removeMonitor(ctx, m)
}

// Remove deletes this owned manager after another running, registered
// candidate is available. Add a replacement before removing the last
// candidate. On failure the handle remains owned for retry or cluster cleanup.
// Do not edit these manager identities externally while a topology operation
// is in progress. A handle of a removed manager returns an error.
func (m *ManagerContainer) Remove(ctx context.Context) error {
	c, err := m.cluster("ManagerContainer.Remove")
	if err != nil {
		return err
	}
	return c.removeManager(ctx, m)
}

// Reopen starts a new container on the store and registration that Detach
// retained, and waits for the OSD to be up/in. It returns a new *OSDContainer
// with the same ID, UUID and placement; the previous handle of the OSD keeps
// naming the removed container and is stale.
//
// Before any write it requires the store's identity files, keyring and block
// inode and size to be unchanged since the detach, the MON registration to
// hold the same UUID and be down, and the MONs' key for the OSD to equal the
// stored one. A refusal there, a failed cleanup of the stale admin socket,
// or a canceled context before the container is requested leaves the OSD
// detached for another attempt; none of them dispatches a creation request.
// The cleanup runs under the supervisor. If its end stays unconfirmed, the
// detached OSD records it and every later Reopen first has to stop or
// confirm it, so that a late cleanup cannot remove the socket of the
// replacement; until then Reopen refuses without creating a container.
// Once the new container is requested the detached OSD is consumed: an
// error returns the new handle, which the cluster owns. If that handle has a
// container, detach it again, remove it or terminate the cluster. If the
// creation request returned no container, its reply may have been lost after
// the runtime accepted it, so it is never replayed and the handle has no
// container to detach: Detach and Reopen refuse it, Remove refuses the last
// OSD, and only Terminate recovers it. Reopen does not reconcile a container
// that such a lost reply may have created. The store is started through the
// ordinary bootstrap, which validates the stored identity again and never
// formats a store that has its ready marker.
func (d *DetachedOSD) Reopen(ctx context.Context) (*OSDContainer, error) {
	if d == nil || d.cluster == nil {
		return nil, invalidRequest("DetachedOSD.Reopen: the detached OSD does not belong to a cluster")
	}

	return d.cluster.reopenOSD(ctx, d)
}
