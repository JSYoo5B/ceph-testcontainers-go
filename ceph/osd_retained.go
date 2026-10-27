package ceph

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

// DetachedOSD is the retained store and MON registration of an OSD whose
// container OSDContainer.Detach removed. DetachedOSD.Reopen of the same cluster starts a new
// container on it once. The cluster owns the store: Terminate discards it
// with the owned volume, and the MONs keep the registration down meanwhile.
type DetachedOSD struct {
	cluster   *Container
	id        int
	osdUUID   string
	fsid      string
	placement OSDConfig
	image     string
	store     osdStoreIdentity
	reopened  bool
	// staleCleanups holds socket cleanups whose end was unconfirmed. A
	// reopen settles them before it cleans again or creates a container, so
	// that a late removal cannot unlink the successor's socket. Guarded by
	// the cluster's topology lock.
	staleCleanups []rotationRun
}

// ID returns the retained numeric OSD ID.
func (d *DetachedOSD) ID() int { return d.id }

// UUID returns the retained OSD UUID that the store and its MON registration hold.
func (d *DetachedOSD) UUID() string { return d.osdUUID }

// ClusterFSID returns the cluster FSID that the retained store holds.
func (d *DetachedOSD) ClusterFSID() string { return d.fsid }

// Placement returns the CRUSH location that DetachedOSD.Reopen starts the OSD in.
func (d *DetachedOSD) Placement() OSDConfig { return d.placement }

// osdStoreIdentity is what a detached store holds: its identity files and the
// inode and size of its block file, read through the volume helper.
type osdStoreIdentity struct {
	whoami, osdUUID, clusterFSID, keyring, block string
}

var osdStoreFiles = []string{"ready", "whoami", "fsid", "ceph_fsid", "keyring", "block"}

// retainedStore reads the identity of osd.id's directory without writing it.
// Every file must be a regular file, not a symbolic link.
func (s *osdVolumeStorage) retainedStore(ctx context.Context, id int) (osdStoreIdentity, error) {
	if id < 0 {
		return osdStoreIdentity{}, errors.New("OSD volume directory ID must be nonnegative")
	}
	if err := s.check(ctx); err != nil {
		return osdStoreIdentity{}, err
	}
	dir := osdVolumeHelperPath + "/ceph-" + strconv.Itoa(id)
	for _, args := range [][]string{{"test", "-d", dir}, {"test", "!", "-L", dir}} {
		if _, err := command(ctx, s.helper, args...); err != nil {
			return osdStoreIdentity{}, fmt.Errorf("retained osd.%d directory is missing or a symbolic link: %w", id, err)
		}
	}
	for _, name := range osdStoreFiles {
		path := dir + "/" + name
		for _, args := range [][]string{{"test", "-f", path}, {"test", "!", "-L", path}} {
			if _, err := command(ctx, s.helper, args...); err != nil {
				return osdStoreIdentity{}, fmt.Errorf("retained osd.%d store lacks a regular %s: %w", id, name, err)
			}
		}
	}
	read := func(name string) (string, error) {
		data, err := command(ctx, s.helper, "cat", "--", dir+"/"+name)
		if err != nil {
			return "", fmt.Errorf("read retained osd.%d %s: %w", id, name, err)
		}
		return string(data), nil
	}
	var identity osdStoreIdentity
	var err error
	if identity.whoami, err = read("whoami"); err != nil {
		return osdStoreIdentity{}, err
	}
	if identity.osdUUID, err = read("fsid"); err != nil {
		return osdStoreIdentity{}, err
	}
	if identity.clusterFSID, err = read("ceph_fsid"); err != nil {
		return osdStoreIdentity{}, err
	}
	if identity.keyring, err = read("keyring"); err != nil {
		return osdStoreIdentity{}, err
	}
	block, err := command(ctx, s.helper, "stat", "-c", "%i:%s", "--", dir+"/block")
	if err != nil {
		return osdStoreIdentity{}, fmt.Errorf("inspect retained osd.%d block: %w", id, err)
	}
	identity.block = strings.TrimSpace(string(block))
	return identity, nil
}

// matches reports why a store does not hold osd.id of osdUUID in the cluster
// fsid, or nil. Its keyring must name osd.id with a key.
func (s osdStoreIdentity) matches(id int, osdUUID, fsid string) error {
	switch {
	case strings.TrimSpace(s.whoami) != strconv.Itoa(id):
		return fmt.Errorf("retained store holds osd.%s, not osd.%d", strings.TrimSpace(s.whoami), id)
	case strings.TrimSpace(s.osdUUID) != osdUUID:
		return fmt.Errorf("retained osd.%d store holds another OSD UUID", id)
	case strings.TrimSpace(s.clusterFSID) != fsid:
		return fmt.Errorf("retained osd.%d store belongs to another cluster FSID", id)
	}
	if _, err := osdKeyringSecret(s.keyring, id); err != nil {
		return err
	}
	if s.block == "" {
		return fmt.Errorf("retained osd.%d block identity is unavailable", id)
	}
	return nil
}

// osdKeyringSecret returns the key of the [osd.id] section of keyring.
func osdKeyringSecret(keyring string, id int) (string, error) {
	section, secret := "", ""
	for _, line := range strings.Split(keyring, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.TrimSpace(line[1 : len(line)-1])
			continue
		}
		name, value, ok := strings.Cut(line, "=")
		if ok && section == fmt.Sprintf("osd.%d", id) && strings.TrimSpace(name) == "key" {
			if secret != "" {
				return "", fmt.Errorf("retained osd.%d keyring has more than one key", id)
			}
			secret = strings.TrimSpace(value)
		}
	}
	if secret == "" {
		return "", fmt.Errorf("retained osd.%d keyring has no key for osd.%d", id, id)
	}
	return secret, nil
}

// clusterFSID reads the FSID that the MONs serve.
func (c *Container) clusterFSID(ctx context.Context) (string, error) {
	data, err := c.cephCLI(ctx, "fsid")
	if err != nil {
		return "", fmt.Errorf("read cluster FSID: %w", err)
	}
	fsid, err := uuid.Parse(strings.TrimSpace(string(data)))
	if err != nil || fsid == uuid.Nil {
		return "", fmt.Errorf("cluster FSID %q is not a UUID", strings.TrimSpace(string(data)))
	}
	return fsid.String(), nil
}

// instanceFaultNames are the fault kinds keyed by a container ID, alone or
// after its network name, by the name a detach refusal gives them.
var instanceFaultNames = map[faultKind]string{
	faultNetworkDelay: "network delay", faultNetworkInterruption: "network interruption", faultContainerPause: "container pause",
}

// refuseInstanceFaults refuses an OSD container that a held network or
// pause fault still targets: its undo needs the container OSDContainer.Detach removes.
// The caller holds c.mu.
func (c *Container) refuseInstanceFaults(ctx context.Context, id int, containerID string) error {
	for fid, entry := range c.faults.entries {
		kind, ok := instanceFaultNames[fid.kind]
		if !ok {
			continue
		}
		if fid.key != containerID && !strings.HasSuffix(fid.key, "/"+containerID) {
			continue
		}
		if entry.handle != nil && entry.complete != nil {
			done, err := entry.complete(ctx)
			if err != nil {
				return err
			}
			if done {
				continue
			}
		}
		return fmt.Errorf("restore the %s of osd.%d before detaching it", kind, id)
	}
	return nil
}

// detachOSD implements OSDContainer.Detach, which documents the behavior. The caller
// admitted the shared-cluster policy; the handle is checked under the lock.
func (c *Container) detachOSD(ctx context.Context, handle *OSDContainer) (*DetachedOSD, error) {
	id := handle.ID
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	if c.closed {
		return nil, errClusterTerminated
	}
	if !c.settings.osdVolumeStorage || !c.stores.hasVolume() {
		return nil, invalidRequest("OSDContainer.Detach retains stores only with WithOSDVolumeStorage")
	}
	if err := c.currentOSD(handle); err != nil {
		return nil, err
	}
	osd := handle
	if osd.purged || osd.purgeIssued {
		return nil, fmt.Errorf("osd.%d is being removed; retry OSDContainer.Remove", id)
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	name := fmt.Sprintf("osd.%d", id)
	fsid, err := c.clusterFSID(ctx)
	if err != nil {
		return nil, err
	}
	if !osd.detachRemoved {
		if _, err := c.removalOSDState(ctx, id); err != nil {
			return nil, err
		}
		if osd.Container == nil {
			return nil, fmt.Errorf("%s has no container to detach", name)
		}
		containerID := osd.GetContainerID()
		if err := c.refuseInstanceFaults(ctx, id, containerID); err != nil {
			return nil, err
		}
		osd.detaching = true
		if err := c.fenceNativeTarget(ctx, containerID); err != nil {
			return nil, fmt.Errorf("stop native commands for %s: %w", name, err)
		}
		stopTimeout := 10 * time.Second
		if err := osd.Stop(ctx, &stopTimeout); !onlyMissingHostResource(err) {
			return nil, fmt.Errorf("stop %s: %w", name, err)
		}
		if err := c.waitOSDRemoval(ctx, id, false); err != nil {
			return nil, fmt.Errorf("wait for %s down: %w", name, err)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// A stopped container could still be started through a raw handle;
		// the store is released only after the container is gone.
		if err := osd.Terminate(ctx); !onlyMissingHostResource(err) {
			return nil, fmt.Errorf("remove %s container: %w", name, err)
		}
		osd.detachRemoved = true
	}
	if _, err := c.removalOSDState(ctx, id); err != nil {
		return nil, err
	}
	store, err := c.stores.retainedStore(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := store.matches(id, osd.nativeUUID, fsid); err != nil {
		return nil, err
	}
	detached := &DetachedOSD{cluster: c, id: id, osdUUID: osd.nativeUUID, fsid: fsid, placement: osd.placement, image: osd.image, store: store}
	if c.detachedOSDs == nil {
		c.detachedOSDs = make(map[int]*DetachedOSD)
	}
	c.detachedOSDs[id] = detached
	delete(c.osds, id)
	return detached, nil
}

// reopenOSD implements DetachedOSD.Reopen, which documents the behavior. The caller
// admitted the shared-cluster policy; the handle is checked under the lock.
func (c *Container) reopenOSD(ctx context.Context, detached *DetachedOSD) (*OSDContainer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	if c.closed {
		return nil, errClusterTerminated
	}
	if detached.cluster != c {
		return nil, invalidRequest("the detached OSD belongs to another cluster")
	}
	id := detached.id
	if detached.reopened || c.detachedOSDs[id] != detached {
		return nil, invalid(fmt.Errorf("osd.%d was already reopened", id))
	}
	if c.osds[id] != nil {
		return nil, fmt.Errorf("osd.%d is owned by another handle", id)
	}
	if !c.stores.hasVolume() {
		return nil, errors.New("OSD volume storage is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := c.reopenPreflight(ctx, detached); err != nil {
		return nil, err
	}
	// The stale socket cleanup is idempotent and runs before the handle is
	// consumed, so that its failure is a known zero-dispatch refusal that
	// leaves the detached OSD reusable. The topology lock is held throughout.
	if err := c.clearStaleOSDSocket(ctx, detached); err != nil {
		return nil, err
	}
	// Last check before the irreversible transition.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	detached.reopened = true
	delete(c.detachedOSDs, id)
	osd := &OSDContainer{ID: id, owner: c, nativeUUID: detached.osdUUID, placement: detached.placement, image: detached.image}
	c.osds[id] = osd
	launch := osdLaunch{osd: osd, recipe: c.newOSDLaunchRecipe(osd, []byte(detached.store.keyring))}
	if err := c.startOSD(ctx, launch); err != nil {
		return osd, err
	}
	if err := c.waitOSDsUp(ctx, []int{id}); err != nil {
		return osd, err
	}
	if _, err := c.removalOSDState(ctx, id); err != nil {
		return osd, err
	}
	return osd, nil
}

// clearStaleOSDSocket removes the socket that the removed daemon left in the
// shared run directory, like clearAdminSocket, but under the supervisor. An
// earlier cleanup of this detached OSD whose end was unconfirmed is first
// stopped or confirmed finished; while that fails, no new cleanup or reopen
// can start. A cleanup whose own end stays unconfirmed is recorded in the
// same way and returned as an error. Only an error without such a record
// proves that no cleanup command can still run, and only then may the caller
// retry. The caller holds c.mu.
func (c *Container) clearStaleOSDSocket(ctx context.Context, detached *DetachedOSD) error {
	if c.run == nil {
		return nil
	}
	daemon := fmt.Sprintf("osd.%d", detached.id)
	for len(detached.staleCleanups) > 0 {
		run := detached.staleCleanups[0]
		err := supervised.Cancel(ctx, run.ctr, "stale admin socket cleanup", run.marker)
		if err != nil {
			if _, stateErr := run.ctr.State(ctx); !errdefs.IsNotFound(stateErr) {
				return fmt.Errorf("an earlier cleanup of the admin socket of %s may still run and cannot be stopped yet: %w", daemon, err)
			}
		}
		detached.staleCleanups = detached.staleCleanups[1:]
	}
	control := c.cliContainer()
	if control == nil {
		return errors.New("ceph control container is unavailable")
	}
	result, err := supervised.Command(ctx, control, "stale admin socket cleanup", c.settings.startupTimeout, adminSocketStdoutLimit, adminSocketStderrLimit,
		"rm", "-f", adminSocketDir+"/ceph-"+daemon+".asok")
	if err != nil {
		if marker, unconfirmed := supervised.Unconfirmed(err); unconfirmed {
			detached.staleCleanups = append(detached.staleCleanups, rotationRun{ctr: control, marker: marker})
		}
		return fmt.Errorf("clear admin socket of %s: %w", daemon, err)
	}
	if result.ExitCode != 0 {
		return fmt.Errorf("clear admin socket of %s: %w", daemon, nativeFailure("rm", result.ExitCode, nil, result.Stderr))
	}
	return nil
}

// reopenPreflight reads, without writing, everything that a reopen relies on.
func (c *Container) reopenPreflight(ctx context.Context, detached *DetachedOSD) error {
	id := detached.id
	store, err := c.stores.retainedStore(ctx, id)
	if err != nil {
		return err
	}
	if store != detached.store {
		return fmt.Errorf("retained osd.%d store changed since it was detached", id)
	}
	fsid, err := c.clusterFSID(ctx)
	if err != nil {
		return err
	}
	if err := store.matches(id, detached.osdUUID, fsid); err != nil {
		return err
	}
	dump, err := c.removalOSDDump(ctx)
	if err != nil {
		return err
	}
	state, exists := dump.osds[id]
	if !exists {
		return fmt.Errorf("retained osd.%d is no longer registered", id)
	}
	if actual, err := uuid.Parse(state.UUID); err != nil || actual.String() != detached.osdUUID {
		return fmt.Errorf("native osd.%d UUID no longer matches its retained registration", id)
	}
	if state.Up {
		return fmt.Errorf("retained osd.%d is up; another instance uses its registration", id)
	}
	key, err := c.cephCLI(ctx, "auth", "get-key", fmt.Sprintf("osd.%d", id))
	if err != nil {
		return fmt.Errorf("read the MON key of osd.%d: %w", id, err)
	}
	stored, err := osdKeyringSecret(store.keyring, id)
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(key)) != stored {
		return fmt.Errorf("the MON key of osd.%d differs from its retained keyring", id)
	}
	return nil
}
