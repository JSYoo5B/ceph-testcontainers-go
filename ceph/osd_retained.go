package ceph

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// DetachedOSD is the retained store and MON registration of an OSD whose
// container DetachOSD removed. ReopenOSD of the same cluster starts a new
// container on it once. The cluster owns the store: Terminate discards it
// with the owned volume, and the MONs keep the registration down meanwhile.
type DetachedOSD struct {
	cluster   *Container
	id        int
	osdUUID   string
	fsid      string
	placement OSDConfig
	store     osdStoreIdentity
	reopened  bool
}

// ID returns the retained numeric OSD ID.
func (d *DetachedOSD) ID() int { return d.id }

// UUID returns the retained OSD UUID that the store and its MON registration hold.
func (d *DetachedOSD) UUID() string { return d.osdUUID }

// ClusterFSID returns the cluster FSID that the retained store holds.
func (d *DetachedOSD) ClusterFSID() string { return d.fsid }

// Placement returns the CRUSH location that ReopenOSD starts the OSD in.
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
// pause fault still targets: its undo needs the container DetachOSD removes.
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

// DetachOSD stops and removes an owned OSD's container but keeps its store in
// the owned volume and its MON registration, so that ReopenOSD can start a
// new container on them. It needs WithOSDVolumeStorage. The OSD is down, not
// out or purged, while it is detached; hold noout to keep it in.
//
// Detach admits the native identity, refuses an OSD that a held network or
// pause fault targets, fences the instance against new native commands and
// drains its tracked ones, then stops it, waits for down and removes the
// container. Only after confirmed removal does it read the store's identity
// and publish the DetachedOSD; the old *OSDContainer stays tied to the removed
// instance. On error the OSD stays owned with its fence: retry DetachOSD with
// a fresh context or terminate the cluster. RemoveOSD refuses a detaching or
// detached OSD.
func (c *Container) DetachOSD(ctx context.Context, id int) (*DetachedOSD, error) {
	if gateErr := c.sharedGate("DetachOSD"); gateErr != nil {
		return nil, gateErr
	}
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
	if !c.settings.osdVolumeStorage || c.osdVolume == nil {
		return nil, invalidRequest("DetachOSD retains stores only with WithOSDVolumeStorage")
	}
	osd := c.osds[id]
	if osd == nil {
		if c.detachedOSDs[id] != nil {
			return nil, invalid(fmt.Errorf("osd.%d is already detached", id))
		}
		return nil, invalid(fmt.Errorf("osd.%d is not owned by this cluster", id))
	}
	if osd.purged || osd.purgeIssued {
		return nil, fmt.Errorf("osd.%d is being removed; retry RemoveOSD", id)
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
	store, err := c.osdVolume.retainedStore(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := store.matches(id, osd.nativeUUID, fsid); err != nil {
		return nil, err
	}
	detached := &DetachedOSD{cluster: c, id: id, osdUUID: osd.nativeUUID, fsid: fsid, placement: osd.placement, store: store}
	if c.detachedOSDs == nil {
		c.detachedOSDs = make(map[int]*DetachedOSD)
	}
	c.detachedOSDs[id] = detached
	delete(c.osds, id)
	return detached, nil
}

// ReopenOSD starts a new container on a store and registration that
// DetachOSD of this cluster retained, and waits for the OSD to be up/in. It
// returns a new *OSDContainer with the same ID, UUID and placement; the
// detached OSD's previous handle keeps naming the removed container.
//
// Before any write it requires the store's identity files, keyring and block
// inode and size to be unchanged since the detach, the MON registration to
// hold the same UUID and be down, and the MONs' key for the OSD to equal the
// stored one. A refusal there, a failed cleanup of the stale admin socket,
// or a canceled context before the container is requested leaves the OSD
// detached for another attempt; none of them dispatches a creation request.
// Once the new container is requested the detached OSD is consumed: an
// error returns the new handle, which the cluster owns. If that handle has a
// container, detach it again, remove it or terminate the cluster. If the
// creation request returned no container, its reply may have been lost after
// the runtime accepted it, so it is never replayed and the handle has no
// container to detach: DetachOSD and ReopenOSD refuse it, RemoveOSD refuses
// the last OSD, and only Terminate recovers it. ReopenOSD does not reconcile
// a container that such a lost reply may have created. The store is started through the
// ordinary bootstrap, which validates the stored identity again and never
// formats a store that has its ready marker.
func (c *Container) ReopenOSD(ctx context.Context, detached *DetachedOSD) (*OSDContainer, error) {
	if gateErr := c.sharedGate("ReopenOSD"); gateErr != nil {
		return nil, gateErr
	}
	if detached == nil {
		return nil, invalidRequest("ReopenOSD needs a detached OSD")
	}
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
	if c.osdVolume == nil {
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
	if err := c.clearAdminSocket(ctx, fmt.Sprintf("osd.%d", id)); err != nil {
		return nil, err
	}
	// Last check before the irreversible transition.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	detached.reopened = true
	delete(c.detachedOSDs, id)
	osd := &OSDContainer{ID: id, nativeUUID: detached.osdUUID, placement: detached.placement}
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

// reopenPreflight reads, without writing, everything that a reopen relies on.
func (c *Container) reopenPreflight(ctx context.Context, detached *DetachedOSD) error {
	id := detached.id
	store, err := c.osdVolume.retainedStore(ctx, id)
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
