package ceph

import (
	"context"
	"fmt"
	"slices"

	"github.com/google/uuid"
)

// Observer.OSDStates and the OSD watch compare the native OSDMap with the
// owned OSD registrations. ownedOSDSample takes the topology lock and the
// liveness check, reads one osd dump and copies the registrations under that
// lock; the resolution below runs on the copies without it, so the ownership
// rules can be driven by a decoded dump without a cluster.
// osdRegistry.ownedState and the OSD watch resolve through the same
// functions, so there is one rule for what makes an OSD "owned and
// unchanged".

// osdRegistration is the part of an owned OSD that the observation consults: the
// purge marker and the UUID that registration recorded.
type osdRegistration struct {
	purged     bool
	nativeUUID string
}

// registrationOf copies the registration fields; a nil container has no
// registration, which resolveOwnedOSDState reports as not owned.
func registrationOf(osd *OSDContainer) *osdRegistration {
	if osd == nil {
		return nil
	}
	return &osdRegistration{purged: osd.purged, nativeUUID: osd.nativeUUID}
}

// resolveOwnedOSDState returns the native state of one owned OSD. A missing or
// purged registration, an unusable registered UUID, an absent map entry and a
// replaced native UUID are all errors; none is decoded into an empty state.
func resolveOwnedOSDState(dump osdPolicySnapshot, id int, reg *osdRegistration) (OSDState, error) {
	if reg == nil || reg.purged {
		return OSDState{}, fmt.Errorf("osd.%d is not an active owned OSD", id)
	}
	expected, err := uuid.Parse(reg.nativeUUID)
	if err != nil || expected == uuid.Nil {
		return OSDState{}, fmt.Errorf("osd.%d registration UUID is unavailable", id)
	}
	state, exists := dump.osds[id]
	if !exists {
		return OSDState{}, fmt.Errorf("owned osd.%d is missing from native OSDMap", id)
	}
	actual, err := uuid.Parse(state.UUID)
	if err != nil || actual != expected {
		return OSDState{}, fmt.Errorf("native osd.%d UUID no longer matches its owned registration", id)
	}
	return state, nil
}

// resolveOwnedOSDStates resolves every owned ID against one dump, sorted by
// native ID. The first unresolved ID fails the whole observation.
func resolveOwnedOSDStates(dump osdPolicySnapshot, owned map[int]*osdRegistration) ([]OSDState, error) {
	states := make([]OSDState, 0, len(owned))
	for id, reg := range owned {
		state, err := resolveOwnedOSDState(dump, id, reg)
		if err != nil {
			return nil, err
		}
		states = append(states, state)
	}
	slices.SortFunc(states, func(a, b OSDState) int { return a.ID - b.ID })
	return states, nil
}

// osdStates implements Observer.OSDStates.
func (c *Container) osdStates(ctx context.Context) ([]OSDState, error) {
	dump, owned, err := c.ownedOSDSample(ctx)
	if err != nil {
		return nil, err
	}
	return resolveOwnedOSDStates(dump, owned)
}

// ownedOSDSample runs one osd dump under the topology lock and copies the
// owned registrations that the same lock protects, so the dump and the
// registry describe one serialized observation. The lock is released before it
// returns; the caller resolves the copies without it.
func (c *Container) ownedOSDSample(ctx context.Context) (osdPolicySnapshot, map[int]*osdRegistration, error) {
	if err := c.lockTopology(ctx); err != nil {
		return osdPolicySnapshot{}, nil, err
	}
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return osdPolicySnapshot{}, nil, err
	}
	owned := make(map[int]*osdRegistration, c.fleet.osdHandles.count())
	for id, osd := range c.fleet.osdHandles.each() {
		owned[id] = registrationOf(osd)
	}
	dump, err := readOSDPolicyDump(ctx, c.conn().ceph)
	if err != nil {
		return osdPolicySnapshot{}, nil, err
	}
	return dump, owned, nil
}
