package ceph

import (
	"context"
	"fmt"
	"slices"

	"github.com/google/uuid"
)

// OSDStates reads the native OSDMap and compares it with the owned OSD
// registrations. The exported method still takes the topology lock and the
// liveness check, then hands this code a copy of the registrations, so the
// ownership rules and the command order can be driven by a fake query without a
// cluster. Container.ownedOSDState delegates here too, so there is one rule for
// what makes an OSD "owned and unchanged".

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

// readOwnedOSDStates runs one `osd dump` and resolves every owned ID against it,
// sorted by native ID. The first unresolved ID fails the whole observation.
func readOwnedOSDStates(ctx context.Context, run cliQuery, owned map[int]*osdRegistration) ([]OSDState, error) {
	dump, err := readOSDPolicyDump(ctx, run)
	if err != nil {
		return nil, err
	}
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
