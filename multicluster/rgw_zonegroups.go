package multicluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// Zonegroups returns a sorted snapshot of the owned groups and their attached
// zones. Detached zones are omitted after their removal period is committed.
func (f *RGWMultisite) Zonegroups() []RGWZonegroup {
	f.topologyMu.Lock()
	defer f.topologyMu.Unlock()
	groups := make(map[string]*RGWZonegroup)
	for _, state := range f.zoneStates() {
		name := state.Zonegroup
		group := groups[name]
		if group == nil {
			group = &RGWZonegroup{Name: name, ID: f.groupIDs[name], MasterZone: f.groupMasters[name]}
			groups[name] = group
		}
		group.Zones = append(group.Zones, state.RGWZone)
	}
	result := make([]RGWZonegroup, 0, len(groups))
	for _, group := range groups {
		slices.SortFunc(group.Zones, func(a, b RGWZone) int { return strings.Compare(a.Name, b.Name) })
		result = append(result, *group)
	}
	slices.SortFunc(result, func(a, b RGWZonegroup) int { return strings.Compare(a.Name, b.Name) })
	return result
}

// AddZonegroup adds a non-master region with one or more fresh independent Ceph
// clusters. MasterZone selects its local master. The realm metadata master is
// unchanged. Metadata is shared across regions; object data replicates within
// each region. Existing S3 endpoint mappings must be refreshed after reload.
// On error, any newly created groups/zones/containers remain owned for Terminate.
// image selects the new gateways. Setup clients use each new cluster's control
// image unless the fixture was configured with ControlImage.
func (f *RGWMultisite) AddZonegroup(ctx context.Context, image string, config RGWZonegroupConfig, opts ...testcontainers.ContainerCustomizer) (*RGWZonegroup, error) {
	if f == nil || !validRGWZoneName(config.Name) || len(config.Zones) == 0 {
		return nil, errors.New("RGW zonegroup requires a valid name and at least one zone")
	}
	if err := lockRGWSyncObservation(ctx, &f.topologyMu); err != nil {
		return nil, err
	}
	defer f.topologyMu.Unlock()
	if _, exists := f.groupIDs[config.Name]; exists {
		return nil, fmt.Errorf("RGW zonegroup %s is already owned", config.Name)
	}
	zones := slices.Clone(config.Zones)
	master := config.MasterZone
	if master == "" {
		master = zones[0].Name
	}
	index := slices.IndexFunc(zones, func(zone RGWZoneConfig) bool { return zone.Name == master })
	if index < 0 {
		return nil, errors.New("RGW zonegroup master must name one of its zones")
	}
	selected := zones[index]
	zones = append([]RGWZoneConfig{selected}, slices.Delete(zones, index, index+1)...)
	for i := range zones {
		if zones[i].Zonegroup != "" && zones[i].Zonegroup != config.Name {
			return nil, errors.New("conflicting RGW zonegroup name")
		}
		zones[i].Zonegroup = config.Name
		if !validRGWZoneName(zones[i].Name) {
			return nil, errors.New("invalid RGW zone name")
		}
		for _, existing := range f.zoneStates() {
			if existing.Name == zones[i].Name {
				return nil, fmt.Errorf("RGW zone %s is already owned", zones[i].Name)
			}
			if err := validatePair(image, existing.cluster, zones[i].Cluster); err != nil {
				return nil, err
			}
		}
		for j := range i {
			if zones[j].Name == zones[i].Name {
				return nil, errors.New("RGW zones must have unique names")
			}
			if err := validatePair(image, zones[j].Cluster, zones[i].Cluster); err != nil {
				return nil, err
			}
		}
	}
	result := &RGWZonegroup{Name: config.Name, MasterZone: master}
	for i, zone := range zones {
		created, err := f.addZone(ctx, image, zone, i == 0, opts...)
		if created != nil {
			result.Zones = append(result.Zones, *created)
		}
		result.ID = f.groupIDs[config.Name]
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

// RemoveZone detaches one owned non-master zone from its zonegroup, commits and
// propagates the new period, then removes that zone's owned gateway and CLI.
// The realm, zone configuration, users, pools and stored objects remain in its
// storage cluster; this is not a destructive zone/pool purge. Detached names and
// clusters cannot be adopted through AddZone. The final zone of a group and any
// current realm/group master must first be replaced or promoted explicitly.
// Partial period propagation/container cleanup can be retried with the same
// name. Terminate also removes any remaining owned containers. Remaining gateway
// application endpoints may change on bridge-mode reload.
func (f *RGWMultisite) RemoveZone(ctx context.Context, name string) error {
	if f == nil || !validRGWZoneName(name) {
		return errors.New("invalid RGW fixture or zone name")
	}
	if err := lockRGWSyncObservation(ctx, &f.topologyMu); err != nil {
		return err
	}
	defer f.topologyMu.Unlock()
	if f.closed {
		return errors.New("RGW multisite is terminated")
	}
	for pendingName, pending := range f.removedZones {
		if pendingName != name && (!pending.converged || !pending.gatewayRemoved || !pending.clientRemoved) {
			return errors.New("finish the pending RGW zone removal before detaching another zone")
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()
	removal := f.removedZones[name]
	states := f.zoneStates()
	if len(states) == 0 {
		return errors.New("RGW multisite has no attached zones")
	}
	data, err := f.admin(ctx, states[0].client, states[0].Name, "period", "get", "--format", "json")
	if err != nil {
		return err
	}
	period, err := decodeRGWTopologyPeriod(data)
	if err != nil {
		return err
	}
	master := rgwPeriodMaster(period, states)
	if master == nil {
		return errors.New("current RGW metadata master is not an attached owned zone")
	}
	if removal == nil {
		state, err := f.removableZone(period, master, states, name)
		if err != nil {
			return err
		}
		removal = &rgwZoneRemoval{state: state}
		if f.removedZones == nil {
			f.removedZones = make(map[string]*rgwZoneRemoval)
		}
		f.removedZones[name] = removal
	}
	state := removal.state
	if state.ID == period.MasterZone {
		return errors.New("cannot finish removal of the current RGW realm metadata master")
	}
	for _, group := range period.PeriodMap.Zonegroups {
		if group.MasterZone == state.ID {
			return errors.New("cannot finish removal of the current RGW zonegroup master")
		}
	}
	if err := f.checkZonePeriod(period, master, states); err != nil {
		// A commit can succeed while its response is lost. Accept only the
		// exact graph with this target removed, never an unrelated mutation.
		remaining := slices.DeleteFunc(slices.Clone(states), func(zone *rgwZoneState) bool { return zone.Name == name })
		if removal.detached || f.checkZonePeriod(period, master, remaining) != nil {
			return err
		}
	}
	if !removal.detached {
		// Fence the leaving gateway before removing it from the committed graph.
		if state.Gateway == nil || state.client == nil {
			return errors.New("cannot detach a partially initialized RGW zone")
		}
		status, err := state.Gateway.State(ctx)
		if err != nil || status == nil {
			return fmt.Errorf("inspect leaving RGW zone: %v", err)
		}
		if status.Running {
			stop := 3 * time.Second
			if err := state.Gateway.Stop(ctx, &stop); err != nil {
				return err
			}
		}
		// Read the master's local group configuration before mutating it. This
		// handles a successful remove whose previous Exec response was lost.
		groupData, err := f.adminInGroup(ctx, master.client, state.Zonegroup, state.Name, "zonegroup", "get")
		if err != nil {
			return err
		}
		var group struct {
			Zones []struct{ ID, Name string } `json:"zones"`
		}
		if err := json.Unmarshal(groupData, &group); err != nil {
			return err
		}
		present := slices.ContainsFunc(group.Zones, func(zone struct{ ID, Name string }) bool { return zone.ID == state.ID })
		if present {
			if _, err := f.adminInGroup(ctx, master.client, state.Zonegroup, state.Name, "zonegroup", "remove", "--zone-id", state.ID); err != nil {
				return err
			}
		}
		if _, err := f.admin(ctx, master.client, master.Name, "period", "update", "--commit"); err != nil {
			return err
		}
		removal.detached = true
		states = f.zoneStates()
	}
	if !removal.converged {
		if err := f.waitZonePeriods(ctx, master, states); err != nil {
			return err
		}
		if err := f.reloadZoneGateways(ctx, states, nil); err != nil {
			return err
		}
		removal.converged = true
	}
	if !removal.gatewayRemoved {
		if state.cluster != nil {
			if err := state.cluster.RemoveRGW(ctx, state.Gateway.GatewayName); err != nil {
				return fmt.Errorf("remove detached RGW gateway: %w", err)
			}
		} else if err := ignoreMissing(state.Gateway.Terminate(ctx)); err != nil {
			return err
		}
		removal.gatewayRemoved = true
	}
	if !removal.clientRemoved {
		if err := ignoreMissing(state.client.Terminate(ctx)); err != nil {
			return fmt.Errorf("remove detached RGW CLI: %w", err)
		}
		removal.clientRemoved = true
	}
	return nil
}

func rgwPeriodMaster(period *rgwTopologyPeriod, states []*rgwZoneState) *rgwZoneState {
	for _, state := range states {
		if state.ID == period.MasterZone {
			return state
		}
	}
	return nil
}

func (f *RGWMultisite) removableZone(period *rgwTopologyPeriod, master *rgwZoneState, states []*rgwZoneState, name string) (*rgwZoneState, error) {
	if err := f.checkZonePeriod(period, master, states); err != nil {
		return nil, err
	}
	var target *rgwZoneState
	for _, state := range states {
		if state.Name == name {
			target = state
		}
	}
	if target == nil {
		return nil, fmt.Errorf("RGW zone %s is not attached and owned", name)
	}
	if target.ID == master.ID {
		return nil, errors.New("cannot remove the current RGW realm metadata master")
	}
	for _, group := range period.PeriodMap.Zonegroups {
		if group.Name == target.Zonegroup {
			if group.MasterZone == target.ID {
				return nil, errors.New("cannot remove the current RGW zonegroup master")
			}
			if len(group.Zones) < 2 {
				return nil, errors.New("cannot remove the final RGW zonegroup zone")
			}
		}
	}
	return target, nil
}
