package multicluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// RGWZoneConfig assigns one fresh, independent storage cluster to a named zone.
type RGWZoneConfig struct {
	Name, Zonegroup string
	Cluster         *ceph.Container
}

// RGWZonegroupConfig defines a region and its local master zone. MasterZone
// defaults to the first zone. Object data replicates within each zonegroup;
// users and bucket metadata share the realm across zonegroups.
type RGWZonegroupConfig struct {
	Name, MasterZone string
	Zones            []RGWZoneConfig
}

// RGWTopologyConfig creates one realm spanning at least two independent zones.
// Use Zones/Zonegroup/MetadataMaster for a single region, or Zonegroups and
// MasterZonegroup for several regions with separately selected local masters.
// MetadataMaster defaults to the first zone; MasterZonegroup to the first group.
// All clusters use the same network mode and must not serve standalone RGW data.
// ControlImage optionally overrides every zone's setup client image. Otherwise
// each client uses its own cluster's control image, including added zones.
type RGWTopologyConfig struct {
	Zones                            []RGWZoneConfig
	Realm, Zonegroup, MetadataMaster string
	ControlImage                     string
	Zonegroups                       []RGWZonegroupConfig
	MasterZonegroup                  string
}

// RGWZone describes one owned multisite zone. PeerEndpoint is reachable by
// gateways/CLI containers in the fixture's HTTP network or Docker host namespace.
// Applications obtain their endpoint separately with Gateway.S3Endpoint.
type RGWZone struct {
	Name, ID, Zonegroup, PeerEndpoint string
	Gateway                           *ceph.RGWContainer
}

// RGWZonegroup is an owned region snapshot. MasterZone reflects the last
// topology operation; direct native promotions require a subsequent operation
// to refresh it. Each zone retains its own storage cluster and daemon endpoint.
type RGWZonegroup struct {
	Name, ID, MasterZone string
	Zones                []RGWZone
}

type rgwZoneRemoval struct {
	state                                              *rgwZoneState
	detached, converged, gatewayRemoved, clientRemoved bool
}

type rgwZoneState struct {
	RGWZone
	cluster *ceph.Container
	client  testcontainers.Container
}

// RunRGWTopology creates a multi-zone fixture using the same ownership and
// cleanup contract as RunRGWMultisite. The initial master becomes Source, and
// the first remaining zone becomes Destination for compatibility. Zones returns
// all sites. A non-nil result on error owns partial resources and needs cleanup.
// image selects RGW gateways; each setup client uses its cluster's control
// image unless ControlImage supplies a shared override.
func RunRGWTopology(ctx context.Context, image string, config RGWTopologyConfig, opts ...testcontainers.ContainerCustomizer) (*RGWMultisite, error) {
	zones, err := prepareRGWTopology(image, config)
	if err != nil {
		return nil, err
	}
	for _, zone := range zones {
		if err := ensureFreshRGWCluster(ctx, zone.Cluster); err != nil {
			return nil, err
		}
	}
	f, err := RunRGWMultisite(ctx, image, RGWMultisiteConfig{
		Source: zones[0].Cluster, Destination: zones[1].Cluster,
		SourceZone: zones[0].Name, DestinationZone: zones[1].Name,
		Realm: config.Realm, Zonegroup: zones[0].Zonegroup, ControlImage: config.ControlImage,
		destinationZonegroup: zones[1].Zonegroup,
	}, opts...)
	if err != nil {
		return f, err
	}
	if len(zones) > 2 {
		// The outer constructor still owns fresh initialization and has not
		// exposed f. Additional remote commits need a final master-local
		// staging snapshot of the complete graph before returning it.
		f.bootstrapStagingPending = true
	}
	for _, zone := range zones[2:] {
		f.topologyMu.Lock()
		_, exists := f.groupIDs[zone.Zonegroup]
		_, err := f.addZone(ctx, image, zone, !exists, opts...)
		f.topologyMu.Unlock()
		if err != nil {
			return f, err
		}
	}
	if len(zones) > 2 {
		if err := f.canonicalizeBootstrapStaging(ctx); err != nil {
			return f, err
		}
	}
	return f, nil
}

func prepareRGWTopology(image string, config RGWTopologyConfig) ([]RGWZoneConfig, error) {
	zones, err := normalizeRGWTopology(config)
	if err != nil {
		return nil, err
	}
	for i, zone := range zones {
		for j := range i {
			if err := validatePair(image, zones[j].Cluster, zone.Cluster); err != nil {
				return nil, fmt.Errorf("RGW zones %s/%s: %w", zones[j].Name, zone.Name, err)
			}
		}
	}
	return zones, nil
}

func normalizeRGWTopology(config RGWTopologyConfig) ([]RGWZoneConfig, error) {
	if len(config.Zonegroups) > 0 {
		if len(config.Zones) > 0 || config.Zonegroup != "" || config.MetadataMaster != "" {
			return nil, errors.New("RGW topology must use either Zones or Zonegroups")
		}
		groups := slices.Clone(config.Zonegroups)
		masterGroup := config.MasterZonegroup
		if masterGroup == "" {
			masterGroup = groups[0].Name
		}
		var zones []RGWZoneConfig
		var masterName string
		seen := make(map[string]bool)
		for _, group := range groups {
			if !validRGWZoneName(group.Name) || seen[group.Name] || len(group.Zones) == 0 {
				return nil, errors.New("RGW zonegroups require unique valid names and at least one zone")
			}
			seen[group.Name] = true
			ordered := slices.Clone(group.Zones)
			master := group.MasterZone
			if master == "" {
				master = ordered[0].Name
			}
			index := slices.IndexFunc(ordered, func(zone RGWZoneConfig) bool { return zone.Name == master })
			if index < 0 {
				return nil, errors.New("RGW zonegroup master must name a zone in that group")
			}
			selected := ordered[index]
			ordered = slices.Delete(ordered, index, index+1)
			ordered = append([]RGWZoneConfig{selected}, ordered...)
			for i := range ordered {
				if ordered[i].Zonegroup != "" && ordered[i].Zonegroup != group.Name {
					return nil, errors.New("RGW zone has conflicting zonegroup")
				}
				ordered[i].Zonegroup = group.Name
			}
			if group.Name == masterGroup {
				masterName = master
				zones = append(ordered, zones...)
			} else {
				zones = append(zones, ordered...)
			}
		}
		if masterName == "" {
			return nil, errors.New("RGW master zonegroup must name a configured group")
		}
		config.Zones, config.MetadataMaster = zones, masterName
	} else {
		if config.MasterZonegroup != "" {
			return nil, errors.New("MasterZonegroup requires Zonegroups")
		}
		if config.Zonegroup == "" {
			config.Zonegroup = "us-east-1"
		}
		if !validRGWZoneName(config.Zonegroup) {
			return nil, errors.New("invalid RGW zonegroup name")
		}
		config.Zones = slices.Clone(config.Zones)
		for i := range config.Zones {
			if config.Zones[i].Zonegroup != "" && config.Zones[i].Zonegroup != config.Zonegroup {
				return nil, errors.New("flat RGW topology uses one zonegroup; use Zonegroups for multiple groups")
			}
			config.Zones[i].Zonegroup = config.Zonegroup
		}
	}
	if len(config.Zones) < 2 {
		return nil, errors.New("RGW multisite topology requires at least two zones")
	}
	zones := slices.Clone(config.Zones)
	master := config.MetadataMaster
	if master == "" {
		master = zones[0].Name
	}
	masterIndex := -1
	for i, zone := range zones {
		if !validRGWZoneName(zone.Name) {
			return nil, errors.New("invalid RGW zone name")
		}
		if zone.Name == master {
			masterIndex = i
		}
		for j := range i {
			if zones[j].Name == zone.Name {
				return nil, fmt.Errorf("duplicate RGW zone %s", zone.Name)
			}
		}
	}
	if masterIndex < 0 {
		return nil, errors.New("RGW metadata master must name a configured zone")
	}
	// Move the initial master to the front without reordering the other sites.
	selected := zones[masterIndex]
	zones = append(slices.Delete(zones, masterIndex, masterIndex+1), RGWZoneConfig{})
	copy(zones[1:], zones[:len(zones)-1])
	zones[0] = selected
	return zones, nil
}

func validRGWZoneName(name string) bool {
	return name != "" && strings.TrimSpace(name) == name && !strings.HasPrefix(name, "-") && strings.IndexFunc(name, unicode.IsControl) == -1
}

// Zones returns owned zone descriptors sorted by name, including partial zones
// returned by a failed AddZone. It does not expose private system credentials.
func (f *RGWMultisite) Zones() []RGWZone {
	f.topologyMu.Lock()
	defer f.topologyMu.Unlock()
	states := f.zoneStates()
	result := make([]RGWZone, 0, len(states))
	for _, state := range states {
		result = append(result, state.RGWZone)
	}
	slices.SortFunc(result, func(a, b RGWZone) int { return strings.Compare(a.Name, b.Name) })
	return result
}

// zoneStates is called while topologyMu is held.
func (f *RGWMultisite) zoneStates() []*rgwZoneState {
	states := []*rgwZoneState{
		{RGWZone: RGWZone{Name: f.config.SourceZone, ID: f.SourceZoneID, Zonegroup: f.zoneGroupName(f.config.SourceZone), PeerEndpoint: f.sourceURL, Gateway: f.Source}, cluster: f.config.Source, client: f.sourceClient},
		{RGWZone: RGWZone{Name: f.config.DestinationZone, ID: f.DestinationZoneID, Zonegroup: f.zoneGroupName(f.config.DestinationZone), PeerEndpoint: f.destinationURL, Gateway: f.Destination}, cluster: f.config.Destination, client: f.destinationClient},
	}
	for _, zone := range f.additionalZones {
		states = append(states, zone)
	}
	return slices.DeleteFunc(states, func(state *rgwZoneState) bool {
		removal := f.removedZones[state.Name]
		return removal != nil && removal.detached
	})
}

// ZoneAdmin runs native radosgw-admin in any owned zone, including a partial
// zone whose CLI client started successfully. Do not edit period/zone state
// concurrently with AddZone or external metadata master transitions.
func (f *RGWMultisite) ZoneAdmin(ctx context.Context, name string, args ...string) ([]byte, error) {
	if err := lockRGWSyncObservation(ctx, &f.topologyMu); err != nil {
		return nil, err
	}
	var client testcontainers.Container
	if !f.closed {
		for _, zone := range f.zoneStates() {
			if zone.Name == name {
				client = zone.client
				break
			}
		}
	}
	f.topologyMu.Unlock()
	if client == nil {
		return nil, fmt.Errorf("RGW zone %s has no running owned CLI client", name)
	}
	return f.admin(ctx, client, name, args...)
}

// AddZone joins another fresh storage cluster to the existing realm/zonegroup.
// It selects the metadata master from the current committed period, verifies
// every registered zone's identity and endpoint, and refreshes the period in
// all sites. Existing running gateways restart to load the final period,
// retaining their listener ports and storage; fenced gateways stay stopped.
// The new gateway reloads only if its host-mode endpoint changed after startup.
// Running secondaries finish initial metadata full sync before reload. Complete
// any external master transition first, or keep the previous master fenced.
// Refresh S3Endpoint afterwards because bridge-mode published ports may change.
// A non-nil partial result remains owned on error; terminate the fixture if
// initialization fails rather than adopting an external zone.
// image selects the new gateway. Its setup client uses the new cluster's
// control image unless the fixture was configured with ControlImage.
func (f *RGWMultisite) AddZone(ctx context.Context, image string, config RGWZoneConfig, opts ...testcontainers.ContainerCustomizer) (*RGWZone, error) {
	if f == nil || !validRGWZoneName(config.Name) {
		return nil, errors.New("invalid RGW fixture or zone name")
	}
	if err := lockRGWSyncObservation(ctx, &f.topologyMu); err != nil {
		return nil, err
	}
	defer f.topologyMu.Unlock()
	return f.addZone(ctx, image, config, false, opts...)
}

func (f *RGWMultisite) addZone(ctx context.Context, image string, config RGWZoneConfig, createGroup bool, opts ...testcontainers.ContainerCustomizer) (*RGWZone, error) {
	if config.Zonegroup == "" {
		config.Zonegroup = f.config.Zonegroup
	}
	if !validRGWZoneName(config.Name) || !validRGWZoneName(config.Zonegroup) {
		return nil, errors.New("invalid RGW zone or zonegroup name")
	}
	if removal := f.removedZones[config.Name]; removal != nil {
		return nil, errors.New("detached RGW zones retain configuration and cannot be adopted as fresh zones")
	}
	for _, removal := range f.removedZones {
		if !removal.converged || !removal.gatewayRemoved || !removal.clientRemoved {
			return nil, errors.New("finish the pending RGW zone removal before adding zones")
		}
		if removal.state.cluster == config.Cluster {
			return nil, errors.New("detached RGW storage clusters retain configuration and cannot be adopted as fresh zones")
		}
	}
	_, knownGroup := f.groupIDs[config.Zonegroup]
	if knownGroup == createGroup {
		return nil, errors.New("AddZone requires an owned zonegroup; AddZonegroup requires a fresh group name")
	}
	if f.closed || f.Source == nil || f.Destination == nil || f.RealmID == "" || f.systemAccess == "" || f.systemSecret == "" {
		return nil, errors.New("RGW multisite must be initialized and running")
	}
	for _, zone := range f.zoneStates() {
		if zone.Name == config.Name {
			return nil, fmt.Errorf("RGW zone %s is already owned", config.Name)
		}
		if err := validatePair(image, zone.cluster, config.Cluster); err != nil {
			return nil, err
		}
		if zone.ID == "" || zone.client == nil || zone.Gateway == nil {
			return nil, errors.New("finish initializing existing RGW zones before adding another")
		}
	}
	clientImage, err := rgwControlImage(f.config.ControlImage, config.Cluster)
	if err != nil {
		return nil, err
	}
	if err := ensureFreshRGWCluster(ctx, config.Cluster); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()
	data, err := f.admin(ctx, f.zoneStates()[0].client, f.zoneStates()[0].Name, "period", "get", "--format", "json")
	if err != nil {
		return nil, err
	}
	period, err := decodeRGWTopologyPeriod(data)
	if err != nil {
		return nil, err
	}
	var master *rgwZoneState
	for _, zone := range f.zoneStates() {
		if zone.ID == period.MasterZone {
			master = zone
		}
	}
	if master == nil {
		return nil, errors.New("current RGW metadata master is not an owned zone")
	}
	if err := f.checkZonePeriod(period, master, f.zoneStates()); err != nil {
		return nil, err
	}
	for _, group := range period.PeriodMap.Zonegroups {
		for _, zone := range group.Zones {
			if zone.Name == config.Name {
				return nil, fmt.Errorf("RGW zone %s already exists in the committed period", config.Name)
			}
		}
	}
	state := &rgwZoneState{RGWZone: RGWZone{Name: config.Name, Zonegroup: config.Zonegroup}, cluster: config.Cluster}
	if f.additionalZones == nil {
		f.additionalZones = make(map[string]*rgwZoneState)
	}
	f.additionalZones[config.Name] = state
	f.scopeMu.Lock()
	if f.zoneGroups == nil {
		f.zoneGroups = make(map[string]string)
	}
	f.zoneGroups[config.Name] = config.Zonegroup
	f.scopeMu.Unlock()
	peerNetwork := ""
	if f.httpNetwork != nil {
		peerNetwork = f.httpNetwork.Name
	}
	state.client, err = runClient(ctx, clientImage, config.Cluster, peerNetwork, &f.owned)
	if err != nil {
		return &state.RGWZone, err
	}
	alias := "rgw-zone-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	if !config.Cluster.UsesHostNetwork() {
		state.PeerEndpoint = "http://" + alias + ":7480"
	}
	commands := [][]string{
		{"realm", "pull", "--url", master.PeerEndpoint, "--access-key", f.systemAccess, "--secret", f.systemSecret},
		{"realm", "default"},
	}
	if createGroup {
		commands = append(commands, rgwEndpointArgs([]string{"zonegroup", "create", "--api-name", config.Zonegroup}, state.PeerEndpoint))
	}
	create := []string{"zone", "create", "--access-key", f.systemAccess, "--secret", f.systemSecret}
	if createGroup {
		create = append(create, "--master")
	}
	commands = append(commands, rgwEndpointArgs(create, state.PeerEndpoint), []string{"period", "update", "--commit"})
	for _, command := range commands {
		if _, err := f.admin(ctx, state.client, config.Name, command...); err != nil {
			return &state.RGWZone, fmt.Errorf("join RGW zone %s: %w", config.Name, err)
		}
	}
	state.Gateway, err = f.runGateway(ctx, image, config.Cluster, f.httpNetwork, alias, config.Name, opts...)
	if err != nil {
		return &state.RGWZone, err
	}
	if config.Cluster.UsesHostNetwork() {
		state.PeerEndpoint, err = state.Gateway.DaemonEndpoint(ctx)
		if err != nil {
			return &state.RGWZone, err
		}
		commands := [][]string{{"zone", "modify", "--endpoints", state.PeerEndpoint}}
		if createGroup {
			commands = append(commands, []string{"zonegroup", "modify", "--endpoints", state.PeerEndpoint})
		}
		commands = append(commands, []string{"period", "update", "--commit"})
		for _, command := range commands {
			if _, err := f.admin(ctx, state.client, config.Name, command...); err != nil {
				return &state.RGWZone, err
			}
		}
	}
	identity, err := f.admin(ctx, state.client, config.Name, "zone", "get")
	if err != nil {
		return &state.RGWZone, err
	}
	var native struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(identity, &native); err != nil || native.ID == "" {
		return &state.RGWZone, errors.New("invalid added RGW zone identity")
	}
	state.ID = native.ID
	if createGroup {
		data, err := f.admin(ctx, state.client, state.Name, "zonegroup", "get")
		if err != nil {
			return &state.RGWZone, err
		}
		var native struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(data, &native); err != nil || native.ID == "" {
			return &state.RGWZone, errors.New("invalid added RGW zonegroup identity")
		}
		f.groupIDs[config.Zonegroup] = native.ID
		f.groupMasters[config.Zonegroup] = state.Name
	}
	states := f.zoneStates()
	if err := f.waitZonePeriods(ctx, master, states); err != nil {
		return &state.RGWZone, err
	}
	if err := f.waitZoneMetadataBootstrap(ctx, master, states); err != nil {
		return &state.RGWZone, err
	}
	if err := f.reloadZoneGateways(ctx, states, func(zone *rgwZoneState) bool { return zone == state && !config.Cluster.UsesHostNetwork() }); err != nil {
		return &state.RGWZone, err
	}
	state.Gateway.AccessKey, state.Gateway.SecretKey = f.Source.AccessKey, f.Source.SecretKey
	return &state.RGWZone, nil
}

func (f *RGWMultisite) reloadZoneGateways(ctx context.Context, states []*rgwZoneState, skip func(*rgwZoneState) bool) error {
	for _, zone := range states {
		// A bridge gateway starts after its endpoint was committed, so the
		// new zone already loaded the final period. Avoid interrupting its
		// initial metadata full sync just to reload the same configuration.
		if skip != nil && skip(zone) {
			continue
		}
		status, err := zone.Gateway.State(ctx)
		if err != nil || status == nil {
			return fmt.Errorf("inspect zone %s before period reload: %v", zone.Name, err)
		}
		if !status.Running {
			if !f.reloadNeeded[zone.Name] {
				continue
			}
		} else {
			if f.reloadNeeded == nil {
				f.reloadNeeded = make(map[string]bool)
			}
			f.reloadNeeded[zone.Name] = true
			stop := 3 * time.Second
			if err := zone.Gateway.Stop(ctx, &stop); err != nil {
				return fmt.Errorf("stop zone %s before period reload: %w", zone.Name, err)
			}
		}
		if err := zone.Gateway.Start(ctx); err != nil {
			return fmt.Errorf("restart zone %s after period reload: %w", zone.Name, err)
		}
		delete(f.reloadNeeded, zone.Name)
	}
	return nil
}

type rgwTopologyPeriod struct {
	RealmID         string `json:"realm_id"`
	MasterZone      string `json:"master_zone"`
	MasterZonegroup string `json:"master_zonegroup"`
	PeriodMap       struct {
		Zonegroups []struct {
			ID         string   `json:"id"`
			Name       string   `json:"name"`
			MasterZone string   `json:"master_zone"`
			Endpoints  []string `json:"endpoints"`
			Zones      []struct {
				ID, Name  string
				Endpoints []string `json:"endpoints"`
			} `json:"zones"`
		} `json:"zonegroups"`
	} `json:"period_map"`
}

func decodeRGWTopologyPeriod(data []byte) (*rgwTopologyPeriod, error) {
	var period rgwTopologyPeriod
	if err := json.Unmarshal(data, &period); err != nil || period.RealmID == "" || period.MasterZone == "" || period.PeriodMap.Zonegroups == nil {
		return nil, errors.New("invalid committed RGW topology period")
	}
	return &period, nil
}

func (f *RGWMultisite) checkZonePeriod(period *rgwTopologyPeriod, master *rgwZoneState, states []*rgwZoneState) error {
	if period.RealmID != f.RealmID || period.MasterZone != master.ID {
		return errors.New("committed RGW topology realm/master identity mismatch")
	}
	groups := make(map[string][]*rgwZoneState)
	for _, state := range states {
		name := state.Zonegroup
		if name == "" {
			name = f.config.Zonegroup
		}
		groups[name] = append(groups[name], state)
	}
	if len(period.PeriodMap.Zonegroups) != len(groups) {
		return errors.New("committed RGW topology contains unexpected zonegroups")
	}
	seen := make(map[string]bool)
	for _, group := range period.PeriodMap.Zonegroups {
		expected, ok := groups[group.Name]
		if !ok || seen[group.Name] || len(group.Zones) != len(expected) {
			return errors.New("committed RGW zonegroup contains unexpected zones or duplicate groups")
		}
		seen[group.Name] = true
		if id := f.groupIDs[group.Name]; id != "" && id != group.ID {
			return errors.New("committed RGW zonegroup identity mismatch")
		}
		var groupMaster *rgwZoneState
		for _, zone := range expected {
			if zone.ID == group.MasterZone {
				groupMaster = zone
			}
		}
		if groupMaster == nil || !slices.Equal(group.Endpoints, []string{groupMaster.PeerEndpoint}) {
			return errors.New("committed RGW topology zonegroup master/endpoint mismatch")
		}
		if groupMaster.ID == master.ID && (period.MasterZonegroup != "" || f.groupIDs[group.Name] != "") && group.ID != period.MasterZonegroup {
			return errors.New("committed RGW realm master zonegroup mismatch")
		}
		for _, state := range expected {
			found := false
			for _, zone := range group.Zones {
				if zone.Name == state.Name {
					if zone.ID != state.ID || state.ID == "" || state.PeerEndpoint == "" || !slices.Equal(zone.Endpoints, []string{state.PeerEndpoint}) {
						return fmt.Errorf("committed RGW zone %s identity/endpoint mismatch", state.Name)
					}
					found = true
				}
			}
			if !found {
				return fmt.Errorf("committed RGW topology omits zone %s", state.Name)
			}
		}
	}
	return nil
}

func (f *RGWMultisite) waitZonePeriods(ctx context.Context, master *rgwZoneState, states []*rgwZoneState) error {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	var last error
	for {
		last = nil
		for _, state := range states {
			// A newly started secondary may not yet have received the system
			// user's metadata, so automatic signed period notifications can be
			// rejected. Pull using the master's known bootstrap credentials;
			// realm pull advances the active pointer and reflects zonegroups.
			if state.ID != master.ID {
				if _, err := f.admin(ctx, state.client, state.Name, "realm", "pull", "--url", master.PeerEndpoint,
					"--access-key", f.systemAccess, "--secret", f.systemSecret); err != nil {
					last = fmt.Errorf("pull active RGW topology into %s: %w", state.Name, err)
					break
				}
			}
			data, err := f.admin(ctx, state.client, state.Name, "period", "get", "--format", "json")
			if err == nil {
				var period *rgwTopologyPeriod
				period, err = decodeRGWTopologyPeriod(data)
				if err == nil {
					err = f.checkZonePeriod(period, master, states)
				}
			}
			if err != nil {
				last = fmt.Errorf("RGW topology period in %s: %w", state.Name, err)
				break
			}
		}
		if last == nil {
			data, err := f.admin(ctx, master.client, master.Name, "period", "get", "--format", "json")
			if err != nil {
				return err
			}
			period, err := decodeRGWTopologyPeriod(data)
			if err != nil {
				return err
			}
			if f.groupMasters == nil {
				f.groupMasters = make(map[string]string)
			}
			for _, group := range period.PeriodMap.Zonegroups {
				for _, state := range states {
					if state.ID == group.MasterZone {
						f.groupMasters[group.Name] = state.Name
					}
				}
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return last
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// HTTP readiness does not imply that a secondary finished creating its local
// metadata full-sync maps. Wait for every native shard to enter incremental
// sync before restarting gateways to apply the new topology.
func (f *RGWMultisite) waitZoneMetadataBootstrap(ctx context.Context, master *rgwZoneState, states []*rgwZoneState) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	for _, zone := range states {
		if zone.ID == master.ID {
			continue
		}
		status, err := zone.Gateway.State(ctx)
		if err != nil || status == nil {
			return fmt.Errorf("inspect zone %s before metadata bootstrap: %v", zone.Name, err)
		}
		if !status.Running {
			continue
		}
		var last error
		for {
			data, err := f.admin(ctx, zone.client, zone.Name, "metadata", "sync", "status", "--format", "json")
			if err == nil {
				err = rgwMetadataBootstrapped(data)
			}
			if err == nil {
				break
			}
			last = err
			select {
			case <-ctx.Done():
				return fmt.Errorf("RGW zone %s metadata bootstrap did not complete: %w", zone.Name, last)
			case <-time.After(time.Second):
			}
		}
	}
	return nil
}

func rgwMetadataBootstrapped(data []byte) error {
	var status struct {
		SyncStatus struct {
			Info struct {
				Status    string `json:"status"`
				NumShards int    `json:"num_shards"`
			} `json:"info"`
			Markers []struct {
				Key int `json:"key"`
				Val struct {
					State int `json:"state"`
				} `json:"val"`
			} `json:"markers"`
		} `json:"sync_status"`
	}
	if err := json.Unmarshal(data, &status); err != nil {
		return errors.New("invalid RGW metadata sync status")
	}
	sync := status.SyncStatus
	if sync.Info.Status != "sync" || sync.Info.NumShards <= 0 || len(sync.Markers) != sync.Info.NumShards {
		return errors.New("RGW metadata full-sync maps are not initialized")
	}
	seen := make(map[int]bool, len(sync.Markers))
	for _, marker := range sync.Markers {
		if marker.Key < 0 || marker.Key >= sync.Info.NumShards || seen[marker.Key] {
			return errors.New("invalid RGW metadata sync shard identities")
		}
		seen[marker.Key] = true
		if marker.Val.State != 1 {
			return fmt.Errorf("RGW metadata shard %d has not finished initial full sync", marker.Key)
		}
	}
	return nil
}
