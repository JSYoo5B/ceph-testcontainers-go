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
	Name    string
	Cluster *ceph.Container
}

// RGWTopologyConfig creates one realm and zonegroup spanning two or more zones.
// MetadataMaster names the initial master zone, defaulting to the first zone.
// All clusters use the same network mode and must not serve standalone RGW data.
type RGWTopologyConfig struct {
	Zones                            []RGWZoneConfig
	Realm, Zonegroup, MetadataMaster string
	ControlImage                     string
}

// RGWZone describes one owned multisite zone. PeerEndpoint is reachable by
// gateways/CLI containers in the fixture's HTTP network or Docker host namespace.
// Applications obtain their endpoint separately with Gateway.S3Endpoint.
type RGWZone struct {
	Name, ID, PeerEndpoint string
	Gateway                *ceph.RGWContainer
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
func RunRGWTopology(ctx context.Context, image string, config RGWTopologyConfig, opts ...testcontainers.ContainerCustomizer) (*RGWMultisite, error) {
	zones, err := prepareRGWTopology(image, config)
	if err != nil {
		return nil, err
	}
	f, err := RunRGWMultisite(ctx, image, RGWMultisiteConfig{
		Source: zones[0].Cluster, Destination: zones[1].Cluster,
		SourceZone: zones[0].Name, DestinationZone: zones[1].Name,
		Realm: config.Realm, Zonegroup: config.Zonegroup, ControlImage: config.ControlImage,
	}, opts...)
	if err != nil {
		return f, err
	}
	for _, zone := range zones[2:] {
		if _, err := f.AddZone(ctx, image, zone, opts...); err != nil {
			return f, err
		}
	}
	return f, nil
}

func prepareRGWTopology(image string, config RGWTopologyConfig) ([]RGWZoneConfig, error) {
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
			if err := validatePair(image, zones[j].Cluster, zone.Cluster); err != nil {
				return nil, fmt.Errorf("RGW zones %s/%s: %w", zones[j].Name, zone.Name, err)
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
		{RGWZone: RGWZone{Name: f.config.SourceZone, ID: f.SourceZoneID, PeerEndpoint: f.sourceURL, Gateway: f.Source}, cluster: f.config.Source, client: f.sourceClient},
		{RGWZone: RGWZone{Name: f.config.DestinationZone, ID: f.DestinationZoneID, PeerEndpoint: f.destinationURL, Gateway: f.Destination}, cluster: f.config.Destination, client: f.destinationClient},
	}
	for _, zone := range f.additionalZones {
		states = append(states, zone)
	}
	return states
}

// ZoneAdmin runs native radosgw-admin in any owned zone, including a partial
// zone whose CLI client started successfully. Do not edit period/zone state
// concurrently with AddZone or external metadata master transitions.
func (f *RGWMultisite) ZoneAdmin(ctx context.Context, name string, args ...string) ([]byte, error) {
	f.topologyMu.Lock()
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
func (f *RGWMultisite) AddZone(ctx context.Context, image string, config RGWZoneConfig, opts ...testcontainers.ContainerCustomizer) (*RGWZone, error) {
	if f == nil || !validRGWZoneName(config.Name) {
		return nil, errors.New("invalid RGW fixture or zone name")
	}
	f.topologyMu.Lock()
	defer f.topologyMu.Unlock()
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
	ctx, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()
	data, err := f.SourceAdmin(ctx, "period", "get", "--format", "json")
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
	state := &rgwZoneState{RGWZone: RGWZone{Name: config.Name}, cluster: config.Cluster}
	if f.additionalZones == nil {
		f.additionalZones = make(map[string]*rgwZoneState)
	}
	f.additionalZones[config.Name] = state
	peerNetwork := ""
	if f.httpNetwork != nil {
		peerNetwork = f.httpNetwork.Name
	}
	state.client, err = runClient(ctx, f.config.ControlImage, config.Cluster, peerNetwork, &f.owned)
	if err != nil {
		return &state.RGWZone, err
	}
	alias := "rgw-zone-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	if !config.Cluster.UsesHostNetwork() {
		state.PeerEndpoint = "http://" + alias + ":7480"
	}
	for _, command := range [][]string{
		{"realm", "pull", "--url", master.PeerEndpoint, "--access-key", f.systemAccess, "--secret", f.systemSecret},
		{"realm", "default"},
		rgwEndpointArgs([]string{"zone", "create", "--access-key", f.systemAccess, "--secret", f.systemSecret}, state.PeerEndpoint),
		{"period", "update", "--commit"},
	} {
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
		for _, command := range [][]string{{"zone", "modify", "--endpoints", state.PeerEndpoint}, {"period", "update", "--commit"}} {
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
	states := f.zoneStates()
	if err := f.waitZonePeriods(ctx, master, states); err != nil {
		return &state.RGWZone, err
	}
	if err := f.waitZoneMetadataBootstrap(ctx, master, states); err != nil {
		return &state.RGWZone, err
	}
	for _, zone := range states {
		// A bridge gateway starts after its endpoint was committed, so the
		// new zone already loaded the final period. Avoid interrupting its
		// initial metadata full sync just to reload the same configuration.
		if zone == state && !config.Cluster.UsesHostNetwork() {
			continue
		}
		status, err := zone.Gateway.State(ctx)
		if err != nil || status == nil {
			return &state.RGWZone, fmt.Errorf("inspect zone %s before period reload: %v", zone.Name, err)
		}
		if !status.Running {
			continue
		}
		stop := 3 * time.Second
		if err := zone.Gateway.Stop(ctx, &stop); err != nil {
			return &state.RGWZone, fmt.Errorf("stop zone %s before period reload: %w", zone.Name, err)
		}
		if err := zone.Gateway.Start(ctx); err != nil {
			return &state.RGWZone, fmt.Errorf("restart zone %s after period reload: %w", zone.Name, err)
		}
	}
	state.Gateway.AccessKey, state.Gateway.SecretKey = f.Source.AccessKey, f.Source.SecretKey
	return &state.RGWZone, nil
}

type rgwTopologyPeriod struct {
	RealmID    string `json:"realm_id"`
	MasterZone string `json:"master_zone"`
	PeriodMap  struct {
		Zonegroups []struct {
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
	for _, group := range period.PeriodMap.Zonegroups {
		if group.Name != f.config.Zonegroup {
			continue
		}
		if group.MasterZone != master.ID || !slices.Equal(group.Endpoints, []string{master.PeerEndpoint}) {
			return errors.New("committed RGW topology zonegroup master/endpoint mismatch")
		}
		for _, state := range states {
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
		return nil
	}
	return errors.New("committed RGW topology omits requested zonegroup")
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
