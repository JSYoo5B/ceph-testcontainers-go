package multisite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
)

// ZoneConfig assigns one fresh, independent storage cluster to a named zone.
type ZoneConfig struct {
	Name, Zonegroup string
	Cluster         *ceph.Container
}

// ZonegroupConfig defines a region and its local master zone. MasterZone
// defaults to the first zone. Object data replicates within each zonegroup;
// users and bucket metadata share the realm across zonegroups.
type ZonegroupConfig struct {
	Name, MasterZone string
	Zones            []ZoneConfig
}

// TopologyConfig creates one realm spanning at least two independent zones.
// Use Zones/Zonegroup/MetadataMaster for a single region, or Zonegroups and
// MasterZonegroup for several regions with separately selected local masters.
// MetadataMaster defaults to the first zone; MasterZonegroup to the first group.
// All clusters use the same network mode and must not serve standalone RGW data.
// SetupImage optionally overrides every zone's setup client image. Otherwise
// each client uses its own cluster's RGW image, including added zones.
type TopologyConfig struct {
	Zones                            []ZoneConfig
	Realm, Zonegroup, MetadataMaster string
	SetupImage                       string
	Zonegroups                       []ZonegroupConfig
	MasterZonegroup                  string
}

// Zone describes one owned multisite zone. PeerEndpoint is reachable by
// gateways/CLI containers in the fixture's HTTP network or Docker host namespace.
// Applications obtain their endpoint separately with Gateway.S3Endpoint.
type Zone struct {
	Name, ID, Zonegroup, PeerEndpoint string
	Gateway                           *rgw.Gateway
}

// Zonegroup is an owned region snapshot. MasterZone reflects the last
// topology operation; direct native promotions require a subsequent operation
// to refresh it. Each zone retains its own storage cluster and daemon endpoint.
type Zonegroup struct {
	Name, ID, MasterZone string
	Zones                []Zone
}

type rgwZoneRemoval struct {
	state                                              *rgwZoneState
	detached, converged, gatewayRemoved, clientRemoved bool
}

type rgwZoneState struct {
	Zone
	cluster *ceph.Container
	client  testcontainers.Container
}

// runTopology implements Run with WithTopology. A non-nil result on error owns
// partial resources and needs cleanup.
func runTopology(ctx context.Context, image string, config TopologyConfig, opts ...testcontainers.ContainerCustomizer) (*Fixture, error) {
	zones, err := prepareRGWTopology(ctx, image, config)
	if err != nil {
		return nil, err
	}
	for _, zone := range zones {
		if err := ensureFreshRGWCluster(ctx, zone.Cluster); err != nil {
			return nil, err
		}
	}
	f, err := runPair(ctx, image, Config{
		Source: zones[0].Cluster, Destination: zones[1].Cluster,
		SourceZone: zones[0].Name, DestinationZone: zones[1].Name,
		Realm: config.Realm, Zonegroup: zones[0].Zonegroup, SetupImage: config.SetupImage,
		destinationZonegroup: zones[1].Zonegroup,
	}, opts...)
	if err != nil {
		return f, err
	}
	if len(zones) > 2 {
		// The outer constructor still owns fresh initialization and has not
		// exposed f. Additional remote commits need a final master-local
		// staging snapshot of the complete graph before returning it.
		f.topology.bootstrapStagingPending = true
	}
	for _, zone := range zones[2:] {
		f.topologyMu.Lock()
		_, exists := f.topology.groupIDs[zone.Zonegroup]
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

func prepareRGWTopology(ctx context.Context, image string, config TopologyConfig) ([]ZoneConfig, error) {
	zones, err := normalizeRGWTopology(config)
	if err != nil {
		return nil, err
	}
	for i, zone := range zones {
		for j := range i {
			if err := multicluster.ValidatePair(ctx, image, zones[j].Cluster, zone.Cluster); err != nil {
				return nil, fmt.Errorf("RGW zones %s/%s: %w", zones[j].Name, zone.Name, err)
			}
		}
		if err := multicluster.RequireRoleImage(image, multicluster.RoleRGW, zone.Cluster); err != nil {
			return nil, fmt.Errorf("RGW zone %s gateway image: %w", zone.Name, err)
		}
	}
	return zones, nil
}

func normalizeRGWTopology(config TopologyConfig) ([]ZoneConfig, error) {
	var err error
	if len(config.Zonegroups) > 0 {
		if len(config.Zones) > 0 || config.Zonegroup != "" || config.MetadataMaster != "" {
			return nil, multisiteInvalid(errors.New("RGW topology must use either Zones or Zonegroups"))
		}
		config.Zones, config.MetadataMaster, err = flattenRGWZonegroups(config.Zonegroups, config.MasterZonegroup)
	} else {
		config.Zones, err = flatRGWZones(config)
	}
	if err != nil {
		return nil, err
	}
	return orderRGWZones(config.Zones, config.MetadataMaster)
}

// flattenRGWZonegroups lists the zones of every group with each group's
// master first and the master group's zones ahead of the others. It returns
// the master group's master zone as the realm metadata master.
func flattenRGWZonegroups(groups []ZonegroupConfig, masterGroup string) ([]ZoneConfig, string, error) {
	groups = slices.Clone(groups)
	if masterGroup == "" {
		masterGroup = groups[0].Name
	}
	var zones []ZoneConfig
	var masterName string
	seen := make(map[string]bool)
	for _, group := range groups {
		if !validRGWZoneName(group.Name) || seen[group.Name] || len(group.Zones) == 0 {
			return nil, "", multisiteInvalid(errors.New("RGW zonegroups require unique valid names and at least one zone"))
		}
		seen[group.Name] = true
		ordered, master, err := rgwZonegroupZones(group)
		if err != nil {
			return nil, "", err
		}
		if group.Name == masterGroup {
			masterName = master
			zones = append(ordered, zones...)
		} else {
			zones = append(zones, ordered...)
		}
	}
	if masterName == "" {
		return nil, "", multisiteInvalid(errors.New("RGW master zonegroup must name a configured group"))
	}
	return zones, masterName, nil
}

// rgwZonegroupZones returns the group's zones, assigned to the group, with
// its master zone (by default the first) moved to the front.
func rgwZonegroupZones(group ZonegroupConfig) ([]ZoneConfig, string, error) {
	ordered := slices.Clone(group.Zones)
	master := group.MasterZone
	if master == "" {
		master = ordered[0].Name
	}
	index := slices.IndexFunc(ordered, func(zone ZoneConfig) bool { return zone.Name == master })
	if index < 0 {
		return nil, "", multisiteInvalid(errors.New("RGW zonegroup master must name a zone in that group"))
	}
	selected := ordered[index]
	ordered = slices.Delete(ordered, index, index+1)
	ordered = append([]ZoneConfig{selected}, ordered...)
	for i := range ordered {
		if ordered[i].Zonegroup != "" && ordered[i].Zonegroup != group.Name {
			return nil, "", multisiteInvalid(errors.New("RGW zone has conflicting zonegroup"))
		}
		ordered[i].Zonegroup = group.Name
	}
	return ordered, master, nil
}

// flatRGWZones assigns every zone of a single-region topology to its
// zonegroup.
func flatRGWZones(config TopologyConfig) ([]ZoneConfig, error) {
	if config.MasterZonegroup != "" {
		return nil, multisiteInvalid(errors.New("MasterZonegroup requires Zonegroups"))
	}
	if config.Zonegroup == "" {
		config.Zonegroup = "us-east-1"
	}
	if !validRGWZoneName(config.Zonegroup) {
		return nil, multisiteInvalid(errors.New("invalid RGW zonegroup name"))
	}
	zones := slices.Clone(config.Zones)
	for i := range zones {
		if zones[i].Zonegroup != "" && zones[i].Zonegroup != config.Zonegroup {
			return nil, multisiteInvalid(errors.New("flat RGW topology uses one zonegroup; use Zonegroups for multiple groups"))
		}
		zones[i].Zonegroup = config.Zonegroup
	}
	return zones, nil
}

// orderRGWZones requires at least two uniquely and validly named zones and
// moves the metadata master (by default the first) to the front without
// reordering the other sites.
func orderRGWZones(zones []ZoneConfig, master string) ([]ZoneConfig, error) {
	if len(zones) < 2 {
		return nil, multisiteInvalid(errors.New("RGW multisite topology requires at least two zones"))
	}
	zones = slices.Clone(zones)
	if master == "" {
		master = zones[0].Name
	}
	masterIndex := -1
	for i, zone := range zones {
		if !validRGWZoneName(zone.Name) {
			return nil, multisiteInvalid(errors.New("invalid RGW zone name"))
		}
		if zone.Name == master {
			masterIndex = i
		}
		for j := range i {
			if zones[j].Name == zone.Name {
				return nil, multisiteInvalid(fmt.Errorf("duplicate RGW zone %s", zone.Name))
			}
		}
	}
	if masterIndex < 0 {
		return nil, multisiteInvalid(errors.New("RGW metadata master must name a configured zone"))
	}
	selected := zones[masterIndex]
	zones = append(slices.Delete(zones, masterIndex, masterIndex+1), ZoneConfig{})
	copy(zones[1:], zones[:len(zones)-1])
	zones[0] = selected
	return zones, nil
}

func validRGWZoneName(name string) bool {
	return name != "" && strings.TrimSpace(name) == name && !strings.HasPrefix(name, "-") && strings.IndexFunc(name, unicode.IsControl) == -1
}

// Zones returns owned zone descriptors sorted by name, including partial zones
// returned by a failed AddZone. It does not expose private system credentials.
//
//purpose:observation
func (f *Fixture) Zones() []Zone {
	f.topologyMu.Lock()
	defer f.topologyMu.Unlock()
	states := f.zoneStates()
	result := make([]Zone, 0, len(states))
	for _, state := range states {
		result = append(result, state.Zone)
	}
	slices.SortFunc(result, func(a, b Zone) int { return strings.Compare(a.Name, b.Name) })
	return result
}

// zoneStates is called while topologyMu is held.
func (f *Fixture) zoneStates() []*rgwZoneState {
	states := []*rgwZoneState{
		{Zone: Zone{Name: f.realm.config.SourceZone, ID: f.SourceZoneID, Zonegroup: f.realm.zoneGroupName(f.realm.config.SourceZone), PeerEndpoint: f.realm.sourceURL, Gateway: f.Source}, cluster: f.realm.config.Source, client: f.realm.sourceClient},
		{Zone: Zone{Name: f.realm.config.DestinationZone, ID: f.DestinationZoneID, Zonegroup: f.realm.zoneGroupName(f.realm.config.DestinationZone), PeerEndpoint: f.realm.destinationURL, Gateway: f.Destination}, cluster: f.realm.config.Destination, client: f.realm.destinationClient},
	}
	for _, zone := range f.topology.additional {
		states = append(states, zone)
	}
	return slices.DeleteFunc(states, func(state *rgwZoneState) bool {
		removal := f.topology.removed[state.Name]
		return removal != nil && removal.detached
	})
}

// ZoneAdmin runs native radosgw-admin in any owned zone, including a partial
// zone whose CLI client started successfully. Do not edit period/zone state
// concurrently with AddZone or external metadata master transitions.
//
//purpose:native
func (f *Fixture) ZoneAdmin(ctx context.Context, name string, args ...string) ([]byte, error) {
	if err := multicluster.LockObservation(ctx, &f.topologyMu); err != nil {
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
	return f.realm.admin(ctx, client, name, args...)
}

// AddZone joins another fresh storage cluster to the existing realm/zonegroup.
// It selects the metadata master from the current committed period, verifies
// every registered zone's identity and endpoint, and refreshes the period in
// all sites. Existing running gateways restart to load the final period,
// retaining their listener ports and storage; fenced gateways stay stopped
// unless an earlier reload of theirs was interrupted.
// The new gateway reloads only if its host-mode endpoint changed after startup.
// Running secondaries finish initial metadata full sync before reload. Complete
// any external master transition first, or keep the previous master fenced.
// Refresh S3Endpoint afterwards because bridge-mode published ports may change.
// A non-nil partial result remains owned on error; terminate the fixture if
// initialization fails rather than adopting an external zone.
// image selects the new gateway. Its setup client uses the new cluster's rgw
// image unless the fixture was configured with SetupImage.
//
//purpose:construction,condition
func (f *Fixture) AddZone(ctx context.Context, image string, config ZoneConfig, opts ...testcontainers.ContainerCustomizer) (*Zone, error) {
	if f == nil || !validRGWZoneName(config.Name) {
		return nil, errors.New("invalid RGW fixture or zone name")
	}
	if err := multicluster.LockObservation(ctx, &f.topologyMu); err != nil {
		return nil, err
	}
	defer f.topologyMu.Unlock()
	return f.addZone(ctx, image, config, false, opts...)
}

func (f *Fixture) addZone(ctx context.Context, image string, config ZoneConfig, createGroup bool, opts ...testcontainers.ContainerCustomizer) (*Zone, error) {
	addition, err := f.admitZoneAddition(ctx, image, config, createGroup)
	if err != nil {
		return nil, err
	}
	config = addition.config
	ctx, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()
	master, err := f.additionMaster(ctx, config.Name)
	if err != nil {
		return nil, err
	}
	state := f.registerAddedZone(config)
	peerNetwork := ""
	if f.httpNetwork != nil {
		peerNetwork = f.httpNetwork.Name
	}
	state.client, err = multicluster.RunClient(ctx, addition.clientImage, config.Cluster, peerNetwork, &f.owned)
	if err != nil {
		return &state.Zone, err
	}
	alias := "rgw-zone-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	if !addition.hostNetwork {
		state.PeerEndpoint = "http://" + alias + ":7480"
	}
	newGroup := ""
	if createGroup {
		newGroup = config.Zonegroup
	}
	if err := f.realm.joinZone(ctx, zoneJoin{client: state.client, zone: config.Name, masterURL: master.PeerEndpoint, endpoint: state.PeerEndpoint, newGroup: newGroup}); err != nil {
		return &state.Zone, fmt.Errorf("join RGW zone %s: %w", config.Name, err)
	}
	state.Gateway, err = f.runGateway(ctx, image, config.Cluster, f.httpNetwork, alias, config.Name, opts...)
	if err != nil {
		return &state.Zone, err
	}
	if addition.hostNetwork {
		if state.PeerEndpoint, err = state.Gateway.DaemonEndpoint(ctx); err != nil {
			return &state.Zone, err
		}
		if err := f.realm.publishZoneEndpoint(ctx, state.client, config.Name, state.PeerEndpoint, createGroup); err != nil {
			return &state.Zone, err
		}
	}
	if err := f.recordAddedZoneIdentity(ctx, state, createGroup); err != nil {
		return &state.Zone, err
	}
	states := f.zoneStates()
	if err := f.waitZonePeriods(ctx, master, states); err != nil {
		return &state.Zone, err
	}
	if err := f.realm.waitZoneMetadataBootstrap(ctx, master, states); err != nil {
		return &state.Zone, err
	}
	if err := f.topology.reloadZoneGateways(ctx, states, func(zone *rgwZoneState) bool { return zone == state && !addition.hostNetwork }); err != nil {
		return &state.Zone, err
	}
	state.Gateway.AccessKey, state.Gateway.SecretKey = f.Source.AccessKey, f.Source.SecretKey
	return &state.Zone, nil
}

// zoneAddition is an admitted AddZone or AddZonegroup request: the zone with
// its zonegroup filled in and what its fresh cluster provides.
type zoneAddition struct {
	config      ZoneConfig
	hostNetwork bool
	clientImage string
}

// admitZoneAddition refuses an addition before anything is created: invalid
// names, detached or pending removals, a zonegroup that does not match
// createGroup, an uninitialized or closed fixture, a duplicate or incompatible
// zone and a cluster that is not fresh. The caller holds topologyMu.
func (f *Fixture) admitZoneAddition(ctx context.Context, image string, config ZoneConfig, createGroup bool) (zoneAddition, error) {
	if config.Zonegroup == "" {
		config.Zonegroup = f.realm.config.Zonegroup
	}
	if !validRGWZoneName(config.Name) || !validRGWZoneName(config.Zonegroup) {
		return zoneAddition{}, errors.New("invalid RGW zone or zonegroup name")
	}
	if removal := f.topology.removed[config.Name]; removal != nil {
		return zoneAddition{}, errors.New("detached RGW zones retain configuration and cannot be adopted as fresh zones")
	}
	for _, removal := range f.topology.removed {
		if !removal.converged || !removal.gatewayRemoved || !removal.clientRemoved {
			return zoneAddition{}, errors.New("finish the pending RGW zone removal before adding zones")
		}
		if removal.state.cluster == config.Cluster {
			return zoneAddition{}, errors.New("detached RGW storage clusters retain configuration and cannot be adopted as fresh zones")
		}
	}
	if _, knownGroup := f.topology.groupIDs[config.Zonegroup]; knownGroup == createGroup {
		return zoneAddition{}, errors.New("AddZone requires an owned zonegroup; AddZonegroup requires a fresh group name")
	}
	if f.closed || f.Source == nil || f.Destination == nil || f.RealmID == "" || f.realm.systemAccess == "" || f.realm.systemSecret == "" {
		return zoneAddition{}, errors.New("RGW multisite must be initialized and running")
	}
	for _, zone := range f.zoneStates() {
		if zone.Name == config.Name {
			return zoneAddition{}, fmt.Errorf("RGW zone %s is already owned", config.Name)
		}
		if err := multicluster.ValidatePair(ctx, image, zone.cluster, config.Cluster); err != nil {
			return zoneAddition{}, err
		}
		if zone.ID == "" || zone.client == nil || zone.Gateway == nil {
			return zoneAddition{}, errors.New("finish initializing existing RGW zones before adding another")
		}
	}
	facts, err := ceph.FactsOf(config.Cluster)
	if err != nil {
		return zoneAddition{}, err
	}
	clientImage, err := rgwSetupImage(f.realm.config.SetupImage, facts.Images.RGW)
	if err != nil {
		return zoneAddition{}, err
	}
	if err := ensureFreshRGWCluster(ctx, config.Cluster); err != nil {
		return zoneAddition{}, err
	}
	return zoneAddition{config: config, hostNetwork: facts.UsesHostNetwork, clientImage: clientImage}, nil
}

// additionMaster reads the committed period, verifies every owned zone in it
// and returns the current metadata master. It refuses a zone name that the
// period already contains.
func (f *Fixture) additionMaster(ctx context.Context, name string) (*rgwZoneState, error) {
	states := f.zoneStates()
	period, err := f.realm.committedPeriod(ctx, states[0])
	if err != nil {
		return nil, err
	}
	master := rgwPeriodMaster(period, states)
	if master == nil {
		return nil, errors.New("current RGW metadata master is not an owned zone")
	}
	if err := f.checkZonePeriod(period, master, states); err != nil {
		return nil, err
	}
	for _, group := range period.PeriodMap.Zonegroups {
		for _, zone := range group.Zones {
			if zone.Name == name {
				return nil, multisiteInvalid(fmt.Errorf("RGW zone %s already exists in the committed period", name))
			}
		}
	}
	return master, nil
}

// registerAddedZone records the new zone before its first container exists,
// so a partial addition stays owned and visible through Zones.
func (f *Fixture) registerAddedZone(config ZoneConfig) *rgwZoneState {
	state := &rgwZoneState{Zone: Zone{Name: config.Name, Zonegroup: config.Zonegroup}, cluster: config.Cluster}
	if f.topology.additional == nil {
		f.topology.additional = make(map[string]*rgwZoneState)
	}
	f.topology.additional[config.Name] = state
	f.realm.recordZoneGroup(config.Name, config.Zonegroup)
	return state
}

// recordZoneGroup records the zonegroup that commands for zone run in.
func (r *rgwRealmAccess) recordZoneGroup(zone, group string) {
	r.scopeMu.Lock()
	defer r.scopeMu.Unlock()
	if r.zoneGroups == nil {
		r.zoneGroups = make(map[string]string)
	}
	r.zoneGroups[zone] = group
}

// recordAddedZoneIdentity reads the added zone's native ID and, for a new
// zonegroup, the group's ID with the zone as its master.
func (f *Fixture) recordAddedZoneIdentity(ctx context.Context, state *rgwZoneState, createGroup bool) error {
	id, err := f.realm.nativeID(ctx, state.client, state.Name, "zone")
	if err != nil {
		return err
	}
	if id == "" {
		return errors.New("invalid added RGW zone identity")
	}
	state.ID = id
	if !createGroup {
		return nil
	}
	groupID, err := f.realm.nativeID(ctx, state.client, state.Name, "zonegroup")
	if err != nil {
		return err
	}
	if groupID == "" {
		return errors.New("invalid added RGW zonegroup identity")
	}
	f.topology.groupIDs[state.Zonegroup] = groupID
	f.topology.groupMasters[state.Zonegroup] = state.Name
	return nil
}

// committedPeriod reads and decodes the committed period as zone sees it.
func (r *rgwRealmAccess) committedPeriod(ctx context.Context, zone *rgwZoneState) (*rgwTopologyPeriod, error) {
	data, err := r.admin(ctx, zone.client, zone.Name, "period", "get", "--format", "json")
	if err != nil {
		return nil, err
	}
	return decodeRGWTopologyPeriod(data)
}

func (t *rgwZoneTopology) reloadZoneGateways(ctx context.Context, states []*rgwZoneState, skip func(*rgwZoneState) bool) error {
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
			if !t.reloadNeeded[zone.Name] {
				continue
			}
		} else {
			if t.reloadNeeded == nil {
				t.reloadNeeded = make(map[string]bool)
			}
			t.reloadNeeded[zone.Name] = true
			stop := 3 * time.Second
			if err := zone.Gateway.Stop(ctx, &stop); err != nil {
				return fmt.Errorf("stop zone %s before period reload: %w", zone.Name, err)
			}
		}
		if err := zone.Gateway.Start(ctx); err != nil {
			return fmt.Errorf("restart zone %s after period reload: %w", zone.Name, err)
		}
		delete(t.reloadNeeded, zone.Name)
	}
	return nil
}

// rgwTopologyPeriod is the part of a committed period that the topology
// checks read: realm, master zone and zonegroup, and each zonegroup's zones
// and endpoints.
type rgwTopologyPeriod struct {
	RealmID, MasterZone, MasterZonegroup string
	PeriodMap                            struct{ Zonegroups []rgwTopologyZonegroup }
}

type rgwTopologyZonegroup struct {
	ID, Name, MasterZone string
	Endpoints            []string
	Zones                []rgwTopologyZone
}

type rgwTopologyZone struct {
	ID, Name  string
	Endpoints []string
}

var errInvalidTopologyPeriod = errors.New("invalid committed RGW topology period")

// decodeRGWTopologyPeriod reads a committed period by exact member name. A
// member of the wrong type, a zonegroup or zone that is not an object, or a
// period without realm, master zone or zonegroup list is invalid.
func decodeRGWTopologyPeriod(data []byte) (*rgwTopologyPeriod, error) {
	document, err := syncJSON(data)
	if err != nil {
		return nil, errInvalidTopologyPeriod
	}
	doc := nativeDoc(document)
	var period rgwTopologyPeriod
	var ok [3]bool
	period.RealmID, ok[0] = doc.text("realm_id")
	period.MasterZone, ok[1] = doc.text("master_zone")
	period.MasterZonegroup, ok[2] = doc.text("master_zonegroup")
	groups, isList := doc.object("period_map")["zonegroups"].([]any)
	if ok != [3]bool{true, true, true} || !isList || period.RealmID == "" || period.MasterZone == "" {
		return nil, errInvalidTopologyPeriod
	}
	period.PeriodMap.Zonegroups = make([]rgwTopologyZonegroup, 0, len(groups))
	for _, value := range groups {
		object, isObject := value.(map[string]any)
		if !isObject {
			return nil, errInvalidTopologyPeriod
		}
		group, err := decodeRGWTopologyZonegroup(object)
		if err != nil {
			return nil, err
		}
		period.PeriodMap.Zonegroups = append(period.PeriodMap.Zonegroups, group)
	}
	return &period, nil
}

func decodeRGWTopologyZonegroup(doc nativeDoc) (rgwTopologyZonegroup, error) {
	var group rgwTopologyZonegroup
	var ok [4]bool
	group.ID, ok[0] = doc.text("id")
	group.Name, ok[1] = doc.text("name")
	group.MasterZone, ok[2] = doc.text("master_zone")
	group.Endpoints, ok[3] = doc.texts("endpoints")
	zones, isList := doc["zones"].([]any)
	if ok != [4]bool{true, true, true, true} || !isList && doc["zones"] != nil {
		return rgwTopologyZonegroup{}, errInvalidTopologyPeriod
	}
	for _, value := range zones {
		object, isObject := value.(map[string]any)
		if !isObject {
			return rgwTopologyZonegroup{}, errInvalidTopologyPeriod
		}
		zone := nativeDoc(object)
		var entry rgwTopologyZone
		var zoneOK [3]bool
		entry.ID, zoneOK[0] = zone.text("id")
		entry.Name, zoneOK[1] = zone.text("name")
		entry.Endpoints, zoneOK[2] = zone.texts("endpoints")
		if zoneOK != [3]bool{true, true, true} {
			return rgwTopologyZonegroup{}, errInvalidTopologyPeriod
		}
		group.Zones = append(group.Zones, entry)
	}
	return group, nil
}

// checkZonePeriod requires a committed period to describe exactly the owned
// topology: this realm and master, one zonegroup per owned group, and every
// owned zone with its identity and peer endpoint.
func (f *Fixture) checkZonePeriod(period *rgwTopologyPeriod, master *rgwZoneState, states []*rgwZoneState) error {
	if period.RealmID != f.RealmID || period.MasterZone != master.ID {
		return errors.New("committed RGW topology realm/master identity mismatch")
	}
	groups := f.zoneStatesByGroup(states)
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
		if err := f.checkPeriodZonegroup(period, group, expected, master); err != nil {
			return err
		}
		if err := checkPeriodZones(group, expected); err != nil {
			return err
		}
	}
	return nil
}

// zoneStatesByGroup groups the owned zones by zonegroup name; a zone without
// one belongs to the fixture's configured zonegroup.
func (f *Fixture) zoneStatesByGroup(states []*rgwZoneState) map[string][]*rgwZoneState {
	groups := make(map[string][]*rgwZoneState)
	for _, state := range states {
		name := state.Zonegroup
		if name == "" {
			name = f.realm.config.Zonegroup
		}
		groups[name] = append(groups[name], state)
	}
	return groups
}

// checkPeriodZonegroup checks one committed zonegroup's identity, its master
// zone and endpoint, and, for the realm master's group, the realm's master
// zonegroup.
func (f *Fixture) checkPeriodZonegroup(period *rgwTopologyPeriod, group rgwTopologyZonegroup, expected []*rgwZoneState, master *rgwZoneState) error {
	if id := f.topology.groupIDs[group.Name]; id != "" && id != group.ID {
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
	if groupMaster.ID == master.ID && (period.MasterZonegroup != "" || f.topology.groupIDs[group.Name] != "") && group.ID != period.MasterZonegroup {
		return errors.New("committed RGW realm master zonegroup mismatch")
	}
	return nil
}

// checkPeriodZones requires every owned zone of a zonegroup to be committed
// with its own ID and exactly its peer endpoint.
func checkPeriodZones(group rgwTopologyZonegroup, expected []*rgwZoneState) error {
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
	return nil
}

func (f *Fixture) waitZonePeriods(ctx context.Context, master *rgwZoneState, states []*rgwZoneState) error {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	for {
		last := f.zonePeriodsLag(ctx, master, states)
		if last == nil {
			return f.recordGroupMasters(ctx, master, states)
		}
		select {
		case <-ctx.Done():
			return last
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// zonePeriodsLag pulls the active topology into every secondary and checks
// each zone's period, returning the first zone that does not have it yet.
func (f *Fixture) zonePeriodsLag(ctx context.Context, master *rgwZoneState, states []*rgwZoneState) error {
	for _, state := range states {
		// A newly started secondary may not yet have received the system
		// user's metadata, so automatic signed period notifications can be
		// rejected. Pull using the master's known bootstrap credentials;
		// realm pull advances the active pointer and reflects zonegroups.
		if state.ID != master.ID {
			if _, err := f.realm.admin(ctx, state.client, state.Name, "realm", "pull", "--url", master.PeerEndpoint,
				"--access-key", f.realm.systemAccess, "--secret", f.realm.systemSecret); err != nil {
				return fmt.Errorf("pull active RGW topology into %s: %w", state.Name, err)
			}
		}
		data, err := f.realm.admin(ctx, state.client, state.Name, "period", "get", "--format", "json")
		if err == nil {
			var period *rgwTopologyPeriod
			period, err = decodeRGWTopologyPeriod(data)
			if err == nil {
				err = f.checkZonePeriod(period, master, states)
			}
		}
		if err != nil {
			return fmt.Errorf("RGW topology period in %s: %w", state.Name, err)
		}
	}
	return nil
}

// recordGroupMasters records each zonegroup's master zone from the master's
// committed period.
func (f *Fixture) recordGroupMasters(ctx context.Context, master *rgwZoneState, states []*rgwZoneState) error {
	data, err := f.realm.admin(ctx, master.client, master.Name, "period", "get", "--format", "json")
	if err != nil {
		return err
	}
	period, err := decodeRGWTopologyPeriod(data)
	if err != nil {
		return err
	}
	if f.topology.groupMasters == nil {
		f.topology.groupMasters = make(map[string]string)
	}
	for _, group := range period.PeriodMap.Zonegroups {
		for _, state := range states {
			if state.ID == group.MasterZone {
				f.topology.groupMasters[group.Name] = state.Name
			}
		}
	}
	return nil
}

// HTTP readiness does not imply that a secondary finished creating its local
// metadata full-sync maps. Wait for every native shard to enter incremental
// sync before restarting gateways to apply the new topology.
func (r *rgwRealmAccess) waitZoneMetadataBootstrap(ctx context.Context, master *rgwZoneState, states []*rgwZoneState) error {
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
			data, err := r.admin(ctx, zone.client, zone.Name, "metadata", "sync", "status", "--format", "json")
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
