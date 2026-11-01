package multisite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/dockerbridge"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
)

// Config describes a two-zone realm in fresh test clusters. The
// source is the metadata master; both zones accept object writes.
// Setup clients use each cluster's RGW image unless SetupImage overrides it.
//
// Realm and the zone names default to generated names and Zonegroup to
// us-east-1; set the zone names when the test refers to zones by name (sync
// policies, ZoneAdmin, SyncStatus).
type Config struct {
	Source, Destination                           *ceph.Container
	Realm, Zonegroup, SourceZone, DestinationZone string
	// SetupImage explicitly selects the RGW administration runtime.
	SetupImage           string
	destinationZonegroup string
}

// Fixture owns one gateway and CLI client per zone: two from Run, and one more
// for each WithTopology zone, AddZone or AddZonegroup. Bridge mode adds a
// private HTTP network; host mode uses allocated gateway ports on the Docker
// host. Source and Destination expose matching ordinary S3 test credentials.
type Fixture struct {
	Source, Destination                      *rgw.Gateway
	RealmID, SourceZoneID, DestinationZoneID string
	// realm is how the fixture reaches the realm: the configuration, the
	// pair's CLI clients and peer URLs, the system credentials and each added
	// zone's zonegroup.
	realm rgwRealmAccess
	// topology is the zones and zonegroups the fixture added or removed after
	// the pair, their native identities and the gateways awaiting a reload.
	topology    rgwZoneTopology
	owned       multicluster.Resources
	topologyMu  sync.Mutex
	httpNetwork *testcontainers.DockerNetwork
	closed      bool
}

// rgwRealmAccess runs radosgw-admin in the realm. The configuration, the
// pair's clients and URLs and the system credentials are fixed once setup has
// recorded them. zoneGroups, the zonegroup of each added zone, is guarded by
// scopeMu, because commands resolve it without the topology lock.
type rgwRealmAccess struct {
	config                          Config
	sourceClient, destinationClient testcontainers.Container
	sourceURL, destinationURL       string
	systemAccess, systemSecret      string
	scopeMu                         sync.RWMutex
	zoneGroups                      map[string]string
}

// rgwZoneTopology is the realm topology beyond the original pair: the added
// zones, the removals in progress, each zonegroup's native ID and master
// zone, the gateways whose period reload was interrupted, and whether the
// pair's bootstrap staging period still needs canonicalizing. It is guarded
// by Fixture.topologyMu, or owned by setup before the fixture is shared.
type rgwZoneTopology struct {
	additional              map[string]*rgwZoneState
	removed                 map[string]*rgwZoneRemoval
	groupIDs                map[string]string
	groupMasters            map[string]string
	reloadNeeded            map[string]bool
	bootstrapStagingPending bool
}

// Option carries the realm configuration of a fixture to Run: a two-zone
// Config (WithConfig) or a larger TopologyConfig (WithTopology). It is not a
// container customization: its Customize does nothing, so it never reaches a
// gateway.
type Option struct {
	config   *Config
	topology *TopologyConfig
}

// Customize implements testcontainers.ContainerCustomizer.
func (Option) Customize(*testcontainers.GenericContainerRequest) error { return nil }

// WithConfig carries a two-zone realm on two existing clusters to Run.
//
//purpose:construction
func WithConfig(config Config) Option { return Option{config: &config} }

// WithTopology carries a realm of at least two zones, in one or several
// zonegroups, to Run. The zone lists are copied.
//
//purpose:construction
func WithTopology(config TopologyConfig) Option {
	config.Zones = slices.Clone(config.Zones)
	config.Zonegroups = slices.Clone(config.Zonegroups)
	for i := range config.Zonegroups {
		config.Zonegroups[i].Zones = slices.Clone(config.Zonegroups[i].Zones)
	}
	return Option{topology: &config}
}

// Run configures native RGW multisite on existing clusters. Exactly one
// WithConfig or WithTopology among opts selects the realm; the other
// customizers apply to every gateway, last. Do not replace required
// networking or commands. A non-nil result returned with an error must still
// be terminated. The clusters must not already serve standalone RGW traffic.
// All clusters must use the same network mode. Host mode advertises the
// configured daemon-host addresses, which must be mutually reachable.
// img is the gateway image and is required; setup clients use each cluster's
// RGW image unless SetupImage explicitly overrides it.
//
// With WithTopology the initial master becomes Source and the first remaining
// zone becomes Destination; Zones returns all sites.
//
// Use fresh clusters from ceph.Run, without gateways or RGW configuration.
// WithConfig creates one ordinary S3 test user on the master; both gateways
// expose its keys in AccessKey and SecretKey, and their Region is the zonegroup
// name. The two-zone setup, and each further WithTopology zone, is bounded by
// six minutes in addition to ctx. On Ceph 19, 19.2.6
// cannot set up multisite: the master gateway rejects realm pull from a
// secondary with HTTP 403 ("'x-amz-content-sha256' supplied, but not in
// CanonicalHeaders"), and no setting disables the check; use 19.2.5.
//
//purpose:construction
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Fixture, error) {
	carrier, opts, err := multicluster.SplitConfig[Option]("RGW multisite", "WithConfig or WithTopology", opts)
	if err != nil {
		return nil, err
	}
	switch {
	case carrier.topology != nil:
		return runTopology(ctx, img, *carrier.topology, opts...)
	case carrier.config != nil:
		return runPair(ctx, img, *carrier.config, opts...)
	}
	return nil, servicehook.Invalid(errors.New("RGW multisite Option must come from WithConfig or WithTopology"))
}

// runPair configures a two-zone realm; runTopology builds larger ones on it.
func runPair(ctx context.Context, image string, config Config, opts ...testcontainers.ContainerCustomizer) (*Fixture, error) {
	setup, err := preparePair(ctx, image, config)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()
	f := newPairFixture(setup.config)
	bridge, err := f.startPairClients(ctx, setup)
	if err != nil {
		return f, err
	}
	if err := f.bootstrapSourceZone(ctx, image, setup, bridge, opts...); err != nil {
		return f, err
	}
	if err := f.joinDestinationZone(ctx, image, setup, bridge, opts...); err != nil {
		return f, err
	}
	if err := f.recordPairIdentities(ctx); err != nil {
		return f, err
	}
	// With host networking or separate zonegroups, the source gateway started
	// against a period that the destination's join and endpoint commits have
	// since replaced.
	reload := setup.sourceHostNetwork || setup.config.destinationZonegroup != setup.config.Zonegroup
	if err := f.finishPairBootstrap(ctx, reload); err != nil {
		return f, err
	}
	return f, f.issueS3Credentials(ctx, setup.suffix)
}

// pairSetup is a validated two-zone request: the completed configuration, the
// unique suffix of its generated names and what each cluster provides.
type pairSetup struct {
	config                                    Config
	suffix                                    string
	sourceHostNetwork, destinationHostNetwork bool
	sourceClientImage, destinationClientImage string
}

// preparePair validates both clusters and the requested names before anything
// is created.
func preparePair(ctx context.Context, image string, config Config) (pairSetup, error) {
	if err := multicluster.ValidatePair(ctx, image, config.Source, config.Destination); err != nil {
		return pairSetup{}, err
	}
	if err := multicluster.RequireRoleImage(image, multicluster.RoleRGW, config.Source, config.Destination); err != nil {
		return pairSetup{}, fmt.Errorf("configure RGW multisite gateway image: %w", err)
	}
	sourceFacts, err := ceph.FactsOf(config.Source)
	if err != nil {
		return pairSetup{}, err
	}
	destinationFacts, err := ceph.FactsOf(config.Destination)
	if err != nil {
		return pairSetup{}, err
	}
	setup := pairSetup{sourceHostNetwork: sourceFacts.UsesHostNetwork, destinationHostNetwork: destinationFacts.UsesHostNetwork}
	if setup.sourceClientImage, err = rgwSetupImage(config.SetupImage, sourceFacts.Images.RGW); err != nil {
		return pairSetup{}, err
	}
	if setup.destinationClientImage, err = rgwSetupImage(config.SetupImage, destinationFacts.Images.RGW); err != nil {
		return pairSetup{}, err
	}
	for _, cluster := range []*ceph.Container{config.Source, config.Destination} {
		if err := ensureFreshRGWCluster(ctx, cluster); err != nil {
			return pairSetup{}, err
		}
	}
	setup.suffix = strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	if setup.config, err = completePairConfig(config, setup.suffix); err != nil {
		return pairSetup{}, err
	}
	return setup, nil
}

// completePairConfig fills the default realm, zonegroup and zone names from
// suffix and refuses names radosgw-admin would misread.
func completePairConfig(config Config, suffix string) (Config, error) {
	if config.Realm == "" {
		config.Realm = "tc-multicluster-" + suffix
	}
	if config.Zonegroup == "" {
		config.Zonegroup = "us-east-1"
	}
	if config.SourceZone == "" {
		config.SourceZone = "tc-primary-" + suffix
	}
	if config.DestinationZone == "" {
		config.DestinationZone = "tc-secondary-" + suffix
	}
	for _, name := range []string{config.Realm, config.Zonegroup, config.SourceZone, config.DestinationZone} {
		if strings.TrimSpace(name) != name || name == "" || strings.HasPrefix(name, "-") || strings.ContainsAny(name, "\x00\r\n") {
			return Config{}, fmt.Errorf("invalid RGW realm/zone name")
		}
	}
	if config.SourceZone == config.DestinationZone {
		return Config{}, fmt.Errorf("RGW zones must be distinct")
	}
	if config.destinationZonegroup == "" {
		config.destinationZonegroup = config.Zonegroup
	}
	return config, nil
}

func newPairFixture(config Config) *Fixture {
	f := &Fixture{realm: rgwRealmAccess{config: config, zoneGroups: map[string]string{config.SourceZone: config.Zonegroup, config.DestinationZone: config.destinationZonegroup}}, topology: rgwZoneTopology{bootstrapStagingPending: true, groupIDs: make(map[string]string), groupMasters: map[string]string{config.Zonegroup: config.SourceZone}}}
	if config.destinationZonegroup != config.Zonegroup {
		f.topology.groupMasters[config.destinationZonegroup] = config.DestinationZone
	}
	return f
}

// startPairClients creates the bridge network of a bridge-mode pair and both
// CLI clients. Everything it creates is owned by f before an error returns.
func (f *Fixture) startPairClients(ctx context.Context, setup pairSetup) (*testcontainers.DockerNetwork, error) {
	var bridge *testcontainers.DockerNetwork
	peerNetwork := ""
	if !setup.sourceHostNetwork {
		var err error
		bridge, err = dockerbridge.New(ctx)
		if bridge != nil {
			f.httpNetwork = bridge
			f.owned.AddCleanup("remove RGW HTTP bridge", func(cleanupCtx context.Context) error {
				return multicluster.IgnoreMissing(bridge.Remove(cleanupCtx))
			})
		}
		if err != nil {
			return nil, fmt.Errorf("create RGW HTTP bridge: %w", err)
		}
		peerNetwork = bridge.Name
	}
	var err error
	if f.realm.sourceClient, err = multicluster.RunClient(ctx, setup.sourceClientImage, setup.config.Source, peerNetwork, &f.owned); err != nil {
		return nil, err
	}
	if f.realm.destinationClient, err = multicluster.RunClient(ctx, setup.destinationClientImage, setup.config.Destination, peerNetwork, &f.owned); err != nil {
		return nil, err
	}
	return bridge, nil
}

func pairAliases(suffix string) (source, destination string) {
	return "rgw-primary-" + suffix, "rgw-secondary-" + suffix
}

// bootstrapSourceZone creates the realm, the master zonegroup and zone and the
// system user on the source, starts its gateway and, in host mode, commits the
// gateway's actual listener as the master endpoint.
func (f *Fixture) bootstrapSourceZone(ctx context.Context, image string, setup pairSetup, bridge *testcontainers.DockerNetwork, opts ...testcontainers.ContainerCustomizer) error {
	sourceAlias, destinationAlias := pairAliases(setup.suffix)
	f.realm.sourceURL, f.realm.destinationURL = "http://"+sourceAlias+":7480", "http://"+destinationAlias+":7480"
	if setup.sourceHostNetwork {
		// A named host gateway chooses its port at startup. Commit the fresh
		// zone without endpoints first, then publish its actual owned listener.
		f.realm.sourceURL, f.realm.destinationURL = "", ""
	}
	f.realm.systemAccess, f.realm.systemSecret = strings.ReplaceAll(uuid.NewString(), "-", ""), uuid.NewString()+uuid.NewString()
	if err := f.realm.runZoneCommands(ctx, f.realm.sourceClient, f.realm.config.SourceZone, [][]string{
		{"realm", "create", "--default"},
		rgwEndpointArgs([]string{"zonegroup", "create", "--master", "--default"}, f.realm.sourceURL),
		rgwEndpointArgs([]string{"zone", "create", "--master", "--default"}, f.realm.sourceURL),
		{"user", "create", "--uid", "tc-sync-" + setup.suffix, "--display-name", "Testcontainers multisite sync", "--system", "--access-key", f.realm.systemAccess, "--secret-key", f.realm.systemSecret},
		{"zone", "modify", "--access-key", f.realm.systemAccess, "--secret", f.realm.systemSecret},
		{"period", "update", "--commit"},
	}); err != nil {
		return err
	}
	var err error
	if f.Source, err = f.runGateway(ctx, image, setup.config.Source, bridge, sourceAlias, f.realm.config.SourceZone, opts...); err != nil {
		return err
	}
	if !setup.sourceHostNetwork {
		return nil
	}
	f.realm.sourceURL, err = f.publishHostEndpoint(ctx, f.Source, f.realm.sourceClient, f.realm.config.SourceZone, true)
	return err
}

// joinDestinationZone joins the destination to the realm through the source,
// starts its gateway and, in host mode, commits its actual listener.
func (f *Fixture) joinDestinationZone(ctx context.Context, image string, setup pairSetup, bridge *testcontainers.DockerNetwork, opts ...testcontainers.ContainerCustomizer) error {
	newGroup := ""
	if f.realm.config.destinationZonegroup != f.realm.config.Zonegroup {
		newGroup = f.realm.config.destinationZonegroup
	}
	if err := f.realm.joinZone(ctx, zoneJoin{client: f.realm.destinationClient, zone: f.realm.config.DestinationZone, masterURL: f.realm.sourceURL, endpoint: f.realm.destinationURL, newGroup: newGroup}); err != nil {
		return err
	}
	_, destinationAlias := pairAliases(setup.suffix)
	var err error
	if f.Destination, err = f.runGateway(ctx, image, setup.config.Destination, bridge, destinationAlias, f.realm.config.DestinationZone, opts...); err != nil {
		return err
	}
	if !setup.destinationHostNetwork {
		return nil
	}
	f.realm.destinationURL, err = f.publishHostEndpoint(ctx, f.Destination, f.realm.destinationClient, f.realm.config.DestinationZone, newGroup != "")
	return err
}

// publishHostEndpoint commits a host-mode gateway's actual listener as its
// zone's endpoint while the gateway is stopped, then starts it again on the
// same port, and returns the endpoint. A host gateway chooses its port when it
// first starts, so that start serves a period without the zone's endpoint.
// Committing the endpoint while it still runs would make it reload the period
// in process, during its initial metadata full sync and before it holds the
// system user to accept the master's period push. Started after the commit,
// it loads the final period once at startup, as a bridge gateway does. The
// restart rebinds the released port, which another process could take in
// between; the start then fails and is not retried.
func (f *Fixture) publishHostEndpoint(ctx context.Context, gateway *rgw.Gateway, client testcontainers.Container, zone string, groupMaster bool) (string, error) {
	endpoint, err := gateway.DaemonEndpoint(ctx)
	if err != nil {
		return "", err
	}
	grace := 3 * time.Second
	if err := gateway.Stop(ctx, &grace); err != nil {
		return endpoint, fmt.Errorf("stop RGW zone %s before publishing its endpoint: %w", zone, err)
	}
	if err := f.realm.publishZoneEndpoint(ctx, client, zone, endpoint, groupMaster); err != nil {
		return endpoint, err
	}
	if err := gateway.Start(ctx); err != nil {
		return endpoint, fmt.Errorf("start RGW zone %s after publishing its endpoint: %w", zone, err)
	}
	return endpoint, nil
}

// recordPairIdentities reads the native realm, zone and zonegroup IDs and
// requires the destination to have joined the source's realm as a distinct
// zone.
func (f *Fixture) recordPairIdentities(ctx context.Context) error {
	for _, item := range []struct {
		client       testcontainers.Container
		zone, entity string
		target       *string
	}{
		{f.realm.sourceClient, f.realm.config.SourceZone, "realm", &f.RealmID},
		{f.realm.sourceClient, f.realm.config.SourceZone, "zone", &f.SourceZoneID},
		{f.realm.destinationClient, f.realm.config.DestinationZone, "zone", &f.DestinationZoneID},
	} {
		id, err := f.realm.nativeID(ctx, item.client, item.zone, item.entity)
		if err != nil {
			return err
		}
		if id == "" {
			return fmt.Errorf("invalid RGW %s identity", item.entity)
		}
		*item.target = id
	}
	destinationRealm, err := f.realm.nativeID(ctx, f.realm.destinationClient, f.realm.config.DestinationZone, "realm")
	if err != nil {
		return err
	}
	if destinationRealm != f.RealmID || f.SourceZoneID == f.DestinationZoneID {
		return fmt.Errorf("RGW multisite realm/zone identities do not match the requested topology")
	}
	for _, zone := range f.zoneStates() {
		id, err := f.realm.nativeID(ctx, zone.client, zone.Name, "zonegroup")
		if err != nil {
			return err
		}
		if id == "" {
			return fmt.Errorf("invalid RGW zonegroup identity")
		}
		f.topology.groupIDs[zone.Zonegroup] = id
	}
	return nil
}

// finishPairBootstrap waits until both zones commit the same period and
// leaves a canonical staging snapshot. With reload, it loads the verified
// final period into both gateways before S3 operations are admitted instead
// of racing asynchronous reload.
func (f *Fixture) finishPairBootstrap(ctx context.Context, reload bool) error {
	if err := f.waitZonePeriods(ctx, f.zoneStates()[0], f.zoneStates()); err != nil {
		return err
	}
	// A secondary commits its own staging object remotely. The master can
	// retain its earlier source-only staging object after the final current
	// period includes both zones. Finish the fresh bootstrap with a canonical
	// unpublished staging snapshot so later guarded policy updates can proceed.
	if err := f.canonicalizeBootstrapStaging(ctx); err != nil {
		return err
	}
	if !reload {
		return nil
	}
	if err := f.realm.waitZoneMetadataBootstrap(ctx, f.zoneStates()[0], f.zoneStates()); err != nil {
		return err
	}
	return f.topology.reloadZoneGateways(ctx, f.zoneStates(), nil)
}

// issueS3Credentials creates the ordinary S3 test user on the master and
// gives its single key to both gateways.
func (f *Fixture) issueS3Credentials(ctx context.Context, suffix string) error {
	user, err := f.realm.admin(ctx, f.realm.sourceClient, f.realm.config.SourceZone, "user", "create", "--uid", "tc-user-"+suffix, "--display-name", "Testcontainers multicluster S3 user")
	if err != nil {
		return err
	}
	var credentials struct {
		Keys []struct {
			AccessKey string `json:"access_key"`
			SecretKey string `json:"secret_key"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(user, &credentials); err != nil || len(credentials.Keys) != 1 || credentials.Keys[0].AccessKey == "" || credentials.Keys[0].SecretKey == "" {
		return fmt.Errorf("invalid RGW S3 test credentials")
	}
	for _, gateway := range []*rgw.Gateway{f.Source, f.Destination} {
		gateway.AccessKey = credentials.Keys[0].AccessKey
		gateway.SecretKey = credentials.Keys[0].SecretKey
	}
	return nil
}

// zoneJoin is a fresh zone joining the realm through its CLI client: it pulls
// the realm from the metadata master, creates the zone (with newGroup, first
// that zonegroup, with the zone as its master) and commits the period.
// endpoint is the committed peer endpoint; it is empty in host mode until the
// zone's gateway listens.
type zoneJoin struct {
	client                              testcontainers.Container
	zone, masterURL, endpoint, newGroup string
}

func (r *rgwRealmAccess) joinZone(ctx context.Context, join zoneJoin) error {
	commands := [][]string{
		{"realm", "pull", "--url", join.masterURL, "--access-key", r.systemAccess, "--secret", r.systemSecret},
		{"realm", "default"},
	}
	create := []string{"zone", "create", "--access-key", r.systemAccess, "--secret", r.systemSecret}
	if join.newGroup != "" {
		commands = append(commands, rgwEndpointArgs([]string{"zonegroup", "create", "--api-name", join.newGroup}, join.endpoint))
		create = append(create, "--master")
	}
	commands = append(commands, rgwEndpointArgs(create, join.endpoint), []string{"period", "update", "--commit"})
	return r.runZoneCommands(ctx, join.client, join.zone, commands)
}

// publishZoneEndpoint commits a host-mode gateway's actual listener as the
// zone's endpoint and, for a zonegroup master, as the group's endpoint.
func (r *rgwRealmAccess) publishZoneEndpoint(ctx context.Context, client testcontainers.Container, zone, endpoint string, groupMaster bool) error {
	commands := [][]string{{"zone", "modify", "--endpoints", endpoint}}
	if groupMaster {
		commands = append(commands, []string{"zonegroup", "modify", "--endpoints", endpoint})
	}
	return r.runZoneCommands(ctx, client, zone, append(commands, []string{"period", "update", "--commit"}))
}

func (r *rgwRealmAccess) runZoneCommands(ctx context.Context, client testcontainers.Container, zone string, commands [][]string) error {
	for _, command := range commands {
		if _, err := r.admin(ctx, client, zone, command...); err != nil {
			return err
		}
	}
	return nil
}

// nativeID reads the id of the zone's native realm, zonegroup or zone
// document. A document without a readable id returns an empty ID and no error.
func (r *rgwRealmAccess) nativeID(ctx context.Context, client testcontainers.Container, zone, entity string) (string, error) {
	data, err := r.admin(ctx, client, zone, entity, "get")
	if err != nil {
		return "", err
	}
	var native struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(data, &native); err != nil {
		return "", nil
	}
	return native.ID, nil
}

// An empty override lets each zone select its cluster's RGW administration
// runtime, including zones added after initial setup.
func rgwSetupImage(override, clusterRGWImage string) (string, error) {
	image := override
	if image == "" {
		image = clusterRGWImage
	}
	if strings.TrimSpace(image) == "" {
		return "", fmt.Errorf("RGW setup image must not be empty")
	}
	return image, nil
}

func (f *Fixture) runGateway(ctx context.Context, image string, cluster *ceph.Container, bridge *testcontainers.DockerNetwork, alias, zone string, opts ...testcontainers.ContainerCustomizer) (*rgw.Gateway, error) {
	moduleOpts := []testcontainers.ContainerCustomizer{testcontainers.WithImage(image)}
	if bridge != nil {
		moduleOpts = append(moduleOpts, network.WithNetwork([]string{alias}, bridge))
	}
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{
		Name: alias, Realm: f.realm.config.Realm, Zonegroup: f.realm.zoneGroupName(zone), Zone: zone,
		Region: f.realm.zoneGroupName(zone), SkipUserCreation: true,
	}, append(moduleOpts, opts...)...)
	if gateway != nil {
		f.owned.AddContainer(gateway.Container)
	}
	if err != nil {
		return gateway, fmt.Errorf("run RGW zone %s: %w", zone, err)
	}
	return gateway, nil
}

func rgwEndpointArgs(args []string, endpoint string) []string {
	if endpoint != "" {
		args = append(args, "--endpoints", endpoint)
	}
	return args
}

func (f *Fixture) validatePeriodEndpoints(data []byte) error {
	var period struct {
		RealmID    string `json:"realm_id"`
		MasterZone string `json:"master_zone"`
		PeriodMap  struct {
			Zonegroups []struct {
				Name       string   `json:"name"`
				MasterZone string   `json:"master_zone"`
				Endpoints  []string `json:"endpoints"`
				Zones      []struct {
					ID        string   `json:"id"`
					Name      string   `json:"name"`
					Endpoints []string `json:"endpoints"`
				} `json:"zones"`
			} `json:"zonegroups"`
		} `json:"period_map"`
	}
	if err := json.Unmarshal(data, &period); err != nil {
		return fmt.Errorf("decode committed RGW period: %w", err)
	}
	if period.RealmID != f.RealmID || period.MasterZone != f.SourceZoneID {
		return fmt.Errorf("committed RGW period has unexpected realm or metadata master")
	}
	for _, group := range period.PeriodMap.Zonegroups {
		if group.Name != f.realm.config.Zonegroup {
			continue
		}
		if group.MasterZone != f.SourceZoneID || !slices.Equal(group.Endpoints, []string{f.realm.sourceURL}) {
			return fmt.Errorf("committed zonegroup %s has unexpected master/endpoints: %v", group.Name, group.Endpoints)
		}
		found := 0
		for _, zone := range group.Zones {
			var expectedID, expectedURL string
			switch zone.Name {
			case f.realm.config.SourceZone:
				expectedID, expectedURL = f.SourceZoneID, f.realm.sourceURL
			case f.realm.config.DestinationZone:
				expectedID, expectedURL = f.DestinationZoneID, f.realm.destinationURL
			default:
				continue
			}
			if zone.ID != expectedID || !slices.Equal(zone.Endpoints, []string{expectedURL}) {
				return fmt.Errorf("committed zone %s has unexpected ID/endpoints: %v; expected endpoint %s", zone.Name, zone.Endpoints, expectedURL)
			}
			found++
		}
		if found != 2 {
			return fmt.Errorf("committed zonegroup contains %d expected zones, want 2", found)
		}
		return nil
	}
	return fmt.Errorf("committed period has no zonegroup %s", f.realm.config.Zonegroup)
}

func (r *rgwRealmAccess) admin(ctx context.Context, client testcontainers.Container, zone string, args ...string) ([]byte, error) {
	return r.adminInGroup(ctx, client, r.zoneGroupName(zone), zone, args...)
}

func (r *rgwRealmAccess) adminInGroup(ctx context.Context, client testcontainers.Container, group, zone string, args ...string) ([]byte, error) {
	command := []string{"radosgw-admin", "--keyring", "/etc/ceph/ceph.client.admin.keyring", "--rgw-realm", r.config.Realm, "--rgw-zonegroup", group, "--rgw-zone", zone, "--osd-pool-default-pg-num", "1", "--osd-pool-default-pgp-num", "0"}
	return multicluster.Exec(ctx, client, append(command, args...)...)
}

func (r *rgwRealmAccess) zoneGroupName(zone string) string {
	r.scopeMu.RLock()
	defer r.scopeMu.RUnlock()
	if group := r.zoneGroups[zone]; group != "" {
		return group
	}
	return r.config.Zonegroup
}

func ensureFreshRGWCluster(ctx context.Context, cluster *ceph.Container) error {
	gateways, err := rgw.Gateways(ctx, cluster)
	if err != nil {
		return fmt.Errorf("inspect RGW storage gateways: %w", err)
	}
	if len(gateways) != 0 {
		return fmt.Errorf("RGW topology requires fresh storage clusters without existing gateways")
	}
	data, err := cluster.Ceph(ctx, "osd", "pool", "ls", "--format", "json")
	if err != nil {
		return fmt.Errorf("inspect RGW storage cluster: %w", err)
	}
	var pools []string
	if err := json.Unmarshal(data, &pools); err != nil {
		return fmt.Errorf("invalid RGW storage pool list: %w", err)
	}
	if slices.Contains(pools, ".rgw.root") {
		return fmt.Errorf("RGW topology requires fresh storage clusters without existing RGW configuration")
	}
	return nil
}

// PullSourcePeriod imports the destination's realm and current committed
// period into the source cluster, using private system-user credentials.
// Native realm pull updates the realm's current-period pointer and reflects
// its zonegroups; period pull alone only stores the fetched period. After a
// metadata master change, fence the former master before calling this method
// and restart its gateway afterwards. This does not promote a zone or restart
// a gateway automatically.
//
//purpose:condition
func (f *Fixture) PullSourcePeriod(ctx context.Context) error {
	if f.realm.systemAccess == "" || f.realm.systemSecret == "" || f.realm.destinationURL == "" {
		return fmt.Errorf("RGW multisite system credentials are unavailable")
	}
	_, err := f.realm.admin(ctx, f.realm.sourceClient, f.realm.config.SourceZone, "realm", "pull", "--url", f.realm.destinationURL, "--access-key", f.realm.systemAccess, "--secret", f.realm.systemSecret)
	return err
}

// PullDestinationPeriod imports the source's realm and current committed
// period into the destination cluster, including the current-period pointer
// and reflected zonegroups. Use it to recover a fenced destination after a
// planned failback; promotion and gateway restart remain caller decisions.
//
//purpose:condition
func (f *Fixture) PullDestinationPeriod(ctx context.Context) error {
	if f.realm.systemAccess == "" || f.realm.systemSecret == "" || f.realm.sourceURL == "" {
		return fmt.Errorf("RGW multisite system credentials are unavailable")
	}
	_, err := f.realm.admin(ctx, f.realm.destinationClient, f.realm.config.DestinationZone, "realm", "pull", "--url", f.realm.sourceURL, "--access-key", f.realm.systemAccess, "--secret", f.realm.systemSecret)
	return err
}

// Terminate removes owned gateways, CLI clients and the HTTP bridge. Realm,
// zone configuration and data remain; neither Ceph cluster is terminated.
//
//purpose:lifetime
func (f *Fixture) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	if err := multicluster.LockObservation(ctx, &f.topologyMu); err != nil {
		return err
	}
	defer f.topologyMu.Unlock()
	f.closed = true
	return f.owned.Terminate(ctx, opts...)
}
