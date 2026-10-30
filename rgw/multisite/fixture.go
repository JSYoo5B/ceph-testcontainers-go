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
type Config struct {
	Source, Destination                           *ceph.Container
	Realm, Zonegroup, SourceZone, DestinationZone string
	// SetupImage explicitly selects the RGW administration runtime.
	SetupImage           string
	destinationZonegroup string
}

// Fixture owns two gateways and their CLI clients. Bridge mode adds a
// private HTTP network; host mode uses allocated gateway ports on the Docker
// host. Source and Destination expose matching ordinary S3 test credentials.
type Fixture struct {
	Source, Destination                      *rgw.Gateway
	RealmID, SourceZoneID, DestinationZoneID string
	config                                   Config
	sourceClient, destinationClient          testcontainers.Container
	sourceURL, destinationURL                string
	systemAccess, systemSecret               string
	owned                                    multicluster.Resources
	topologyMu                               sync.Mutex
	httpNetwork                              *testcontainers.DockerNetwork
	additionalZones                          map[string]*rgwZoneState
	zoneGroups                               map[string]string
	scopeMu                                  sync.RWMutex
	groupIDs                                 map[string]string
	groupMasters                             map[string]string
	removedZones                             map[string]*rgwZoneRemoval
	reloadNeeded                             map[string]bool
	bootstrapStagingPending                  bool
	closed                                   bool
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
func WithConfig(config Config) Option { return Option{config: &config} }

// WithTopology carries a realm of at least two zones, in one or several
// zonegroups, to Run. The zone lists are copied.
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
	if err := multicluster.ValidatePair(ctx, image, config.Source, config.Destination); err != nil {
		return nil, err
	}
	if err := multicluster.RequireRoleImage(image, multicluster.RoleRGW, config.Source, config.Destination); err != nil {
		return nil, fmt.Errorf("configure RGW multisite gateway image: %w", err)
	}
	sourceFacts, err := ceph.FactsOf(config.Source)
	if err != nil {
		return nil, err
	}
	destinationFacts, err := ceph.FactsOf(config.Destination)
	if err != nil {
		return nil, err
	}
	sourceClientImage, err := rgwSetupImage(config.SetupImage, sourceFacts.Images.RGW)
	if err != nil {
		return nil, err
	}
	destinationClientImage, err := rgwSetupImage(config.SetupImage, destinationFacts.Images.RGW)
	if err != nil {
		return nil, err
	}
	for _, cluster := range []*ceph.Container{config.Source, config.Destination} {
		if err := ensureFreshRGWCluster(ctx, cluster); err != nil {
			return nil, err
		}
	}
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
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
			return nil, fmt.Errorf("invalid RGW realm/zone name")
		}
	}
	if config.SourceZone == config.DestinationZone {
		return nil, fmt.Errorf("RGW zones must be distinct")
	}
	if config.destinationZonegroup == "" {
		config.destinationZonegroup = config.Zonegroup
	}
	ctx, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()
	f := &Fixture{config: config, bootstrapStagingPending: true,
		zoneGroups: map[string]string{config.SourceZone: config.Zonegroup, config.DestinationZone: config.destinationZonegroup},
		groupIDs:   make(map[string]string), groupMasters: map[string]string{config.Zonegroup: config.SourceZone}}
	if config.destinationZonegroup != config.Zonegroup {
		f.groupMasters[config.destinationZonegroup] = config.DestinationZone
	}
	var bridge *testcontainers.DockerNetwork
	peerNetwork := ""
	if !sourceFacts.UsesHostNetwork {
		bridge, err = dockerbridge.New(ctx)
		if bridge != nil {
			f.httpNetwork = bridge
			f.owned.AddCleanup("remove RGW HTTP bridge", func(cleanupCtx context.Context) error {
				return multicluster.IgnoreMissing(bridge.Remove(cleanupCtx))
			})
		}
		if err != nil {
			return f, fmt.Errorf("create RGW HTTP bridge: %w", err)
		}
		peerNetwork = bridge.Name
	}
	f.sourceClient, err = multicluster.RunClient(ctx, sourceClientImage, config.Source, peerNetwork, &f.owned)
	if err != nil {
		return f, err
	}
	f.destinationClient, err = multicluster.RunClient(ctx, destinationClientImage, config.Destination, peerNetwork, &f.owned)
	if err != nil {
		return f, err
	}
	sourceAlias, destinationAlias := "rgw-primary-"+suffix, "rgw-secondary-"+suffix
	sourceURL, destinationURL := "http://"+sourceAlias+":7480", "http://"+destinationAlias+":7480"
	if sourceFacts.UsesHostNetwork {
		// A named host gateway chooses its port at startup. Commit the fresh
		// zone without endpoints first, then publish its actual owned listener.
		sourceURL, destinationURL = "", ""
	}
	systemAccess, systemSecret := strings.ReplaceAll(uuid.NewString(), "-", ""), uuid.NewString()+uuid.NewString()
	f.sourceURL, f.destinationURL = sourceURL, destinationURL
	f.systemAccess, f.systemSecret = systemAccess, systemSecret
	primaryCommands := [][]string{
		{"realm", "create", "--default"},
		rgwEndpointArgs([]string{"zonegroup", "create", "--master", "--default"}, sourceURL),
		rgwEndpointArgs([]string{"zone", "create", "--master", "--default"}, sourceURL),
		{"user", "create", "--uid", "tc-sync-" + suffix, "--display-name", "Testcontainers multisite sync", "--system", "--access-key", systemAccess, "--secret-key", systemSecret},
		{"zone", "modify", "--access-key", systemAccess, "--secret", systemSecret},
		{"period", "update", "--commit"},
	}
	for _, cmd := range primaryCommands {
		if _, err := f.admin(ctx, f.sourceClient, f.config.SourceZone, cmd...); err != nil {
			return f, err
		}
	}
	f.Source, err = f.runGateway(ctx, image, config.Source, bridge, sourceAlias, config.SourceZone, opts...)
	if err != nil {
		return f, err
	}
	if sourceFacts.UsesHostNetwork {
		sourceURL, err = f.Source.DaemonEndpoint(ctx)
		if err != nil {
			return f, err
		}
		f.sourceURL = sourceURL
		for _, cmd := range [][]string{
			{"zone", "modify", "--endpoints", sourceURL},
			{"zonegroup", "modify", "--endpoints", sourceURL},
			{"period", "update", "--commit"},
		} {
			if _, err := f.admin(ctx, f.sourceClient, f.config.SourceZone, cmd...); err != nil {
				return f, err
			}
		}
	}
	secondaryCommands := [][]string{
		{"realm", "pull", "--url", sourceURL, "--access-key", systemAccess, "--secret", systemSecret},
		{"realm", "default"},
	}
	if config.destinationZonegroup != config.Zonegroup {
		secondaryCommands = append(secondaryCommands, rgwEndpointArgs([]string{"zonegroup", "create", "--api-name", config.destinationZonegroup}, destinationURL))
	}
	zoneCreate := []string{"zone", "create", "--access-key", systemAccess, "--secret", systemSecret}
	if config.destinationZonegroup != config.Zonegroup {
		zoneCreate = append(zoneCreate, "--master")
	}
	secondaryCommands = append(secondaryCommands, rgwEndpointArgs(zoneCreate, destinationURL), []string{"period", "update", "--commit"})
	for _, cmd := range secondaryCommands {
		if _, err := f.admin(ctx, f.destinationClient, f.config.DestinationZone, cmd...); err != nil {
			return f, err
		}
	}
	f.Destination, err = f.runGateway(ctx, image, config.Destination, bridge, destinationAlias, config.DestinationZone, opts...)
	if err != nil {
		return f, err
	}
	if destinationFacts.UsesHostNetwork {
		destinationURL, err = f.Destination.DaemonEndpoint(ctx)
		if err != nil {
			return f, err
		}
		f.destinationURL = destinationURL
		commands := [][]string{{"zone", "modify", "--endpoints", destinationURL}}
		if config.destinationZonegroup != config.Zonegroup {
			commands = append(commands, []string{"zonegroup", "modify", "--endpoints", destinationURL})
		}
		commands = append(commands, []string{"period", "update", "--commit"})
		for _, cmd := range commands {
			if _, err := f.admin(ctx, f.destinationClient, f.config.DestinationZone, cmd...); err != nil {
				return f, err
			}
		}
	}
	for _, item := range []struct {
		client       testcontainers.Container
		zone, entity string
		target       *string
	}{
		{f.sourceClient, config.SourceZone, "realm", &f.RealmID},
		{f.sourceClient, config.SourceZone, "zone", &f.SourceZoneID},
		{f.destinationClient, config.DestinationZone, "zone", &f.DestinationZoneID},
	} {
		data, err := f.admin(ctx, item.client, item.zone, item.entity, "get")
		if err != nil {
			return f, err
		}
		var identity struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(data, &identity); err != nil || identity.ID == "" {
			return f, fmt.Errorf("invalid RGW %s identity", item.entity)
		}
		*item.target = identity.ID
	}
	data, err := f.admin(ctx, f.destinationClient, f.config.DestinationZone, "realm", "get")
	if err != nil {
		return f, err
	}
	var destinationRealm struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(data, &destinationRealm); err != nil || destinationRealm.ID != f.RealmID || f.SourceZoneID == f.DestinationZoneID {
		return f, fmt.Errorf("RGW multisite realm/zone identities do not match the requested topology")
	}
	for _, zone := range f.zoneStates() {
		data, err := f.admin(ctx, zone.client, zone.Name, "zonegroup", "get")
		if err != nil {
			return f, err
		}
		var group struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(data, &group); err != nil || group.ID == "" {
			return f, fmt.Errorf("invalid RGW zonegroup identity")
		}
		f.groupIDs[zone.Zonegroup] = group.ID
	}
	if err := f.waitZonePeriods(ctx, f.zoneStates()[0], f.zoneStates()); err != nil {
		return f, err
	}
	// A secondary commits its own staging object remotely. The master can
	// retain its earlier source-only staging object after the final current
	// period includes both zones. Finish the fresh bootstrap with a canonical
	// unpublished staging snapshot so later guarded policy updates can proceed.
	if err := f.canonicalizeBootstrapStaging(ctx); err != nil {
		return f, err
	}
	if sourceFacts.UsesHostNetwork || config.destinationZonegroup != config.Zonegroup {
		// Both gateways started against an earlier bootstrap period before
		// allocated endpoints were committed. Load the verified final period
		// before admitting S3 operations instead of racing asynchronous reload.
		if err := f.waitZoneMetadataBootstrap(ctx, f.zoneStates()[0], f.zoneStates()); err != nil {
			return f, err
		}
		if err := f.reloadZoneGateways(ctx, f.zoneStates(), nil); err != nil {
			return f, err
		}
	}
	user, err := f.admin(ctx, f.sourceClient, f.config.SourceZone, "user", "create", "--uid", "tc-user-"+suffix, "--display-name", "Testcontainers multicluster S3 user")
	if err != nil {
		return f, err
	}
	var credentials struct {
		Keys []struct {
			AccessKey string `json:"access_key"`
			SecretKey string `json:"secret_key"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(user, &credentials); err != nil || len(credentials.Keys) != 1 || credentials.Keys[0].AccessKey == "" || credentials.Keys[0].SecretKey == "" {
		return f, fmt.Errorf("invalid RGW S3 test credentials")
	}
	for _, gateway := range []*rgw.Gateway{f.Source, f.Destination} {
		gateway.AccessKey = credentials.Keys[0].AccessKey
		gateway.SecretKey = credentials.Keys[0].SecretKey
	}
	return f, nil
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
		Name: alias, Realm: f.config.Realm, Zonegroup: f.zoneGroupName(zone), Zone: zone,
		Region: f.zoneGroupName(zone), SkipUserCreation: true,
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
		if group.Name != f.config.Zonegroup {
			continue
		}
		if group.MasterZone != f.SourceZoneID || !slices.Equal(group.Endpoints, []string{f.sourceURL}) {
			return fmt.Errorf("committed zonegroup %s has unexpected master/endpoints: %v", group.Name, group.Endpoints)
		}
		found := 0
		for _, zone := range group.Zones {
			var expectedID, expectedURL string
			switch zone.Name {
			case f.config.SourceZone:
				expectedID, expectedURL = f.SourceZoneID, f.sourceURL
			case f.config.DestinationZone:
				expectedID, expectedURL = f.DestinationZoneID, f.destinationURL
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
	return fmt.Errorf("committed period has no zonegroup %s", f.config.Zonegroup)
}

func (f *Fixture) admin(ctx context.Context, client testcontainers.Container, zone string, args ...string) ([]byte, error) {
	return f.adminInGroup(ctx, client, f.zoneGroupName(zone), zone, args...)
}

func (f *Fixture) adminInGroup(ctx context.Context, client testcontainers.Container, group, zone string, args ...string) ([]byte, error) {
	command := []string{"radosgw-admin", "--keyring", "/etc/ceph/ceph.client.admin.keyring", "--rgw-realm", f.config.Realm, "--rgw-zonegroup", group, "--rgw-zone", zone, "--osd-pool-default-pg-num", "1", "--osd-pool-default-pgp-num", "0"}
	return multicluster.Exec(ctx, client, append(command, args...)...)
}

func (f *Fixture) zoneGroupName(zone string) string {
	f.scopeMu.RLock()
	defer f.scopeMu.RUnlock()
	if group := f.zoneGroups[zone]; group != "" {
		return group
	}
	return f.config.Zonegroup
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
func (f *Fixture) PullSourcePeriod(ctx context.Context) error {
	if f.systemAccess == "" || f.systemSecret == "" || f.destinationURL == "" {
		return fmt.Errorf("RGW multisite system credentials are unavailable")
	}
	_, err := f.admin(ctx, f.sourceClient, f.config.SourceZone, "realm", "pull", "--url", f.destinationURL, "--access-key", f.systemAccess, "--secret", f.systemSecret)
	return err
}

// PullDestinationPeriod imports the source's realm and current committed
// period into the destination cluster, including the current-period pointer
// and reflected zonegroups. Use it to recover a fenced destination after a
// planned failback; promotion and gateway restart remain caller decisions.
func (f *Fixture) PullDestinationPeriod(ctx context.Context) error {
	if f.systemAccess == "" || f.systemSecret == "" || f.sourceURL == "" {
		return fmt.Errorf("RGW multisite system credentials are unavailable")
	}
	_, err := f.admin(ctx, f.destinationClient, f.config.DestinationZone, "realm", "pull", "--url", f.sourceURL, "--access-key", f.systemAccess, "--secret", f.systemSecret)
	return err
}

// Terminate removes owned gateways, CLI clients and the HTTP bridge. Realm,
// zone configuration and data remain; neither Ceph cluster is terminated.
func (f *Fixture) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	if err := multicluster.LockObservation(ctx, &f.topologyMu); err != nil {
		return err
	}
	defer f.topologyMu.Unlock()
	f.closed = true
	return f.owned.Terminate(ctx, opts...)
}
