package federation

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// RGWMultisiteConfig describes a two-zone realm in fresh test clusters. The
// source is the metadata master; both zones accept object writes. ControlImage
// optionally selects the CLI image independently from the RGW daemon image.
type RGWMultisiteConfig struct {
	Source, Destination                           *ceph.Container
	Realm, Zonegroup, SourceZone, DestinationZone string
	ControlImage                                  string
}

// RGWMultisite owns two gateways, their CLI clients and an HTTP bridge. Each
// gateway connects only to its own Ceph cluster and the bridge. Source and
// Destination expose matching ordinary S3 test credentials after setup.
type RGWMultisite struct {
	Source, Destination                      *ceph.RGWContainer
	RealmID, SourceZoneID, DestinationZoneID string
	config                                   RGWMultisiteConfig
	sourceClient, destinationClient          testcontainers.Container
	owned                                    resources
}

// RunRGWMultisite configures native RGW multisite on two existing clusters.
// opts customize both gateways and are applied last. Do not replace required
// networking or commands. A non-nil result returned with an error must still
// be terminated. The clusters must not already serve standalone RGW traffic.
func RunRGWMultisite(ctx context.Context, image string, config RGWMultisiteConfig, opts ...testcontainers.ContainerCustomizer) (*RGWMultisite, error) {
	if err := validatePair(image, config.Source, config.Destination); err != nil {
		return nil, err
	}
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	if config.Realm == "" {
		config.Realm = "tc-federation-" + suffix
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
	if config.ControlImage == "" {
		config.ControlImage = image
	}
	if strings.TrimSpace(config.ControlImage) == "" {
		return nil, fmt.Errorf("RGW control image must not be empty")
	}
	ctx, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()
	f := &RGWMultisite{config: config}
	bridge, err := network.New(ctx)
	if err != nil {
		return f, fmt.Errorf("create RGW HTTP bridge: %w", err)
	}
	f.owned.addCleanup("remove RGW HTTP bridge", func(cleanupCtx context.Context) error {
		return ignoreMissing(bridge.Remove(cleanupCtx))
	})
	f.sourceClient, err = runClient(ctx, config.ControlImage, config.Source, bridge.Name, &f.owned)
	if err != nil {
		return f, err
	}
	f.destinationClient, err = runClient(ctx, config.ControlImage, config.Destination, bridge.Name, &f.owned)
	if err != nil {
		return f, err
	}
	sourceAlias, destinationAlias := "rgw-primary-"+suffix, "rgw-secondary-"+suffix
	sourceURL, destinationURL := "http://"+sourceAlias+":7480", "http://"+destinationAlias+":7480"
	systemAccess, systemSecret := strings.ReplaceAll(uuid.NewString(), "-", ""), uuid.NewString()+uuid.NewString()
	primaryCommands := [][]string{
		{"realm", "create", "--default"},
		{"zonegroup", "create", "--master", "--default", "--endpoints", sourceURL},
		{"zone", "create", "--master", "--default", "--endpoints", sourceURL},
		{"user", "create", "--uid", "tc-sync-" + suffix, "--display-name", "Testcontainers multisite sync", "--system", "--access-key", systemAccess, "--secret-key", systemSecret},
		{"zone", "modify", "--access-key", systemAccess, "--secret", systemSecret},
		{"period", "update", "--commit"},
	}
	for _, cmd := range primaryCommands {
		if _, err := f.SourceAdmin(ctx, cmd...); err != nil {
			return f, err
		}
	}
	f.Source, err = f.runGateway(ctx, image, config.Source, bridge, sourceAlias, config.SourceZone, opts...)
	if err != nil {
		return f, err
	}
	secondaryCommands := [][]string{
		{"realm", "pull", "--url", sourceURL, "--access-key", systemAccess, "--secret", systemSecret},
		{"realm", "default"},
		{"zone", "create", "--endpoints", destinationURL, "--access-key", systemAccess, "--secret", systemSecret},
		{"period", "update", "--commit"},
	}
	for _, cmd := range secondaryCommands {
		if _, err := f.DestinationAdmin(ctx, cmd...); err != nil {
			return f, err
		}
	}
	f.Destination, err = f.runGateway(ctx, image, config.Destination, bridge, destinationAlias, config.DestinationZone, opts...)
	if err != nil {
		return f, err
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
	data, err := f.DestinationAdmin(ctx, "realm", "get")
	if err != nil {
		return f, err
	}
	var destinationRealm struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(data, &destinationRealm); err != nil || destinationRealm.ID != f.RealmID || f.SourceZoneID == f.DestinationZoneID {
		return f, fmt.Errorf("RGW multisite realm/zone identities do not match the requested topology")
	}
	user, err := f.SourceAdmin(ctx, "user", "create", "--uid", "tc-user-"+suffix, "--display-name", "Testcontainers federated S3 user")
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
	for _, gateway := range []*ceph.RGWContainer{f.Source, f.Destination} {
		gateway.AccessKey = credentials.Keys[0].AccessKey
		gateway.SecretKey = credentials.Keys[0].SecretKey
	}
	return f, nil
}

func (f *RGWMultisite) runGateway(ctx context.Context, image string, cluster *ceph.Container, bridge *testcontainers.DockerNetwork, alias, zone string, opts ...testcontainers.ContainerCustomizer) (*ceph.RGWContainer, error) {
	moduleOpts := []testcontainers.ContainerCustomizer{
		cluster.WithClient(), network.WithNetwork([]string{alias}, bridge),
		testcontainers.WithEntrypoint("/bin/sh", "-c", `mkdir -p /var/run/ceph; exec radosgw -f -n client.admin --keyring /etc/ceph/ceph.client.admin.keyring --rgw-realm "$1" --rgw-zonegroup "$2" --rgw-zone "$3" --rgw-frontends 'beast port=7480' --rgw-thread-pool-size 4 --rgw-sync-obj-etag-verify true --osd-pool-default-pg-num 1 --osd-pool-default-pgp-num 0`, "rgw-multisite"),
		testcontainers.WithCmd(f.config.Realm, f.config.Zonegroup, zone), testcontainers.WithExposedPorts("7480/tcp"),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/").WithPort("7480/tcp").WithStatusCodeMatcher(func(status int) bool { return status == http.StatusOK || status == http.StatusForbidden }).WithStartupTimeout(3 * time.Minute)),
	}
	ctr, err := testcontainers.Run(ctx, image, append(moduleOpts, opts...)...)
	f.owned.addContainer(ctr)
	var gateway *ceph.RGWContainer
	if ctr != nil {
		gateway = &ceph.RGWContainer{Container: ctr, Region: f.config.Zonegroup}
	}
	if err != nil {
		return gateway, fmt.Errorf("run RGW zone %s: %w", zone, err)
	}
	return gateway, nil
}

func (f *RGWMultisite) admin(ctx context.Context, client testcontainers.Container, zone string, args ...string) ([]byte, error) {
	command := []string{"radosgw-admin", "--keyring", "/etc/ceph/ceph.client.admin.keyring", "--rgw-realm", f.config.Realm, "--rgw-zonegroup", f.config.Zonegroup, "--rgw-zone", zone, "--osd-pool-default-pg-num", "1", "--osd-pool-default-pgp-num", "0"}
	return exec(ctx, client, append(command, args...)...)
}

// SourceAdmin executes radosgw-admin in the metadata master's realm and zone.
func (f *RGWMultisite) SourceAdmin(ctx context.Context, args ...string) ([]byte, error) {
	return f.admin(ctx, f.sourceClient, f.config.SourceZone, args...)
}

// DestinationAdmin executes radosgw-admin in the secondary's realm and zone.
func (f *RGWMultisite) DestinationAdmin(ctx context.Context, args ...string) ([]byte, error) {
	return f.admin(ctx, f.destinationClient, f.config.DestinationZone, args...)
}

// Terminate removes owned gateways, CLI clients and the HTTP bridge. Realm,
// zone configuration and data remain; neither Ceph cluster is terminated.
func (f *RGWMultisite) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	return f.owned.terminate(ctx, opts...)
}
