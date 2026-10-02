package multicluster

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// RBDMirrorConfig connects two existing clusters through one destination-side
// rbd-mirror daemon. Both clusters must already contain an initialized RBD Pool.
// SourceSite and DestinationSite default to "source" and "destination".
type RBDMirrorConfig struct {
	Source          *ceph.Container
	Destination     *ceph.Container
	Pool            string
	SourceSite      string
	DestinationSite string
}

// RBDMirror owns its daemon and setup CLI containers, not the Ceph clusters.
// Stop and Start control the daemon; Terminate removes all owned containers.
type RBDMirror struct {
	testcontainers.Container
	sourceClient, destinationClient testcontainers.Container
	config                          RBDMirrorConfig
	owned                           resources
}

// RunRBDMirror enables image-mode mirroring on both pools, imports a receiving
// peer into the destination, and starts a daemon connected to both clusters.
// A new peer is rx-only; an existing tx-only peer becomes rx-tx so creating a
// reverse link preserves the transmission already used by the first link.
// image must contain the RBD CLI and rbd-mirror. Enable snapshot mirroring and
// create checkpoints on each intended source image separately with the RBD CLI.
//
// The caller must Terminate a non-nil result even when setup returns an error.
// Terminate removes runtime containers but leaves mirroring mode, peer and auth
// configuration in the disposable clusters; it never destroys pools or data.
// Customizers apply to the daemon only and must preserve its networking, Ceph
// configuration, credentials and entrypoint.
func RunRBDMirror(ctx context.Context, image string, config RBDMirrorConfig, opts ...testcontainers.ContainerCustomizer) (*RBDMirror, error) {
	if err := validatePair(image, config.Source, config.Destination); err != nil {
		return nil, fmt.Errorf("configure RBD mirror: %w", err)
	}
	config, err := normalizeRBDMirrorConfig(config)
	if err != nil {
		return nil, fmt.Errorf("configure RBD mirror: %w", err)
	}
	mirror := &RBDMirror{config: config}
	sourceClient, err := runClient(ctx, image, config.Source, config.Destination.NetworkName(), &mirror.owned)
	if err != nil {
		return mirror, fmt.Errorf("run source RBD setup client: %w", err)
	}
	mirror.sourceClient = sourceClient
	destinationClient, err := runClient(ctx, image, config.Destination, config.Source.NetworkName(), &mirror.owned)
	if err != nil {
		return mirror, fmt.Errorf("run destination RBD setup client: %w", err)
	}
	mirror.destinationClient = destinationClient
	for _, site := range []struct {
		client testcontainers.Container
		name   string
	}{
		{sourceClient, config.SourceSite},
		{destinationClient, config.DestinationSite},
	} {
		if _, err := exec(ctx, site.client, "rbd", "mirror", "pool", "enable", "--site-name", site.name, config.Pool, "image"); err != nil {
			return mirror, fmt.Errorf("enable %s RBD pool mirroring: %w", site.name, err)
		}
	}
	if err := mirror.Rebootstrap(ctx); err != nil {
		return mirror, err
	}

	clientName := "client.rbd-mirror.tc-" + uuid.NewString()
	keyring, err := config.Destination.Ceph(ctx, "auth", "get-or-create", clientName,
		"mon", "profile rbd-mirror", "osd", "profile rbd")
	if err != nil {
		return mirror, fmt.Errorf("create RBD mirror daemon credentials: %w", err)
	}
	daemonOpts := []testcontainers.ContainerCustomizer{
		config.Destination.WithClient(),
		network.WithNetworkName(nil, config.Source.NetworkName()),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader: bytes.NewReader(keyring), ContainerFilePath: "/etc/ceph/rbd-mirror.keyring", FileMode: 0o600,
		}),
		testcontainers.WithEntrypoint("rbd-mirror"),
		testcontainers.WithCmd("-f", "--name", clientName, "--keyring", "/etc/ceph/rbd-mirror.keyring",
			"--admin-socket", "/tmp/rbd-mirror.asok", "--log-to-stderr=true", "--log-to-file=false"),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"test", "-S", "/tmp/rbd-mirror.asok"}).WithStartupTimeout(time.Minute)),
	}
	daemonOpts = append(daemonOpts, opts...)
	daemon, err := testcontainers.Run(ctx, image, daemonOpts...)
	if daemon != nil {
		mirror.Container = daemon
		mirror.owned.addContainer(daemon)
	}
	if err != nil {
		return mirror, fmt.Errorf("run destination RBD mirror daemon: %w", err)
	}
	return mirror, nil
}

// Rebootstrap creates or refreshes this link's destination receiving peer using
// the existing setup clients. It is useful after an explicit peer removal, and
// fills credentials and upgrades a matching tx-only peer to rx-tx when creating
// a reverse link. Native bootstrap import refreshes its monitor/key attributes
// but does not populate an existing peer's client name or receiving direction.
// The caller owns the policy for quiescing writes and resynchronizing images.
func (m *RBDMirror) Rebootstrap(ctx context.Context) (returnErr error) {
	if m == nil || m.sourceClient == nil || m.destinationClient == nil {
		return errors.New("RBD mirror setup clients are unavailable")
	}
	config := m.config
	token, err := m.SourceRBD(ctx, "mirror", "pool", "peer", "bootstrap", "create", "--site-name", config.SourceSite, config.Pool)
	if err != nil {
		return fmt.Errorf("create RBD mirror peer token: %w", err)
	}
	if len(bytes.TrimSpace(token)) == 0 {
		return errors.New("source returned an empty RBD mirror peer token")
	}
	bootstrap, err := decodeRBDBootstrapIdentity(token)
	if err != nil {
		return fmt.Errorf("read source RBD mirror bootstrap identity: %w", err)
	}
	tokenPath := "/tmp/rbd-mirror-peer-token-" + uuid.NewString()
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := exec(cleanupCtx, m.destinationClient, "rm", "-f", tokenPath); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("remove RBD mirror peer token file: %w", err))
		}
	}()
	if err := m.destinationClient.CopyToContainer(ctx, token, tokenPath, 0o600); err != nil {
		return fmt.Errorf("copy RBD mirror peer token: %w", err)
	}
	if _, err := m.DestinationRBD(ctx, "mirror", "pool", "peer", "bootstrap", "import",
		"--site-name", config.DestinationSite, "--direction", "rx-only", config.Pool, tokenPath); err != nil {
		return fmt.Errorf("import rx-only RBD mirror peer: %w", err)
	}
	// Bootstrap import reuses an existing peer without updating its client or
	// direction. It does refresh the monitor/key attributes from the token.
	// The remote daemon can already have registered this site as tx-only. Use
	// both its site identity and mirror UUID so only this link is updated.
	fsid, err := exec(ctx, m.sourceClient, "ceph", "fsid")
	if err != nil {
		return fmt.Errorf("read source cluster identity: %w", err)
	}
	if bootstrap.FSID != strings.TrimSpace(string(fsid)) {
		return errors.New("source RBD bootstrap token has a conflicting cluster identity")
	}
	sourceInfo, err := m.SourceRBD(ctx, "mirror", "pool", "info", config.Pool, "--format", "json")
	if err != nil {
		return fmt.Errorf("read source RBD mirror identity: %w", err)
	}
	var sourcePool struct {
		MirrorUUID string `json:"mirror_uuid"`
	}
	if err := json.Unmarshal(sourceInfo, &sourcePool); err != nil || sourcePool.MirrorUUID == "" {
		return fmt.Errorf("read source RBD mirror identity: invalid pool info (decode error: %v)", err)
	}
	destinationInfo, err := m.DestinationRBD(ctx, "mirror", "pool", "info", config.Pool, "--format", "json")
	if err != nil {
		return fmt.Errorf("read destination RBD mirror peer: %w", err)
	}
	peer, err := matchingRBDMirrorPeer(destinationInfo, config.SourceSite, strings.TrimSpace(string(fsid)), sourcePool.MirrorUUID)
	if err != nil {
		return fmt.Errorf("identify imported RBD mirror peer: %w", err)
	}
	if peer.ClientName != bootstrap.ClientName {
		if _, err := m.DestinationRBD(ctx, "mirror", "pool", "peer", "set", config.Pool, peer.UUID, "client", bootstrap.ClientName); err != nil {
			return fmt.Errorf("configure imported RBD mirror peer client: %w", err)
		}
	}
	if peer.Direction == "tx-only" {
		if _, err := m.DestinationRBD(ctx, "mirror", "pool", "peer", "set", config.Pool, peer.UUID, "direction", "rx-tx"); err != nil {
			return fmt.Errorf("enable receiving on imported RBD mirror peer: %w", err)
		}
	}
	return nil
}

type rbdMirrorPeer struct {
	UUID       string `json:"uuid"`
	Direction  string `json:"direction"`
	SiteName   string `json:"site_name"`
	MirrorUUID string `json:"mirror_uuid"`
	ClientName string `json:"client_name"`
}

type rbdBootstrapIdentity struct {
	FSID       string
	ClientName string
}

// Decode only identity fields. Neither errors nor callers need the CephX key.
func decodeRBDBootstrapIdentity(token []byte) (rbdBootstrapIdentity, error) {
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(token)))
	if err != nil {
		return rbdBootstrapIdentity{}, errors.New("invalid bootstrap token encoding")
	}
	var identity struct {
		FSID     string `json:"fsid"`
		ClientID string `json:"client_id"`
	}
	if err := json.Unmarshal(decoded, &identity); err != nil {
		return rbdBootstrapIdentity{}, errors.New("invalid bootstrap token identity JSON")
	}
	if _, err := uuid.Parse(identity.FSID); err != nil {
		return rbdBootstrapIdentity{}, errors.New("invalid bootstrap token cluster FSID")
	}
	if identity.ClientID == "" || strings.HasPrefix(identity.ClientID, "-") || strings.ContainsAny(identity.ClientID, "/ \t\r\n") {
		return rbdBootstrapIdentity{}, errors.New("invalid bootstrap token client identity")
	}
	return rbdBootstrapIdentity{FSID: identity.FSID, ClientName: "client." + identity.ClientID}, nil
}

func matchingRBDMirrorPeer(info []byte, site, fsid, mirrorUUID string) (rbdMirrorPeer, error) {
	var pool struct {
		Peers []rbdMirrorPeer `json:"peers"`
	}
	if site == "" || fsid == "" || mirrorUUID == "" {
		return rbdMirrorPeer{}, errors.New("source site, FSID and mirror UUID are required")
	}
	if err := json.Unmarshal(info, &pool); err != nil {
		return rbdMirrorPeer{}, fmt.Errorf("decode RBD pool info: %w", err)
	}
	var match rbdMirrorPeer
	for _, peer := range pool.Peers {
		if peer.SiteName != site && peer.SiteName != fsid {
			continue
		}
		if peer.UUID == "" || (peer.MirrorUUID != "" && peer.MirrorUUID != mirrorUUID) {
			return rbdMirrorPeer{}, errors.New("peer site matches but its mirror identity is missing or conflicts")
		}
		if match.UUID != "" {
			return rbdMirrorPeer{}, errors.New("multiple RBD mirror peers match the source identity")
		}
		switch peer.Direction {
		case "rx-only", "rx-tx", "tx-only":
		default:
			return rbdMirrorPeer{}, fmt.Errorf("unsupported RBD peer direction %q", peer.Direction)
		}
		match = peer
	}
	if match.UUID == "" {
		return rbdMirrorPeer{}, errors.New("imported RBD mirror peer is missing")
	}
	return match, nil
}

// SourceRBD executes the RBD CLI in this link's source setup client. Arguments
// are passed without shell interpolation; use --format json for state queries.
func (m *RBDMirror) SourceRBD(ctx context.Context, args ...string) ([]byte, error) {
	if m == nil || m.sourceClient == nil {
		return nil, errors.New("RBD mirror source setup client is unavailable")
	}
	return exec(ctx, m.sourceClient, append([]string{"rbd"}, args...)...)
}

// DestinationRBD executes the RBD CLI in the destination setup client. It can
// promote/demote images, request resync, and inspect or remove explicit peers.
func (m *RBDMirror) DestinationRBD(ctx context.Context, args ...string) ([]byte, error) {
	if m == nil || m.destinationClient == nil {
		return nil, errors.New("RBD mirror destination setup client is unavailable")
	}
	return exec(ctx, m.destinationClient, append([]string{"rbd"}, args...)...)
}

// Terminate removes the daemon and setup clients while preserving both clusters,
// their image data, and the mirroring/auth configuration written during setup.
func (m *RBDMirror) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	return m.owned.terminate(ctx, opts...)
}

func normalizeRBDMirrorConfig(config RBDMirrorConfig) (RBDMirrorConfig, error) {
	config.Pool = strings.TrimSpace(config.Pool)
	config.SourceSite = strings.TrimSpace(config.SourceSite)
	config.DestinationSite = strings.TrimSpace(config.DestinationSite)
	if config.SourceSite == "" {
		config.SourceSite = "source"
	}
	if config.DestinationSite == "" {
		config.DestinationSite = "destination"
	}
	if config.Pool == "" {
		return config, errors.New("RBD pool is required")
	}
	if strings.HasPrefix(config.Pool, "-") || strings.ContainsAny(config.Pool, "/ \t\r\n") {
		return config, errors.New("RBD pool must be a pool name without namespace or option syntax")
	}
	if strings.HasPrefix(config.SourceSite, "-") || strings.HasPrefix(config.DestinationSite, "-") {
		return config, errors.New("RBD site names must not begin with '-'")
	}
	if config.SourceSite == config.DestinationSite {
		return config, errors.New("source and destination RBD site names must differ")
	}
	return config, nil
}
