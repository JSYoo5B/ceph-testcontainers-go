package multicluster

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// RBDMirrorMode selects the per-image replication mechanism, independently of
// the pool/namespace scope. Image scope enables images explicitly; pool scope
// automatically enrolls images carrying the journaling feature.
type RBDMirrorMode string

const (
	RBDMirrorModeSnapshot RBDMirrorMode = "snapshot"
	RBDMirrorModeJournal  RBDMirrorMode = "journal"
)

// RBDMirrorConfig connects two existing clusters through destination-side
// rbd-mirror daemons. Both clusters must already contain an initialized RBD Pool.
// SourceSite and DestinationSite default to "source" and "destination".
type RBDMirrorConfig struct {
	Source          *ceph.Container
	Destination     *ceph.Container
	Pool            string
	SourceSite      string
	DestinationSite string
	// Scope defaults to image. Pool scope automatically mirrors every journaled
	// image in the selected namespace, including existing images.
	Scope RBDMirrorScope
	// Each empty namespace means the unnamed default namespace. Both named
	// namespaces must already exist; they are never created or removed here.
	// Different names, including named/default pairs, are mapped explicitly.
	SourceNamespace, DestinationNamespace string
	// Mode defaults to snapshot for image scope and journal for pool scope.
	// Pool scope supports journal only; it never creates mirror checkpoints.
	Mode RBDMirrorMode
	// DaemonCount defaults to one. Multiple daemons share the pool's receiving
	// peer and participate in Ceph's native leader election and image assignment.
	DaemonCount int
}

// RBDMirror owns its daemons and setup CLI containers, not the Ceph clusters.
// The embedded Container is the initial daemon's compatibility handle. Its
// Stop and Start affect only that daemon. Removing it clears the handle without
// assigning another daemon. Use Daemons for current membership. Terminate
// removes all owned runtime containers, including partially created or
// previously removed daemons.
type RBDMirror struct {
	testcontainers.Container
	sourceClient, destinationClient testcontainers.Container
	config                          RBDMirrorConfig
	owned                           resources
	mu                              sync.Mutex
	closed                          bool
	image                           string
	daemonOpts                      []testcontainers.ContainerCustomizer
	daemons                         []*RBDMirrorDaemon
	initialDaemonName               string
	poolIdentities                  *rbdMirrorPoolIdentities
	policyIdentities                *rbdMirrorPolicyIdentities
}

// RBDMirrorDaemon is one destination-side rbd-mirror process. Every daemon has
// a distinct Ceph user; receiving peer credentials remain in the destination's
// config-key store. Stop and Start retain its identity and native pool state.
type RBDMirrorDaemon struct {
	testcontainers.Container
	DaemonName string
	ClientName string
	mu         sync.Mutex
	terminated bool
}

// RBDMirrorDaemonStatus is the native daemon admin socket's pool membership and
// election state. A running container can report no pool replayers until Ceph
// completes discovery; callers must inspect the intended pool and peer.
type RBDMirrorDaemonStatus struct {
	PoolReplayers []RBDMirrorPoolReplayerStatus `json:"pool_replayers"`
}

// RBDMirrorPoolReplayerStatus describes one pool/peer replayer. Image replay is
// distributed among instances; being a pool leader does not mean replaying all
// images locally.
type RBDMirrorPoolReplayerStatus struct {
	Pool             string   `json:"pool"`
	Peer             string   `json:"peer"`
	State            string   `json:"state"`
	InstanceID       string   `json:"instance_id"`
	LeaderInstanceID string   `json:"leader_instance_id"`
	Leader           bool     `json:"leader"`
	Instances        []string `json:"instances"`
}

// Status queries native rbd-mirror election state through this daemon's admin
// socket. It fails when the process is stopped or its socket is unavailable.
func (d *RBDMirrorDaemon) Status(ctx context.Context) (*RBDMirrorDaemonStatus, error) {
	if d == nil || d.Container == nil {
		return nil, errors.New("RBD mirror daemon is unavailable")
	}
	data, err := exec(ctx, d.Container, "ceph", "--admin-daemon", "/tmp/rbd-mirror.asok", "rbd", "mirror", "status")
	if err != nil {
		return nil, fmt.Errorf("read RBD mirror daemon %s status: %w", d.DaemonName, err)
	}
	var status RBDMirrorDaemonStatus
	if err := json.Unmarshal(data, &status); err != nil {
		return nil, fmt.Errorf("decode RBD mirror daemon %s status: %w", d.DaemonName, err)
	}
	if status.PoolReplayers == nil {
		return nil, fmt.Errorf("RBD mirror daemon %s returned no pool_replayers status array", d.DaemonName)
	}
	return &status, nil
}

// Terminate removes this daemon's container. Successful removals are remembered
// so fixture cleanup does not repeat hooks; failed removals remain retryable.
// Peer, CephX and image configuration belongs to the disposable clusters.
func (d *RBDMirrorDaemon) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.terminated || d.Container == nil {
		return nil
	}
	if err := ignoreMissing(d.Container.Terminate(ctx, opts...)); err != nil {
		return err
	}
	d.terminated = true
	return nil
}

// RunRBDMirror enables the selected scope on both pool/namespaces, imports a receiving
// peer into the destination, and starts a daemon connected to both clusters.
// A new peer is rx-only; an existing tx-only peer becomes rx-tx so creating a
// reverse link preserves the transmission already used by the first link.
// image must meet the control or all role requirements, including the rbd and
// ceph CLIs, rbd-mirror and the common utilities. It supplies both setup clients
// and mirror daemon containers. Enable each intended source
// image through EnableImage or the RBD CLI in image scope. Pool scope
// automatically enrolls journal-enabled images. Snapshot images need subsequent
// mirror checkpoints; journal images replay writes without mirror snapshots.
//
// The caller must Terminate a non-nil result even when setup returns an error.
// Terminate removes runtime containers but leaves mirroring mode, peer and auth
// configuration in the disposable clusters; it never destroys pools or data.
// Customizers apply to the daemon only and must preserve its networking, Ceph
// configuration, credentials and entrypoint.
// Both clusters must use the same network mode. Host-mode clusters share the
// Docker host namespace; bridge-mode daemons attach to both cluster networks.
// Initial daemon names are a through z, then node-27 and onward. A process's
// socket readiness does not guarantee pool discovery or leader convergence;
// query the daemon Status for native election state.
// Named namespace setup initializes a disabled default namespace as init-only,
// preserving an existing active default policy. Existing target scope, remote
// namespace mapping and configured site names must match; this method does not
// silently reconfigure them. Site names are cluster-wide and must agree across
// links. External pool/namespace/mirror edits must not race setup; native named
// namespaces have no separate generation. Quiesce existing writers before
// enabling pool scope. PolicyStatus reports the current native mapping.
// Successful setup captures default/selected namespace mirror UUIDs, scopes and
// mappings. Later typed mutations reject replacement or changed policies.
func RunRBDMirror(ctx context.Context, image string, config RBDMirrorConfig, opts ...testcontainers.ContainerCustomizer) (*RBDMirror, error) {
	if err := validatePair(image, config.Source, config.Destination); err != nil {
		return nil, fmt.Errorf("configure RBD mirror: %w", err)
	}
	config, err := normalizeRBDMirrorConfig(config)
	if err != nil {
		return nil, fmt.Errorf("configure RBD mirror: %w", err)
	}
	identities, err := preflightRBDMirrorPools(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("preflight RBD mirror pools/namespaces: %w", err)
	}
	mirror := &RBDMirror{config: config, image: image, daemonOpts: slices.Clone(opts), poolIdentities: identities}
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
	if err := mirror.provisionRBDMirrorPolicies(ctx); err != nil {
		return mirror, err
	}
	if err := mirror.Rebootstrap(ctx); err != nil {
		return mirror, err
	}

	for index := range config.DaemonCount {
		name := fmt.Sprintf("node-%d", index+1)
		if index < 26 {
			name = string(rune('a' + index))
		}
		if _, err := mirror.AddDaemon(ctx, name); err != nil {
			return mirror, err
		}
	}
	return mirror, nil
}

// Daemons returns a membership snapshot of this fixture's current daemons,
// including stopped containers. It does not discover daemons owned by another
// fixture, even if they replay the same pool.
func (m *RBDMirror) Daemons() []*RBDMirrorDaemon {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	daemons := slices.Clone(m.daemons)
	slices.SortFunc(daemons, func(a, b *RBDMirrorDaemon) int { return strings.Compare(a.DaemonName, b.DaemonName) })
	return daemons
}

// AddDaemon adds another process using the existing receiving peer, without
// rebootstrap or image mutation. Construction customizers apply to every daemon;
// opts are appended for this daemon only. A non-nil result on error is tracked
// for RemoveDaemon or fixture Terminate, including partial startup failures.
func (m *RBDMirror) AddDaemon(ctx context.Context, name string, opts ...testcontainers.ContainerCustomizer) (*RBDMirrorDaemon, error) {
	if m == nil {
		return nil, errors.New("RBD mirror fixture is unavailable")
	}
	if err := validateRBDMirrorDaemonName(name); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.sourceClient == nil || m.destinationClient == nil || m.config.Destination == nil {
		return nil, errors.New("RBD mirror fixture is unavailable or terminated")
	}
	for _, daemon := range m.daemons {
		if daemon.DaemonName == name {
			return nil, fmt.Errorf("RBD mirror daemon %q already exists", name)
		}
	}
	if err := m.checkRBDMirrorPolicyIdentities(ctx); err != nil {
		return nil, err
	}
	clientName := "client.rbd-mirror.tc-" + uuid.NewString()
	keyring, err := m.config.Destination.Ceph(ctx, "auth", "get-or-create", clientName,
		"mon", "profile rbd-mirror", "osd", "profile rbd")
	if err != nil {
		return nil, fmt.Errorf("create RBD mirror daemon credentials: %w", err)
	}
	daemonOpts := []testcontainers.ContainerCustomizer{
		m.config.Destination.WithClient(),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader: bytes.NewReader(keyring), ContainerFilePath: "/etc/ceph/rbd-mirror.keyring", FileMode: 0o600,
		}),
		testcontainers.WithEntrypoint("rbd-mirror"),
		testcontainers.WithCmd("-f", "--name", clientName, "--keyring", "/etc/ceph/rbd-mirror.keyring",
			"--admin-socket", "/tmp/rbd-mirror.asok", "--log-to-stderr=true", "--log-to-file=false"),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"test", "-S", "/tmp/rbd-mirror.asok"}).WithStartupTimeout(time.Minute)),
	}
	if !m.config.Source.UsesHostNetwork() {
		daemonOpts = append(daemonOpts, network.WithNetworkName(nil, m.config.Source.NetworkName()))
	}
	daemonOpts = append(daemonOpts, m.daemonOpts...)
	daemonOpts = append(daemonOpts, opts...)
	ctr, err := testcontainers.Run(ctx, m.image, daemonOpts...)
	var daemon *RBDMirrorDaemon
	if ctr != nil {
		daemon = &RBDMirrorDaemon{Container: ctr, DaemonName: name, ClientName: clientName}
		m.daemons = append(m.daemons, daemon)
		m.owned.addContainer(daemon)
		if m.initialDaemonName == "" {
			m.initialDaemonName = name
			m.Container = daemon
		}
	}
	if err != nil {
		return daemon, fmt.Errorf("run destination RBD mirror daemon: %w", err)
	}
	return daemon, nil
}

// RemoveDaemon terminates a daemon owned by this fixture. Removing the last
// process deliberately pauses replication while retaining peer and image state;
// AddDaemon can resume it. Failed cleanup retains membership for a retry. The
// legacy embedded handle is cleared when the initial daemon is removed.
func (m *RBDMirror) RemoveDaemon(ctx context.Context, name string, opts ...testcontainers.TerminateOption) error {
	if m == nil {
		return errors.New("RBD mirror fixture is unavailable")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("RBD mirror fixture is terminated")
	}
	for i, daemon := range m.daemons {
		if daemon.DaemonName != name {
			continue
		}
		if err := daemon.Terminate(ctx, opts...); err != nil {
			return fmt.Errorf("remove RBD mirror daemon %s: %w", name, err)
		}
		m.daemons = slices.Delete(m.daemons, i, i+1)
		if name == m.initialDaemonName {
			m.Container = nil
		}
		return nil
	}
	return fmt.Errorf("RBD mirror daemon %q is not owned by this fixture", name)
}

func validateRBDMirrorDaemonName(name string) error {
	if name == "" || len(name) > 63 || name[0] == '-' {
		return errors.New("RBD mirror daemon name must contain 1 to 63 ASCII letters, digits, underscores or hyphens, without a leading hyphen")
	}
	for _, char := range name {
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '_' && char != '-' {
			return errors.New("RBD mirror daemon name must contain only ASCII letters, digits, underscores or hyphens")
		}
	}
	return nil
}

// Rebootstrap creates or refreshes this link's destination receiving peer using
// the existing setup clients. It is useful after an explicit peer removal, and
// fills credentials and upgrades a matching tx-only peer to rx-tx when creating
// a reverse link. Native bootstrap import refreshes its monitor/key attributes
// but does not populate an existing peer's client name or receiving direction.
// The caller owns the policy for quiescing writes and resynchronizing images.
func (m *RBDMirror) Rebootstrap(ctx context.Context) (returnErr error) {
	if m == nil {
		return errors.New("RBD mirror setup clients are unavailable")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.sourceClient == nil || m.destinationClient == nil {
		return errors.New("RBD mirror setup clients are unavailable")
	}
	config := m.config
	if err := m.checkRBDMirrorPolicyIdentities(ctx); err != nil {
		return err
	}
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
	if err := m.checkRBDMirrorPolicyIdentities(ctx); err != nil {
		return err
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
		if err := m.checkRBDMirrorPolicyIdentities(ctx); err != nil {
			return err
		}
		if _, err := m.DestinationRBD(ctx, "mirror", "pool", "peer", "set", config.Pool, peer.UUID, "client", bootstrap.ClientName); err != nil {
			return fmt.Errorf("configure imported RBD mirror peer client: %w", err)
		}
	}
	if peer.Direction == "tx-only" {
		if err := m.checkRBDMirrorPolicyIdentities(ctx); err != nil {
			return err
		}
		if _, err := m.DestinationRBD(ctx, "mirror", "pool", "peer", "set", config.Pool, peer.UUID, "direction", "rx-tx"); err != nil {
			return fmt.Errorf("enable receiving on imported RBD mirror peer: %w", err)
		}
	}
	return m.checkRBDMirrorPolicyIdentities(ctx)
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

// EnableImage enables the configured snapshot or journal mode on an existing
// source image in this link's Pool and SourceNamespace. imageName is a name, without pool, namespace
// or snapshot syntax. A matching enabled image is left unchanged; a different
// enabled mode is rejected before any mutation.
//
// Journal mode requires the exclusive-lock image feature. The native enable
// command adds journaling when necessary; this method does not change the
// image's other features or broaden the pool's mirroring scope. Callers must
// quiesce their application before changing an existing image's mirroring.
func (m *RBDMirror) EnableImage(ctx context.Context, imageName string) error {
	if m == nil {
		return errors.New("RBD mirror source setup client is unavailable")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.sourceClient == nil {
		return errors.New("RBD mirror source setup client is unavailable or terminated")
	}
	if imageName == "" || strings.TrimSpace(imageName) != imageName || strings.HasPrefix(imageName, "-") || strings.ContainsAny(imageName, "/@\x00\t\r\n") {
		return errors.New("RBD image must be a name without pool, namespace, snapshot or option syntax")
	}
	if m.config.Scope == RBDMirrorScopePool {
		return errors.New("pool-scope mirroring enrolls journal-enabled images automatically; EnableImage is only for image scope")
	}
	if err := m.checkRBDMirrorPolicyIdentities(ctx); err != nil {
		return err
	}
	mode, err := normalizeRBDMirrorMode(m.config.Mode)
	if err != nil {
		return err
	}
	image := rbdMirrorImageSpec(m.config.Pool, m.config.SourceNamespace, imageName)
	data, err := m.SourceRBD(ctx, "info", image, "--format", "json")
	if err != nil {
		return fmt.Errorf("read source RBD image: %w", err)
	}
	var info struct {
		Features  []string `json:"features"`
		Mirroring *struct {
			Mode  string `json:"mode"`
			State string `json:"state"`
		} `json:"mirroring"`
	}
	if err := json.Unmarshal(data, &info); err != nil {
		return fmt.Errorf("decode source RBD image: %w", err)
	}
	if info.Mirroring != nil && info.Mirroring.State != "disabled" && info.Mirroring.State != "" {
		if info.Mirroring.State != "enabled" {
			return fmt.Errorf("source image mirroring is %q, expected a stable enabled or disabled state", info.Mirroring.State)
		}
		if info.Mirroring.Mode != string(mode) {
			return fmt.Errorf("source image already uses %s mirroring; changing to %s requires explicit disable/reconfiguration", info.Mirroring.Mode, mode)
		}
		return m.checkRBDMirrorPolicyIdentities(ctx)
	}
	if mode == RBDMirrorModeJournal && !slices.Contains(info.Features, "exclusive-lock") {
		return errors.New("journal mirroring requires the source image's exclusive-lock feature; enable it explicitly before mirroring")
	}
	if err := m.checkRBDMirrorPolicyIdentities(ctx); err != nil {
		return err
	}
	if _, err := m.SourceRBD(ctx, "mirror", "image", "enable", image, string(mode)); err != nil {
		return fmt.Errorf("enable source %s RBD mirroring: %w", mode, err)
	}
	return m.checkRBDMirrorPolicyIdentities(ctx)
}

// Terminate removes the daemon and setup clients while preserving both clusters,
// their image data, and the mirroring/auth configuration written during setup.
func (m *RBDMirror) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	if err := m.owned.terminate(ctx, opts...); err != nil {
		return err
	}
	m.daemons = nil
	m.Container = nil
	return nil
}

func normalizeRBDMirrorConfig(config RBDMirrorConfig) (RBDMirrorConfig, error) {
	if config.DaemonCount < 0 {
		return config, errors.New("RBD mirror daemon count must not be negative")
	}
	if config.DaemonCount == 0 {
		config.DaemonCount = 1
	}
	config.Pool = strings.TrimSpace(config.Pool)
	config.SourceSite = strings.TrimSpace(config.SourceSite)
	config.DestinationSite = strings.TrimSpace(config.DestinationSite)
	config, err := normalizeRBDMirrorScopeAndNamespaces(config)
	if err != nil {
		return config, err
	}
	mode, err := normalizeRBDMirrorMode(config.Mode)
	if err != nil {
		return config, err
	}
	config.Mode = mode
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

func normalizeRBDMirrorMode(mode RBDMirrorMode) (RBDMirrorMode, error) {
	if mode == "" {
		return RBDMirrorModeSnapshot, nil
	}
	switch mode {
	case RBDMirrorModeSnapshot, RBDMirrorModeJournal:
		return mode, nil
	default:
		return "", fmt.Errorf("unsupported RBD mirror mode %q; use snapshot or journal", mode)
	}
}
