package mirror

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Mode selects the per-image replication mechanism, independently of
// the pool/namespace scope. Image scope enables images explicitly; pool scope
// automatically enrolls images carrying the journaling feature.
type Mode string

const (
	ModeSnapshot Mode = "snapshot"
	ModeJournal  Mode = "journal"
)

// Option carries the Config of a mirror to Run. It is not a container
// customization: its Customize does nothing, so it never reaches a daemon.
type Option struct{ config Config }

// Customize implements testcontainers.ContainerCustomizer.
func (Option) Customize(*testcontainers.GenericContainerRequest) error { return nil }

// WithConfig is the grouped option function that carries config to Run. Run
// takes exactly one.
func WithConfig(config Config) Option { return Option{config: config} }

// Config connects two existing clusters through destination-side
// rbd-mirror daemons. Both clusters must already contain an initialized RBD Pool.
// SourceSite and DestinationSite default to "source" and "destination".
type Config struct {
	Source          *ceph.Container
	Destination     *ceph.Container
	Pool            string
	SourceSite      string
	DestinationSite string
	// Scope defaults to image. Pool scope automatically mirrors every journaled
	// image in the selected namespace, including existing images.
	Scope Scope
	// Each empty namespace means the unnamed default namespace. Both named
	// namespaces must already exist; they are never created or removed here.
	// Different names, including named/default pairs, are mapped explicitly.
	SourceNamespace, DestinationNamespace string
	// Mode defaults to snapshot for image scope and journal for pool scope.
	// Pool scope supports journal only; it never creates mirror checkpoints.
	Mode Mode
	// DaemonCount defaults to one. Multiple daemons share the pool's receiving
	// peer and participate in Ceph's native leader election and image assignment.
	DaemonCount int
	// NoInitialDaemons configures policies and the receiving peer without
	// starting a mirror daemon. DaemonCount must be zero. AddDaemon starts
	// processes explicitly later; DaemonCount=0 otherwise still defaults to one.
	NoInitialDaemons bool
}

// Mirror owns its daemons and setup CLI containers, not the Ceph clusters.
// The embedded Container is the initial daemon's compatibility handle. Its
// Stop and Start affect only that daemon. Removing it clears the handle without
// assigning another daemon. Use Daemons for current membership. Terminate
// removes all owned runtime containers, including partially created or
// previously removed daemons.
type Mirror struct {
	testcontainers.Container
	// pair is the mirrored source/destination pair, its setup clients and the
	// native pool and policy identities captured for it.
	pair rbdMirrorPair
	// receiver is the destination's receiving peer identity.
	receiver rbdReceiverPeer
	// roster is the daemon membership and how a new daemon is launched.
	roster rbdDaemonRoster
	owned  multicluster.Resources
	mu     sync.Mutex
	closed bool
}

// rbdMirrorPair is the source/destination pair a fixture mirrors. The config
// and setup clients are fixed once setup has started them; the identities are
// captured during setup, and later operations refuse a pair whose native pools
// or policies no longer match them. Its methods run while the caller holds
// Mirror.mu, or during setup before the fixture is shared.
type rbdMirrorPair struct {
	config                          Config
	sourceClient, destinationClient testcontainers.Container
	poolIdentities                  *rbdMirrorPoolIdentities
	policyIdentities                *rbdMirrorPolicyIdentities
	// legacyCLI marks the setup clients whose rbd has no --remote-namespace,
	// and legacyClusters the clusters whose client toolkit's rbd has none.
	legacyCLI      map[testcontainers.Container]bool
	legacyClusters map[*ceph.Container]bool
}

// rbdReceiverPeer is the destination's receiving peer as setup recorded it:
// both clusters' identities, the peer and its generation, which Rebootstrap
// advances, and whether setup completed. It is guarded by Mirror.mu.
type rbdReceiverPeer struct {
	clusters       *rbdReceiverClusterIdentities
	peer           *rbdReceiverPeerIdentity
	generation     uint64
	setupConfirmed bool
}

// rbdDaemonRoster is the fixture's daemon membership and the launch settings
// every new daemon shares. The first daemon added is the initial one, which
// also backs the fixture's embedded container. It is guarded by Mirror.mu.
type rbdDaemonRoster struct {
	image       string
	opts        []testcontainers.ContainerCustomizer
	daemons     []*Daemon
	initialName string
}

// Daemon is one destination-side rbd-mirror process. Every daemon has
// a distinct Ceph user; receiving peer credentials remain in the destination's
// config-key store. Stop and Start retain its identity and native pool state.
type Daemon struct {
	testcontainers.Container
	DaemonName                                                  string
	ClientName                                                  string
	owner                                                       *Mirror
	mu                                                          sync.Mutex
	terminated                                                  bool
	receiverHandle                                              testcontainers.Container
	receiverContainerID, receiverClientName, receiverDaemonName string
	receiverStartupConfirmed                                    bool
}

// DaemonStatus is the native daemon admin socket's pool membership and
// election state. A running container can report no pool replayers until Ceph
// completes discovery; callers must inspect the intended pool and peer.
type DaemonStatus struct {
	PoolReplayers []PoolReplayerStatus `json:"pool_replayers"`
}

// PoolReplayerStatus describes one pool/peer replayer. Image replay is
// distributed among instances; being a pool leader does not mean replaying all
// images locally.
type PoolReplayerStatus struct {
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
func (d *Daemon) Status(ctx context.Context) (*DaemonStatus, error) {
	if d == nil || d.Container == nil {
		return nil, errors.New("RBD mirror daemon is unavailable")
	}
	data, err := multicluster.Exec(ctx, d.Container, "ceph", "--admin-daemon", "/tmp/rbd-mirror.asok", "rbd", "mirror", "status")
	if err != nil {
		return nil, fmt.Errorf("read RBD mirror daemon %s status: %w", d.DaemonName, err)
	}
	var status DaemonStatus
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
func (d *Daemon) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	if d == nil {
		return nil
	}
	return multicluster.TerminateOnce(ctx, &d.mu, &d.terminated, d.Container, opts...)
}

// Run enables the selected scope on both pool/namespaces, imports a receiving
// peer into the destination, and starts the configured initial daemons connected
// to both clusters. NoInitialDaemons retains setup without launching a daemon.
// A new peer is rx-only; an existing tx-only peer becomes rx-tx so creating a
// reverse link preserves the transmission already used by the first link.
// img is the mirror daemon image and is required: it must meet the rbd-aux (or
// all) role requirements, including rbd-mirror, the rbd and ceph CLIs and the
// common utilities, and is refused when it is known to be another role of a
// cluster (for example its essential image). The destination's Images().RBDAux
// is the usual choice, because the daemons run for the destination. Setup
// clients are not started from it: each cluster's Images().Client supplies
// them. The Config comes from exactly one WithConfig among opts. Enable each intended source
// image through EnableImage or the RBD CLI in image scope. Pool scope
// automatically enrolls journal-enabled images. Snapshot images need subsequent
// mirror checkpoints; journal images replay writes without mirror snapshots.
//
// The caller must Terminate a non-nil result even when setup returns an error.
// Terminate removes runtime containers but leaves mirroring mode, peer and auth
// configuration in the disposable clusters; it never destroys pools or data.
// The other customizers apply to the daemon only and must preserve its
// networking, Ceph configuration, credentials and entrypoint.
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
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Mirror, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("configure RBD mirror: %w", err)
	}
	carrier, opts, err := multicluster.SplitConfig[Option]("RBD mirror", "WithConfig", opts)
	if err != nil {
		return nil, err
	}
	config, image := carrier.config, img
	if _, err := multicluster.NormalizeInitialMirrorDaemonCount(config.DaemonCount, config.NoInitialDaemons); err != nil {
		return nil, servicehook.Invalid(fmt.Errorf("configure RBD mirror: RBD %w", err))
	}
	if err := multicluster.ValidatePair(ctx, image, config.Source, config.Destination); err != nil {
		return nil, fmt.Errorf("configure RBD mirror: %w", err)
	}
	if err := multicluster.RequireRoleImage(image, multicluster.RoleRBDAux, config.Source, config.Destination); err != nil {
		return nil, fmt.Errorf("configure RBD mirror daemon image: %w", err)
	}
	config, err = normalizeRBDMirrorConfig(config)
	if err != nil {
		return nil, servicehook.Invalid(fmt.Errorf("configure RBD mirror: %w", err))
	}
	sourceFacts, err := ceph.FactsOf(config.Source)
	if err != nil {
		return nil, err
	}
	destinationFacts, err := ceph.FactsOf(config.Destination)
	if err != nil {
		return nil, err
	}
	sourceImage, destinationImage := sourceFacts.Images.Client, destinationFacts.Images.Client
	if sourceImage == "" || destinationImage == "" {
		return nil, servicehook.Invalid(fmt.Errorf("RBD mirror setup requires both client images"))
	}
	receiverClusters, err := captureRBDReceiverClusters(ctx, config)
	if err != nil {
		return nil, err
	}
	identities, err := preflightRBDMirrorPools(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("preflight RBD mirror pools/namespaces: %w", err)
	}
	mirror := &Mirror{pair: rbdMirrorPair{config: config, poolIdentities: identities}, roster: rbdDaemonRoster{image: image, opts: slices.Clone(opts)}, receiver: rbdReceiverPeer{clusters: receiverClusters}}
	sourceClient, err := multicluster.RunClient(ctx, sourceImage, config.Source, destinationFacts.NetworkName, &mirror.owned)
	if err != nil {
		return mirror, fmt.Errorf("run source RBD setup client: %w", err)
	}
	mirror.pair.sourceClient = sourceClient
	destinationClient, err := multicluster.RunClient(ctx, destinationImage, config.Destination, sourceFacts.NetworkName, &mirror.owned)
	if err != nil {
		return mirror, fmt.Errorf("run destination RBD setup client: %w", err)
	}
	mirror.pair.destinationClient = destinationClient
	if err := mirror.pair.detectNamespaceMapping(ctx); err != nil {
		return mirror, err
	}
	if err := mirror.pair.provisionPolicies(ctx); err != nil {
		return mirror, err
	}
	if err := mirror.Rebootstrap(ctx); err != nil {
		return mirror, err
	}

	for index := range config.DaemonCount {
		if _, err := mirror.AddDaemon(ctx, multicluster.InitialMirrorDaemonName(index)); err != nil {
			return mirror, err
		}
	}
	if err := ctx.Err(); err != nil {
		return mirror, fmt.Errorf("confirm RBD mirror construction: %w", err)
	}
	mirror.receiver.setupConfirmed = true
	return mirror, nil
}

// Daemons returns a membership snapshot of this fixture's current daemons,
// including stopped containers. It does not discover daemons owned by another
// fixture, even if they replay the same pool.
func (m *Mirror) Daemons() []*Daemon {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.roster.sorted()
}

// AddDaemon adds another process using the existing receiving peer, without
// rebootstrap or image mutation. Construction customizers apply to every daemon;
// opts are appended for this daemon only. A non-nil result on error is tracked
// for Daemon.Remove or fixture Terminate, including partial startup failures.
func (m *Mirror) AddDaemon(ctx context.Context, name string, opts ...testcontainers.ContainerCustomizer) (*Daemon, error) {
	if m == nil {
		return nil, errors.New("RBD mirror fixture is unavailable")
	}
	if err := validateRBDMirrorDaemonName(name); err != nil {
		return nil, err
	}
	if err := multicluster.LockObservation(ctx, &m.mu); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	if m.closed || m.pair.sourceClient == nil || m.pair.destinationClient == nil || m.pair.config.Destination == nil {
		return nil, servicehook.Invalid(errors.New("RBD mirror fixture is unavailable or terminated"))
	}
	if m.roster.named(name) {
		return nil, servicehook.Invalid(fmt.Errorf("RBD mirror daemon %q already exists", name))
	}
	if err := m.pair.checkPolicyIdentities(ctx); err != nil {
		return nil, err
	}
	clientName := "client.rbd-mirror.tc-" + uuid.NewString()
	keyring, err := m.pair.config.Destination.Ceph(ctx, "auth", "get-or-create", clientName,
		"mon", "profile rbd-mirror", "osd", "profile rbd")
	if err != nil {
		return nil, fmt.Errorf("create RBD mirror daemon credentials: %w", err)
	}
	sourceFacts, err := ceph.FactsOf(m.pair.config.Source)
	if err != nil {
		return nil, err
	}
	daemonOpts := []testcontainers.ContainerCustomizer{
		m.pair.config.Destination.WithClient(),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader: bytes.NewReader(keyring), ContainerFilePath: "/etc/ceph/rbd-mirror.keyring", FileMode: 0o600,
		}),
		testcontainers.WithEntrypoint("rbd-mirror"),
		testcontainers.WithCmd("-f", "--name", clientName, "--keyring", "/etc/ceph/rbd-mirror.keyring",
			"--admin-socket", "/tmp/rbd-mirror.asok", "--log-to-stderr=true", "--log-to-file=false"),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"test", "-S", "/tmp/rbd-mirror.asok"}).WithStartupTimeout(time.Minute)),
	}
	if !sourceFacts.UsesHostNetwork {
		daemonOpts = append(daemonOpts, network.WithNetworkName(nil, sourceFacts.NetworkName))
	}
	ctr, err := m.roster.launch(ctx, daemonOpts, opts)
	var daemon *Daemon
	if ctr != nil {
		daemon = &Daemon{Container: ctr, DaemonName: name, ClientName: clientName, owner: m, receiverHandle: ctr, receiverContainerID: ctr.GetContainerID(), receiverClientName: clientName, receiverDaemonName: name, receiverStartupConfirmed: err == nil}
		m.owned.AddContainer(daemon)
		if m.roster.add(daemon) {
			m.Container = daemon
		}
	}
	if err != nil {
		return daemon, fmt.Errorf("run destination RBD mirror daemon: %w", err)
	}
	return daemon, nil
}

// sorted returns the members ordered by daemon name.
func (r *rbdDaemonRoster) sorted() []*Daemon {
	daemons := slices.Clone(r.daemons)
	slices.SortFunc(daemons, func(a, b *Daemon) int { return strings.Compare(a.DaemonName, b.DaemonName) })
	return daemons
}

// named reports whether a member already uses name.
func (r *rbdDaemonRoster) named(name string) bool {
	return slices.ContainsFunc(r.daemons, func(d *Daemon) bool { return d.DaemonName == name })
}

// has reports whether d is a member, by identity.
func (r *rbdDaemonRoster) has(d *Daemon) bool { return slices.Contains(r.daemons, d) }

// launch starts a daemon container from the shared image with the
// fixture-wide customizers after base and before this daemon's own opts.
func (r *rbdDaemonRoster) launch(ctx context.Context, base, opts []testcontainers.ContainerCustomizer) (*testcontainers.DockerContainer, error) {
	all := append(slices.Clone(base), r.opts...)
	return testcontainers.Run(ctx, r.image, append(all, opts...)...)
}

// add records d as a member and reports whether it is the initial daemon,
// the first one ever added.
func (r *rbdDaemonRoster) add(d *Daemon) bool {
	r.daemons = append(r.daemons, d)
	if r.initialName != "" {
		return false
	}
	r.initialName = d.DaemonName
	return true
}

// remove drops member d and reports whether it was the initial daemon.
func (r *rbdDaemonRoster) remove(d *Daemon) bool {
	r.daemons = slices.DeleteFunc(r.daemons, func(m *Daemon) bool { return m == d })
	return d.DaemonName == r.initialName
}

// Remove terminates this daemon and removes it from its fixture's membership.
// Removing the last process deliberately pauses replication while retaining
// peer and image state; AddDaemon can resume it. Failed cleanup retains
// membership for a retry. The legacy embedded handle is cleared when the
// initial daemon is removed.
func (d *Daemon) Remove(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	if d == nil || d.owner == nil {
		return servicehook.Invalid(errors.New("RBD mirror daemon is not owned by a fixture"))
	}
	m := d.owner
	if err := multicluster.LockObservation(ctx, &m.mu); err != nil {
		return err
	}
	defer m.mu.Unlock()
	if m.closed {
		return servicehook.Invalid(errors.New("RBD mirror fixture is terminated"))
	}
	if !m.roster.has(d) {
		return servicehook.Invalid(fmt.Errorf("RBD mirror daemon %q is not owned by this fixture", d.DaemonName))
	}
	if err := d.Terminate(ctx, opts...); err != nil {
		return fmt.Errorf("remove RBD mirror daemon %s: %w", d.DaemonName, err)
	}
	if m.roster.remove(d) {
		m.Container = nil
	}
	return nil
}

func validateRBDMirrorDaemonName(name string) error {
	if name == "" || len(name) > 63 || name[0] == '-' {
		return servicehook.Invalid(errors.New("RBD mirror daemon name must contain 1 to 63 ASCII letters, digits, underscores or hyphens, without a leading hyphen"))
	}
	for _, char := range name {
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '_' && char != '-' {
			return servicehook.Invalid(errors.New("RBD mirror daemon name must contain only ASCII letters, digits, underscores or hyphens"))
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
func (m *Mirror) Rebootstrap(ctx context.Context) (returnErr error) {
	if m == nil {
		return errors.New("RBD mirror setup clients are unavailable")
	}
	if err := multicluster.LockObservation(ctx, &m.mu); err != nil {
		return err
	}
	defer m.mu.Unlock()
	if m.closed || m.pair.sourceClient == nil || m.pair.destinationClient == nil {
		return errors.New("RBD mirror setup clients are unavailable")
	}
	if err := m.pair.checkPolicyIdentities(ctx); err != nil {
		return err
	}
	var receiverCandidate *rbdReceiverPeerIdentity
	if m.receiver.clusters != nil {
		if err := m.receiver.checkClusterFSIDs(ctx, &m.pair); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		// Invalidate only after owner/preflight/context admission, immediately
		// before the first owned native bootstrap mutation attempt. A reply lost
		// later must not leave a previous readiness capability usable.
		m.receiver.peer = nil
		m.receiver.generation++
		defer func() {
			// Registered before token cleanup: LIFO cleanup completes first.
			ctxErr := ctx.Err()
			if returnErr == nil && ctxErr != nil {
				returnErr = rbdReceiverQuery(ctx, "confirm RBD receiving bootstrap", ctxErr)
			}
			if returnErr == nil && receiverCandidate != nil && ctxErr == nil {
				receiverCandidate.generation = m.receiver.generation
				m.receiver.peer = receiverCandidate
			}
		}()
	}
	token, bootstrap, err := m.pair.createBootstrapToken(ctx)
	if err != nil {
		return err
	}
	tokenPath := "/tmp/rbd-mirror-peer-token-" + uuid.NewString()
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := multicluster.Exec(cleanupCtx, m.pair.destinationClient, "rm", "-f", tokenPath); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("remove RBD mirror peer token file: %w", err))
		}
	}()
	if err := m.pair.importBootstrapToken(ctx, token, tokenPath); err != nil {
		return err
	}
	fsid, sourceMirrorUUID, err := m.pair.sourceIdentity(ctx, bootstrap)
	if err != nil {
		return err
	}
	if err := m.pair.reconcileImportedPeer(ctx, bootstrap, fsid, sourceMirrorUUID); err != nil {
		return err
	}
	if m.receiver.clusters != nil {
		receiverCandidate, err = m.receiver.capturePeer(ctx, &m.pair, bootstrap)
		if err != nil {
			return err
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
func (m *Mirror) SourceRBD(ctx context.Context, args ...string) ([]byte, error) {
	if m == nil {
		return nil, errors.New("RBD mirror source setup client is unavailable")
	}
	return m.pair.sourceRBD(ctx, args...)
}

// DestinationRBD executes the RBD CLI in the destination setup client. It can
// promote/demote images, request resync, and inspect or remove explicit peers.
func (m *Mirror) DestinationRBD(ctx context.Context, args ...string) ([]byte, error) {
	if m == nil {
		return nil, errors.New("RBD mirror destination setup client is unavailable")
	}
	return m.pair.destinationRBD(ctx, args...)
}

func (p *rbdMirrorPair) sourceRBD(ctx context.Context, args ...string) ([]byte, error) {
	if p.sourceClient == nil {
		return nil, errors.New("RBD mirror source setup client is unavailable")
	}
	return multicluster.Exec(ctx, p.sourceClient, append([]string{"rbd"}, args...)...)
}

func (p *rbdMirrorPair) destinationRBD(ctx context.Context, args ...string) ([]byte, error) {
	if p.destinationClient == nil {
		return nil, errors.New("RBD mirror destination setup client is unavailable")
	}
	return multicluster.Exec(ctx, p.destinationClient, append([]string{"rbd"}, args...)...)
}

// createBootstrapToken creates a source peer bootstrap token and reads the
// identity it carries.
func (p *rbdMirrorPair) createBootstrapToken(ctx context.Context) ([]byte, rbdBootstrapIdentity, error) {
	token, err := p.sourceRBD(ctx, "mirror", "pool", "peer", "bootstrap", "create", "--site-name", p.config.SourceSite, p.config.Pool)
	if err != nil {
		return nil, rbdBootstrapIdentity{}, fmt.Errorf("create RBD mirror peer token: %w", err)
	}
	if len(bytes.TrimSpace(token)) == 0 {
		return nil, rbdBootstrapIdentity{}, errors.New("source returned an empty RBD mirror peer token")
	}
	bootstrap, err := decodeRBDBootstrapIdentity(token)
	if err != nil {
		return nil, rbdBootstrapIdentity{}, fmt.Errorf("read source RBD mirror bootstrap identity: %w", err)
	}
	return token, bootstrap, nil
}

// importBootstrapToken copies the token to tokenPath on the destination and
// imports it as an rx-only peer. The caller removes the file.
func (p *rbdMirrorPair) importBootstrapToken(ctx context.Context, token []byte, tokenPath string) error {
	if err := p.destinationClient.CopyToContainer(ctx, token, tokenPath, 0o600); err != nil {
		return fmt.Errorf("copy RBD mirror peer token: %w", err)
	}
	if err := p.checkPolicyIdentities(ctx); err != nil {
		return err
	}
	if _, err := p.destinationRBD(ctx, "mirror", "pool", "peer", "bootstrap", "import",
		"--site-name", p.config.DestinationSite, "--direction", "rx-only", p.config.Pool, tokenPath); err != nil {
		return fmt.Errorf("import rx-only RBD mirror peer: %w", err)
	}
	return nil
}

// sourceIdentity reads the source cluster FSID, which must be the token's, and
// the source pool's mirror UUID.
func (p *rbdMirrorPair) sourceIdentity(ctx context.Context, bootstrap rbdBootstrapIdentity) (fsid, mirrorUUID string, err error) {
	raw, err := multicluster.Exec(ctx, p.sourceClient, "ceph", "fsid")
	if err != nil {
		return "", "", fmt.Errorf("read source cluster identity: %w", err)
	}
	fsid = strings.TrimSpace(string(raw))
	if bootstrap.FSID != fsid {
		return "", "", errors.New("source RBD bootstrap token has a conflicting cluster identity")
	}
	sourceInfo, err := p.sourceRBD(ctx, "mirror", "pool", "info", p.config.Pool, "--format", "json")
	if err != nil {
		return "", "", fmt.Errorf("read source RBD mirror identity: %w", err)
	}
	var sourcePool struct {
		MirrorUUID string `json:"mirror_uuid"`
	}
	err = json.Unmarshal(sourceInfo, &sourcePool)
	// An rbd without remote namespaces, as in Ceph 19, does not print the
	// UUID, whatever release the cluster runs.
	if err == nil && sourcePool.MirrorUUID == "" && p.legacyClient(p.sourceClient) {
		sourcePool.MirrorUUID, err = readLegacyRBDMirrorUUID(ctx, p.sourceClient, p.config.Pool, "")
	}
	if err != nil || sourcePool.MirrorUUID == "" {
		return "", "", fmt.Errorf("read source RBD mirror identity: invalid pool info (decode error: %v)", err)
	}
	return fsid, sourcePool.MirrorUUID, nil
}

// reconcileImportedPeer completes the imported peer. Bootstrap import reuses
// an existing peer without updating its client or direction; it does refresh
// the monitor/key attributes from the token. The remote daemon can already
// have registered this site as tx-only, so the peer is matched by both its
// site identity and the source mirror UUID and only this link is updated.
func (p *rbdMirrorPair) reconcileImportedPeer(ctx context.Context, bootstrap rbdBootstrapIdentity, fsid, sourceMirrorUUID string) error {
	destinationInfo, err := p.destinationRBD(ctx, "mirror", "pool", "info", p.config.Pool, "--format", "json")
	if err != nil {
		return fmt.Errorf("read destination RBD mirror peer: %w", err)
	}
	peer, err := matchingRBDMirrorPeer(destinationInfo, p.config.SourceSite, fsid, sourceMirrorUUID)
	if err != nil {
		return fmt.Errorf("identify imported RBD mirror peer: %w", err)
	}
	if peer.ClientName != bootstrap.ClientName {
		if err := p.checkPolicyIdentities(ctx); err != nil {
			return err
		}
		if _, err := p.destinationRBD(ctx, "mirror", "pool", "peer", "set", p.config.Pool, peer.UUID, "client", bootstrap.ClientName); err != nil {
			return fmt.Errorf("configure imported RBD mirror peer client: %w", err)
		}
	}
	if peer.Direction == "tx-only" {
		if err := p.checkPolicyIdentities(ctx); err != nil {
			return err
		}
		if _, err := p.destinationRBD(ctx, "mirror", "pool", "peer", "set", p.config.Pool, peer.UUID, "direction", "rx-tx"); err != nil {
			return fmt.Errorf("enable receiving on imported RBD mirror peer: %w", err)
		}
	}
	return p.checkPolicyIdentities(ctx)
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
func (m *Mirror) EnableImage(ctx context.Context, imageName string) error {
	if m == nil {
		return servicehook.Invalid(errors.New("RBD mirror source setup client is unavailable"))
	}
	if err := multicluster.LockObservation(ctx, &m.mu); err != nil {
		return err
	}
	defer m.mu.Unlock()
	if m.closed || m.pair.sourceClient == nil {
		return servicehook.Invalid(errors.New("RBD mirror source setup client is unavailable or terminated"))
	}
	if imageName == "" || strings.TrimSpace(imageName) != imageName || strings.HasPrefix(imageName, "-") || strings.ContainsAny(imageName, "/@\x00\t\r\n") {
		return servicehook.Invalid(errors.New("RBD image must be a name without pool, namespace, snapshot or option syntax"))
	}
	if m.pair.config.Scope == ScopePool {
		return servicehook.Invalid(errors.New("pool-scope mirroring enrolls journal-enabled images automatically; EnableImage is only for image scope"))
	}
	if err := m.pair.checkPolicyIdentities(ctx); err != nil {
		return err
	}
	mode, err := normalizeRBDMirrorMode(m.pair.config.Mode)
	if err != nil {
		return err
	}
	image := rbdMirrorImageSpec(m.pair.config.Pool, m.pair.config.SourceNamespace, imageName)
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
			return servicehook.Invalid(fmt.Errorf("source image mirroring is %q, expected a stable enabled or disabled state", info.Mirroring.State))
		}
		if info.Mirroring.Mode != string(mode) {
			return servicehook.Invalid(fmt.Errorf("source image already uses %s mirroring; changing to %s requires explicit disable/reconfiguration", info.Mirroring.Mode, mode))
		}
		return m.pair.checkPolicyIdentities(ctx)
	}
	if mode == ModeJournal && !slices.Contains(info.Features, "exclusive-lock") {
		return servicehook.Invalid(errors.New("journal mirroring requires the source image's exclusive-lock feature; enable it explicitly before mirroring"))
	}
	if err := m.pair.checkPolicyIdentities(ctx); err != nil {
		return err
	}
	if _, err := m.SourceRBD(ctx, "mirror", "image", "enable", image, string(mode)); err != nil {
		return fmt.Errorf("enable source %s RBD mirroring: %w", mode, err)
	}
	return m.pair.checkPolicyIdentities(ctx)
}

// Terminate removes the daemon and setup clients while preserving both clusters,
// their image data, and the mirroring/auth configuration written during setup.
func (m *Mirror) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	if m == nil {
		return nil
	}
	return multicluster.TerminateFixture(ctx, &m.mu, &m.closed, &m.owned, func() {
		m.roster.daemons = nil
		m.Container = nil
	}, opts...)
}

func normalizeRBDMirrorConfig(config Config) (Config, error) {
	count, err := multicluster.NormalizeInitialMirrorDaemonCount(config.DaemonCount, config.NoInitialDaemons)
	if err != nil {
		return config, fmt.Errorf("RBD %w", err)
	}
	config.DaemonCount = count
	config.Pool = strings.TrimSpace(config.Pool)
	config.SourceSite = strings.TrimSpace(config.SourceSite)
	config.DestinationSite = strings.TrimSpace(config.DestinationSite)
	config, err = normalizeRBDMirrorScopeAndNamespaces(config)
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

func normalizeRBDMirrorMode(mode Mode) (Mode, error) {
	if mode == "" {
		return ModeSnapshot, nil
	}
	switch mode {
	case ModeSnapshot, ModeJournal:
		return mode, nil
	default:
		return "", servicehook.Invalid(fmt.Errorf("unsupported RBD mirror mode %q; use snapshot or journal", mode))
	}
}
