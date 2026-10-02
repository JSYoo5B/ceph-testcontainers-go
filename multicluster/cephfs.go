package multicluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// CephFSMirrorConfig connects existing CephFS filesystems. The caller creates
// both clusters and their MDS/filesystems before starting snapshot mirroring.
type CephFSMirrorConfig struct {
	Source, Destination                     *ceph.Container
	SourceFilesystem, DestinationFilesystem string
	DestinationSite                         string
	Directories                             []string
}

// CephFSMirror owns its daemon and any network attachment added to the source
// MGR. It embeds the daemon for Stop/Start failure injection. It does not own
// either Ceph cluster or the filesystems' data.
type CephFSMirror struct {
	testcontainers.Container
	SourceFilesystem, DestinationFilesystem     string
	SourceClientEntity, DestinationClientEntity string
	Directories                                 []string
	owned                                       resources
	mu                                          sync.Mutex
	source, destination                         *ceph.Container
	destinationSite, peerID                     string
	pendingPeerImport                           *cephFSPeerIdentity
}

// peer_list in Tentacle's mgr mirroring/fs/snapshot_mirror.py reports
// UUID -> {client_name, site_name, fs_name}. site_name is the remote cluster
// name; unlike the daemon's status JSON, this command does not use cluster_name.
type cephFSPeerIdentity struct {
	ClientName     string `json:"client_name"`
	SiteName       string `json:"site_name"`
	FilesystemName string `json:"fs_name"`
}

// RunCephFSMirror configures one-way directory snapshot mirroring between two
// existing filesystems and starts cephfs-mirror in a separate container. image
// must contain a compatible cephfs-mirror binary. MGRs must supply mirroring.
// A non-nil result returned with an error must be terminated for partial cleanup.
// Terminate leaves Ceph auth, peer and directory policies in the caller-owned
// disposable clusters; it does not undo configuration or delete mirrored data.
func RunCephFSMirror(ctx context.Context, image string, config CephFSMirrorConfig, opts ...testcontainers.ContainerCustomizer) (*CephFSMirror, error) {
	if err := validatePair(image, config.Source, config.Destination); err != nil {
		return nil, err
	}
	config, err := normalizeCephFSMirrorConfig(config)
	if err != nil {
		return nil, err
	}
	manager := config.Source.ManagerContainer()
	if manager == nil || manager.GetContainerID() == "" {
		return nil, errors.New("source Ceph manager is unavailable")
	}
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	sourceID, destinationID := "tc-cephfs-mirror-"+suffix, "tc-cephfs-peer-"+suffix
	mirror := &CephFSMirror{
		SourceFilesystem: config.SourceFilesystem, DestinationFilesystem: config.DestinationFilesystem,
		SourceClientEntity: "client." + sourceID, DestinationClientEntity: "client." + destinationID,
		Directories: append([]string(nil), config.Directories...),
		source:      config.Source, destination: config.Destination, destinationSite: config.DestinationSite,
	}
	for _, cluster := range []*ceph.Container{config.Source, config.Destination} {
		if _, err := cluster.Ceph(ctx, "mgr", "module", "enable", "mirroring"); err != nil {
			return mirror, fmt.Errorf("enable CephFS mirroring manager module: %w", err)
		}
	}
	// The manager mounts the peer filesystem during bootstrap import to verify
	// its identity and record ceph.mirror.info. The daemon's network attachment
	// alone cannot provide this control-plane connectivity.
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return mirror, fmt.Errorf("create Docker client for CephFS manager networking: %w", err)
	}
	ownedAttachment := false
	mirror.owned.addCleanup("disconnect CephFS manager and close Docker client", func(cleanupCtx context.Context) error {
		if ownedAttachment {
			if _, err := docker.NetworkDisconnect(cleanupCtx, config.Destination.NetworkName(), mobycl.NetworkDisconnectOptions{Container: manager.GetContainerID()}); ignoreMissing(err) != nil {
				// Keep the client available when disconnect must be retried.
				return err
			}
			ownedAttachment = false
		}
		return docker.Close()
	})
	inspection, err := docker.ContainerInspect(ctx, manager.GetContainerID(), mobycl.ContainerInspectOptions{})
	if err != nil {
		return mirror, fmt.Errorf("inspect source CephFS manager: %w", err)
	}
	if inspection.Container.NetworkSettings == nil {
		return mirror, errors.New("source CephFS manager has no Docker network settings")
	}
	if _, attached := inspection.Container.NetworkSettings.Networks[config.Destination.NetworkName()]; !attached {
		if _, err := docker.NetworkConnect(ctx, config.Destination.NetworkName(), mobycl.NetworkConnectOptions{Container: manager.GetContainerID()}); err != nil {
			return mirror, fmt.Errorf("connect source CephFS manager to destination network: %w", err)
		}
		ownedAttachment = true
	}
	keyring, err := config.Source.Ceph(ctx, "auth", "get-or-create", mirror.SourceClientEntity,
		"mon", "profile cephfs-mirror", "mds", "allow r",
		"osd", "allow rw tag cephfs metadata=*, allow r tag cephfs data=*", "mgr", "allow r")
	if err != nil {
		return mirror, fmt.Errorf("create source CephFS mirror credentials: %w", err)
	}
	if _, err := config.Destination.Ceph(ctx, "fs", "authorize", config.DestinationFilesystem, mirror.DestinationClientEntity, "/", "rwps"); err != nil {
		return mirror, fmt.Errorf("authorize destination CephFS mirror peer: %w", err)
	}
	if _, err := config.Source.Ceph(ctx, "fs", "snapshot", "mirror", "enable", config.SourceFilesystem); err != nil {
		return mirror, fmt.Errorf("enable CephFS source snapshot mirroring: %w", err)
	}
	if _, err := mirror.RebootstrapPeer(ctx); err != nil {
		return mirror, err
	}
	for _, directory := range config.Directories {
		if _, err := config.Source.Ceph(ctx, "fs", "snapshot", "mirror", "add", config.SourceFilesystem, directory); err != nil {
			return mirror, fmt.Errorf("configure CephFS mirror directory %q: %w", directory, err)
		}
	}
	const socket = "/run/ceph/cephfs-mirror.asok"
	moduleOpts := []testcontainers.ContainerCustomizer{
		config.Source.WithClient(), network.WithNetworkName(nil, config.Destination.NetworkName()),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader: bytes.NewReader(keyring), ContainerFilePath: "/etc/ceph/ceph.client." + sourceID + ".keyring", FileMode: 0o600,
		}),
		testcontainers.WithEntrypoint("cephfs-mirror"),
		testcontainers.WithCmd("--id", sourceID, "-f", "--admin-socket", socket, "--cephfs-mirror-directory-scan-interval", "1"),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"test", "-S", socket}).WithStartupTimeout(2 * time.Minute)),
	}
	moduleOpts = append(moduleOpts, opts...)
	daemon, err := testcontainers.Run(ctx, image, moduleOpts...)
	if daemon != nil {
		mirror.Container = daemon
		mirror.owned.addContainer(daemon)
	}
	if err != nil {
		return mirror, fmt.Errorf("run CephFS snapshot mirror daemon: %w", err)
	}
	return mirror, nil
}

// Terminate removes owned containers and the MGR network attachment. It leaves
// caller-owned clusters, auth entities, peer policies and snapshots intact.
// It may be retried after a cleanup error.
func (mirror *CephFSMirror) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	return mirror.owned.terminate(ctx, opts...)
}

// AddDirectory registers an absolute CephFS path for snapshot mirroring. Ceph
// rejects overlapping paths. The path refers to the filesystem root, not a
// container mount point. Registration does not create the directory or data.
func (mirror *CephFSMirror) AddDirectory(ctx context.Context, directory string) error {
	directory, err := normalizeMirrorDirectory(directory)
	if err != nil {
		return err
	}
	mirror.mu.Lock()
	defer mirror.mu.Unlock()
	if mirror.source == nil {
		return errors.New("CephFS mirror is not initialized")
	}
	if _, err := mirror.source.Ceph(ctx, "fs", "snapshot", "mirror", "add", mirror.SourceFilesystem, directory); err != nil {
		return fmt.Errorf("add CephFS mirror directory %q: %w", directory, err)
	}
	mirror.Directories = append(mirror.Directories, directory)
	return nil
}

// RemoveDirectory stops tracking a path, preserving its source and destination
// data and snapshots. Daemon updates are asynchronous; inspect mirror daemon
// status before assuming in-flight synchronization has stopped.
func (mirror *CephFSMirror) RemoveDirectory(ctx context.Context, directory string) error {
	directory, err := normalizeMirrorDirectory(directory)
	if err != nil {
		return err
	}
	mirror.mu.Lock()
	defer mirror.mu.Unlock()
	if mirror.source == nil {
		return errors.New("CephFS mirror is not initialized")
	}
	if _, err := mirror.source.Ceph(ctx, "fs", "snapshot", "mirror", "remove", mirror.SourceFilesystem, directory); err != nil {
		return fmt.Errorf("remove CephFS mirror directory %q: %w", directory, err)
	}
	mirror.Directories = slices.DeleteFunc(mirror.Directories, func(item string) bool { return item == directory })
	return nil
}

// PeerIDs lists configured source filesystem peers in UUID order. These are
// monitor policies, not a guarantee that daemon synchronization has stopped.
func (mirror *CephFSMirror) PeerIDs(ctx context.Context) ([]string, error) {
	mirror.mu.Lock()
	defer mirror.mu.Unlock()
	return mirror.peerIDs(ctx)
}

func (mirror *CephFSMirror) peerIDs(ctx context.Context) ([]string, error) {
	peers, err := mirror.peerRecords(ctx)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids, nil
}

func (mirror *CephFSMirror) peerRecords(ctx context.Context) (map[string]json.RawMessage, error) {
	if mirror.source == nil {
		return nil, errors.New("CephFS mirror is not initialized")
	}
	data, err := mirror.source.Ceph(ctx, "fs", "snapshot", "mirror", "peer_list", mirror.SourceFilesystem)
	if err != nil {
		return nil, fmt.Errorf("list CephFS mirror peers: %w", err)
	}
	var peers map[string]json.RawMessage
	if err := json.Unmarshal(data, &peers); err != nil {
		return nil, fmt.Errorf("decode CephFS mirror peers: %w", err)
	}
	if peers == nil {
		return nil, errors.New("CephFS mirror peer list is not a JSON object")
	}
	for id := range peers {
		if _, err := uuid.Parse(id); err != nil {
			return nil, fmt.Errorf("invalid CephFS mirror peer UUID %q: %w", id, err)
		}
	}
	return peers, nil
}

// RemovePeer unregisters only the peer this mirror bootstrapped. It preserves
// destination data, snapshots and credentials. Daemon changes are asynchronous;
// wait for the UUID to disappear from daemon status before re-adding the peer.
func (mirror *CephFSMirror) RemovePeer(ctx context.Context, id string) error {
	mirror.mu.Lock()
	defer mirror.mu.Unlock()
	if id == "" || id != mirror.peerID {
		return errors.New("CephFS mirror does not own this peer")
	}
	if _, err := mirror.source.Ceph(ctx, "fs", "snapshot", "mirror", "peer_remove", mirror.SourceFilesystem, id); err != nil {
		return fmt.Errorf("remove CephFS mirror peer: %w", err)
	}
	mirror.peerID = ""
	return nil
}

// RebootstrapPeer creates a new destination token and imports it into the same
// source filesystem, retaining previously mirrored snapshots. The destination
// and credentials are those configured during construction. Ceph currently
// supports one peer, so existing source policies are refused rather than taken
// over. After RemovePeer, callers must first verify daemon synchronization has
// stopped as described in the CephFS mirroring documentation.
// If import or its subsequent peer lookup fails, retry with a fresh context.
// A pending import is reconciled only against the exact destination identity
// this mirror attempted to bootstrap, using the server-assigned peer UUID.
func (mirror *CephFSMirror) RebootstrapPeer(ctx context.Context) (string, error) {
	mirror.mu.Lock()
	defer mirror.mu.Unlock()
	if mirror.destination == nil {
		return "", errors.New("CephFS mirror is not initialized")
	}
	peers, err := mirror.peerRecords(ctx)
	if err != nil {
		return "", err
	}
	if len(peers) != 0 {
		if mirror.pendingPeerImport == nil {
			return "", errors.New("remove the existing CephFS mirror peer before bootstrapping")
		}
		return mirror.reconcilePendingPeer(peers)
	}
	expected := mirror.pendingPeerImport
	if expected == nil {
		expected = &cephFSPeerIdentity{
			ClientName: mirror.DestinationClientEntity, SiteName: mirror.destinationSite, FilesystemName: mirror.DestinationFilesystem,
		}
	}
	bootstrap, err := mirror.destination.Ceph(ctx, "fs", "snapshot", "mirror", "peer_bootstrap", "create",
		expected.FilesystemName, expected.ClientName, expected.SiteName)
	if err != nil {
		return "", fmt.Errorf("create destination CephFS peer bootstrap token: %w", err)
	}
	var token struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(bootstrap, &token); err != nil || token.Token == "" {
		return "", errors.New("destination did not return a valid CephFS peer bootstrap token")
	}
	// An error from Exec cannot prove the manager did not apply the command.
	// Keep the attempted identity before import, including on import failure.
	mirror.pendingPeerImport = expected
	if _, err := mirror.source.Ceph(ctx, "fs", "snapshot", "mirror", "peer_bootstrap", "import", mirror.SourceFilesystem, token.Token); err != nil {
		return "", fmt.Errorf("import destination CephFS peer: %w", err)
	}
	peers, err = mirror.peerRecords(ctx)
	if err != nil {
		return "", err
	}
	return mirror.reconcilePendingPeer(peers)
}

// Called under mirror.mu. A failed match retains the pending identity so a
// later retry can recover, but never grants ownership of an unrelated peer.
func (mirror *CephFSMirror) reconcilePendingPeer(peers map[string]json.RawMessage) (string, error) {
	id, err := matchPendingCephFSPeer(peers, mirror.pendingPeerImport)
	if err != nil {
		return "", err
	}
	mirror.peerID = id
	mirror.pendingPeerImport = nil
	return id, nil
}

func matchPendingCephFSPeer(peers map[string]json.RawMessage, expected *cephFSPeerIdentity) (string, error) {
	if expected == nil || expected.ClientName == "" || expected.SiteName == "" || expected.FilesystemName == "" {
		return "", errors.New("no CephFS peer import is pending")
	}
	if len(peers) != 1 {
		return "", fmt.Errorf("pending CephFS bootstrap has %d peers, want one", len(peers))
	}
	for id, raw := range peers {
		if _, err := uuid.Parse(id); err != nil {
			return "", fmt.Errorf("invalid pending CephFS peer UUID %q: %w", id, err)
		}
		var observed cephFSPeerIdentity
		if err := json.Unmarshal(raw, &observed); err != nil {
			return "", fmt.Errorf("decode pending CephFS peer identity: %w", err)
		}
		if observed != *expected {
			return "", errors.New("existing CephFS peer does not match this pending bootstrap destination")
		}
		return id, nil
	}
	return "", errors.New("pending CephFS bootstrap peer disappeared")
}

func normalizeMirrorDirectory(directory string) (string, error) {
	if !path.IsAbs(directory) || strings.ContainsRune(directory, 0) {
		return "", fmt.Errorf("CephFS mirror directory %q must be an absolute path", directory)
	}
	return path.Clean(directory), nil
}

func normalizeCephFSMirrorConfig(config CephFSMirrorConfig) (CephFSMirrorConfig, error) {
	for _, name := range []string{config.SourceFilesystem, config.DestinationFilesystem} {
		if name == "" || strings.ContainsAny(name, "/\x00\r\n\t ") {
			return config, errors.New("source and destination CephFS names must be non-empty without slashes or whitespace")
		}
	}
	if config.DestinationSite == "" {
		config.DestinationSite = "destination"
	}
	if strings.ContainsAny(config.DestinationSite, "@/\x00\r\n\t ") {
		return config, errors.New("CephFS destination site cannot contain separators or whitespace")
	}
	if len(config.Directories) == 0 {
		return config, errors.New("at least one CephFS mirror directory is required")
	}
	directories := make([]string, 0, len(config.Directories))
	for _, directory := range config.Directories {
		var err error
		directory, err = normalizeMirrorDirectory(directory)
		if err != nil {
			return config, err
		}
		for _, previous := range directories {
			if directory == previous || directory == "/" || previous == "/" || strings.HasPrefix(directory, previous+"/") || strings.HasPrefix(previous, directory+"/") {
				return config, fmt.Errorf("CephFS mirror directories %q and %q overlap", previous, directory)
			}
		}
		directories = append(directories, directory)
	}
	config.Directories = directories
	return config, nil
}
