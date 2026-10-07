package multicluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
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
	// DaemonCount defaults to one. Multiple daemons let the native MGR module
	// distribute directories and reassign them after an instance disappears.
	DaemonCount int
}

// CephFSMirrorDaemon is one owned mirror process. DaemonName is a stable fixture
// identity; the native instance identity is its authenticated client GID.
// All daemons in a fixture share SourceClientEntity and the same peer policy.
type CephFSMirrorDaemon struct {
	testcontainers.Container
	DaemonName string
	mu         sync.Mutex
	removed    bool
}

// Terminate removes this process without modifying cluster policy. Successful
// cleanup is remembered so fixture cleanup does not run container hooks twice;
// a failed removal remains retryable. RemoveDaemon also removes its membership.
func (daemon *CephFSMirrorDaemon) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	if daemon == nil {
		return nil
	}
	if err := lockRGWSyncObservation(ctx, &daemon.mu); err != nil {
		return err
	}
	defer daemon.mu.Unlock()
	if daemon.removed || daemon.Container == nil {
		return nil
	}
	if err := ignoreMissing(daemon.Container.Terminate(ctx, opts...)); err != nil {
		return err
	}
	daemon.removed = true
	return nil
}

// CephFSMirror owns its daemons and network attachments added to source MGR
// candidates. Its embedded Container is the initial daemon, and becomes nil
// after that daemon is removed. Use Daemons for the current topology. It does not own
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
	filesystemID                                int
	metadataPool                                string
	metadataPoolID, destinationMetadataPoolID   int64
	destinationFilesystemID                     int
	pendingPeerImport                           *cephFSPeerIdentity
	peerRemoval                                 *CephFSMirrorPeerRemoval
	peerGeneration                              uint64
	managerNetworking                           *cephFSManagerNetworking
	closed                                      bool
	daemons                                     []*CephFSMirrorDaemon
	daemonImage, sourceID                       string
	daemonKeyring                               []byte
	daemonOptions                               []testcontainers.ContainerCustomizer
	initialDaemonAssigned                       bool
	ownedDirectories                            map[string]bool
	pendingDirectoryRelease                     map[string]bool
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
// existing filesystems and starts each cephfs-mirror in a separate container. image
// must meet the control or all role requirements, including cephfs-mirror and
// the ceph CLI used to query its local admin socket. MGRs must supply mirroring.
// A non-nil result returned with an error must be terminated for partial cleanup.
// Terminate leaves Ceph auth, peer and directory policies in the caller-owned
// disposable clusters; it does not undo configuration or delete mirrored data.
// Both clusters must use the same network mode. Host mode requires no extra
// daemon or manager attachment; bridge mode joins the peer cluster network.
// Initial daemon names are a through z, then node-27 and onward.
func RunCephFSMirror(ctx context.Context, image string, config CephFSMirrorConfig, opts ...testcontainers.ContainerCustomizer) (*CephFSMirror, error) {
	if err := validatePair(ctx, image, config.Source, config.Destination); err != nil {
		return nil, err
	}
	config, err := normalizeCephFSMirrorConfig(config)
	if err != nil {
		return nil, err
	}
	if _, err := cephFSManagerCandidates(ctx, config.Source); err != nil {
		return nil, err
	}
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	sourceID, destinationID := "tc-cephfs-mirror-"+suffix, "tc-cephfs-peer-"+suffix
	mirror := &CephFSMirror{
		SourceFilesystem: config.SourceFilesystem, DestinationFilesystem: config.DestinationFilesystem,
		SourceClientEntity: "client." + sourceID, DestinationClientEntity: "client." + destinationID,
		Directories: append([]string(nil), config.Directories...),
		source:      config.Source, destination: config.Destination, destinationSite: config.DestinationSite,
		daemonImage: image, sourceID: sourceID, daemonOptions: append([]testcontainers.ContainerCustomizer(nil), opts...),
		ownedDirectories: make(map[string]bool),
	}
	for _, cluster := range []*ceph.Container{config.Source, config.Destination} {
		if _, err := cluster.Ceph(ctx, "mgr", "module", "enable", "mirroring"); err != nil {
			return mirror, fmt.Errorf("enable CephFS mirroring manager module: %w", err)
		}
	}
	if err := mirror.AttachManagers(ctx); err != nil {
		return mirror, err
	}
	keyring, err := config.Source.Ceph(ctx, "auth", "get-or-create", mirror.SourceClientEntity,
		"mon", "profile cephfs-mirror", "mds", "allow r fsname="+config.SourceFilesystem,
		"osd", "allow rw tag cephfs metadata="+config.SourceFilesystem+", allow r tag cephfs data="+config.SourceFilesystem, "mgr", "allow r")
	if err != nil {
		return mirror, fmt.Errorf("create source CephFS mirror credentials: %w", err)
	}
	mirror.daemonKeyring = append([]byte(nil), keyring...)
	if _, err := config.Destination.Ceph(ctx, "fs", "authorize", config.DestinationFilesystem, mirror.DestinationClientEntity, "/", "rwps"); err != nil {
		return mirror, fmt.Errorf("authorize destination CephFS mirror peer: %w", err)
	}
	if _, err := config.Source.Ceph(ctx, "fs", "snapshot", "mirror", "enable", config.SourceFilesystem); err != nil {
		return mirror, fmt.Errorf("enable CephFS source snapshot mirroring: %w", err)
	}
	if err := mirror.loadFilesystemIdentity(ctx); err != nil {
		return mirror, err
	}
	if _, err := mirror.RebootstrapPeer(ctx); err != nil {
		return mirror, err
	}
	for i := 0; i < config.DaemonCount; i++ {
		name := "node-" + strconv.Itoa(i+1)
		if i < 26 {
			name = string(rune('a' + i))
		}
		if _, err := mirror.AddDaemon(ctx, name); err != nil {
			return mirror, err
		}
	}
	// Register the complete initial set before assigning directories. Tentacle's
	// native add-instance policy has a throttled shuffle path; starting processes
	// one by one with already-assigned directories needlessly enters that path.
	if err := mirror.waitForDaemonRegistrations(ctx, config.DaemonCount); err != nil {
		return mirror, err
	}
	for _, directory := range config.Directories {
		if _, err := config.Source.Ceph(ctx, "fs", "snapshot", "mirror", "add", config.SourceFilesystem, directory); err != nil {
			return mirror, fmt.Errorf("configure CephFS mirror directory %q: %w", directory, err)
		}
		mirror.ownedDirectories[directory] = true
	}
	return mirror, nil
}

func (mirror *CephFSMirror) waitForDaemonRegistrations(ctx context.Context, count int) error {
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var lastErr error
	for {
		serviceData, err := mirror.source.Ceph(waitCtx, "service", "dump", "--format", "json")
		var ownedIDs map[string]bool
		if err == nil {
			ownedIDs, err = cephFSMirrorOwnedInstanceIDs(serviceData, mirror.sourceID)
			if err == nil && len(ownedIDs) != count {
				err = fmt.Errorf("owned native registered daemons=%d want=%d", len(ownedIDs), count)
			}
		}
		var data []byte
		if err == nil {
			data, err = mirror.source.Ceph(waitCtx, "fs", "snapshot", "mirror", "daemon", "status")
		}
		if err == nil {
			var registered int
			registered, err = countCephFSMirrorRegistrations(data, mirror.SourceFilesystem, mirror.peerID, ownedIDs)
			if err == nil && registered == count {
				// Service reporting and directory policy discovery are independent
				// async paths. Verify every owned GID reached the policy before
				// adding any initial directory (zero assignments are expected).
				var watcherIDs map[string]bool
				watcherIDs, err = mirror.ownedFilesystemWatcherIDs(waitCtx)
				var distribution []byte
				if err == nil {
					distribution, err = mirror.source.Ceph(waitCtx, "fs", "snapshot", "mirror", "show", "distribution", mirror.SourceFilesystem)
				}
				if err == nil {
					err = cephFSMirrorPolicyKnowsInstances(distribution, watcherIDs)
				}
				if err == nil {
					return nil
				}
			}
			if err == nil {
				err = fmt.Errorf("native registered daemons=%d want=%d", registered, count)
			}
		}
		lastErr = err
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for initial CephFS mirror daemon registrations (last observation: %v): %w", lastErr, waitCtx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func cephFSMirrorOwnedInstanceIDs(data []byte, sourceID string) (map[string]bool, error) {
	var serviceMap struct {
		Services map[string]struct {
			Daemons map[string]json.RawMessage `json:"daemons"`
		} `json:"services"`
	}
	if err := json.Unmarshal(data, &serviceMap); err != nil {
		return nil, fmt.Errorf("decode native CephFS mirror service map: %w", err)
	}
	ids := make(map[string]bool)
	for id, raw := range serviceMap.Services["cephfs-mirror"].Daemons {
		gid, err := strconv.ParseUint(id, 10, 64)
		if err != nil || gid == 0 {
			continue
		}
		var daemon struct {
			Metadata map[string]string `json:"metadata"`
		}
		if err := json.Unmarshal(raw, &daemon); err != nil {
			return nil, fmt.Errorf("decode native CephFS mirror service %s: %w", id, err)
		}
		if daemon.Metadata["id"] == sourceID {
			ids[id] = true
		}
	}
	return ids, nil
}

func (mirror *CephFSMirror) loadFilesystemIdentity(ctx context.Context) error {
	data, err := mirror.source.Ceph(ctx, "fs", "get", mirror.SourceFilesystem, "--format", "json")
	if err != nil {
		return fmt.Errorf("inspect mirrored filesystem identity: %w", err)
	}
	var filesystem struct {
		ID     int `json:"id"`
		MDSMap struct {
			MetadataPool *int64 `json:"metadata_pool"`
		} `json:"mdsmap"`
	}
	if err := json.Unmarshal(data, &filesystem); err != nil || filesystem.ID <= 0 || filesystem.MDSMap.MetadataPool == nil {
		return errors.New("source did not return mirrored filesystem ID and metadata pool")
	}
	data, err = mirror.source.Ceph(ctx, "osd", "pool", "ls", "detail", "--format", "json")
	if err != nil {
		return fmt.Errorf("inspect mirrored filesystem metadata pool: %w", err)
	}
	var pools []struct {
		ID   int64  `json:"pool_id"`
		Name string `json:"pool_name"`
	}
	if err := json.Unmarshal(data, &pools); err != nil {
		return fmt.Errorf("decode mirrored filesystem metadata pools: %w", err)
	}
	for _, pool := range pools {
		if pool.ID == *filesystem.MDSMap.MetadataPool && pool.Name != "" {
			mirror.filesystemID, mirror.metadataPool = filesystem.ID, pool.Name
			mirror.metadataPoolID = *filesystem.MDSMap.MetadataPool
			if mirror.destination != nil {
				data, err := mirror.destination.Ceph(ctx, "fs", "get", mirror.DestinationFilesystem, "--format", "json")
				if err != nil {
					return fmt.Errorf("inspect destination mirrored filesystem identity: %w", err)
				}
				var remote struct {
					ID     int `json:"id"`
					MDSMap struct {
						MetadataPool *int64 `json:"metadata_pool"`
					} `json:"mdsmap"`
				}
				if json.Unmarshal(data, &remote) != nil || remote.ID <= 0 || remote.MDSMap.MetadataPool == nil {
					return errors.New("destination did not return mirrored filesystem ID and metadata pool")
				}
				mirror.destinationFilesystemID, mirror.destinationMetadataPoolID = remote.ID, *remote.MDSMap.MetadataPool
			}
			return nil
		}
	}
	return fmt.Errorf("mirrored filesystem metadata pool ID %d was not found in native pool listing", *filesystem.MDSMap.MetadataPool)
}

// ServiceDaemon's RADOS session and FSMirror's filesystem RADOS session have
// different GIDs. Resolve each owned process through its exact admin-socket
// rados_inst address and the native mirror index object's watcher list.
func (mirror *CephFSMirror) ownedFilesystemWatcherIDs(ctx context.Context) (map[string]bool, error) {
	control, err := mirror.source.ControlContainerContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("select CephFS source control handle: %w", err)
	}
	if control == nil {
		return nil, errors.New("CephFS source control container is unavailable")
	}
	data, err := exec(ctx, control, "rados", "--pool", mirror.metadataPool, "listwatchers", "cephfs_mirror")
	if err != nil {
		return nil, fmt.Errorf("list mirrored filesystem watchers: %w", err)
	}
	watchers, err := parseCephFSMirrorWatchers(data)
	if err != nil {
		return nil, err
	}
	ids := make(map[string]bool)
	for _, daemon := range mirror.daemons {
		data, err := exec(ctx, daemon, "ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "status", fmt.Sprintf("%s@%d", mirror.SourceFilesystem, mirror.filesystemID))
		if err != nil {
			return nil, fmt.Errorf("inspect filesystem session for mirror daemon %s: %w", daemon.DaemonName, err)
		}
		var status struct {
			RadosInstance string                     `json:"rados_inst"`
			Peers         map[string]json.RawMessage `json:"peers"`
		}
		if err := json.Unmarshal(data, &status); err != nil {
			return nil, fmt.Errorf("decode mirror daemon filesystem session: %w", err)
		}
		if _, exists := status.Peers[mirror.peerID]; !exists {
			return nil, fmt.Errorf("mirror daemon %s has not discovered its peer", daemon.DaemonName)
		}
		id := watchers[status.RadosInstance]
		if id == "" {
			return nil, fmt.Errorf("mirror daemon %s filesystem session %q is not a native watcher", daemon.DaemonName, status.RadosInstance)
		}
		if ids[id] {
			return nil, fmt.Errorf("owned mirror daemons share filesystem watcher instance %s", id)
		}
		ids[id] = true
	}
	if len(watchers) != len(ids) {
		return nil, fmt.Errorf("mirrored filesystem has %d native watchers but only %d belong to this fixture; wait for retired instances or remove foreign mirror processes", len(watchers), len(ids))
	}
	return ids, nil
}

func parseCephFSMirrorWatchers(data []byte) (map[string]string, error) {
	watchers := make(map[string]string)
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 3 || !strings.HasPrefix(fields[0], "watcher=") || !strings.HasPrefix(fields[1], "client.") || !strings.HasPrefix(fields[2], "cookie=") {
			return nil, fmt.Errorf("invalid native CephFS mirror watcher entry %q", line)
		}
		address, id := strings.TrimPrefix(fields[0], "watcher="), strings.TrimPrefix(fields[1], "client.")
		gid, err := strconv.ParseUint(id, 10, 64)
		if err != nil || gid == 0 || address == "" {
			return nil, fmt.Errorf("invalid native CephFS mirror watcher identity %q", line)
		}
		if previous := watchers[address]; previous != "" && previous != id {
			return nil, fmt.Errorf("ambiguous native CephFS mirror watcher address %q", address)
		}
		watchers[address] = id
	}
	return watchers, nil
}

func cephFSMirrorPolicyKnowsInstances(data []byte, ids map[string]bool) error {
	var distribution struct {
		Mapping map[string]string `json:"mapping"`
	}
	if err := json.Unmarshal(data, &distribution); err != nil {
		return fmt.Errorf("decode native CephFS mirror policy distribution: %w", err)
	}
	if len(distribution.Mapping) != len(ids) {
		return fmt.Errorf("native CephFS mirror policy has %d instances; expected exactly %d current owned filesystem watchers", len(distribution.Mapping), len(ids))
	}
	for id := range ids {
		if _, present := distribution.Mapping[id]; !present {
			return fmt.Errorf("native CephFS mirror policy has not discovered owned instance %s", id)
		}
	}
	return nil
}

func countCephFSMirrorRegistrations(data []byte, filesystem, peerID string, ownedIDs map[string]bool) (int, error) {
	var statuses []struct {
		DaemonID    uint64 `json:"daemon_id"`
		Filesystems []struct {
			Name  string `json:"name"`
			Peers []struct {
				UUID string `json:"uuid"`
			} `json:"peers"`
		} `json:"filesystems"`
	}
	if err := json.Unmarshal(data, &statuses); err != nil {
		return 0, fmt.Errorf("decode native CephFS mirror daemon registrations: %w", err)
	}
	registered := make(map[uint64]bool)
	for _, status := range statuses {
		if !ownedIDs[strconv.FormatUint(status.DaemonID, 10)] {
			continue
		}
		for _, fs := range status.Filesystems {
			if fs.Name != filesystem {
				continue
			}
			for _, peer := range fs.Peers {
				if peer.UUID == peerID {
					registered[status.DaemonID] = true
				}
			}
		}
	}
	return len(registered), nil
}

// Daemons returns the owned daemon inventory sorted by DaemonName, including
// stopped and partially started processes. The returned slice is a copy.
// Native assignment convergence is asynchronous.
func (mirror *CephFSMirror) Daemons() []*CephFSMirrorDaemon {
	if mirror == nil {
		return nil
	}
	mirror.mu.Lock()
	defer mirror.mu.Unlock()
	daemons := append([]*CephFSMirrorDaemon(nil), mirror.daemons...)
	slices.SortFunc(daemons, func(a, b *CephFSMirrorDaemon) int { return strings.Compare(a.DaemonName, b.DaemonName) })
	return daemons
}

// AddDaemon starts another process using this fixture's existing credentials,
// directories and peer. It does not bootstrap a second peer. Constructor
// customizers apply to every daemon, followed by the customizers supplied here.
// A non-nil partial result remains owned and may be removed or terminated.
// Native peer discovery and assignment are asynchronous. Redistributing existing
// directories depends on the native MGR; Ceph 20.2.4 raises a DirectoryState
// error during live expansion. Use RebalanceDirectories to explicitly reconcile
// this fixture's directory assignments after adding a process.
// Ceph's Tentacle documentation describes native assignment and failover but
// also notes that deploying multiple CephFS mirror daemons is untested upstream.
func (mirror *CephFSMirror) AddDaemon(ctx context.Context, daemonName string, opts ...testcontainers.ContainerCustomizer) (*CephFSMirrorDaemon, error) {
	if err := lockRGWSyncObservation(ctx, &mirror.mu); err != nil {
		return nil, err
	}
	defer mirror.mu.Unlock()
	if err := mirror.guardPeerRemovalOverlap(); err != nil {
		return nil, err
	}
	if mirror.closed {
		return nil, errors.New("CephFS mirror has been terminated")
	}
	if mirror.source == nil || mirror.destination == nil || mirror.daemonImage == "" || len(mirror.daemonKeyring) == 0 {
		return nil, errors.New("CephFS mirror is not initialized")
	}
	if err := validateCephFSMirrorDaemonName(daemonName); err != nil {
		return nil, err
	}
	for _, existing := range mirror.daemons {
		if existing.DaemonName == daemonName {
			return nil, fmt.Errorf("CephFS mirror daemon %q already exists", daemonName)
		}
	}
	const socket = "/var/run/ceph/cephfs-mirror.asok"
	moduleOpts := []testcontainers.ContainerCustomizer{
		mirror.source.WithClient(),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader: bytes.NewReader(mirror.daemonKeyring), ContainerFilePath: "/etc/ceph/ceph.client." + mirror.sourceID + ".keyring", FileMode: 0o600,
		}),
		testcontainers.WithEntrypoint("cephfs-mirror"),
		testcontainers.WithCmd("--id", mirror.sourceID, "-f", "--admin-socket", socket, "--cephfs-mirror-directory-scan-interval", "1"),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"test", "-S", socket}).WithStartupTimeout(2 * time.Minute)),
	}
	if !mirror.source.UsesHostNetwork() {
		moduleOpts = append(moduleOpts, network.WithNetworkName(nil, mirror.destination.NetworkName()))
	}
	moduleOpts = append(moduleOpts, mirror.daemonOptions...)
	moduleOpts = append(moduleOpts, opts...)
	container, err := testcontainers.Run(ctx, mirror.daemonImage, moduleOpts...)
	var daemon *CephFSMirrorDaemon
	if container != nil {
		daemon = mirror.registerDaemonContainer(container, daemonName)
	}
	if err != nil {
		return daemon, fmt.Errorf("run CephFS snapshot mirror daemon %q: %w", daemonName, err)
	}
	return daemon, nil
}

// Called under mirror.mu. Register partial startup results before propagating
// an error so either RemoveDaemon or Terminate can release them later.
func (mirror *CephFSMirror) registerDaemonContainer(container testcontainers.Container, daemonName string) *CephFSMirrorDaemon {
	daemon := &CephFSMirrorDaemon{Container: container, DaemonName: daemonName}
	if !mirror.initialDaemonAssigned {
		mirror.Container = daemon
		mirror.initialDaemonAssigned = true
	}
	mirror.daemons = append(mirror.daemons, daemon)
	mirror.owned.addContainer(daemon)
	return daemon
}

// RemoveDaemon terminates one owned process. Removing the last daemon is
// allowed for a complete replication outage; AddDaemon resumes using the same
// auth, peer and directory policy. Source and destination data are preserved.
// Native MGR registration and directory reassignment are asynchronous. A failed
// termination retains the inventory entry so removal can be retried.
func (mirror *CephFSMirror) RemoveDaemon(ctx context.Context, daemonName string, opts ...testcontainers.TerminateOption) error {
	if err := lockRGWSyncObservation(ctx, &mirror.mu); err != nil {
		return err
	}
	defer mirror.mu.Unlock()
	if mirror.closed {
		return errors.New("CephFS mirror has been terminated")
	}
	for index, daemon := range mirror.daemons {
		if daemon.DaemonName != daemonName {
			continue
		}
		if err := daemon.Terminate(ctx, opts...); err != nil {
			return fmt.Errorf("remove CephFS mirror daemon %q: %w", daemonName, err)
		}
		if mirror.Container != nil && mirror.Container.GetContainerID() == daemon.GetContainerID() {
			mirror.Container = nil
		}
		mirror.daemons = slices.Delete(mirror.daemons, index, index+1)
		return nil
	}
	return fmt.Errorf("CephFS mirror does not own daemon %q", daemonName)
}

func validateCephFSMirrorDaemonName(name string) error {
	if name == "" || len(name) > 63 {
		return errors.New("CephFS mirror daemon name must contain 1 to 63 letters, digits, dots, underscores or hyphens")
	}
	for _, character := range name {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '.' || character == '_' || character == '-') {
			return errors.New("CephFS mirror daemon name must contain 1 to 63 letters, digits, dots, underscores or hyphens")
		}
	}
	return nil
}

// Terminate removes owned containers and MGR network attachments. It leaves
// caller-owned clusters, auth entities, peer policies and snapshots intact.
// It may be retried after a cleanup error.
func (mirror *CephFSMirror) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	if mirror == nil {
		return nil
	}
	if err := lockRGWSyncObservation(ctx, &mirror.mu); err != nil {
		return err
	}
	defer mirror.mu.Unlock()
	mirror.closed = true
	if err := mirror.owned.terminate(ctx, opts...); err != nil {
		return err
	}
	mirror.daemons = nil
	mirror.Container = nil
	return nil
}

// AttachManagers reconciles peer-network access for every currently owned source
// MGR candidate, including standbys and stopped candidates. Call it after
// AddManager when the new candidate needs peer access before a later failover.
// RebootstrapPeer also calls it before importing a peer token. Host-network
// fixtures already share the namespace and require no Docker attachments.
// Existing attachments supplied by the caller remain caller-owned.
func (mirror *CephFSMirror) AttachManagers(ctx context.Context) error {
	if err := lockRGWSyncObservation(ctx, &mirror.mu); err != nil {
		return err
	}
	defer mirror.mu.Unlock()
	return mirror.attachManagers(ctx)
}

// Called under mirror.mu; Terminate uses the same lock so the Docker client
// cannot close while a topology reconciliation is using it.
func (mirror *CephFSMirror) attachManagers(ctx context.Context) error {
	if mirror.closed {
		return errors.New("CephFS mirror has been terminated")
	}
	if mirror.source == nil || mirror.destination == nil {
		return errors.New("CephFS mirror is not initialized")
	}
	ids, err := cephFSManagerCandidates(ctx, mirror.source)
	if err != nil {
		return err
	}
	if mirror.source.UsesHostNetwork() {
		return nil
	}
	if mirror.managerNetworking == nil {
		docker, err := testcontainers.NewDockerClientWithOpts(ctx)
		if err != nil {
			return fmt.Errorf("create Docker client for CephFS manager networking: %w", err)
		}
		mirror.managerNetworking = &cephFSManagerNetworking{
			docker: docker, network: mirror.destination.NetworkName(), owned: make(map[string]bool),
		}
		mirror.owned.addCleanup("disconnect CephFS managers and close Docker client", mirror.managerNetworking.cleanup)
	}
	return mirror.managerNetworking.attach(ctx, ids)
}

func cephFSManagerCandidates(ctx context.Context, cluster *ceph.Container) ([]string, error) {
	if cluster == nil {
		return nil, errors.New("source Ceph manager is unavailable")
	}
	// Enabling a Python MGR module can restart the active manager's module
	// runtime and temporarily clear mgrmap.available. Topology construction
	// needs the recovered owned active, rather than a single transient map.
	waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	return waitForCephFSManagerCandidates(waitCtx, 500*time.Millisecond, func(observeCtx context.Context) (ceph.ManagerStatus, []*ceph.ManagerContainer, error) {
		managers, err := cluster.ManagersContext(observeCtx)
		if err != nil {
			return ceph.ManagerStatus{}, nil, err
		}
		status, err := cluster.ManagerStatus(observeCtx)
		return status, managers, err
	})
}

func waitForCephFSManagerCandidates(ctx context.Context, interval time.Duration, observe func(context.Context) (ceph.ManagerStatus, []*ceph.ManagerContainer, error)) ([]string, error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lastErr := errors.New("manager map has not been observed")
	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("wait for a running owned active source Ceph manager (last observation: %v): %w", lastErr, err)
		}
		status, managers, err := observe(ctx)
		if err != nil {
			lastErr = fmt.Errorf("inspect source Ceph manager map: %w", err)
		} else {
			ids, err := selectCephFSManagerCandidates(status, managers)
			if err == nil {
				return ids, nil
			}
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for a running owned active source Ceph manager (last observation: %v): %w", lastErr, ctx.Err())
		case <-ticker.C:
		}
	}
}

func selectCephFSManagerCandidates(status ceph.ManagerStatus, managers []*ceph.ManagerContainer) ([]string, error) {
	if !status.Available || status.ActiveName == "" {
		return nil, fmt.Errorf("source Ceph manager is unavailable: available=%t active=%q gid=%d", status.Available, status.ActiveName, status.ActiveGID)
	}
	ids := make([]string, 0, len(managers))
	activeOwned := false
	for _, manager := range managers {
		if manager == nil || manager.Container == nil || manager.GetContainerID() == "" {
			continue
		}
		ids = append(ids, manager.GetContainerID())
		if manager.DaemonName == status.ActiveName && manager.IsRunning() {
			activeOwned = true
		}
	}
	if !activeOwned {
		return nil, fmt.Errorf("active source Ceph manager %q gid=%d is not a running owned candidate", status.ActiveName, status.ActiveGID)
	}
	return ids, nil
}

type cephFSManagerDocker interface {
	ContainerInspect(context.Context, string, mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error)
	NetworkConnect(context.Context, string, mobycl.NetworkConnectOptions) (mobycl.NetworkConnectResult, error)
	NetworkDisconnect(context.Context, string, mobycl.NetworkDisconnectOptions) (mobycl.NetworkDisconnectResult, error)
	Close() error
}

// One Docker client survives partial connect/disconnect failures so cleanup can
// be retried. Only an attachment observed absent before our connect is owned.
type cephFSManagerNetworking struct {
	docker  cephFSManagerDocker
	network string
	owned   map[string]bool
	closed  bool
}

func (networking *cephFSManagerNetworking) attached(ctx context.Context, id string) (bool, error) {
	inspection, err := networking.docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
	if err != nil {
		return false, err
	}
	if inspection.Container.NetworkSettings == nil {
		return false, errors.New("source CephFS manager has no Docker network settings")
	}
	_, attached := inspection.Container.NetworkSettings.Networks[networking.network]
	return attached, nil
}

func (networking *cephFSManagerNetworking) attach(ctx context.Context, ids []string) error {
	if networking.closed {
		return errors.New("CephFS manager networking has been closed")
	}
	for _, id := range ids {
		attached, err := networking.attached(ctx, id)
		if err != nil {
			return fmt.Errorf("inspect source CephFS manager %s: %w", id, err)
		}
		if attached {
			continue
		}
		// Exec/transport failure cannot establish that Docker did not connect
		// the endpoint. Retain this previously absent attachment for cleanup.
		networking.owned[id] = true
		if _, err := networking.docker.NetworkConnect(ctx, networking.network, mobycl.NetworkConnectOptions{Container: id}); err != nil {
			return fmt.Errorf("connect source CephFS manager %s to destination network: %w", id, err)
		}
	}
	return nil
}

func (networking *cephFSManagerNetworking) cleanup(ctx context.Context) error {
	if networking.closed {
		return nil
	}
	ids := make([]string, 0, len(networking.owned))
	for id := range networking.owned {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	var errs []error
	for _, id := range ids {
		attached, err := networking.attached(ctx, id)
		if err != nil {
			if ignoreMissing(err) == nil {
				delete(networking.owned, id)
			} else {
				errs = append(errs, fmt.Errorf("inspect source CephFS manager %s during cleanup: %w", id, err))
			}
			continue
		}
		if attached {
			if _, err := networking.docker.NetworkDisconnect(ctx, networking.network, mobycl.NetworkDisconnectOptions{Container: id}); ignoreMissing(err) != nil {
				errs = append(errs, fmt.Errorf("disconnect source CephFS manager %s: %w", id, err))
				continue
			}
		}
		delete(networking.owned, id)
	}
	if len(errs) != 0 {
		return errors.Join(errs...)
	}
	if err := networking.docker.Close(); err != nil {
		return fmt.Errorf("close CephFS manager Docker client: %w", err)
	}
	networking.closed = true
	return nil
}

// AddDirectory registers an absolute CephFS path for snapshot mirroring. Ceph
// rejects overlapping paths. The path refers to the filesystem root, not a
// container mount point. Registration does not create the directory or data.
func (mirror *CephFSMirror) AddDirectory(ctx context.Context, directory string) error {
	directory, err := normalizeMirrorDirectory(directory)
	if err != nil {
		return err
	}
	if err := lockRGWSyncObservation(ctx, &mirror.mu); err != nil {
		return err
	}
	defer mirror.mu.Unlock()
	if err := mirror.guardPeerRemovalOverlap(); err != nil {
		return err
	}
	if mirror.source == nil {
		return errors.New("CephFS mirror is not initialized")
	}
	if _, err := mirror.source.Ceph(ctx, "fs", "snapshot", "mirror", "add", mirror.SourceFilesystem, directory); err != nil {
		return fmt.Errorf("add CephFS mirror directory %q: %w", directory, err)
	}
	mirror.Directories = append(mirror.Directories, directory)
	if mirror.ownedDirectories == nil {
		mirror.ownedDirectories = make(map[string]bool)
	}
	mirror.ownedDirectories[directory] = true
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
	if err := lockRGWSyncObservation(ctx, &mirror.mu); err != nil {
		return err
	}
	defer mirror.mu.Unlock()
	if err := mirror.guardPeerRemovalOverlap(); err != nil {
		return err
	}
	if mirror.source == nil {
		return errors.New("CephFS mirror is not initialized")
	}
	if _, err := mirror.source.Ceph(ctx, "fs", "snapshot", "mirror", "remove", mirror.SourceFilesystem, directory); err != nil {
		return fmt.Errorf("remove CephFS mirror directory %q: %w", directory, err)
	}
	mirror.Directories = slices.DeleteFunc(mirror.Directories, func(item string) bool { return item == directory })
	delete(mirror.ownedDirectories, directory)
	delete(mirror.pendingDirectoryRelease, directory)
	return nil
}

// RebalanceDirectories explicitly redistributes this fixture's directory
// policies among its current running daemons. It unregisters only owned paths,
// waits until the native MGR finishes releasing their assignments, and registers
// the same paths again. Files, snapshots, peer and auth entities are preserved.
// Replication of these paths pauses during reconciliation; this is a fixture
// operation, distinct from native daemon failure recovery. It can be used after
// AddDaemon; Ceph 20.2.4's native MGR fails to reshuffle already-assigned paths
// when another live daemon joins.
// Every current member must be running; remove stopped members first. Intended
// membership is retained on partial CLI failure so a fresh-context retry can
// recover. Caller-added policies and other filesystems are not modified.
func (mirror *CephFSMirror) RebalanceDirectories(ctx context.Context) error {
	if err := lockRGWSyncObservation(ctx, &mirror.mu); err != nil {
		return err
	}
	defer mirror.mu.Unlock()
	if err := mirror.guardPeerRemovalOverlap(); err != nil {
		return err
	}
	if mirror.closed {
		return errors.New("CephFS mirror has been terminated")
	}
	if mirror.source == nil || mirror.peerID == "" {
		return errors.New("CephFS mirror is not initialized")
	}
	directories := make([]string, 0, len(mirror.ownedDirectories))
	for directory, owned := range mirror.ownedDirectories {
		if owned {
			directories = append(directories, directory)
		}
	}
	slices.Sort(directories)
	if len(directories) == 0 {
		return nil
	}
	if len(mirror.daemons) == 0 {
		return errors.New("CephFS directory rebalance requires a running mirror daemon")
	}
	for _, daemon := range mirror.daemons {
		if !daemon.IsRunning() {
			return fmt.Errorf("CephFS directory rebalance requires daemon %q to be running; remove stopped members first", daemon.DaemonName)
		}
	}
	if err := mirror.waitForDaemonRegistrations(ctx, len(mirror.daemons)); err != nil {
		return err
	}
	if mirror.pendingDirectoryRelease == nil {
		mirror.pendingDirectoryRelease = make(map[string]bool)
	}
	return rebalanceCephFSMirrorDirectories(ctx, directories, mirror.sourceDirectories,
		func(commandCtx context.Context, operation, directory string) error {
			_, err := mirror.source.Ceph(commandCtx, "fs", "snapshot", "mirror", operation, mirror.SourceFilesystem, directory)
			return err
		}, mirror.pendingDirectoryRelease, 500*time.Millisecond)
}

func (mirror *CephFSMirror) sourceDirectories(ctx context.Context) ([]string, error) {
	data, err := mirror.source.Ceph(ctx, "fs", "snapshot", "mirror", "ls", mirror.SourceFilesystem)
	if err != nil {
		return nil, fmt.Errorf("list CephFS mirror directories: %w", err)
	}
	var directories []string
	if err := json.Unmarshal(data, &directories); err != nil || directories == nil {
		return nil, errors.New("source did not return a CephFS mirror directory list")
	}
	return directories, nil
}

// The intent slice is deliberately unchanged throughout partial mutation. A
// retry first drains whichever owned paths currently exist, then restores all.
func rebalanceCephFSMirrorDirectories(ctx context.Context, intent []string, list func(context.Context) ([]string, error), mutate func(context.Context, string, string) error, pending map[string]bool, interval time.Duration) error {
	current, err := list(ctx)
	if err != nil {
		return err
	}
	for _, directory := range intent {
		if !slices.Contains(current, directory) {
			continue
		}
		previousAttempt := pending[directory]
		pending[directory] = true
		if err := mutate(ctx, "remove", directory); err != nil {
			// An earlier response may have been lost after native removal began.
			// Tentacle's MGR rejects another remove while purging; only this
			// tracked retry may proceed to the native release barrier. If the
			// earlier command was never applied, the retry still executes remove.
			if !previousAttempt || !strings.Contains(err.Error(), "is under removal") {
				return fmt.Errorf("release owned CephFS mirror directory %q: %w", directory, err)
			}
		}
	}
	drainCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	for {
		current, err := list(drainCtx)
		if err != nil {
			return err
		}
		remaining := false
		for _, directory := range intent {
			remaining = remaining || slices.Contains(current, directory)
		}
		if !remaining {
			break
		}
		select {
		case <-drainCtx.Done():
			return fmt.Errorf("wait for native CephFS mirror directory release: %w", drainCtx.Err())
		case <-time.After(interval):
		}
	}
	for _, directory := range intent {
		delete(pending, directory)
	}
	for _, directory := range intent {
		if err := mutate(ctx, "add", directory); err != nil {
			return fmt.Errorf("restore owned CephFS mirror directory %q: %w", directory, err)
		}
	}
	return nil
}

// PeerIDs lists configured source filesystem peers in UUID order. These are
// monitor policies, not a guarantee that daemon synchronization has stopped.
func (mirror *CephFSMirror) PeerIDs(ctx context.Context) ([]string, error) {
	if err := lockRGWSyncObservation(ctx, &mirror.mu); err != nil {
		return nil, err
	}
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
// policy acknowledgement does not prove native replayer teardown. For a retained
// strict same-session drain receipt, use BeginPeerRemoval and WaitDrained.
func (mirror *CephFSMirror) RemovePeer(ctx context.Context, id string) error {
	if err := lockRGWSyncObservation(ctx, &mirror.mu); err != nil {
		return err
	}
	defer mirror.mu.Unlock()
	if mirror.peerRemoval != nil && mirror.peerRemoval.peerID == id && !mirror.peerRemoval.completed {
		if err := mirror.peerRemoval.checkHandle(); err != nil {
			return err
		}
		return mirror.peerRemoval.resume(ctx)
	}
	if err := mirror.guardPeerRemovalOverlap(); err != nil {
		return err
	}
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
// Current source MGR candidates receive peer-network access before import,
// including replacements added after the mirror was constructed.
func (mirror *CephFSMirror) RebootstrapPeer(ctx context.Context) (string, error) {
	if mirror == nil {
		return "", cephFSObserveGuard("CephFS mirror fixture is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if err := lockRGWSyncObservation(ctx, &mirror.mu); err != nil {
		return "", err
	}
	defer mirror.mu.Unlock()
	if err := mirror.guardPeerRemovalOverlap(); err != nil {
		return "", err
	}
	if err := mirror.confirmCephFSRefreshHandle(false); err != nil {
		return "", err
	}
	if err := mirror.checkCephFSRefreshFilesystems(ctx); err != nil {
		return "", err
	}
	if err := mirror.attachManagers(ctx); err != nil {
		return "", err
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
		return "", cephFSObserveQuery("create destination CephFS peer bootstrap token", err)
	}
	var token struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(bootstrap, &token); err != nil || token.Token == "" {
		return "", errors.New("destination did not return a valid CephFS peer bootstrap token")
	}
	token.Token, err = mirror.normalizeCephFSPeerBootstrap(ctx, token.Token)
	if err != nil {
		return "", err
	}
	// An error from Exec cannot prove the manager did not apply the command.
	// Keep the attempted identity before import, including on import failure.
	mirror.pendingPeerImport = expected
	if _, err := mirror.source.Ceph(ctx, "fs", "snapshot", "mirror", "peer_bootstrap", "import", mirror.SourceFilesystem, token.Token); err != nil {
		return "", cephFSObserveQuery("import destination CephFS peer", err)
	}
	if err := mirror.checkCephFSRefreshFilesystems(ctx); err != nil {
		return "", err
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
	if mirror.peerID != id {
		mirror.peerGeneration++
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
	if config.DaemonCount < 0 {
		return config, errors.New("CephFS mirror daemon count must not be negative")
	}
	if config.DaemonCount == 0 {
		config.DaemonCount = 1
	}
	for _, name := range []string{config.SourceFilesystem, config.DestinationFilesystem} {
		if !validCephFSMirrorFilesystemName(name) {
			return config, errors.New("source and destination CephFS names must use letters, digits, underscores, dots or hyphens, starting with a letter, digit or underscore")
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

func validCephFSMirrorFilesystemName(name string) bool {
	if name == "" || name[0] == '.' || name[0] == '-' {
		return false
	}
	for _, character := range name {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '.' || character == '_' || character == '-') {
			return false
		}
	}
	return true
}
