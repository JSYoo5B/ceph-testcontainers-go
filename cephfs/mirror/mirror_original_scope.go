package mirror

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"github.com/google/uuid"
)

// cephFSOriginalScope is the original identity that a directory addition, a
// directory removal and a peer removal compare every later observation with:
// the two clusters and their FSIDs, both filesystems and their metadata
// pools, the source client, the owned peer and its generation. It records no
// daemon, witness or progress, so a directory addition captures it without a
// live daemon. Its methods run while Mirror.mu is held.
type cephFSOriginalScope struct {
	mirror                                                              *Mirror
	source, destination                                                 *ceph.Container
	sourceFilesystem, destinationFilesystem, sourceClient, metadataPool string
	sourceID, destinationID                                             int
	sourceMetadata, destinationMetadata                                 int64
	peerID                                                              string
	peer                                                                cephFSPeerIdentity
	generation                                                          uint64
	clusters                                                            [2]string
	readClusters                                                        func(context.Context) ([2]string, error)
}

// captureOriginalScope records the fixture's current identity for peerID
// after its native cluster identities are read and found distinct. operation
// names the receipt in the refusals. Mirror.mu is held.
func (mirror *Mirror) captureOriginalScope(ctx context.Context, operation, peerID string, readClusters func(context.Context) ([2]string, error)) (*cephFSOriginalScope, error) {
	if mirror.metadataPoolID <= 0 || mirror.destinationMetadataPoolID <= 0 || mirror.SourceClientEntity == "" || mirror.metadataPool == "" {
		return nil, cephFSObserveGuard(operation + " original filesystem metadata identity is unavailable")
	}
	if readClusters == nil {
		return nil, cephFSObserveGuard(operation + " original cluster identity reader is unavailable")
	}
	clusters, err := readClusters(ctx)
	if err != nil {
		return nil, err
	}
	if !validCephFSRemovalClusters(clusters) {
		return nil, cephFSObserveGuard(operation + " original cluster identities are unavailable or equal")
	}
	return &cephFSOriginalScope{mirror: mirror, source: mirror.source, destination: mirror.destination,
		sourceFilesystem: mirror.SourceFilesystem, destinationFilesystem: mirror.DestinationFilesystem, sourceClient: mirror.SourceClientEntity, metadataPool: mirror.metadataPool,
		sourceID: mirror.filesystemID, destinationID: mirror.destinationFilesystemID, sourceMetadata: mirror.metadataPoolID, destinationMetadata: mirror.destinationMetadataPoolID,
		peerID: peerID, peer: cephFSPeerIdentity{mirror.DestinationClientEntity, mirror.destinationSite, mirror.DestinationFilesystem}, generation: mirror.ops.peerGeneration, clusters: clusters, readClusters: readClusters}, nil
}

// usable reports whether the scope still has its fixture, both clusters and
// its cluster identity reader.
func (s *cephFSOriginalScope) usable() bool {
	return s != nil && s.mirror != nil && s.source != nil && s.destination != nil && s.readClusters != nil
}

// sameFixture reports whether the fixture still has the recorded clusters,
// filesystems, metadata pools, clients, destination site and peer. A peer
// removal passes allowClearedPeer, because its own request clears the
// fixture's peer. Mirror.mu is held.
func (s *cephFSOriginalScope) sameFixture(allowClearedPeer bool) bool {
	m := s.mirror
	peer := m.peerID == s.peerID || (allowClearedPeer && m.peerID == "")
	return peer && m.source == s.source && m.destination == s.destination && m.SourceFilesystem == s.sourceFilesystem && m.DestinationFilesystem == s.destinationFilesystem && m.SourceClientEntity == s.sourceClient && m.filesystemID == s.sourceID && m.destinationFilesystemID == s.destinationID && m.metadataPoolID == s.sourceMetadata && m.destinationMetadataPoolID == s.destinationMetadata && m.metadataPool == s.metadataPool && m.DestinationClientEntity == s.peer.ClientName && m.destinationSite == s.peer.SiteName
}

// directoryPolicy reads the source filesystem's native directory policy with
// the strict ls decoder. operation names the receipt in a query error.
func (s *cephFSOriginalScope) directoryPolicy(ctx context.Context, operation string) (map[string]bool, error) {
	data, err := s.source.Ceph(ctx, "fs", "snapshot", "mirror", "ls", s.sourceFilesystem)
	if err != nil {
		return nil, cephFSObserveQuery("read original CephFS "+operation+" policy", err)
	}
	return decodeCephFSDirectoryPolicy(data)
}

// cephFSDaemonSession is one daemon's filesystem session as its admin socket
// reports it: the RADOS instance address and whether it sees the owned peer.
type cephFSDaemonSession struct {
	address     string
	peerPresent bool
}

// watchers lists the native watchers of the source filesystem's mirror index
// object by RADOS address.
func (s *cephFSOriginalScope) watchers(ctx context.Context) (map[string]string, error) {
	control, err := s.source.ClientContainer(ctx)
	if err != nil {
		return nil, cephFSObserveQuery("select CephFS control container", err)
	}
	if control == nil {
		return nil, cephFSObserveGuard("CephFS control container is unavailable")
	}
	data, err := multicluster.Exec(ctx, control, "rados", "--pool", s.metadataPool, "listwatchers", "cephfs_mirror")
	if err != nil {
		return nil, cephFSObserveQuery("read original CephFS filesystem watchers", err)
	}
	value, err := parseCephFSMirrorWatchers(data)
	if err != nil {
		return nil, cephFSObserveGuard("decode original CephFS filesystem watchers")
	}
	return value, ctx.Err()
}

// session reads daemon's session of the original source filesystem and
// checks its peer against the original peer identity.
func (s *cephFSOriginalScope) session(ctx context.Context, daemon *Daemon) (cephFSDaemonSession, error) {
	data, err := multicluster.Exec(ctx, daemon, "ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "status", fmt.Sprintf("%s@%d", s.sourceFilesystem, s.sourceID))
	if err != nil {
		return cephFSDaemonSession{}, cephFSObserveQuery("read original CephFS filesystem session", err)
	}
	return decodeCephFSRemovalSession(data, s.peerID, s.peer)
}

// catalog reports whether daemon's admin socket still lists the original
// peer's status command.
func (s *cephFSOriginalScope) catalog(ctx context.Context, daemon *Daemon) (bool, error) {
	data, err := multicluster.Exec(ctx, daemon, "ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "help")
	if err != nil {
		return false, cephFSObserveQuery("read original CephFS command catalog", err)
	}
	return decodeCephFSRemovalCatalog(data, fmt.Sprintf("fs mirror status %s@%d", s.sourceFilesystem, s.sourceID), fmt.Sprintf("fs mirror peer status %s@%d %s", s.sourceFilesystem, s.sourceID, s.peerID))
}

func decodeCephFSRemovalCatalog(data []byte, fsCommand, peerCommand string) (bool, error) {
	var raw map[string]json.RawMessage
	if json.Unmarshal(data, &raw) != nil || raw == nil {
		return false, cephFSObserveGuard("decode CephFS command catalog object")
	}
	commands := make(map[string]string)
	for command, value := range raw {
		var text *string
		if json.Unmarshal(value, &text) != nil || text == nil || *text == "" {
			return false, cephFSObserveGuard("decode CephFS command catalog help text")
		}
		commands[command] = *text
	}
	if commands[fsCommand] == "" || commands["help"] == "" || commands["get_command_descriptions"] == "" {
		return false, cephFSObserveGuard("original CephFS filesystem command catalog is incomplete")
	}
	_, present := commands[peerCommand]
	return present, nil
}

func decodeCephFSRemovalSession(data []byte, id string, expected cephFSPeerIdentity) (cephFSDaemonSession, error) {
	var result cephFSDaemonSession
	var value struct {
		State    json.RawMessage            `json:"state"`
		Address  *string                    `json:"rados_inst"`
		Peers    map[string]json.RawMessage `json:"peers"`
		SnapDirs *struct {
			Count *int `json:"dir_count"`
		} `json:"snap_dirs"`
	}
	if json.Unmarshal(data, &value) != nil {
		return result, cephFSObserveGuard("decode CephFS filesystem session")
	}
	if len(value.State) != 0 {
		var state string
		if json.Unmarshal(value.State, &state) != nil || !slices.Contains([]string{"failed", "blocklisted"}, state) {
			return result, cephFSObserveGuard("decode CephFS filesystem failure state")
		}
		return result, cephFSObserveQuery("original CephFS filesystem session is unavailable", nil)
	}
	if value.Address == nil || *value.Address == "" || value.Peers == nil || value.SnapDirs == nil || value.SnapDirs.Count == nil || *value.SnapDirs.Count < 0 {
		return result, cephFSObserveGuard("decode normal CephFS filesystem session")
	}
	result.address = *value.Address
	present, err := decodeCephFSRemovalPeers(value.Peers, id, expected, true)
	result.peerPresent = present
	return result, err
}

// peer_list uses site_name, while FSMap/admin session use remote.cluster_name.
func decodeCephFSRemovalPeers(peers map[string]json.RawMessage, id string, expected cephFSPeerIdentity, remote bool) (bool, error) {
	if peers == nil {
		return false, cephFSObserveGuard("decode CephFS peer object")
	}
	for key := range peers {
		parsed, err := uuid.Parse(key)
		if err != nil || parsed.String() != key || parsed == uuid.Nil {
			return false, cephFSObserveGuard("decode CephFS peer UUID")
		}
	}
	if len(peers) == 0 {
		return false, nil
	}
	entry, exists := peers[id]
	if len(peers) != 1 || !exists {
		return false, cephFSObserveGuard("CephFS has a substituted peer UUID")
	}
	if remote {
		var wrapper *struct {
			Remote json.RawMessage `json:"remote"`
		}
		if json.Unmarshal(entry, &wrapper) != nil || wrapper == nil || len(wrapper.Remote) == 0 {
			return false, cephFSObserveGuard("decode CephFS remote identity")
		}
		entry = wrapper.Remote
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(entry, &fields) != nil || fields == nil {
		return false, cephFSObserveGuard("decode CephFS destination tuple")
	}
	keys := []string{"client_name", "site_name", "fs_name"}
	if remote {
		keys[1] = "cluster_name"
	}
	values := make([]string, 3)
	for i, key := range keys {
		var text *string
		if json.Unmarshal(fields[key], &text) != nil || text == nil || *text == "" {
			return false, cephFSObserveGuard("decode CephFS destination tuple field")
		}
		values[i] = *text
	}
	if (cephFSPeerIdentity{values[0], values[1], values[2]}) != expected {
		return false, cephFSObserveGuard("CephFS original destination tuple differs")
	}
	return true, nil
}

// policy validates the cluster, filesystem and metadata pool identities and
// reports whether the original peer is present in the MGR policy and in the
// source FSMap. Source FSMap is a MON view; peer_list is MGR's cached view.
func (s *cephFSOriginalScope) policy(ctx context.Context) (bool, bool, error) {
	clusters, err := s.readClusters(ctx)
	if err != nil {
		return false, false, err
	}
	if clusters != s.clusters {
		return false, false, cephFSObserveGuard("CephFS original cluster identity differs")
	}
	var fsPeers map[string]json.RawMessage
	for _, site := range []struct {
		cluster *ceph.Container
		name    string
		id      int
		pool    int64
		source  bool
	}{{s.source, s.sourceFilesystem, s.sourceID, s.sourceMetadata, true}, {s.destination, s.destinationFilesystem, s.destinationID, s.destinationMetadata, false}} {
		data, err := site.cluster.Ceph(ctx, "fs", "get", site.name, "--format", "json")
		if err != nil {
			return false, false, cephFSObserveQuery("read original CephFS filesystem identity", err)
		}
		var value struct {
			ID     *int `json:"id"`
			MDSMap *struct {
				Name *string `json:"fs_name"`
				Pool *int64  `json:"metadata_pool"`
			} `json:"mdsmap"`
			MirrorInfo *struct {
				Peers map[string]json.RawMessage `json:"peers"`
			} `json:"mirror_info"`
		}
		if json.Unmarshal(data, &value) != nil || value.ID == nil || *value.ID != site.id || value.MDSMap == nil || value.MDSMap.Name == nil || *value.MDSMap.Name != site.name || value.MDSMap.Pool == nil || *value.MDSMap.Pool != site.pool {
			return false, false, cephFSObserveGuard("CephFS original filesystem or metadata pool differs")
		}
		if site.source {
			if value.MirrorInfo == nil || value.MirrorInfo.Peers == nil {
				return false, false, cephFSObserveGuard("CephFS source mirroring policy is unavailable")
			}
			fsPeers = value.MirrorInfo.Peers
		}
	}
	fsPresent, err := decodeCephFSRemovalPeers(fsPeers, s.peerID, s.peer, true)
	if err != nil {
		return false, false, err
	}
	data, err := s.source.Ceph(ctx, "fs", "snapshot", "mirror", "peer_list", s.sourceFilesystem)
	if err != nil {
		return false, false, cephFSObserveQuery("read original CephFS manager policy", err)
	}
	var peers map[string]json.RawMessage
	if json.Unmarshal(data, &peers) != nil {
		return false, false, cephFSObserveGuard("decode original CephFS manager policy")
	}
	present, err := decodeCephFSRemovalPeers(peers, s.peerID, s.peer, false)
	return present, fsPresent, errors.Join(err, ctx.Err())
}

func validCephFSRemovalClusters(values [2]string) bool {
	if values[0] == values[1] {
		return false
	}
	for _, value := range values {
		parsed, err := uuid.Parse(value)
		if err != nil || parsed.String() != value || parsed == uuid.Nil {
			return false
		}
	}
	return true
}

func (mirror *Mirror) peerRemovalClusterIdentities(ctx context.Context) ([2]string, error) {
	var result [2]string
	for i, cluster := range []*ceph.Container{mirror.source, mirror.destination} {
		if cluster == nil {
			return result, cephFSObserveGuard("CephFS original cluster is unavailable")
		}
		observer, err := ceph.Observe(cluster)
		if err != nil {
			return result, err
		}
		original, err := readCephFSRemovalClusterIdentity(ctx, observer.MonitorBootstrapAddresses,
			func(queryCtx context.Context) (string, error) {
				status, err := observer.QuorumStatus(queryCtx)
				return status.MonMap.FSID, err
			},
			func(queryCtx context.Context) (string, error) {
				data, err := cluster.Ceph(queryCtx, "fsid")
				return strings.TrimSpace(string(data)), err
			})
		if err != nil {
			return result, err
		}
		result[i] = original
	}
	return result, ctx.Err()
}

// Common validation attests the fixture's private original template before and
// after the native identity observations. Native FSID alone is never adopted.
// The common API does not type query versus schema errors, so non-context
// failures conservatively guard this observation. The retained intent survives:
// retry Status/Wait with a fresh context after quorum availability is restored.
func readCephFSRemovalClusterIdentity(ctx context.Context, bootstrap, quorum, native func(context.Context) (string, error)) (string, error) {
	validate := func() error {
		addresses, err := bootstrap(ctx)
		if err != nil {
			cause := cephFSObserveQuery("validate original CephFS monitor bootstrap", err)
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return cause
			}
			return errors.Join(cephFSObserveGuard("original CephFS monitor identity is not confirmed"), cause)
		}
		if addresses == "" || strings.ContainsAny(addresses, "\x00\r\n") {
			return cephFSObserveGuard("original CephFS monitor bootstrap is unavailable")
		}
		return ctx.Err()
	}
	if err := validate(); err != nil {
		return "", err
	}
	original, err := quorum(ctx)
	if err != nil {
		cause := cephFSObserveQuery("read original CephFS monmap FSID", err)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return "", cause
		}
		return "", errors.Join(cephFSObserveGuard("original CephFS monmap identity is not confirmed"), cause)
	}
	parsed, err := uuid.Parse(original)
	if err != nil || parsed.String() != original || parsed == uuid.Nil {
		return "", cephFSObserveGuard("decode original CephFS monmap FSID")
	}
	current, err := native(ctx)
	if err != nil {
		return "", cephFSObserveQuery("read original CephFS native FSID", err)
	}
	if current != original {
		return "", cephFSObserveGuard("CephFS native FSID differs from its confirmed monmap")
	}
	if err := validate(); err != nil {
		return "", err
	}
	return original, ctx.Err()
}
