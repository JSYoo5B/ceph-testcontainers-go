package mirror

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
)

// The readers in this file each own one native report: they run its
// command, decode it strictly and return the decoded values, and every
// caller keeps its own decision on those values. A failed command is a
// retryable query error named by the caller's operation and wrapping the
// cause; a report that does not decode is a permanent guard error.

// cephFSMirrorAdminSocket is the admin socket every owned daemon serves.
const cephFSMirrorAdminSocket = "/var/run/ceph/cephfs-mirror.asok"

// adminSocket runs one command on daemon's admin socket.
func adminSocket(ctx context.Context, daemon testcontainers.Container, command ...string) ([]byte, error) {
	return multicluster.Exec(ctx, daemon, append([]string{"ceph", "--admin-daemon", cephFSMirrorAdminSocket}, command...)...)
}

// sessionName is the admin socket name of the source filesystem's session,
// <filesystem>@<ID>.
func (p cephFSMirrorPair) sessionName() string {
	return fmt.Sprintf("%s@%d", p.sourceFilesystem, p.sourceFilesystemID)
}

// cephFSFilesystemIdentity is one `fs get` report: the filesystem ID, its
// metadata pool ID and, for a filesystem with snapshot mirroring enabled,
// the peers of its FSMap mirror_info (nil without mirror_info).
type cephFSFilesystemIdentity struct {
	id           int
	metadataPool int64
	mirrorPeers  map[string]cephFSPeerIdentity
}

// readFilesystemIdentity reads `fs get name` from cluster.
func readFilesystemIdentity(ctx context.Context, cluster *ceph.Container, name, operation string) (cephFSFilesystemIdentity, error) {
	data, err := cluster.Ceph(ctx, "fs", "get", name, "--format", "json")
	if err != nil {
		return cephFSFilesystemIdentity{}, cephFSObserveQuery(operation, err)
	}
	return decodeFilesystemIdentity(data, name)
}

// decodeFilesystemIdentity requires a positive ID, the queried fs_name and a
// metadata pool ID. A mirror_info object must carry a peer object.
func decodeFilesystemIdentity(data []byte, name string) (cephFSFilesystemIdentity, error) {
	var native struct {
		ID     *int `json:"id"`
		MDSMap *struct {
			Name *string `json:"fs_name"`
			Pool *int64  `json:"metadata_pool"`
		} `json:"mdsmap"`
		MirrorInfo *struct {
			Peers map[string]json.RawMessage `json:"peers"`
		} `json:"mirror_info"`
	}
	if json.Unmarshal(data, &native) != nil || native.ID == nil || *native.ID <= 0 || native.MDSMap == nil ||
		native.MDSMap.Name == nil || *native.MDSMap.Name != name || native.MDSMap.Pool == nil || *native.MDSMap.Pool < 0 {
		return cephFSFilesystemIdentity{}, cephFSObserveGuard("decode CephFS filesystem identity")
	}
	identity := cephFSFilesystemIdentity{id: *native.ID, metadataPool: *native.MDSMap.Pool}
	if native.MirrorInfo != nil {
		peers, err := decodeCephFSPeers(native.MirrorInfo.Peers, true)
		if err != nil {
			return cephFSFilesystemIdentity{}, err
		}
		identity.mirrorPeers = peers
	}
	return identity, nil
}

// readPeerList reads the MGR's `fs snapshot mirror peer_list` of the
// source filesystem.
func readPeerList(ctx context.Context, pair cephFSMirrorPair, operation string) (map[string]cephFSPeerIdentity, error) {
	data, err := pair.source.Ceph(ctx, "fs", "snapshot", "mirror", "peer_list", pair.sourceFilesystem)
	if err != nil {
		return nil, cephFSObserveQuery(operation, err)
	}
	return decodePeerList(data)
}

func decodePeerList(data []byte) (map[string]cephFSPeerIdentity, error) {
	var peers map[string]json.RawMessage
	if json.Unmarshal(data, &peers) != nil {
		return nil, cephFSObserveGuard("decode CephFS peer object")
	}
	return decodeCephFSPeers(peers, false)
}

// decodeCephFSPeers decodes a peer object by peer UUID. peer_list names
// the destination as {client_name, site_name, fs_name}; with remote, the
// FSMap and a daemon session name it as {remote: {client_name, cluster_name,
// fs_name}}. Every key must be a canonical, non-nil UUID and every field a
// non-empty string.
func decodeCephFSPeers(peers map[string]json.RawMessage, remote bool) (map[string]cephFSPeerIdentity, error) {
	if peers == nil {
		return nil, cephFSObserveGuard("decode CephFS peer object")
	}
	result := make(map[string]cephFSPeerIdentity, len(peers))
	keys := []string{"client_name", "site_name", "fs_name"}
	if remote {
		keys[1] = "cluster_name"
	}
	for key, entry := range peers {
		parsed, err := uuid.Parse(key)
		if err != nil || parsed.String() != key || parsed == uuid.Nil {
			return nil, cephFSObserveGuard("decode CephFS peer UUID")
		}
		if remote {
			var wrapper *struct {
				Remote json.RawMessage `json:"remote"`
			}
			if json.Unmarshal(entry, &wrapper) != nil || wrapper == nil || len(wrapper.Remote) == 0 {
				return nil, cephFSObserveGuard("decode CephFS remote identity")
			}
			entry = wrapper.Remote
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(entry, &fields) != nil || fields == nil {
			return nil, cephFSObserveGuard("decode CephFS destination tuple")
		}
		values := make([]string, len(keys))
		for i, name := range keys {
			var text *string
			if json.Unmarshal(fields[name], &text) != nil || text == nil || *text == "" {
				return nil, cephFSObserveGuard("decode CephFS destination tuple field")
			}
			values[i] = *text
		}
		result[key] = cephFSPeerIdentity{ClientName: values[0], SiteName: values[1], FilesystemName: values[2]}
	}
	return result, nil
}

// cephFSDaemonSessionReport is one daemon's `fs mirror status` of the
// source filesystem session: failed for a failed or blocklisted session,
// otherwise its RADOS instance address and its peers.
type cephFSDaemonSessionReport struct {
	failed  bool
	address string
	peers   map[string]cephFSPeerIdentity
}

// readDaemonSession reads daemon's session of the pair's source filesystem.
func readDaemonSession(ctx context.Context, daemon testcontainers.Container, pair cephFSMirrorPair, operation string) (cephFSDaemonSessionReport, error) {
	data, err := adminSocket(ctx, daemon, "fs", "mirror", "status", pair.sessionName())
	if err != nil {
		return cephFSDaemonSessionReport{}, cephFSObserveQuery(operation, err)
	}
	return decodeDaemonSession(data)
}

// decodeDaemonSession reads a failure state, or a normal session with its
// address, peer object and directory count.
func decodeDaemonSession(data []byte) (cephFSDaemonSessionReport, error) {
	var native struct {
		State    json.RawMessage            `json:"state"`
		Address  *string                    `json:"rados_inst"`
		Peers    map[string]json.RawMessage `json:"peers"`
		SnapDirs *struct {
			Count *int `json:"dir_count"`
		} `json:"snap_dirs"`
	}
	if json.Unmarshal(data, &native) != nil {
		return cephFSDaemonSessionReport{}, cephFSObserveGuard("decode CephFS filesystem session")
	}
	if len(native.State) != 0 {
		var state string
		if json.Unmarshal(native.State, &state) != nil || !slices.Contains([]string{"failed", "blocklisted"}, state) {
			return cephFSDaemonSessionReport{}, cephFSObserveGuard("decode CephFS filesystem failure state")
		}
		return cephFSDaemonSessionReport{failed: true}, nil
	}
	if native.Address == nil || *native.Address == "" || native.Peers == nil || native.SnapDirs == nil || native.SnapDirs.Count == nil || *native.SnapDirs.Count < 0 {
		return cephFSDaemonSessionReport{}, cephFSObserveGuard("decode normal CephFS filesystem session")
	}
	peers, err := decodeCephFSPeers(native.Peers, true)
	if err != nil {
		return cephFSDaemonSessionReport{}, err
	}
	return cephFSDaemonSessionReport{address: *native.Address, peers: peers}, nil
}

// readWatchers lists the native watchers of the source filesystem's
// cephfs_mirror index object by RADOS address, through the source's
// control container.
func readWatchers(ctx context.Context, pair cephFSMirrorPair, operation string) (map[string]string, error) {
	control, err := pair.source.ClientContainer(ctx)
	if err != nil {
		return nil, cephFSObserveQuery("select CephFS control container", err)
	}
	if control == nil {
		return nil, cephFSObserveGuard("CephFS source control container is unavailable")
	}
	data, err := multicluster.Exec(ctx, control, "rados", "--pool", pair.metadataPool, "listwatchers", "cephfs_mirror")
	if err != nil {
		return nil, cephFSObserveQuery(operation, err)
	}
	watchers, err := parseCephFSMirrorWatchers(data)
	if err != nil {
		return nil, cephFSObserveGuard("decode CephFS filesystem watchers")
	}
	return watchers, nil
}

// cephFSObservedMapping is one directory's MGR assignment: its state and,
// when mapped, the filesystem watcher instance it is assigned to.
type cephFSObservedMapping struct{ State, InstanceID string }

// cephFSDirectoryMappingStates are the states `fs snapshot mirror dirmap`
// reports; v20.2.4 reports a transient UNASSOCIATED directory as null, read
// here as "unassociated".
var cephFSDirectoryMappingStates = []string{"mapped", "mapping", "unmapping", "shuffling", "resolving", "stalled"}

// readDirectoryMapping reads the MGR's `fs snapshot mirror dirmap` of
// directory. A directory the policy does not track is a query error that
// matches fs.ErrNotExist.
func readDirectoryMapping(ctx context.Context, pair cephFSMirrorPair, directory string) (cephFSObservedMapping, error) {
	data, err := pair.source.Ceph(ctx, "fs", "snapshot", "mirror", "dirmap", pair.sourceFilesystem, directory)
	if err != nil {
		return cephFSObservedMapping{}, cephFSObserveQuery("read CephFS directory mapping", err)
	}
	return decodeDirectoryMapping(data)
}

func decodeDirectoryMapping(data []byte) (cephFSObservedMapping, error) {
	var result cephFSObservedMapping
	var native struct {
		State      json.RawMessage `json:"state"`
		InstanceID *string         `json:"instance_id"`
	}
	if json.Unmarshal(data, &native) != nil || len(native.State) == 0 {
		return result, cephFSObserveGuard("decode CephFS directory mapping state")
	}
	if string(native.State) == "null" {
		result.State = "unassociated"
	} else if json.Unmarshal(native.State, &result.State) != nil || !slices.Contains(cephFSDirectoryMappingStates, result.State) {
		return result, cephFSObserveGuard("decode CephFS directory mapping state")
	}
	if native.InstanceID != nil {
		result.InstanceID = *native.InstanceID
	}
	if result.State == "mapped" {
		if id, err := strconv.ParseUint(result.InstanceID, 10, 64); err != nil || id == 0 {
			return result, cephFSObserveGuard("decode CephFS mapped filesystem watcher identity")
		}
	}
	return result, nil
}
