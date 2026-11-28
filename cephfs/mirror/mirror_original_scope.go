package mirror

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/google/uuid"
)

// cephFSOriginalScope is the original identity that a directory addition, a
// directory removal and a peer removal compare every later observation with:
// a copy of the fixture's pair, the owned peer and its generation, and the
// two cluster FSIDs. It records no daemon, witness or progress, so a
// directory addition captures it without a live daemon. Its methods run while
// Container.mu is held.
type cephFSOriginalScope struct {
	mirror       *Container
	pair         cephFSMirrorPair
	peerID       string
	generation   uint64
	clusters     [2]string
	readClusters func(context.Context) ([2]string, error)
}

// captureOriginalScope records the fixture's current identity for peerID
// after its native cluster identities are read and found distinct. operation
// names the receipt in the refusals. Container.mu is held.
func (mirror *Container) captureOriginalScope(ctx context.Context, operation, peerID string, readClusters func(context.Context) ([2]string, error)) (*cephFSOriginalScope, error) {
	pair := mirror.pair
	if !mirror.describes(pair) || pair.sourceFilesystem == "" || pair.destinationFilesystem == "" || !pair.identityConfirmed || pair.sourceMetadataPoolID < 0 || pair.destinationMetadataPoolID < 0 || pair.sourceClient == "" || pair.metadataPool == "" {
		return nil, cephFSObserveGuard(operation + " original filesystem metadata identity is unavailable")
	}
	if readClusters == nil {
		return nil, cephFSObserveGuard(operation + " original cluster identity reader is unavailable")
	}
	clusters, err := readClusters(ctx)
	if err != nil {
		return nil, err
	}
	if !validCephFSOriginalClusters(clusters) {
		return nil, cephFSObserveGuard(operation + " original cluster identities are unavailable or equal")
	}
	return &cephFSOriginalScope{mirror: mirror, pair: pair, peerID: peerID, generation: mirror.ops.peerGeneration, clusters: clusters, readClusters: readClusters}, nil
}

// usable reports whether the scope still has its fixture, both clusters and
// its cluster identity reader.
func (s *cephFSOriginalScope) usable() bool {
	return s != nil && s.mirror != nil && s.pair.source != nil && s.pair.destination != nil && s.readClusters != nil
}

// sameFixture reports whether the fixture still has the recorded pair and
// peer, and whether its exported descriptors still name that pair. A peer
// removal passes allowClearedPeer, because its own request clears the
// fixture's peer. Container.mu is held.
func (s *cephFSOriginalScope) sameFixture(allowClearedPeer bool) bool {
	m := s.mirror
	peer := m.peer.id == s.peerID || (allowClearedPeer && m.peer.id == "")
	return peer && m.pair == s.pair && m.describes(s.pair)
}

// directoryPolicy reads the source filesystem's native directory policy with
// the strict ls decoder. operation names the receipt in a query error.
func (s *cephFSOriginalScope) directoryPolicy(ctx context.Context, operation string) (map[string]bool, error) {
	data, err := s.pair.source.Ceph(ctx, "fs", "snapshot", "mirror", "ls", s.pair.sourceFilesystem)
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
	watchers, err := readWatchers(ctx, s.pair, "read original CephFS filesystem watchers")
	if err != nil {
		return nil, err
	}
	return watchers, ctx.Err()
}

// session reads daemon's session of the original source filesystem and
// checks its peer against the original peer identity. A failed session is a
// retryable query error.
func (s *cephFSOriginalScope) session(ctx context.Context, daemon *Daemon) (cephFSDaemonSession, error) {
	report, err := readDaemonSession(ctx, daemon, s.pair, "read original CephFS filesystem session")
	if err != nil {
		return cephFSDaemonSession{}, err
	}
	if report.failed {
		return cephFSDaemonSession{}, cephFSObserveQuery("original CephFS filesystem session is unavailable", nil)
	}
	present, err := checkCephFSOriginalPeer(report.peers, s.peerID, s.pair.peerIdentity())
	return cephFSDaemonSession{address: report.address, peerPresent: present}, err
}

// catalog reports whether daemon's admin socket still lists the original
// peer's status command.
func (s *cephFSOriginalScope) catalog(ctx context.Context, daemon *Daemon) (bool, error) {
	data, err := adminSocket(ctx, daemon, "help")
	if err != nil {
		return false, cephFSObserveQuery("read original CephFS command catalog", err)
	}
	return decodeCephFSOriginalCatalog(data, "fs mirror status "+s.pair.sessionName(), "fs mirror peer status "+s.pair.sessionName()+" "+s.peerID)
}

func decodeCephFSOriginalCatalog(data []byte, fsCommand, peerCommand string) (bool, error) {
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

// checkCephFSOriginalPeer reports whether peers holds the original peer id
// with the expected destination. No peer is absence; any other peer, or the
// original peer with another destination, is a guard error.
func checkCephFSOriginalPeer(peers map[string]cephFSPeerIdentity, id string, expected cephFSPeerIdentity) (bool, error) {
	if len(peers) == 0 {
		return false, nil
	}
	observed, exists := peers[id]
	if len(peers) != 1 || !exists {
		return false, cephFSObserveGuard("CephFS has a substituted peer UUID")
	}
	if observed != expected {
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
	var fsPeers map[string]cephFSPeerIdentity
	for _, site := range []struct {
		cluster *ceph.Container
		name    string
		id      int
		pool    int64
		source  bool
	}{{s.pair.source, s.pair.sourceFilesystem, s.pair.sourceFilesystemID, s.pair.sourceMetadataPoolID, true}, {s.pair.destination, s.pair.destinationFilesystem, s.pair.destinationFilesystemID, s.pair.destinationMetadataPoolID, false}} {
		identity, err := readFilesystemIdentity(ctx, site.cluster, site.name, "read original CephFS filesystem identity")
		if err != nil && !errors.Is(err, errCephFSObservationGuard) {
			return false, false, err
		}
		if err != nil || identity.id != site.id || identity.metadataPool != site.pool {
			return false, false, errors.Join(cephFSObserveGuard("CephFS original filesystem or metadata pool differs"), err)
		}
		if site.source {
			if identity.mirrorPeers == nil {
				return false, false, cephFSObserveGuard("CephFS source mirroring policy is unavailable")
			}
			fsPeers = identity.mirrorPeers
		}
	}
	fsPresent, err := checkCephFSOriginalPeer(fsPeers, s.peerID, s.pair.peerIdentity())
	if err != nil {
		return false, false, err
	}
	peers, err := readPeerList(ctx, s.pair, "read original CephFS manager policy")
	if err != nil {
		return false, false, err
	}
	present, err := checkCephFSOriginalPeer(peers, s.peerID, s.pair.peerIdentity())
	return present, fsPresent, errors.Join(err, ctx.Err())
}

func validCephFSOriginalClusters(values [2]string) bool {
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

func (mirror *Container) originalClusterIdentities(ctx context.Context) ([2]string, error) {
	var result [2]string
	for i, site := range []struct {
		cluster *ceph.Container
		name    string
	}{{mirror.pair.source, "source"}, {mirror.pair.destination, "destination"}} {
		cluster := site.cluster
		if cluster == nil {
			return result, cephFSObserveGuard("CephFS original cluster is unavailable")
		}
		observer, err := ceph.Observe(cluster)
		if err != nil {
			return result, err
		}
		original, err := readCephFSOriginalClusterIdentity(ctx, site.name, observer.MonitorBootstrapAddresses,
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
func readCephFSOriginalClusterIdentity(ctx context.Context, site string, bootstrap, quorum, native func(context.Context) (string, error)) (string, error) {
	validate := func(phase string) error {
		started := time.Now()
		addresses, err := bootstrap(ctx)
		if err != nil {
			operation := fmt.Sprintf("validate original CephFS %s monitor bootstrap %s FSID readback (elapsed %s)", site, phase, time.Since(started).Round(time.Millisecond))
			cause := cephFSObserveQuery(operation, err)
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
	if err := validate("before"); err != nil {
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
	if err := validate("after"); err != nil {
		return "", err
	}
	return original, ctx.Err()
}
