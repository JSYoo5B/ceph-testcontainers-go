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
	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// RefreshMonitorConfig updates only the source-local mon_host in every current
// owned daemon's ceph.conf, including stopped daemons. Other configuration and
// keyrings are retained. It does not restart processes, update remote peer
// bootstrap metadata or change directory assignments. Successful copies remain
// on partial failure; retry with a fresh context. External file writers must
// not race this operation.
func (mirror *CephFSMirror) RefreshMonitorConfig(ctx context.Context) error {
	return mirror.refreshMonitorConfig(ctx, func(ctx context.Context, client testcontainers.Container) error {
		return mirror.source.RefreshClientMonitorConfig(ctx, client)
	})
}

func (mirror *CephFSMirror) refreshMonitorConfig(ctx context.Context, refresh func(context.Context, testcontainers.Container) error) error {
	if mirror == nil {
		return cephFSObserveGuard("CephFS mirror fixture is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if err := lockRGWSyncObservation(ctx, &mirror.mu); err != nil {
		return err
	}
	defer mirror.mu.Unlock()
	if err := mirror.confirmCephFSRefreshHandle(true); err != nil {
		return err
	}
	if err := mirror.checkCephFSObservedIdentities(ctx); err != nil {
		return err
	}
	var errs []error
	seen := make(map[string]bool)
	for _, daemon := range mirror.daemons {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if daemon == nil {
			errs = append(errs, cephFSObserveGuard("owned CephFS mirror daemon is unavailable"))
			continue
		}
		if err := lockRGWSyncObservation(ctx, &daemon.mu); err != nil {
			errs = append(errs, err)
			break
		}
		if !daemon.removed {
			if daemon.Container == nil || daemon.GetContainerID() == "" {
				errs = append(errs, cephFSObserveGuard("owned CephFS mirror container identity is unavailable"))
			} else if id := daemon.GetContainerID(); !seen[id] {
				seen[id] = true
				if err := refresh(ctx, daemon); err != nil {
					errs = append(errs, fmt.Errorf("refresh CephFS mirror daemon %s: %w", daemon.DaemonName, err))
				}
			}
		}
		daemon.mu.Unlock()
	}
	if err := mirror.checkCephFSObservedIdentities(ctx); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(append(errs, ctx.Err())...)
}

// RefreshPeerMonitorConfig updates only mon_host in this fixture's existing
// peer config-key. It retains the peer UUID, credentials, destination FSID and
// all other fields. Existing native replayers read these values at initialization;
// callers stop and cold-start their daemons when they need the new bootstrap.
// This method does not mutate process state, auth, peer membership or snapshots.
// A failed write can have applied: successful changes remain and retries skip
// a config-key already holding the current addresses. External config-key writers
// must not race this operation; Ceph provides no value compare-and-swap here.
func (mirror *CephFSMirror) RefreshPeerMonitorConfig(ctx context.Context) error {
	return mirror.refreshPeerMonitorConfig(ctx, func(ctx context.Context) (string, error) {
		return mirror.destination.MonitorBootstrapAddresses(ctx)
	})
}

func (mirror *CephFSMirror) refreshPeerMonitorConfig(ctx context.Context, addresses func(context.Context) (string, error)) error {
	if mirror == nil {
		return cephFSObserveGuard("CephFS mirror fixture is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if err := lockRGWSyncObservation(ctx, &mirror.mu); err != nil {
		return err
	}
	defer mirror.mu.Unlock()
	if err := mirror.confirmCephFSRefreshHandle(true); err != nil {
		return err
	}
	if err := mirror.checkCephFSObservedIdentities(ctx); err != nil {
		return err
	}
	identity, err := mirror.readCephFSRemoteMonitorIdentity(ctx, addresses)
	if err != nil {
		return err
	}
	key := "cephfs/mirror/peer/" + mirror.SourceFilesystem + "/" + mirror.peerID
	data, err := mirror.source.Ceph(ctx, "config-key", "get", key)
	if err != nil {
		return cephFSObserveQuery("read owned CephFS peer bootstrap configuration", err)
	}
	fields, err := decodeCephFSBootstrapFields(data, identity.FSID, false)
	if err != nil {
		return err
	}
	var oldHosts string
	_ = json.Unmarshal(fields["mon_host"], &oldHosts)
	var writeErr error
	if oldHosts != identity.Addresses {
		fields["mon_host"], _ = json.Marshal(identity.Addresses)
		updated, _ := json.Marshal(fields)
		if err := mirror.checkCephFSObservedIdentities(ctx); err != nil {
			return err
		}
		writeErr = writeCephFSOwnedPeerConfig(ctx, mirror.source, key, updated)
		if err := ctx.Err(); err != nil {
			return errors.Join(writeErr, err)
		}
		after, err := mirror.source.Ceph(ctx, "config-key", "get", key)
		if err != nil {
			return errors.Join(writeErr, cephFSObserveQuery("confirm owned CephFS peer bootstrap configuration", err))
		}
		confirmed, err := decodeCephFSBootstrapFields(after, identity.FSID, false)
		if err != nil {
			return errors.Join(writeErr, err)
		}
		if !cephFSBootstrapFieldsEqual(fields, confirmed) {
			return errors.Join(writeErr, cephFSObserveGuard("CephFS peer bootstrap configuration changed during refresh"))
		}
	}
	if err := mirror.checkCephFSObservedIdentities(ctx); err != nil {
		return errors.Join(writeErr, err)
	}
	after, err := mirror.readCephFSRemoteMonitorIdentity(ctx, addresses)
	if err != nil {
		return errors.Join(writeErr, err)
	}
	if after != identity {
		return errors.Join(writeErr, cephFSObserveGuard("CephFS destination monitor identity changed during refresh"))
	}
	return errors.Join(writeErr, ctx.Err())
}

// The bootstrap contains an auth key. Keep its payload out of process argv,
// command hooks and native CLI error strings, including after cancellation.
func writeCephFSOwnedPeerConfig(ctx context.Context, source *ceph.Container, key string, data []byte) (returnErr error) {
	control := source.ControlContainer()
	if control == nil {
		return cephFSObserveGuard("CephFS source control container is unavailable")
	}
	file := "/tmp/tc-cephfs-peer-refresh-" + uuid.NewString() + ".json"
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := exec(cleanupCtx, control, "rm", "-f", "--", file); err != nil {
			returnErr = errors.Join(returnErr, cephFSObserveQuery("remove CephFS peer bootstrap temporary file", err))
		}
	}()
	if err := control.CopyToContainer(ctx, data, file, 0o600); err != nil {
		return cephFSObserveQuery("copy CephFS peer bootstrap temporary file", err)
	}
	if _, err := source.Ceph(ctx, "config-key", "set", key, "-i", file); err != nil {
		return cephFSObserveQuery("write owned CephFS peer bootstrap configuration", err)
	}
	return ctx.Err()
}

func (mirror *CephFSMirror) confirmCephFSRefreshHandle(requirePeer bool) error {
	if mirror.closed || mirror.source == nil || mirror.destination == nil || mirror.filesystemID <= 0 || mirror.destinationFilesystemID <= 0 {
		return cephFSObserveGuard("CephFS mirror original filesystem identities are unavailable")
	}
	if requirePeer && (mirror.peerID == "" || mirror.pendingPeerImport != nil) {
		return cephFSObserveGuard("CephFS mirror owned peer identity is unavailable")
	}
	return nil
}

func (mirror *CephFSMirror) checkCephFSRefreshFilesystems(ctx context.Context) error {
	for _, site := range []struct {
		name   string
		id     int
		poolID int64
		read   func(context.Context, ...string) ([]byte, error)
	}{
		{mirror.SourceFilesystem, mirror.filesystemID, mirror.metadataPoolID, mirror.source.Ceph},
		{mirror.DestinationFilesystem, mirror.destinationFilesystemID, mirror.destinationMetadataPoolID, mirror.destination.Ceph},
	} {
		data, err := site.read(ctx, "fs", "get", site.name, "--format", "json")
		if err != nil {
			return cephFSObserveQuery("read original CephFS bootstrap filesystem", err)
		}
		var current struct {
			ID     *int `json:"id"`
			MDSMap *struct {
				MetadataPool *int64 `json:"metadata_pool"`
			} `json:"mdsmap"`
		}
		if json.Unmarshal(data, &current) != nil || current.ID == nil || *current.ID != site.id || current.MDSMap == nil || current.MDSMap.MetadataPool == nil || *current.MDSMap.MetadataPool != site.poolID {
			return cephFSObserveGuard("CephFS original bootstrap filesystem or metadata pool identity differs")
		}
	}
	return ctx.Err()
}

type cephFSRemoteMonitorIdentity struct{ FSID, Addresses string }

func (mirror *CephFSMirror) readCephFSRemoteMonitorIdentity(ctx context.Context, addresses func(context.Context) (string, error)) (cephFSRemoteMonitorIdentity, error) {
	var result cephFSRemoteMonitorIdentity
	current, err := addresses(ctx)
	if err != nil {
		return result, cephFSObserveQuery("read original CephFS destination monitor bootstrap", err)
	}
	if current == "" || strings.ContainsAny(current, "\x00\r\n") {
		return result, cephFSObserveGuard("CephFS destination monitor bootstrap is unavailable")
	}
	data, err := mirror.destination.Ceph(ctx, "fsid")
	if err != nil {
		return result, cephFSObserveQuery("read CephFS destination cluster identity", err)
	}
	fsid := strings.TrimSpace(string(data))
	parsed, err := uuid.Parse(fsid)
	if err != nil || parsed.String() != fsid {
		return result, cephFSObserveGuard("decode CephFS destination cluster identity")
	}
	return cephFSRemoteMonitorIdentity{FSID: fsid, Addresses: current}, ctx.Err()
}

// Called under mirror.mu after original filesystem identities have been captured.
// The MGR's warm rados client can report its old mon_host even after all MONs roll.
func (mirror *CephFSMirror) normalizeCephFSPeerBootstrap(ctx context.Context, token string) (string, error) {
	if err := mirror.confirmCephFSRefreshHandle(false); err != nil {
		return "", err
	}
	if err := mirror.checkCephFSRefreshFilesystems(ctx); err != nil {
		return "", err
	}
	identity, err := mirror.readCephFSRemoteMonitorIdentity(ctx, mirror.destination.MonitorBootstrapAddresses)
	if err != nil {
		return "", err
	}
	updated, err := normalizeCephFSBootstrapToken(token, cephFSPeerIdentity{
		ClientName: mirror.DestinationClientEntity, SiteName: mirror.destinationSite, FilesystemName: mirror.DestinationFilesystem,
	}, identity)
	if err != nil {
		return "", err
	}
	if err := mirror.checkCephFSRefreshFilesystems(ctx); err != nil {
		return "", err
	}
	return updated, ctx.Err()
}

func normalizeCephFSBootstrapToken(token string, peer cephFSPeerIdentity, identity cephFSRemoteMonitorIdentity) (string, error) {
	if len(token) > 1<<20 {
		return "", cephFSObserveGuard("CephFS bootstrap token is oversized")
	}
	data, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return "", cephFSObserveGuard("decode CephFS destination bootstrap token")
	}
	fields, err := decodeCephFSBootstrapFields(data, identity.FSID, true)
	if err != nil {
		return "", err
	}
	for key, expected := range map[string]string{"user": peer.ClientName, "site_name": peer.SiteName, "filesystem": peer.FilesystemName} {
		var actual string
		_ = json.Unmarshal(fields[key], &actual)
		if expected == "" || actual != expected {
			return "", cephFSObserveGuard("CephFS bootstrap token destination differs")
		}
	}
	if identity.Addresses == "" || strings.ContainsAny(identity.Addresses, "\x00\r\n") {
		return "", cephFSObserveGuard("CephFS destination monitor bootstrap is unavailable")
	}
	fields["mon_host"], _ = json.Marshal(identity.Addresses)
	updated, _ := json.Marshal(fields)
	return base64.StdEncoding.EncodeToString(updated), nil
}

func decodeCephFSBootstrapFields(data []byte, expectedFSID string, token bool) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if len(data) > 1<<20 || json.Unmarshal(data, &fields) != nil || fields == nil {
		return nil, cephFSObserveGuard("decode CephFS peer bootstrap configuration object")
	}
	required := []string{"fsid", "key", "mon_host"}
	if token {
		required = append(required, "user", "site_name", "filesystem")
	}
	for _, name := range required {
		var value *string
		if json.Unmarshal(fields[name], &value) != nil || value == nil || *value == "" {
			return nil, cephFSObserveGuard("decode CephFS peer bootstrap identity fields")
		}
	}
	var fsid string
	_ = json.Unmarshal(fields["fsid"], &fsid)
	parsed, err := uuid.Parse(fsid)
	if err != nil || parsed.String() != fsid || fsid != expectedFSID {
		return nil, cephFSObserveGuard("CephFS peer bootstrap cluster identity differs")
	}
	return fields, nil
}

func cephFSBootstrapFieldsEqual(a, b map[string]json.RawMessage) bool {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return bytes.Equal(left, right)
}
