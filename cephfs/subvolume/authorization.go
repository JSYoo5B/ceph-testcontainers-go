package subvolume

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/volbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// AuthorizationConfig creates a fresh Cephx principal. Access must be "r" or
// "rw". Namespace-isolated subvolumes are required: a path cap alone cannot
// restrict direct RADOS access to the subvolume's file data.
type AuthorizationConfig struct {
	ClientID string
	Access   string
}

// Authorization describes an owned native volumes grant. Client provides
// credentials for ClientConfig.WithIdentity and independent Linux clients.
// Exported fields are descriptions; changing them cannot redirect mutations.
// Copies share grant/deauthorization state. The namespace cap covers the
// entire actual namespace, which may be shared by clones inheriting a source
// layout.
type Authorization struct {
	Client         *ceph.ClientConfig
	AuthID         string
	Access         string
	FilesystemName string
	SubvolumeName  string
	GroupName      string
	Path           string
	DataPool       string
	PoolNamespace  string
	identity       *authorizationRecord
}

// AuthorizedClient is one entry in native authorized_list. This is volumes
// metadata, not a complete audit of independently edited Cephx caps.
type AuthorizedClient struct {
	AuthID string
	Access string
}

type authorizationRecord struct {
	volume                                          *volumeRecord
	client                                          *ceph.ClientConfig
	poolID                                          int64
	dataPool, namespace, access                     string
	authorizeAttempted, authorized, deauthAttempted bool
	deauthorized                                    bool
	remainingCaps                                   map[string]string
}

// Authorize creates a fresh identity with an explicit fresh key, then
// asks MGR volumes to grant its exact native UUID path and pool/namespace. It
// refuses existing principals and non-isolated layouts. Native --allow_existing_id
// is used only for this immediately created, key-verified identity, never to
// adopt a user. MON visibility is restricted to this filesystem. This grant
// provides no layout, snapshot or root-squash administration capabilities.
//
// A non-nil result with an error may contain an owned partial creation. It is
// never silently adopted by a second Authorize call; inspect through Ceph or
// explicitly Authorization.Deauthorize when its Client creation was confirmed.
// External auth/layout edits must not race these operations.
//
//purpose:construction,handoff
func (volume *Subvolume) Authorize(ctx context.Context, config AuthorizationConfig) (*Authorization, error) {
	fs, err := volume.filesystem()
	if err != nil {
		return nil, err
	}
	entity, err := clientEntity(config.ClientID)
	if err != nil {
		return nil, err
	}
	if config.Access != "r" && config.Access != "rw" {
		return nil, errors.New("subvolume access must be r or rw")
	}
	if volume == nil {
		return nil, errors.New("subvolume is required")
	}
	ctx, fsID, done, err := volbase.Begin(ctx, fs)
	if err != nil {
		return nil, err
	}
	defer done()
	info, pool, err := authorizationTarget(ctx, fs, fsID, volume)
	if err != nil {
		return nil, err
	}
	// Fresh-key auth add prevents native get-or-create from silently reusing a
	// concurrently created external principal. Initial caps contain no file-data
	// access; volumes adds both exact MDS and OSD clauses after key verification.
	client, err := fs.Cluster().CreateClient(ctx, entity, ceph.ClientCaps{Mon: "allow r fsname=" + fs.Name()})
	if client == nil {
		return nil, err
	}
	identity := &authorizationRecord{volume: volume.identity, client: client, poolID: pool.ID,
		dataPool: info.DataPool, namespace: info.PoolNamespace, access: config.Access}
	grant := &Authorization{Client: client, AuthID: client.User(), Access: config.Access,
		FilesystemName: fs.Name(), SubvolumeName: volume.identity.Name, GroupName: volume.identity.Group,
		Path: volume.identity.Path, DataPool: info.DataPool, PoolNamespace: info.PoolNamespace, identity: identity}
	if err != nil {
		return grant, err
	}
	return grant, grantAuthorization(ctx, fs, volume, identity)
}

// authorizationTarget reads the native subvolume an authorization targets:
// it must be active, complete, the same incarnation and in an isolated pool
// namespace of a pool registered with this filesystem. The subvolume
// operation gate is held.
func authorizationTarget(ctx context.Context, fs *fsbase.Core, fsID int64, volume *Subvolume) (*fsbase.SubvolumeInfo, ceph.PoolState, error) {
	info, err := fs.Volumes().SubvolumeInfo(ctx, volume.identity.Name, volume.identity.Group)
	if err != nil {
		return nil, ceph.PoolState{}, err
	}
	if volume.identity.Removed || info.State != "complete" {
		return nil, ceph.PoolState{}, errors.New("subvolume must be active and complete")
	}
	if err := volume.identity.CheckIdentity(fsID, info.Path, info.CreatedAt); err != nil {
		return nil, ceph.PoolState{}, err
	}
	if info.PoolNamespace == "" {
		return nil, ceph.PoolState{}, errors.New("subvolume authorization requires a native isolated pool namespace")
	}
	state, err := fs.ReadNativePools(ctx)
	if err != nil {
		return nil, ceph.PoolState{}, err
	}
	pool, err := state.PoolByName(info.DataPool)
	if err != nil || !slices.Contains(state.DataPools, pool.ID) {
		return nil, ceph.PoolState{}, errors.New("subvolume layout pool is not registered with this filesystem")
	}
	return info, pool, nil
}

// grantAuthorization records the attempt and applies the native subvolume
// authorization to the fresh principal under the topology lock: the principal
// must have no file-data capabilities before, and exactly the expected MDS
// and OSD clauses with its key preserved after.
func grantAuthorization(ctx context.Context, fs *fsbase.Core, volume *Subvolume, identity *authorizationRecord) error {
	if err := servicehook.LockTopology(ctx, fs.Cluster()); err != nil {
		return err
	}
	defer servicehook.Unlock(fs.Cluster())
	before, err := authorizationClientCaps(ctx, fs.Volumes(), identity)
	if err != nil {
		return err
	}
	if before["mds"] != "" || before["osd"] != "" {
		return errors.New("fresh principal acquired file-data capabilities outside this request")
	}
	if volume.identity.authorizations == nil {
		volume.identity.authorizations = make(map[*authorizationRecord]struct{})
	}
	volume.identity.authorizations[identity] = struct{}{}
	identity.authorizeAttempted = true
	args := fsbase.AppendGroup([]string{"fs", "subvolume", "authorize", fs.Name(), volume.identity.Name, identity.client.User()}, volume.identity.Group)
	args = append(args, "--access_level", identity.access, "--allow_existing_id")
	if _, err := servicehook.AuthCommand(ctx, fs.Cluster(), "authorize owned subvolume client", args...); err != nil {
		return err
	}
	current, err := authorizationClientCaps(ctx, fs.Volumes(), identity)
	if err != nil {
		return err
	}
	expected := maps.Clone(before)
	expected["mds"], expected["osd"] = identity.nativeClauses()
	if !equalAuthorizationCaps(current, expected) {
		return errors.New("native subvolume authorization did not preserve the key and expected capability clauses")
	}
	if err := checkNativeAuthorization(ctx, fs, identity, true); err != nil {
		return err
	}
	identity.authorized = true
	return nil
}

func decodeAuthorizedClients(data []byte) ([]AuthorizedClient, error) {
	var raw []map[string]string
	if err := json.Unmarshal(data, &raw); err != nil || raw == nil {
		return nil, errors.New("decode native subvolume authorized clients")
	}
	result := make([]AuthorizedClient, 0, len(raw))
	seen := make(map[string]bool)
	for _, entry := range raw {
		if len(entry) != 1 {
			return nil, errors.New("invalid native subvolume authorization entry")
		}
		for id, access := range entry {
			// Native keys are raw auth IDs. A raw ID may itself start with
			// "client."; normalizing it as a qualified entity would lose that
			// prefix and reject an otherwise valid native principal.
			if !naming.ClientID.MatchString(id) || seen[id] || (access != "r" && access != "rw") {
				return nil, errors.New("invalid or duplicate native subvolume authorized client")
			}
			seen[id] = true
			result = append(result, AuthorizedClient{AuthID: id, Access: access})
		}
	}
	slices.SortFunc(result, func(a, b AuthorizedClient) int { return strings.Compare(a.AuthID, b.AuthID) })
	return result, nil
}

func nativeAuthorizedClients(ctx context.Context, v fsbase.VolumeCommands, name, group string) ([]AuthorizedClient, error) {
	args := fsbase.AppendGroup([]string{"fs", "subvolume", "authorized_list", v.Name(), name}, group)
	data, err := v.Cluster().Ceph(ctx, args...)
	if err != nil {
		return nil, err
	}
	return decodeAuthorizedClients(data)
}

// AuthorizedClients reads the native volumes authorization metadata of the
// subvolume name in group of fs, including principals created through raw
// Ceph. It grants no mutation ownership.
//
//purpose:observation
func AuthorizedClients(ctx context.Context, fs *cephfs.Filesystem, name, group string) ([]AuthorizedClient, error) {
	return authorizedClients(ctx, coreOf(fs), name, group)
}

func authorizedClients(ctx context.Context, fs *fsbase.Core, name, group string) ([]AuthorizedClient, error) {
	if err := validateName(name, false); err != nil {
		return nil, err
	}
	if err := validateName(group, true); err != nil {
		return nil, err
	}
	ctx, _, done, err := volbase.Begin(ctx, fs)
	if err != nil {
		return nil, err
	}
	defer done()
	return nativeAuthorizedClients(ctx, fs.Volumes(), name, group)
}

func (identity *authorizationRecord) nativeClauses() (string, string) {
	return "allow " + identity.access + " path=" + identity.volume.Path,
		"allow " + identity.access + " pool=" + identity.dataPool + " namespace=" + identity.namespace
}

// authorizationClientCaps must be called with cluster.mu held. It also supports
// a confirmed own deletion when reconciling a lost native deauthorize reply.
func authorizationClientCaps(ctx context.Context, v fsbase.VolumeCommands, identity *authorizationRecord) (map[string]string, error) {
	c, client := v.Cluster(), identity.client
	if servicehook.Closed(c) || client == nil || !servicehook.ClientConfirmed(client, c) {
		return nil, errors.New("subvolume principal must be a confirmed identity created by this cluster")
	}
	// A key replacement or capability change that may still apply would
	// race the subvolume grants.
	if err := servicehook.SettleClient(ctx, c, client); err != nil {
		return nil, err
	}
	data, err := servicehook.AuthCommand(ctx, c, "inspect owned subvolume principal", "auth", "ls", "--format", "json")
	if err != nil {
		return nil, err
	}
	var listing struct {
		Entries []struct {
			Entity string            `json:"entity"`
			Key    string            `json:"key"`
			Caps   map[string]string `json:"caps"`
		} `json:"auth_dump"`
	}
	if err := json.Unmarshal(data, &listing); err != nil || listing.Entries == nil {
		return nil, errors.New("decode subvolume principal auth database")
	}
	var result map[string]string
	for _, entry := range listing.Entries {
		if entry.Entity != client.Name() {
			continue
		}
		if result != nil || entry.Key == "" || entry.Caps == nil || entry.Key != clusterutil.KeyringKey(servicehook.ClientKeyring(client), client.Name()) {
			return nil, errors.New("subvolume principal was duplicated or its key was replaced")
		}
		result = maps.Clone(entry.Caps)
	}
	if result == nil && !servicehook.ClientRevoked(client) && !(identity.deauthAttempted && len(identity.remainingCaps) == 0) {
		return nil, errors.New("subvolume principal disappeared outside this fixture")
	}
	return result, nil
}

func checkAuthorizationVolume(ctx context.Context, fs *fsbase.Core, fsID int64, identity *authorizationRecord) error {
	if err := validateVolumeHandle(fs, identity.volume); err != nil {
		return err
	}
	if identity.volume.Removed {
		return errors.New("authorized subvolume was removed")
	}
	info, err := fs.Volumes().SubvolumeInfo(ctx, identity.volume.Name, identity.volume.Group)
	if err != nil {
		return err
	}
	if err := identity.volume.CheckIdentity(fsID, info.Path, info.CreatedAt); err != nil {
		return err
	}
	if info.State != "complete" || info.DataPool != identity.dataPool || info.PoolNamespace != identity.namespace {
		return errors.New("authorized subvolume state or native pool/namespace layout changed")
	}
	state, err := fs.ReadNativePools(ctx)
	if err != nil {
		return err
	}
	pool, err := state.PoolByID(identity.poolID)
	if err != nil || pool.Name != identity.dataPool || !slices.Contains(state.DataPools, identity.poolID) {
		return errors.New("authorized subvolume pool was replaced or detached")
	}
	return nil
}

func checkNativeAuthorization(ctx context.Context, fs *fsbase.Core, identity *authorizationRecord, present bool) error {
	actual, err := nativeAuthorizationPresent(ctx, fs, identity)
	if err != nil {
		return err
	}
	if actual != present {
		return errors.New("native authorized_list does not match the owned grant state")
	}
	return nil
}

func nativeAuthorizationPresent(ctx context.Context, fs *fsbase.Core, identity *authorizationRecord) (bool, error) {
	listing, err := nativeAuthorizedClients(ctx, fs.Volumes(), identity.volume.Name, identity.volume.Group)
	if err != nil {
		return false, err
	}
	for _, entry := range listing {
		if entry.AuthID != identity.client.User() {
			continue
		}
		if entry.Access == identity.access {
			return true, nil
		}
		return false, errors.New("native subvolume authorization differs from the owned grant")
	}
	return false, nil
}

func authorizationCapTokens(cap string) []string {
	if cap == "" {
		return nil
	}
	result := strings.Split(cap, ",")
	for i := range result {
		result[i] = strings.TrimSpace(result[i])
	}
	slices.Sort(result)
	return result
}

func equalAuthorizationCaps(a, b map[string]string) bool {
	for service, cap := range a {
		if !slices.Equal(authorizationCapTokens(cap), authorizationCapTokens(b[service])) {
			return false
		}
	}
	for service, cap := range b {
		if !slices.Equal(authorizationCapTokens(cap), authorizationCapTokens(a[service])) {
			return false
		}
	}
	return true
}

func (identity *authorizationRecord) withoutOwnedClauses(caps map[string]string, require bool) (map[string]string, error) {
	result := maps.Clone(caps)
	if result == nil {
		result = make(map[string]string)
	}
	mds, osd := identity.nativeClauses()
	matched := make(map[string]int)
	for _, entry := range []struct{ service, clause string }{{"mds", mds}, {"osd", osd}} {
		tokens := authorizationCapTokens(result[entry.service])
		matches := 0
		for _, token := range tokens {
			if token == entry.clause {
				matches++
			}
		}
		if matches > 1 || (require && matches != 1) {
			return nil, errors.New("owned native capability clause was replaced or duplicated")
		}
		matched[entry.service] = matches
		otherAccess := "r"
		if identity.access == "r" {
			otherAccess = "rw"
		}
		otherClause := strings.Replace(entry.clause, "allow "+identity.access+" ", "allow "+otherAccess+" ", 1)
		if slices.Contains(tokens, otherClause) {
			return nil, errors.New("owned native capability access level was changed")
		}
		tokens = slices.DeleteFunc(tokens, func(token string) bool { return token == entry.clause })
		if len(tokens) == 0 {
			delete(result, entry.service)
		} else {
			result[entry.service] = strings.Join(tokens, ", ")
		}
	}
	if matched["mds"] != matched["osd"] || (!require && matched["mds"] == 0 && (caps["mds"] != "" || caps["osd"] != "")) {
		return nil, errors.New("partial authorization has unconfirmed or inconsistent file-data capabilities")
	}
	// Native access.py drops only its generic MON allow-r when no MDS/OSD
	// rights remain. Our initial fsname-scoped MON grant retains the key.
	if result["mds"] == "" && result["osd"] == "" && result["mon"] == "allow r" {
		delete(result, "mon")
	}
	return result, nil
}

// Deauthorize removes only this native path/pool/namespace grant and
// volumes metadata. It verifies the original volume, layout and principal key;
// unrelated capabilities and the key are preserved unless native Ceph deletes
// an identity with no remaining rights. It does not revoke overlapping grants
// added separately, remove files or evict established sessions. Use a fresh
// client to observe changed permissions, then Authorization.EvictClients when needed.
// A lost reply keeps retry state; native metadata/caps are reconciled before
// repeating the command. Outside edits after an uncertain result are surfaced
// without restoring or overwriting them.
//
//purpose:condition,lifetime
func (grant *Authorization) Deauthorize(ctx context.Context) error {
	fs, err := grant.filesystem()
	if err != nil {
		return err
	}
	ctx, fsID, done, err := volbase.Begin(ctx, fs)
	if err != nil {
		return err
	}
	defer done()
	identity := grant.identity
	if identity.deauthorized {
		return nil
	}
	if err := checkAuthorizationVolume(ctx, fs, fsID, identity); err != nil {
		return err
	}
	if err := servicehook.LockTopology(ctx, fs.Cluster()); err != nil {
		return err
	}
	defer servicehook.Unlock(fs.Cluster())
	current, err := authorizationClientCaps(ctx, fs.Volumes(), identity)
	if err != nil {
		return err
	}
	present, err := nativeAuthorizationPresent(ctx, fs, identity)
	if err != nil {
		return err
	}
	if identity.deauthAttempted && equalAuthorizationCaps(current, identity.remainingCaps) && !present {
		identity.deauthorized = true
		servicehook.SetClientRevoked(identity.client, current == nil)
		return nil
	}
	if identity.authorized && !identity.deauthAttempted && !present {
		return errors.New("owned grant disappeared outside this fixture")
	}
	remaining, err := identity.withoutOwnedClauses(current, identity.authorized || present)
	if err != nil {
		return err
	}
	identity.remainingCaps, identity.deauthAttempted = remaining, true
	args := fsbase.AppendGroup([]string{"fs", "subvolume", "deauthorize", fs.Name(), identity.volume.Name, identity.client.User()}, identity.volume.Group)
	if _, err := servicehook.AuthCommand(ctx, fs.Cluster(), "deauthorize owned subvolume client", args...); err != nil {
		return err
	}
	current, err = authorizationClientCaps(ctx, fs.Volumes(), identity)
	if err != nil {
		return err
	}
	if !equalAuthorizationCaps(current, remaining) {
		return errors.New("native deauthorization result did not preserve unrelated capabilities")
	}
	if err := checkNativeAuthorization(ctx, fs, identity, false); err != nil {
		return err
	}
	identity.deauthorized = true
	servicehook.SetClientRevoked(identity.client, current == nil)
	return nil
}

// EvictClients evicts existing sessions with this principal and the
// exact original subvolume mount root. Deauthorization must have completed
// first; independently mounted roots and future connections permitted by other
// grants are outside this native operation's scope. Eviction may blocklist the
// client's connection address. A native error may represent partial eviction
// across MDS ranks; a retry repeats the same owned scope.
//
// To remove the principal itself afterwards, call grant.Client.Delete.
//
//purpose:condition
func (grant *Authorization) EvictClients(ctx context.Context) error {
	fs, err := grant.filesystem()
	if err != nil {
		return err
	}
	ctx, fsID, done, err := volbase.Begin(ctx, fs)
	if err != nil {
		return err
	}
	defer done()
	identity := grant.identity
	if !identity.deauthorized {
		return errors.New("deauthorize the owned subvolume grant before evicting sessions")
	}
	if err := checkAuthorizationVolume(ctx, fs, fsID, identity); err != nil {
		return err
	}
	if err := servicehook.LockTopology(ctx, fs.Cluster()); err != nil {
		return err
	}
	defer servicehook.Unlock(fs.Cluster())
	if _, err := authorizationClientCaps(ctx, fs.Volumes(), identity); err != nil {
		return err
	}
	if err := checkNativeAuthorization(ctx, fs, identity, false); err != nil {
		return err
	}
	args := fsbase.AppendGroup([]string{"fs", "subvolume", "evict", fs.Name(), identity.volume.Name, identity.client.User()}, identity.volume.Group)
	_, err = servicehook.AuthCommand(ctx, fs.Cluster(), fmt.Sprintf("evict owned %s sessions at subvolume root", identity.client.Name()), args...)
	return err
}

func clientEntity(name string) (string, error) {
	id := strings.TrimPrefix(name, "client.")
	if !naming.ClientID.MatchString(id) {
		return "", errors.New("client name must be an ID or client.ID using letters, digits, dots, underscores or hyphens")
	}
	return "client." + id, nil
}
