package cephfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// SubvolumeAuthorizationConfig creates a fresh Cephx principal. Access
// must be "r" or "rw". Namespace-isolated subvolumes are required: a path cap
// alone cannot restrict direct RADOS access to the subvolume's file data.
type SubvolumeAuthorizationConfig struct {
	ClientID string
	Access   string
}

// SubvolumeAuthorization describes an owned native volumes grant. Client
// provides credentials for ClientConfig.WithIdentity and independent Linux clients.
// Exported fields are descriptions; changing them cannot redirect mutations.
// Copies share grant/deauthorization state. The namespace cap covers the entire
// actual namespace, which may be shared by clones inheriting a source layout.
type SubvolumeAuthorization struct {
	Client         *ceph.ClientConfig
	AuthID         string
	Access         string
	FilesystemName string
	SubvolumeName  string
	GroupName      string
	Path           string
	DataPool       string
	PoolNamespace  string
	identity       *cephFSAuthorizationIdentity
}

// SubvolumeAuthorizedClient is one entry in native authorized_list. This
// is volumes metadata, not a complete audit of independently edited Cephx caps.
type SubvolumeAuthorizedClient struct {
	AuthID string
	Access string
}

type cephFSAuthorizationIdentity struct {
	volume                                          *cephFSVolumeIdentity
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
// explicitly SubvolumeAuthorization.Deauthorize when its Client creation was confirmed.
// External auth/layout edits must not race these operations.
func (volume *Subvolume) Authorize(ctx context.Context, config SubvolumeAuthorizationConfig) (*SubvolumeAuthorization, error) {
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
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	info, err := fs.subvolumeInfo(ctx, volume.identity.name, volume.identity.group)
	if err != nil {
		return nil, err
	}
	if volume.identity.removed || info.State != "complete" {
		return nil, errors.New("subvolume must be active and complete")
	}
	if err := fs.checkVolumeIdentity(fsID, volume.identity, info.Path, info.CreatedAt); err != nil {
		return nil, err
	}
	if info.PoolNamespace == "" {
		return nil, errors.New("subvolume authorization requires a native isolated pool namespace")
	}
	state, err := fs.readNativePools(ctx)
	if err != nil {
		return nil, err
	}
	pool, err := state.poolByName(info.DataPool)
	if err != nil || !slices.Contains(state.dataPools, pool.ID) {
		return nil, errors.New("subvolume layout pool is not registered with this filesystem")
	}
	// Fresh-key auth add prevents native get-or-create from silently reusing a
	// concurrently created external principal. Initial caps contain no file-data
	// access; volumes adds both exact MDS and OSD clauses after key verification.
	client, err := fs.cluster.CreateClient(ctx, entity, ceph.ClientCaps{Mon: "allow r fsname=" + fs.name})
	if client == nil {
		return nil, err
	}
	identity := &cephFSAuthorizationIdentity{volume: volume.identity, client: client, poolID: pool.ID,
		dataPool: info.DataPool, namespace: info.PoolNamespace, access: config.Access}
	grant := &SubvolumeAuthorization{Client: client, AuthID: client.User(), Access: config.Access,
		FilesystemName: fs.name, SubvolumeName: volume.identity.name, GroupName: volume.identity.group,
		Path: volume.identity.path, DataPool: info.DataPool, PoolNamespace: info.PoolNamespace, identity: identity}
	if err != nil {
		return grant, err
	}
	if err := servicehook.LockTopology(ctx, fs.cluster); err != nil {
		return grant, err
	}
	defer servicehook.Unlock(fs.cluster)
	before, err := fs.authorizationClientCaps(ctx, identity)
	if err != nil {
		return grant, err
	}
	if before["mds"] != "" || before["osd"] != "" {
		return grant, errors.New("fresh principal acquired file-data capabilities outside this request")
	}
	if volume.identity.authorizations == nil {
		volume.identity.authorizations = make(map[*cephFSAuthorizationIdentity]struct{})
	}
	volume.identity.authorizations[identity] = struct{}{}
	identity.authorizeAttempted = true
	args := appendSubvolumeGroup([]string{"fs", "subvolume", "authorize", fs.name, volume.identity.name, client.User()}, volume.identity.group)
	args = append(args, "--access_level", config.Access, "--allow_existing_id")
	if _, err := servicehook.AuthCommand(ctx, fs.cluster, "authorize owned subvolume client", args...); err != nil {
		return grant, err
	}
	current, err := fs.authorizationClientCaps(ctx, identity)
	if err != nil {
		return grant, err
	}
	expected := maps.Clone(before)
	expected["mds"], expected["osd"] = identity.nativeClauses()
	if !equalAuthorizationCaps(current, expected) {
		return grant, errors.New("native subvolume authorization did not preserve the key and expected capability clauses")
	}
	if err := fs.checkNativeAuthorization(ctx, identity, true); err != nil {
		return grant, err
	}
	identity.authorized = true
	return grant, nil
}

func decodeSubvolumeAuthorizedClients(data []byte) ([]SubvolumeAuthorizedClient, error) {
	var raw []map[string]string
	if err := json.Unmarshal(data, &raw); err != nil || raw == nil {
		return nil, errors.New("decode native subvolume authorized clients")
	}
	result := make([]SubvolumeAuthorizedClient, 0, len(raw))
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
			result = append(result, SubvolumeAuthorizedClient{AuthID: id, Access: access})
		}
	}
	slices.SortFunc(result, func(a, b SubvolumeAuthorizedClient) int { return strings.Compare(a.AuthID, b.AuthID) })
	return result, nil
}

func (fs *Filesystem) nativeAuthorizedClients(ctx context.Context, name, group string) ([]SubvolumeAuthorizedClient, error) {
	args := appendSubvolumeGroup([]string{"fs", "subvolume", "authorized_list", fs.name, name}, group)
	data, err := fs.cluster.Ceph(ctx, args...)
	if err != nil {
		return nil, err
	}
	return decodeSubvolumeAuthorizedClients(data)
}

// SubvolumeAuthorizedClients reads native volumes authorization metadata,
// including principals created through raw Ceph. It grants no mutation ownership.
func (fs *Filesystem) SubvolumeAuthorizedClients(ctx context.Context, name, group string) ([]SubvolumeAuthorizedClient, error) {
	if err := validateCephFSVolumeName(name, false); err != nil {
		return nil, err
	}
	if err := validateCephFSVolumeName(group, true); err != nil {
		return nil, err
	}
	ctx, _, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return fs.nativeAuthorizedClients(ctx, name, group)
}

func (identity *cephFSAuthorizationIdentity) nativeClauses() (string, string) {
	return "allow " + identity.access + " path=" + identity.volume.path,
		"allow " + identity.access + " pool=" + identity.dataPool + " namespace=" + identity.namespace
}

// authorizationClientCaps must be called with cluster.mu held. It also supports
// a confirmed own deletion when reconciling a lost native deauthorize reply.
func (fs *Filesystem) authorizationClientCaps(ctx context.Context, identity *cephFSAuthorizationIdentity) (map[string]string, error) {
	c, client := fs.cluster, identity.client
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

func (fs *Filesystem) checkAuthorizationVolume(ctx context.Context, fsID int64, identity *cephFSAuthorizationIdentity) error {
	if err := fs.validateVolumeHandle(identity.volume); err != nil {
		return err
	}
	if identity.volume.removed {
		return errors.New("authorized subvolume was removed")
	}
	info, err := fs.subvolumeInfo(ctx, identity.volume.name, identity.volume.group)
	if err != nil {
		return err
	}
	if err := fs.checkVolumeIdentity(fsID, identity.volume, info.Path, info.CreatedAt); err != nil {
		return err
	}
	if info.State != "complete" || info.DataPool != identity.dataPool || info.PoolNamespace != identity.namespace {
		return errors.New("authorized subvolume state or native pool/namespace layout changed")
	}
	state, err := fs.readNativePools(ctx)
	if err != nil {
		return err
	}
	pool, err := state.poolByID(identity.poolID)
	if err != nil || pool.Name != identity.dataPool || !slices.Contains(state.dataPools, identity.poolID) {
		return errors.New("authorized subvolume pool was replaced or detached")
	}
	return nil
}

func (fs *Filesystem) checkNativeAuthorization(ctx context.Context, identity *cephFSAuthorizationIdentity, present bool) error {
	actual, err := fs.nativeAuthorizationPresent(ctx, identity)
	if err != nil {
		return err
	}
	if actual != present {
		return errors.New("native authorized_list does not match the owned grant state")
	}
	return nil
}

func (fs *Filesystem) nativeAuthorizationPresent(ctx context.Context, identity *cephFSAuthorizationIdentity) (bool, error) {
	listing, err := fs.nativeAuthorizedClients(ctx, identity.volume.name, identity.volume.group)
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

func (identity *cephFSAuthorizationIdentity) withoutOwnedClauses(caps map[string]string, require bool) (map[string]string, error) {
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
// client to observe changed permissions, then SubvolumeAuthorization.EvictClients when needed.
// A lost reply keeps retry state; native metadata/caps are reconciled before
// repeating the command. Outside edits after an uncertain result are surfaced
// without restoring or overwriting them.
func (grant *SubvolumeAuthorization) Deauthorize(ctx context.Context) error {
	fs, err := grant.filesystem()
	if err != nil {
		return err
	}
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return err
	}
	defer done()
	identity := grant.identity
	if identity.deauthorized {
		return nil
	}
	if err := fs.checkAuthorizationVolume(ctx, fsID, identity); err != nil {
		return err
	}
	if err := servicehook.LockTopology(ctx, fs.cluster); err != nil {
		return err
	}
	defer servicehook.Unlock(fs.cluster)
	current, err := fs.authorizationClientCaps(ctx, identity)
	if err != nil {
		return err
	}
	present, err := fs.nativeAuthorizationPresent(ctx, identity)
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
	args := appendSubvolumeGroup([]string{"fs", "subvolume", "deauthorize", fs.name, identity.volume.name, identity.client.User()}, identity.volume.group)
	if _, err := servicehook.AuthCommand(ctx, fs.cluster, "deauthorize owned subvolume client", args...); err != nil {
		return err
	}
	current, err = fs.authorizationClientCaps(ctx, identity)
	if err != nil {
		return err
	}
	if !equalAuthorizationCaps(current, remaining) {
		return errors.New("native deauthorization result did not preserve unrelated capabilities")
	}
	if err := fs.checkNativeAuthorization(ctx, identity, false); err != nil {
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
func (grant *SubvolumeAuthorization) EvictClients(ctx context.Context) error {
	fs, err := grant.filesystem()
	if err != nil {
		return err
	}
	ctx, fsID, done, err := fs.beginSubvolumeOperation(ctx)
	if err != nil {
		return err
	}
	defer done()
	identity := grant.identity
	if !identity.deauthorized {
		return errors.New("deauthorize the owned subvolume grant before evicting sessions")
	}
	if err := fs.checkAuthorizationVolume(ctx, fsID, identity); err != nil {
		return err
	}
	if err := servicehook.LockTopology(ctx, fs.cluster); err != nil {
		return err
	}
	defer servicehook.Unlock(fs.cluster)
	if _, err := fs.authorizationClientCaps(ctx, identity); err != nil {
		return err
	}
	if err := fs.checkNativeAuthorization(ctx, identity, false); err != nil {
		return err
	}
	args := appendSubvolumeGroup([]string{"fs", "subvolume", "evict", fs.name, identity.volume.name, identity.client.User()}, identity.volume.group)
	_, err = servicehook.AuthCommand(ctx, fs.cluster, fmt.Sprintf("evict owned %s sessions at subvolume root", identity.client.Name()), args...)
	return err
}
