package multicluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go/internal/cluster"
	"github.com/testcontainers/testcontainers-go"
)

type rbdReceiverClusterIdentities struct {
	source, destination               string
	sourceCluster, destinationCluster *ceph.Container
}

type rbdReceiverPeerIdentity struct {
	generation                           uint64
	uuid, site, client, sourceMirrorUUID string
}

type rbdReceiverObservationError struct {
	message   string
	cause     error
	permanent bool
}

func (e *rbdReceiverObservationError) Error() string { return e.message }
func (e *rbdReceiverObservationError) Unwrap() error { return e.cause }
func rbdReceiverGuard(message string) error {
	return &rbdReceiverObservationError{message: message, permanent: true}
}

// Arbitrary transport/native stderr is never retained by this observation.
// Take exactly one caller-context snapshot and preserve both canonical causes.
func rbdReceiverQuery(ctx context.Context, operation string, native error) error {
	ctxErr := ctx.Err()
	var causes []error
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		if errors.Is(ctxErr, cause) || errors.Is(native, cause) {
			causes = append(causes, cause)
		}
	}
	return &rbdReceiverObservationError{message: operation + " failed", cause: errors.Join(causes...)}
}

func rbdReceiverJSON(data []byte) error {
	if len(data) == 0 || len(data) > 4<<20 {
		return rbdReceiverGuard("RBD receiver native JSON size is invalid")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var walk func(int) error
	walk = func(depth int) error {
		if depth > 64 {
			return errors.New("depth")
		}
		token, err := d.Token()
		if err != nil {
			return err
		}
		delim, composite := token.(json.Delim)
		if !composite {
			return nil
		}
		switch delim {
		case '{':
			seen := make(map[string]bool)
			for d.More() {
				token, err := d.Token()
				key, ok := token.(string)
				if err != nil || !ok || seen[key] {
					return errors.New("key")
				}
				seen[key] = true
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
		case '[':
			for d.More() {
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
		default:
			return errors.New("delimiter")
		}
		end, err := d.Token()
		if err != nil || (delim == '{' && end != json.Delim('}')) || (delim == '[' && end != json.Delim(']')) {
			return errors.New("end")
		}
		return nil
	}
	if walk(0) != nil {
		return rbdReceiverGuard("decode RBD receiver native JSON: malformed or duplicate fields")
	}
	if _, err := d.Token(); err != io.EOF {
		return rbdReceiverGuard("decode RBD receiver native JSON: trailing data")
	}
	return nil
}

func rbdReceiverExec(ctx context.Context, client testcontainers.Container, operation string, args ...string) ([]byte, error) {
	data, err := exec(ctx, client, args...)
	if err != nil {
		return nil, rbdReceiverQuery(ctx, operation, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, rbdReceiverQuery(ctx, operation, err)
	}
	return data, nil
}

func rbdReceiverUUID(value string) bool {
	u, err := uuid.Parse(value)
	return err == nil && u != uuid.Nil && u.String() == value
}

// Parse only the fixture's immutable bootstrap snapshot, never a container's
// potentially edited file. A single explicit global FSID is required.
func rbdReceiverBootstrapFSID(config []byte) (string, error) {
	if len(config) == 0 || len(config) > 1<<20 || bytes.ContainsRune(config, '\x00') {
		return "", rbdReceiverGuard("RBD original bootstrap configuration is invalid")
	}
	global, count := false, 0
	var fsid string
	for _, raw := range strings.Split(string(config), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "!") {
			return "", rbdReceiverGuard("RBD bootstrap includes are unsupported")
		}
		if strings.HasPrefix(line, "[") {
			end := strings.IndexByte(line, ']')
			if end < 0 || strings.TrimSpace(line[end+1:]) != "" {
				return "", rbdReceiverGuard("RBD bootstrap section is invalid")
			}
			global = strings.TrimSpace(line[1:end]) == "global"
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok || strings.TrimSpace(key) != "fsid" {
			continue
		}
		count++
		if !global || count != 1 {
			return "", rbdReceiverGuard("RBD bootstrap cluster identity is ambiguous")
		}
		value = strings.TrimSpace(value)
		if len(value) >= 2 && (value[0] == '\'' || value[0] == '"') && value[len(value)-1] == value[0] {
			value = value[1 : len(value)-1]
		}
		if !rbdReceiverUUID(value) {
			return "", rbdReceiverGuard("RBD bootstrap cluster identity is invalid")
		}
		fsid = value
	}
	if count != 1 {
		return "", rbdReceiverGuard("RBD original bootstrap cluster identity is unavailable")
	}
	return fsid, nil
}

func captureRBDReceiverClusters(ctx context.Context, config RBDMirrorConfig) (*rbdReceiverClusterIdentities, error) {
	ids := &rbdReceiverClusterIdentities{sourceCluster: config.Source, destinationCluster: config.Destination}
	for _, site := range []struct {
		cluster *ceph.Container
		result  *string
	}{{config.Source, &ids.source}, {config.Destination, &ids.destination}} {
		configuration, _, err := site.cluster.ConnectionConfigContext(ctx)
		if err != nil {
			return nil, rbdReceiverQuery(ctx, "read RBD original bootstrap identity", err)
		}
		fsid, err := rbdReceiverBootstrapFSID(configuration)
		if err != nil {
			return nil, err
		}
		control, err := site.cluster.ControlContainerContext(ctx)
		if err != nil {
			return nil, rbdReceiverQuery(ctx, "inspect RBD original cluster control", err)
		}
		if control == nil {
			return nil, rbdReceiverGuard("RBD original cluster control is unavailable")
		}
		if err := readRBDReceiverFSID(ctx, control, fsid); err != nil {
			return nil, err
		}
		*site.result = fsid
	}
	if ids.source == ids.destination {
		return nil, rbdReceiverGuard("RBD original clusters must have distinct identities")
	}
	return ids, nil
}

func readRBDReceiverFSID(ctx context.Context, client testcontainers.Container, expected string) error {
	data, err := rbdReceiverExec(ctx, client, "read RBD original cluster identity", "ceph", "--connect-timeout", "5", "fsid")
	if err != nil {
		return err
	}
	actual := strings.TrimSpace(string(data))
	if !rbdReceiverUUID(actual) || actual != expected {
		return rbdReceiverGuard("RBD original cluster identity changed")
	}
	return nil
}

func (m *RBDMirror) checkRBDReceiverClusterFSIDs(ctx context.Context) error {
	ids := m.receiverClusters
	if ids == nil || ids.sourceCluster != m.config.Source || ids.destinationCluster != m.config.Destination || !rbdReceiverUUID(ids.source) || !rbdReceiverUUID(ids.destination) || ids.source == ids.destination {
		return rbdReceiverGuard("RBD original cluster identities are not confirmed")
	}
	for _, site := range []struct {
		client testcontainers.Container
		fsid   string
	}{{m.sourceClient, ids.source}, {m.destinationClient, ids.destination}} {
		if site.client == nil {
			return rbdReceiverGuard("RBD owned setup client is unavailable")
		}
		if err := readRBDReceiverFSID(ctx, site.client, site.fsid); err != nil {
			return err
		}
	}
	return nil
}

func readRBDReceiverPolicy(ctx context.Context, client testcontainers.Container, pool, namespace string, legacy bool) (nativeRBDMirrorPolicy, []rbdMirrorPeer, error) {
	var policy nativeRBDMirrorPolicy
	data, err := rbdReceiverExec(ctx, client, "read RBD receiver namespace policy", "rbd", "mirror", "pool", "info", rbdMirrorNamespaceSpec(pool, namespace), "--format", "json")
	if err != nil {
		return policy, nil, err
	}
	if err := rbdReceiverJSON(data); err != nil {
		return policy, nil, err
	}
	var fields struct {
		Mode   *string         `json:"mode"`
		UUID   *string         `json:"mirror_uuid"`
		Remote *string         `json:"remote_namespace"`
		Site   *string         `json:"site_name"`
		Peers  json.RawMessage `json:"peers"`
	}
	if json.Unmarshal(data, &fields) != nil || fields.Mode == nil || !slices.Contains([]string{"disabled", "init-only", "image", "pool"}, *fields.Mode) {
		return policy, nil, rbdReceiverGuard("decode RBD receiver namespace policy")
	}
	if legacy {
		fields.Remote = legacyRBDRemoteNamespace(*fields.Mode, fields.Remote, namespace)
		if *fields.Mode != "disabled" && fields.UUID == nil {
			value, err := readLegacyRBDMirrorUUID(ctx, client, pool, namespace)
			if err != nil {
				return policy, nil, err
			}
			fields.UUID = &value
		}
	}
	policy.Mode, policy.RemoteNamespace = *fields.Mode, fields.Remote
	if fields.UUID != nil {
		policy.MirrorUUID = *fields.UUID
	}
	if fields.Site != nil {
		policy.SiteName = *fields.Site
	}
	if policy.Mode != "disabled" && (!rbdReceiverUUID(policy.MirrorUUID) || fields.Remote == nil) || namespace != "" && policy.Mode == "init-only" {
		return policy, nil, rbdReceiverGuard("decode RBD receiver namespace identity")
	}
	var peers []rbdMirrorPeer
	if namespace == "" && policy.Mode != "disabled" {
		var nativePeers []struct {
			UUID      *string `json:"uuid"`
			Direction *string `json:"direction"`
			Site      *string `json:"site_name"`
			Mirror    *string `json:"mirror_uuid"`
			Client    *string `json:"client_name"`
		}
		if len(fields.Peers) == 0 || json.Unmarshal(fields.Peers, &nativePeers) != nil || nativePeers == nil {
			return policy, nil, rbdReceiverGuard("decode RBD receiver pool peers")
		}
		for _, peer := range nativePeers {
			if peer.UUID == nil || peer.Direction == nil || peer.Site == nil || peer.Mirror == nil || peer.Client == nil {
				return policy, nil, rbdReceiverGuard("decode RBD receiver peer identity fields")
			}
			peers = append(peers, rbdMirrorPeer{UUID: *peer.UUID, Direction: *peer.Direction, SiteName: *peer.Site, MirrorUUID: *peer.Mirror, ClientName: *peer.Client})
		}
		seen := make(map[string]bool)
		for _, peer := range peers {
			if !rbdReceiverUUID(peer.UUID) || seen[peer.UUID] || peer.SiteName == "" || peer.ClientName == "" && peer.Direction != "tx-only" || !slices.Contains([]string{"tx-only", "rx-only", "rx-tx"}, peer.Direction) || peer.MirrorUUID != "" && !rbdReceiverUUID(peer.MirrorUUID) {
				return policy, nil, rbdReceiverGuard("decode RBD receiver peer identity")
			}
			seen[peer.UUID] = true
		}
	}
	return policy, peers, nil
}

func readRBDReceiverPool(ctx context.Context, client testcontainers.Container, name string, expected int64) error {
	data, err := rbdReceiverExec(ctx, client, "read RBD receiver pool identity", "ceph", "--connect-timeout", "5", "osd", "pool", "ls", "detail", "--format", "json")
	if err != nil {
		return err
	}
	if err := rbdReceiverJSON(data); err != nil {
		return err
	}
	var pools []struct {
		ID   *int64  `json:"pool_id"`
		Name *string `json:"pool_name"`
		Type *int    `json:"type"`
	}
	if json.Unmarshal(data, &pools) != nil || pools == nil {
		return rbdReceiverGuard("decode RBD receiver pool identity")
	}
	ids, names := make(map[int64]bool), make(map[string]bool)
	found := false
	for _, pool := range pools {
		if pool.ID == nil || *pool.ID < 0 || pool.Name == nil || *pool.Name == "" || pool.Type == nil || (*pool.Type != 1 && *pool.Type != 3) || ids[*pool.ID] || names[*pool.Name] {
			return rbdReceiverGuard("decode RBD receiver pool identity")
		}
		ids[*pool.ID], names[*pool.Name] = true, true
		if *pool.Name == name {
			if *pool.ID != expected || *pool.Type != 1 {
				return rbdReceiverGuard("RBD original receiver pool identity changed")
			}
			found = true
		}
	}
	if !found {
		return rbdReceiverGuard("RBD original receiver pool is missing")
	}
	return nil
}

// The receiving peer is a pool-level identity, even for named namespaces.
func (m *RBDMirror) checkRBDReceiverPolicies(ctx context.Context) ([]rbdMirrorPeer, error) {
	if m.poolIdentities == nil || m.policyIdentities == nil {
		return nil, rbdReceiverGuard("RBD original receiver pool and policies are not confirmed")
	}
	var destinationPeers []rbdMirrorPeer
	for _, site := range []struct {
		client      testcontainers.Container
		id          int64
		namespace   string
		identity    rbdMirrorSitePolicyIdentity
		destination bool
	}{
		{m.sourceClient, m.poolIdentities.source, m.config.SourceNamespace, m.policyIdentities.source, false},
		{m.destinationClient, m.poolIdentities.destination, m.config.DestinationNamespace, m.policyIdentities.destination, true},
	} {
		if err := readRBDReceiverPool(ctx, site.client, m.config.Pool, site.id); err != nil {
			return nil, err
		}
		base, peers, err := readRBDReceiverPolicy(ctx, site.client, m.config.Pool, "", m.legacyClient(site.client))
		if err != nil {
			return nil, err
		}
		selected := base
		if site.namespace != "" {
			selected, _, err = readRBDReceiverPolicy(ctx, site.client, m.config.Pool, site.namespace, m.legacyClient(site.client))
			if err != nil {
				return nil, err
			}
		}
		if !sameRBDMirrorPolicy(base, site.identity.base) || !sameRBDMirrorPolicy(selected, site.identity.selected) {
			return nil, rbdReceiverGuard("RBD original receiver namespace policy changed")
		}
		if site.destination {
			destinationPeers = peers
		}
		if err := readRBDReceiverPool(ctx, site.client, m.config.Pool, site.id); err != nil {
			return nil, err
		}
	}
	return destinationPeers, nil
}

func (m *RBDMirror) captureRBDReceiverPeer(ctx context.Context, bootstrap rbdBootstrapIdentity) (*rbdReceiverPeerIdentity, error) {
	if err := m.checkRBDReceiverClusterFSIDs(ctx); err != nil {
		return nil, err
	}
	if bootstrap.FSID != m.receiverClusters.source {
		return nil, rbdReceiverGuard("RBD bootstrap differs from the original source cluster")
	}
	peers, err := m.checkRBDReceiverPolicies(ctx)
	if err != nil {
		return nil, err
	}
	var candidate *rbdReceiverPeerIdentity
	for _, peer := range peers {
		if peer.SiteName != m.config.SourceSite && peer.SiteName != m.receiverClusters.source {
			continue
		}
		if candidate != nil || !slices.Contains([]string{"rx-only", "rx-tx"}, peer.Direction) || peer.ClientName != bootstrap.ClientName || peer.MirrorUUID != "" && peer.MirrorUUID != m.policyIdentities.source.base.MirrorUUID {
			return nil, rbdReceiverGuard("RBD receiving peer readback conflicts with bootstrap identity")
		}
		candidate = &rbdReceiverPeerIdentity{uuid: peer.UUID, site: peer.SiteName, client: peer.ClientName, sourceMirrorUUID: m.policyIdentities.source.base.MirrorUUID}
	}
	if candidate == nil {
		return nil, rbdReceiverGuard("RBD receiving peer readback is unavailable")
	}
	if err := m.checkRBDReceiverClusterFSIDs(ctx); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, rbdReceiverQuery(ctx, "confirm RBD receiving peer", err)
	}
	return candidate, nil
}

func (m *RBDMirror) checkRBDReceiverIdentities(ctx context.Context) error {
	if err := m.checkRBDReceiverClusterFSIDs(ctx); err != nil {
		return err
	}
	peers, err := m.checkRBDReceiverPolicies(ctx)
	if err != nil {
		return err
	}
	identity := m.receiverPeer
	if identity == nil {
		return rbdReceiverGuard("RBD receiving peer identity is not confirmed")
	}
	found := false
	for _, peer := range peers {
		if peer.UUID != identity.uuid {
			if peer.SiteName == identity.site || peer.SiteName == m.config.SourceSite || peer.SiteName == m.receiverClusters.source {
				return rbdReceiverGuard("RBD original receiving peer identity changed")
			}
			continue
		}
		if found || peer.SiteName != identity.site || peer.ClientName != identity.client || !slices.Contains([]string{"rx-only", "rx-tx"}, peer.Direction) || peer.MirrorUUID != "" && peer.MirrorUUID != identity.sourceMirrorUUID {
			return rbdReceiverGuard("RBD original receiving peer identity changed")
		}
		found = true
	}
	if !found {
		return rbdReceiverGuard("RBD original receiving peer is missing")
	}
	return m.checkRBDReceiverClusterFSIDs(ctx)
}

func rbdReceiverInstance(value string) bool {
	n, err := strconv.ParseUint(value, 10, 64)
	return err == nil && n > 0 && strconv.FormatUint(n, 10) == value
}

func rbdReceiverPermanent(err error) bool {
	if err == nil {
		return false
	}
	if observation, ok := err.(*rbdReceiverObservationError); ok && observation.permanent {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			if rbdReceiverPermanent(child) {
				return true
			}
		}
		return false
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return rbdReceiverPermanent(wrapped.Unwrap())
	}
	return false
}

func rbdReceiverContainerID(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if c < '0' || c > '9' && c < 'a' || c > 'f' {
			return false
		}
	}
	return true
}
