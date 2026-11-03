package mirror

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/google/uuid"
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
	return &rbdReceiverObservationError{message: operation + " failed", cause: clusterutil.Quiet(errors.Join(causes...), native)}
}

// canonicalRBDUUID reports whether value is a non-nil UUID in its canonical
// lower-case form.
func canonicalRBDUUID(value string) bool {
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
		if !canonicalRBDUUID(value) {
			return "", rbdReceiverGuard("RBD bootstrap cluster identity is invalid")
		}
		fsid = value
	}
	if count != 1 {
		return "", rbdReceiverGuard("RBD original bootstrap cluster identity is unavailable")
	}
	return fsid, nil
}

func captureRBDReceiverClusters(ctx context.Context, config Config) (*rbdReceiverClusterIdentities, error) {
	ids := &rbdReceiverClusterIdentities{sourceCluster: config.Source, destinationCluster: config.Destination}
	for _, site := range []struct {
		cluster *ceph.Container
		result  *string
	}{{config.Source, &ids.source}, {config.Destination, &ids.destination}} {
		cc, err := site.cluster.ConnectionConfig(ctx)
		configuration := cc.Conf
		if err != nil {
			return nil, rbdReceiverQuery(ctx, "read RBD original bootstrap identity", err)
		}
		fsid, err := rbdReceiverBootstrapFSID(configuration)
		if err != nil {
			return nil, err
		}
		control, err := site.cluster.ControlContainer(ctx)
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
	data, err := rbdReceiverRead.exec(ctx, client, "read RBD original cluster identity", "ceph", "--connect-timeout", "5", "fsid")
	if err != nil {
		return err
	}
	actual := strings.TrimSpace(string(data))
	if !canonicalRBDUUID(actual) || actual != expected {
		return rbdReceiverGuard("RBD original cluster identity changed")
	}
	return nil
}

func (r *rbdReceiverPeer) checkClusterFSIDs(ctx context.Context, pair *rbdMirrorPair) error {
	ids := r.clusters
	if ids == nil || ids.sourceCluster != pair.config.Source || ids.destinationCluster != pair.config.Destination || !canonicalRBDUUID(ids.source) || !canonicalRBDUUID(ids.destination) || ids.source == ids.destination {
		return rbdReceiverGuard("RBD original cluster identities are not confirmed")
	}
	for _, site := range []struct {
		client testcontainers.Container
		fsid   string
	}{{pair.sourceClient, ids.source}, {pair.destinationClient, ids.destination}} {
		if site.client == nil {
			return rbdReceiverGuard("RBD owned setup client is unavailable")
		}
		if err := readRBDReceiverFSID(ctx, site.client, site.fsid); err != nil {
			return err
		}
	}
	return nil
}

// decodeRBDReceiverPeers reads the peers of a mirrored pool policy: each
// with a distinct canonical UUID, a site, a direction, a client unless it only
// transmits, and a canonical mirror UUID when it has one.
func decodeRBDReceiverPeers(raw json.RawMessage) ([]rbdMirrorPeer, error) {
	var nativePeers []struct {
		UUID      *string `json:"uuid"`
		Direction *string `json:"direction"`
		Site      *string `json:"site_name"`
		Mirror    *string `json:"mirror_uuid"`
		Client    *string `json:"client_name"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &nativePeers) != nil || nativePeers == nil {
		return nil, rbdReceiverGuard("decode RBD receiver pool peers")
	}
	var peers []rbdMirrorPeer
	for _, peer := range nativePeers {
		if peer.UUID == nil || peer.Direction == nil || peer.Site == nil || peer.Mirror == nil || peer.Client == nil {
			return nil, rbdReceiverGuard("decode RBD receiver peer identity fields")
		}
		peers = append(peers, rbdMirrorPeer{UUID: *peer.UUID, Direction: *peer.Direction, SiteName: *peer.Site, MirrorUUID: *peer.Mirror, ClientName: *peer.Client})
	}
	seen := make(map[string]bool)
	for _, peer := range peers {
		if !canonicalRBDUUID(peer.UUID) || seen[peer.UUID] || peer.SiteName == "" || peer.ClientName == "" && peer.Direction != "tx-only" || !slices.Contains([]string{"tx-only", "rx-only", "rx-tx"}, peer.Direction) || peer.MirrorUUID != "" && !canonicalRBDUUID(peer.MirrorUUID) {
			return nil, rbdReceiverGuard("decode RBD receiver peer identity")
		}
		seen[peer.UUID] = true
	}
	return peers, nil
}

// The receiving peer is a pool-level identity, even for named namespaces.
func (p *rbdMirrorPair) checkReceiverPolicies(ctx context.Context) ([]rbdMirrorPeer, error) {
	if p.poolIdentities == nil || p.policyIdentities == nil {
		return nil, rbdReceiverGuard("RBD original receiver pool and policies are not confirmed")
	}
	sites := p.setupSites()
	rawPeers, err := rbdReceiverRead.checkSites(ctx, p.config.Pool, sites, "RBD original receiver namespace policy changed")
	if err != nil {
		return nil, err
	}
	var destinationPeers []rbdMirrorPeer
	for i, site := range sites {
		if site.identity.base.Mode == "disabled" {
			continue
		}
		peers, err := decodeRBDReceiverPeers(rawPeers[i])
		if err != nil {
			return nil, err
		}
		if site.client == p.destinationClient {
			destinationPeers = peers
		}
	}
	return destinationPeers, nil
}

func (r *rbdReceiverPeer) capturePeer(ctx context.Context, pair *rbdMirrorPair, bootstrap rbdBootstrapIdentity) (*rbdReceiverPeerIdentity, error) {
	if err := r.checkClusterFSIDs(ctx, pair); err != nil {
		return nil, err
	}
	if bootstrap.FSID != r.clusters.source {
		return nil, rbdReceiverGuard("RBD bootstrap differs from the original source cluster")
	}
	peers, err := pair.checkReceiverPolicies(ctx)
	if err != nil {
		return nil, err
	}
	var candidate *rbdReceiverPeerIdentity
	for _, peer := range peers {
		if peer.SiteName != pair.config.SourceSite && peer.SiteName != r.clusters.source {
			continue
		}
		if candidate != nil || !slices.Contains([]string{"rx-only", "rx-tx"}, peer.Direction) || peer.ClientName != bootstrap.ClientName || peer.MirrorUUID != "" && peer.MirrorUUID != pair.policyIdentities.source.base.MirrorUUID {
			return nil, rbdReceiverGuard("RBD receiving peer readback conflicts with bootstrap identity")
		}
		candidate = &rbdReceiverPeerIdentity{uuid: peer.UUID, site: peer.SiteName, client: peer.ClientName, sourceMirrorUUID: pair.policyIdentities.source.base.MirrorUUID}
	}
	if candidate == nil {
		return nil, rbdReceiverGuard("RBD receiving peer readback is unavailable")
	}
	if err := r.checkClusterFSIDs(ctx, pair); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, rbdReceiverQuery(ctx, "confirm RBD receiving peer", err)
	}
	return candidate, nil
}

func (r *rbdReceiverPeer) checkIdentities(ctx context.Context, pair *rbdMirrorPair) error {
	if err := r.checkClusterFSIDs(ctx, pair); err != nil {
		return err
	}
	peers, err := pair.checkReceiverPolicies(ctx)
	if err != nil {
		return err
	}
	identity := r.peer
	if identity == nil {
		return rbdReceiverGuard("RBD receiving peer identity is not confirmed")
	}
	found := false
	for _, peer := range peers {
		if peer.UUID != identity.uuid {
			if peer.SiteName == identity.site || peer.SiteName == pair.config.SourceSite || peer.SiteName == r.clusters.source {
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
	return r.checkClusterFSIDs(ctx, pair)
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
