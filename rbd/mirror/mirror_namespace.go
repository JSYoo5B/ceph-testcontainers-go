package mirror

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/strictjson"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd/internal/resourcename"
	"slices"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
)

// Scope selects explicit image enrollment or automatic journal-image
// enrollment for one pool/namespace. It is separate from snapshot/journal Mode.
type Scope string

const (
	// ScopeImage enables images one by one, with Mirror.EnableImage or rbd
	// mirror image enable.
	ScopeImage Scope = "image"
	// ScopePool enrolls every image with the journaling feature
	// automatically; it implies ModeJournal.
	ScopePool Scope = "pool"
)

// NamespaceState reports native policy for a selected pool/namespace.
// An empty Namespace/RemoteNamespace identifies the actual default namespace.
// Mode is disabled, init-only, image or pool. MirrorUUID is a native mirroring
// identity, not the cluster FSID. A configured policy alone proves no replication.
type NamespaceState struct {
	PoolID                                             int64
	Pool, Namespace, RemoteNamespace, Mode, MirrorUUID string
}

// Policies reports both current native policies. Readback does not
// adopt ownership of namespaces or permit their deletion.
type Policies struct {
	Source, Destination NamespaceState
}

// PolicyStatus reads this link's selected namespace policies after checking the
// original native pool IDs. External policy changes are reported as native
// state; callers compare Scope and RemoteNamespace with their desired fixture.
//
//purpose:observation
func (m *Mirror) PolicyStatus(ctx context.Context) (Policies, error) {
	var status Policies
	if m == nil {
		return status, servicehook.Invalid(errors.New("RBD mirror fixture is unavailable"))
	}
	if err := multicluster.LockObservation(ctx, &m.mu); err != nil {
		return Policies{}, err
	}
	defer m.mu.Unlock()
	if m.closed || m.pair.sourceClient == nil || m.pair.destinationClient == nil {
		return status, servicehook.Invalid(errors.New("RBD mirror fixture is unavailable or terminated"))
	}
	if err := m.pair.checkPools(ctx); err != nil {
		return status, err
	}
	source, _, err := rbdSetupRead.policy(ctx, m.pair.sourceClient, m.pair.config.Pool, m.pair.config.SourceNamespace, m.pair.legacyClient(m.pair.sourceClient))
	if err != nil {
		return status, err
	}
	destination, _, err := rbdSetupRead.policy(ctx, m.pair.destinationClient, m.pair.config.Pool, m.pair.config.DestinationNamespace, m.pair.legacyClient(m.pair.destinationClient))
	if err != nil {
		return status, err
	}
	status.Source = namespacePolicyState(m.pair.config.Pool, m.pair.config.SourceNamespace, source)
	status.Destination = namespacePolicyState(m.pair.config.Pool, m.pair.config.DestinationNamespace, destination)
	if m.pair.poolIdentities != nil {
		status.Source.PoolID, status.Destination.PoolID = m.pair.poolIdentities.source, m.pair.poolIdentities.destination
	}
	return status, m.pair.checkPools(ctx)
}

func namespacePolicyState(pool, namespace string, native nativeRBDMirrorPolicy) NamespaceState {
	remote := ""
	if native.RemoteNamespace != nil {
		remote = *native.RemoteNamespace
	}
	return NamespaceState{Pool: pool, Namespace: namespace, RemoteNamespace: remote, Mode: native.Mode, MirrorUUID: native.MirrorUUID}
}

func normalizeRBDMirrorScopeAndNamespaces(config Config) (Config, error) {
	if config.Scope == "" {
		config.Scope = ScopeImage
	}
	if config.Scope != ScopeImage && config.Scope != ScopePool {
		return config, servicehook.Invalid(errors.New("RBD mirror scope must be image or pool"))
	}
	for _, namespace := range []string{config.SourceNamespace, config.DestinationNamespace} {
		if namespace != "" && resourcename.Check("namespace", namespace) != nil {
			return config, servicehook.Invalid(errors.New("RBD mirror namespaces must be empty for default or use letters, digits, underscores, dots or dashes without starting with a dot or dash"))
		}
	}
	if config.Scope == ScopePool {
		if config.Mode == "" {
			config.Mode = ModeJournal
		}
		if config.Mode != ModeJournal {
			return config, servicehook.Invalid(errors.New("pool-scope RBD mirroring supports journal mode only"))
		}
	}
	return config, nil
}

func rbdMirrorNamespaceSpec(pool, namespace string) string {
	if namespace == "" {
		return pool
	}
	return pool + "/" + namespace
}

func rbdMirrorImageSpec(pool, namespace, image string) string {
	return rbdMirrorNamespaceSpec(pool, namespace) + "/" + image
}

type rbdMirrorPoolIdentities struct {
	source, destination int64
	// sourcePool and destinationPool identify the native pools by ID, so
	// that a later pool that reuses the name is refused.
	sourcePool, destinationPool *ceph.Pool
}

func preflightRBDMirrorPools(ctx context.Context, config Config) (*rbdMirrorPoolIdentities, error) {
	identities := &rbdMirrorPoolIdentities{}
	for _, site := range []struct {
		cluster   *ceph.Container
		namespace string
		id        *int64
		handle    **ceph.Pool
	}{
		{config.Source, config.SourceNamespace, &identities.source, &identities.sourcePool},
		{config.Destination, config.DestinationNamespace, &identities.destination, &identities.destinationPool},
	} {
		handle, err := site.cluster.Pool(ctx, config.Pool)
		if err != nil {
			return nil, err
		}
		pool, err := handle.Status(ctx)
		if err != nil {
			return nil, err
		}
		*site.handle = handle
		if pool.Type != "replicated" {
			return nil, errors.New("RBD mirror metadata pool must be replicated")
		}
		namespaces, err := rbd.ListNamespaces(ctx, site.cluster, config.Pool)
		if err != nil {
			return nil, err
		}
		if site.namespace != "" && !slices.Contains(namespaces, site.namespace) {
			return nil, fmt.Errorf("RBD mirror namespace %q must already exist in pool %q", site.namespace, config.Pool)
		}
		*site.id = pool.ID
	}
	return identities, nil
}

func (p *rbdMirrorPair) checkPools(ctx context.Context) error {
	if p.poolIdentities == nil {
		return nil
	} // Only internal unit fixtures lack capture.
	for _, site := range []struct {
		cluster *ceph.Container
		id      int64
		handle  *ceph.Pool
	}{
		{p.config.Source, p.poolIdentities.source, p.poolIdentities.sourcePool}, {p.config.Destination, p.poolIdentities.destination, p.poolIdentities.destinationPool},
	} {
		handle := site.handle
		if handle == nil {
			// Only internal unit fixtures lack the captured handle.
			var err error
			if handle, err = site.cluster.Pool(ctx, p.config.Pool); err != nil {
				return err
			}
		}
		pool, err := handle.Status(ctx)
		if err != nil {
			return err
		}
		if pool.ID != site.id || pool.Type != "replicated" {
			return errors.New("RBD mirror pool identity/type changed; refusing policy or peer mutation")
		}
	}
	return nil
}

type nativeRBDMirrorPolicy struct {
	Mode            string  `json:"mode"`
	MirrorUUID      string  `json:"mirror_uuid"`
	RemoteNamespace *string `json:"remote_namespace"`
	SiteName        string  `json:"site_name"`
}

// rbdNativeRead classifies the failures of the package's native readbacks for
// one caller: query wraps a command that failed, guard reports output that is
// malformed or shows identity drift. Setup, image observation and receiver
// observation keep their own classifications; the parsing is shared.
type rbdNativeRead struct {
	query func(ctx context.Context, operation string, err error) error
	guard func(message string) error
}

var (
	// rbdSetupRead classifies the reads of setup and of the policy readers.
	rbdSetupRead = rbdNativeRead{
		query: func(_ context.Context, operation string, err error) error {
			return fmt.Errorf("%s: %w", operation, err)
		},
		guard: func(message string) error { return errors.New(message) },
	}
	// rbdObservedRead classifies the reads of an image status observation.
	rbdObservedRead = rbdNativeRead{
		query: func(_ context.Context, operation string, err error) error { return rbdImageQueryError(operation, err) },
		guard: rbdImageGuardError,
	}
	// rbdReceiverRead classifies the reads of a receiver observation, which
	// never retain native output.
	rbdReceiverRead = rbdNativeRead{query: rbdReceiverQuery, guard: rbdReceiverGuard}
)

// exec runs args in client. A call whose ctx ended while the command ran
// fails even when the command succeeded.
func (r rbdNativeRead) exec(ctx context.Context, client testcontainers.Container, operation string, args ...string) ([]byte, error) {
	data, err := multicluster.Exec(ctx, client, args...)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return nil, r.query(ctx, operation, err)
	}
	return data, nil
}

// json refuses a native JSON document that is oversized, has trailing data,
// duplicate fields or is malformed.
func (r rbdNativeRead) json(data []byte) error {
	switch err := strictjson.Check(data, strictjson.Options{MaxBytes: 4 << 20}); {
	case err == nil:
		return nil
	case errors.Is(err, strictjson.ErrSize):
		return r.guard("RBD native JSON size is invalid")
	case errors.Is(err, strictjson.ErrTrailing):
		return r.guard("decode RBD native JSON: trailing data")
	default:
		return r.guard("decode RBD native JSON: malformed or duplicate fields")
	}
}

// policy reads the mirror policy of pool, or of its named namespace, with
// rbd mirror pool info. A mirrored policy needs a canonical non-nil mirror
// UUID and a remote namespace; legacy fills what Ceph 19 omits. It also
// returns the native peers, undecoded.
func (r rbdNativeRead) policy(ctx context.Context, client testcontainers.Container, pool, namespace string, legacy bool) (nativeRBDMirrorPolicy, json.RawMessage, error) {
	var policy nativeRBDMirrorPolicy
	data, err := r.exec(ctx, client, "read RBD mirror namespace policy", "rbd", "mirror", "pool", "info", rbdMirrorNamespaceSpec(pool, namespace), "--format", "json")
	if err != nil {
		return policy, nil, err
	}
	if err := r.json(data); err != nil {
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
		return policy, nil, r.guard("decode RBD mirror namespace policy: invalid native mode")
	}
	if legacy {
		fields.Remote = legacyRBDRemoteNamespace(*fields.Mode, fields.Remote, namespace)
		if *fields.Mode != "disabled" && (fields.UUID == nil || *fields.UUID == "") {
			value, err := readLegacyRBDMirrorUUID(ctx, client, pool, namespace)
			if err != nil {
				return policy, nil, r.query(ctx, "read RBD mirror namespace policy", err)
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
	if policy.Mode != "disabled" && (!canonicalRBDUUID(policy.MirrorUUID) || policy.RemoteNamespace == nil) {
		return policy, nil, r.guard("decode RBD mirror namespace policy: missing or non-canonical UUID, or missing remote namespace")
	}
	if namespace != "" && policy.Mode == "init-only" {
		return policy, nil, r.guard("native init-only mode is invalid on a named namespace")
	}
	return policy, fields.Peers, nil
}

// sitePolicies reads the base policy of pool and the selected policy of
// namespace, the base itself for the default namespace, and the base's peers.
func (r rbdNativeRead) sitePolicies(ctx context.Context, client testcontainers.Container, pool, namespace string, legacy bool) (rbdMirrorSitePolicyIdentity, json.RawMessage, error) {
	base, peers, err := r.policy(ctx, client, pool, "", legacy)
	if err != nil {
		return rbdMirrorSitePolicyIdentity{}, nil, err
	}
	selected := base
	if namespace != "" {
		if selected, _, err = r.policy(ctx, client, pool, namespace, legacy); err != nil {
			return rbdMirrorSitePolicyIdentity{}, nil, err
		}
	}
	return rbdMirrorSitePolicyIdentity{base: base, selected: selected}, peers, nil
}

// pool requires the replicated pool name to still have the expected ID in
// osd pool ls detail, every listed pool to be well formed and unique.
func (r rbdNativeRead) pool(ctx context.Context, client testcontainers.Container, name string, expected int64) error {
	data, err := r.exec(ctx, client, "read RBD mirror pool identity", "ceph", "--connect-timeout", "5", "osd", "pool", "ls", "detail", "--format", "json")
	if err != nil {
		return err
	}
	if err := r.json(data); err != nil {
		return err
	}
	var pools []struct {
		ID   *int64  `json:"pool_id"`
		Name *string `json:"pool_name"`
		Type *int    `json:"type"`
	}
	if json.Unmarshal(data, &pools) != nil || pools == nil {
		return r.guard("decode RBD mirror pool identity")
	}
	found := false
	seenIDs, seenNames := make(map[int64]bool), make(map[string]bool)
	for _, pool := range pools {
		if pool.ID == nil || *pool.ID < 0 || pool.Name == nil || *pool.Name == "" || pool.Type == nil ||
			seenIDs[*pool.ID] || seenNames[*pool.Name] || (*pool.Type != 1 && *pool.Type != 3) {
			return r.guard("decode RBD mirror pool identity: missing, malformed or duplicate fields")
		}
		seenIDs[*pool.ID], seenNames[*pool.Name] = true, true
		if *pool.Name == name {
			if *pool.ID != expected || *pool.Type != 1 {
				return r.guard("RBD mirror pool identity or type changed during observation")
			}
			found = true
		}
	}
	if !found {
		return r.guard("RBD mirror pool no longer exists")
	}
	return nil
}

// daemonStatus reads the rbd-mirror status of daemon through its admin
// socket.
func (r rbdNativeRead) daemonStatus(ctx context.Context, daemon testcontainers.Container, operation string) (*DaemonStatus, error) {
	data, err := r.exec(ctx, daemon, operation, "ceph", "--admin-daemon", "/tmp/rbd-mirror.asok", "rbd", "mirror", "status")
	if err != nil {
		return nil, err
	}
	if err := r.json(data); err != nil {
		return nil, err
	}
	var status DaemonStatus
	if json.Unmarshal(data, &status) != nil || status.PoolReplayers == nil {
		return nil, r.guard("decode RBD receiving process status: missing or malformed pool replayers")
	}
	return &status, nil
}

// rbdMirrorPolicySite is one site whose recorded pool and policy identity a
// check rereads: through client, with legacy decoding.
type rbdMirrorPolicySite struct {
	client    testcontainers.Container
	legacy    bool
	poolID    int64
	namespace string
	identity  rbdMirrorSitePolicyIdentity
}

// checkSites rereads each site's pool identity, base and selected policy and
// pool identity again, and refuses a policy that differs from the recorded
// identity with changed. It returns the base peers of each site, undecoded.
func (r rbdNativeRead) checkSites(ctx context.Context, pool string, sites []rbdMirrorPolicySite, changed string) ([]json.RawMessage, error) {
	peers := make([]json.RawMessage, len(sites))
	for i, site := range sites {
		if err := r.pool(ctx, site.client, pool, site.poolID); err != nil {
			return nil, err
		}
		current, sitePeers, err := r.sitePolicies(ctx, site.client, pool, site.namespace, site.legacy)
		if err != nil {
			return nil, err
		}
		if !sameRBDMirrorPolicy(current.base, site.identity.base) || !sameRBDMirrorPolicy(current.selected, site.identity.selected) {
			return nil, r.guard(changed)
		}
		if err := r.pool(ctx, site.client, pool, site.poolID); err != nil {
			return nil, err
		}
		peers[i] = sitePeers
	}
	return peers, ctx.Err()
}

// setupSites returns both sites of the pair, read through their setup
// clients.
func (p *rbdMirrorPair) setupSites() []rbdMirrorPolicySite {
	return []rbdMirrorPolicySite{
		{p.sourceClient, p.legacyClient(p.sourceClient), p.poolIdentities.source, p.config.SourceNamespace, p.policyIdentities.source},
		{p.destinationClient, p.legacyClient(p.destinationClient), p.poolIdentities.destination, p.config.DestinationNamespace, p.policyIdentities.destination},
	}
}

// legacyClient reports whether client is a setup client of this link that
// cannot map namespaces: its rbd has no --remote-namespace, or its cluster
// cannot (see legacyCluster). Such a site has no init-only mode either.
func (p *rbdMirrorPair) legacyClient(client testcontainers.Container) bool {
	switch client {
	case nil:
		return false
	case p.sourceClient:
		return p.legacyCLI[client] || p.legacyCluster(p.config.Source)
	case p.destinationClient:
		return p.legacyCLI[client] || p.legacyCluster(p.config.Destination)
	}
	return false
}

// legacyCluster reports whether cluster cannot map namespaces: the rbd of its
// client toolkit, whose release its daemons run, has no --remote-namespace.
// A patched image of an older release can have it. Without that evidence,
// as for a link that Run did not set up, the release decides.
func (p *rbdMirrorPair) legacyCluster(cluster *ceph.Container) bool {
	if legacy, detected := p.legacyClusters[cluster]; detected {
		return legacy
	}
	return multicluster.CephBefore(cluster, 20)
}

// detectNamespaceMapping reads whether each setup client and each cluster's
// client toolkit can map namespaces.
func (p *rbdMirrorPair) detectNamespaceMapping(ctx context.Context) error {
	p.legacyCLI, p.legacyClusters = map[testcontainers.Container]bool{}, map[*ceph.Container]bool{}
	for _, client := range []testcontainers.Container{p.sourceClient, p.destinationClient} {
		mapping, err := rbdNamespaceMappingOption(ctx, client)
		if err != nil {
			return err
		}
		p.legacyCLI[client] = !mapping
	}
	for _, cluster := range []*ceph.Container{p.config.Source, p.config.Destination} {
		control, err := cluster.ClientContainer(ctx)
		if err != nil {
			return fmt.Errorf("inspect RBD mirror cluster control handle: %w", err)
		}
		mapping, err := rbdNamespaceMappingOption(ctx, control)
		if err != nil {
			return err
		}
		p.legacyClusters[cluster] = !mapping
	}
	return nil
}

// rbdNamespaceMappingOption reports whether client's rbd can map a mirrored
// namespace to another name: its usage of "mirror pool enable" lists
// --remote-namespace, as from Ceph 20.
func rbdNamespaceMappingOption(ctx context.Context, client testcontainers.Container) (bool, error) {
	usage, err := multicluster.Exec(ctx, client, "rbd", "help", "mirror", "pool", "enable")
	if err != nil {
		return false, fmt.Errorf("read the options of rbd mirror pool enable: %w", err)
	}
	return strings.Contains(string(usage), "--remote-namespace"), nil
}

// namespaceMappingUnsupported refuses a mapping between differently named
// namespaces, naming the side that lacks it: the cluster, or else the setup
// client.
func (p *rbdMirrorPair) namespaceMappingUnsupported(ctx context.Context, client testcontainers.Container, cluster *ceph.Container, site, namespace, remote string) error {
	refusal := &ceph.OperationError{Kind: ceph.ErrUnsupported, Target: site + " setup client",
		Capability: "rbd mirror pool enable --remote-namespace",
		Err:        fmt.Errorf("mirroring RBD namespace %q to %q needs Ceph 20 or later; Ceph 19 pairs only namespaces with the same name", namespace, remote)}
	image := client
	if p.legacyCluster(cluster) {
		refusal.Target, refusal.Version = site+" cluster", ""
		if facts, err := ceph.FactsOf(cluster); err == nil {
			refusal.Version = facts.CephVersion
		}
		image, _ = cluster.ClientContainer(ctx)
	}
	if image != nil {
		if inspect, err := image.Inspect(ctx); err == nil && inspect != nil {
			refusal.Image = inspect.Image
		}
	}
	return refusal
}

// readLegacyRBDMirrorUUID reads the mirror UUID that Ceph 19's rbd CLI does
// not print. cls_rbd keeps it as the raw text value of the rbd_mirroring
// object's mirror_uuid omap key.
func readLegacyRBDMirrorUUID(ctx context.Context, client testcontainers.Container, pool, namespace string) (string, error) {
	args := []string{"rados", "--pool", pool}
	if namespace != "" {
		args = append(args, "--namespace", namespace)
	}
	data, err := multicluster.Exec(ctx, client, append(args, "getomapval", "rbd_mirroring", "mirror_uuid", "/dev/stdout")...)
	if err != nil {
		return "", fmt.Errorf("read Ceph 19 RBD mirror UUID: %w", err)
	}
	return decodeLegacyRBDMirrorUUID(data)
}

func decodeLegacyRBDMirrorUUID(data []byte) (string, error) {
	value := string(data)
	if parsed, err := uuid.Parse(value); err != nil || parsed.String() != value {
		return "", errors.New("decode Ceph 19 RBD mirror UUID: not a canonical UUID")
	}
	return value, nil
}

// legacyRBDRemoteNamespace fills the remote namespace that Ceph 19 omits.
// Before Ceph 20 a mirrored namespace always pairs with the same name.
func legacyRBDRemoteNamespace(mode string, remote *string, namespace string) *string {
	if remote != nil || mode == "disabled" {
		return remote
	}
	return &namespace
}

type rbdMirrorPolicyStep struct {
	client                        testcontainers.Container
	namespace, remote, site, mode string
	previous                      nativeRBDMirrorPolicy
	// legacy marks a Ceph 19 client without --remote-namespace.
	legacy bool
}

func planRBDMirrorSite(client testcontainers.Container, namespace, remote, site string, scope Scope, base, selected nativeRBDMirrorPolicy, legacy bool) ([]rbdMirrorPolicyStep, error) {
	if base.Mode != "disabled" && base.SiteName != site {
		return nil, servicehook.Invalid(errors.New("existing cluster-wide RBD mirror site name differs; reconfigure explicitly"))
	}
	steps := []rbdMirrorPolicyStep{}
	if namespace != "" && base.Mode == "disabled" {
		// Ceph 19 has no init-only mode. Image mode on the default namespace
		// is the closest: it mirrors no image until one is enabled explicitly.
		mode := "init-only"
		if legacy {
			mode = "image"
		}
		steps = append(steps, rbdMirrorPolicyStep{client: client, mode: mode, site: site, previous: base, legacy: legacy})
	}
	if selected.Mode == "image" || selected.Mode == "pool" {
		if selected.Mode != string(scope) || selected.RemoteNamespace == nil || *selected.RemoteNamespace != remote {
			return nil, servicehook.Invalid(errors.New("existing RBD mirror namespace scope or remote mapping differs; reconfigure explicitly"))
		}
		return steps, nil
	}
	steps = append(steps, rbdMirrorPolicyStep{client: client, namespace: namespace, remote: remote, site: site, mode: string(scope), previous: selected, legacy: legacy})
	return steps, nil
}

func (p *rbdMirrorPair) provisionPolicies(ctx context.Context) error {
	if p.policyIdentities != nil {
		if err := p.checkPolicyIdentities(ctx); err != nil {
			return err
		}
	}
	steps, configuredSites, err := p.planPolicies(ctx)
	if err != nil {
		return err
	}
	for _, step := range steps {
		if err := p.applyPolicyStep(ctx, step, configuredSites); err != nil {
			return err
		}
	}
	return p.capturePolicyIdentities(ctx)
}

// planPolicies reads both endpoints' site names and namespace policies and
// plans the steps that bring them to the configured scope. Both endpoints are
// checked before the first mutation, so an incompatible destination cannot
// broaden the source's existing scope. configuredSites maps each site to the
// cluster-wide site name it had.
func (p *rbdMirrorPair) planPolicies(ctx context.Context) ([]rbdMirrorPolicyStep, map[string]string, error) {
	var steps []rbdMirrorPolicyStep
	configuredSites := make(map[string]string)
	for _, site := range []struct {
		client                  testcontainers.Container
		cluster                 *ceph.Container
		namespace, remote, name string
	}{
		{p.sourceClient, p.config.Source, p.config.SourceNamespace, p.config.DestinationNamespace, p.config.SourceSite},
		{p.destinationClient, p.config.Destination, p.config.DestinationNamespace, p.config.SourceNamespace, p.config.DestinationSite},
	} {
		legacy := p.legacyClient(site.client)
		if legacy && site.namespace != site.remote {
			return nil, nil, p.namespaceMappingUnsupported(ctx, site.client, site.cluster, site.name, site.namespace, site.remote)
		}
		configuredSite, err := readConfiguredRBDMirrorSite(ctx, site.client)
		if err != nil {
			return nil, nil, err
		}
		if configuredSite != "" && configuredSite != site.name {
			return nil, nil, servicehook.Invalid(errors.New("existing cluster-wide RBD mirror site name differs; reconfigure explicitly"))
		}
		configuredSites[site.name] = configuredSite
		current, _, err := rbdSetupRead.sitePolicies(ctx, site.client, p.config.Pool, site.namespace, legacy)
		if err != nil {
			return nil, nil, err
		}
		planned, err := planRBDMirrorSite(site.client, site.namespace, site.remote, site.name, p.config.Scope, current.base, current.selected, legacy)
		if err != nil {
			return nil, nil, err
		}
		steps = append(steps, planned...)
	}
	return steps, configuredSites, nil
}

// applyPolicyStep enables one planned policy after checking that the pools,
// the site name and the namespace policy are still what was planned, then
// reads the policy back. A failure may leave earlier steps persisted.
func (p *rbdMirrorPair) applyPolicyStep(ctx context.Context, step rbdMirrorPolicyStep, configuredSites map[string]string) error {
	if err := p.checkPools(ctx); err != nil {
		return err
	}
	configuredSite, err := readConfiguredRBDMirrorSite(ctx, step.client)
	if err != nil {
		return err
	}
	if configuredSite != configuredSites[step.site] {
		return errors.New("cluster-wide RBD mirror site name changed during setup; refusing mutation")
	}
	current, _, err := rbdSetupRead.policy(ctx, step.client, p.config.Pool, step.namespace, step.legacy)
	if err != nil {
		return err
	}
	if !sameRBDMirrorPolicy(current, step.previous) {
		return errors.New("RBD mirror namespace policy changed during setup; refusing mutation")
	}
	args := []string{"rbd", "mirror", "pool", "enable", "--site-name", step.site, rbdMirrorNamespaceSpec(p.config.Pool, step.namespace), step.mode}
	if step.mode != "init-only" && !step.legacy {
		// Even an empty value is intentional: omission would default to the
		// local namespace and break named -> default mappings.
		args = append(args, "--remote-namespace", step.remote)
	}
	if _, err := multicluster.Exec(ctx, step.client, args...); err != nil {
		return fmt.Errorf("enable RBD mirror %s policy (earlier steps may have persisted): %w", step.mode, err)
	}
	current, _, err = rbdSetupRead.policy(ctx, step.client, p.config.Pool, step.namespace, step.legacy)
	if err != nil {
		return err
	}
	if current.Mode != step.mode || (step.mode != "init-only" && (current.RemoteNamespace == nil || *current.RemoteNamespace != step.remote)) {
		return errors.New("RBD mirror namespace policy readback differs from requested configuration")
	}
	configuredSites[step.site] = step.site
	return nil
}

// capturePolicyIdentities reads the provisioned policies, requires them to be
// the configured scope, mapping and site names, and records them on first
// setup.
func (p *rbdMirrorPair) capturePolicyIdentities(ctx context.Context) error {
	identities, err := p.readPolicyIdentities(ctx)
	if err != nil {
		return err
	}
	for _, site := range []struct {
		identity     rbdMirrorSitePolicyIdentity
		remote, name string
	}{
		{identities.source, p.config.DestinationNamespace, p.config.SourceSite},
		{identities.destination, p.config.SourceNamespace, p.config.DestinationSite},
	} {
		if site.identity.selected.Mode != string(p.config.Scope) || site.identity.selected.RemoteNamespace == nil || *site.identity.selected.RemoteNamespace != site.remote || site.identity.base.SiteName != site.name {
			return errors.New("RBD mirror policies changed before identity capture")
		}
	}
	if p.policyIdentities == nil {
		p.policyIdentities = identities
	}
	return p.checkPolicyIdentities(ctx)
}

type rbdMirrorSitePolicyIdentity struct {
	base, selected nativeRBDMirrorPolicy
}

type rbdMirrorPolicyIdentities struct {
	source, destination rbdMirrorSitePolicyIdentity
}

func (p *rbdMirrorPair) readPolicyIdentities(ctx context.Context) (*rbdMirrorPolicyIdentities, error) {
	if err := p.checkPools(ctx); err != nil {
		return nil, err
	}
	identities := &rbdMirrorPolicyIdentities{}
	for _, site := range []struct {
		client    testcontainers.Container
		namespace string
		identity  *rbdMirrorSitePolicyIdentity
	}{
		{p.sourceClient, p.config.SourceNamespace, &identities.source},
		{p.destinationClient, p.config.DestinationNamespace, &identities.destination},
	} {
		current, _, err := rbdSetupRead.sitePolicies(ctx, site.client, p.config.Pool, site.namespace, p.legacyClient(site.client))
		if err != nil {
			return nil, err
		}
		*site.identity = current
	}
	return identities, p.checkPools(ctx)
}

func (p *rbdMirrorPair) checkPolicyIdentities(ctx context.Context) error {
	if p.policyIdentities == nil {
		if p.poolIdentities != nil {
			return errors.New("RBD mirror policies were not confirmed during setup")
		}
		return nil // Hand-constructed internal tests have no setup identity.
	}
	current, err := p.readPolicyIdentities(ctx)
	if err != nil {
		return err
	}
	if !sameRBDMirrorPolicy(current.source.base, p.policyIdentities.source.base) || !sameRBDMirrorPolicy(current.source.selected, p.policyIdentities.source.selected) || !sameRBDMirrorPolicy(current.destination.base, p.policyIdentities.destination.base) || !sameRBDMirrorPolicy(current.destination.selected, p.policyIdentities.destination.selected) {
		return errors.New("RBD mirror UUID, scope, remote mapping or site identity changed; refusing mutation")
	}
	return nil
}

// Ceph omits site_name from disabled pool info, although another pool can
// already use the cluster-wide config key. Read only this key's value; do not
// dump peer credentials from the monitor config-key store.
func readConfiguredRBDMirrorSite(ctx context.Context, client testcontainers.Container) (string, error) {
	const key = "rbd/mirror/site_name"
	data, err := multicluster.Exec(ctx, client, "ceph", "config-key", "ls", "--format", "json")
	if err != nil {
		return "", err
	}
	var keys []string
	if json.Unmarshal(data, &keys) != nil || keys == nil {
		return "", errors.New("decode native config-key names")
	}
	if !slices.Contains(keys, key) {
		return "", nil
	}
	data, err = multicluster.Exec(ctx, client, "ceph", "config-key", "get", key)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func sameRBDMirrorPolicy(a, b nativeRBDMirrorPolicy) bool {
	if a.Mode != b.Mode || a.MirrorUUID != b.MirrorUUID || a.SiteName != b.SiteName || (a.RemoteNamespace == nil) != (b.RemoteNamespace == nil) {
		return false
	}
	return a.RemoteNamespace == nil || *a.RemoteNamespace == *b.RemoteNamespace
}
