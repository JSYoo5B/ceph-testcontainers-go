package multicluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/google/uuid"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go/internal/cluster"
	"github.com/testcontainers/testcontainers-go"
)

// RBDMirrorScope selects explicit image enrollment or automatic journal-image
// enrollment for one pool/namespace. It is separate from snapshot/journal Mode.
type RBDMirrorScope string

const (
	RBDMirrorScopeImage RBDMirrorScope = "image"
	RBDMirrorScopePool  RBDMirrorScope = "pool"
)

// RBDMirrorNamespaceState reports native policy for a selected pool/namespace.
// An empty Namespace/RemoteNamespace identifies the actual default namespace.
// Mode is disabled, init-only, image or pool. MirrorUUID is a native mirroring
// identity, not the cluster FSID. A configured policy alone proves no replication.
type RBDMirrorNamespaceState struct {
	PoolID                                             int64
	Pool, Namespace, RemoteNamespace, Mode, MirrorUUID string
}

// RBDMirrorPolicies reports both current native policies. Readback does not
// adopt ownership of namespaces or permit their deletion.
type RBDMirrorPolicies struct {
	Source, Destination RBDMirrorNamespaceState
}

// PolicyStatus reads this link's selected namespace policies after checking the
// original native pool IDs. External policy changes are reported as native
// state; callers compare Scope and RemoteNamespace with their desired fixture.
func (m *RBDMirror) PolicyStatus(ctx context.Context) (RBDMirrorPolicies, error) {
	var status RBDMirrorPolicies
	if m == nil {
		return status, errors.New("RBD mirror fixture is unavailable")
	}
	if err := lockRGWSyncObservation(ctx, &m.mu); err != nil {
		return RBDMirrorPolicies{}, err
	}
	defer m.mu.Unlock()
	if m.closed || m.sourceClient == nil || m.destinationClient == nil {
		return status, errors.New("RBD mirror fixture is unavailable or terminated")
	}
	if err := m.checkRBDMirrorPools(ctx); err != nil {
		return status, err
	}
	source, err := readRBDMirrorNamespacePolicy(ctx, m.sourceClient, m.config.Pool, m.config.SourceNamespace, cephBefore(m.config.Source, 20))
	if err != nil {
		return status, err
	}
	destination, err := readRBDMirrorNamespacePolicy(ctx, m.destinationClient, m.config.Pool, m.config.DestinationNamespace, cephBefore(m.config.Destination, 20))
	if err != nil {
		return status, err
	}
	status.Source = namespacePolicyState(m.config.Pool, m.config.SourceNamespace, source)
	status.Destination = namespacePolicyState(m.config.Pool, m.config.DestinationNamespace, destination)
	if m.poolIdentities != nil {
		status.Source.PoolID, status.Destination.PoolID = m.poolIdentities.source, m.poolIdentities.destination
	}
	return status, m.checkRBDMirrorPools(ctx)
}

func namespacePolicyState(pool, namespace string, native nativeRBDMirrorPolicy) RBDMirrorNamespaceState {
	remote := ""
	if native.RemoteNamespace != nil {
		remote = *native.RemoteNamespace
	}
	return RBDMirrorNamespaceState{Pool: pool, Namespace: namespace, RemoteNamespace: remote, Mode: native.Mode, MirrorUUID: native.MirrorUUID}
}

var rbdMirrorNamespaceName = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)

func normalizeRBDMirrorScopeAndNamespaces(config RBDMirrorConfig) (RBDMirrorConfig, error) {
	if config.Scope == "" {
		config.Scope = RBDMirrorScopeImage
	}
	if config.Scope != RBDMirrorScopeImage && config.Scope != RBDMirrorScopePool {
		return config, errors.New("RBD mirror scope must be image or pool")
	}
	for _, namespace := range []string{config.SourceNamespace, config.DestinationNamespace} {
		if namespace != "" && !rbdMirrorNamespaceName.MatchString(namespace) {
			return config, errors.New("RBD mirror namespaces must be empty for default or use letters, digits, underscores, dots or dashes without starting with a dot or dash")
		}
	}
	if config.Scope == RBDMirrorScopePool {
		if config.Mode == "" {
			config.Mode = RBDMirrorModeJournal
		}
		if config.Mode != RBDMirrorModeJournal {
			return config, errors.New("pool-scope RBD mirroring supports journal mode only")
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

type rbdMirrorPoolIdentities struct{ source, destination int64 }

func preflightRBDMirrorPools(ctx context.Context, config RBDMirrorConfig) (*rbdMirrorPoolIdentities, error) {
	identities := &rbdMirrorPoolIdentities{}
	for _, site := range []struct {
		cluster   *ceph.Container
		namespace string
		id        *int64
	}{
		{config.Source, config.SourceNamespace, &identities.source},
		{config.Destination, config.DestinationNamespace, &identities.destination},
	} {
		pool, err := site.cluster.PoolStatus(ctx, config.Pool)
		if err != nil {
			return nil, err
		}
		if pool.Type != "replicated" {
			return nil, errors.New("RBD mirror metadata pool must be replicated")
		}
		namespaces, err := ceph.ListRBDNamespaces(ctx, site.cluster, config.Pool)
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

func (m *RBDMirror) checkRBDMirrorPools(ctx context.Context) error {
	if m.poolIdentities == nil {
		return nil
	} // Only internal unit fixtures lack capture.
	for _, site := range []struct {
		cluster *ceph.Container
		id      int64
	}{
		{m.config.Source, m.poolIdentities.source}, {m.config.Destination, m.poolIdentities.destination},
	} {
		pool, err := site.cluster.PoolStatus(ctx, m.config.Pool)
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

func readRBDMirrorNamespacePolicy(ctx context.Context, client testcontainers.Container, pool, namespace string, legacy bool) (nativeRBDMirrorPolicy, error) {
	var policy nativeRBDMirrorPolicy
	data, err := exec(ctx, client, "rbd", "mirror", "pool", "info", rbdMirrorNamespaceSpec(pool, namespace), "--format", "json")
	if err != nil {
		return policy, err
	}
	if json.Unmarshal(data, &policy) != nil || !slices.Contains([]string{"disabled", "init-only", "image", "pool"}, policy.Mode) {
		return policy, errors.New("decode RBD mirror namespace policy: invalid native mode")
	}
	if legacy {
		policy.RemoteNamespace = legacyRBDRemoteNamespace(policy.Mode, policy.RemoteNamespace, namespace)
		if policy.Mode != "disabled" && policy.MirrorUUID == "" {
			if policy.MirrorUUID, err = readLegacyRBDMirrorUUID(ctx, client, pool, namespace); err != nil {
				return policy, err
			}
		}
	}
	if policy.Mode != "disabled" && (policy.MirrorUUID == "" || policy.RemoteNamespace == nil) {
		return policy, errors.New("decode RBD mirror namespace policy: missing UUID or remote namespace")
	}
	if namespace != "" && policy.Mode == "init-only" {
		return policy, errors.New("native init-only mode is invalid on a named namespace")
	}
	return policy, nil
}

// legacyClient reports whether client is the setup client of a Ceph 19
// cluster in this link.
func (m *RBDMirror) legacyClient(client testcontainers.Container) bool {
	switch {
	case client == nil:
		return false
	case client == m.sourceClient:
		return cephBefore(m.config.Source, 20)
	case client == m.destinationClient:
		return cephBefore(m.config.Destination, 20)
	}
	return false
}

// readLegacyRBDMirrorUUID reads the mirror UUID that Ceph 19's rbd CLI does
// not print. cls_rbd keeps it as the raw text value of the rbd_mirroring
// object's mirror_uuid omap key.
func readLegacyRBDMirrorUUID(ctx context.Context, client testcontainers.Container, pool, namespace string) (string, error) {
	args := []string{"rados", "--pool", pool}
	if namespace != "" {
		args = append(args, "--namespace", namespace)
	}
	data, err := exec(ctx, client, append(args, "getomapval", "rbd_mirroring", "mirror_uuid", "/dev/stdout")...)
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

func planRBDMirrorSite(client testcontainers.Container, namespace, remote, site string, scope RBDMirrorScope, base, selected nativeRBDMirrorPolicy, legacy bool) ([]rbdMirrorPolicyStep, error) {
	if base.Mode != "disabled" && base.SiteName != site {
		return nil, errors.New("existing cluster-wide RBD mirror site name differs; reconfigure explicitly")
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
			return nil, errors.New("existing RBD mirror namespace scope or remote mapping differs; reconfigure explicitly")
		}
		return steps, nil
	}
	steps = append(steps, rbdMirrorPolicyStep{client: client, namespace: namespace, remote: remote, site: site, mode: string(scope), previous: selected, legacy: legacy})
	return steps, nil
}

func (m *RBDMirror) provisionRBDMirrorPolicies(ctx context.Context) error {
	if m.policyIdentities != nil {
		if err := m.checkRBDMirrorPolicyIdentities(ctx); err != nil {
			return err
		}
	}
	var steps []rbdMirrorPolicyStep
	configuredSites := make(map[string]string)
	// Check both endpoints before the first mutation, so an incompatible
	// destination cannot broaden the source's existing scope.
	for _, site := range []struct {
		client                  testcontainers.Container
		cluster                 *ceph.Container
		namespace, remote, name string
	}{
		{m.sourceClient, m.config.Source, m.config.SourceNamespace, m.config.DestinationNamespace, m.config.SourceSite},
		{m.destinationClient, m.config.Destination, m.config.DestinationNamespace, m.config.SourceNamespace, m.config.DestinationSite},
	} {
		legacy := cephBefore(site.cluster, 20)
		if legacy && site.namespace != site.remote {
			return fmt.Errorf("mirroring RBD namespace %q to %q needs Ceph 20 or later; Ceph 19 pairs only namespaces with the same name", site.namespace, site.remote)
		}
		configuredSite, err := readConfiguredRBDMirrorSite(ctx, site.client)
		if err != nil {
			return err
		}
		if configuredSite != "" && configuredSite != site.name {
			return errors.New("existing cluster-wide RBD mirror site name differs; reconfigure explicitly")
		}
		configuredSites[site.name] = configuredSite
		base, err := readRBDMirrorNamespacePolicy(ctx, site.client, m.config.Pool, "", legacy)
		if err != nil {
			return err
		}
		selected := base
		if site.namespace != "" {
			selected, err = readRBDMirrorNamespacePolicy(ctx, site.client, m.config.Pool, site.namespace, legacy)
			if err != nil {
				return err
			}
		}
		planned, err := planRBDMirrorSite(site.client, site.namespace, site.remote, site.name, m.config.Scope, base, selected, legacy)
		if err != nil {
			return err
		}
		steps = append(steps, planned...)
	}
	for _, step := range steps {
		if err := m.checkRBDMirrorPools(ctx); err != nil {
			return err
		}
		configuredSite, err := readConfiguredRBDMirrorSite(ctx, step.client)
		if err != nil {
			return err
		}
		if configuredSite != configuredSites[step.site] {
			return errors.New("cluster-wide RBD mirror site name changed during setup; refusing mutation")
		}
		current, err := readRBDMirrorNamespacePolicy(ctx, step.client, m.config.Pool, step.namespace, step.legacy)
		if err != nil {
			return err
		}
		if !sameRBDMirrorPolicy(current, step.previous) {
			return errors.New("RBD mirror namespace policy changed during setup; refusing mutation")
		}
		args := []string{"rbd", "mirror", "pool", "enable", "--site-name", step.site, rbdMirrorNamespaceSpec(m.config.Pool, step.namespace), step.mode}
		if step.mode != "init-only" && !step.legacy {
			// Even an empty value is intentional: omission would default to the
			// local namespace and break named -> default mappings.
			args = append(args, "--remote-namespace", step.remote)
		}
		if _, err := exec(ctx, step.client, args...); err != nil {
			return fmt.Errorf("enable RBD mirror %s policy (earlier steps may have persisted): %w", step.mode, err)
		}
		current, err = readRBDMirrorNamespacePolicy(ctx, step.client, m.config.Pool, step.namespace, step.legacy)
		if err != nil {
			return err
		}
		if current.Mode != step.mode || (step.mode != "init-only" && (current.RemoteNamespace == nil || *current.RemoteNamespace != step.remote)) {
			return errors.New("RBD mirror namespace policy readback differs from requested configuration")
		}
		configuredSites[step.site] = step.site
	}
	identities, err := m.readRBDMirrorPolicyIdentities(ctx)
	if err != nil {
		return err
	}
	for _, site := range []struct {
		identity     rbdMirrorSitePolicyIdentity
		remote, name string
	}{
		{identities.source, m.config.DestinationNamespace, m.config.SourceSite},
		{identities.destination, m.config.SourceNamespace, m.config.DestinationSite},
	} {
		if site.identity.selected.Mode != string(m.config.Scope) || site.identity.selected.RemoteNamespace == nil || *site.identity.selected.RemoteNamespace != site.remote || site.identity.base.SiteName != site.name {
			return errors.New("RBD mirror policies changed before identity capture")
		}
	}
	if m.policyIdentities == nil {
		m.policyIdentities = identities
	}
	return m.checkRBDMirrorPolicyIdentities(ctx)
}

type rbdMirrorSitePolicyIdentity struct {
	base, selected nativeRBDMirrorPolicy
}

type rbdMirrorPolicyIdentities struct {
	source, destination rbdMirrorSitePolicyIdentity
}

func (m *RBDMirror) readRBDMirrorPolicyIdentities(ctx context.Context) (*rbdMirrorPolicyIdentities, error) {
	if err := m.checkRBDMirrorPools(ctx); err != nil {
		return nil, err
	}
	identities := &rbdMirrorPolicyIdentities{}
	for _, site := range []struct {
		client    testcontainers.Container
		namespace string
		identity  *rbdMirrorSitePolicyIdentity
	}{
		{m.sourceClient, m.config.SourceNamespace, &identities.source},
		{m.destinationClient, m.config.DestinationNamespace, &identities.destination},
	} {
		base, err := readRBDMirrorNamespacePolicy(ctx, site.client, m.config.Pool, "", m.legacyClient(site.client))
		if err != nil {
			return nil, err
		}
		selected := base
		if site.namespace != "" {
			selected, err = readRBDMirrorNamespacePolicy(ctx, site.client, m.config.Pool, site.namespace, m.legacyClient(site.client))
			if err != nil {
				return nil, err
			}
		}
		site.identity.base, site.identity.selected = base, selected
	}
	return identities, m.checkRBDMirrorPools(ctx)
}

func (m *RBDMirror) checkRBDMirrorPolicyIdentities(ctx context.Context) error {
	if m.policyIdentities == nil {
		if m.poolIdentities != nil {
			return errors.New("RBD mirror policies were not confirmed during setup")
		}
		return nil // Hand-constructed internal tests have no setup identity.
	}
	current, err := m.readRBDMirrorPolicyIdentities(ctx)
	if err != nil {
		return err
	}
	if !sameRBDMirrorPolicy(current.source.base, m.policyIdentities.source.base) || !sameRBDMirrorPolicy(current.source.selected, m.policyIdentities.source.selected) || !sameRBDMirrorPolicy(current.destination.base, m.policyIdentities.destination.base) || !sameRBDMirrorPolicy(current.destination.selected, m.policyIdentities.destination.selected) {
		return errors.New("RBD mirror UUID, scope, remote mapping or site identity changed; refusing mutation")
	}
	return nil
}

// Ceph omits site_name from disabled pool info, although another pool can
// already use the cluster-wide config key. Read only this key's value; do not
// dump peer credentials from the monitor config-key store.
func readConfiguredRBDMirrorSite(ctx context.Context, client testcontainers.Container) (string, error) {
	const key = "rbd/mirror/site_name"
	data, err := exec(ctx, client, "ceph", "config-key", "ls", "--format", "json")
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
	data, err = exec(ctx, client, "ceph", "config-key", "get", key)
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
