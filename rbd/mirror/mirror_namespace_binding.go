package mirror

import (
	"context"
	"encoding/json"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"slices"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// Namespace is a retained read-only binding of already configured
// namespace policies to one Mirror's original pool, peer and owned cohort.
// It owns no resources and has no independent daemon or cleanup lifecycle.
// An actual owner Rebootstrap attempt invalidates this binding permanently;
// after successful Rebootstrap explicitly BindNamespace again for a new view.
type Namespace struct {
	owner *Mirror
	scope rbdReceiverScope
}

type rbdReceiverScope struct {
	selection [3]string // pool, source namespace, destination namespace
	policies  *rbdMirrorPolicyIdentities
	binding   *rbdNamespaceBindingIdentity
}

type rbdNamespaceBindingIdentity struct {
	owner                           *Mirror
	peer                            *rbdReceiverPeerIdentity
	clusters                        *rbdReceiverClusterIdentities
	pools                           *rbdMirrorPoolIdentities
	originalPolicies                *rbdMirrorPolicyIdentities
	sourceClient, destinationClient testcontainers.Container
}

func (p *rbdMirrorPair) originalReceiverScope() *rbdReceiverScope {
	return &rbdReceiverScope{selection: [3]string{p.config.Pool, p.config.SourceNamespace, p.config.DestinationNamespace}, policies: p.policyIdentities}
}

// Called with the original owner's gate held. A scope never redirects ownership
// and never replaces the owner's configuration, policy capture or mutex.
func (scope *rbdReceiverScope) guard(m *Mirror) error {
	if scope == nil || scope.selection[0] != m.pair.config.Pool || scope.policies == nil {
		return rbdReceiverGuard("RBD receiver scope is unavailable or changed")
	}
	if b := scope.binding; b != nil {
		if b.owner != m || b.peer == nil || b.peer != m.receiver.peer || b.peer.generation != m.receiver.generation || b.clusters != m.receiver.clusters || b.pools != m.pair.poolIdentities || b.originalPolicies != m.pair.policyIdentities || !sameRBDReceiverHandle(b.sourceClient, m.pair.sourceClient) || !sameRBDReceiverHandle(b.destinationClient, m.pair.destinationClient) {
			return rbdReceiverGuard("RBD namespace binding original owner or bootstrap authority changed")
		}
	}
	return nil
}

// BindNamespace reads existing reciprocal image/pool policies without changing
// policies, peers, images or containers. Empty names select the actual default
// namespace; named namespaces must exist. Both selected native scopes must match.
// The original successful owner setup and captured base policies remain required.
// Binding takes at most 30 seconds, bounded by ctx, including owner admission.
// Repeated calls produce separate immutable views, not resource owners or retries.
//
// Binding the default namespace requires an active reciprocal default policy; a
// default namespace in init-only mode cannot be bound. Configure an extra
// namespace pair with SourceRBD and DestinationRBD instead of a second Run,
// then bind it here. A Rebootstrap that reaches the native bootstrap step
// invalidates every earlier view permanently, even when it fails, and Terminate
// invalidates them too; bind again after a successful Rebootstrap.
func (m *Mirror) BindNamespace(ctx context.Context, sourceNamespace, destinationNamespace string) (*Namespace, error) {
	if m == nil {
		return nil, rbdReceiverGuard("RBD namespace binding owner is unavailable")
	}
	for _, name := range []string{sourceNamespace, destinationNamespace} {
		if name != "" && !rbdMirrorNamespaceName.MatchString(name) {
			return nil, rbdReceiverGuard("RBD namespace binding names are invalid")
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := multicluster.LockObservation(ctx, &m.mu); err != nil {
		return nil, rbdReceiverQuery(ctx, "admit RBD namespace binding", err)
	}
	defer m.mu.Unlock()
	if _, err := m.receiverWitness(m.pair.originalReceiverScope(), nil, nil); err != nil {
		return nil, err
	}
	if err := m.receiver.checkIdentities(ctx, &m.pair); err != nil {
		return nil, err
	}
	view := &Namespace{owner: m, scope: rbdReceiverScope{selection: [3]string{m.pair.config.Pool, sourceNamespace, destinationNamespace}, binding: &rbdNamespaceBindingIdentity{owner: m, peer: m.receiver.peer, clusters: m.receiver.clusters, pools: m.pair.poolIdentities, originalPolicies: m.pair.policyIdentities, sourceClient: m.pair.sourceClient, destinationClient: m.pair.destinationClient}}}
	policies, err := m.pair.readBindingPolicies(ctx, &view.scope)
	if err != nil {
		return nil, err
	}
	view.scope.policies = policies
	if err := m.checkRBDReceiverScopeIdentities(ctx, &view.scope); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, rbdReceiverQuery(ctx, "confirm RBD namespace binding", err)
	}
	return view, nil
}

// ReceiverStatus observes this bound namespace pair using the original owner's
// exact current owned cohort. Zero owned daemons is non-ready. It shares the
// owner's 30-second admission/process deadline and never creates a receiver.
func (v *Namespace) ReceiverStatus(ctx context.Context, expectedDaemons ...string) (ReceiverStatus, error) {
	if v == nil || v.owner == nil || v.scope.binding == nil {
		return ReceiverStatus{}, rbdReceiverGuard("RBD namespace binding is unavailable")
	}
	status, _, err := v.owner.receiverStatusForScope(ctx, &v.scope, expectedDaemons, nil)
	return status, err
}

// WaitReceiverReady waits up to four minutes, bounded by ctx, for this bound
// namespace's discovery and exact owner-cohort election. It pins the originally
// admitted cohort; replacement handles and new peer generations are not adopted.
// Same-container restart may change its native instance. Polls release the owner
// gate. Ready does not prove replay or delivery of any image/write/checkpoint.
func (v *Namespace) WaitReceiverReady(ctx context.Context, expectedDaemons ...string) (ReceiverStatus, error) {
	if v == nil || v.owner == nil || v.scope.binding == nil {
		return ReceiverStatus{}, rbdReceiverGuard("RBD namespace binding is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	return waitRBDReceiverReady(ctx, time.Second, func(ctx context.Context, previous *rbdReceiverWitness) (ReceiverStatus, *rbdReceiverWitness, error) {
		return v.owner.receiverStatusForScope(ctx, &v.scope, expectedDaemons, previous)
	})
}

func (p *rbdMirrorPair) readBindingPolicies(ctx context.Context, scope *rbdReceiverScope) (*rbdMirrorPolicyIdentities, error) {
	policies := &rbdMirrorPolicyIdentities{}
	for _, site := range []struct {
		client                      testcontainers.Container
		namespace, remote, siteName string
		base                        nativeRBDMirrorPolicy
		result                      *rbdMirrorSitePolicyIdentity
	}{
		{p.sourceClient, scope.selection[1], scope.selection[2], p.config.SourceSite, p.policyIdentities.source.base, &policies.source},
		{p.destinationClient, scope.selection[2], scope.selection[1], p.config.DestinationSite, p.policyIdentities.destination.base, &policies.destination},
	} {
		if err := readRBDNamespaceBindingCatalog(ctx, site.client, scope.selection[0], site.namespace); err != nil {
			return nil, err
		}
		selected, _, err := readRBDReceiverPolicy(ctx, site.client, scope.selection[0], site.namespace, p.legacyClient(site.client))
		if err != nil {
			return nil, err
		}
		// Named native policies may omit site_name: the original pool-level site
		// remains authoritative. Preserve the exact selected readback nonetheless.
		if !slices.Contains([]string{"image", "pool"}, selected.Mode) || selected.RemoteNamespace == nil || *selected.RemoteNamespace != site.remote || site.base.SiteName != site.siteName || selected.SiteName != "" && selected.SiteName != site.siteName {
			return nil, rbdReceiverGuard("RBD namespace binding selected scope, remote mapping or site is incompatible")
		}
		site.result.base, site.result.selected = site.base, selected
	}
	if policies.source.selected.Mode != policies.destination.selected.Mode {
		return nil, rbdReceiverGuard("RBD namespace binding selected scopes differ")
	}
	return policies, nil
}

func readRBDNamespaceBindingCatalog(ctx context.Context, client testcontainers.Container, pool, namespace string) error {
	if namespace == "" {
		return nil // Ceph deliberately omits the default namespace from this list.
	}
	data, err := rbdReceiverExec(ctx, client, "read RBD namespace binding catalog", "rbd", "namespace", "list", "--pool", pool, "--format", "json")
	if err != nil {
		return err
	}
	if err := rbdReceiverJSON(data); err != nil {
		return err
	}
	var entries []struct {
		Name *string `json:"name"`
	}
	if json.Unmarshal(data, &entries) != nil || entries == nil {
		return rbdReceiverGuard("decode RBD namespace binding catalog")
	}
	seen := make(map[string]bool)
	for _, entry := range entries {
		if entry.Name == nil || *entry.Name == "" || seen[*entry.Name] {
			return rbdReceiverGuard("decode RBD namespace binding catalog identity")
		}
		seen[*entry.Name] = true
	}
	if !seen[namespace] {
		return rbdReceiverGuard("RBD namespace binding named namespace is absent")
	}
	return nil
}

func (m *Mirror) checkRBDReceiverScopeIdentities(ctx context.Context, scope *rbdReceiverScope) error {
	if err := scope.guard(m); err != nil {
		return err
	}
	if err := m.receiver.checkIdentities(ctx, &m.pair); err != nil {
		return err
	}
	if scope.binding == nil {
		return nil // Keep the original owner observation's exact query contract.
	}
	current, err := m.pair.readBindingPolicies(ctx, scope)
	if err != nil {
		return err
	}
	if !sameRBDMirrorPolicy(current.source.selected, scope.policies.source.selected) || !sameRBDMirrorPolicy(current.destination.selected, scope.policies.destination.selected) {
		return rbdReceiverGuard("RBD namespace binding original selected policy changed")
	}
	if err := m.receiver.checkIdentities(ctx, &m.pair); err != nil {
		return err
	}
	return scope.guard(m)
}
