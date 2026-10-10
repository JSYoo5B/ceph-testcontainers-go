package multicluster

import (
	"context"
	"encoding/json"
	"slices"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// RBDMirrorNamespace is a retained read-only binding of already configured
// namespace policies to one RBDMirror's original pool, peer and owned cohort.
// It owns no resources and has no independent daemon or cleanup lifecycle.
// An actual owner Rebootstrap attempt invalidates this binding permanently;
// after successful Rebootstrap explicitly BindNamespace again for a new view.
type RBDMirrorNamespace struct {
	owner *RBDMirror
	scope rbdReceiverScope
}

type rbdReceiverScope struct {
	selection [3]string // pool, source namespace, destination namespace
	policies  *rbdMirrorPolicyIdentities
	binding   *rbdNamespaceBindingIdentity
}

type rbdNamespaceBindingIdentity struct {
	owner                           *RBDMirror
	peer                            *rbdReceiverPeerIdentity
	clusters                        *rbdReceiverClusterIdentities
	pools                           *rbdMirrorPoolIdentities
	originalPolicies                *rbdMirrorPolicyIdentities
	sourceClient, destinationClient testcontainers.Container
}

func (m *RBDMirror) originalReceiverScope() *rbdReceiverScope {
	return &rbdReceiverScope{selection: [3]string{m.config.Pool, m.config.SourceNamespace, m.config.DestinationNamespace}, policies: m.policyIdentities}
}

// Called with the original owner's gate held. A scope never redirects ownership
// and never replaces the owner's configuration, policy capture or mutex.
func (scope *rbdReceiverScope) guard(m *RBDMirror) error {
	if scope == nil || scope.selection[0] != m.config.Pool || scope.policies == nil {
		return rbdReceiverGuard("RBD receiver scope is unavailable or changed")
	}
	if b := scope.binding; b != nil {
		if b.owner != m || b.peer == nil || b.peer != m.receiverPeer || b.peer.generation != m.receiverPeerGeneration || b.clusters != m.receiverClusters || b.pools != m.poolIdentities || b.originalPolicies != m.policyIdentities || !sameRBDReceiverHandle(b.sourceClient, m.sourceClient) || !sameRBDReceiverHandle(b.destinationClient, m.destinationClient) {
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
func (m *RBDMirror) BindNamespace(ctx context.Context, sourceNamespace, destinationNamespace string) (*RBDMirrorNamespace, error) {
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
	if err := lockRGWSyncObservation(ctx, &m.mu); err != nil {
		return nil, rbdReceiverQuery(ctx, "admit RBD namespace binding", err)
	}
	defer m.mu.Unlock()
	if _, err := m.receiverWitness(m.originalReceiverScope(), nil, nil); err != nil {
		return nil, err
	}
	if err := m.checkRBDReceiverIdentities(ctx); err != nil {
		return nil, err
	}
	view := &RBDMirrorNamespace{owner: m, scope: rbdReceiverScope{selection: [3]string{m.config.Pool, sourceNamespace, destinationNamespace}, binding: &rbdNamespaceBindingIdentity{owner: m, peer: m.receiverPeer, clusters: m.receiverClusters, pools: m.poolIdentities, originalPolicies: m.policyIdentities, sourceClient: m.sourceClient, destinationClient: m.destinationClient}}}
	policies, err := m.readRBDNamespaceBindingPolicies(ctx, &view.scope)
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
func (v *RBDMirrorNamespace) ReceiverStatus(ctx context.Context, expectedDaemons ...string) (RBDMirrorReceiverStatus, error) {
	if v == nil || v.owner == nil || v.scope.binding == nil {
		return RBDMirrorReceiverStatus{}, rbdReceiverGuard("RBD namespace binding is unavailable")
	}
	status, _, err := v.owner.receiverStatusForScope(ctx, &v.scope, expectedDaemons, nil)
	return status, err
}

// WaitReceiverReady waits up to four minutes, bounded by ctx, for this bound
// namespace's discovery and exact owner-cohort election. It pins the originally
// admitted cohort; replacement handles and new peer generations are not adopted.
// Same-container restart may change its native instance. Polls release the owner
// gate. Ready does not prove replay or delivery of any image/write/checkpoint.
func (v *RBDMirrorNamespace) WaitReceiverReady(ctx context.Context, expectedDaemons ...string) (RBDMirrorReceiverStatus, error) {
	if v == nil || v.owner == nil || v.scope.binding == nil {
		return RBDMirrorReceiverStatus{}, rbdReceiverGuard("RBD namespace binding is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	return waitRBDReceiverReady(ctx, time.Second, func(ctx context.Context, previous *rbdReceiverWitness) (RBDMirrorReceiverStatus, *rbdReceiverWitness, error) {
		return v.owner.receiverStatusForScope(ctx, &v.scope, expectedDaemons, previous)
	})
}

func (m *RBDMirror) readRBDNamespaceBindingPolicies(ctx context.Context, scope *rbdReceiverScope) (*rbdMirrorPolicyIdentities, error) {
	policies := &rbdMirrorPolicyIdentities{}
	for _, site := range []struct {
		client                      testcontainers.Container
		namespace, remote, siteName string
		base                        nativeRBDMirrorPolicy
		result                      *rbdMirrorSitePolicyIdentity
	}{
		{m.sourceClient, scope.selection[1], scope.selection[2], m.config.SourceSite, m.policyIdentities.source.base, &policies.source},
		{m.destinationClient, scope.selection[2], scope.selection[1], m.config.DestinationSite, m.policyIdentities.destination.base, &policies.destination},
	} {
		if err := readRBDNamespaceBindingCatalog(ctx, site.client, scope.selection[0], site.namespace); err != nil {
			return nil, err
		}
		selected, _, err := readRBDReceiverPolicy(ctx, site.client, scope.selection[0], site.namespace, m.legacyClient(site.client))
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

func (m *RBDMirror) checkRBDReceiverScopeIdentities(ctx context.Context, scope *rbdReceiverScope) error {
	if err := scope.guard(m); err != nil {
		return err
	}
	if err := m.checkRBDReceiverIdentities(ctx); err != nil {
		return err
	}
	if scope.binding == nil {
		return nil // Keep the original owner observation's exact query contract.
	}
	current, err := m.readRBDNamespaceBindingPolicies(ctx, scope)
	if err != nil {
		return err
	}
	if !sameRBDMirrorPolicy(current.source.selected, scope.policies.source.selected) || !sameRBDMirrorPolicy(current.destination.selected, scope.policies.destination.selected) {
		return rbdReceiverGuard("RBD namespace binding original selected policy changed")
	}
	if err := m.checkRBDReceiverIdentities(ctx); err != nil {
		return err
	}
	return scope.guard(m)
}
