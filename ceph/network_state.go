package ceph

import (
	"context"

	"github.com/testcontainers/testcontainers-go"
)

// networkState is the Docker bridge state of one cluster: the public bridge,
// the optional replication bridge, the Ceph subnets of both planes and whether
// Terminate has removed each bridge. It is the first component of the internal
// state split described in docs/design/container-decomposition.md and owns no
// lock of its own, so every lock-order rule of Container applies unchanged.
//
// Guards, per field group:
//   - public, cluster, publicSubnet and clusterSubnet are the recorded
//     bridges. Run writes them while it builds the cluster, before the cluster
//     is returned, and nothing writes them afterwards (the unit-test builder
//     TestCluster is the one other writer, on a fresh cluster). Readers need no
//     lock, which FactsOf relies on after termination.
//     TestClusterFactsRecordedSourcesAreNotRewrittenOutsideStartup enforces it.
//   - publicRemoved and clusterRemoved are written only by removeBridges and
//     read only by it, so they are guarded by Container.mu, which terminate
//     holds from lockTopology until it returns.
type networkState struct {
	public, cluster *testcontainers.DockerNetwork
	publicSubnet    string
	clusterSubnet   string

	publicRemoved, clusterRemoved bool
}

// bridges returns the recorded bridges, the public one first. A host-network
// cluster has none, and a cluster without a replication network has one.
func (n *networkState) bridges() []*testcontainers.DockerNetwork {
	var out []*testcontainers.DockerNetwork
	for _, nw := range []*testcontainers.DockerNetwork{n.public, n.cluster} {
		if nw != nil {
			out = append(out, nw)
		}
	}
	return out
}

// bridgeNames returns the names of bridges, in the same order.
func (n *networkState) bridgeNames() []string {
	out := []string{}
	for _, nw := range n.bridges() {
		out = append(out, nw.Name)
	}
	return out
}

// removeBridges removes the replication bridge and then the public bridge
// that are recorded and not removed yet, except those that blocked names. A
// bridge whose removal fails stays recorded as present, so a later Terminate
// retries it, and the failure is returned. The caller holds Container.mu.
func (n *networkState) removeBridges(ctx context.Context, blocked map[string]bool) []error {
	var errs []error
	if n.cluster != nil && !n.clusterRemoved && !blocked[n.cluster.Name] {
		if err := n.cluster.Remove(ctx); !onlyMissingHostResource(err) {
			errs = append(errs, err)
		} else {
			n.clusterRemoved = true
		}
	}
	if n.public != nil && !n.publicRemoved && !blocked[n.public.Name] {
		if err := n.public.Remove(ctx); !onlyMissingHostResource(err) {
			errs = append(errs, err)
		} else {
			n.publicRemoved = true
		}
	}
	return errs
}
