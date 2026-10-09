//go:build all || (integration && hostnetwork && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_rbd))))

//ci: timeout=60m job-timeout=70

package integration_test

import (
	"testing"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

// Host-mode federation reuses the complete bridge-mode native mirror and
// backup assertions. Each cluster retains its own FSID, credentials and data.
func TestHostNetworkRBDSnapshotMirror(t *testing.T) {
	testMultiClusterRBDSnapshotMirror(t, ceph.WithHostNetwork())
}
