//go:build all || (integration && hostnetwork && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_rbd))))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,rbd-aux

package rbd_test

import (
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// Host-mode federation reuses the complete bridge-mode native mirror and
// backup assertions. Each cluster retains its own FSID, credentials and data.
func TestHostNetworkRBDSnapshotMirror(t *testing.T) {
	harness.RunMultiClusterRBDSnapshotMirror(t, ceph.WithHostNetwork())
}
