//go:build all || (integration && hostnetwork && multicluster && (!ci || (ci_topology && (!ci_batch || ci_batch_topology_extensions_cephfs_mirror_host))))

//ci: timeout=40m job-timeout=50 image-roles=essential,client,cephfs

package cephfs_test

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"testing"
)

func TestHostNetworkCephFSMirrorDaemonRebalanceTopology(t *testing.T) {
	harness.RunCephFSMirrorDaemonTopology(t, true, true)
}
