//go:build all || (integration && hostnetwork && multicluster && (!ci || (ci_topology && (!ci_batch || ci_batch_topology_extensions_rbd_daemons))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"testing"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

func TestHostNetworkRBDMirrorDaemonTopology(t *testing.T) {
	testMultiClusterRBDMirrorDaemonTopology(t, ceph.WithHostNetwork())
}
