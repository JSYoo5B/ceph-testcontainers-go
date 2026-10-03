//go:build integration && hostnetwork && multicluster

package integration_test

import (
	"testing"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

func TestHostNetworkRBDMirrorDaemonTopology(t *testing.T) {
	testMultiClusterRBDMirrorDaemonTopology(t, ceph.WithHostNetwork())
}
