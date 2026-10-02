//go:build integration && hostnetwork && multicluster

package integration_test

import (
	"testing"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

func TestHostNetworkRGWMultisite(t *testing.T) {
	testMultiClusterRGWMultisite(t, ceph.WithHostNetwork())
}

func TestHostNetworkRGWThreeZoneTopology(t *testing.T) {
	testRGWThreeZoneTopology(t, true)
}
