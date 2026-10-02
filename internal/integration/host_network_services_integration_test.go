//go:build integration && hostnetwork

package integration_test

import (
	"testing"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

// Host-mode services reuse every assertion from their bridge-mode baselines.
// Native clients run in the Docker daemon's host namespace, which belongs to
// the Linux VM on Docker Desktop. They do not use host kernel mounts or cgo.
func TestHostNetworkRBDLifecycle(t *testing.T) {
	testRBDLifecycle(t, ceph.WithHostNetwork())
}

func TestHostNetworkCephFSFilesystem(t *testing.T) {
	testCephFSFilesystem(t, ceph.WithHostNetwork())
}
