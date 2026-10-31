//go:build all || (integration && hostnetwork && (!ci || (ci_short && (!ci_batch || ci_batch_rbd_fixtures_basic))))

//ci: timeout=120m job-timeout=130 image-roles=essential,client

package rbd_test

import (
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// Host-mode services reuse every assertion from their bridge-mode baselines.
// Native clients run in the Docker daemon's host namespace, which belongs to
// the Linux VM on Docker Desktop. They do not use host kernel mounts or cgo.
func TestHostNetworkRBDLifecycle(t *testing.T) {
	harness.RunRBDLifecycle(t, ceph.WithHostNetwork())
}
