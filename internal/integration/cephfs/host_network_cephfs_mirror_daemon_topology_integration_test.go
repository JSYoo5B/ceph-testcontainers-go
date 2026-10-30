//go:build (all || (integration && hostnetwork && multicluster)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_native_shuffle)))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,cephfs

package cephfs_test

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"testing"
)

func TestHostNetworkCephFSMirrorDaemonTopology(t *testing.T) {
	harness.RunCephFSMirrorDaemonTopology(t, true)
}
