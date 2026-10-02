//go:build integration && hostnetwork && multicluster

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

func TestHostNetworkCephFSSnapshotMirrorAndBackup(t *testing.T) {
	testMultiClusterCephFSSnapshotMirrorAndBackup(t, ceph.WithHostNetwork())
}
