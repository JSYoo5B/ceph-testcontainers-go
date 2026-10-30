//go:build all || (integration && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_rbd))))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,rbd-aux

package rbd_test

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// TestMultiClusterRBDJournalMirrorFailback uses native librbd writes and fresh
// reads. It never creates mirror snapshots: journal replay carries changes,
// including a receiver restart and planned primary ownership A -> B -> A.
func TestMultiClusterRBDJournalMirrorFailback(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		host, name := mode.Host, mode.Name
		t.Run(name, func(t *testing.T) {
			var opts []testcontainers.ContainerCustomizer
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			harness.RunMultiClusterRBDJournalMirrorFailback(t, opts...)
		})
	}
}
