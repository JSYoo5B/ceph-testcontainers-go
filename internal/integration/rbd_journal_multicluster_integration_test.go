//go:build all || (integration && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_rbd))))

//ci: timeout=60m job-timeout=70

package integration_test

import (
	"testing"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// TestMultiClusterRBDJournalMirrorFailback uses native librbd writes and fresh
// reads. It never creates mirror snapshots: journal replay carries changes,
// including a receiver restart and planned primary ownership A -> B -> A.
func TestMultiClusterRBDJournalMirrorFailback(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			var opts []testcontainers.ContainerCustomizer
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			testMultiClusterRBDJournalMirrorFailback(t, opts...)
		})
	}
}
