//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_clients))))

//ci: timeout=45m job-timeout=55 image-roles=essential,client,cephfs,rbd-aux,rgw

package cephfs_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// Two live libcephfs clients with client metrics enabled open one file. The
// active MDS's per-client counters show both rank rows at zero before the
// work gate; after only the writer's acknowledged write and fsync, the
// writer's row covers the acknowledged bytes while the idle client's row
// stays at zero. A third client reads the exact bytes back. The verdict is
// the Docker-free counter-window judge's.
func TestCephFSClientMetrics(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testCephFSClientMetrics(t)
}
