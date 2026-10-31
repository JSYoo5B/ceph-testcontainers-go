//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_rbd_sparse_read))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// The rbd CLI exports an image with two written extents through librbd,
// which reads each data object with CEPH_OSD_OP_SPARSE_READ. Each request,
// the OSD's reply and the client's receipt join on tid, object and range; a
// written object returns only its extent and the extent map, an absent one
// -ENOENT, and the export equals the written image.
func TestRBDSparseReadReceipt(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testRBDSparseReadReceipt(t)
}
