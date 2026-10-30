//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_default))))

//ci: timeout=20m job-timeout=35 image-roles=essential,client,cephfs,rgw

package rbd_test

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"testing"
)

// TestRBDLifecycle verifies userspace RBD I/O via the image's CLI. It does not
// map a kernel block device or mount a filesystem on an RBD image.
func TestRBDLifecycle(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	harness.RunRBDLifecycle(t)
}
