//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_default))))

//ci: timeout=20m job-timeout=35

package integration_test

import (
	"testing"
)

// TestRBDLifecycle verifies userspace RBD I/O via the image's CLI. It does not
// map a kernel block device or mount a filesystem on an RBD image.
func TestRBDLifecycle(t *testing.T) {
	testRBDLifecycle(t)
}
