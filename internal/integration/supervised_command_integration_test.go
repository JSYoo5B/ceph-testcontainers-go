//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_supervised_command))))

//ci: timeout=10m job-timeout=20 image-roles=essential

package integration_test

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"testing"
)

func TestSupervisedCommandCancellation(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testSupervisedCommandCancellation(t)
}
