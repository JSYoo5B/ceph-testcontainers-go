//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_native_command))))

//ci: timeout=25m job-timeout=35 image-roles=essential,client

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

func TestNativeCommands(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testNativeCommands(t)
}
