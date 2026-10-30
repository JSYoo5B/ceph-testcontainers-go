//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_client_auth))))

//ci: timeout=25m job-timeout=35 representative=auth image-roles=essential,client

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

func TestClientKeyReplacement(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	harness.RunClientKeyReplacement(t)
}
