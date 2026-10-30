//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_auth_ciphers))))

//ci: timeout=25m job-timeout=35 image-roles=essential,client

package integration_test

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"testing"
)

func TestAuthCiphersAndTicketLifetimes(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	image, imageOpts := harness.IntegrationImages(t)
	harness.RunAuthCiphers(t, image, imageOpts)
}

// TestAuthCiphersWithoutKeyTypeOption runs the official Tentacle 20.2.3 image
// for every role, independently of the release under test.
func TestAuthCiphersWithoutKeyTypeOption(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	harness.RunAuthCiphers(t, harness.TentacleWithoutKeyTypes, nil)
}
