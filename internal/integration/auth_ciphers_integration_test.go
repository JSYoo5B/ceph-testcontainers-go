//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_daemon_basics))))

//ci: timeout=45m job-timeout=55 image-roles=essential,client,rbd-aux

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

func TestAuthCiphersAndTicketLifetimes(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	image, imageOpts := harness.IntegrationImages(t)
	runAuthCiphers(t, image, imageOpts, false)
}

// TestAuthCiphersWithoutKeyTypeOption runs the negative control image, the
// official Tentacle 20.2.3 image, for every role, independently of the
// release under test. It fails when that image has key type support.
func TestAuthCiphersWithoutKeyTypeOption(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	runAuthCiphers(t, tentacleWithoutKeyTypes, nil, true)
}
