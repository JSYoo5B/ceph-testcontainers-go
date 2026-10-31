//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_daemon_basics))))

//ci: timeout=45m job-timeout=55 image-roles=essential,client,rbd-aux

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

func TestDaemonAddresses(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testDaemonAddresses(t)
}

func TestHostNetworkDaemonAddresses(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testDaemonAddresses(t, ceph.WithHostNetwork())
}
