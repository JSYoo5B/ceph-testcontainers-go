//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_daemon_addresses))))

//ci: timeout=25m job-timeout=35 image-roles=essential,client

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestDaemonAddresses(t *testing.T) {
	parallelWhenEnabled(t)
	testDaemonAddresses(t)
}

func TestHostNetworkDaemonAddresses(t *testing.T) {
	parallelWhenEnabled(t)
	testDaemonAddresses(t, ceph.WithHostNetwork())
}
