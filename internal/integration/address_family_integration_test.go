//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_address_family))))

//ci: timeout=25m job-timeout=35 image-roles=essential,client,cephfs,rgw

package integration_test

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestIPv6Listeners(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testAddressFamily(t, ceph.AddressIPv6)
}

func TestIPv4MappedIPv6Listeners(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testAddressFamily(t, ceph.AddressIPv4MappedIPv6)
}
