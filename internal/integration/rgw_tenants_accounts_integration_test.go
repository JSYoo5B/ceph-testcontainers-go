//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rgw_fixtures))))

//ci: timeout=120m job-timeout=130

package integration_test

import (
	"testing"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

func TestRGWTenantsAndAccounts(t *testing.T) {
	parallelWhenEnabled(t)
	testRGWTenantsAndAccounts(t)
}

func TestHostNetworkRGWTenantsAndAccounts(t *testing.T) {
	parallelWhenEnabled(t)
	testRGWTenantsAndAccounts(t, ceph.WithHostNetwork())
}
