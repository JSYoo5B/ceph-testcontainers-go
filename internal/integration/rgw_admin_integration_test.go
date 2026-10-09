//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_default))))

//ci: timeout=20m job-timeout=35

package integration_test

import (
	"testing"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

func TestRGWUserAdministration(t *testing.T) {
	parallelWhenEnabled(t)
	testRGWUserAdministration(t)
}

func TestHostNetworkRGWUserAdministration(t *testing.T) {
	parallelWhenEnabled(t)
	testRGWUserAdministration(t, ceph.WithHostNetwork())
}
