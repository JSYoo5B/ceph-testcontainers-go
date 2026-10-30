//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_default))))

//ci: timeout=20m job-timeout=35 image-roles=essential,client,cephfs,rgw

package rgw_test

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestRGWUserAdministration(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testRGWUserAdministration(t)
}

func TestHostNetworkRGWUserAdministration(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testRGWUserAdministration(t, ceph.WithHostNetwork())
}
