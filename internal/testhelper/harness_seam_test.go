package testhelper

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
)

// The test-only tenant lookup is served by the value that ceph/harness
// registers; importing this package imports the harness, so the slot is set.
func TestHarnessTenantSlotServesTheTenantClusterLookup(t *testing.T) {
	if _, ok := hookreg.HarnessTenant.(interface {
		TenantCluster(t *harness.Tenant) *ceph.Container
	}); !ok {
		t.Fatal("ceph/harness did not register the tenant cluster lookup")
	}
}
