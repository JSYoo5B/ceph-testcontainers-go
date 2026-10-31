//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_address_families))))

//ci: timeout=50m job-timeout=60 image-roles=essential,client,cephfs,rgw

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// TestDualStackReplacement replaces a MON, an OSD, the MDS and the MGR of an
// AddressDualStack cluster with new containers, one at a time, while one
// native client keeps its librados and libcephfs sessions open. After each
// replacement it checks that the new daemon advertises concrete addresses of
// both families that differ from the old daemon's, that the maps dropped the
// old daemon, and that the retained client completed its operations through
// the new daemon over the family its vector lists first.
func TestDualStackReplacement(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testDualStackReplacement(t)
}
