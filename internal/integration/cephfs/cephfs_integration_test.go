//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_default))))

//ci: timeout=20m job-timeout=35 image-roles=essential,client,cephfs,rgw

package cephfs_test

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"testing"
)

func TestCephFSFilesystem(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testCephFSFilesystem(t)
}
