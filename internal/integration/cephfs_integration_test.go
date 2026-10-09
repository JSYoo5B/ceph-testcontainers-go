//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_default))))

//ci: timeout=20m job-timeout=35

package integration_test

import (
	"testing"
)

func TestCephFSFilesystem(t *testing.T) {
	testCephFSFilesystem(t)
}
