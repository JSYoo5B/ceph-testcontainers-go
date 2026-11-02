//go:build integration && ci_optional && ci_batch_block_io

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package integration_test

import "testing"

func TestBlockIOMainDeviceRecovery(t *testing.T) { testBlockIOMainDeviceRecovery(t) }
