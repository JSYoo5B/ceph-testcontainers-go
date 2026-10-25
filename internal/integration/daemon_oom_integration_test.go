//go:build integration && ci_optional && ci_batch_daemon_oom

//ci: timeout=15m job-timeout=25 image-roles=essential,client

package integration_test

import "testing"

// TestDaemonMemoryQualification keeps the experimental OOM admission strict.
func TestDaemonMemoryQualification(t *testing.T) { testDaemonMemoryQualification(t, true) }
