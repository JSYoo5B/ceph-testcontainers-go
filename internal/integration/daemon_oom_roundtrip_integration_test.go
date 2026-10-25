//go:build integration && ci_optional && ci_batch_daemon_memory_roundtrip

//ci: timeout=15m job-timeout=25 image-roles=essential,client

package integration_test

import "testing"

// TestDaemonMemoryFiniteRoundtrip qualifies the reversible prerequisite alone.
func TestDaemonMemoryFiniteRoundtrip(t *testing.T) { testDaemonMemoryQualification(t, false) }
