//go:build all || (integration && (!ci || (ci_recovery && (!ci_batch || ci_batch_clock_offset))))

//ci: timeout=30m job-timeout=45 image-roles=essential,client architecture=arm64

package integration_test

import "testing"

// TestClockOffsetMonitors offsets the wall clock of a follower and of the
// leader MON by plus and minus two seconds through fault.ApplyClockOffset,
// and requires MON_CLOCK_SKEW to appear and clear in the same processes,
// with the quorum kept, CLOCK_MONOTONIC unchanged and a held lease restored
// by Terminate.
func TestClockOffsetMonitors(t *testing.T) { testClockOffsetMonitors(t) }

// TestClockOffsetUnarmedMonitor checks that a cluster without ClockConfig
// starts its MON without the interposer and refuses an offset.
func TestClockOffsetUnarmedMonitor(t *testing.T) { testClockOffsetUnarmed(t) }
