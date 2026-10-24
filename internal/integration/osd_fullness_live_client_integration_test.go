//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_osd_fullness_live_client))))

//ci: timeout=25m job-timeout=35 image-roles=essential,client

package integration_test

import "testing"

// TestOSDFullnessLiveClientDuringBarrier keeps one retained live writer's
// operation ledger across the backfillfull policy barrier (a granted
// reservation revoked, and a reservation refused at admission) and its
// restoration. It records how the writer's operations ended (served, blocked,
// failed or unknown) and compares that with what the pinned source promises
// for client writes under this policy, which is that they are served. Every
// operation keeps the outcome the writer recorded, nothing is replayed, and an
// independent client reads back every acknowledged object after the policy is
// lifted. It does not cover recovery_toofull or the full ratio, revocation
// after bytes were transferred, crossing min_size during a transfer, a second
// OSD fault, or physical capacity exhaustion.
func TestOSDFullnessLiveClientDuringBarrier(t *testing.T) { testOSDFullnessLiveClient(t) }
