//go:build all || (integration && (!ci || (ci_recovery && (!ci_batch || ci_batch_ec_admission))))

//ci: timeout=30m job-timeout=40 image-roles=essential,client

package integration_test

import "testing"

// Default K+1 and explicit K admission are separate pools/fixtures. Each loss
// is chosen from the actual primary or a non-primary position of the complete
// original shard vector. The stores are stopped and retained, then restored.
func TestECPrimaryAdmissionBoundaries(t *testing.T) {
	for _, target := range []struct {
		name    string
		primary bool
	}{{"primary", true}, {"nonprimary", false}} {
		t.Run(target.name, func(t *testing.T) {
			for _, policy := range []struct {
				name    string
				minSize int
			}{{"default", 0}, {"permissive", 2}} {
				t.Run(policy.name, func(t *testing.T) { testECAdmission(t, policy.minSize, target.primary) })
			}
		})
	}
}
