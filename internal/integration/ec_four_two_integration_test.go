//go:build all || (integration && (!ci || (ci_recovery && (!ci_batch || ci_batch_ec_four_two))))

//ci: timeout=40m job-timeout=50 image-roles=essential,client

package integration_test

import "testing"

func TestECFourTwoAdmissionBoundaries(t *testing.T) {
	for _, policy := range []struct {
		name    string
		minSize int
	}{{"default", 0}, {"permissive", 4}} {
		t.Run(policy.name, func(t *testing.T) { testECFourTwoAdmission(t, policy.minSize) })
	}
}
