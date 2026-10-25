//go:build all || (integration && (!ci || (ci_recovery && (!ci_batch || ci_batch_ec_below_k))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package integration_test

import "testing"

func TestECBelowKTemporaryUnavailability(t *testing.T) { testECBelowK(t) }
