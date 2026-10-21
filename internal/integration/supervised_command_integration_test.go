//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_supervised_command))))

//ci: timeout=10m job-timeout=20 image-roles=essential

package integration_test

import "testing"

func TestSupervisedCommandCancellation(t *testing.T) {
	parallelWhenEnabled(t)
	testSupervisedCommandCancellation(t)
}
