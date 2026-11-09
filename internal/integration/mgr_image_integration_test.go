//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_mgr_image))))

//ci: timeout=40m job-timeout=55 image-roles=essential,client,mgr-modules

package integration_test

import "testing"

func TestMGRImageOfficialSlot(t *testing.T) {
	testMGRImageOfficialSlot(t)
}

func TestMGRImageWithoutEssentialManager(t *testing.T) {
	testMGRImageWithoutEssentialManager(t)
}

func TestMGRImageConsumerModule(t *testing.T) {
	testMGRImageConsumerModule(t)
}

func TestMGRImageOtherReleaseNeverStarts(t *testing.T) {
	testMGRImageOtherReleaseNeverStarts(t)
}
