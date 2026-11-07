//go:build all || (integration && multicluster && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_test

import "testing"

func TestRGWCommittedEndpointRequiresTheLiveListener(t *testing.T) {
	const reserved, other = "http://192.0.2.10:7480", "http://192.0.2.10:7481"
	if !rgwPeriodHasEndpoint([]string{reserved}, reserved, reserved) {
		t.Fatal("rejected the matching committed listener")
	}
	if rgwPeriodHasEndpoint([]string{reserved}, reserved, other) || rgwPeriodHasEndpoint([]string{other}, reserved, reserved) ||
		rgwPeriodHasEndpoint([]string{reserved, other}, reserved, reserved) || rgwPeriodHasEndpoint([]string{""}, "", "") {
		t.Fatal("accepted a stale, extra or missing committed listener")
	}
}
