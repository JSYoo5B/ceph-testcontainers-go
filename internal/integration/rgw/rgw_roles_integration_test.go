//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rgw_fixtures))))

//ci: timeout=120m job-timeout=130 image-roles=essential,client,rgw

package rgw_test

import "testing"

// TestRGWOwnedRole creates an owned STS role in a real gateway whose STS is
// enabled with only a session-token key and STS authentication. A fresh AWS
// SDK process, which plays the SUT, gets the role's ARN: an untrusted
// principal's AssumeRole is refused, and the trusted principal's temporary
// credentials read the allowed object and are refused for another object
// and for a write. Native readback confirms the role's identity, trust and
// policy. The fixture refuses a caller's role of the same name and a role
// carrying a caller's policy, creates nothing on a canceled call, removes a
// role whose policy RGW refused, and its Remove leaves the caller's role
// unchanged while STS refuses the removed role.
func TestRGWOwnedRole(t *testing.T) {
	rgwRoles(t)
}
