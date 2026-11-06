//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_suspended_conditional_writes_test

import "testing"

func TestCondValidator(t *testing.T) {
	for _, etag := range []string{
		`"d41d8cd98f00b204e9800998ecf8427e"`,
		`"9b2cf535f27731c974343645a3985328-1"`,
		`"9b2cf535f27731c974343645a3985328-10000"`,
	} {
		if !condValidator(etag) {
			t.Errorf("condValidator(%q) = false, want true", etag)
		}
	}
	for _, etag := range []string{
		"",
		`""`,
		"d41d8cd98f00b204e9800998ecf8427e",
		`W/"d41d8cd98f00b204e9800998ecf8427e"`,
		`"d41d8cd98f00b204e9800998ecf8427"`,
		`"D41D8CD98F00B204E9800998ECF8427E"`,
		`"9b2cf535f27731c974343645a3985328-0"`,
		`"9b2cf535f27731c974343645a3985328-"`,
		`*`,
		condWrongETag,
	} {
		if condValidator(etag) {
			t.Errorf("condValidator(%q) = true, want false", etag)
		}
	}
}
