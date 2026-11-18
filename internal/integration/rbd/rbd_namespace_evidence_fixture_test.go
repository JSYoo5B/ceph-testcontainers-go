//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rbd_test

import (
	"errors"
	"testing"
)

func TestRBDNamespaceAbsenceRequiresSuccessfulListing(t *testing.T) {
	for _, tc := range []struct {
		name    string
		code    int
		listing string
		err     error
		want    bool
	}{
		{"other images", 0, `["volume"]`, nil, true},
		{"empty namespace", 0, `[]`, nil, true},
		{"present", 0, `["plain"]`, nil, false},
		{"permission failure", 13, `[]`, nil, false},
		{"missing namespace", 2, `[]`, nil, false},
		{"transport failure", 0, `[]`, errors.New("connection failed"), false},
		{"null", 0, `null`, nil, false},
		{"malformed", 0, `{}`, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := rbdNamespaceExcludesImage(tc.code, []byte(tc.listing), tc.err, "plain"); (err == nil) != tc.want {
				t.Fatalf("absence error=%v, want success=%t", err, tc.want)
			}
		})
	}
}
