//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_empty_sse_header_test

import (
	"net/http"
	"testing"
)

func TestJudgeEmptyPUT(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		status                int
		code, message, sse    string
		unchanged, stored     bool
		reproduced, wantError bool
	}{
		{"ordinary write", http.StatusOK, "", "", "", false, true, true, false},
		{"200 without readback", http.StatusOK, "", "", "", true, false, false, true},
		{"encrypted 200", http.StatusOK, "", "", "AES256", false, true, false, true},
		{"invalid algorithm with different wording", http.StatusBadRequest, "InvalidArgument", "The empty value is invalid", "", true, false, false, false},
		{"invalid algorithm changed state", http.StatusBadRequest, "InvalidArgument", "Invalid algorithm", "", false, true, false, true},
		{"different validation error", http.StatusBadRequest, "InvalidRequest", "", "", true, false, false, true},
		{"authentication refusal", http.StatusForbidden, "SignatureDoesNotMatch", "", "", true, false, false, true},
		{"server failure", http.StatusInternalServerError, "InternalError", "", "", true, false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := judgeEmptyPUT(sseResult{Code: tc.status, Failure: sseFailure{Code: tc.code, Message: tc.message}, Encryption: tc.sse}, tc.unchanged, tc.stored)
			if got != tc.reproduced || (err != nil) != tc.wantError {
				t.Fatalf("reproduced=%t err=%v, want reproduced=%t error=%t", got, err, tc.reproduced, tc.wantError)
			}
		})
	}
}
