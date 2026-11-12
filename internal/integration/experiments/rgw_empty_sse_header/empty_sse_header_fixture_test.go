//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_empty_sse_header_test

import (
	"strings"
	"testing"
)

func TestJudgeEmptyHeaderPut(t *testing.T) {
	control := sseFailure{Code: "InvalidArgument", Message: "invalid encryption algorithm"}
	refused := sseResult{Code: 400, Failure: control}
	for _, c := range []struct {
		name    string
		put     sseResult
		changed string
		symptom bool
		fault   string
	}{
		{"accepted new key", sseResult{Code: 200}, "the PUT left an object", true, ""},
		{"accepted without a visible change", sseResult{Code: 200}, "", true, ""},
		{"refused like the control", refused, "", false, ""},
		{"refused but the version changed", refused, "the key changed", false, "another fault"},
		{"refused with another message", sseResult{Code: 400, Failure: sseFailure{Code: "InvalidArgument", Message: "other"}}, "", false, "another fault"},
		{"signature refusal", sseResult{Code: 403, Failure: sseFailure{Code: "SignatureDoesNotMatch"}}, "", false, "another fault"},
		{"server error", sseResult{Code: 500}, "", false, "another fault"},
	} {
		symptom, detail, err := judgeEmptyHeaderPut(c.put, control, c.changed)
		if c.fault != "" {
			if err == nil || !strings.HasPrefix(err.Error(), c.fault) {
				t.Errorf("%s: %v, want %s", c.name, err, c.fault)
			}
			continue
		}
		if err != nil || symptom != c.symptom || detail == "" {
			t.Errorf("%s: symptom=%t %q %v, want symptom=%t", c.name, symptom, detail, err, c.symptom)
		}
	}
}
