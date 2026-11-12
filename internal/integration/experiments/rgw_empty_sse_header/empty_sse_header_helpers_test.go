//go:build all || integration

package rgw_empty_sse_header_test

import (
	"fmt"
	"net/http"
)

type sseFailure struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// sseResult is the native answer to one PUT.
type sseResult struct {
	Code       int
	Failure    sseFailure
	Encryption string
	VersionID  string
}

// judgeEmptyHeaderPut judges one empty-header PUT against the invalid
// algorithm control. The documented symptom is an accepted PUT (200), which
// writes an object or a version. The validation refusal the control also
// gets (400 InvalidArgument with the same message) with the stored state
// unchanged means the symptom is absent. Any other answer, or a refusal that
// still changed the stored state, is another fault: no verdict.
func judgeEmptyHeaderPut(r sseResult, control sseFailure, changed string) (symptom bool, detail string, err error) {
	switch {
	case r.Code == http.StatusOK:
		return true, fmt.Sprintf("accepted with 200 (%s)", changed), nil
	case r.Code == http.StatusBadRequest && r.Failure.Code == "InvalidArgument" && r.Failure.Message == control.Message && changed == "":
		return false, fmt.Sprintf("refused with 400 InvalidArgument %q, stored state unchanged", r.Failure.Message), nil
	default:
		return false, "", fmt.Errorf("another fault: status %d %s %q, stored state %q; want 200 or 400 InvalidArgument %q", r.Code, r.Failure.Code, r.Failure.Message, changed, control.Message)
	}
}
