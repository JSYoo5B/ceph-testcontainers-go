//go:build all || integration

package rgw_empty_sse_header_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
)

type sseFailure struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

// sseResult is the native answer to one PUT.
type sseResult struct {
	Code       int
	Failure    sseFailure
	Encryption string
	VersionID  string
}

// judgeEmptyPUT requires a successful ordinary write or a validation refusal
// corroborated by independent readback of the stored state.
func judgeEmptyPUT(r sseResult, unchanged, storedOrdinary bool) (bool, error) {
	switch {
	case r.Code == http.StatusOK && r.Encryption == "" && storedOrdinary:
		return true, nil
	case r.Code == http.StatusBadRequest && r.Failure.Code == "InvalidArgument" && unchanged:
		return false, nil
	default:
		return false, fmt.Errorf("empty algorithm: unqualified outcome status=%d code=%s message=%q response-sse=%q unchanged=%t stored-ordinary=%t",
			r.Code, r.Failure.Code, r.Failure.Message, r.Encryption, unchanged, storedOrdinary)
	}
}
