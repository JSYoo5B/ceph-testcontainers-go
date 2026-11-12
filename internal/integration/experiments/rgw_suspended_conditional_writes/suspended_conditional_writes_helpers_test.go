//go:build all || integration

package rgw_suspended_conditional_writes_test

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

// condWrongETag is a well-formed ETag that no object written by the test has.
const condWrongETag = `"00000000000000000000000000000000"`

// condValidatorPattern is the strong ETag RGW returns for an object: the
// quoted MD5 of a single PUT, or of the part ETags with a part count for a
// completed multipart upload.
var condValidatorPattern = regexp.MustCompile(`^"[0-9a-f]{32}(?:-[1-9][0-9]{0,4})?"$`)

// condValidator reports whether etag can stand as the If-Match validator of
// a step. A missing or malformed value would send an empty or meaningless
// precondition, which a gateway can ignore and still answer 200.
func condValidator(etag string) bool {
	return condValidatorPattern.MatchString(etag) && etag != condWrongETag
}

// condResult is the native answer to one conditional write.
type condResult struct {
	Code    int
	ErrCode string
}

func (r condResult) String() string {
	return fmt.Sprintf("%d %q", r.Code, r.ErrCode)
}

// condPreconditionFailed is the refusal of a write whose precondition does
// not hold.
var condPreconditionFailed = condResult{Code: http.StatusPreconditionFailed, ErrCode: "PreconditionFailed"}

// condStep is one conditional write of the sequence: the answer AWS
// semantics want and the one the gateway gave.
type condStep struct {
	Name      string
	Got, Want condResult
}

// judgeCondSteps judges the steps of one sequence. The documented symptom is
// a precondition checked against something other than the current version:
// 412 PreconditionFailed for a step whose precondition holds (AWS wants 200)
// or whose key has no current version (AWS wants 404 NoSuchKey). Every step
// at its AWS answer is the symptom's absence. Any other answer is another
// fault: no verdict.
func judgeCondSteps(steps []condStep) (symptom bool, detail string, err error) {
	if len(steps) == 0 {
		return false, "", fmt.Errorf("another fault: no conditional step ran")
	}
	var symptoms, others []string
	for _, s := range steps {
		switch {
		case s.Got == s.Want:
		case s.Got == condPreconditionFailed && (s.Want.Code == http.StatusOK || s.Want == condResult{Code: http.StatusNotFound, ErrCode: "NoSuchKey"}):
			symptoms = append(symptoms, fmt.Sprintf("%s answered %s, AWS semantics want %s", s.Name, s.Got, s.Want))
		default:
			others = append(others, fmt.Sprintf("%s answered %s, AWS semantics want %s", s.Name, s.Got, s.Want))
		}
	}
	switch {
	case len(others) > 0:
		return false, "", fmt.Errorf("another fault: %s", strings.Join(others, "; "))
	case len(symptoms) > 0:
		return true, strings.Join(symptoms, "; "), nil
	default:
		return false, fmt.Sprintf("all %d conditional steps followed AWS semantics", len(steps)), nil
	}
}
