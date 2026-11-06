//go:build all || integration

package rgw_suspended_conditional_writes_test

import "regexp"

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
