//go:build all || integration

package rgw_lifecycle_expiration_header_test

import (
	"fmt"
	"regexp"
	"time"
)

// expHeaderPattern is the x-amz-expiration value RGW sends:
// expiry-date="<RFC 1123 date in GMT>", rule-id="<ID>".
var expHeaderPattern = regexp.MustCompile(`^expiry-date="([^"]+)", rule-id="([^"]+)"$`)

// expHeader is a parsed x-amz-expiration value.
type expHeader struct {
	Date   time.Time
	RuleID string
}

// parseExpHeader parses an x-amz-expiration value. The date must be a
// midnight in GMT, which every Days-based expiration is.
func parseExpHeader(value string) (expHeader, error) {
	m := expHeaderPattern.FindStringSubmatch(value)
	if m == nil {
		return expHeader{}, fmt.Errorf("x-amz-expiration %q is not expiry-date=\"...\", rule-id=\"...\"", value)
	}
	date, err := time.Parse(time.RFC1123, m[1])
	if err != nil {
		return expHeader{}, fmt.Errorf("x-amz-expiration date %q: %w", m[1], err)
	}
	if date.Location().String() != "GMT" && date.Location() != time.UTC {
		return expHeader{}, fmt.Errorf("x-amz-expiration date %q is not in GMT", m[1])
	}
	date = date.UTC()
	if !date.Equal(date.Truncate(24 * time.Hour)) {
		return expHeader{}, fmt.Errorf("x-amz-expiration date %q is not a midnight", m[1])
	}
	return expHeader{Date: date, RuleID: m[2]}, nil
}

// expExpected is the expiry date of a Days rule for an object last modified
// at mtime: the midnight in UTC that follows mtime plus days, as both S3 and
// RGW round it (RGW adds days, removes the time of day and adds one day).
func expExpected(mtime time.Time, days int) time.Time {
	return mtime.UTC().Truncate(24*time.Hour).AddDate(0, 0, days+1)
}
