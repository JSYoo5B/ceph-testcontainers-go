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

// expVersion is one version written by the test.
type expVersion struct {
	ID    string
	Data  []byte
	MTime time.Time
}

// expRule is one lifecycle rule of the case; a zero day count leaves the
// action out.
type expRule struct {
	ID             string
	Prefix         string
	ExpirationDays int
	NoncurrentDays int
}

// expRequest is one GET or HEAD of the matrix.
type expRequest struct {
	Name      string
	Path      string
	Version   expVersion
	Header    string
	Present   bool
	VersionID string
}

// expJudged is a request of an observation and whether S3 reports the
// rule's Expiration action for it.
type expJudged struct {
	request     expRequest
	reportsRule bool
}

// judgeExpRequest judges one request on a bucket whose only rule is rule.
// current is the modification time of the key's current version. When
// reportsRule is set, S3 reports the rule's Expiration days for the request;
// otherwise it sends no header.
//
// The documented symptom: the header names the rule with the date that its
// NoncurrentVersionExpiration days give for the requested or the current
// version's modification time. The S3 answer is its absence. A request that
// should report the Expiration and gets no header at all failed a control
// (the rule is not eligible). Any other header is another fault: no verdict.
func judgeExpRequest(r expRequest, rule expRule, reportsRule bool, current time.Time) (symptom bool, detail string, err error) {
	at := fmt.Sprintf("%s (%s)", r.Name, r.Path)
	if !r.Present {
		if reportsRule {
			return false, "", fmt.Errorf("control failed: %s: no x-amz-expiration, want rule %s: the rule is not eligible", at, rule.ID)
		}
		return false, at + ": no header", nil
	}
	got, perr := parseExpHeader(r.Header)
	if perr != nil {
		return false, "", fmt.Errorf("another fault: %s: %w", at, perr)
	}
	reported := fmt.Sprintf("%s: rule %q expiring %s", at, got.RuleID, got.Date.Format(time.RFC1123))
	noncurrent := rule.NoncurrentDays > 0 && got.RuleID == rule.ID &&
		(got.Date.Equal(expExpected(r.Version.MTime, rule.NoncurrentDays)) || got.Date.Equal(expExpected(current, rule.NoncurrentDays)))
	switch {
	case reportsRule && rule.ExpirationDays > 0 && got.RuleID == rule.ID && got.Date.Equal(expExpected(current, rule.ExpirationDays)):
		return false, fmt.Sprintf("%s, the Expiration of %d days", reported, rule.ExpirationDays), nil
	case noncurrent:
		return true, fmt.Sprintf("%s, the NoncurrentVersionExpiration of %d days", reported, rule.NoncurrentDays), nil
	case reportsRule:
		return false, "", fmt.Errorf("another fault: %s, want rule %q with the Expiration of %d days from %s", reported, rule.ID, rule.ExpirationDays, current.UTC().Format(time.RFC3339))
	default:
		return false, "", fmt.Errorf("another fault: %s, want no header", reported)
	}
}
