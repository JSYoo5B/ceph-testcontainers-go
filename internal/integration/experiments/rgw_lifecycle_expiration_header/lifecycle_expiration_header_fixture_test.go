//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_lifecycle_expiration_header_test

import (
	"testing"
	"time"
)

func TestExpHeaderParsing(t *testing.T) {
	got, err := parseExpHeader(`expiry-date="Fri, 21 Dec 2012 00:00:00 GMT", rule-id="tc-current"`)
	if err != nil || !got.Date.Equal(time.Date(2012, 12, 21, 0, 0, 0, 0, time.UTC)) || got.RuleID != "tc-current" {
		t.Fatalf("parseExpHeader = %+v, %v", got, err)
	}
	for _, value := range []string{
		"",
		`expiry-date="Fri, 21 Dec 2012 00:00:00 GMT"`,
		`expiry-date="Fri, 21 Dec 2012 00:00:01 GMT", rule-id="r"`,
		`expiry-date="2012-12-21T00:00:00Z", rule-id="r"`,
		`expiry-date="Fri, 21 Dec 2012 00:00:00 GMT", rule-id=""`,
	} {
		if _, err := parseExpHeader(value); err == nil {
			t.Errorf("parseExpHeader(%q) accepted a malformed value", value)
		}
	}
}

func TestExpExpectedRoundsToTheFollowingMidnight(t *testing.T) {
	for _, tc := range []struct {
		mtime time.Time
		days  int
		want  time.Time
	}{
		{time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC), 7, time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)},
		{time.Date(2026, 11, 7, 23, 59, 59, 0, time.UTC), 7, time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)},
		{time.Date(2026, 12, 31, 12, 0, 0, 0, time.UTC), 1, time.Date(2027, 1, 2, 0, 0, 0, 0, time.UTC)},
		{time.Date(2026, 11, 7, 1, 0, 0, 0, time.FixedZone("UTC+9", 9*3600)), 7, time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)},
	} {
		if got := expExpected(tc.mtime, tc.days); !got.Equal(tc.want) {
			t.Errorf("expExpected(%s, %d) = %s, want %s", tc.mtime, tc.days, got, tc.want)
		}
	}
}
