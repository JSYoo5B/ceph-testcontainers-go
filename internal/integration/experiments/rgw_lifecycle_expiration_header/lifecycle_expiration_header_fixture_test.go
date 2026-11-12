//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_lifecycle_expiration_header_test

import (
	"strings"
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

func TestJudgeExpRequest(t *testing.T) {
	mtime := time.Date(2026, 11, 7, 10, 30, 0, 0, time.UTC)
	older := time.Date(2026, 11, 6, 23, 59, 0, 0, time.UTC)
	rule := expRule{ID: "r", Prefix: "docs/", ExpirationDays: 7, NoncurrentDays: 90}
	header := func(id string, date time.Time) string {
		return `expiry-date="` + date.Format(time.RFC1123) + `", rule-id="` + id + `"`
	}
	request := func(version time.Time, value string) expRequest {
		return expRequest{Name: "q", Path: "/b/docs/key", Version: expVersion{ID: "v", MTime: version}, Header: value, Present: value != ""}
	}
	for _, c := range []struct {
		name        string
		r           expRequest
		reportsRule bool
		symptom     bool
		fault       string
	}{
		{"no header where none is due", request(mtime, ""), false, false, ""},
		{"expiration reported", request(mtime, header("r", expExpected(mtime, 7))), true, false, ""},
		{"noncurrent days on the plain request", request(mtime, header("r", expExpected(mtime, 90))), true, true, ""},
		{"noncurrent days on an explicit version", request(mtime, header("r", expExpected(mtime, 90))), false, true, ""},
		{"noncurrent days from the older version", request(older, header("r", expExpected(older, 90))), false, true, ""},
		{"noncurrent days from the current version", request(older, header("r", expExpected(mtime, 90))), false, true, ""},
		{"no header where the rule is due", request(mtime, ""), true, false, "control failed: "},
		{"expiration on an explicit version", request(mtime, header("r", expExpected(mtime, 7))), false, false, "another fault: "},
		{"another rule", request(mtime, header("other", expExpected(mtime, 90))), false, false, "another fault: "},
		{"another date", request(mtime, header("r", expExpected(mtime, 8))), true, false, "another fault: "},
		{"malformed", request(mtime, "expiry-date=soon"), false, false, "another fault: "},
	} {
		symptom, detail, err := judgeExpRequest(c.r, rule, c.reportsRule, mtime)
		if c.fault != "" {
			if err == nil || !strings.HasPrefix(err.Error(), c.fault) {
				t.Errorf("%s: %t %q %v, want %s", c.name, symptom, detail, err, c.fault)
			}
			continue
		}
		if err != nil || symptom != c.symptom || detail == "" {
			t.Errorf("%s: symptom=%t %q %v, want symptom=%t", c.name, symptom, detail, err, c.symptom)
		}
	}
	// A rule without NoncurrentVersionExpiration has no symptom to show.
	if _, _, err := judgeExpRequest(request(mtime, header("r", expExpected(mtime, 30))), expRule{ID: "r", ExpirationDays: 30}, false, mtime); err == nil {
		t.Error("an Expiration header on an explicit version of a current-only rule was judged, want another fault")
	}
}
