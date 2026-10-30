package rgw

import (
	"strings"
	"testing"
)

// The user usage reader gained the shared nesting limit.
func TestUserUsageJSONGainedTheNestingLimit(t *testing.T) {
	deep := strings.Repeat("[", 100) + "0" + strings.Repeat("]", 100)
	data := strings.Replace(rgwUsageFixture, `"stats":`, `"future":`+deep+`,"stats":`, 1)
	if _, err := decodeRGWUserUsage([]byte(data)); err == nil {
		t.Fatal("user usage accepted nesting beyond the shared limit")
	}
	if _, err := decodeRGWUserUsage([]byte(rgwUsageFixture)); err != nil {
		t.Fatal(err)
	}
}
