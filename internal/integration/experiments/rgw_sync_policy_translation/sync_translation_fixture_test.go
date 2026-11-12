//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_sync_policy_translation_test

import (
	"errors"
	"net/http"
	"strings"
	"testing"
)

// These cases feed synthetic destination answers to the judges. They are
// not native evidence of what a gateway replicates.
func TestJudgePriority(t *testing.T) {
	ia := priorityReplica{Class: "STANDARD_IA", Listed: true}
	missing := priorityReplica{Arrived: errors.New("wait owned RGW replica: deadline exceeded")}
	for _, c := range []struct {
		name      string
		blue, red priorityReplica
		symptom   bool
		fault     string
	}{
		{"both through the higher-priority pipe", ia, ia, false, ""},
		{"red never arrives", ia, missing, true, ""},
		{"blue through the lower-priority pipe", priorityReplica{Class: "STANDARD", Listed: true}, ia, true, ""},
		{"blue never arrives", missing, ia, false, "control failed:"},
		{"red omitted from the listing", ia, priorityReplica{}, false, "another fault:"},
	} {
		symptom, detail, err := judgePriority(c.blue, c.red)
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

func TestJudgeDenial(t *testing.T) {
	for _, c := range []struct {
		name    string
		held    bool
		status  int
		code    string
		failure error
		symptom bool
		fault   string
	}{
		{"held", true, 0, "", nil, false, ""},
		{"served before the grant", false, http.StatusOK, "", nil, true, ""},
		{"access denied", false, http.StatusForbidden, "AccessDenied", nil, false, "another fault:"},
		{"transport", false, 0, "", errors.New("reset"), false, "another fault:"},
	} {
		symptom, detail, err := judgeDenial(c.held, "for 35s", c.status, c.code, c.failure)
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
