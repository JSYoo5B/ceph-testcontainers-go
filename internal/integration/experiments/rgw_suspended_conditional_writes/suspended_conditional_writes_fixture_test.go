//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_suspended_conditional_writes_test

import (
	"net/http"
	"strings"
	"testing"
)

func TestCondValidator(t *testing.T) {
	for _, etag := range []string{
		`"d41d8cd98f00b204e9800998ecf8427e"`,
		`"9b2cf535f27731c974343645a3985328-1"`,
		`"9b2cf535f27731c974343645a3985328-10000"`,
	} {
		if !condValidator(etag) {
			t.Errorf("condValidator(%q) = false, want true", etag)
		}
	}
	for _, etag := range []string{
		"",
		`""`,
		"d41d8cd98f00b204e9800998ecf8427e",
		`W/"d41d8cd98f00b204e9800998ecf8427e"`,
		`"d41d8cd98f00b204e9800998ecf8427"`,
		`"D41D8CD98F00B204E9800998ECF8427E"`,
		`"9b2cf535f27731c974343645a3985328-0"`,
		`"9b2cf535f27731c974343645a3985328-"`,
		`*`,
		condWrongETag,
	} {
		if condValidator(etag) {
			t.Errorf("condValidator(%q) = true, want false", etag)
		}
	}
}

func TestJudgeCondSteps(t *testing.T) {
	ok := condResult{Code: http.StatusOK}
	missing := condResult{Code: http.StatusNotFound, ErrCode: "NoSuchKey"}
	followed := []condStep{
		{"existing-if-none-match", condPreconditionFailed, condPreconditionFailed},
		{"existing-if-match", ok, ok},
		{"deleted-if-match", missing, missing},
	}
	with := func(i int, got condResult) []condStep {
		steps := append([]condStep{}, followed...)
		steps[i].Got = got
		return steps
	}
	for _, c := range []struct {
		name    string
		steps   []condStep
		symptom bool
		fault   bool
	}{
		{"aws semantics", followed, false, false},
		{"refused if-match", with(1, condPreconditionFailed), true, false},
		{"refused instead of missing", with(2, condPreconditionFailed), true, false},
		{"accepted duplicate", with(0, ok), false, true},
		{"server error", with(1, condResult{Code: http.StatusInternalServerError, ErrCode: "InternalError"}), false, true},
		{"412 without a code", with(1, condResult{Code: http.StatusPreconditionFailed}), false, true},
		{"symptom with another fault", func() []condStep { s := with(1, condPreconditionFailed); s[0].Got = ok; return s }(), false, true},
		{"no step", nil, false, true},
	} {
		symptom, detail, err := judgeCondSteps(c.steps)
		if c.fault {
			if err == nil || !strings.HasPrefix(err.Error(), "another fault: ") {
				t.Errorf("%s: %t %q %v, want another fault", c.name, symptom, detail, err)
			}
			continue
		}
		if err != nil || symptom != c.symptom || detail == "" {
			t.Errorf("%s: symptom=%t %q %v, want symptom=%t", c.name, symptom, detail, err, c.symptom)
		}
	}
}
