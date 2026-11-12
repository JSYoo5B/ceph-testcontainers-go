//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rbd_mirror_demote_restart_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// These cases feed synthetic results to the judges. They are not native
// evidence of what the receiver does.
func TestJudgeDemotionResume(t *testing.T) {
	for _, c := range []struct {
		name               string
		completed, timeout bool
		probe              error
		symptom            bool
		fault              string
	}{
		{"completed", true, false, nil, false, ""},
		{"still incomplete at the bound", false, true, errors.New("not observed"), true, ""},
		{"copy gone", false, false, errors.New("demotion copy 7 is gone"), false, "another fault:"},
	} {
		symptom, detail, err := judgeDemotionResume(c.completed, c.timeout, c.probe, 7, "snapshots")
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

func TestJudgeResumedBytes(t *testing.T) {
	const object = rbdDemoteRestartObject
	before := bytes.Repeat([]byte{1}, 4*object)
	after := bytes.Repeat([]byte{2}, 4*object)
	staleOne := bytes.Clone(after)
	copy(staleOne[2*object:3*object], before)
	torn := bytes.Clone(after)
	torn[object+5] = 9
	for _, c := range []struct {
		name    string
		data    []byte
		symptom bool
		fault   string
	}{
		{"exact bytes", after, false, ""},
		{"one object keeps the previous bytes", staleOne, true, ""},
		{"an object holds neither", torn, false, "another fault:"},
		{"export failed", nil, false, "another fault:"},
		{"short export", after[:object], false, "another fault:"},
	} {
		symptom, detail, err := judgeResumedBytes(c.data, before, after, 2)
		if c.fault != "" {
			if err == nil || !strings.HasPrefix(err.Error(), c.fault) {
				t.Errorf("%s: %v, want %s", c.name, err, c.fault)
			}
			continue
		}
		if err != nil || symptom != c.symptom || detail == "" {
			t.Errorf("%s: symptom=%t %q %v, want symptom=%t", c.name, symptom, detail, err, c.symptom)
		}
		if c.symptom && !strings.Contains(detail, "[2]") {
			t.Errorf("%s: %q does not name object 2", c.name, detail)
		}
	}
}
