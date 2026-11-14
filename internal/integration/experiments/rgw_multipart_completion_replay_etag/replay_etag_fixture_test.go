//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_multipart_completion_replay_etag_test

import (
	"slices"
	"testing"
)

func TestReplayETagVerdict(t *testing.T) {
	const tag = `"etag-1"`
	const xmlLoss = "empty replay XML ETag"
	const headerLoss = "missing or empty replay HTTP ETag"
	valid := []string{tag}
	for _, tc := range []struct {
		name, xml     string
		first, replay []string
		verdict       string
		symptoms      []string
	}{
		{"preserved", tag, valid, valid, "NOT_REPRODUCED", nil},
		{"no header prerequisite", tag, nil, nil, "NOT_REPRODUCED", nil},
		{"invalid first header", tag, []string{"other"}, nil, "NOT_REPRODUCED", nil},
		{"XML lost without header prerequisite", `""`, nil, nil, "REPRODUCED", []string{xmlLoss}},
		{"XML lost with header preserved", "", valid, valid, "REPRODUCED", []string{xmlLoss}},
		{"HTTP header absent with XML preserved", tag, valid, nil, "REPRODUCED", []string{headerLoss}},
		{"HTTP header empty with XML preserved", tag, valid, []string{""}, "REPRODUCED", []string{headerLoss}},
		{"both lost", `""`, valid, []string{`""`}, "REPRODUCED", []string{xmlLoss, headerLoss}},
		{"unexpected XML", "other", valid, valid, "", nil},
		{"unexpected HTTP header", tag, valid, []string{"other"}, "", nil},
		{"duplicate replay HTTP header", tag, valid, []string{tag, tag}, "", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			verdict, symptoms, err := replayETagVerdict(tag, tc.xml, tc.first, tc.replay)
			if (err != nil) != (tc.verdict == "") || verdict != tc.verdict || !slices.Equal(symptoms, tc.symptoms) {
				t.Fatalf("verdict=%q symptoms=%q err=%v", verdict, symptoms, err)
			}
		})
	}
}
