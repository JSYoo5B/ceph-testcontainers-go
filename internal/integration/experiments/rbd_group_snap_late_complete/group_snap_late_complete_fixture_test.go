//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rbd_group_snap_late_complete_test

import (
	"strings"
	"testing"
)

// These cases feed synthetic outcomes to judgeLateComplete. They are not
// native evidence of what a server does with a late update.
func TestJudgeLateCompleteReportsOnlyTheResurrectedSnapshot(t *testing.T) {
	const victim = "1a2b"
	removed := []string{"image_x", "snap_max_order", "snap_order_0f0f", "snapshot_0f0f"}
	orphan := append([]string{"snapshot_" + victim}, removed...)
	restored := append([]string{"snap_order_" + victim}, orphan...)
	for _, tc := range []struct {
		name     string
		got      groupSnapOutcome
		symptoms int
		want     string
	}{
		{name: "accepted and listed again", got: groupSnapOutcome{Keys: orphan, Listed: []string{"healthy", "victim"}}, symptoms: 2},
		{name: "accepted and the listing fails on the orphan record", got: groupSnapOutcome{Keys: orphan, ListExit: 22, ListOutput: "rbd: failed to list group snapshots: (22) Invalid argument"}, symptoms: 2},
		{name: "refused as invalid, absent", got: groupSnapOutcome{Return: -22, Keys: removed, Listed: []string{"healthy"}}},
		{name: "accepted without a record, absent", got: groupSnapOutcome{Keys: removed, Listed: []string{"healthy"}}},
		{name: "accepted with an orphan record that the listing hides", got: groupSnapOutcome{Keys: orphan, Listed: []string{"healthy"}}},
		{name: "timed out", got: groupSnapOutcome{Return: -110, Keys: removed, Listed: []string{"healthy"}}, want: "trigger not delivered:"},
		{name: "not permitted", got: groupSnapOutcome{Return: -1, Keys: removed, Listed: []string{"healthy"}}, want: "trigger not delivered:"},
		{name: "an unknown positive result", got: groupSnapOutcome{Return: 1, Keys: orphan, Listed: []string{"healthy", "victim"}}, want: "trigger not delivered:"},
		{name: "refused but listed again", got: groupSnapOutcome{Return: -22, Keys: orphan, Listed: []string{"healthy", "victim"}}, want: "another fault:"},
		{name: "listed again without a record", got: groupSnapOutcome{Keys: removed, Listed: []string{"healthy", "victim"}}, want: "another fault:"},
		{name: "listed again with its order entry", got: groupSnapOutcome{Keys: restored, Listed: []string{"healthy", "victim"}}, want: "another fault:"},
		{name: "listed again without the sibling", got: groupSnapOutcome{Keys: orphan, Listed: []string{"victim"}}, want: "another fault:"},
		{name: "listed again with a third snapshot", got: groupSnapOutcome{Keys: orphan, Listed: []string{"healthy", "victim", "other"}}, want: "another fault:"},
		{name: "the listing fails without an orphan record", got: groupSnapOutcome{Keys: removed, ListExit: 2, ListOutput: "rbd: failed to list group snapshots"}, want: "another fault:"},
		{name: "the listing fails after a refusal", got: groupSnapOutcome{Return: -22, Keys: orphan, ListExit: 22}, want: "another fault:"},
		{name: "the listing is empty", got: groupSnapOutcome{Keys: removed}, want: "another fault:"},
		{name: "the listing has another snapshot", got: groupSnapOutcome{Keys: removed, Listed: []string{"healthy", "other"}}, want: "another fault:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			symptoms, err := judgeLateComplete(victim, tc.got)
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.HasPrefix(err.Error(), tc.want)) {
				t.Fatalf("error %v, want %q", err, tc.want)
			}
			if len(symptoms) != tc.symptoms {
				t.Fatalf("symptoms %q, want %d", symptoms, tc.symptoms)
			}
		})
	}
}
