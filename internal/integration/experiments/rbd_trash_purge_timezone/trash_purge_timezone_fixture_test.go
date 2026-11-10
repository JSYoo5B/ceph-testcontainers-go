//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rbd_trash_purge_timezone_test

import (
	"strings"
	"testing"
)

// These cases feed synthetic trash listings to judgeTrashPurge. They are not
// native evidence of what a purge removes.
func TestJudgeTrashPurgeAttributesOnlyTheShiftedSetToTheTimezone(t *testing.T) {
	const hour, cutoff = int64(3600), int64(1_800_000_000)
	// Deadlines 10 minutes, 6 hours, 15 hours and 30 hours after moves that
	// ended 10 hours before the cutoff.
	before := []trashPurgeEntry{
		{ID: "a1", Name: "due-10m", Deadline: cutoff - 10*hour + 600},
		{ID: "a2", Name: "due-15h", Deadline: cutoff + 5*hour},
		{ID: "a3", Name: "due-30h", Deadline: cutoff + 20*hour},
		{ID: "a4", Name: "due-6h", Deadline: cutoff - 4*hour},
	}
	byInstant := []trashPurgeEntry{before[1], before[2]}
	early := []trashPurgeEntry{before[1], before[2], before[3]}
	late := []trashPurgeEntry{before[2]}
	other := []trashPurgeEntry{{ID: "b2", Name: "due-15h", Deadline: before[1].Deadline}, before[2]}
	for _, tc := range []struct {
		name          string
		offset, shift int64
		after         []trashPurgeEntry
		want          string
	}{
		{name: "utc keeps the set of the instant", after: byInstant},
		{name: "ahead keeps the set of the instant", offset: 9 * hour, shift: -9 * hour, after: byInstant},
		{name: "behind keeps the set of the instant", offset: -9 * hour, shift: 9 * hour, after: byInstant},
		{name: "ahead keeps the set of the early reading", offset: 9 * hour, shift: -9 * hour, after: early, want: "observed defect: a client process +9 h from UTC purged with a cutoff -9 h from the instant"},
		{name: "behind keeps the set of the late reading", offset: -9 * hour, shift: 9 * hour, after: late, want: "observed defect: a client process -9 h from UTC purged with a cutoff +9 h from the instant"},
		{name: "utc removes nothing", after: before, want: "control failed: a client process in UTC"},
		{name: "utc removes everything", want: "control failed: a client process in UTC"},
		{name: "utc keeps the set of a shifted reading", after: early, want: "control failed: a client process in UTC"},
		{name: "ahead removes everything", offset: 9 * hour, shift: -9 * hour, want: "another fault: a client process +9 h from UTC"},
		{name: "ahead removes nothing", offset: 9 * hour, shift: -9 * hour, after: before, want: "another fault: a client process +9 h from UTC"},
		{name: "ahead keeps the set of the late reading", offset: 9 * hour, shift: -9 * hour, after: late, want: "another fault: a client process +9 h from UTC"},
		{name: "behind keeps the set of the early reading", offset: -9 * hour, shift: 9 * hour, after: early, want: "another fault: a client process -9 h from UTC"},
		{name: "behind keeps an image of another id", offset: -9 * hour, shift: 9 * hour, after: other, want: "another fault: a client process -9 h from UTC"},
		{name: "ahead misses the set without a shifted reading", offset: 9 * hour, after: early, want: "another fault: a client process +9 h from UTC"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := judgeTrashPurge(tc.offset, cutoff, cutoff+tc.shift, before, tc.after)
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.HasPrefix(err.Error(), tc.want)) {
				t.Fatalf("error %v, want %q", err, tc.want)
			}
		})
	}
}
