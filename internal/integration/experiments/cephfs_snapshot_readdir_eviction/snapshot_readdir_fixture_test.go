//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_snapshot_readdir_eviction_test

import (
	"slices"
	"strings"
	"testing"
)

// These cases feed synthetic listings to judgeSnapshotListings. They are not
// native evidence of what a client lists.
func TestJudgeSnapshotListings(t *testing.T) {
	want, live := snapshotNames(snapshotPrefix, snapshotEntries), snapshotNames(livePrefix, snapshotEntries)
	if want[0] != "snap-0000" || live[snapshotEntries-1] != "live-2999" {
		t.Fatalf("names %s, %s", want[0], live[snapshotEntries-1])
	}
	// What the affected clients returned in shape: the snapshot's first
	// reply, then the live directory from an offset.
	mixed := append(slices.Clone(want[:1539]), live[1515:]...)
	for _, tc := range []struct {
		name   string
		change func(control, trigger *snapshotListing)
		want   string
	}{
		{name: "exact", want: "absent: the trigger client dropped its dentry of the snapshot and still listed exactly the snapshot"},
		{name: "exact in another order", change: func(_, trigger *snapshotListing) { slices.Reverse(trigger.Names) }, want: "absent: "},
		{name: "live names in the trigger's snapshot listing", change: func(_, trigger *snapshotListing) {
			trigger.Names, trigger.HeadRequests = mixed, 2
		}, want: "symptom: the first 50 entries are names of the snapshot, and the entries read after the client dropped its dentry of the snapshot hold names of the live directory: 3024 entries returned, 1539 of the wanted names found, missing 1461 (snap-1539, snap-1540, snap-1541, ...), returned twice 0 [], names of the other directory state 1485 (live-1515, live-1516, live-1517, ...), unknown 0 []; 2 of its 3 readdir requests did not name the snapshot"},
		{name: "one live name beside the whole snapshot", change: func(_, trigger *snapshotListing) { trigger.Names = append(trigger.Names, "live-0007") }, want: "symptom: "},
		{name: "a live name in the continuation only", change: func(_, trigger *snapshotListing) { trigger.Names[snapshotFirst] = "live-0007" }, want: "symptom: "},
		// Contamination before the lookups is not what the dropped dentry
		// did, whatever follows it.
		{name: "a live name before the lookups only", change: func(_, trigger *snapshotListing) { trigger.Names[0] = "live-0007" }, want: `another fault: the trigger client's first 50 entries, read before it dropped its dentry of the snapshot, are not distinct names of the snapshot: entry 0 is "live-0007"`},
		{name: "a live name as the last entry before the lookups", change: func(_, trigger *snapshotListing) { trigger.Names[snapshotFirst-1] = "live-0007" }, want: `another fault: the trigger client's first 50 entries, read before it dropped its dentry of the snapshot, are not distinct names of the snapshot: entry 49 is "live-0007"`},
		{name: "a live name before the lookups and live names after", change: func(_, trigger *snapshotListing) {
			trigger.Names, trigger.HeadRequests = slices.Clone(mixed), 2
			trigger.Names[0] = "live-0007"
		}, want: "another fault: the trigger client's first 50 entries"},
		{name: "a name twice before the lookups", change: func(_, trigger *snapshotListing) { trigger.Names[3] = trigger.Names[2] }, want: `another fault: the trigger client's first 50 entries, read before it dropped its dentry of the snapshot, are not distinct names of the snapshot: entry 3 returns "snap-0002" again`},
		{name: "an unknown name before the lookups", change: func(_, trigger *snapshotListing) { trigger.Names[10] = "stray" }, want: `another fault: the trigger client's first 50 entries, read before it dropped its dentry of the snapshot, are not distinct names of the snapshot: entry 10 is "stray"`},
		{name: "fewer entries than read first", change: func(_, trigger *snapshotListing) { trigger.Names = trigger.Names[:snapshotFirst-1] }, want: "another fault: the trigger client's first 50 entries, read before it dropped its dentry of the snapshot, are not distinct names of the snapshot: the listing holds 49 entries"},
		{name: "a missing name only", change: func(_, trigger *snapshotListing) { trigger.Names = trigger.Names[1:] }, want: "another fault: the snapshot listing after the client dropped its dentry of the snapshot holds no live name and is not the snapshot: 2999 entries returned, 2999 of the wanted names found, missing 1 [snap-0000]"},
		{name: "a name twice only", change: func(_, trigger *snapshotListing) { trigger.Names = append(trigger.Names, "snap-0001") }, want: "another fault: "},
		{name: "an unknown name only", change: func(_, trigger *snapshotListing) { trigger.Names = append(trigger.Names, "stray") }, want: "another fault: "},
		{name: "live names in the control's snapshot listing", change: func(control, trigger *snapshotListing) {
			control.Names, trigger.Names = mixed, mixed
		}, want: "control failed: the snapshot listing of the client with the default cache: 3024 entries returned"},
		{name: "the control's live listing short", change: func(control, _ *snapshotListing) { control.Live = control.Live[:10] }, want: "control failed: the live listing of the control client: 10 entries returned"},
		{name: "the trigger's live listing with a snapshot name", change: func(_, trigger *snapshotListing) {
			trigger.Names, trigger.Live = mixed, append(trigger.Live, "snap-0003")
		}, want: "control failed: the live listing of the trigger client: 3001 entries returned"},
		{name: "the dentry kept", change: func(_, trigger *snapshotListing) { trigger.Trimmed = false }, want: "condition not established: the trigger client has a cache of 100 entries and dropped its dentry"},
		{name: "the dentry kept with live names", change: func(_, trigger *snapshotListing) { trigger.Names, trigger.Trimmed = mixed, false }, want: "condition not established: the trigger client"},
		{name: "the trigger with another cache", change: func(_, trigger *snapshotListing) { trigger.Cache = 16384 }, want: "condition not established: the trigger client has a cache of 16384"},
		{name: "the control with a small cache", change: func(control, _ *snapshotListing) { control.Cache = 4999 }, want: "condition not established: the control client has a cache of 4999"},
		{name: "the control dropped the dentry", change: func(control, _ *snapshotListing) { control.Trimmed = true }, want: "condition not established: the control client"},
		{name: "one client", change: func(control, trigger *snapshotListing) { trigger.Client = control.Client }, want: "condition not established: both listings are of client 11"},
		{name: "one readdir request", change: func(_, trigger *snapshotListing) { trigger.Requests = 1 }, want: "condition not established: the trigger client read 50 entries, looked up 2000 files elsewhere and sent 1 readdir requests"},
		{name: "no lookups", change: func(control, _ *snapshotListing) { control.Stats = 0 }, want: "condition not established: the control client read 50 entries, looked up 0 files"},
		{name: "everything read first", change: func(_, trigger *snapshotListing) { trigger.First = 3000 }, want: "condition not established: the trigger client read 3000 entries"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			control := snapshotListing{Client: 11, Cache: 16384, First: snapshotFirst, Stats: snapshotOthers, Names: slices.Clone(want), Live: slices.Clone(live), Requests: 3}
			trigger := snapshotListing{Client: 12, Cache: snapshotCache, First: snapshotFirst, Stats: snapshotOthers, Names: slices.Clone(want), Live: slices.Clone(live), Requests: 3, Trimmed: true}
			if tc.change != nil {
				tc.change(&control, &trigger)
			}
			symptom, detail, err := judgeSnapshotListings(want, live, control, trigger)
			got := "absent: " + detail
			switch {
			case err != nil:
				got = err.Error()
			case symptom:
				got = "symptom: " + detail
			}
			if !strings.HasPrefix(got, tc.want) {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}
