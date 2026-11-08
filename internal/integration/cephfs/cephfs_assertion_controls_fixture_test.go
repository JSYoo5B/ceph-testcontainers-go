//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/snapschedule"
)

func TestCephFSRemovalDispatchRequiresFreshCompleteIdlePolicy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		states map[string]string
		accept bool
	}{
		{"no admission list", nil, false},
		{"missing target", map[string]string{"/other": "mapped"}, false},
		{"unobserved sibling state", map[string]string{"/new": "mapped", "/other": ""}, false},
		{"in-flight assignment", map[string]string{"/new": "mapped", "/other": "unmapping"}, false},
		{"previous path still listed", map[string]string{"/new": "mapped", "/old": "mapped"}, false},
		{"prior release complete", map[string]string{"/new": "mapped", "/other": "stalled"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := checkCephFSRemovalDispatch(tc.states, []string{"/old"}, "/new"); (err == nil) != tc.accept {
				t.Fatalf("accepted=%t, error=%v", tc.accept, err)
			}
		})
	}
}

func TestCephFSRetentionRequiresBothNewestSnapshotIdentities(t *testing.T) {
	first := time.Date(2026, 1, 8, 14, 0, 0, 0, time.UTC)
	times := []time.Time{first, first.Add(time.Minute), first.Add(2 * time.Minute)}
	old, middle, newest := "scheduled-2026-01-08-14_00_00_UTC", "scheduled-2026-01-08-14_01_00_UTC", "scheduled-2026-01-08-14_02_00_UTC"
	for _, tc := range []struct {
		names []string
		match bool
	}{
		{[]string{middle, newest}, true}, {[]string{old, middle}, false}, {[]string{old, newest}, false}, {[]string{newest}, false},
	} {
		if scheduleRetainsNewestTwo(tc.names, times) != tc.match {
			t.Fatalf("names %v accepted=%t", tc.names, !tc.match)
		}
	}
}

func TestCephFSRetentionWaitsForSettledCountsAroundTheListing(t *testing.T) {
	first := time.Date(2026, 1, 8, 14, 0, 0, 0, time.UTC)
	times := []time.Time{first, first.Add(time.Minute), first.Add(2 * time.Minute), first.Add(3 * time.Minute)}
	three := snapschedule.Status{Created: 3, Pruned: 1, First: first, Last: times[2]}
	four := snapschedule.Status{Created: 4, Pruned: 2, First: first, Last: times[3]}
	pruning := four
	pruning.Pruned = 1
	prunedAgain := three
	prunedAgain.Pruned++
	firstPair := []string{"scheduled-2026-01-08-14_01_00_UTC", "scheduled-2026-01-08-14_02_00_UTC"}
	nextPair := []string{firstPair[1], "scheduled-2026-01-08-14_03_00_UTC"}
	for _, tc := range []struct {
		name                          string
		watch, before, after          snapschedule.Status
		names                         []string
		reads                         int
		crossed, accepted, wrongNames bool
	}{
		{"prune pending", pruning, pruning, pruning, nil, 0, false, false, false},
		{"stale before listing", three, four, four, nil, 0, false, false, false},
		{"creation during listing", three, three, pruning, append(slices.Clone(firstPair), nextPair[1]), 1, true, false, false},
		{"prune during listing", three, three, prunedAgain, firstPair, 1, true, false, false},
		{"same counts after listing", three, three, three, firstPair, 1, false, true, false},
		{"new stable pair on retry", four, four, four, nextPair, 1, false, true, false},
		{"wrong pair in stable window", four, four, four, firstPair, 1, false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nStatus, nLists := 0, 0
			names, crossed, err := readScheduleRetention(t.Context(), tc.watch, times[:tc.watch.Created], func(context.Context) (snapschedule.Status, error) {
				nStatus++
				if nStatus == 1 {
					return tc.before, nil
				}
				return tc.after, nil
			}, func(context.Context) ([]string, error) { nLists++; return tc.names, nil })
			if nLists != tc.reads || (names != nil) != tc.accepted || crossed != tc.crossed || (err != nil) != tc.wrongNames || (tc.accepted && !slices.Equal(names, tc.names)) {
				t.Fatalf("listed %d times, names %v, crossed %t, error %v", nLists, names, crossed, err)
			}
		})
	}
}

func TestCephFSHeldReceiptRequiresNativeNonexistence(t *testing.T) {
	for _, tc := range []struct {
		code           int
		err            error
		present, valid bool
	}{
		{0, nil, true, true}, {1, nil, false, true}, {2, nil, false, false}, {1, context.Canceled, false, false},
	} {
		present, err := cephFSReceiptFilePresent(tc.code, tc.err)
		if present != tc.present || (err == nil) != tc.valid {
			t.Fatalf("code %d error %v: present %t, error %v", tc.code, tc.err, present, err)
		}
	}
}

func TestCephFSShortWindowRejectsTheOrdinaryReconnectTimeout(t *testing.T) {
	for _, tc := range []struct {
		held  time.Duration
		match bool
	}{{10 * time.Second, true}, {0, false}, {44 * time.Second, false}, {45 * time.Second, false}} {
		if cephFSShortReconnectWindow(tc.held, 10*time.Second, 45*time.Second) != tc.match {
			t.Fatal(tc)
		}
	}
}
