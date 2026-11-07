//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_test

import (
	"context"
	"testing"
	"time"
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
