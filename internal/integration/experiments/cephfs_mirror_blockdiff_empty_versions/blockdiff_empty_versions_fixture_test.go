//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_mirror_blockdiff_empty_versions_test

import (
	"strings"
	"testing"
)

// These cases feed synthetic outcomes to judgeBlockdiff. They are not native
// evidence of what an MDS does with the request.
func TestJudgeBlockdiffReportsOnlyTheDocumentedAbort(t *testing.T) {
	source := blockdiffSnapshot{Present: true, ID: 4, Name: "s3", Files: map[string]blockdiffFile{
		"f": {Inode: 1, Size: 4096, SHA256: "aa"}, "g": {Inode: 2, Size: 1 << 20, SHA256: "bb"},
	}}
	arrived := blockdiffSnapshot{Present: true, ID: 9, Name: "s3", Files: map[string]blockdiffFile{
		"f": {Inode: 7, Size: 4096, SHA256: "aa"}, "g": {Inode: 8, Size: 1 << 20, SHA256: "bb"},
	}}
	short := blockdiffSnapshot{Present: true, Name: "s3", Files: map[string]blockdiffFile{"f": {Size: 0, SHA256: "cc"}, "g": arrived.Files["g"]}}
	other := blockdiffSnapshot{Present: true, Name: "s3", Files: map[string]blockdiffFile{"f": {Size: 4096, SHA256: "cc"}, "g": arrived.Files["g"]}}
	partial := blockdiffSnapshot{Present: true, Name: "s3", Files: map[string]blockdiffFile{"g": arrived.Files["g"]}}
	abort := []string{
		"/src/include/interval_set.h: 493: FAILED ceph_assert(len > 0)",
		" 5: (MDCache::aggregate_snap_sets(...)+0x4a1) [0x55d0]",
	}
	live := blockdiffOutcome{SameProcess: true, RequestsRead: true, RequestsBefore: 4, RequestsAfter: 6, Synced: true, Source: source, Destination: arrived}
	with := func(change func(*blockdiffOutcome)) blockdiffOutcome {
		got := live
		change(&got)
		return got
	}
	for _, tc := range []struct {
		name     string
		got      blockdiffOutcome
		symptoms bool
		want     string
	}{
		{name: "live, asked, arrived", got: live},
		{name: "ended with the assertion in the aggregation", got: blockdiffOutcome{Process: "exited 1", Log: abort, RequestsBefore: 4, Source: source}, symptoms: true},
		{name: "ended with the assertion although the snapshot arrived", got: with(func(o *blockdiffOutcome) { o.SameProcess, o.Log = false, abort }), symptoms: true},
		{name: "ended without a log", got: blockdiffOutcome{Process: "exited 137", Source: source}, want: "another fault:"},
		{name: "ended with another assertion in the aggregation", got: blockdiffOutcome{Log: []string{"FAILED ceph_assert(it != end)", abort[1]}, Source: source}, want: "another fault:"},
		{name: "ended with the assertion elsewhere", got: blockdiffOutcome{Log: abort[:1], Source: source}, want: "another fault:"},
		{name: "ended with only the aggregation in the log", got: blockdiffOutcome{Log: abort[1:], Source: source}, want: "another fault:"},
		{name: "live, counter unchanged", got: with(func(o *blockdiffOutcome) { o.RequestsAfter = o.RequestsBefore }), want: "trigger not established:"},
		{name: "live, counter lower", got: with(func(o *blockdiffOutcome) { o.RequestsAfter = 0 }), want: "trigger not established:"},
		{name: "live, counter not read", got: with(func(o *blockdiffOutcome) { o.RequestsRead = false }), want: "trigger not established:"},
		{name: "live, never asked, snapshot arrived", got: with(func(o *blockdiffOutcome) { o.RequestsBefore, o.RequestsAfter = 0, 0 }), want: "trigger not established:"},
		{name: "live, asked, not synchronized", got: with(func(o *blockdiffOutcome) { o.Synced, o.Destination = false, blockdiffSnapshot{} }), want: "another fault:"},
		{name: "live, asked, snapshot absent", got: with(func(o *blockdiffOutcome) { o.Destination = blockdiffSnapshot{} }), want: "another fault:"},
		{name: "live, asked, another size", got: with(func(o *blockdiffOutcome) { o.Destination = short }), want: "another fault:"},
		{name: "live, asked, other bytes", got: with(func(o *blockdiffOutcome) { o.Destination = other }), want: "another fault:"},
		{name: "live, asked, a file missing", got: with(func(o *blockdiffOutcome) { o.Destination = partial }), want: "another fault:"},
		{name: "live, asked, the source was not read", got: with(func(o *blockdiffOutcome) { o.Source = blockdiffSnapshot{} }), want: "another fault:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			symptoms, err := judgeBlockdiff(tc.got)
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.HasPrefix(err.Error(), tc.want)) {
				t.Fatalf("error %v, want %q", err, tc.want)
			}
			if (len(symptoms) > 0) != tc.symptoms {
				t.Fatalf("symptoms %q, want some: %t", symptoms, tc.symptoms)
			}
		})
	}
}

// These listings are synthetic "rados listsnaps" documents.
func TestVersionSizesReadsTheCloneOfTheSnapshotAndTheHead(t *testing.T) {
	for _, tc := range []struct {
		name, listing string
		previous      uint64
		clone, head   uint64
		wantErr       bool
	}{
		{name: "empty clone and empty head", previous: 3, listing: `{"name":"x","clones":[{"id":2,"snapshots":[{"id":2}],"size":1048576},{"id":3,"snapshots":[{"id":3}],"size":0},{"id":"head","snapshots":[],"size":0}]}`},
		{name: "nonempty clone and empty head", previous: 2, clone: 1048576, listing: `{"clones":[{"id":2,"snapshots":[{"id":2}],"size":1048576},{"id":"head","snapshots":[],"size":0}]}`},
		{name: "nonempty clone and nonempty head", previous: 2, clone: 1048576, head: 1048576, listing: `{"clones":[{"id":2,"snapshots":[{"id":2}],"size":1048576,"overlap":[{"offset":65536,"length":983040}]},{"id":"head","snapshots":[],"size":1048576}]}`},
		{name: "a clone that holds two snapshots", previous: 2, clone: 7, listing: `{"clones":[{"id":3,"snapshots":[{"id":2},{"id":3}],"size":7},{"id":"head","snapshots":[],"size":0}]}`, wantErr: true},
		{name: "a clone that holds an older and the previous snapshot", previous: 3, clone: 7, listing: `{"clones":[{"id":3,"snapshots":[{"id":2},{"id":3}],"size":7},{"id":"head","snapshots":[],"size":0}]}`},
		{name: "no clone of the snapshot", previous: 3, listing: `{"clones":[{"id":2,"snapshots":[{"id":2}],"size":1048576},{"id":"head","snapshots":[],"size":0}]}`, wantErr: true},
		{name: "a newer clone", previous: 2, listing: `{"clones":[{"id":2,"snapshots":[{"id":2}],"size":1},{"id":3,"snapshots":[{"id":3}],"size":0},{"id":"head","snapshots":[],"size":0}]}`, wantErr: true},
		{name: "no head", previous: 2, listing: `{"clones":[{"id":2,"snapshots":[{"id":2}],"size":0}]}`, wantErr: true},
		{name: "a version without a size", previous: 2, listing: `{"clones":[{"id":2,"snapshots":[{"id":2}]},{"id":"head","snapshots":[],"size":0}]}`, wantErr: true},
		{name: "no versions", previous: 2, listing: `{"clones":[]}`, wantErr: true},
		{name: "an error text", previous: 2, listing: `error listing snap shots: (2) No such file or directory`, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clone, head, err := versionSizes([]byte(tc.listing), tc.previous)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error %v, want one: %t", err, tc.wantErr)
			}
			if err == nil && (clone != tc.clone || head != tc.head) {
				t.Fatalf("sizes %d and %d, want %d and %d", clone, head, tc.clone, tc.head)
			}
		})
	}
}
