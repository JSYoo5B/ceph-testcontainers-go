//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_mirror_blockdiff_empty_versions_test

import (
	"strings"
	"testing"
)

const (
	digestA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	digestB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	digestC = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

// These cases feed synthetic outcomes to judgeBlockdiff. They are not native
// evidence of what an MDS does with the request.
func TestJudgeBlockdiffReportsOnlyTheDocumentedAbort(t *testing.T) {
	source := blockdiffSnapshot{Present: true, ID: 4, Name: "s3", Files: map[string]blockdiffFile{
		"f": {Inode: 1, Size: 4096, SHA256: digestA}, "g": {Inode: 2, Size: 1 << 20, SHA256: digestB},
	}}
	arrived := blockdiffSnapshot{Present: true, ID: 9, Name: "s3", Files: map[string]blockdiffFile{
		"f": {Inode: 7, Size: 4096, SHA256: digestA}, "g": {Inode: 8, Size: 1 << 20, SHA256: digestB},
	}}
	short := blockdiffSnapshot{Present: true, ID: 9, Name: "s3", Files: map[string]blockdiffFile{"f": {Inode: 7, Size: 0, SHA256: digestA}, "g": arrived.Files["g"]}}
	other := blockdiffSnapshot{Present: true, ID: 9, Name: "s3", Files: map[string]blockdiffFile{"f": {Inode: 7, Size: 4096, SHA256: digestC}, "g": arrived.Files["g"]}}
	partial := blockdiffSnapshot{Present: true, ID: 9, Name: "s3", Files: map[string]blockdiffFile{"g": arrived.Files["g"]}}
	absent := blockdiffSnapshot{Name: "s3"}
	abort := []string{
		"/src/include/interval_set.h: 493: FAILED ceph_assert(len > 0)",
		" 4: (MDCache::aggregate_snap_sets(std::vector<...> const&, CInode*, CInode*, BlockDiff*, Context*)+0x633) [0x55d0]",
	}
	before := mdsProcess{Running: true, Status: "running", StartedAt: "2026-01-01T00:00:00Z", GID: 4214}
	exited := mdsProcess{Status: "exited", ExitCode: 1, StartedAt: before.StartedAt}
	live := blockdiffOutcome{Before: before, After: before, AfterRead: true, LogRead: true, RequestsRead: true, RequestsBefore: 4, RequestsAfter: 6,
		Synced: true, Source: source, Destination: arrived, DestinationRead: true}
	ended := blockdiffOutcome{Before: before, After: exited, AfterRead: true, Log: abort, LogRead: true, RequestsBefore: 4,
		Source: source, Destination: absent, DestinationRead: true}
	with := func(base blockdiffOutcome, change func(*blockdiffOutcome)) blockdiffOutcome {
		change(&base)
		return base
	}
	for _, tc := range []struct {
		name     string
		got      blockdiffOutcome
		symptoms bool
		want     string
	}{
		{name: "live, asked, arrived", got: live},
		{name: "live, asked from a count of zero, arrived", got: with(live, func(o *blockdiffOutcome) { o.RequestsBefore, o.RequestsAfter = 0, 2 })},
		{name: "ended with the assertion in the aggregation", got: ended, symptoms: true},
		{name: "ended with the assertion although the snapshot arrived", got: with(ended, func(o *blockdiffOutcome) { o.Synced, o.Destination = true, arrived }), symptoms: true},
		{name: "ended with the assertion among other matching lines", got: with(ended, func(o *blockdiffOutcome) {
			o.Log = []string{" 9: (MDCache::aggregate_snap_sets(...)+0x1) [0x1]", abort[0], abort[1]}
		}), symptoms: true},

		// The process after the trigger was not observed.
		{name: "state unreadable although the log has the abort", got: with(ended, func(o *blockdiffOutcome) {
			o.After, o.AfterRead, o.AfterError = mdsProcess{}, false, "connection refused"
		}), want: "outcome not observed:"},
		{name: "state unreadable and everything else fine", got: with(live, func(o *blockdiffOutcome) {
			o.After, o.AfterRead, o.AfterError = mdsProcess{}, false, "context deadline exceeded"
		}), want: "outcome not observed:"},
		{name: "running but the FSMap could not be read", got: with(live, func(o *blockdiffOutcome) { o.After.GID, o.AfterRead, o.AfterError = 0, false, "timed out" }), want: "outcome not observed:"},
		// The process before the trigger was not a live daemon.
		{name: "no live process before the trigger", got: with(ended, func(o *blockdiffOutcome) { o.Before = mdsProcess{StartedAt: before.StartedAt} }), want: "trigger not established:"},
		{name: "no GID before the trigger", got: with(live, func(o *blockdiffOutcome) { o.Before.GID, o.After.GID = 0, 0 }), want: "trigger not established:"},

		// The container stopped, but not with the documented evidence.
		{name: "ended and the log could not be read", got: with(ended, func(o *blockdiffOutcome) { o.LogRead, o.LogError = false, "permission denied" }), want: "outcome not observed:"},
		{name: "ended and the log broke off after the abort", got: with(ended, func(o *blockdiffOutcome) { o.LogRead, o.LogError = false, "unexpected EOF" }), want: "outcome not observed:"},
		{name: "ended without a matching log line", got: with(ended, func(o *blockdiffOutcome) { o.Log = nil }), want: "another fault:"},
		{name: "killed without a matching log line", got: with(ended, func(o *blockdiffOutcome) { o.Log, o.After.ExitCode = nil, 137 }), want: "another fault:"},
		{name: "ended with another assertion in the aggregation", got: with(ended, func(o *blockdiffOutcome) { o.Log = []string{"FAILED ceph_assert(it != end)", abort[1]} }), want: "another fault:"},
		{name: "ended with the assertion elsewhere", got: with(ended, func(o *blockdiffOutcome) { o.Log = abort[:1] }), want: "another fault:"},
		{name: "ended with only the aggregation in the log", got: with(ended, func(o *blockdiffOutcome) { o.Log = abort[1:] }), want: "another fault:"},
		{name: "ended with the aggregation only before the assertion", got: with(ended, func(o *blockdiffOutcome) { o.Log = []string{abort[1], abort[0]} }), want: "another fault:"},
		{name: "ended with a debug line of the aggregation after the assertion", got: with(ended, func(o *blockdiffOutcome) { o.Log = []string{abort[0], "mds.0.cache aggregate_snap_sets: hole: [0~0]"} }), want: "another fault:"},
		{name: "stopped with exit code 0 and the abort in the log", got: with(ended, func(o *blockdiffOutcome) { o.After.ExitCode = 0 }), want: "another fault:"},
		{name: "restarted and stopped with the abort in the log", got: with(ended, func(o *blockdiffOutcome) { o.After.StartedAt = "2026-01-01T00:05:00Z" }), want: "another fault:"},

		// The container runs, but not as the process of the trigger.
		{name: "restarted and running", got: with(live, func(o *blockdiffOutcome) { o.After.StartedAt = "2026-01-01T00:05:00Z" }), want: "another fault:"},
		{name: "running with another GID", got: with(live, func(o *blockdiffOutcome) { o.After.GID = 5000 }), want: "another fault:"},
		{name: "running with another GID and the abort in the log", got: with(live, func(o *blockdiffOutcome) { o.After.GID, o.Log = 5000, abort }), want: "another fault:"},

		// The live process was not asked, or its answer was not observed.
		{name: "live, counter unchanged", got: with(live, func(o *blockdiffOutcome) { o.RequestsAfter = o.RequestsBefore }), want: "trigger not established:"},
		{name: "live, counter lower", got: with(live, func(o *blockdiffOutcome) { o.RequestsAfter = 0 }), want: "trigger not established:"},
		{name: "live, never asked, snapshot arrived", got: with(live, func(o *blockdiffOutcome) { o.RequestsBefore, o.RequestsAfter = 0, 0 }), want: "trigger not established:"},
		{name: "live, counter not read", got: with(live, func(o *blockdiffOutcome) { o.RequestsRead, o.RequestsError = false, "timed out" }), want: "outcome not observed:"},
		{name: "live, counter not read and left at zero", got: with(live, func(o *blockdiffOutcome) { o.RequestsRead, o.RequestsAfter, o.RequestsBefore = false, 0, 0 }), want: "outcome not observed:"},
		{name: "live, asked, not synchronized", got: with(live, func(o *blockdiffOutcome) { o.Synced, o.Destination = false, absent }), want: "another fault:"},
		{name: "live, asked, destination not read", got: with(live, func(o *blockdiffOutcome) {
			o.Destination, o.DestinationRead, o.DestinationError = blockdiffSnapshot{}, false, "timed out"
		}), want: "outcome not observed:"},
		{name: "live, asked, snapshot absent", got: with(live, func(o *blockdiffOutcome) { o.Destination = absent }), want: "another fault:"},
		{name: "live, asked, another size", got: with(live, func(o *blockdiffOutcome) { o.Destination = short }), want: "another fault:"},
		{name: "live, asked, other bytes", got: with(live, func(o *blockdiffOutcome) { o.Destination = other }), want: "another fault:"},
		{name: "live, asked, a file missing", got: with(live, func(o *blockdiffOutcome) { o.Destination = partial }), want: "another fault:"},
		{name: "live, asked, the source was not read", got: with(live, func(o *blockdiffOutcome) { o.Source = blockdiffSnapshot{} }), want: "another fault:"},
		{name: "live, asked, both sides without files", got: with(live, func(o *blockdiffOutcome) {
			o.Source, o.Destination = blockdiffSnapshot{Present: true}, blockdiffSnapshot{Present: true}
		}), want: "another fault:"},
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
		{name: "a clone that holds an older and the previous snapshot", previous: 3, clone: 7, listing: `{"clones":[{"id":3,"snapshots":[{"id":2},{"id":3}],"size":7},{"id":"head","snapshots":[],"size":0}]}`},
		{name: "a head without a list of snapshots", previous: 2, clone: 7, listing: `{"clones":[{"id":2,"snapshots":[{"id":2}],"size":7},{"id":"head","size":0}]}`},
		{name: "a clone that holds a newer snapshot too", previous: 2, listing: `{"clones":[{"id":3,"snapshots":[{"id":2},{"id":3}],"size":7},{"id":"head","snapshots":[],"size":0}]}`, wantErr: true},
		{name: "no clone of the snapshot", previous: 3, listing: `{"clones":[{"id":2,"snapshots":[{"id":2}],"size":1048576},{"id":"head","snapshots":[],"size":0}]}`, wantErr: true},
		{name: "two clones of the snapshot", previous: 2, listing: `{"clones":[{"id":1,"snapshots":[{"id":2}],"size":0},{"id":2,"snapshots":[{"id":2}],"size":0},{"id":"head","snapshots":[],"size":0}]}`, wantErr: true},
		{name: "a newer clone", previous: 2, listing: `{"clones":[{"id":2,"snapshots":[{"id":2}],"size":1},{"id":3,"snapshots":[{"id":3}],"size":0},{"id":"head","snapshots":[],"size":0}]}`, wantErr: true},
		{name: "no head", previous: 2, listing: `{"clones":[{"id":2,"snapshots":[{"id":2}],"size":0}]}`, wantErr: true},
		{name: "two heads", previous: 2, listing: `{"clones":[{"id":2,"snapshots":[{"id":2}],"size":0},{"id":"head","size":0},{"id":"head","size":0}]}`, wantErr: true},
		// A size, an ID or a list that is missing or null is not a zero.
		{name: "a clone without a size", previous: 2, listing: `{"clones":[{"id":2,"snapshots":[{"id":2}]},{"id":"head","snapshots":[],"size":0}]}`, wantErr: true},
		{name: "a clone with a null size", previous: 2, listing: `{"clones":[{"id":2,"snapshots":[{"id":2}],"size":null},{"id":"head","snapshots":[],"size":0}]}`, wantErr: true},
		{name: "a head without a size", previous: 2, listing: `{"clones":[{"id":2,"snapshots":[{"id":2}],"size":0},{"id":"head","snapshots":[]}]}`, wantErr: true},
		{name: "a head with a null size", previous: 2, listing: `{"clones":[{"id":2,"snapshots":[{"id":2}],"size":0},{"id":"head","snapshots":[],"size":null}]}`, wantErr: true},
		{name: "a version without an ID", previous: 2, listing: `{"clones":[{"id":2,"snapshots":[{"id":2}],"size":5},{"snapshots":[],"size":0},{"id":"head","snapshots":[],"size":0}]}`, wantErr: true},
		{name: "a version with a null ID", previous: 2, listing: `{"clones":[{"id":2,"snapshots":[{"id":2}],"size":5},{"id":null,"snapshots":[],"size":0},{"id":"head","snapshots":[],"size":0}]}`, wantErr: true},
		{name: "a version with another text as ID", previous: 2, listing: `{"clones":[{"id":2,"snapshots":[{"id":2}],"size":5},{"id":"snapdir","snapshots":[],"size":0},{"id":"head","snapshots":[],"size":0}]}`, wantErr: true},
		{name: "a snapshot without an ID", previous: 2, listing: `{"clones":[{"id":2,"snapshots":[{}],"size":5},{"id":2,"snapshots":[{"id":2}],"size":5},{"id":"head","snapshots":[],"size":0}]}`, wantErr: true},
		{name: "a snapshot with a null ID", previous: 2, listing: `{"clones":[{"id":2,"snapshots":[{"id":null},{"id":2}],"size":5},{"id":"head","snapshots":[],"size":0}]}`, wantErr: true},
		{name: "a clone without a list of snapshots", previous: 2, listing: `{"clones":[{"id":2,"size":5},{"id":"head","snapshots":[],"size":0}]}`, wantErr: true},
		{name: "a previous snapshot of zero and a snapshot ID of zero", previous: 0, listing: `{"clones":[{"id":1,"snapshots":[{"id":0}],"size":0},{"id":"head","snapshots":[],"size":0}]}`, wantErr: true},
		{name: "no versions", previous: 2, listing: `{"clones":[]}`, wantErr: true},
		{name: "no list of versions", previous: 2, listing: `{"name":"x"}`, wantErr: true},
		{name: "a null list of versions", previous: 2, listing: `{"clones":null}`, wantErr: true},
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

// These dumps are synthetic "perf dump mds_server" documents.
func TestBlockdiffRequestsTellsAnObservedZeroFromAMissingCount(t *testing.T) {
	for _, tc := range []struct {
		name, dump string
		want       uint64
		wantErr    bool
	}{
		{name: "a count", dump: `{"mds_server":{"req_blockdiff_latency":{"avgcount":4,"sum":0.01,"avgtime":0.0025}}}`, want: 4},
		{name: "a count of zero", dump: `{"mds_server":{"req_blockdiff_latency":{"avgcount":0,"sum":0,"avgtime":0}}}`},
		{name: "no count", dump: `{"mds_server":{"req_blockdiff_latency":{"sum":0.01}}}`, wantErr: true},
		{name: "a null count", dump: `{"mds_server":{"req_blockdiff_latency":{"avgcount":null}}}`, wantErr: true},
		{name: "a text count", dump: `{"mds_server":{"req_blockdiff_latency":{"avgcount":"4"}}}`, wantErr: true},
		{name: "a negative count", dump: `{"mds_server":{"req_blockdiff_latency":{"avgcount":-1}}}`, wantErr: true},
		{name: "a null counter", dump: `{"mds_server":{"req_blockdiff_latency":null}}`, wantErr: true},
		{name: "no blockdiff counter", dump: `{"mds_server":{"req_create_latency":{"avgcount":4}}}`, wantErr: true},
		{name: "no server section", dump: `{}`, wantErr: true},
		{name: "a null server section", dump: `{"mds_server":null}`, wantErr: true},
		{name: "an error text", dump: `Error ENOENT: problem getting command descriptions from mds.a`, wantErr: true},
		{name: "nothing", dump: ``, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := blockdiffRequests([]byte(tc.dump))
			if (err != nil) != tc.wantErr {
				t.Fatalf("error %v, want one: %t", err, tc.wantErr)
			}
			if err == nil && got != tc.want {
				t.Fatalf("count %d, want %d", got, tc.want)
			}
		})
	}
}

// These reports are synthetic outputs of the snapshot script.
func TestParseBlockdiffSnapshotTellsAnObservedZeroFromAMissingField(t *testing.T) {
	file := func(fields string) string { return `{` + fields + `}` }
	whole := file(`"ino":1,"size":4096,"sha256":"` + digestA + `"`)
	report := func(f, g string) string {
		return `{"present":true,"id":4,"name":"s3","files":{"f":` + f + `,"g":` + g + `}}`
	}
	for _, tc := range []struct {
		name, report string
		wantPresent  bool
		wantSize     uint64
		wantErr      bool
	}{
		{name: "a present snapshot", report: report(whole, whole), wantPresent: true, wantSize: 4096},
		{name: "a file of size zero", report: report(file(`"ino":1,"size":0,"sha256":"`+digestA+`"`), whole), wantPresent: true},
		{name: "an absent snapshot", report: `{"present":false,"name":"s3"}`},
		{name: "a file without a size", report: report(file(`"ino":1,"sha256":"`+digestA+`"`), whole), wantErr: true},
		{name: "a file with a null size", report: report(file(`"ino":1,"size":null,"sha256":"`+digestA+`"`), whole), wantErr: true},
		{name: "a file without an inode number", report: report(file(`"size":4096,"sha256":"`+digestA+`"`), whole), wantErr: true},
		{name: "a file with the inode number zero", report: report(file(`"ino":0,"size":4096,"sha256":"`+digestA+`"`), whole), wantErr: true},
		{name: "a file without a digest", report: report(file(`"ino":1,"size":4096`), whole), wantErr: true},
		{name: "a file with an empty digest", report: report(file(`"ino":1,"size":4096,"sha256":""`), whole), wantErr: true},
		{name: "a file with a short digest", report: report(file(`"ino":1,"size":4096,"sha256":"aa"`), whole), wantErr: true},
		{name: "a null file", report: report(`null`, whole), wantErr: true},
		{name: "one file", report: `{"present":true,"id":4,"name":"s3","files":{"g":` + whole + `}}`, wantErr: true},
		{name: "a third file", report: `{"present":true,"id":4,"name":"s3","files":{"f":` + whole + `,"g":` + whole + `,"h":` + whole + `}}`, wantErr: true},
		{name: "another file in place of the trigger file", report: `{"present":true,"id":4,"name":"s3","files":{"h":` + whole + `,"g":` + whole + `}}`, wantErr: true},
		{name: "no files", report: `{"present":true,"id":4,"name":"s3"}`, wantErr: true},
		{name: "null files", report: `{"present":true,"id":4,"name":"s3","files":null}`, wantErr: true},
		{name: "no ID", report: `{"present":true,"name":"s3","files":{"f":` + whole + `,"g":` + whole + `}}`, wantErr: true},
		{name: "a null ID", report: `{"present":true,"id":null,"name":"s3","files":{"f":` + whole + `,"g":` + whole + `}}`, wantErr: true},
		{name: "the ID zero", report: `{"present":true,"id":0,"name":"s3","files":{"f":` + whole + `,"g":` + whole + `}}`, wantErr: true},
		{name: "no presence", report: `{"id":4,"name":"s3","files":{"f":` + whole + `,"g":` + whole + `}}`, wantErr: true},
		{name: "a null presence", report: `{"present":null,"name":"s3"}`, wantErr: true},
		{name: "no name", report: `{"present":false}`, wantErr: true},
		{name: "an absent snapshot with files", report: `{"present":false,"name":"s3","files":{}}`, wantErr: true},
		{name: "an empty document", report: `{}`, wantErr: true},
		{name: "a traceback", report: "Traceback (most recent call last):", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseBlockdiffSnapshot([]byte(tc.report))
			if (err != nil) != tc.wantErr {
				t.Fatalf("error %v, want one: %t", err, tc.wantErr)
			}
			if err == nil && (got.Present != tc.wantPresent || got.Files["f"].Size != tc.wantSize) {
				t.Fatalf("snapshot %+v, want present %t and the size %d", got, tc.wantPresent, tc.wantSize)
			}
		})
	}
}
