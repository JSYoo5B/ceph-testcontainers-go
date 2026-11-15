//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_retained_snapshot_full_pool_test

import (
	"strings"
	"syscall"
	"testing"
)

const (
	testLivePath     = "/volumes/_nogroup/retained-full/1111"
	testSnapshotPath = "/volumes/_nogroup/retained-full/.snap/checkpoint/1111"
)

// contractReport is the report of a release that keeps the contract: the
// failed removal changed nothing and the retry retained the snapshot.
func contractReport() removalReport {
	live, snapshot := retainedDigest(liveLabel), retainedDigest(snapshotLabel)
	return removalReport{
		LivePath: testLivePath, SnapshotPath: testSnapshotPath, LiveSHA: live, SnapshotSHA: snapshot,
		Meta: metaSeen{Sections: map[string]map[string]string{"GLOBAL": {
			"version": "2", "type": "subvolume", "path": testLivePath, "state": "complete",
		}}},
		Live:     fileSeen{Size: retainedPayloadBytes, SHA256: live},
		Snapshot: fileSeen{Size: retainedPayloadBytes, SHA256: snapshot},
		GetPath:  cephOutcome{OK: true, Out: testLivePath + "\n"}, SnapshotGetPath: cephOutcome{OK: true, Out: testSnapshotPath + "\n"},
		Retry:     cephOutcome{OK: true},
		RetryInfo: cephOutcome{OK: true}, RetryState: "snapshot-retained",
		RetryGetPath:         cephOutcome{Errno: "ENOENT", Reason: "subvolume is removed and has only snapshots retained"},
		RetrySnapshotGetPath: cephOutcome{OK: true, Out: testSnapshotPath},
		RetrySnapshot:        fileSeen{Size: retainedPayloadBytes, SHA256: snapshot},
	}
}

// These cases feed synthetic reports to judgeRemoval. They are not native
// evidence of what a release does.
func TestJudgeRemoval(t *testing.T) {
	missing := cephOutcome{Errno: "ENOENT", Reason: "mount path missing for subvolume 'retained-full'"}
	for _, tc := range []struct {
		name   string
		change func(r *removalReport)
		want   []string
		fault  string
	}{
		{name: "contract"},
		{name: "live incarnation trashed before the descriptor was persisted", change: func(r *removalReport) {
			r.Live = fileSeen{Errno: int(syscall.ENOENT)}
			r.GetPath, r.SnapshotGetPath, r.Retry = missing, missing, missing
		}, want: []string{
			"the persisted metadata names " + testLivePath + " as the complete live incarnation, but it is gone",
			"getpath failed: ENOENT",
			"snapshot getpath failed: ENOENT",
			"the retried removal failed: ENOENT",
		}},
		{name: "retry fails on a consistent subvolume", change: func(r *removalReport) { r.Retry = cephOutcome{Errno: "EINVAL"} },
			want: []string{"the retried removal failed: EINVAL"}},
		{name: "snapshot bytes lost", change: func(r *removalReport) { r.Snapshot = fileSeen{Errno: int(syscall.ENOENT)} },
			want: []string{"the snapshot file is unreadable or changed: errno 2"}},
		{name: "retry leaves the subvolume complete", change: func(r *removalReport) {
			r.RetryState, r.RetryGetPath = "complete", cephOutcome{OK: true, Out: testLivePath}
		}, want: []string{"after the retry the subvolume is not snapshot-retained", "after the retry getpath still returns a live incarnation"}},
		{name: "retry loses the snapshot", change: func(r *removalReport) { r.RetrySnapshotGetPath = missing },
			want: []string{"after the retry snapshot getpath failed: ENOENT"}},
		{name: "retry snapshot bytes changed", change: func(r *removalReport) { r.RetrySnapshot.SHA256 = r.LiveSHA },
			want: []string{"after the retry the snapshot file is unreadable or changed"}},
		{name: "unreadable metadata", change: func(r *removalReport) { r.Meta = metaSeen{Errno: int(syscall.EIO)} },
			fault: "another fault: the persisted metadata is unreadable"},
		{name: "removal persisted despite its error", change: func(r *removalReport) {
			r.Meta.Sections["GLOBAL"]["state"], r.Meta.Sections["GLOBAL"]["path"] = "snapshot-retained", ""
		}, fault: "another fault: the failed removal persisted state \"snapshot-retained\""},
		{name: "live bytes changed", change: func(r *removalReport) { r.Live.SHA256 = r.SnapshotSHA },
			fault: "another fault: the live file holds"},
		{name: "live file unreadable otherwise", change: func(r *removalReport) { r.Live = fileSeen{Errno: int(syscall.EIO)} },
			fault: "another fault: the live file is unreadable"},
		{name: "getpath answers another path", change: func(r *removalReport) { r.GetPath.Out = "/elsewhere" },
			fault: "another fault: getpath returned \"/elsewhere\""},
		{name: "snapshot getpath answers another path", change: func(r *removalReport) { r.SnapshotGetPath.Out = "/elsewhere" },
			fault: "another fault: snapshot getpath returned"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report := contractReport()
			if tc.change != nil {
				tc.change(&report)
			}
			symptoms, err := judgeRemoval(report)
			if tc.fault != "" {
				if err == nil || !strings.HasPrefix(err.Error(), tc.fault) {
					t.Fatalf("error %v, want %q", err, tc.fault)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(symptoms) != len(tc.want) {
				t.Fatalf("symptoms %q, want %q", symptoms, tc.want)
			}
			for i, want := range tc.want {
				if !strings.HasPrefix(symptoms[i], want) {
					t.Fatalf("symptom %d is %q, want %q", i, symptoms[i], want)
				}
			}
		})
	}
	if got := len(retainedPayload(liveLabel)); got != retainedPayloadBytes {
		t.Fatalf("payload of %d bytes", got)
	}
	if retainedDigest(liveLabel) == retainedDigest(snapshotLabel) {
		t.Fatal("the live and snapshot contents are not distinguishable")
	}
}
