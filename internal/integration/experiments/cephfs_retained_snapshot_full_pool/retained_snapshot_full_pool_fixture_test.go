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
	denied := cephOutcome{Errno: "EACCES", Reason: "permission denied"}
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
		{name: "retry reports the missing path on a consistent subvolume", change: func(r *removalReport) { r.Retry = missing },
			want: []string{"the retried removal failed: ENOENT"}},
		{name: "snapshot file gone", change: func(r *removalReport) { r.Snapshot = fileSeen{Errno: int(syscall.ENOENT)} },
			want: []string{"the snapshot file is gone: errno 2"}},
		{name: "snapshot file changed", change: func(r *removalReport) { r.Snapshot.SHA256 = r.LiveSHA },
			want: []string{"the snapshot file changed"}},
		{name: "retry leaves the subvolume complete", change: func(r *removalReport) {
			r.RetryState, r.RetryGetPath = "complete", cephOutcome{OK: true, Out: testLivePath}
		}, want: []string{"after the retry the subvolume is not snapshot-retained", "after the retry getpath still returns a live incarnation"}},
		{name: "retry leaves the subvolume without a path", change: func(r *removalReport) { r.RetryInfo, r.RetryState = missing, "" },
			want: []string{"after the retry info failed: ENOENT"}},
		{name: "retry loses the snapshot", change: func(r *removalReport) { r.RetrySnapshotGetPath = missing },
			want: []string{"after the retry snapshot getpath failed: ENOENT"}},
		{name: "retry moves the snapshot", change: func(r *removalReport) { r.RetrySnapshotGetPath.Out = "/elsewhere" },
			want: []string{"after the retry snapshot getpath returned \"/elsewhere\""}},
		{name: "retry snapshot file gone", change: func(r *removalReport) { r.RetrySnapshot = fileSeen{Errno: int(syscall.ENOENT)} },
			want: []string{"after the retry the snapshot file is gone"}},
		{name: "retry snapshot bytes changed", change: func(r *removalReport) { r.RetrySnapshot.SHA256 = r.LiveSHA },
			want: []string{"after the retry the snapshot file changed"}},
		// A failure with another errno than the missing path the stage
		// expects establishes neither the symptom nor its absence.
		{name: "getpath denied", change: func(r *removalReport) { r.GetPath = denied },
			fault: "another fault: getpath failed with EACCES"},
		{name: "getpath without an errno", change: func(r *removalReport) { r.GetPath = cephOutcome{Reason: "timed out"} },
			fault: "another fault: getpath failed with "},
		{name: "snapshot getpath denied", change: func(r *removalReport) { r.SnapshotGetPath = denied },
			fault: "another fault: snapshot getpath failed with EACCES"},
		{name: "snapshot getpath denied on a gone incarnation", change: func(r *removalReport) {
			r.Live, r.GetPath, r.SnapshotGetPath = fileSeen{Errno: int(syscall.ENOENT)}, missing, denied
		}, fault: "another fault: snapshot getpath failed with EACCES"},
		{name: "retry denied", change: func(r *removalReport) { r.Retry = denied },
			fault: "another fault: the retried removal failed with EACCES"},
		{name: "retry refused as invalid", change: func(r *removalReport) { r.Retry = cephOutcome{Errno: "EINVAL"} },
			fault: "another fault: the retried removal failed with EINVAL"},
		{name: "retry still full", change: func(r *removalReport) { r.Retry = cephOutcome{Errno: "ENOSPC"} },
			fault: "another fault: the retried removal failed with ENOSPC"},
		{name: "retry denied on a gone incarnation", change: func(r *removalReport) {
			r.Live, r.GetPath, r.SnapshotGetPath, r.Retry = fileSeen{Errno: int(syscall.ENOENT)}, missing, missing, denied
		}, fault: "another fault: the retried removal failed with EACCES"},
		{name: "info after the retry denied", change: func(r *removalReport) { r.RetryInfo, r.RetryState = denied, "" },
			fault: "another fault: info after the retry failed with EACCES"},
		{name: "getpath after the retry denied", change: func(r *removalReport) { r.RetryGetPath = denied },
			fault: "another fault: getpath after the retry failed with EACCES"},
		{name: "getpath after the retry timed out", change: func(r *removalReport) { r.RetryGetPath = cephOutcome{Errno: "ETIMEDOUT"} },
			fault: "another fault: getpath after the retry failed with ETIMEDOUT"},
		{name: "snapshot getpath after the retry denied", change: func(r *removalReport) { r.RetrySnapshotGetPath = denied },
			fault: "another fault: snapshot getpath after the retry failed with EACCES"},
		{name: "snapshot file unreadable otherwise", change: func(r *removalReport) { r.Snapshot = fileSeen{Errno: int(syscall.EACCES)} },
			fault: "another fault: the snapshot file is unreadable: errno 13"},
		{name: "retry snapshot file unreadable otherwise", change: func(r *removalReport) { r.RetrySnapshot = fileSeen{Errno: int(syscall.EIO)} },
			fault: "another fault: after the retry the snapshot file is unreadable: errno 5"},
		{name: "unreadable metadata", change: func(r *removalReport) { r.Meta = metaSeen{Errno: int(syscall.EIO)} },
			fault: "another fault: the persisted metadata is unreadable"},
		{name: "removal persisted despite its error", change: func(r *removalReport) {
			r.Meta.Sections["GLOBAL"]["state"], r.Meta.Sections["GLOBAL"]["path"] = "snapshot-retained", ""
		}, fault: "another fault: the failed removal persisted state \"snapshot-retained\""},
		{name: "live bytes changed", change: func(r *removalReport) { r.Live.SHA256 = r.SnapshotSHA },
			fault: "another fault: the live file holds"},
		{name: "live file unreadable otherwise", change: func(r *removalReport) { r.Live = fileSeen{Errno: int(syscall.EACCES)} },
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
