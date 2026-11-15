//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rbd_mirror_unlink_trashed_snapshot_test

import (
	"strings"
	"testing"
)

// Receiver log lines in the form the case reads them. They are synthetic;
// they are not native evidence of what a receiver does.
const (
	logStart   = "debug 2026-11-15T04:20:01.000+0000 7f00 10 rbd::mirror::image_replayer::snapshot::Replayer: 0x5600 unlink_peer: remote_snap_id=5"
	logRefused = "debug 2026-11-15T04:20:01.010+0000 7f01 -1 librbd::mirror::snapshot::UnlinkPeerRequest: 0x5700 unlink_peer: not mirror snapshot (snap_id=5)"
	logSkipped = "debug 2026-11-15T04:20:01.010+0000 7f01 15 librbd::mirror::snapshot::UnlinkPeerRequest: 0x5700 unlink_peer: missing or trashed snapshot: snap_id=5"
	logMissing = "debug 2026-11-15T04:20:01.010+0000 7f01 15 librbd::mirror::snapshot::UnlinkPeerRequest: 0x5700 unlink_peer: missing snapshot: snap_id=5"
	logInvalid = "debug 2026-11-15T04:20:01.020+0000 7f00 -1 rbd::mirror::image_replayer::snapshot::Replayer: 0x5600 handle_unlink_peer: failed to unlink local peer from remote image: (22) Invalid argument"
	logTimeout = "debug 2026-11-15T04:20:01.020+0000 7f00 -1 rbd::mirror::image_replayer::snapshot::Replayer: 0x5600 handle_unlink_peer: failed to unlink local peer from remote image: (110) Connection timed out"
)

func unlinkLog(lines ...string) string { return strings.Join(lines, "\n") + "\n" }

func TestJudgeUnlinkReportsOnlyTheRefusedTrashedSnapshot(t *testing.T) {
	// Lines about other snapshots, which the case must not read as S1's.
	other := []string{
		strings.ReplaceAll(logStart, "=5", "=15"),
		strings.ReplaceAll(logRefused, "=5)", "=15)"),
		strings.ReplaceAll(logSkipped, "=5", "=51"),
		strings.ReplaceAll(logMissing, "=5", "=4"),
	}
	for _, tc := range []struct {
		name     string
		log      string
		symptoms int
		ready    bool
		fails    string
	}{
		{name: "refused and the replayer failed", log: unlinkLog(logStart, logRefused, logInvalid), symptoms: 2, ready: true},
		{name: "skipped as missing or trashed", log: unlinkLog(logStart, logSkipped), ready: true},
		{name: "refused, the replayer has not reported yet", log: unlinkLog(logStart, logRefused)},
		{name: "unlink started only", log: unlinkLog(logStart)},
		{name: "nothing about the snapshot", log: unlinkLog(other...)},
		{name: "a failure before the unlink from the snapshot is not its", log: unlinkLog(logInvalid, logStart)},
		{name: "found fully removed", log: unlinkLog(logStart, logMissing), fails: "trigger not established"},
		{name: "refused and skipped", log: unlinkLog(logStart, logRefused, logSkipped), fails: "another fault"},
		// The request logs on another thread than the replayer.
		{name: "refused before the replayer's unlink line", log: unlinkLog(logRefused, logStart, logInvalid), symptoms: 2, ready: true},
		{name: "refused, the replayer's lines not yet logged", log: unlinkLog(logRefused)},
		{name: "a failure of the next unlink is not the snapshot's", log: unlinkLog(logStart, logRefused, other[0], logInvalid)},
		{name: "a failure without the replayer's unlink from the snapshot", log: unlinkLog(logRefused, logInvalid)},
		{name: "skipped, then the replayer failed", log: unlinkLog(logStart, logSkipped, logInvalid), fails: "another fault"},
		{name: "refused, then another errno", log: unlinkLog(logStart, logRefused, logTimeout), fails: "another fault"},
		{name: "another errno without a refusal", log: unlinkLog(logStart, logTimeout), fails: "another fault"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			symptoms, ready, err := judgeUnlink(5, readUnlinkOutcome(tc.log, 5))
			if tc.fails != "" {
				if err == nil || !strings.Contains(err.Error(), tc.fails) {
					t.Fatalf("got symptoms %q ready %t error %v, want an error containing %q", symptoms, ready, err, tc.fails)
				}
				return
			}
			if err != nil || ready != tc.ready || len(symptoms) != tc.symptoms {
				t.Fatalf("got symptoms %q ready %t error %v, want %d symptoms ready %t", symptoms, ready, err, tc.symptoms, tc.ready)
			}
		})
	}
}

func TestReadUnlinkOutcomeAcceptsCarriageReturns(t *testing.T) {
	got := readUnlinkOutcome(strings.ReplaceAll(unlinkLog(logStart, logRefused, logInvalid), "\n", "\r\n"), 5)
	if !got.Started || !got.Refused || len(got.FailedErrnos) != 1 || got.FailedErrnos[0] != 22 {
		t.Fatalf("outcome %+v", got)
	}
}

func TestTrashedMirrorSnapshotNeedsTheOriginalMirrorRecord(t *testing.T) {
	listing := []byte(`[
{"id":4,"name":".mirror.primary.g.a","namespace":{"type":"mirror","state":"primary","mirror_peer_uuids":["p"],"complete":true}},
{"id":5,"name":"6d1f","namespace":{"type":"trash","original_namespace_type":"mirror","original_name":".mirror.primary.g.b"}},
{"id":6,"name":"7e20","namespace":{"type":"trash","original_namespace_type":"user","original_name":".mirror.primary.g.c"}}]`)
	snaps, err := parseUnlinkSnapshots(listing)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		id   uint64
		name string
		want bool
	}{
		{5, ".mirror.primary.g.b", true},
		{5, ".mirror.primary.g.a", false},
		{4, ".mirror.primary.g.a", false},
		{6, ".mirror.primary.g.c", false},
		{7, ".mirror.primary.g.b", false},
	} {
		if got := trashedMirrorSnapshot(snaps, tc.id, tc.name); got != tc.want {
			t.Errorf("trashedMirrorSnapshot(%d, %s) = %t, want %t", tc.id, tc.name, got, tc.want)
		}
	}
	if _, err := parseUnlinkSnapshots([]byte("rbd: error")); err == nil {
		t.Fatal("a listing that is not JSON was accepted")
	}
}
