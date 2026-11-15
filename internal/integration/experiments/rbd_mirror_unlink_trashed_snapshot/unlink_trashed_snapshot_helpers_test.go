//go:build all || integration

package rbd_mirror_unlink_trashed_snapshot_test

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// unlinkSnapshot is one entry of rbd snap ls --all: the namespace fields that
// tell a mirror snapshot, with the peers still linked to it, from the trashed
// record of a removed one.
type unlinkSnapshot struct {
	ID        uint64 `json:"id"`
	Name      string `json:"name"`
	Namespace struct {
		Type                  string   `json:"type"`
		State                 string   `json:"state"`
		MirrorPeerUUIDs       []string `json:"mirror_peer_uuids"`
		Complete              *bool    `json:"complete"`
		PrimarySnapID         uint64   `json:"primary_snap_id"`
		OriginalNamespaceType string   `json:"original_namespace_type"`
		OriginalName          string   `json:"original_name"`
	} `json:"namespace"`
}

func parseUnlinkSnapshots(listing []byte) ([]unlinkSnapshot, error) {
	var snaps []unlinkSnapshot
	if err := json.Unmarshal(listing, &snaps); err != nil {
		return nil, fmt.Errorf("decode snapshot listing: %w: %s", err, listing)
	}
	return snaps, nil
}

func findUnlinkSnapshot(snaps []unlinkSnapshot, id uint64) (unlinkSnapshot, bool) {
	for _, snap := range snaps {
		if snap.ID == id {
			return snap, true
		}
	}
	return unlinkSnapshot{}, false
}

// trashedMirrorSnapshot reports whether id is listed as the trashed record
// of the mirror snapshot named name: a removal that a clone still holds.
func trashedMirrorSnapshot(snaps []unlinkSnapshot, id uint64, name string) bool {
	snap, ok := findUnlinkSnapshot(snaps, id)
	return ok && snap.Namespace.Type == "trash" && snap.Namespace.OriginalNamespaceType == "mirror" && snap.Namespace.OriginalName == name
}

// The receiver lines the judgement reads. The receiver's snapshot replayer
// names the remote snapshot it unlinks itself from (debug_rbd_mirror 10)
// and reports a failed unlink with its errno. The unlink request of the
// remote image (debug_rbd 15 on the source cluster's peer client) refuses a
// snapshot that is not a mirror snapshot, and in a corrected release skips a
// trashed one together with a missing one.
var (
	unlinkStartLine    = regexp.MustCompile(`image_replayer::snapshot::Replayer: \S+ unlink_peer: remote_snap_id=(\d+)\s*$`)
	unlinkFailedLine   = regexp.MustCompile(`image_replayer::snapshot::Replayer: \S+ handle_unlink_peer: failed to unlink local peer from remote image: \((\d+)\) `)
	unlinkRefusedLine  = regexp.MustCompile(`librbd::mirror::snapshot::UnlinkPeerRequest: \S+ unlink_peer: not mirror snapshot \(snap_id=(\d+)\)\s*$`)
	unlinkSkippedLine  = regexp.MustCompile(`librbd::mirror::snapshot::UnlinkPeerRequest: \S+ unlink_peer: missing or trashed snapshot: snap_id=(\d+)\s*$`)
	unlinkMissingLine  = regexp.MustCompile(`librbd::mirror::snapshot::UnlinkPeerRequest: \S+ unlink_peer: missing snapshot: snap_id=(\d+)\s*$`)
	unlinkInvalidErrno = 22
)

// unlinkOutcome is what the receiver log says about its unlink from one
// snapshot. Lines holds the matched lines in log order.
type unlinkOutcome struct {
	Started, Refused, Skipped, Missing bool
	// FailedErrnos are the errnos of the replayer's failed unlinks that
	// follow its unlink from the snapshot.
	FailedErrnos []int
	Lines        []string
}

// readUnlinkOutcome collects the lines about the receiver's unlink from snap.
// The replayer runs one unlink at a time and its failure line names no
// snapshot, so a failure counts only between the replayer's unlink from snap
// and its next unlink. Both lines come from the receiver's own context and
// keep their order. The request's lines come from the context of its
// connection to the source, which logs on its own thread, so they are not
// ordered against the replayer's.
func readUnlinkOutcome(log string, snap uint64) unlinkOutcome {
	var got unlinkOutcome
	current := false
	id := strconv.FormatUint(snap, 10)
	for _, line := range strings.Split(log, "\n") {
		line = strings.TrimRight(line, "\r")
		if m := unlinkStartLine.FindStringSubmatch(line); m != nil {
			// The replayer's next unlink ends the one from snap.
			current = m[1] == id
			if current {
				got.Started = true
				got.Lines = append(got.Lines, line)
			}
			continue
		}
		if m := unlinkRefusedLine.FindStringSubmatch(line); m != nil && m[1] == id {
			got.Refused = true
			got.Lines = append(got.Lines, line)
			continue
		}
		if m := unlinkSkippedLine.FindStringSubmatch(line); m != nil && m[1] == id {
			got.Skipped = true
			got.Lines = append(got.Lines, line)
			continue
		}
		if m := unlinkMissingLine.FindStringSubmatch(line); m != nil && m[1] == id {
			got.Missing = true
			got.Lines = append(got.Lines, line)
			continue
		}
		if m := unlinkFailedLine.FindStringSubmatch(line); m != nil && current {
			errno, err := strconv.Atoi(m[1])
			if err != nil {
				errno = -1
			}
			got.FailedErrnos = append(got.FailedErrnos, errno)
			got.Lines = append(got.Lines, line)
		}
	}
	return got
}

// judgeUnlink turns the receiver's unlink from the trashed snapshot into the
// symptoms of the defect. ready is false while the outcome is still
// incomplete. It returns symptoms when the unlink request refused the
// trashed snapshot as not a mirror snapshot and the replayer failed with
// EINVAL, and none when the request skipped it as missing or trashed and
// the replayer did not fail. It is not ready while the replayer has not
// logged its unlink from snap. Everything else is an error: an unlink that
// found the snapshot fully removed did not meet a trashed one, and any other
// failure or a contradictory log is another fault.
func judgeUnlink(snap uint64, got unlinkOutcome) (symptoms []string, ready bool, err error) {
	evidence := strings.Join(got.Lines, " | ")
	switch {
	case got.Missing:
		return nil, false, fmt.Errorf("trigger not established: the unlink found snapshot %d fully removed, not trashed: %s", snap, evidence)
	case got.Refused && got.Skipped:
		return nil, false, fmt.Errorf("another fault: snapshot %d was both refused and skipped: %s", snap, evidence)
	case !got.Started:
		// The replayer's line may still be on its way.
		return nil, false, nil
	}
	for _, errno := range got.FailedErrnos {
		if errno != unlinkInvalidErrno || !got.Refused {
			return nil, false, fmt.Errorf("another fault: the replayer's unlink after snapshot %d failed with errno %d: %s", snap, errno, evidence)
		}
	}
	switch {
	case got.Refused && len(got.FailedErrnos) > 0:
		return []string{
			fmt.Sprintf("the unlink request refused trashed snapshot %d as not a mirror snapshot", snap),
			fmt.Sprintf("the snapshot replayer failed: failed to unlink local peer from remote image: (%d) Invalid argument", unlinkInvalidErrno),
		}, true, nil
	case got.Skipped:
		return nil, true, nil
	}
	return nil, false, nil
}
