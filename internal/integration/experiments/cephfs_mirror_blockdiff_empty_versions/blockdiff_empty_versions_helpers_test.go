//go:build all || integration

package cephfs_mirror_blockdiff_empty_versions_test

import (
	"encoding/json"
	"fmt"
	"strings"
)

// blockdiffFile is the size and content of one file in a snapshot.
type blockdiffFile struct {
	Inode  uint64 `json:"ino"`
	Size   uint64 `json:"size"`
	SHA256 string `json:"sha256"`
}

// blockdiffSnapshot is a snapshot of the mirrored directory as a client of
// one filesystem reads it. ID is that filesystem's own snapshot ID.
type blockdiffSnapshot struct {
	Present bool                     `json:"present"`
	ID      uint64                   `json:"id"`
	Name    string                   `json:"name"`
	Files   map[string]blockdiffFile `json:"files"`
}

// sameContent reports whether other has the files of snapshot with the same
// sizes and content. Inode numbers and snapshot IDs belong to each
// filesystem and are not compared.
func (snapshot blockdiffSnapshot) sameContent(other blockdiffSnapshot) bool {
	if !snapshot.Present || !other.Present || len(snapshot.Files) == 0 || len(snapshot.Files) != len(other.Files) {
		return false
	}
	for name, file := range snapshot.Files {
		if got, ok := other.Files[name]; !ok || got.Size != file.Size || got.SHA256 != file.SHA256 || file.SHA256 == "" {
			return false
		}
	}
	return true
}

// versionSizes reads the JSON of "rados listsnaps" for one object and
// returns the size of the clone that holds the snapshot previous and the
// size of the head. It refuses a listing in which previous is in no clone,
// the object has no head, or a clone holds a snapshot newer than previous:
// the head is then not the version that follows the clone of previous.
func versionSizes(listing []byte, previous uint64) (clone, head uint64, err error) {
	var object struct {
		Clones []struct {
			ID        json.RawMessage `json:"id"`
			Snapshots []struct {
				ID uint64 `json:"id"`
			} `json:"snapshots"`
			Size *uint64 `json:"size"`
		} `json:"clones"`
	}
	if err := json.Unmarshal(listing, &object); err != nil {
		return 0, 0, fmt.Errorf("object versions %s: %w", listing, err)
	}
	var hasClone, hasHead bool
	for _, version := range object.Clones {
		if version.Size == nil {
			return 0, 0, fmt.Errorf("object versions %s: a version has no size", listing)
		}
		if string(version.ID) == `"head"` {
			if hasHead {
				return 0, 0, fmt.Errorf("object versions %s: two heads", listing)
			}
			hasHead, head = true, *version.Size
			continue
		}
		for _, snapshot := range version.Snapshots {
			switch {
			case snapshot.ID > previous:
				return 0, 0, fmt.Errorf("object versions %s: a clone holds snapshot %d, newer than %d", listing, snapshot.ID, previous)
			case snapshot.ID == previous:
				hasClone, clone = true, *version.Size
			}
		}
	}
	if !hasClone || !hasHead {
		return 0, 0, fmt.Errorf("object versions %s: clone of snapshot %d found: %t, head found: %t", listing, previous, hasClone, hasHead)
	}
	return clone, head, nil
}

// The two parts of the source MDS log that identify the documented abort:
// the assertion that an interval has a length, and the function that
// aggregates the object versions of a blockdiff request.
const (
	blockdiffAssertion = "FAILED ceph_assert(len > 0)"
	blockdiffFrame     = "aggregate_snap_sets"
)

// blockdiffOutcome is what the trigger step left.
type blockdiffOutcome struct {
	// SameProcess reports that the source MDS container still runs the
	// process it ran before the trigger and that the process answers
	// commands as the same daemon generation (GID).
	SameProcess bool
	// Process describes the MDS container after the trigger.
	Process string
	// Log holds the lines of the source MDS log that contain
	// blockdiffAssertion or blockdiffFrame.
	Log []string
	// RequestsRead reports that the blockdiff request counter was read from
	// the same process after the trigger; RequestsBefore and RequestsAfter
	// are its values around the trigger step.
	RequestsRead                  bool
	RequestsBefore, RequestsAfter uint64
	// Synced reports that the mirror named the trigger snapshot as last
	// synchronized, and Status the last mirror status of the directory.
	Synced bool
	Status string
	// Source and Destination are the trigger snapshot on either side.
	Source, Destination blockdiffSnapshot
}

// judgeBlockdiff decides what the incremental synchronization of the
// trigger snapshot did to the source MDS.
//
// It returns the symptoms of the defect when the MDS process that served the
// source is gone and its log has the interval assertion in the function that
// aggregates object versions. It returns neither symptoms nor an error when
// the same process is live, served at least one more blockdiff request, and
// the snapshot arrived with the source's sizes and content. Everything else
// is an error: a live MDS that served no blockdiff request was not asked the
// question, an MDS that ended without the assertion ended for another
// reason, and a live MDS with a missing or different snapshot is another
// fault.
func judgeBlockdiff(got blockdiffOutcome) ([]string, error) {
	if !got.SameProcess {
		var assertion, frame bool
		for _, line := range got.Log {
			assertion = assertion || strings.Contains(line, blockdiffAssertion)
			frame = frame || strings.Contains(line, blockdiffFrame)
		}
		if !assertion || !frame {
			return nil, fmt.Errorf("another fault: the source MDS process ended (%s) and its log has the assertion %q: %t, the function %s: %t; log lines %q", got.Process, blockdiffAssertion, assertion, blockdiffFrame, frame, got.Log)
		}
		symptoms := []string{"the source MDS process that served the blockdiff request ended: " + got.Process}
		for _, line := range got.Log {
			if len(symptoms) < 5 {
				symptoms = append(symptoms, "MDS log: "+strings.TrimSpace(line))
			}
		}
		return append(symptoms, fmt.Sprintf("the mirror did not finish the snapshot (synced: %t, present on the destination: %t): %s", got.Synced, got.Destination.Present, got.Status)), nil
	}
	if !got.RequestsRead || got.RequestsAfter <= got.RequestsBefore {
		return nil, fmt.Errorf("trigger not established: the source MDS served no blockdiff request for the trigger file (counter read: %t, %d before, %d after); mirror status %s", got.RequestsRead, got.RequestsBefore, got.RequestsAfter, got.Status)
	}
	if !got.Synced {
		return nil, fmt.Errorf("another fault: the source MDS is live and served the blockdiff request, and the mirror did not report the snapshot as synchronized: %s", got.Status)
	}
	if !got.Source.sameContent(got.Destination) {
		return nil, fmt.Errorf("another fault: the source MDS is live and the snapshot was synchronized, and the destination has %+v, want the sizes and content of %+v", got.Destination, got.Source)
	}
	return nil, nil
}
