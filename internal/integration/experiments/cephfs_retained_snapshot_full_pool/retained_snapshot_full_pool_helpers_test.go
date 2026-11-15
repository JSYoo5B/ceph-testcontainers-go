//go:build all || integration

package cephfs_retained_snapshot_full_pool_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"syscall"
)

const (
	// retainedName is the subvolume of the case, in the default group, and
	// retainedSnapshot its snapshot.
	retainedName     = "retained-full"
	retainedSnapshot = "checkpoint"
	// retainedPayloadBytes is the size of the file the subvolume holds.
	retainedPayloadBytes = 256 << 10
	// snapshotLabel names the content the snapshot holds, liveLabel the
	// content the live incarnation holds after the snapshot.
	snapshotLabel = "snapshot-content"
	liveLabel     = "live-content"
)

// retainedPayload is the content written for label: the SHA-256 of the label
// repeated to retainedPayloadBytes, as the client script writes it.
func retainedPayload(label string) []byte {
	block := sha256.Sum256([]byte(label))
	return bytes.Repeat(block[:], retainedPayloadBytes/len(block))
}

// retainedDigest is the hex SHA-256 of the content written for label.
func retainedDigest(label string) string {
	sum := sha256.Sum256(retainedPayload(label))
	return hex.EncodeToString(sum[:])
}

// cephOutcome is the result of one ceph command: whether it succeeded, the
// errno and reason it reported otherwise, and its output.
type cephOutcome struct {
	OK            bool
	Errno, Reason string
	Out           string
}

func (o cephOutcome) String() string {
	if o.OK {
		return "ok"
	}
	return fmt.Sprintf("%s (%s)", o.Errno, o.Reason)
}

// fileSeen is what the client read of one file: errno 0 with its size and
// SHA-256, or the errno of the open or read.
type fileSeen struct {
	Errno  int    `json:"errno"`
	Size   int    `json:"size"`
	SHA256 string `json:"sha256"`
}

// metaSeen is the persisted subvolume metadata file (.meta) as the client
// parsed it, by section, or the errno of reading it.
type metaSeen struct {
	Errno    int                          `json:"errno"`
	Sections map[string]map[string]string `json:"sections"`
}

// dirSeen is a directory listing, or the errno of reading it.
type dirSeen struct {
	Errno int      `json:"errno"`
	Names []string `json:"names"`
}

// removalReport is what the case observed after the removal with retained
// snapshots failed on the full cluster and the thresholds were restored,
// and after the removal was retried. LivePath, SnapshotPath and the two
// digests are what the subvolume held before the removal.
type removalReport struct {
	LivePath, SnapshotPath string
	LiveSHA, SnapshotSHA   string
	// Meta is the persisted metadata; Live and Snapshot are direct reads of
	// the file at the live and snapshot paths.
	Meta           metaSeen
	Live, Snapshot fileSeen
	// GetPath and SnapshotGetPath are the volumes interface's answers.
	GetPath, SnapshotGetPath cephOutcome
	// Retry is the second removal with retained snapshots.
	Retry cephOutcome
	// After a successful retry: the subvolume's state from info, the live
	// and snapshot getpath answers and the file read at the snapshot path
	// the volumes interface returned.
	RetryInfo                          cephOutcome
	RetryState                         string
	RetryGetPath, RetrySnapshotGetPath cephOutcome
	RetrySnapshot                      fileSeen
}

// judgeRemoval decides whether the failed removal left the subvolume
// inconsistent or unrecoverable. The contract is that a removal with
// retained snapshots that fails to persist its descriptor changes nothing:
// the metadata still names the live incarnation as complete, the
// incarnation and the snapshot keep their bytes and paths, and a retry
// retains the snapshot. It returns the symptoms of the defect, none for the
// contract, and an error starting with "another fault:" for a state that is
// neither.
func judgeRemoval(r removalReport) ([]string, error) {
	if r.Meta.Errno != 0 {
		return nil, fmt.Errorf("another fault: the persisted metadata is unreadable after the thresholds were restored: errno %d", r.Meta.Errno)
	}
	global := r.Meta.Sections["GLOBAL"]
	if global["state"] != "complete" || global["path"] != r.LivePath {
		return nil, fmt.Errorf("another fault: the failed removal persisted state %q and path %q, want complete and %s", global["state"], global["path"], r.LivePath)
	}
	var symptoms []string
	switch {
	case r.Live.Errno == int(syscall.ENOENT):
		symptoms = append(symptoms, fmt.Sprintf("the persisted metadata names %s as the complete live incarnation, but it is gone", r.LivePath))
	case r.Live.Errno != 0:
		return nil, fmt.Errorf("another fault: the live file is unreadable: errno %d", r.Live.Errno)
	case r.Live.SHA256 != r.LiveSHA:
		return nil, fmt.Errorf("another fault: the live file holds %s, want %s", r.Live.SHA256, r.LiveSHA)
	}
	switch path := strings.TrimSpace(r.GetPath.Out); {
	case !r.GetPath.OK:
		symptoms = append(symptoms, "getpath failed: "+r.GetPath.String())
	case path != r.LivePath:
		return nil, fmt.Errorf("another fault: getpath returned %q, want %s", path, r.LivePath)
	}
	if r.Snapshot.Errno != 0 || r.Snapshot.SHA256 != r.SnapshotSHA {
		symptoms = append(symptoms, fmt.Sprintf("the snapshot file is unreadable or changed: errno %d, sha256 %s", r.Snapshot.Errno, r.Snapshot.SHA256))
	}
	switch path := strings.TrimSpace(r.SnapshotGetPath.Out); {
	case !r.SnapshotGetPath.OK:
		symptoms = append(symptoms, "snapshot getpath failed: "+r.SnapshotGetPath.String())
	case path != r.SnapshotPath:
		return nil, fmt.Errorf("another fault: snapshot getpath returned %q, want %s", path, r.SnapshotPath)
	}
	if !r.Retry.OK {
		return append(symptoms, "the retried removal failed: "+r.Retry.String()), nil
	}
	if !r.RetryInfo.OK || r.RetryState != "snapshot-retained" {
		symptoms = append(symptoms, fmt.Sprintf("after the retry the subvolume is not snapshot-retained: info %s, state %q", r.RetryInfo, r.RetryState))
	}
	if r.RetryGetPath.OK {
		symptoms = append(symptoms, "after the retry getpath still returns a live incarnation")
	}
	switch path := strings.TrimSpace(r.RetrySnapshotGetPath.Out); {
	case !r.RetrySnapshotGetPath.OK:
		symptoms = append(symptoms, "after the retry snapshot getpath failed: "+r.RetrySnapshotGetPath.String())
	case path != r.SnapshotPath:
		symptoms = append(symptoms, fmt.Sprintf("after the retry snapshot getpath returned %q, want %s", path, r.SnapshotPath))
	case r.RetrySnapshot.Errno != 0 || r.RetrySnapshot.SHA256 != r.SnapshotSHA:
		symptoms = append(symptoms, fmt.Sprintf("after the retry the snapshot file is unreadable or changed: errno %d, sha256 %s", r.RetrySnapshot.Errno, r.RetrySnapshot.SHA256))
	}
	return symptoms, nil
}
