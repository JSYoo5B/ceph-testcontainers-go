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

// fullRemovalTrigger records the initial command's exit and complete error
// output. The traceback can exceed the ordinary NativeFailure reason bound.
type fullRemovalTrigger struct {
	exitCode int
	stderr   string
}

func (r fullRemovalTrigger) verify() error {
	if r.exitCode == 0 {
		return fmt.Errorf("condition not established: the removal with retained snapshots succeeded while OSD_FULL was raised")
	}
	for line := range strings.SplitSeq(r.stderr, "\n") {
		if reason, ok := strings.CutPrefix(strings.TrimSpace(line), "Error ENOSPC:"); ok {
			switch strings.TrimSpace(reason) {
			case "fsync failed", "error in write":
				return nil
			}
		}
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Error EINVAL:") {
			// Affected volumes modules leave the metadata write's exception
			// uncaught. Correlate the no-space exception with that write.
			if strings.Contains(r.stderr, "metadata_manager.py") && strings.Contains(r.stderr, "in flush") &&
				(strings.Contains(r.stderr, "cephfs.NoSpace: fsync failed") ||
					strings.Contains(r.stderr, "cephfs.NoSpace: error in write")) {
				return nil
			}
		}
	}
	return fmt.Errorf("condition not established: the full-cluster removal did not report a descriptor write failing with no space (exit %d)", r.exitCode)
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
	Trigger                fullRemovalTrigger
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

// missingErrno is the errno a volumes call reports for a subvolume path that
// does not exist: the incarnation of a subvolume whose metadata names a gone
// path, or the live path of a snapshot-retained subvolume. Every failure a
// stage can turn into a verdict must be this errno; any other failure (a
// denied lookup, a timeout, a busy MGR) establishes neither the symptom nor
// its absence and fails the test.
const missingErrno = "ENOENT"

// unrelated returns the "another fault:" error of a command failure that is
// not the stage's expected missingErrno.
func unrelated(stage string, o cephOutcome) error {
	return fmt.Errorf("another fault: %s failed with %s, which is neither the expected answer nor %s", stage, o, missingErrno)
}

// judgeRemoval decides whether the failed removal left the subvolume
// inconsistent or unrecoverable. The contract is that a removal with
// retained snapshots that fails to persist its descriptor changes nothing:
// the metadata still names the live incarnation as complete, the
// incarnation and the snapshot keep their bytes and paths, and a retry
// retains the snapshot. It returns the symptoms of the defect, none for the
// contract, and an error starting with "another fault:" for a state that is
// neither, including a command or read that fails with another errno than
// the missing path that the stage expects.
func judgeRemoval(r removalReport) ([]string, error) {
	if err := r.Trigger.verify(); err != nil {
		return nil, err
	}
	enoent := int(syscall.ENOENT)
	if r.Meta.Errno != 0 {
		return nil, fmt.Errorf("another fault: the persisted metadata is unreadable after the thresholds were restored: errno %d", r.Meta.Errno)
	}
	global := r.Meta.Sections["GLOBAL"]
	if global["state"] != "complete" || global["path"] != r.LivePath {
		return nil, fmt.Errorf("another fault: the failed removal persisted state %q and path %q, want complete and %s", global["state"], global["path"], r.LivePath)
	}
	var symptoms []string
	switch {
	case r.Live.Errno == enoent:
		symptoms = append(symptoms, fmt.Sprintf("the persisted metadata names %s as the complete live incarnation, but it is gone", r.LivePath))
	case r.Live.Errno != 0:
		return nil, fmt.Errorf("another fault: the live file is unreadable: errno %d", r.Live.Errno)
	case r.Live.SHA256 != r.LiveSHA:
		return nil, fmt.Errorf("another fault: the live file holds %s, want %s", r.Live.SHA256, r.LiveSHA)
	}
	switch path := strings.TrimSpace(r.GetPath.Out); {
	case !r.GetPath.OK && r.GetPath.Errno != missingErrno:
		return nil, unrelated("getpath", r.GetPath)
	case !r.GetPath.OK:
		symptoms = append(symptoms, "getpath failed: "+r.GetPath.String())
	case path != r.LivePath:
		return nil, fmt.Errorf("another fault: getpath returned %q, want %s", path, r.LivePath)
	}
	switch {
	case r.Snapshot.Errno == enoent:
		symptoms = append(symptoms, "the snapshot file is gone: errno 2")
	case r.Snapshot.Errno != 0:
		return nil, fmt.Errorf("another fault: the snapshot file is unreadable: errno %d", r.Snapshot.Errno)
	case r.Snapshot.SHA256 != r.SnapshotSHA:
		symptoms = append(symptoms, "the snapshot file changed: sha256 "+r.Snapshot.SHA256)
	}
	switch path := strings.TrimSpace(r.SnapshotGetPath.Out); {
	case !r.SnapshotGetPath.OK && r.SnapshotGetPath.Errno != missingErrno:
		return nil, unrelated("snapshot getpath", r.SnapshotGetPath)
	case !r.SnapshotGetPath.OK:
		symptoms = append(symptoms, "snapshot getpath failed: "+r.SnapshotGetPath.String())
	case path != r.SnapshotPath:
		return nil, fmt.Errorf("another fault: snapshot getpath returned %q, want %s", path, r.SnapshotPath)
	}
	switch {
	case !r.Retry.OK && r.Retry.Errno != missingErrno:
		return nil, unrelated("the retried removal", r.Retry)
	case !r.Retry.OK:
		return append(symptoms, "the retried removal failed: "+r.Retry.String()), nil
	}
	switch {
	case !r.RetryInfo.OK && r.RetryInfo.Errno != missingErrno:
		return nil, unrelated("info after the retry", r.RetryInfo)
	case !r.RetryInfo.OK:
		symptoms = append(symptoms, "after the retry info failed: "+r.RetryInfo.String())
	case r.RetryState != "snapshot-retained":
		symptoms = append(symptoms, fmt.Sprintf("after the retry the subvolume is not snapshot-retained: state %q", r.RetryState))
	}
	switch {
	case r.RetryGetPath.OK:
		symptoms = append(symptoms, "after the retry getpath still returns a live incarnation")
	case r.RetryGetPath.Errno != missingErrno:
		return nil, unrelated("getpath after the retry", r.RetryGetPath)
	}
	switch path := strings.TrimSpace(r.RetrySnapshotGetPath.Out); {
	case !r.RetrySnapshotGetPath.OK && r.RetrySnapshotGetPath.Errno != missingErrno:
		return nil, unrelated("snapshot getpath after the retry", r.RetrySnapshotGetPath)
	case !r.RetrySnapshotGetPath.OK:
		symptoms = append(symptoms, "after the retry snapshot getpath failed: "+r.RetrySnapshotGetPath.String())
	case path != r.SnapshotPath:
		symptoms = append(symptoms, fmt.Sprintf("after the retry snapshot getpath returned %q, want %s", path, r.SnapshotPath))
	case r.RetrySnapshot.Errno == enoent:
		symptoms = append(symptoms, "after the retry the snapshot file is gone: errno 2")
	case r.RetrySnapshot.Errno != 0:
		return nil, fmt.Errorf("another fault: after the retry the snapshot file is unreadable: errno %d", r.RetrySnapshot.Errno)
	case r.RetrySnapshot.SHA256 != r.SnapshotSHA:
		symptoms = append(symptoms, "after the retry the snapshot file changed: sha256 "+r.RetrySnapshot.SHA256)
	}
	return symptoms, nil
}
