//go:build all || integration

package cephfs_mirror_blockdiff_empty_versions_test

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// blockdiffFiles are the files of the mirrored directory: f, the trigger
// file, and g, a file that always has data.
var blockdiffFiles = []string{"f", "g"}

// blockdiffFile is the size and content of one file in a snapshot.
type blockdiffFile struct {
	Inode  uint64
	Size   uint64
	SHA256 string
}

// blockdiffSnapshot is a snapshot of the mirrored directory as a client of
// one filesystem reads it. ID is that filesystem's own snapshot ID. A
// snapshot that is not Present has no ID and no files.
type blockdiffSnapshot struct {
	Present bool
	ID      uint64
	Name    string
	Files   map[string]blockdiffFile
}

var blockdiffDigest = regexp.MustCompile(`\A[0-9a-f]{64}\z`)

// parseBlockdiffSnapshot reads one report of the snapshot script. The
// report must say whether the snapshot is present and name it. A present
// snapshot must have a nonzero ID and exactly the files of blockdiffFiles,
// each with a nonzero inode number, a size and a SHA-256. A size of 0 is an
// observation; a missing or null size is not.
func parseBlockdiffSnapshot(report []byte) (blockdiffSnapshot, error) {
	var raw struct {
		Present *bool   `json:"present"`
		ID      *uint64 `json:"id"`
		Name    *string `json:"name"`
		Files   map[string]struct {
			Inode  *uint64 `json:"ino"`
			Size   *uint64 `json:"size"`
			SHA256 *string `json:"sha256"`
		} `json:"files"`
	}
	fail := func(format string, args ...any) (blockdiffSnapshot, error) {
		return blockdiffSnapshot{}, fmt.Errorf("snapshot report %s: %s", report, fmt.Sprintf(format, args...))
	}
	if err := json.Unmarshal(report, &raw); err != nil {
		return fail("%v", err)
	}
	if raw.Present == nil || raw.Name == nil || *raw.Name == "" {
		return fail("presence or name is missing")
	}
	snapshot := blockdiffSnapshot{Present: *raw.Present, Name: *raw.Name}
	if !snapshot.Present {
		if raw.ID != nil || raw.Files != nil {
			return fail("an absent snapshot has an ID or files")
		}
		return snapshot, nil
	}
	if raw.ID == nil || *raw.ID == 0 || len(raw.Files) != len(blockdiffFiles) {
		return fail("a present snapshot needs a nonzero ID and %d files", len(blockdiffFiles))
	}
	snapshot.ID, snapshot.Files = *raw.ID, map[string]blockdiffFile{}
	for _, name := range blockdiffFiles {
		file, ok := raw.Files[name]
		if !ok || file.Inode == nil || *file.Inode == 0 || file.Size == nil || file.SHA256 == nil || !blockdiffDigest.MatchString(*file.SHA256) {
			return fail("file %s is missing or lacks a nonzero inode number, a size or a SHA-256", name)
		}
		snapshot.Files[name] = blockdiffFile{Inode: *file.Inode, Size: *file.Size, SHA256: *file.SHA256}
	}
	return snapshot, nil
}

// sameContent reports whether other has the files of snapshot with the same
// sizes and content. Both must be present snapshots with every file of
// blockdiffFiles. Inode numbers and snapshot IDs belong to each filesystem
// and are not compared.
func (snapshot blockdiffSnapshot) sameContent(other blockdiffSnapshot) bool {
	if !snapshot.Present || !other.Present || len(snapshot.Files) != len(blockdiffFiles) || len(other.Files) != len(blockdiffFiles) {
		return false
	}
	for _, name := range blockdiffFiles {
		want, ok := snapshot.Files[name]
		got, found := other.Files[name]
		if !ok || !found || got.Size != want.Size || got.SHA256 != want.SHA256 || !blockdiffDigest.MatchString(want.SHA256) {
			return false
		}
	}
	return true
}

// versionSizes reads the JSON of "rados listsnaps" for one object and
// returns the size of the clone that holds the snapshot previous and the
// size of the head. Every version must have an ID, the text "head" or a
// number, and a size, and every snapshot of a clone an ID; a size of 0 is
// an observation, a missing or null one is not. It refuses a listing in
// which previous, which must not be 0, is in no clone or in two, the object
// has no head, or a clone holds a snapshot newer than previous: the head is
// then not the version that follows the clone of previous.
func versionSizes(listing []byte, previous uint64) (clone, head uint64, err error) {
	var object struct {
		Clones *[]struct {
			ID        json.RawMessage `json:"id"`
			Snapshots *[]struct {
				ID *uint64 `json:"id"`
			} `json:"snapshots"`
			Size *uint64 `json:"size"`
		} `json:"clones"`
	}
	fail := func(format string, args ...any) (uint64, uint64, error) {
		return 0, 0, fmt.Errorf("object versions %s: %s", listing, fmt.Sprintf(format, args...))
	}
	if previous == 0 {
		return fail("no previous snapshot ID to look for")
	}
	if err := json.Unmarshal(listing, &object); err != nil {
		return fail("%v", err)
	}
	if object.Clones == nil {
		return fail("no list of versions")
	}
	var hasClone, hasHead bool
	for _, version := range *object.Clones {
		if version.Size == nil {
			return fail("a version has no size")
		}
		if string(version.ID) == `"head"` {
			if hasHead {
				return fail("two heads")
			}
			hasHead, head = true, *version.Size
			continue
		}
		var id uint64
		if len(version.ID) == 0 || string(version.ID) == "null" || json.Unmarshal(version.ID, &id) != nil {
			return fail("a version has the ID %q, neither \"head\" nor a number", version.ID)
		}
		if version.Snapshots == nil {
			return fail("clone %d has no list of snapshots", id)
		}
		for _, snapshot := range *version.Snapshots {
			switch {
			case snapshot.ID == nil:
				return fail("a snapshot of clone %d has no ID", id)
			case *snapshot.ID > previous:
				return fail("a clone holds snapshot %d, newer than %d", *snapshot.ID, previous)
			case *snapshot.ID == previous && hasClone:
				return fail("two clones hold snapshot %d", previous)
			case *snapshot.ID == previous:
				hasClone, clone = true, *version.Size
			}
		}
	}
	if !hasClone || !hasHead {
		return fail("clone of snapshot %d found: %t, head found: %t", previous, hasClone, hasHead)
	}
	return clone, head, nil
}

// blockdiffRequests reads the JSON of an MDS "perf dump mds_server" and
// returns how many blockdiff requests the daemon served: the sample count
// of its blockdiff latency counter. The count must be in the dump as a
// number. A count of 0 is an observation; a dump without the counter, or
// with a missing or null count, is not.
func blockdiffRequests(dump []byte) (uint64, error) {
	var raw struct {
		Server *struct {
			Blockdiff *struct {
				Count *uint64 `json:"avgcount"`
			} `json:"req_blockdiff_latency"`
		} `json:"mds_server"`
	}
	if err := json.Unmarshal(dump, &raw); err != nil {
		return 0, fmt.Errorf("MDS counters %s: %w", dump, err)
	}
	if raw.Server == nil || raw.Server.Blockdiff == nil || raw.Server.Blockdiff.Count == nil {
		return 0, fmt.Errorf("MDS counters %s: no mds_server.req_blockdiff_latency.avgcount", dump)
	}
	return *raw.Server.Blockdiff.Count, nil
}

// mdsProcess is one observation of the source MDS: the state of its
// container and, while the container runs, the GID of the filesystem's
// active daemon in the FSMap.
type mdsProcess struct {
	Running   bool
	Status    string
	ExitCode  int
	StartedAt string
	GID       uint64
}

func (process mdsProcess) String() string {
	text := fmt.Sprintf("container status %s, exit code %d, started %s", process.Status, process.ExitCode, process.StartedAt)
	if process.Running {
		text += fmt.Sprintf(", GID %d", process.GID)
	}
	return text
}

// live reports whether process is a running daemon with a start time and a
// GID.
func (process mdsProcess) live() bool {
	return process.Running && process.StartedAt != "" && process.GID != 0
}

// The two parts of the source MDS log that identify the documented abort:
// the assertion that an interval has a length, and, in the backtrace after
// it, the function that aggregates the object versions of a blockdiff
// request.
const (
	blockdiffAssertion = "FAILED ceph_assert(len > 0)"
	blockdiffFrame     = "MDCache::aggregate_snap_sets("
)

// blockdiffOutcome is what the trigger step left.
type blockdiffOutcome struct {
	// Before is the source MDS process before the trigger. After is the
	// process after it, when AfterRead; otherwise the observation failed
	// with AfterError.
	Before, After mdsProcess
	AfterRead     bool
	AfterError    string
	// Log holds, in their order, the distinct lines of the source MDS
	// container's log that contain blockdiffAssertion or blockdiffFrame,
	// when LogRead: the log was read to its end. LogError says why not.
	Log      []string
	LogRead  bool
	LogError string
	// RequestsBefore is the blockdiff request counter of the MDS before the
	// trigger. RequestsAfter is the counter after it, when RequestsRead;
	// otherwise the read failed with RequestsError.
	RequestsBefore, RequestsAfter uint64
	RequestsRead                  bool
	RequestsError                 string
	// Synced reports that the mirror named the trigger snapshot as last
	// synchronized, and Status the last mirror status of the directory.
	Synced bool
	Status string
	// Source is the trigger snapshot on the source. Destination is the
	// snapshot of that name on the destination, when DestinationRead.
	Source, Destination blockdiffSnapshot
	DestinationRead     bool
	DestinationError    string
}

// judgeBlockdiff decides what the incremental synchronization of the
// trigger snapshot did to the source MDS.
//
// It returns the symptoms of the defect only when all of this was observed:
// the container that ran the MDS before the trigger was read as stopped,
// never restarted, with a nonzero exit code, its log was read to the end,
// and the log has the interval assertion followed by the function that
// aggregates object versions. It returns neither symptoms nor an error only
// when all of this was observed: the container runs the same process with
// the same GID, the request counter was read again and is higher, the
// mirror reported the snapshot as synchronized, and the destination's copy
// was read and has the source's sizes and content.
//
// Everything else is an error. An observation that failed (the container's
// state, the log, the counter, the destination) decides nothing. A live MDS
// that served no further blockdiff request was not asked the question. An
// MDS that ended without the assertion, or was replaced, ended for another
// reason. A live MDS with a missing or different snapshot is another fault.
func judgeBlockdiff(got blockdiffOutcome) ([]string, error) {
	if !got.Before.live() {
		return nil, fmt.Errorf("trigger not established: the source MDS before the trigger was not a live process: %s", got.Before)
	}
	if !got.AfterRead {
		return nil, fmt.Errorf("outcome not observed: the source MDS process after the trigger could not be read: %s", got.AfterError)
	}
	if !got.After.Running {
		if got.After.StartedAt != got.Before.StartedAt || got.After.ExitCode == 0 {
			return nil, fmt.Errorf("another fault: the source MDS container stopped, but not as the process of the trigger ending with a failure: %s, then %s", got.Before, got.After)
		}
		if !got.LogRead {
			return nil, fmt.Errorf("outcome not observed: the source MDS container stopped (%s) and its log could not be read: %s", got.After, got.LogError)
		}
		asserted, framed := -1, -1
		for i, line := range got.Log {
			switch {
			case asserted < 0 && strings.Contains(line, blockdiffAssertion):
				asserted = i
			case asserted >= 0 && framed < 0 && strings.Contains(line, blockdiffFrame):
				framed = i
			}
		}
		if framed < 0 {
			return nil, fmt.Errorf("another fault: the source MDS container stopped (%s) and its log does not have the assertion %q followed by the function %s; matching log lines %q", got.After, blockdiffAssertion, blockdiffFrame, got.Log)
		}
		return []string{
			"the source MDS process that served the blockdiff request ended: " + got.After.String(),
			"MDS log: " + strings.TrimSpace(got.Log[asserted]),
			"MDS log: " + strings.TrimSpace(got.Log[framed]),
			fmt.Sprintf("the mirror did not finish the snapshot (synced: %t, destination read: %t, present there: %t): %s", got.Synced, got.DestinationRead, got.Destination.Present, got.Status),
		}, nil
	}
	if !got.After.live() || got.After.StartedAt != got.Before.StartedAt || got.After.GID != got.Before.GID {
		return nil, fmt.Errorf("another fault: the source MDS is not the process of the trigger any more: %s, then %s", got.Before, got.After)
	}
	if !got.RequestsRead {
		return nil, fmt.Errorf("outcome not observed: the blockdiff request counter of the live source MDS could not be read: %s", got.RequestsError)
	}
	if got.RequestsAfter <= got.RequestsBefore {
		return nil, fmt.Errorf("trigger not established: the source MDS served no blockdiff request for the trigger file (%d before, %d after); mirror status %s", got.RequestsBefore, got.RequestsAfter, got.Status)
	}
	if !got.Synced {
		return nil, fmt.Errorf("another fault: the source MDS is live and served the blockdiff request, and the mirror did not report the snapshot as synchronized: %s", got.Status)
	}
	if !got.DestinationRead {
		return nil, fmt.Errorf("outcome not observed: the snapshot on the destination could not be read: %s", got.DestinationError)
	}
	if !got.Source.sameContent(got.Destination) {
		return nil, fmt.Errorf("another fault: the source MDS is live and the snapshot was synchronized, and the destination has %+v, want the sizes and content of %+v", got.Destination, got.Source)
	}
	return nil, nil
}
