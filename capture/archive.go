package capture

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

var _ ceph.CaptureFileSource = (*Session)(nil)

// CaptureFile opens the settled session's original, caller-owned output for
// read-only archive copying. The size and SHA256 identify all retrieved bytes,
// not merely the structural validator's prefix. It does not stop the session
// or transfer ownership of its file. Close the returned reader.
//
// Finalization.CaptureWithFile is the explicit consent to archive these raw,
// unsanitized packets; ordinary Finalization.Capture never opens this source.
// The archive copies only for a failure: it verifies EOF, the exact size and
// the full SHA256 before it publishes the copy as
// capture-<registration>-<name>.pcap, refuses a symlink or other non-regular
// source, and fails verification when the file changed after retrieval.
// Keep the file and its directory until the finalization returns: a
// t.TempDir registered after TerminateOnCleanup is removed first, so create
// it before registering the finalization or use t.ArtifactDir.
func (s *Session) CaptureFile(ctx context.Context) (io.ReadCloser, int64, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, "", err
	}
	if s == nil {
		return nil, 0, "", errors.New("capture file unavailable")
	}
	if !s.mu.TryLock() {
		return nil, 0, "", errors.New("capture source is busy")
	}
	defer s.mu.Unlock()
	if !s.finished || !s.report.Retrieved || s.report.RetrievedSHA256 == "" {
		return nil, 0, "", errors.New("capture file has no settled retrieval identity")
	}
	// Lstat plus SameFile refuses symlinks and replacements between inspection
	// and open. The archive independently checks size and digest on this handle.
	before, err := os.Lstat(s.request.Output)
	if err != nil || !before.Mode().IsRegular() {
		return nil, 0, "", errors.New("capture source is not an accessible regular file")
	}
	file, err := os.Open(s.request.Output)
	if err != nil {
		return nil, 0, "", errors.New("capture source cannot be opened")
	}
	after, err := file.Stat()
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) || after.Size() != s.report.RetrievedBytes {
		_ = file.Close()
		return nil, 0, "", errors.New("capture source identity changed")
	}
	return file, s.report.RetrievedBytes, s.report.RetrievedSHA256, nil
}
