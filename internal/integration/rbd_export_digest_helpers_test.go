//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"net/netip"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
)

// rbdExportDigest exports an image into a private temporary directory,
// stops with the export's own status when it fails, and only then hashes the
// file. It removes the directory on every path; a failed removal fails an
// otherwise successful run and is reported on standard error, while an
// export or hash failure keeps its own status. Arguments: the image, then
// the rbd command with its options.
const rbdExportDigest = `dir=$(mktemp -d "${TMPDIR:-/tmp}/rbd-export.XXXXXXXX") || exit 125
trap 'rm -rf "$dir"; exit 143' HUP INT TERM
image=$1
shift
if "$@" export --no-progress "$image" "$dir/image" >&2; then
    md5sum < "$dir/image"
    status=$?
    test "$status" -eq 0 || echo "md5sum exited $status" >&2
else
    status=$?
    echo "rbd export exited $status" >&2
fi
trap - HUP INT TERM
rm -rf "$dir" || {
    removal=$?
    echo "remove the temporary export directory exited $removal" >&2
    test "$status" -ne 0 || status=$removal
}
exit "$status"`

// checkedRBDExportDigest returns the MD5 digest of image as rbd (with its
// options) exports it in client. It fails when the export, the hash or the
// removal of the exported file exits nonzero or the digest is malformed;
// standard error stays apart from the digest and is shown only on failure.
func checkedRBDExportDigest(ctx context.Context, client testcontainers.Container, image string, rbdArgs ...string) (string, error) {
	execCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	code, reader, err := client.Exec(execCtx, append([]string{"sh", "-c", rbdExportDigest, "sh", image}, rbdArgs...))
	if err != nil {
		return "", fmt.Errorf("export %s: %w", image, err)
	}
	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, reader); err != nil {
		return "", fmt.Errorf("export %s: read output: %w", image, err)
	}
	return rbdExportResult(image, code, stdout.Bytes(), stderr.Bytes())
}

// rbdExportResult checks one run of rbdExportDigest: its exit code, then the
// digest on standard output.
func rbdExportResult(image string, code int, stdout, stderr []byte) (string, error) {
	diagnostics := stderr[:min(len(stderr), 2048)]
	if code != 0 {
		return "", fmt.Errorf("export %s: exit %d: %s", image, code, diagnostics)
	}
	fields := strings.Fields(string(stdout))
	if len(fields) == 0 || len(fields[0]) != 32 || strings.Trim(fields[0], "0123456789abcdef") != "" {
		return "", fmt.Errorf("export %s: md5sum printed %q: %s", image, stdout[:min(len(stdout), 256)], diagnostics)
	}
	return fields[0], nil
}

// immutableCacheParentReplies returns the parent data objects of objects
// whose reads the OSD answered successfully to client within window, a part
// of its debug_ms 1 log.
func immutableCacheParentReplies(window []byte, client netip.Addr, objects map[string]bool) (map[string]bool, error) {
	replied := map[string]bool{}
	for object := range objects {
		replies, err := harness.RBDDataReadReplies(window, object, client)
		if err != nil {
			return nil, err
		}
		if len(replies) > 0 {
			replied[object] = true
		}
	}
	return replied, nil
}

// immutableCacheReadCount requires the OSD to have answered reads of exactly
// want distinct parent data objects.
func immutableCacheReadCount(replied map[string]bool, want int) error {
	if len(replied) != want {
		return fmt.Errorf("the OSD answered reads of %d distinct parent data objects, want %d: %v", len(replied), want, replied)
	}
	return nil
}
