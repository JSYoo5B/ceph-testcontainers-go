//go:build all || integration

package harness

import (
	"fmt"
	"strings"
)

// CephFSOriginalFSIDCheck judges one `ceph fsid` read of an original
// cluster. A command that failed, or ran out of its context, returned no
// identity, so it is reported as a failed read with its cause and never as
// a changed FSID; only an identity that was returned and differs is a
// change.
func CephFSOriginalFSIDCheck(cluster, want string, data []byte, err error) error {
	if err != nil {
		return fmt.Errorf("reading independent native %s cluster FSID: %w", cluster, err)
	}
	if got := strings.TrimSpace(string(data)); got != want {
		return fmt.Errorf("independent native original %s cluster FSID changed: got %q, want %q", cluster, got, want)
	}
	return nil
}
