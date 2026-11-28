package ceph

import (
	"fmt"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
)

// maxDaemonNameBytes is the longest MON, MGR or MDS name whose admin socket
// path, adminSocketDir/ceph-<kind>.<name>.asok, fits the 107 bytes that Ceph
// allows for a UNIX socket path. A longer name never binds its admin socket,
// so the daemon never becomes ready.
const maxDaemonNameBytes = 107 - len(adminSocketDir+"/ceph-mon.") - len(".asok")

// validateDaemonName is the one name rule of MON, MGR and MDS names: creation
// and every reader of a daemon by name use it, so a reader accepts every name
// creation accepts. role names the daemon in the refusal, which matches
// ErrInvalidRequest.
func validateDaemonName(role, name string) error {
	if !naming.DaemonName.MatchString(name) {
		return invalid(fmt.Errorf("invalid %s name %q: it must start with a letter or digit and contain only letters, digits, '_', '.' and '-'", role, name))
	}
	if len(name) > maxDaemonNameBytes {
		return invalid(fmt.Errorf("invalid %s name %q: it is %d bytes, longer than the %d bytes its admin socket path allows", role, name, len(name), maxDaemonNameBytes))
	}
	return nil
}
