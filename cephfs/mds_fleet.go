package cephfs

import (
	"context"
	"errors"
	"fmt"
	iofs "io/fs"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/testcontainers/testcontainers-go"
)

// mdsFleet runs the MDS lifecycle of one filesystem: the daemon starts with
// their admission, publication and credentials, scaling with its phases, MDS
// status and readiness, the cold start of a filesystem created without an
// initial MDS, the replacement of a stopped daemon, the removal of a stopped
// daemon and the teardown of every daemon when the filesystem is removed.
//
// It owns the filesystem's MDS state (mds: the roster and the cold-start
// record) and the first-MDS slot (first: the Filesystem's embedded container,
// which the cold start, retirement, stopped removal and teardown re-seat). It
// acts for the filesystem through its identity (id) and reads the descriptor
// fields a caller may edit (descriptor) only to refuse an edited descriptor. It
// holds no pointer to the Filesystem itself: Filesystem.fleet is its only
// constructor, binding each of these once, and an MDS creation record keeps a
// copy of the fleet that created it.
//
// The fleet adds no lock. Its operations take setupMu and the cluster mutex
// themselves, in the package order (setupMu first), at the same points as when
// they were Filesystem methods; the roster's two-key contract, written on
// mdsRoster, still decides which of them may read or write the MDS state.
type mdsFleet struct {
	id         *filesystemIdentity
	mds        *mdsState
	first      *testcontainers.Container
	descriptor filesystemDescriptor
}

// filesystemDescriptor points at the exported descriptor fields of a
// Filesystem. Callers may edit them; the MDS ownership proofs refuse a
// descriptor that no longer matches the identity.
type filesystemDescriptor struct {
	name, metadataPool, dataPool *string
}

// fleet returns the MDS fleet of fs. fs is not nil.
func (fs *Filesystem) fleet() mdsFleet {
	return mdsFleet{
		id: &fs.id, mds: &fs.mds, first: &fs.Container,
		descriptor: filesystemDescriptor{name: &fs.FilesystemName, metadataPool: &fs.MetadataPool, dataPool: &fs.DataPool},
	}
}

// removeAll terminates every owned daemon of a filesystem being removed,
// deletes its service registration and key, drops it from the roster and
// clears the first-MDS slot. Owner held: removeFilesystem holds setupMu and the
// cluster mutex and has already removed the native filesystem.
func (f mdsFleet) removeAll(ctx context.Context) error {
	c, name := f.id.cluster, f.id.name
	for _, daemon := range f.mds.roster.snapshot() {
		if daemon.Container != nil {
			if err := daemon.Terminate(ctx); err != nil && !servicehook.OnlyMissingHostResource(err) {
				return fmt.Errorf("terminate mds.%s of removed cephfs %q: %w", daemon.ID, name, err)
			}
			service := "mds." + daemon.ID
			if owned := servicehook.Service(c, service); owned != nil && owned.GetContainerID() == daemon.GetContainerID() {
				servicehook.DeleteService(c, service)
			}
		}
		if _, err := c.Ceph(ctx, "auth", "del", "mds."+daemon.ID); err != nil && !errors.Is(err, iofs.ErrNotExist) && !strings.Contains(err.Error(), "does not exist") {
			return fmt.Errorf("remove key of mds.%s: %w", daemon.ID, err)
		}
		f.mds.roster.remove(daemon)
	}
	*f.first = nil
	return nil
}

// addConfigTargets adds the containers of the owned daemons that read the
// cluster's ceph.conf to targets. Owner held: ceph.Container calls
// ConfigTargets with its mutex held.
func (f mdsFleet) addConfigTargets(targets map[string]testcontainers.Container) {
	for _, mds := range f.mds.roster.members() {
		if mds != nil && mds.Container != nil {
			targets["mds."+mds.ID] = mds.Container
		}
	}
}
