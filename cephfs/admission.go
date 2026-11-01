package cephfs

import (
	"context"
	"errors"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// admissionSpec is what one begin*Operation flow keeps for itself when it
// shares the admission lifetime: whether it rechecks cancellation after the
// setup gate, its own ownership predicate and its own refusal text.
type admissionSpec struct {
	// recheckCanceled makes admitOperation test ctx.Err() after setupMu is acquired.
	// Subvolume and data-pool admission do; session admission does not.
	recheckCanceled bool
	// refusal is returned when check reports the handle invalid.
	refusal string
	// check is the operation's own predicate. It runs with setupMu and the
	// cluster mutex held, before the timeout context exists, and must not take
	// either lock. A returned error is passed through as is; valid == false is
	// reported as refusal. It may capture values read under the locks.
	check func(ctx context.Context) (valid bool, err error)
}

// admitOperation owns only the lifetime shared by the begin*Operation family: the
// canceled and nil-handle prefix, setupMu (context aware), the cluster
// topology mutex, the startup timeout, and the release. Locks are taken in the
// order of the package: setupMu, then LockTopology (cephfs/entry.go:100-102). The
// topology mutex is released before admitOperation returns; the caller holds setupMu,
// through the returned done, for the whole operation.
//
// Every operation keeps its own predicate in spec.check. admitOperation has no notion of
// a valid filesystem: the three flows differ on purpose: subvolume and session
// operations require FilesystemName to equal the recorded name, data-pool
// operations do not, and MDSStatus takes no admission. Merging them would
// change which edits of an exported field are refused.
//
// On any error the returned done is a no-op and no lock is held. On success the
// caller must call done once: it cancels the timeout and releases setupMu.
func (fs *Filesystem) admitOperation(ctx context.Context, spec admissionSpec) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return ctx, func() {}, err
	}
	if fs == nil || fs.base().Cluster() == nil {
		return ctx, func() {}, errors.New("CephFS filesystem is unavailable")
	}
	c := fs.base().Cluster()
	if err := clusterutil.LockMutex(ctx, &filesystems(c).setupMu); err != nil {
		return ctx, func() {}, err
	}
	if spec.recheckCanceled {
		if err := ctx.Err(); err != nil {
			filesystems(c).setupMu.Unlock()
			return ctx, func() {}, err
		}
	}
	if err := servicehook.LockTopology(ctx, c); err != nil {
		filesystems(c).setupMu.Unlock()
		return ctx, func() {}, err
	}
	timeout := servicehook.StartupTimeout(c)
	valid, err := spec.check(ctx)
	servicehook.Unlock(c)
	if err != nil {
		filesystems(c).setupMu.Unlock()
		return ctx, func() {}, err
	}
	if !valid {
		filesystems(c).setupMu.Unlock()
		return ctx, func() {}, errors.New(spec.refusal)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	return ctx, func() { cancel(); filesystems(c).setupMu.Unlock() }, nil
}
