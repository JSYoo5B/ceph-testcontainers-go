// Package fault injects dedicated faults into an existing Ceph fixture.
// Cluster-bound operations that return a handle use ceph.Run's lifetime and are
// restored by its Terminate, in this order: network shaping first, endpoint restoration
// second, then paused containers are resumed, then clock offsets and CPU limits
// are restored last. Standalone link operations
// (InterruptLink, ApplyLinkProfile) are restored by their caller before it
// removes their container or bridge. Keep a non-nil handle returned with an
// error: its restore stays retryable after an uncertain result.
//
// The error categories are those of package ceph; the sentinels of this
// package match them with errors.Is.
//
// A nil cluster is refused by every cluster-bound operation before it takes
// any lock, with an error matching ceph.ErrInvalidRequest. A refusal returns a
// nil handle, so every handle type follows one nil-receiver rule: its
// restoring method (Restore, PauseLease.Resume) returns nil, since there is
// nothing to restore, and its observation methods (Status) return an error
// matching ceph.ErrInvalidRequest. A deferred restore of a refused operation
// is therefore safe.
//
// Policy and admission controls that only change how Ceph admits work, such
// as full ratios, OSD flags and the session blocklist, live in package
// override even when a test uses them to provoke failures. Object read-error
// injection is here because it is a purpose-built synthetic fault, although
// Ceph implements it as a daemon debug switch; scrub, repair and inspection
// of the damaged PG stay in package ceph.
package fault

import (
	"context"
	"sync"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/nshelper"
)

func lockMutex(ctx context.Context, mu *sync.Mutex) error { return clusterutil.LockMutex(ctx, mu) }

// requireCluster refuses a nil cluster for the operation op, before any
// lock: the cluster's mutex cannot be taken on a nil cluster.
func requireCluster(c *ceph.Container, op string) error {
	if c == nil {
		return nshelper.InvalidRequest("%s requires a cluster", op)
	}
	return nil
}

// unavailable is the refusal of an observation method called on a nil
// handle of kind.
func unavailable(kind string) error {
	return nshelper.InvalidRequest("the %s is unavailable: the handle is nil", kind)
}
