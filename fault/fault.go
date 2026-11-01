// Package fault injects dedicated faults into an existing Ceph fixture.
// Cluster-bound operations that return a handle use ceph.Run's lifetime and are
// restored by its Terminate, in this order: network shaping first, endpoint restoration
// second, then paused containers are resumed. Standalone link operations
// (InterruptLink, ApplyLinkProfile) are restored by their caller before it
// removes their container or bridge. Keep a non-nil handle returned with an
// error: its restore stays retryable after an uncertain result.
//
// The error categories are those of package ceph; the sentinels of this
// package match them with errors.Is.
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
	"fmt"
	"sync"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
)

type categoryError struct {
	text string
	kind error
}

func (e *categoryError) Error() string        { return e.text }
func (e *categoryError) Is(target error) bool { return target == e.kind }
func invalidRequest(format string, args ...any) error {
	return &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Err: fmt.Errorf(format, args...)}
}
func lockMutex(ctx context.Context, mu *sync.Mutex) error { return clusterutil.LockMutex(ctx, mu) }
