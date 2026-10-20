// Package fault injects dedicated faults into an existing Ceph fixture.
// Cluster-bound operations use ceph.Run's lifetime and are restored by its
// Terminate. Standalone link operations are restored by their caller.
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
