package ceph

import (
	"context"
	"sync"

	"github.com/jsyoo5b/ceph-testcontainers-go/internal/clusterutil"
)

// Caller owns Unlock after success. No goroutine remains waiting after cancel.
func lockTopologyMutex(ctx context.Context, mutex *sync.Mutex) error {
	return clusterutil.LockMutex(ctx, mutex)
}

// Caller owns RUnlock after success. Writers may hold this gate across cleanup.
func lockTopologyReadMutex(ctx context.Context, mutex *sync.RWMutex) error {
	return clusterutil.RLockMutex(ctx, mutex)
}
