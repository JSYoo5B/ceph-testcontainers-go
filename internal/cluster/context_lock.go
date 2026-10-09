package cluster

import (
	"context"
	"sync"
	"time"
)

// Caller owns Unlock after success. No goroutine remains waiting after cancel.
func lockTopologyMutex(ctx context.Context, mutex *sync.Mutex) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if mutex.TryLock() {
			if err := ctx.Err(); err != nil {
				mutex.Unlock()
				return err
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// Caller owns RUnlock after success. Writers may hold this gate across cleanup.
func lockTopologyReadMutex(ctx context.Context, mutex *sync.RWMutex) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if mutex.TryRLock() {
			if err := ctx.Err(); err != nil {
				mutex.RUnlock()
				return err
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}
