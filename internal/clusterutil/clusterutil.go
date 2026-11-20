// Package clusterutil holds small helpers that the cluster implementation and
// the service packages share: context-bounded locking, polling, operation
// errors and keyring parsing. It depends on neither of them.
package clusterutil

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// OwnerLabel is the label whose value is the owner ID of the cluster that
// created a container. Terminate removes the containers that carry it, also
// those whose creation reply was lost.
const OwnerLabel = "org.testcontainers.ceph.cluster"

// LockMutex acquires mutex unless ctx ends first. The caller owns Unlock after
// success. No goroutine remains waiting after cancellation.
//
// Callers that find the mutex busy are served in arrival order: only the
// longest-waiting caller tries the mutex, so a caller that arrives later
// cannot take it first, however long each holder keeps it. A caller that
// gives up leaves the queue without delaying those behind it. A plain Lock
// or TryLock on the same mutex is outside this order.
func LockMutex(ctx context.Context, mutex *sync.Mutex) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w, locked := lockQueues.enter(mutex)
	if !locked {
		if err := w.acquire(ctx, mutex); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		mutex.Unlock()
		return err
	}
	return nil
}

// lockQueues holds the callers of LockMutex that wait for a busy mutex, in
// arrival order. A mutex has an entry only while a caller waits for it.
var lockQueues lockQueueSet

type lockQueueSet struct {
	mu      sync.Mutex
	waiting map[*sync.Mutex][]*lockWaiter
}

// lockWaiter is one queued caller. turn is closed when it reaches the head.
type lockWaiter struct {
	turn chan struct{}
}

// enter takes a free mutex that nobody waits for, or queues the caller.
func (s *lockQueueSet) enter(mutex *sync.Mutex) (*lockWaiter, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	queue := s.waiting[mutex]
	if len(queue) == 0 && mutex.TryLock() {
		return nil, true
	}
	w := &lockWaiter{turn: make(chan struct{})}
	if len(queue) == 0 {
		close(w.turn)
	}
	if s.waiting == nil {
		s.waiting = make(map[*sync.Mutex][]*lockWaiter)
	}
	s.waiting[mutex] = append(queue, w)
	return w, false
}

// leave removes w, which took the mutex or gave up, and passes the head on.
func (s *lockQueueSet) leave(mutex *sync.Mutex, w *lockWaiter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	queue := s.waiting[mutex]
	index := slices.Index(queue, w)
	queue = slices.Delete(queue, index, index+1)
	if len(queue) == 0 {
		delete(s.waiting, mutex)
		return
	}
	if index == 0 {
		close(queue[0].turn)
	}
	s.waiting[mutex] = queue
}

// acquire waits for w's turn and then polls the mutex until it is free.
func (w *lockWaiter) acquire(ctx context.Context, mutex *sync.Mutex) error {
	defer lockQueues.leave(mutex, w)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.turn:
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if mutex.TryLock() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// RLockMutex acquires mutex for reading unless ctx ends first. The caller owns
// RUnlock after success. Writers may hold this gate across cleanup.
func RLockMutex(ctx context.Context, mutex *sync.RWMutex) error {
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

// Poll calls check every half second until it reports success without an
// error or ctx ends. The last error joins the context's error.
func Poll(ctx context.Context, check func() (bool, error)) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var last error
	for {
		if err := ctx.Err(); err != nil {
			return errors.Join(err, last)
		}
		ok, err := check()
		if err != nil {
			last = err
		}
		if err := ctx.Err(); err != nil {
			return errors.Join(err, last)
		}
		if ok && err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), last)
		case <-ticker.C:
		}
	}
}

// OperationError names a failed native operation without its output, which
// can contain secrets. It keeps the caller's context error and a native
// cancellation or deadline as causes, and the classification of native
// without its output.
func OperationError(ctx context.Context, operation string, native error) error {
	causes := []error{errors.New(operation)}
	contextErr := ctx.Err()
	if contextErr != nil {
		causes = append(causes, contextErr)
	}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		if errors.Is(native, cause) && !errors.Is(contextErr, cause) {
			causes = append(causes, cause)
		}
	}
	return Quiet(errors.Join(causes...), native)
}

// Quiet returns visible, keeping the classification of native without its
// output; package ceph sets it.
var Quiet = func(visible, native error) error { return visible }

// KeyringKey returns the key of entity in a Ceph keyring, or "".
func KeyringKey(keyring []byte, entity string) string {
	active := false
	for _, line := range strings.Split(string(keyring), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "[") {
			active = line == "["+entity+"]"
			continue
		}
		if active {
			key, value, ok := strings.Cut(line, "=")
			if ok && strings.TrimSpace(key) == "key" {
				return strings.TrimSpace(value)
			}
		}
	}
	return ""
}

// TextFile returns data as a file to copy into a container.
func TextFile(path string, data []byte, mode int64) testcontainers.ContainerFile {
	return testcontainers.ContainerFile{Reader: bytes.NewReader(data), ContainerFilePath: path, FileMode: mode}
}
