// Package clusterutil holds small helpers that the cluster implementation and
// the service packages share: context-bounded locking, polling, operation
// errors and keyring parsing. It depends on neither of them.
package clusterutil

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// LockMutex acquires mutex unless ctx ends first. The caller owns Unlock after
// success. No goroutine remains waiting after cancellation.
func LockMutex(ctx context.Context, mutex *sync.Mutex) error {
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
