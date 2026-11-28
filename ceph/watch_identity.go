package ceph

import (
	"context"
	"errors"
	"fmt"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
	"github.com/google/uuid"
)

// watchOriginalFSID returns the FSID of this fixture's original bootstrap
// configuration, which a cluster watch pins at its baseline. It waits for the
// bootstrap record no longer than ctx allows.
func (c *Container) watchOriginalFSID(ctx context.Context, operation string) (string, error) {
	identity, err := c.bootstrap.identity(ctx)
	if err != nil {
		return "", err
	}
	if identity.err != nil || !identity.confirmed {
		return "", clientOperationError(ctx, operation+": the original bootstrap identity is unavailable", identity.err)
	}
	if _, ok := canonicalFSID(identity.fsid); !ok {
		return "", errors.New(operation + ": the original bootstrap FSID is invalid")
	}
	return identity.fsid, nil
}

// checkWatchFSID compares the FSID that one read reported with the pinned
// original FSID. A malformed native FSID is a failed read; a valid one of
// another cluster is marked with endObservation, with the cause every
// cluster watch ends with on a foreign FSID.
func checkWatchFSID(operation, what, native, original string) error {
	id, ok := canonicalFSID(native)
	if !ok || native != id.String() {
		return fmt.Errorf("%s: the %s FSID is not a canonical UUID", operation, what)
	}
	if pinned, _ := canonicalFSID(original); id != pinned {
		return endObservation(fmt.Errorf("%s: the %s FSID differs from the original cluster", operation, what), invalid(errForeignFSID))
	}
	return nil
}

// watchEnd is the one end rule of the cluster watches. A read error marked
// with endObservation ends the watch with its cause, a terminated cluster
// ends it with errClusterTerminated, and any other error is a failed read
// that the next read may repair. A foreign FSID is marked with the cause
// invalid(errForeignFSID), so every watch it ends stops with a cause that
// matches ErrInvalidRequest.
func watchEnd(operation string, err error) error {
	if cause := observationEndCause(err); cause != nil {
		return watch.Fatal(fmt.Errorf("%s: %w", operation, cause))
	}
	if errors.Is(err, errClusterTerminated) {
		return watch.Fatal(fmt.Errorf("%s: %w", operation, err))
	}
	return err
}

// pinnedClusterRead is the read loop of the MON and MGR watches. It pins the
// original bootstrap FSID at the first read that gets it, runs read, checks
// every native FSID the read reports (what names them in errors) against the
// pin, and ends the watch through watchEnd.
func pinnedClusterRead[S any](operation, what string, original originalFSIDRead, read func(context.Context) (S, []string, error)) func(context.Context) (S, error) {
	var fsid string
	return func(ctx context.Context) (S, error) {
		var zero S
		pinned := fsid
		if pinned == "" {
			var err error
			if pinned, err = original(ctx); err != nil {
				return zero, err
			}
		}
		sample, natives, err := read(ctx)
		if err != nil {
			return zero, watchEnd(operation, err)
		}
		for _, native := range natives {
			if err := checkWatchFSID(operation, what, native, pinned); err != nil {
				return zero, watchEnd(operation, err)
			}
		}
		fsid = pinned
		return sample, nil
	}
}

// watchQuery runs one MON or MGR watch query without the topology lock. A
// cluster whose Terminate has started is errClusterTerminated, also when it
// made the query fail.
func watchQuery[R any](c *Container, ctx context.Context, query func(context.Context) (R, error)) (R, error) {
	var zero R
	if c.native.calls.isClosed() {
		return zero, errClusterTerminated
	}
	report, err := query(ctx)
	if err != nil && c.native.calls.isClosed() {
		return zero, errClusterTerminated
	}
	return report, err
}

func canonicalFSID(s string) (uuid.UUID, bool) {
	id, err := uuid.Parse(s)
	return id, err == nil && id != uuid.Nil
}
