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
// another cluster is an identity change that ends the watch.
func checkWatchFSID(operation, what, native, original string) error {
	id, ok := canonicalFSID(native)
	if !ok || native != id.String() {
		return fmt.Errorf("%s: the %s FSID is not a canonical UUID", operation, what)
	}
	if pinned, _ := canonicalFSID(original); id != pinned {
		return watch.Fatal(fmt.Errorf("%s: the %s FSID differs from the original cluster", operation, what))
	}
	return nil
}

func canonicalFSID(s string) (uuid.UUID, bool) {
	id, err := uuid.Parse(s)
	return id, err == nil && id != uuid.Nil
}
