package ceph

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/testcontainers/testcontainers-go"
)

// osdStoreDirectories is the store-directory protocol that the tmpfs and the
// volume backend share: osd.<id> keeps its store in <root>/ceph-<id> of the
// backend's helper container. backend names the backend in error text
// ("memory" or "volume").
type osdStoreDirectories struct {
	root, backend string
}

// path is the store directory of osd.id.
func (d osdStoreDirectories) path(id int) string {
	return d.root + "/ceph-" + strconv.Itoa(id)
}

// requireFresh refuses a store directory of osd.id that exists or is a
// symlink, a dangling one included: test -e follows a symlink, so it alone
// admits a dangling link as fresh. check confirms that the backend and its
// helper are usable; helper is read after it.
func (d osdStoreDirectories) requireFresh(ctx context.Context, discard *osdStoreDiscard, check func(context.Context) error, helper func() testcontainers.Container, id int) error {
	if discard.marker != "" {
		return errors.New("an earlier OSD store discard is unconfirmed; retry removal before reusing storage")
	}
	if id < 0 {
		return fmt.Errorf("OSD %s directory ID must be nonnegative", d.backend)
	}
	if err := check(ctx); err != nil {
		return err
	}
	path := d.path(id)
	for _, kind := range []string{"-e", "-L"} {
		if _, err := command(ctx, helper(), "test", "!", kind, path); err != nil {
			return fmt.Errorf("OSD %s directory for osd.%d is not fresh: %w", d.backend, id, err)
		}
	}
	return nil
}

// discard removes the store directory of a purged osd.id.
func (d osdStoreDirectories) discard(ctx context.Context, discard *osdStoreDiscard, check func(context.Context) error, helper func() testcontainers.Container, id int) error {
	if id < 0 {
		return fmt.Errorf("OSD %s directory ID must be nonnegative", d.backend)
	}
	if err := check(ctx); err != nil {
		return err
	}
	if err := discard.run(ctx, helper(), d.path(id)); err != nil {
		return fmt.Errorf("discard purged osd.%d %s storage: %w", id, d.backend, err)
	}
	return nil
}
