package ceph

import (
	"context"
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

// osdStoreDiscard retains an invocation whose completion is unknown. The
// storage helper has a fixed process identity and Container.mu serializes
// all calls. A retry must settle the old invocation before it can delete
// again or release the numeric directory for reuse.
type osdStoreDiscard struct {
	marker string
}

func (d *osdStoreDiscard) run(ctx context.Context, helper testcontainers.Container, path string) error {
	const label = "OSD store discard"
	if d.marker != "" {
		if err := supervised.Cancel(ctx, helper, label, d.marker); err != nil {
			return fmt.Errorf("earlier OSD store discard may still run: %w", err)
		}
		d.marker = ""
	}
	result, err := supervised.Command(ctx, helper, label, 3*time.Minute, 4096, 4096, "rm", "-rf", "--", path)
	if err != nil {
		if marker, unconfirmed := supervised.Unconfirmed(err); unconfirmed {
			d.marker = marker
		}
		return err
	}
	if result.ExitCode != 0 {
		return nativeFailure("rm", result.ExitCode, result.Stdout, result.Stderr)
	}
	return nil
}
