package ceph

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/volume"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// ownedStorageRelease is the cleanup that osdMemoryStorage.release and
// osdVolumeStorage.release share: terminate the helper container, then remove
// the owned volume after checking that it is still the one this cluster
// created. The two storages differ only in the names they print and in the
// fields and validation they plug in here; error attribution (which step
// failed, and that a failed step wraps the Docker error), ownership (a volume
// that fails validate is never removed, and removal is never forced) and retry
// (a failed helper termination or volume removal leaves helper or removed
// untouched in the way the failing step left it, so the next call repeats
// exactly that step) are the same for both.
type ownedStorageRelease struct {
	// helperName and volumeName name the parts in error text, such as
	// "OSD memory keeper" and "OSD memory volume".
	helperName, volumeName string
	// helper is the container field of the storage; release clears it after a
	// successful termination, also when the container was already gone.
	helper *testcontainers.Container
	// removed is the removal flag of the storage; release sets it after the
	// volume is gone.
	removed *bool
	// name is the owned volume.
	name string
	// client opens the Docker client used for the volume calls.
	client func(context.Context) (osdMemoryVolumeClient, error)
	// validate rejects a volume that is not the one this cluster created.
	validate func(volume.Volume) error
}

func (r ownedStorageRelease) run(ctx context.Context) (err error) {
	if *r.helper != nil {
		if err := (*r.helper).Terminate(ctx, testcontainers.StopTimeout(time.Second)); !onlyMissingHostResource(err) {
			return fmt.Errorf("terminate %s: %w", r.helperName, err)
		}
		*r.helper = nil
	}
	if *r.removed {
		return nil
	}
	docker, err := r.client(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, docker.Close()) }()
	inspected, err := docker.VolumeInspect(ctx, r.name, mobycl.VolumeInspectOptions{})
	if errdefs.IsNotFound(err) {
		*r.removed = true
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect owned %s: %w", r.volumeName, err)
	}
	if err := r.validate(inspected.Volume); err != nil {
		return err
	}
	if _, err := docker.VolumeRemove(ctx, r.name, mobycl.VolumeRemoveOptions{}); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("remove owned %s: %w", r.volumeName, err)
	}
	*r.removed = true
	return nil
}
