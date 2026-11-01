package monstore

import (
	"context"
	"fmt"
	"maps"

	"github.com/moby/moby/api/types/volume"
	mobycl "github.com/moby/moby/client"
)

// volumeDockerFake is the Docker volume engine of the store tests: it keeps
// the one volume it created, counts the calls and fails them on request.
type volumeDockerBase struct {
	stored                           volume.Volume
	createErr, inspectErr, removeErr error
	closeErr                         error
	creates, inspections, removals   int
	closes                           int
	createOptions                    mobycl.VolumeCreateOptions
	removeOptions                    mobycl.VolumeRemoveOptions
}

func (d *volumeDockerBase) VolumeCreate(_ context.Context, opts mobycl.VolumeCreateOptions) (mobycl.VolumeCreateResult, error) {
	d.creates++
	d.createOptions = opts
	// A failed reply can follow a committed allocation.
	d.stored = volume.Volume{Name: opts.Name, Driver: opts.Driver, Options: maps.Clone(opts.DriverOpts), Labels: maps.Clone(opts.Labels)}
	return mobycl.VolumeCreateResult{Volume: d.stored}, d.createErr
}

func (d *volumeDockerBase) VolumeInspect(_ context.Context, name string, _ mobycl.VolumeInspectOptions) (mobycl.VolumeInspectResult, error) {
	d.inspections++
	if name != d.stored.Name && d.inspectErr == nil {
		return mobycl.VolumeInspectResult{}, fmt.Errorf("unexpected volume name %q", name)
	}
	return mobycl.VolumeInspectResult{Volume: d.stored}, d.inspectErr
}

func (d *volumeDockerBase) VolumeRemove(_ context.Context, name string, opts mobycl.VolumeRemoveOptions) (mobycl.VolumeRemoveResult, error) {
	d.removals++
	d.removeOptions = opts
	if name != d.stored.Name {
		return mobycl.VolumeRemoveResult{}, fmt.Errorf("unexpected volume deletion %q", name)
	}
	return mobycl.VolumeRemoveResult{}, d.removeErr
}

func (d *volumeDockerBase) Close() error { d.closes++; return d.closeErr }

type volumeDockerFake struct{ volumeDockerBase }

func (d *volumeDockerFake) VolumeCreate(ctx context.Context, opts mobycl.VolumeCreateOptions) (mobycl.VolumeCreateResult, error) {
	result, err := d.volumeDockerBase.VolumeCreate(ctx, opts)
	d.stored.CreatedAt = "synthetic-created-identity"
	result.Volume = d.stored
	return result, err
}
