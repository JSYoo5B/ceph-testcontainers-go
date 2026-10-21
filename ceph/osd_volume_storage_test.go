package ceph

import (
	"context"
	"errors"
	"io"
	"maps"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/volume"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type osdVolumeDockerFake struct{ osdMemoryDockerFake }

func (d *osdVolumeDockerFake) VolumeCreate(ctx context.Context, opts mobycl.VolumeCreateOptions) (mobycl.VolumeCreateResult, error) {
	result, err := d.osdMemoryDockerFake.VolumeCreate(ctx, opts)
	d.stored.CreatedAt = "synthetic-created-identity"
	result.Volume = d.stored
	return result, err
}

type osdVolumeHelperFake struct {
	osdMemoryContainerFake
	links map[string]bool
}

func (h *osdVolumeHelperFake) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if len(args) == 4 && args[0] == "test" && args[2] == "-L" {
		h.commands = append(h.commands, args)
		if h.execErr != nil {
			return 0, nil, h.execErr
		}
		code := 0
		if h.links[args[3]] {
			code = 1
		}
		return code, noInitialOSDStream(""), nil
	}
	return h.osdMemoryContainerFake.Exec(ctx, args, opts...)
}
func osdVolumeFixture() (*osdVolumeStorage, *osdVolumeDockerFake, *osdVolumeHelperFake) {
	d := &osdVolumeDockerFake{osdMemoryDockerFake: osdMemoryDockerFake{stored: volume.Volume{Name: "synthetic-owned-volume", Driver: "local", CreatedAt: "synthetic-created-identity", Labels: map[string]string{osdVolumeOwnerLabel: "synthetic-owner"}}}}
	h := &osdVolumeHelperFake{osdMemoryContainerFake: osdMemoryContainerFake{running: true, startedAt: "synthetic-start", directories: map[string]bool{}}, links: map[string]bool{}}
	s := &osdVolumeStorage{name: d.stored.Name, owner: "synthetic-owner", createdAt: d.stored.CreatedAt, helper: h, helperStarted: h.startedAt, ready: true, newClient: func(context.Context) (osdMemoryVolumeClient, error) { return d, nil }}
	return s, d, h
}

func TestOSDVolumeStorageAdmissionBeforeAllocation(t *testing.T) {
	for _, opts := range [][]testcontainers.ContainerCustomizer{
		{WithOSDVolumeStorage(), WithOSDInMemoryStorage(1)}, {WithOSDInMemoryStorage(1), WithOSDVolumeStorage()},
		{WithOSDVolumeStorage(), WithOSDObjectStore(OSDMemStore)}, {WithOSDObjectStore(OSDMemStore), WithOSDVolumeStorage()},
	} {
		c, err := Run(t.Context(), DefaultImage, opts...)
		if c != nil || !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("incompatible storage allocated resources: %v", err)
		}
	}
	o := options{blockSize: 123, osds: 2}
	if err := WithOSDVolumeStorage()(&o); err != nil || !o.osdVolumeStorage || o.blockSize != 123 || o.osds != 2 {
		t.Fatal("volume option changed capacity/topology")
	}
}

func TestOSDVolumeLazyAllocationAndUncertainCreateOwnership(t *testing.T) {
	c, _, _ := newInitialOSDCluster(options{osds: 0, osdVolumeStorage: true})
	d := &osdVolumeDockerFake{}
	lost := errors.New("synthetic lost create reply")
	d.createErr = lost
	c.osdVolumeDocker = func(context.Context) (osdMemoryVolumeClient, error) { return d, nil }
	if err := c.startInitialOSDs(t.Context()); err != nil || c.osdVolume != nil || d.creates != 0 {
		t.Fatal("zero-OSD bootstrap allocated storage")
	}
	if err := c.ensureOSDVolume(t.Context()); !errors.Is(err, lost) || c.osdVolume == nil || d.creates != 1 {
		t.Fatal("uncertain create lost ownership")
	}
	s := c.osdVolume
	if err := c.ensureOSDVolume(t.Context()); err == nil || c.osdVolume != s || d.creates != 1 {
		t.Fatal("uncertain create was replayed")
	}
	if d.createOptions.Driver != "local" || len(d.createOptions.DriverOpts) != 0 || d.createOptions.Labels[osdVolumeOwnerLabel] != s.owner {
		t.Fatal("not an owned ordinary local volume")
	}
	if err := c.Terminate(t.Context()); err != nil || c.osdVolume != nil || d.removals != 1 || d.removeOptions.Force {
		t.Fatal("uncertain allocation not safely released", err)
	}
}

func TestOSDVolumeFreshnessAndDiscardPreserveSibling(t *testing.T) {
	s, d, h := osdVolumeFixture()
	path, sibling := osdVolumeHelperPath+"/ceph-7", osdVolumeHelperPath+"/ceph-8"
	if err := s.requireFreshOSD(t.Context(), 7); err != nil {
		t.Fatal(err)
	}
	h.links[path] = true
	if err := s.requireFreshOSD(t.Context(), 7); err == nil {
		t.Fatal("dangling directory link admitted")
	}
	delete(h.links, path)
	h.directories[path], h.directories[sibling] = true, true
	if err := s.requireFreshOSD(t.Context(), 7); err == nil {
		t.Fatal("existing store admitted")
	}
	if err := s.discardOSD(t.Context(), 7); err != nil || h.directories[path] || !h.directories[sibling] {
		t.Fatal("purged directory cleanup changed sibling", err)
	}
	calls := len(h.commands)
	inspections := d.inspections
	if err := s.discardOSD(t.Context(), -1); err == nil || len(h.commands) != calls || d.inspections != inspections {
		t.Fatal("invalid ID reached storage")
	}
	d.stored.Labels[osdVolumeOwnerLabel] = "foreign"
	if err := s.discardOSD(t.Context(), 8); err == nil || len(h.commands) != calls {
		t.Fatal("foreign volume was mutated")
	}
}

func TestOSDVolumeRefusesChangedIdentityBeforeUseOrRelease(t *testing.T) {
	changes := map[string]func(*volume.Volume){
		"owner":           func(v *volume.Volume) { v.Labels[osdVolumeOwnerLabel] = "foreign" },
		"name":            func(v *volume.Volume) { v.Name = "foreign" },
		"driver":          func(v *volume.Volume) { v.Driver = "foreign" },
		"backing":         func(v *volume.Volume) { v.Options = map[string]string{"type": "tmpfs"} },
		"created":         func(v *volume.Volume) { v.CreatedAt = "replaced" },
		"missing-created": func(v *volume.Volume) { v.CreatedAt = "" },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			s, d, h := osdVolumeFixture()
			change(&d.stored)
			if err := s.requireFreshOSD(t.Context(), 7); err == nil || len(h.commands) != 0 {
				t.Fatal("foreign/replaced volume admitted")
			}
			if err := s.release(t.Context()); err == nil || s.removed || d.removals != 0 {
				t.Fatal("foreign/replaced volume removed")
			}
		})
	}
}

func TestOSDVolumeReleaseWaitsForOSDsAndRetriesBusyOrFailedCleanup(t *testing.T) {
	s, d, h := osdVolumeFixture()
	fail := errors.New("synthetic OSD still owns mount")
	osd := &osdMemoryContainerFake{terminateErr: fail}
	c := &Container{osdVolume: s, osds: map[int]*OSDContainer{7: {Container: osd}}}
	if err := c.Terminate(t.Context()); !errors.Is(err, fail) || c.osdVolume != s || h.terminations != 0 || d.removals != 0 {
		t.Fatal("storage released before OSD removal")
	}
	osd.terminateErr = nil
	busy := errors.New("synthetic referenced volume")
	d.removeErr = busy
	if err := c.Terminate(t.Context()); !errors.Is(err, busy) || c.osdVolume != s || s.removed || d.removeOptions.Force {
		t.Fatal("busy volume lost retry ownership")
	}
	attempts := d.removals
	d.removeErr = nil
	if err := c.Terminate(t.Context()); err != nil || c.osdVolume != nil || d.removals != attempts+1 || d.creates != 0 {
		t.Fatal("cleanup retry recreated or lost volume", err)
	}
}

func TestOSDVolumeHelperFailureDoesNotClaimDataLossOrRepair(t *testing.T) {
	for _, state := range []string{"stopped", "restarted", "incomplete", "removed"} {
		t.Run(state, func(t *testing.T) {
			s, d, h := osdVolumeFixture()
			switch state {
			case "stopped":
				h.running = false
			case "restarted":
				h.startedAt = "other"
			case "incomplete":
				s.ready = false
			case "removed":
				s.removed = true
			}
			c := &Container{settings: options{osdVolumeStorage: true}, osdVolume: s}
			if err := c.ensureOSDVolume(t.Context()); err == nil || c.osdVolume != s || d.creates != 0 || d.removals != 0 {
				t.Fatal("unavailable storage was recreated or admitted")
			}
		})
	}
}

func TestOSDVolumeReleaseAcceptsMissingButPreservesOtherErrors(t *testing.T) {
	s, d, _ := osdVolumeFixture()
	d.inspectErr = errdefs.ErrNotFound
	if err := s.release(t.Context()); err != nil || !s.removed || d.removals != 0 {
		t.Fatal("missing volume not settled", err)
	}
	s, d, _ = osdVolumeFixture()
	d.inspectErr = errors.New("synthetic inspect failure")
	if err := s.release(t.Context()); err == nil || s.removed || d.removals != 0 {
		t.Fatal("inspect failure discarded ownership")
	}
}

func TestOSDVolumeLaunchMountIsDistinctFromTmpfs(t *testing.T) {
	c, launch := preparedOSDLaunch(t)
	s, _, _ := osdVolumeFixture()
	c.osdVolume = s
	r := c.newOSDLaunchRecipe(launch.osd, []byte("synthetic-keyring"))
	var req testcontainers.GenericContainerRequest
	if err := r.Customize(&req); err != nil || len(req.Mounts) != 1 {
		t.Fatal("missing owned volume mount", err)
	}
	source := req.Mounts[0].Source.(testcontainers.DockerVolumeMountSource)
	if source.Name != s.name || !source.VolumeOptions.NoCopy || req.Mounts[0].Target != "/var/lib/ceph/osd" {
		t.Fatal("wrong volume attachment")
	}
	if !maps.Equal(source.VolumeOptions.Labels, testcontainers.GenericLabels()) {
		t.Fatal("mount changed volume identity labels")
	}
}

func TestOSDVolumePartialHelperRemainsOwnedUntilCleanup(t *testing.T) {
	c, _, _ := newInitialOSDCluster(options{osds: 0, osdVolumeStorage: true})
	d := &osdVolumeDockerFake{}
	h := &osdVolumeHelperFake{}
	failed := errors.New("synthetic helper start reply lost")
	c.osdVolumeDocker = func(context.Context) (osdMemoryVolumeClient, error) { return d, nil }
	calls := 0
	c.osdVolumeRunner = func(_ context.Context, _ string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		calls++
		var req testcontainers.GenericContainerRequest
		for _, opt := range opts {
			if err := opt.Customize(&req); err != nil {
				t.Fatal(err)
			}
		}
		if len(req.Files) != 0 || len(req.Mounts) != 1 || req.Labels[osdVolumeOwnerLabel] == "" {
			t.Fatal("helper received credentials or wrong storage")
		}
		return h, failed
	}
	if err := c.ensureOSDVolume(t.Context()); !errors.Is(err, failed) || c.osdVolume == nil || c.osdVolume.helper != h || c.osdVolume.ready {
		t.Fatal("partial helper lost ownership", err)
	}
	if err := c.ensureOSDVolume(t.Context()); err == nil || calls != 1 || d.creates != 1 {
		t.Fatal("partial helper setup was replayed")
	}
	h.terminateErr = failed
	if err := c.Terminate(t.Context()); !errors.Is(err, failed) || c.osdVolume == nil || d.removals != 0 {
		t.Fatal("failed helper cleanup lost volume")
	}
	h.terminateErr = nil
	if err := c.Terminate(t.Context()); err != nil || c.osdVolume != nil || d.removals != 1 || d.removeOptions.Force {
		t.Fatal("partial helper cleanup not retryable", err)
	}
}
