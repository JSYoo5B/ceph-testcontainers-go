package ceph

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/volume"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestOSDMemorySizeAdmission(t *testing.T) {
	for _, size := range []int64{0, -1} {
		cluster, err := Run(t.Context(), DefaultImage, WithOSDInMemoryStorage(size))
		if err == nil || cluster != nil {
			t.Fatalf("invalid RAM ceiling allocated a cluster: size=%d cluster=%v err=%v", size, cluster, err)
		}
	}
	settings := options{blockSize: 1 << 30, osds: 3}
	if err := WithOSDInMemoryStorage(1)(&settings); err != nil || settings.osdMemorySize != 1 || settings.blockSize != 1<<30 || settings.osds != 3 {
		t.Fatalf("positive backing ceiling changed logical capacity/topology: %+v err=%v", settings, err)
	}
}

type osdMemoryDockerFake struct {
	stored                           volume.Volume
	createErr, inspectErr, removeErr error
	closeErr                         error
	creates, inspections, removals   int
	closes                           int
	createOptions                    mobycl.VolumeCreateOptions
	removeOptions                    mobycl.VolumeRemoveOptions
}

func (d *osdMemoryDockerFake) VolumeCreate(_ context.Context, opts mobycl.VolumeCreateOptions) (mobycl.VolumeCreateResult, error) {
	d.creates++
	d.createOptions = opts
	// A failed reply can follow a committed allocation.
	d.stored = volume.Volume{Name: opts.Name, Driver: opts.Driver, Options: maps.Clone(opts.DriverOpts), Labels: maps.Clone(opts.Labels)}
	return mobycl.VolumeCreateResult{Volume: d.stored}, d.createErr
}

func (d *osdMemoryDockerFake) VolumeInspect(_ context.Context, name string, _ mobycl.VolumeInspectOptions) (mobycl.VolumeInspectResult, error) {
	d.inspections++
	if name != d.stored.Name && d.inspectErr == nil {
		return mobycl.VolumeInspectResult{}, fmt.Errorf("unexpected volume name %q", name)
	}
	return mobycl.VolumeInspectResult{Volume: d.stored}, d.inspectErr
}

func (d *osdMemoryDockerFake) VolumeRemove(_ context.Context, name string, opts mobycl.VolumeRemoveOptions) (mobycl.VolumeRemoveResult, error) {
	d.removals++
	d.removeOptions = opts
	if name != d.stored.Name {
		return mobycl.VolumeRemoveResult{}, fmt.Errorf("unexpected volume deletion %q", name)
	}
	return mobycl.VolumeRemoveResult{}, d.removeErr
}

func (d *osdMemoryDockerFake) Close() error { d.closes++; return d.closeErr }

type osdMemoryContainerFake struct {
	testcontainers.Container
	stateErr, terminateErr, execErr error
	running                         bool
	startedAt                       string
	states, terminations            int
	directories                     map[string]bool
	commands                        [][]string
}

func (c *osdMemoryContainerFake) State(context.Context) (*container.State, error) {
	c.states++
	return &container.State{Running: c.running, StartedAt: c.startedAt}, c.stateErr
}

func (c *osdMemoryContainerFake) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	c.terminations++
	return c.terminateErr
}

func (c *osdMemoryContainerFake) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.commands = append(c.commands, slices.Clone(args))
	if c.execErr != nil {
		return 0, nil, c.execErr
	}
	code := 0
	switch {
	case len(args) == 4 && slices.Equal(args[:3], []string{"test", "!", "-e"}):
		if c.directories[args[3]] {
			code = 1
		}
	case len(args) == 4 && slices.Equal(args[:3], []string{"rm", "-rf", "--"}):
		delete(c.directories, args[3])
	default:
		return 0, nil, fmt.Errorf("unexpected memory-storage command %q", args)
	}
	return code, bytes.NewReader(nil), nil
}

func newOSDMemoryUnitFixture() (*osdMemoryStorage, *osdMemoryDockerFake, *osdMemoryContainerFake) {
	docker := &osdMemoryDockerFake{stored: volume.Volume{
		Name: "owned-ram-volume", Driver: "local",
		Labels:  map[string]string{osdMemoryOwnerLabel: "original-owner"},
		Options: map[string]string{"type": "tmpfs", "device": "tmpfs", "o": "size=1073741824,mode=0700"},
	}}
	keeper := &osdMemoryContainerFake{running: true, startedAt: "original-start", directories: map[string]bool{}}
	storage := &osdMemoryStorage{name: docker.stored.Name, owner: "original-owner", size: 1 << 30, keeper: keeper, keeperStarted: "original-start", ready: true,
		newClient: func(context.Context) (osdMemoryVolumeClient, error) { return docker, nil },
	}
	return storage, docker, keeper
}

func TestOSDMemoryAllocationKeepsAnUncertainCreateOwned(t *testing.T) {
	s, docker, _ := newOSDMemoryUnitFixture()
	s.keeper, s.ready = nil, false
	lostReply := errors.New("volume created, reply lost")
	docker.createErr = lostReply
	if err := s.createVolume(t.Context()); !errors.Is(err, lostReply) || s.name != docker.stored.Name || s.owner != docker.stored.Labels[osdMemoryOwnerLabel] {
		t.Fatalf("uncertain create lost original identity: storage=%+v err=%v", s, err)
	}
	if docker.createOptions.Driver != "local" || docker.createOptions.DriverOpts["type"] != "tmpfs" || docker.closes != 1 {
		t.Fatal("allocation changed the backing driver or leaked the inspection client")
	}
	if err := s.release(t.Context()); err != nil || !s.volumeRemoved || docker.removals != 1 || docker.removeOptions.Force || docker.creates != 1 {
		t.Fatalf("partial create was not safely cleaned without recreation: %+v err=%v", docker, err)
	}
}

func TestOSDMemoryReleaseRefusesForeignOrChangedVolumes(t *testing.T) {
	for _, change := range []struct {
		name   string
		mutate func(*volume.Volume)
	}{
		{"name", func(v *volume.Volume) { v.Name = "foreign-volume" }},
		{"driver", func(v *volume.Volume) { v.Driver = "foreign-driver" }},
		{"owner", func(v *volume.Volume) { v.Labels[osdMemoryOwnerLabel] = "foreign-owner" }},
		{"filesystem", func(v *volume.Volume) { v.Options["type"] = "ext4" }},
		{"device", func(v *volume.Volume) { v.Options["device"] = "/foreign/disk" }},
		{"capacity", func(v *volume.Volume) { v.Options["o"] = "size=2147483648,mode=0700" }},
	} {
		t.Run(change.name, func(t *testing.T) {
			s, docker, _ := newOSDMemoryUnitFixture()
			s.keeper = nil
			change.mutate(&docker.stored)
			if err := s.validateVolume(docker.stored); err == nil {
				t.Fatal("foreign/changed volume passed original-ownership validation")
			}
			if err := s.release(t.Context()); err == nil || docker.removals != 0 || s.volumeRemoved || docker.closes != 1 {
				t.Fatalf("foreign/changed volume was removed or ownership lost: %+v err=%v", docker, err)
			}
		})
	}
}

func TestOSDMemoryReleaseRetriesOnlyPendingCleanup(t *testing.T) {
	s, docker, keeper := newOSDMemoryUnitFixture()
	keeperError := errors.New("keeper termination failed")
	keeper.terminateErr = keeperError
	if err := s.release(t.Context()); !errors.Is(err, keeperError) || s.keeper != keeper || docker.inspections != 0 || docker.removals != 0 {
		t.Fatalf("failed keeper cleanup released its live backing: storage=%+v err=%v", s, err)
	}
	keeper.terminateErr = nil
	lostReply := errors.New("volume removal response lost")
	docker.removeErr = lostReply
	if err := s.release(t.Context()); !errors.Is(err, lostReply) || s.keeper != nil || s.volumeRemoved || docker.removeOptions.Force {
		t.Fatalf("uncertain removal lost retryable volume ownership: %+v err=%v", s, err)
	}
	docker.inspectErr = errdefs.ErrNotFound
	if err := s.release(t.Context()); err != nil || !s.volumeRemoved || keeper.terminations != 2 || docker.removals != 1 || docker.closes != 2 {
		t.Fatalf("retry repeated completed native work or failed to reconcile absence: storage=%+v docker=%+v err=%v", s, docker, err)
	}
	if err := s.release(t.Context()); err != nil || keeper.terminations != 2 || docker.inspections != 2 || docker.removals != 1 || docker.closes != 2 {
		t.Fatal("completed cleanup repeated resource removal", err)
	}
}

func TestOSDMemoryCloseFailurePreservesTheNativeCause(t *testing.T) {
	s, docker, _ := newOSDMemoryUnitFixture()
	s.keeper = nil
	inspectionError, closeError := errors.New("inspection failed"), errors.New("close failed")
	docker.inspectErr, docker.closeErr = inspectionError, closeError
	if err := s.release(t.Context()); !errors.Is(err, inspectionError) || !errors.Is(err, closeError) || s.volumeRemoved || docker.removals != 0 {
		t.Fatalf("cleanup error swallowed native failure or released uninspected volume: %v", err)
	}
}

func TestOSDMemoryTerminatePreservesKeeperUntilOSDsAreGone(t *testing.T) {
	s, docker, keeper := newOSDMemoryUnitFixture()
	osdError := errors.New("OSD container removal failed")
	osd := &osdMemoryContainerFake{terminateErr: osdError}
	cluster := &Container{osdMemory: s, osds: map[int]*OSDContainer{4: {Container: osd}}}
	if err := cluster.Terminate(t.Context()); !errors.Is(err, osdError) || len(cluster.osds) != 1 || cluster.osdMemory != s || keeper.terminations != 0 || docker.removals != 0 {
		t.Fatalf("unfinished OSD cleanup released its memory store: err=%v", err)
	}
	osd.terminateErr = nil
	if err := cluster.Terminate(t.Context()); err != nil || len(cluster.osds) != 0 || cluster.osdMemory != nil || keeper.terminations != 1 || docker.removals != 1 || docker.removeOptions.Force {
		t.Fatalf("retry did not release storage after the original OSD was removed: err=%v", err)
	}
}

func TestOSDMemoryReuseRequiresTheOriginalLiveKeeper(t *testing.T) {
	for _, condition := range []string{"live", "stopped", "restarted", "inspection-error", "partial", "released"} {
		t.Run(condition, func(t *testing.T) {
			s, docker, keeper := newOSDMemoryUnitFixture()
			cluster := &Container{settings: options{osdMemorySize: 1 << 30}, osdMemory: s}
			nativeError := errors.New("keeper inspect failed")
			switch condition {
			case "stopped":
				keeper.running = false
			case "restarted":
				keeper.startedAt = "later-start"
			case "inspection-error":
				keeper.stateErr = nativeError
			case "partial":
				s.ready = false
			case "released":
				s.volumeRemoved = true
			}
			err := cluster.ensureOSDMemory(t.Context())
			if (err == nil) != (condition == "live") || cluster.osdMemory != s || s.keeper != keeper || docker.creates != 0 || docker.removals != 0 {
				t.Fatalf("keeper failure reallocated/released storage: condition=%s storage=%+v err=%v", condition, s, err)
			}
			if condition == "inspection-error" && !errors.Is(err, nativeError) {
				t.Fatal("native keeper inspection failure was lost", err)
			}
		})
	}
}

func TestOSDMemoryDiscardAllowsOnlyThePurgedNumericDirectory(t *testing.T) {
	s, _, keeper := newOSDMemoryUnitFixture()
	path, sibling := osdMemoryKeeperPath+"/ceph-4", osdMemoryKeeperPath+"/ceph-5"
	keeper.directories[path], keeper.directories[sibling] = true, true
	if err := s.requireFreshOSD(t.Context(), 4); err == nil {
		t.Fatal("old ready/store directory was accepted for a newly allocated ID")
	}
	if err := s.discardOSD(t.Context(), -1); err == nil || len(keeper.commands) != 1 {
		t.Fatal("invalid numeric ID reached a native delete")
	}
	deleteError := errors.New("directory removal response failed")
	keeper.execErr = deleteError
	if err := s.discardOSD(t.Context(), 4); !errors.Is(err, deleteError) || !keeper.directories[path] {
		t.Fatal("native discard failure was swallowed or simulated as success", err)
	}
	keeper.execErr = nil
	if err := s.discardOSD(t.Context(), 4); err != nil || keeper.directories[path] || !keeper.directories[sibling] {
		t.Fatal("discard changed another OSD's storage or failed to remove the original", err)
	}
	if err := s.requireFreshOSD(t.Context(), 4); err != nil {
		t.Fatal("purged ID could not be reused with a fresh store", err)
	}
	if !slices.Equal(keeper.commands[0], []string{"test", "!", "-e", path}) || !slices.Equal(keeper.commands[2], []string{"rm", "-rf", "--", path}) {
		t.Fatal("owned-directory operation expanded beyond its module-derived path", keeper.commands)
	}
}
