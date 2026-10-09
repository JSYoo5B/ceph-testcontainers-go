package ceph

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"time"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/volume"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const osdMemoryOwnerLabel = "org.testcontainers.ceph.osd-memory-owner"
const osdMemoryKeeperPath = "/tc/osd-memory"

type osdMemoryVolumeClient interface {
	VolumeCreate(context.Context, mobycl.VolumeCreateOptions) (mobycl.VolumeCreateResult, error)
	VolumeInspect(context.Context, string, mobycl.VolumeInspectOptions) (mobycl.VolumeInspectResult, error)
	VolumeRemove(context.Context, string, mobycl.VolumeRemoveOptions) (mobycl.VolumeRemoveResult, error)
	Close() error
}

// A single mounted volume bounds all OSD stores in this cluster. The keeper
// stays running when OSDs stop; the local driver otherwise unmounts tmpfs and
// discards its contents when the last running user stops.
type osdMemoryStorage struct {
	name          string
	owner         string
	size          int64
	keeper        testcontainers.Container
	keeperStarted string
	ready         bool
	volumeRemoved bool
	newClient     func(context.Context) (osdMemoryVolumeClient, error)
}

func newOSDMemoryStorage(size int64) *osdMemoryStorage {
	owner := uuid.NewString()
	return &osdMemoryStorage{name: "tc-ceph-osd-memory-" + owner, owner: owner, size: size}
}

func (s *osdMemoryStorage) client(ctx context.Context) (osdMemoryVolumeClient, error) {
	if s.newClient != nil {
		return s.newClient(ctx)
	}
	return testcontainers.NewDockerClientWithOpts(ctx)
}

func (s *osdMemoryStorage) driverOptions() map[string]string {
	return map[string]string{"type": "tmpfs", "device": "tmpfs", "o": fmt.Sprintf("size=%d,mode=0700", s.size)}
}

func (s *osdMemoryStorage) validateVolume(v volume.Volume) error {
	if v.Name != s.name || v.Driver != "local" || v.Labels[osdMemoryOwnerLabel] != s.owner {
		return errors.New("OSD memory volume identity or ownership differs")
	}
	for name, value := range s.driverOptions() {
		if v.Options[name] != value {
			return fmt.Errorf("OSD memory volume option %s differs", name)
		}
	}
	return nil
}

func (s *osdMemoryStorage) createVolume(ctx context.Context) (err error) {
	docker, err := s.client(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, docker.Close()) }()
	labels := testcontainers.GenericLabels()
	labels[osdMemoryOwnerLabel] = s.owner
	created, err := docker.VolumeCreate(ctx, mobycl.VolumeCreateOptions{
		Name: s.name, Driver: "local", DriverOpts: s.driverOptions(), Labels: labels,
	})
	if err != nil {
		return fmt.Errorf("create OSD memory volume: %w", err)
	}
	return s.validateVolume(created.Volume)
}

func (s *osdMemoryStorage) mount(target string) testcontainers.CustomizeRequestOption {
	return testcontainers.WithMounts(testcontainers.ContainerMount{
		Source: testcontainers.DockerVolumeMountSource{Name: s.name, VolumeOptions: &mount.VolumeOptions{
			NoCopy: true, Labels: maps.Clone(testcontainers.GenericLabels()),
		}},
		Target: testcontainers.ContainerMountTarget(target),
	})
}

func (c *Container) ensureOSDMemory(ctx context.Context) error {
	if c.settings.osdMemorySize == 0 {
		return nil
	}
	if c.osdMemory != nil {
		return c.osdMemory.checkKeeper(ctx)
	}
	// Publish before allocation: an interrupted create may have committed and
	// the generated volume name remains owned for explicit cleanup.
	s := newOSDMemoryStorage(c.settings.osdMemorySize)
	c.osdMemory = s
	if err := s.createVolume(ctx); err != nil {
		return err
	}
	keeper, err := testcontainers.Run(ctx, c.settings.controlImage,
		WithIdleEntrypoint(),
		testcontainers.WithHostConfigModifier(func(host *container.HostConfig) { host.NetworkMode = "none" }),
		testcontainers.WithLabels(map[string]string{osdMemoryOwnerLabel: s.owner}),
		s.mount(osdMemoryKeeperPath),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"sh", "-c",
			`awk '$5 == "/tc/osd-memory" && / - tmpfs / {found=1} END {exit !found}' /proc/self/mountinfo`,
		}).WithStartupTimeout(c.settings.startupTimeout)),
	)
	if keeper != nil {
		s.keeper = keeper
	}
	if err != nil {
		return fmt.Errorf("start OSD memory keeper: %w", err)
	}
	state, err := keeper.State(ctx)
	if err != nil {
		return fmt.Errorf("inspect new OSD memory keeper: %w", err)
	}
	if state == nil || !state.Running || state.StartedAt == "" {
		return errors.New("new OSD memory keeper has no running process identity")
	}
	s.keeperStarted = state.StartedAt
	s.ready = true
	return s.checkKeeper(ctx)
}

func (s *osdMemoryStorage) checkKeeper(ctx context.Context) error {
	if !s.ready || s.keeper == nil || s.volumeRemoved {
		return errors.New("OSD memory storage is incomplete; terminate this cluster")
	}
	state, err := s.keeper.State(ctx)
	if err != nil {
		return fmt.Errorf("inspect OSD memory keeper: %w", err)
	}
	if state == nil || !state.Running {
		return errors.New("OSD memory keeper is not running; stored data may be lost")
	}
	if state.StartedAt != s.keeperStarted {
		return errors.New("OSD memory keeper restarted; stored data may be lost")
	}
	return ctx.Err()
}

func (s *osdMemoryStorage) requireFreshOSD(ctx context.Context, id int) error {
	if id < 0 {
		return errors.New("OSD memory directory ID must be nonnegative")
	}
	if err := s.checkKeeper(ctx); err != nil {
		return err
	}
	_, err := command(ctx, s.keeper, "test", "!", "-e", osdMemoryKeeperPath+"/ceph-"+strconv.Itoa(id))
	if err != nil {
		return fmt.Errorf("OSD memory directory for osd.%d is not fresh: %w", id, err)
	}
	return nil
}

func (s *osdMemoryStorage) discardOSD(ctx context.Context, id int) error {
	if id < 0 {
		return errors.New("OSD memory directory ID must be nonnegative")
	}
	if err := s.checkKeeper(ctx); err != nil {
		return err
	}
	_, err := command(ctx, s.keeper, "rm", "-rf", "--", osdMemoryKeeperPath+"/ceph-"+strconv.Itoa(id))
	if err != nil {
		return fmt.Errorf("discard purged osd.%d memory storage: %w", id, err)
	}
	return nil
}

// Called only after all owned OSD containers have been removed. Failed keeper
// or volume removal remains retryable; never force-delete a referenced volume.
func (s *osdMemoryStorage) release(ctx context.Context) (err error) {
	if s.keeper != nil {
		if err := s.keeper.Terminate(ctx, testcontainers.StopTimeout(time.Second)); !onlyMissingHostResource(err) {
			return fmt.Errorf("terminate OSD memory keeper: %w", err)
		}
		s.keeper = nil
	}
	if s.volumeRemoved {
		return nil
	}
	docker, err := s.client(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, docker.Close()) }()
	inspected, err := docker.VolumeInspect(ctx, s.name, mobycl.VolumeInspectOptions{})
	if errdefs.IsNotFound(err) {
		s.volumeRemoved = true
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect owned OSD memory volume: %w", err)
	}
	if err := s.validateVolume(inspected.Volume); err != nil {
		return err
	}
	if _, err := docker.VolumeRemove(ctx, s.name, mobycl.VolumeRemoveOptions{}); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("remove owned OSD memory volume: %w", err)
	}
	s.volumeRemoved = true
	return nil
}
