package ceph

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/volume"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

const osdVolumeOwnerLabel = "org.testcontainers.ceph.osd-volume-owner"
const osdVolumeHelperPath = "/tc/osd-volume"

// An ordinary local volume belongs to the cluster, not any individual OSD.
// Unlike tmpfs, its contents do not depend on keeping a helper running. The
// helper provides isolated directory operations, without cluster credentials.
type osdVolumeStorage struct {
	name, owner, createdAt string
	helper                 testcontainers.Container
	helperStarted          string
	ready, removed         bool
	newClient              func(context.Context) (osdMemoryVolumeClient, error)
}

func newOSDVolumeStorage() *osdVolumeStorage {
	owner := uuid.NewString()
	return &osdVolumeStorage{name: "tc-ceph-osd-volume-" + owner, owner: owner}
}

func (s *osdVolumeStorage) client(ctx context.Context) (osdMemoryVolumeClient, error) {
	if s.newClient != nil {
		return s.newClient(ctx)
	}
	return testcontainers.NewDockerClientWithOpts(ctx)
}

func (s *osdVolumeStorage) validate(v volume.Volume) error {
	if v.Name != s.name || v.Driver != "local" || v.Labels[osdVolumeOwnerLabel] != s.owner || len(v.Options) != 0 || v.CreatedAt == "" {
		return errors.New("OSD volume identity, ownership or ordinary local driver differs")
	}
	if s.createdAt != "" && v.CreatedAt != s.createdAt {
		return errors.New("OSD volume creation identity changed")
	}
	return nil
}

func (s *osdVolumeStorage) create(ctx context.Context) (err error) {
	docker, err := s.client(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, docker.Close()) }()
	labels := testcontainers.GenericLabels()
	labels[osdVolumeOwnerLabel] = s.owner
	result, err := docker.VolumeCreate(ctx, mobycl.VolumeCreateOptions{Name: s.name, Driver: "local", Labels: labels})
	if err != nil {
		return fmt.Errorf("create OSD volume: %w", err)
	}
	if err := s.validate(result.Volume); err != nil {
		return err
	}
	s.createdAt = result.Volume.CreatedAt
	return nil
}

func (s *osdVolumeStorage) inspect(ctx context.Context) (err error) {
	docker, err := s.client(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, docker.Close()) }()
	result, err := docker.VolumeInspect(ctx, s.name, mobycl.VolumeInspectOptions{})
	if err != nil {
		return fmt.Errorf("inspect OSD volume: %w", err)
	}
	return s.validate(result.Volume)
}

func (c *Container) ensureOSDVolume(ctx context.Context) error {
	if !c.settings.osdVolumeStorage {
		return nil
	}
	if c.osdVolume != nil {
		return c.osdVolume.check(ctx)
	}
	// Publish before creation, including a lost create response. Neither a
	// later Add nor cleanup may allocate a replacement to resolve uncertainty.
	s := newOSDVolumeStorage()
	s.newClient = c.osdVolumeDocker
	c.osdVolume = s
	if err := s.create(ctx); err != nil {
		return err
	}
	run := c.osdVolumeRunner
	if run == nil {
		run = runContainer
	}
	helper, err := run(ctx, c.settings.essentialImage, WithIdleEntrypoint(),
		testcontainers.WithHostConfigModifier(func(h *container.HostConfig) { h.NetworkMode = "none" }),
		testcontainers.WithLabels(map[string]string{osdVolumeOwnerLabel: s.owner}), c.ownedLabel(),
		osdLaunchVolume(s.name, osdVolumeHelperPath, true))
	if helper != nil {
		s.helper = helper
	}
	if err != nil {
		return fmt.Errorf("start OSD volume helper: %w", err)
	}
	if helper == nil {
		return errors.New("OSD volume helper was not returned")
	}
	state, err := helper.State(ctx)
	if err != nil {
		return fmt.Errorf("inspect new OSD volume helper: %w", err)
	}
	if state == nil || !state.Running || state.StartedAt == "" {
		return errors.New("OSD volume helper has no running process identity")
	}
	s.helperStarted, s.ready = state.StartedAt, true
	return s.check(ctx)
}

func (s *osdVolumeStorage) check(ctx context.Context) error {
	if !s.ready || s.removed || s.helper == nil {
		return errors.New("OSD volume setup is incomplete; terminate this cluster")
	}
	if err := s.inspect(ctx); err != nil {
		return err
	}
	state, err := s.helper.State(ctx)
	if err != nil {
		return fmt.Errorf("inspect OSD volume helper: %w", err)
	}
	if state == nil || !state.Running || state.StartedAt != s.helperStarted {
		return errors.New("OSD volume helper stopped or changed; new storage operations are refused")
	}
	return ctx.Err()
}

func (s *osdVolumeStorage) requireFreshOSD(ctx context.Context, id int) error {
	if id < 0 {
		return errors.New("OSD volume directory ID must be nonnegative")
	}
	if err := s.check(ctx); err != nil {
		return err
	}
	path := osdVolumeHelperPath + "/ceph-" + strconv.Itoa(id)
	for _, kind := range []string{"-e", "-L"} {
		if _, err := command(ctx, s.helper, "test", "!", kind, path); err != nil {
			return fmt.Errorf("OSD volume directory for osd.%d is not fresh: %w", id, err)
		}
	}
	return nil
}

// Called only after this registration is purged and its container removed.
func (s *osdVolumeStorage) discardOSD(ctx context.Context, id int) error {
	if id < 0 {
		return errors.New("OSD volume directory ID must be nonnegative")
	}
	if err := s.check(ctx); err != nil {
		return err
	}
	if _, err := command(ctx, s.helper, "rm", "-rf", "--", osdVolumeHelperPath+"/ceph-"+strconv.Itoa(id)); err != nil {
		return fmt.Errorf("discard purged osd.%d volume storage: %w", id, err)
	}
	return nil
}

// All tracked OSDs must have been removed. Busy/uncertain removal stays owned
// for retry; never force removal or delete a changed/foreign volume.
func (s *osdVolumeStorage) release(ctx context.Context) (err error) {
	if s.helper != nil {
		if err := s.helper.Terminate(ctx, testcontainers.StopTimeout(time.Second)); !onlyMissingHostResource(err) {
			return fmt.Errorf("terminate OSD volume helper: %w", err)
		}
		s.helper = nil
	}
	if s.removed {
		return nil
	}
	docker, err := s.client(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, docker.Close()) }()
	result, err := docker.VolumeInspect(ctx, s.name, mobycl.VolumeInspectOptions{})
	if errdefs.IsNotFound(err) {
		s.removed = true
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect owned OSD volume: %w", err)
	}
	if err := s.validate(result.Volume); err != nil {
		return err
	}
	if _, err := docker.VolumeRemove(ctx, s.name, mobycl.VolumeRemoveOptions{}); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("remove owned OSD volume: %w", err)
	}
	s.removed = true
	return nil
}
