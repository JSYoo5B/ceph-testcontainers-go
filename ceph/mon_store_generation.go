package ceph

import (
	"context"
	"errors"
	"fmt"
)

// This file is the replacement path of a MON that has its own store volume: the
// old container is stopped and removed, and a new container, possibly from
// another image, mounts the same volume and starts on it. The volume is the
// only thing that carries the MON's data across the replacement.
//
// Nothing calls it. There is no public name, no option, no Docker container
// call (the container work is behind monStoreGenerations, which only tests
// implement) and no native run, so what is checked is the order of the steps
// and the refusals, against a fake. It adds no lock: every method runs under
// Container.mu like the rest of monStoreVolume, and calls no method that takes
// a lock (see docs/design/mon-volume-storage.md).

// monStoreGenerations is the container side of one MON generation.
type monStoreGenerations interface {
	// Create makes and starts the container of launch from image, mounting
	// launch.volume at the MON data directory and passing launch.mode to the
	// entrypoint. It returns the container ID also with an error when a
	// partial container exists. An error without an ID is an unknown outcome.
	Create(ctx context.Context, launch monStoreLaunch, image string) (string, error)
	// WaitReady returns once the MON of container id passed its ready check.
	WaitReady(ctx context.Context, id string) error
	// Remove stops and removes the container, never its volume. A container
	// that is already gone is not an error.
	Remove(ctx context.Context, id string) error
}

func checkMonStoreGenerations(g monStoreGenerations, image string) error {
	if isNilPort(g) {
		return errors.New("the MON store has no container side")
	}
	if image == "" {
		return errors.New("a MON generation needs an image")
	}
	return nil
}

// startGeneration starts the next container on the volume. A new store is
// formatted by its first generation; a store that was ready before is required
// to exist. The pending attachment is published before the container is
// created and the returned ID is recorded also when creation failed, so a lost
// reply stays owned and is never replayed. A first generation that reaches its
// ready check marks the store ready.
func (s *monStoreVolume) startGeneration(ctx context.Context, g monStoreGenerations, image string) (string, error) {
	if err := checkMonStoreGenerations(g, image); err != nil {
		return "", err
	}
	launch, err := s.dispatch(ctx)
	if err != nil {
		return "", err
	}
	id, createErr := g.Create(ctx, launch, image)
	if id != "" {
		if err := s.recordContainer(id); err != nil {
			return id, errors.Join(createErr, err)
		}
	}
	if createErr != nil {
		return id, fmt.Errorf("create MON %s generation %d: %w", s.mon, launch.generation, createErr)
	}
	if id == "" {
		return "", fmt.Errorf("create MON %s generation %d returned no container ID; the outcome stays unknown", s.mon, launch.generation)
	}
	if err := g.WaitReady(ctx, id); err != nil {
		return id, fmt.Errorf("MON %s generation %d is not ready: %w", s.mon, launch.generation, err)
	}
	if launch.mode == monStoreFormat {
		if err := s.ready(); err != nil {
			return id, err
		}
	}
	return id, nil
}

// stopGeneration removes the attached container and confirms that nothing uses
// the volume any more. The attachment is cleared only by that confirmation. An
// attachment whose creation outcome is unknown has no ID to remove and is
// settled only by Terminate.
func (s *monStoreVolume) stopGeneration(ctx context.Context, g monStoreGenerations) error {
	if isNilPort(g) {
		return errors.New("the MON store has no container side")
	}
	if s.attachment == nil {
		return fmt.Errorf("MON %s store has no generation to stop", s.mon)
	}
	if s.attachment.pending {
		return fmt.Errorf("MON %s store creation outcome is unknown; only the labeled-container scan of Terminate settles it", s.mon)
	}
	if err := g.Remove(ctx, s.attachment.id); err != nil {
		return fmt.Errorf("remove MON %s generation %d: %w", s.mon, s.attachment.generation, err)
	}
	return s.confirmGone(ctx, false)
}

// replaceGeneration is the upgrade step: the running generation is removed and
// the next one, from image, starts on the same volume. The checks that the next
// generation needs run before the old container is touched, so a store that
// the next generation would refuse is found while the old MON still runs. If
// the new container then fails, the volume and its attachment record stay, and
// the store is neither formatted nor released.
func (s *monStoreVolume) replaceGeneration(ctx context.Context, g monStoreGenerations, image string) (string, error) {
	if err := checkMonStoreGenerations(g, image); err != nil {
		return "", err
	}
	if s.phase != monStoreBound || s.attachment == nil {
		return "", fmt.Errorf("MON %s store is %s without a ready generation to replace", s.mon, s.phase)
	}
	if s.identity != nil && !s.verified {
		return "", fmt.Errorf("MON %s store identity was not verified for the next generation", s.mon)
	}
	if err := s.inspect(ctx); err != nil {
		return "", err
	}
	if err := s.stopGeneration(ctx, g); err != nil {
		return "", err
	}
	return s.startGeneration(ctx, g, image)
}
