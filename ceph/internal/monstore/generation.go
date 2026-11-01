package monstore

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
// a lock (see docs/internals/mon-volume-storage.md).

// Generations is the container side of one MON generation.
type Generations interface {
	// Create makes the container of launch from image without starting it,
	// mounting launch.volume at the MON data directory, passing launch.mode to
	// the entrypoint and giving it launch.endpoint when that is set. It returns
	// the container ID also with an error when a partial container exists. An
	// error without an ID is an unknown outcome.
	Create(ctx context.Context, launch Launch, image string) (string, error)
	// Start starts the created container of id.
	Start(ctx context.Context, id string) error
	// Observe returns the address the running container of id bound.
	Observe(ctx context.Context, id string) (Endpoint, error)
	// Hold keeps address from every other user once the old container is gone
	// and until the returned hold is released (address.go).
	Hold(ctx context.Context, address Endpoint) (EndpointHold, error)
	// WaitReady returns once the MON of container id passed its ready check.
	WaitReady(ctx context.Context, id string) error
	// Remove stops and removes the container, never its volume. A container
	// that is already gone is not an error.
	Remove(ctx context.Context, id string) error
}

func checkMonStoreGenerations(g Generations, image string) error {
	if isNilPort(g) {
		return errors.New("the MON store has no container side")
	}
	if image == "" {
		return errors.New("a MON generation needs an image")
	}
	return nil
}

// StartGeneration starts the next container on the volume. A new store is
// formatted by its first generation; a store that was ready before is required
// to exist and is given the address its first generation bound. For that, the
// exact hold on the address is taken first, kept while the container is
// created without being started, and released immediately before the start.
// The pending attachment is published before the container is created and the
// returned ID is recorded also when creation failed, so a lost reply stays
// owned and is never replayed. A first generation that reaches its ready check
// marks the store ready and records the address it bound; a later generation
// must have bound the recorded one.
func (s *Volume) StartGeneration(ctx context.Context, g Generations, image string) (string, error) {
	if err := checkMonStoreGenerations(g, image); err != nil {
		return "", err
	}
	later := s.Phase == Bound || s.Phase == Detached
	if later && s.Endpoint == nil {
		return "", fmt.Errorf("MON %s store has no recorded address for the next generation", s.mon)
	}
	if later {
		// A live attachment is refused before anything is held: its container
		// owns the address.
		if s.Attachment != nil {
			return "", fmt.Errorf("MON %s store still has an attachment that is not confirmed gone", s.mon)
		}
		if err := s.holdAddress(ctx, g); err != nil {
			return "", err
		}
	}
	launch, err := s.dispatch(ctx)
	if err != nil {
		return "", errors.Join(err, s.releaseAddressHold(ctx))
	}
	if later {
		address := s.Endpoint.clone()
		launch.Endpoint = &address
	}
	id, createErr := g.Create(ctx, launch, image)
	if id != "" {
		if err := s.recordContainer(id); err != nil {
			return id, errors.Join(createErr, err, s.releaseAddressHold(ctx))
		}
	}
	if createErr != nil {
		return id, errors.Join(fmt.Errorf("create MON %s generation %d: %w", s.mon, launch.Generation, createErr), s.releaseAddressHold(ctx))
	}
	if id == "" {
		return "", errors.Join(fmt.Errorf("create MON %s generation %d returned no container ID; the outcome stays unknown", s.mon, launch.Generation), s.releaseAddressHold(ctx))
	}
	if err := s.releaseAddressHold(ctx); err != nil {
		return id, err
	}
	if err := g.Start(ctx, id); err != nil {
		return id, fmt.Errorf("start MON %s generation %d: %w", s.mon, launch.Generation, err)
	}
	if err := g.WaitReady(ctx, id); err != nil {
		return id, fmt.Errorf("MON %s generation %d is not ready: %w", s.mon, launch.Generation, err)
	}
	if launch.Mode == Format {
		if err := s.ready(); err != nil {
			return id, err
		}
	}
	if err := s.observeAddress(ctx, g, id); err != nil {
		return id, err
	}
	return id, nil
}

// StopGeneration removes the attached container and confirms that nothing uses
// the volume any more. The attachment is cleared only by that confirmation. An
// attachment whose creation outcome is unknown has no ID to remove and is
// settled only by Terminate.
func (s *Volume) StopGeneration(ctx context.Context, g Generations) error {
	if isNilPort(g) {
		return errors.New("the MON store has no container side")
	}
	if s.Attachment == nil {
		return fmt.Errorf("MON %s store has no generation to stop", s.mon)
	}
	if s.Attachment.pending {
		return fmt.Errorf("MON %s store creation outcome is unknown; only the labeled-container scan of Terminate settles it", s.mon)
	}
	if err := g.Remove(ctx, s.Attachment.ID); err != nil {
		return fmt.Errorf("remove MON %s generation %d: %w", s.mon, s.Attachment.generation, err)
	}
	return s.confirmGone(ctx, false)
}

// ReplaceGeneration is the upgrade step: the running generation is removed and
// the next one, from image, starts on the same volume. The checks that the next
// generation needs run before the old container is touched, so a store that
// the next generation would refuse, or that has no address to give it, is found while the old MON still runs. If
// the new container then fails, the volume and its attachment record stay, and
// the store is neither formatted nor released.
func (s *Volume) ReplaceGeneration(ctx context.Context, g Generations, image string) (string, error) {
	if err := checkMonStoreGenerations(g, image); err != nil {
		return "", err
	}
	if s.Phase != Bound || s.Attachment == nil {
		return "", fmt.Errorf("MON %s store is %s without a ready generation to replace", s.mon, s.Phase)
	}
	if s.identity != nil && !s.verified {
		return "", fmt.Errorf("MON %s store identity was not verified for the next generation", s.mon)
	}
	if s.Endpoint == nil {
		return "", fmt.Errorf("MON %s store has no recorded address for the next generation", s.mon)
	}
	if err := s.inspect(ctx); err != nil {
		return "", err
	}
	if err := s.StopGeneration(ctx, g); err != nil {
		return "", err
	}
	return s.StartGeneration(ctx, g, image)
}
