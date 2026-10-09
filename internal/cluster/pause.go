package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"

	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

type containerPauseClient interface {
	ContainerInspect(context.Context, string, mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error)
	ContainerPause(context.Context, string, mobycl.ContainerPauseOptions) (mobycl.ContainerPauseResult, error)
	ContainerUnpause(context.Context, string, mobycl.ContainerUnpauseOptions) (mobycl.ContainerUnpauseResult, error)
	Close() error
}

// ContainerPause owns one container frozen through Docker's cgroup freezer.
// Resume thaws it without restarting the process. A non-nil handle returned
// with an error must be resumed: a transport failure cannot prove that Docker
// did not freeze the container. Resume is retryable and idempotent.
type ContainerPause struct {
	ContainerID string
	mu          sync.Mutex
	docker      containerPauseClient
	complete    bool
}

// PauseContainer freezes every process of an owned daemon or a caller-owned
// WithClient container, as with docker pause. Unlike Stop, the process keeps
// its memory, sockets and Ceph identity, and peers see silence rather than a
// closed connection: a frozen OSD stops answering client operations and
// heartbeats until its peers report it down after the heartbeat grace. Hold
// nodown with TemporaryOSDFlag to keep it up and make client operations wait
// for their own timeouts or be reported as slow. Host networking is supported.
//
// The container that runs this cluster's control CLI is refused because every
// fixture command would block on it. A container that is already paused is
// refused rather than adopted. The cluster retains the handle and resumes it
// during Terminate; resume explicitly before removing a caller-owned container.
func (c *Container) PauseContainer(ctx context.Context, ctr testcontainers.Container) (*ContainerPause, error) {
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	if c.closed || ctr == nil || ctr.GetContainerID() == "" {
		return nil, errors.New("container pause requires a live cluster and a started container")
	}
	id := ctr.GetContainerID()
	control, err := c.ControlContainerContext(ctx)
	if err != nil {
		return nil, err
	}
	if control != nil && control.GetContainerID() == id {
		return nil, errors.New("pausing the control CLI container would block every fixture command")
	}
	if previous := c.pauses[id]; previous != nil {
		complete, err := previous.isComplete(ctx)
		if err != nil {
			return previous, err
		}
		if !complete {
			return previous, errors.New("resume the existing container pause first")
		}
	}
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return nil, err
	}
	pause, err := pauseContainer(ctx, docker, id)
	if pause != nil {
		if c.pauses == nil {
			c.pauses = make(map[string]*ContainerPause)
		}
		c.pauses[id] = pause
	}
	return pause, err
}

func pauseContainer(ctx context.Context, docker containerPauseClient, id string) (*ContainerPause, error) {
	inspection, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
	if err != nil {
		return nil, errors.Join(fmt.Errorf("inspect container before pause: %w", err), docker.Close())
	}
	state := inspection.Container.State
	if state == nil || !state.Running || state.Paused || state.Restarting {
		return nil, errors.Join(errors.New("only a running, unpaused container can be paused"), docker.Close())
	}
	pause := &ContainerPause{ContainerID: id, docker: docker}
	// Return ownership even after an uncertain transport outcome.
	if _, err := docker.ContainerPause(ctx, id, mobycl.ContainerPauseOptions{}); err != nil {
		return pause, fmt.Errorf("pause container: %w", err)
	}
	return pause, nil
}

func (pause *ContainerPause) isComplete(ctx context.Context) (bool, error) {
	if err := lockTopologyMutex(ctx, &pause.mu); err != nil {
		return false, err
	}
	defer pause.mu.Unlock()
	return pause.complete, nil
}

// Resume thaws the container. A removed, stopped or already unpaused container
// needs no further action, so it completes the handle without starting it.
func (pause *ContainerPause) Resume(ctx context.Context) error {
	if pause == nil {
		return errors.New("container pause is unavailable")
	}
	if err := lockTopologyMutex(ctx, &pause.mu); err != nil {
		return err
	}
	defer pause.mu.Unlock()
	if pause.complete {
		return nil
	}
	inspection, err := pause.docker.ContainerInspect(ctx, pause.ContainerID, mobycl.ContainerInspectOptions{})
	if err != nil && !onlyMissingHostResource(err) {
		return fmt.Errorf("inspect paused container: %w", err)
	}
	if err == nil && inspection.Container.State != nil && inspection.Container.State.Paused {
		if _, err := pause.docker.ContainerUnpause(ctx, pause.ContainerID, mobycl.ContainerUnpauseOptions{}); err != nil && !onlyMissingHostResource(err) {
			return fmt.Errorf("resume paused container: %w", err)
		}
	}
	if err := pause.docker.Close(); err != nil {
		return fmt.Errorf("close pause Docker client: %w", err)
	}
	pause.complete = true
	return nil
}
