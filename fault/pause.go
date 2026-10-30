package fault

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

type containerPauseClient interface {
	ContainerInspect(context.Context, string, mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error)
	ContainerPause(context.Context, string, mobycl.ContainerPauseOptions) (mobycl.ContainerPauseResult, error)
	ContainerUnpause(context.Context, string, mobycl.ContainerUnpauseOptions) (mobycl.ContainerUnpauseResult, error)
	Close() error
}

// PauseLease owns one container frozen through Docker's cgroup freezer.
// Resume thaws it without restarting the process. A non-nil handle returned
// with an error must be resumed: a transport failure cannot prove that Docker
// did not freeze the container. Resume is retryable and idempotent.
type PauseLease struct {
	ContainerID string
	mu          sync.Mutex
	docker      containerPauseClient
	complete    bool
}

// Pause freezes every process of an owned daemon or a caller-owned
// WithClient container, as with docker pause. Unlike Stop, the process keeps
// its memory, sockets and Ceph identity, and peers see silence rather than a
// closed connection: a frozen OSD stops answering client operations and
// heartbeats until its peers report it down after the heartbeat grace. Hold
// nodown with override.SetOSDFlag to keep it up and make client operations wait
// for their own timeouts (rados_osd_op_timeout) or be reported as SLOW_OPS
// (after osd_op_complaint_time, 30 seconds by default). Host networking is
// supported.
//
// The container that runs this cluster's control CLI is refused because every
// fixture command would block on it (in a single-MON cluster that is MON a).
// A container that is already paused is refused rather than adopted, and
// only one pause per container is allowed. The cluster retains the handle and
// resumes it during Terminate, after network faults are restored and before
// containers are removed; resume explicitly before removing a caller-owned
// container.
func Pause(ctx context.Context, c *ceph.Container, ctr testcontainers.Container) (*PauseLease, error) {
	return pauseContainerInCluster(ctx, c, ctr, func(ctx context.Context) (containerPauseClient, error) {
		return testcontainers.NewDockerClientWithOpts(ctx)
	})
}

// Only the Docker connection is replaceable in unit tests; admission, locks,
// registry ownership and Terminate use the cluster's actual implementation.
func pauseContainerInCluster(ctx context.Context, c *ceph.Container, ctr testcontainers.Container, connect func(context.Context) (containerPauseClient, error)) (pause *PauseLease, err error) {
	err = faulthook.WithTopology(ctx, c, func() error {
		if faulthook.Closed(c) || ctr == nil || ctr.GetContainerID() == "" {
			return errors.New("container pause requires a live cluster and a started container")
		}
		id := ctr.GetContainerID()
		control, err := faulthook.Control(ctx, c)
		if err != nil {
			return err
		}
		if control != nil && control.GetContainerID() == id {
			return errors.New("pausing the control CLI container would block every fixture command")
		}
		// An incomplete previous pause is returned with the refusal; a complete
		// one stays registered until this one replaces it.
		token, previous, err := faulthook.Reserve(ctx, c, faulthook.ContainerPause, id)
		if err != nil {
			pause, _ = previous.(*PauseLease)
			return err
		}
		docker, err := connect(ctx)
		if err == nil {
			pause, err = pauseContainer(ctx, docker, id)
		}
		held := faulthook.Held{}
		if pause != nil {
			held = faulthook.Held{Handle: pause, Undo: pause.Resume, Complete: pause.isComplete}
		}
		_, settleErr := faulthook.Settle(c, token, held)
		return errors.Join(err, settleErr)
	})
	return pause, err
}

func pauseContainer(ctx context.Context, docker containerPauseClient, id string) (*PauseLease, error) {
	inspection, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
	if err != nil {
		return nil, errors.Join(fmt.Errorf("inspect container before pause: %w", err), docker.Close())
	}
	state := inspection.Container.State
	if state == nil || !state.Running || state.Paused || state.Restarting {
		return nil, errors.Join(errors.New("only a running, unpaused container can be paused"), docker.Close())
	}
	pause := &PauseLease{ContainerID: id, docker: docker}
	// Return ownership even after an uncertain transport outcome.
	if _, err := docker.ContainerPause(ctx, id, mobycl.ContainerPauseOptions{}); err != nil {
		return pause, fmt.Errorf("pause container: %w", err)
	}
	return pause, nil
}

func (pause *PauseLease) isComplete(ctx context.Context) (bool, error) {
	if err := lockMutex(ctx, &pause.mu); err != nil {
		return false, err
	}
	defer pause.mu.Unlock()
	return pause.complete, nil
}

// Resume thaws the container. A removed, stopped or already unpaused container
// needs no further action, so it completes the handle without starting it.
func (pause *PauseLease) Resume(ctx context.Context) error {
	if pause == nil {
		return errors.New("container pause is unavailable")
	}
	if err := lockMutex(ctx, &pause.mu); err != nil {
		return err
	}
	defer pause.mu.Unlock()
	if pause.complete {
		return nil
	}
	inspection, err := pause.docker.ContainerInspect(ctx, pause.ContainerID, mobycl.ContainerInspectOptions{})
	if err != nil && !faulthook.OnlyMissingHostResource(err) {
		return fmt.Errorf("inspect paused container: %w", err)
	}
	if err == nil && inspection.Container.State != nil && inspection.Container.State.Paused {
		if _, err := pause.docker.ContainerUnpause(ctx, pause.ContainerID, mobycl.ContainerUnpauseOptions{}); err != nil && !faulthook.OnlyMissingHostResource(err) {
			return fmt.Errorf("resume paused container: %w", err)
		}
	}
	if err := pause.docker.Close(); err != nil {
		return fmt.Errorf("close pause Docker client: %w", err)
	}
	pause.complete = true
	return nil
}
