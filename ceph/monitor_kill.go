package ceph

import (
	"context"
	"errors"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// killMonitor ends the MON process of ctr before the MON leaves the native
// membership (retireMonitor). The caller holds c.mu.
func (c *Container) killMonitor(ctx context.Context, ctr testcontainers.Container) error {
	if c.monitorKiller != nil {
		return c.monitorKiller(ctx, ctr)
	}
	return killMonitorProcess(ctx, ctr)
}

// killMonitorProcess sends SIGKILL to the container's process and returns once
// Docker reports that it no longer runs. A graceful stop is not enough: a MON
// that handles SIGTERM drops its client sessions and keeps answering
// authentication until its messenger shuts down, and a dropped client that
// authenticates again in that window keeps the gone MON as its session. A
// container that is gone or no longer runs counts as ended.
func killMonitorProcess(ctx context.Context, ctr testcontainers.Container) error {
	id := ctr.GetContainerID()
	if id == "" {
		return nil
	}
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = docker.Close() }()
	if _, err := docker.ContainerKill(ctx, id, mobycl.ContainerKillOptions{Signal: "KILL"}); err != nil && !errdefs.IsNotFound(err) && !errdefs.IsConflict(err) {
		return err
	}
	wait := docker.ContainerWait(ctx, id, mobycl.ContainerWaitOptions{Condition: container.WaitConditionNotRunning})
	select {
	case <-wait.Result:
		return nil
	case err := <-wait.Error:
		if errdefs.IsNotFound(err) {
			return nil
		}
		return err
	case <-ctx.Done():
		return errors.Join(ctx.Err(), errors.New("the killed monitor process was not confirmed to have ended"))
	}
}
