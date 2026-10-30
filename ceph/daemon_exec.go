package ceph

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/containerd/errdefs"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// daemonExecutor runs a command in a daemon container through the Docker exec
// API, whose end, unlike a testcontainers exec, can be confirmed: RotateKey
// places keyring files with it. Its only field is set before the cluster is
// shared and never changes, so the executor has no lock and adds nothing to
// its callers' lock contract. Recording an exec that may still run is the
// caller's: run returns its ID.
type daemonExecutor struct {
	// docker replaces the Docker client in tests; nil uses the engine.
	docker func(context.Context) (keyRotationExecClient, error)
}

// keyRotationExecClient is the part of the Docker client that writes keyring
// files in a daemon container.
type keyRotationExecClient interface {
	ExecCreate(context.Context, string, mobycl.ExecCreateOptions) (mobycl.ExecCreateResult, error)
	ExecStart(context.Context, string, mobycl.ExecStartOptions) (mobycl.ExecStartResult, error)
	ExecInspect(context.Context, string, mobycl.ExecInspectOptions) (mobycl.ExecInspectResult, error)
	Close() error
}

// client returns the Docker client of one exec sequence.
func (e daemonExecutor) client(ctx context.Context) (keyRotationExecClient, error) {
	if e.docker != nil {
		return e.docker(ctx)
	}
	return testcontainers.NewDockerClientWithOpts(ctx)
}

// run runs argv in ctr and returns its exit code once it ended. When Docker
// created the exec but its end cannot be confirmed, run returns the exec's ID
// as unended with the error; the caller records that it may still run.
func (e daemonExecutor) run(ctx context.Context, ctr testcontainers.Container, argv ...string) (code int, unended string, err error) {
	docker, err := e.client(ctx)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = docker.Close() }()
	// An exec that Docker creates after this request failed never starts:
	// nothing else knows its ID.
	created, err := docker.ExecCreate(ctx, ctr.GetContainerID(), mobycl.ExecCreateOptions{Cmd: argv})
	if err != nil {
		return 0, "", err
	}
	_, err = docker.ExecStart(ctx, created.ID, mobycl.ExecStartOptions{Detach: true})
	if err == nil {
		var inspect mobycl.ExecInspectResult
		if inspect, err = waitDaemonExec(ctx, docker, created.ID); err == nil {
			return inspect.ExitCode, "", nil
		}
	}
	// The start request may still reach Docker, or the exec may still run.
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), keyRotationCleanupTimeout)
	defer cancel()
	if finishErr := e.finish(cleanupCtx, created.ID); finishErr != nil {
		return 0, created.ID, errors.Join(err, fmt.Errorf("the exec may still run: %w", finishErr))
	}
	return 0, "", fmt.Errorf("%w; the exec has ended, so it may or may not have applied", err)
}

// finish returns once the exec has ended and can no longer start. It
// starts an exec that Docker has not started, so that a start request still on
// its way finds it done. Docker also refuses to start an exec in a paused
// container, so a refusal proves nothing until the exec shows a process.
func (e daemonExecutor) finish(ctx context.Context, execID string) error {
	docker, err := e.client(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = docker.Close() }()
	if _, err := docker.ExecStart(ctx, execID, mobycl.ExecStartOptions{Detach: true}); err != nil {
		switch {
		case errdefs.IsNotFound(err):
			// Docker drops the execs of a container that stopped.
			return nil
		case !errdefs.IsConflict(err):
			return err
		}
	}
	_, err = waitDaemonExec(ctx, docker, execID)
	if errdefs.IsNotFound(err) {
		return nil
	}
	return err
}

// waitDaemonExec returns once the exec has had a process and it ended.
func waitDaemonExec(ctx context.Context, docker keyRotationExecClient, execID string) (mobycl.ExecInspectResult, error) {
	for {
		inspect, err := docker.ExecInspect(ctx, execID, mobycl.ExecInspectOptions{})
		if err != nil || (!inspect.Running && inspect.PID != 0) {
			return inspect, err
		}
		select {
		case <-ctx.Done():
			return inspect, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}
