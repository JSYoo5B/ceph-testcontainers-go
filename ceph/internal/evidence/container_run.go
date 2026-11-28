package evidence

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/containerd/errdefs"
	mobycl "github.com/moby/moby/client"
)

// RunInspector is the part of the Docker client that inspects a container.
type RunInspector interface {
	ContainerInspect(context.Context, string, mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error)
}

// ErrContainerRemoved is the error of InspectRun for a container that the
// engine no longer has.
var ErrContainerRemoved = errors.New("the container was removed")

// ContainerRun is a container's current run as one inspection reports it:
// what identifies the run and how its log is recorded.
type ContainerRun struct {
	Image string
	// StartedAt is the start time as the engine reported it, and Started
	// its parsed value, zero when it does not parse. A container that was
	// created but never started reports the zero time.
	StartedAt    string
	Started      time.Time
	State        string
	Running      bool
	RestartCount int
	TTY          bool
	// LogDriver is the container's log driver, and LogRotation the
	// json-file max-size, "" when the driver keeps every line.
	LogDriver, LogRotation string
}

// InspectRun inspects container id. A removed container returns
// ErrContainerRemoved, and one whose start time the engine does not report
// an error.
func InspectRun(ctx context.Context, docker RunInspector, id string) (ContainerRun, error) {
	if id == "" {
		return ContainerRun{}, errors.New("the container was not created")
	}
	inspection, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
	switch {
	case errdefs.IsNotFound(err):
		return ContainerRun{}, ErrContainerRemoved
	case err != nil:
		return ContainerRun{}, err
	}
	ctr := inspection.Container
	run := ContainerRun{Image: ctr.Image, RestartCount: ctr.RestartCount}
	if ctr.State != nil {
		run.StartedAt, run.State, run.Running = ctr.State.StartedAt, string(ctr.State.Status), ctr.State.Running
	}
	if ctr.Config != nil {
		run.TTY = ctr.Config.Tty
	}
	if ctr.HostConfig != nil {
		run.LogDriver = ctr.HostConfig.LogConfig.Type
		if size := ctr.HostConfig.LogConfig.Config["max-size"]; size != "" && size != "-1" {
			run.LogRotation = size
		}
	}
	if run.StartedAt == "" {
		return ContainerRun{}, errors.New("the Docker engine reported no start time")
	}
	if started, err := time.Parse(time.RFC3339Nano, run.StartedAt); err == nil && started.Year() > 1 {
		run.Started = started.UTC()
	}
	return run, nil
}

// LogLoss says why the run's log driver can drop lines, so that a read of
// its log cannot show that a line was not logged, or returns "" for
// json-file without rotation, the one driver that keeps every line.
func (r ContainerRun) LogLoss() string {
	switch {
	case r.LogDriver != "json-file":
		return fmt.Sprintf("log driver %q is not covered; only json-file without rotation is", r.LogDriver)
	case r.LogRotation != "":
		return fmt.Sprintf("the json-file driver rotates at max-size %s and can drop lines", r.LogRotation)
	}
	return ""
}
