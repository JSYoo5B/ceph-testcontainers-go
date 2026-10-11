package ceph

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
)

type fakePauseDocker struct {
	state                          *container.State
	inspectErr, pauseErr, thawErr  error
	inspects, pauses, thaws, close int
}

func (d *fakePauseDocker) ContainerInspect(context.Context, string, mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error) {
	d.inspects++
	if d.inspectErr != nil {
		return mobycl.ContainerInspectResult{}, d.inspectErr
	}
	return mobycl.ContainerInspectResult{Container: container.InspectResponse{State: d.state}}, nil
}

func (d *fakePauseDocker) ContainerPause(context.Context, string, mobycl.ContainerPauseOptions) (mobycl.ContainerPauseResult, error) {
	d.pauses++
	if d.pauseErr == nil {
		d.state.Paused = true
	}
	return mobycl.ContainerPauseResult{}, d.pauseErr
}

func (d *fakePauseDocker) ContainerUnpause(context.Context, string, mobycl.ContainerUnpauseOptions) (mobycl.ContainerUnpauseResult, error) {
	d.thaws++
	if d.thawErr == nil {
		d.state.Paused = false
	}
	return mobycl.ContainerUnpauseResult{}, d.thawErr
}

func (d *fakePauseDocker) Close() error { d.close++; return nil }

func TestPauseContainerFreezesOnlyRunningUnpausedContainers(t *testing.T) {
	for name, state := range map[string]container.State{
		"paused":     {Running: true, Paused: true},
		"stopped":    {},
		"restarting": {Running: true, Restarting: true},
	} {
		docker := &fakePauseDocker{state: &state}
		if pause, err := pauseContainer(t.Context(), docker, "fixture"); err == nil || pause != nil || docker.pauses != 0 || docker.close != 1 {
			t.Fatalf("%s container was adopted: pause=%v err=%v calls=%+v", name, pause, err, docker)
		}
	}
	docker := &fakePauseDocker{state: &container.State{Running: true}}
	pause, err := pauseContainer(t.Context(), docker, "fixture")
	if err != nil || pause == nil || pause.ContainerID != "fixture" || docker.pauses != 1 || !docker.state.Paused || docker.close != 0 {
		t.Fatalf("running container was not paused: %v %+v", err, docker)
	}
	if err := pause.Resume(t.Context()); err != nil || docker.thaws != 1 || docker.state.Paused || docker.close != 1 {
		t.Fatalf("pause was not resumed: %v %+v", err, docker)
	}
	if err := pause.Resume(t.Context()); err != nil || docker.thaws != 1 || docker.inspects != 2 {
		t.Fatalf("repeated resume reached Docker: %v %+v", err, docker)
	}
}

func TestContainerPauseUncertainOutcomesRemainRetryable(t *testing.T) {
	// The pause request may have reached Docker before its reply was lost.
	docker := &fakePauseDocker{state: &container.State{Running: true, Paused: true}, pauseErr: errors.New("lost reply")}
	docker.state.Paused = false
	pause, err := pauseContainer(t.Context(), docker, "fixture")
	if err == nil || pause == nil {
		t.Fatalf("uncertain pause did not return ownership: %v", err)
	}
	docker.state.Paused = true
	docker.thawErr = errors.New("daemon unavailable")
	if err := pause.Resume(t.Context()); err == nil || docker.close != 0 {
		t.Fatalf("failed resume completed the handle: %v", err)
	}
	docker.thawErr = nil
	if err := pause.Resume(t.Context()); err != nil || docker.state.Paused || docker.close != 1 {
		t.Fatalf("retried resume failed: %v %+v", err, docker)
	}

	docker = &fakePauseDocker{state: &container.State{Running: true}}
	pause, _ = pauseContainer(t.Context(), docker, "fixture")
	docker.inspectErr = errors.New("inspect unavailable")
	if err := pause.Resume(t.Context()); err == nil || docker.thaws != 0 {
		t.Fatalf("resume ignored an unknown container state: %v", err)
	}
	// A removed container, or one thawed outside the fixture, needs nothing.
	docker.inspectErr = errdefs.ErrNotFound
	if err := pause.Resume(t.Context()); err != nil || docker.thaws != 0 || docker.close != 1 {
		t.Fatalf("removed container was not completed: %v %+v", err, docker)
	}
	docker = &fakePauseDocker{state: &container.State{Running: true}}
	pause, _ = pauseContainer(t.Context(), docker, "fixture")
	docker.state.Paused = false
	if err := pause.Resume(t.Context()); err != nil || docker.thaws != 0 {
		t.Fatalf("externally thawed container was unpaused again: %v %+v", err, docker)
	}
}

func TestPauseContainerRefusesControlAndUnavailableContainers(t *testing.T) {
	ctr := &poolFixtureContainer{}
	cluster := poolFixtureCluster(ctr, 1)
	if _, err := cluster.PauseContainer(t.Context(), nil); err == nil {
		t.Fatal("nil container was accepted")
	}
	controlCluster := &Container{Container: idContainer{ctr, "control"}, settings: options{startupTimeout: time.Second}}
	if pause, err := controlCluster.PauseContainer(t.Context(), idContainer{ctr, "control"}); err == nil || pause != nil {
		t.Fatal("control CLI container was paused")
	}
	controlCluster.closed = true
	if pause, err := controlCluster.PauseContainer(t.Context(), idContainer{ctr, "osd"}); err == nil || pause != nil {
		t.Fatal("terminated cluster accepted a pause")
	}
	var missing *ContainerPause
	if err := missing.Resume(t.Context()); err == nil {
		t.Fatal("nil pause handle resumed")
	}
}

type idContainer struct {
	*poolFixtureContainer
	id string
}

func (ctr idContainer) GetContainerID() string { return ctr.id }
