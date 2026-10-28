package fault

import (
	"context"
	"errors"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

type fakePauseDocker struct {
	state                          *container.State
	inspectErr, pauseErr, thawErr  error
	inspects, pauses, thaws, close int
	pauseOnError, thawOnError      bool
	onPause                        func()
	closeErr                       error
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
	if d.pauseErr == nil || d.pauseOnError {
		d.state.Paused = true
	}
	if d.onPause != nil {
		d.onPause()
	}
	return mobycl.ContainerPauseResult{}, d.pauseErr
}

func (d *fakePauseDocker) ContainerUnpause(context.Context, string, mobycl.ContainerUnpauseOptions) (mobycl.ContainerUnpauseResult, error) {
	d.thaws++
	if d.thawErr == nil || d.thawOnError {
		d.state.Paused = false
	}
	return mobycl.ContainerUnpauseResult{}, d.thawErr
}

func (d *fakePauseDocker) Close() error { d.close++; return d.closeErr }

func TestPauseFreezesOnlyRunningUnpausedContainers(t *testing.T) {
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

func TestPauseLeaseUncertainOutcomesRemainRetryable(t *testing.T) {
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

func TestPauseRefusesControlAndUnavailableContainers(t *testing.T) {
	control := pauseTestContainer{id: "control"}
	cluster := testhelper.TestCluster(control, testhelper.TestConfig{StartupTimeout: time.Second})
	for name, target := range map[string]testcontainers.Container{"nil": nil, "unstarted": pauseTestContainer{}, "control": control} {
		if pause, err := Pause(t.Context(), cluster, target); err == nil || pause != nil {
			t.Fatalf("%s container accepted: %v", name, err)
		}
	}
	testhelper.TestSetClosed(cluster, true)
	if pause, err := Pause(t.Context(), cluster, pauseTestContainer{id: "osd"}); err == nil || pause != nil {
		t.Fatal("terminated cluster accepted a pause")
	}
	var missing *PauseLease
	if err := missing.Resume(t.Context()); err == nil {
		t.Fatal("nil pause handle resumed")
	}
}

type pauseTestContainer struct {
	testcontainers.Container
	id           string
	docker       *fakePauseDocker
	terminations *int
}

func (ctr pauseTestContainer) GetContainerID() string { return ctr.id }
func (ctr pauseTestContainer) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	if ctr.docker != nil && ctr.docker.state.Paused {
		return errors.New("container still frozen during teardown")
	}
	if ctr.terminations != nil {
		(*ctr.terminations)++
	}
	return nil
}

func pauseCluster() *ceph.Container {
	return testhelper.TestCluster(pauseTestContainer{id: "control"}, testhelper.TestConfig{StartupTimeout: time.Second})
}

func trackedPause(c *ceph.Container, id string) (held any) {
	faulthook.WithTopologyNoContext(c, func() { held = testhelper.TestFaults(c, faulthook.ContainerPause)[id] })
	return held
}

func pauseWithDocker(ctx context.Context, c *ceph.Container, d *fakePauseDocker) (*PauseLease, error) {
	return pauseContainerInCluster(ctx, c, pauseTestContainer{id: "osd"}, func(context.Context) (containerPauseClient, error) { return d, nil })
}

func TestClusterPauseTracksCancelledApplyAndRetriesTeardown(t *testing.T) {
	c := pauseCluster()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	d := &fakePauseDocker{state: &container.State{Running: true}, pauseErr: context.Canceled, pauseOnError: true, onPause: cancel}
	terminated := 0
	testhelper.TestReplaceOSDs(c, map[int]*ceph.OSDContainer{0: {Container: pauseTestContainer{id: "osd", docker: d, terminations: &terminated}}})
	pause, err := pauseWithDocker(ctx, c, d)
	if !errors.Is(err, context.Canceled) || pause == nil || trackedPause(c, "osd") != pause || !d.state.Paused {
		t.Fatalf("cancelled apply lost ownership: %v", err)
	}
	// Overlap returns the same uncertain handle without making another request.
	again, err := pauseWithDocker(t.Context(), c, d)
	if err == nil || again != pause || d.pauses != 1 {
		t.Fatalf("overlap replaced the live handle: %v", err)
	}
	d.thawErr = errors.New("unpause unavailable")
	if err := c.Terminate(t.Context()); !errors.Is(err, d.thawErr) || pause.complete || trackedPause(c, "osd") != pause || terminated != 0 || d.close != 0 {
		t.Fatalf("failed thaw lost retryability: %v", err)
	}
	d.thawErr = nil
	if err := c.Terminate(t.Context()); err != nil || !pause.complete || d.state.Paused || terminated != 1 || d.close != 1 {
		t.Fatalf("teardown did not thaw before terminating: %v", err)
	}
	if err := pause.Resume(t.Context()); err != nil || d.thaws != 2 || d.close != 1 {
		t.Fatalf("completed resume repeated Docker work: %v", err)
	}
}

func TestPauseFailedReservationKeepsCompletedPredecessor(t *testing.T) {
	for _, phase := range []string{"connect", "inspect"} {
		t.Run(phase, func(t *testing.T) {
			c := pauseCluster()
			firstDocker := &fakePauseDocker{state: &container.State{Running: true}}
			first, err := pauseWithDocker(t.Context(), c, firstDocker)
			if err != nil {
				t.Fatal(err)
			}
			if err := first.Resume(t.Context()); err != nil {
				t.Fatal(err)
			}
			unavailable := errors.New("Docker unavailable")
			failedDocker := &fakePauseDocker{inspectErr: unavailable}
			failed, err := pauseContainerInCluster(t.Context(), c, pauseTestContainer{id: "osd"}, func(context.Context) (containerPauseClient, error) {
				if phase == "connect" {
					return nil, unavailable
				}
				return failedDocker, nil
			})
			if !errors.Is(err, unavailable) || failed != nil || trackedPause(c, "osd") != first {
				t.Fatalf("failed reservation lost completed predecessor: %v", err)
			}
			freshDocker := &fakePauseDocker{state: &container.State{Running: true}}
			fresh, err := pauseWithDocker(t.Context(), c, freshDocker)
			if err != nil || fresh == nil || fresh == first || trackedPause(c, "osd") != fresh {
				t.Fatalf("new pause did not replace completed predecessor: %v", err)
			}
			if err := first.Resume(t.Context()); err != nil || !freshDocker.state.Paused {
				t.Fatalf("old handle touched its successor: %v", err)
			}
			if err := c.Terminate(t.Context()); err != nil || freshDocker.state.Paused {
				t.Fatalf("new handle not retained for cleanup: %v", err)
			}
		})
	}
}

func TestPauseResumeReconcilesUncertainThawAndClose(t *testing.T) {
	d := &fakePauseDocker{state: &container.State{Running: true}}
	pause, err := pauseContainer(t.Context(), d, "osd")
	if err != nil {
		t.Fatal(err)
	}
	d.thawErr, d.thawOnError = errors.New("lost unpause reply"), true
	if err := pause.Resume(t.Context()); !errors.Is(err, d.thawErr) || pause.complete || d.close != 0 {
		t.Fatalf("uncertain thaw discarded ownership: %v", err)
	}
	d.closeErr = errors.New("close unavailable")
	if err := pause.Resume(t.Context()); !errors.Is(err, d.closeErr) || pause.complete || d.thaws != 1 {
		t.Fatalf("close failure repeated the thaw: %v", err)
	}
	d.closeErr = nil
	if err := pause.Resume(t.Context()); err != nil || !pause.complete || d.thaws != 1 || d.close != 2 {
		t.Fatalf("close retry did not reconcile thawed state: %v", err)
	}
}

func TestPauseQueuedTopologyAndHandleDeadlines(t *testing.T) {
	c := pauseCluster()
	view := testhelper.TestState(c)
	// The public call must return before opening a Docker connection.
	assertContextLockQueued(t, view.Mutex, func(ctx context.Context) error {
		_, err := Pause(ctx, c, pauseTestContainer{id: "osd"})
		return err
	})
	d := &fakePauseDocker{state: &container.State{Running: true}}
	pause, err := pauseWithDocker(t.Context(), c, d)
	if err != nil {
		t.Fatal(err)
	}
	// Overlap takes the handle gate under the cluster gate, retaining the
	// original handle even when waiting for its completion state expires.
	assertContextLockQueued(t, &pause.mu, func(ctx context.Context) error {
		previous, err := pauseWithDocker(ctx, c, d)
		if previous != pause {
			t.Error("queued overlap lost the original pause")
		}
		return err
	})
	assertContextLockQueued(t, &pause.mu, pause.Resume)
	if d.thaws != 0 || d.close != 0 || !d.state.Paused {
		t.Fatal("queued resume changed Docker or ownership")
	}
	if err := c.Terminate(t.Context()); err != nil || !pause.complete || d.state.Paused {
		t.Fatalf("fresh context could not thaw after deadlines: %v", err)
	}
}
