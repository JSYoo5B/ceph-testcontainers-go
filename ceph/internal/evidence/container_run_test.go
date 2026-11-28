package evidence

import (
	"context"
	"errors"
	"testing"

	"github.com/containerd/errdefs"
	dockercontainer "github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
)

// runInspector reports one container with the given log configuration, or
// err.
type runInspector struct {
	driver  string
	options map[string]string
	err     error
}

func (d runInspector) ContainerInspect(context.Context, string, mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error) {
	if d.err != nil {
		return mobycl.ContainerInspectResult{}, d.err
	}
	return mobycl.ContainerInspectResult{Container: dockercontainer.InspectResponse{Image: "sha256:image", RestartCount: 2,
		Config:     &dockercontainer.Config{Tty: true},
		State:      &dockercontainer.State{Status: "running", Running: true, StartedAt: "2026-11-27T04:00:00.5Z"},
		HostConfig: &dockercontainer.HostConfig{LogConfig: dockercontainer.LogConfig{Type: d.driver, Config: d.options}}}}, nil
}

// The log-coverage rule that both the diagnostics log reads and the
// finalize verification apply: only json-file without max-size keeps
// every line.
func TestContainerRunLogLoss(t *testing.T) {
	for name, test := range map[string]struct {
		driver   string
		options  map[string]string
		rotation string
		loses    bool
	}{
		"json-file":               {driver: "json-file"},
		"json-file with max-size": {driver: "json-file", options: map[string]string{"max-size": "10m"}, rotation: "10m", loses: true},
		"json-file with -1":       {driver: "json-file", options: map[string]string{"max-size": "-1"}},
		"json-file with max-file": {driver: "json-file", options: map[string]string{"max-file": "3"}},
		"local":                   {driver: "local", loses: true},
		"no driver reported":      {loses: true},
	} {
		run, err := InspectRun(t.Context(), runInspector{driver: test.driver, options: test.options}, "c")
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if run.LogRotation != test.rotation || (run.LogLoss() != "") != test.loses {
			t.Errorf("%s: rotation %q, loss %q", name, run.LogRotation, run.LogLoss())
		}
	}
}

func TestInspectRun(t *testing.T) {
	run, err := InspectRun(t.Context(), runInspector{driver: "json-file"}, "c")
	if err != nil || run.Image != "sha256:image" || run.RestartCount != 2 || !run.TTY || !run.Running || run.State != "running" ||
		run.StartedAt != "2026-11-27T04:00:00.5Z" || run.Started.Nanosecond() != 5e8 {
		t.Fatalf("run %+v, %v", run, err)
	}
	if _, err := InspectRun(t.Context(), runInspector{err: errdefs.ErrNotFound}, "c"); !errors.Is(err, ErrContainerRemoved) {
		t.Fatalf("removed container = %v", err)
	}
	boom := errors.New("daemon unreachable")
	if _, err := InspectRun(t.Context(), runInspector{err: boom}, "c"); !errors.Is(err, boom) {
		t.Fatalf("inspect error = %v", err)
	}
	if _, err := InspectRun(t.Context(), runInspector{}, ""); err == nil {
		t.Fatal("a container without an ID was inspected")
	}
}
