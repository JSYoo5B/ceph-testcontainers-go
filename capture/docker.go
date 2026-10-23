package capture

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// targetState is the Docker state of a capture target and its endpoint.
type targetState struct {
	running, paused bool
	networkMode     container.NetworkMode
	sandboxID       string
	startedAt       string
	driver          string
	mac             string
}

// sameGeneration reports the same running instance in the same sandbox.
func (t targetState) sameGeneration(current targetState) bool {
	return current.running && !current.paused && current.sandboxID == t.sandboxID && current.startedAt == t.startedAt && current.mac == t.mac
}

// helperState is the Docker state of a helper container.
type helperState struct {
	running    bool
	exitCode   int
	finishedAt time.Time
}

// backend runs the Docker operations of a capture; tests replace it.
type backend interface {
	// target inspects id and its endpoint on network. A removed container
	// returns an error for which errdefs.IsNotFound is true.
	target(ctx context.Context, id, network string) (targetState, error)
	// probe runs argv in a helper from image in networkMode until it exits,
	// and removes it.
	probe(ctx context.Context, image, networkMode string, argv []string) (probeResult, error)
	platform(ctx context.Context, imageID string) (string, error)
	// run starts argv as a helper's process from image in networkMode. A
	// non-empty ID returned with an error must be removed.
	run(ctx context.Context, image, networkMode string, argv []string) (string, error)
	state(ctx context.Context, id string) (helperState, error)
	logs(ctx context.Context, id string, limit int) ([]byte, []byte, error)
	exists(ctx context.Context, id, path string) (bool, error)
	signal(ctx context.Context, id, signal string) error
	// copy returns the content of the regular file at path.
	copy(ctx context.Context, id, path string) (io.ReadCloser, error)
	remove(ctx context.Context, id string) error
}

func classifyDocker(ctx context.Context, err error) error { return faulthook.ClassifyDocker(ctx, err) }

type dockerBackend struct{}

func withDocker[T any](ctx context.Context, do func(*testcontainers.DockerClient) (T, error)) (T, error) {
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		var zero T
		return zero, err
	}
	result, err := do(docker)
	return result, errors.Join(err, docker.Close())
}

func (dockerBackend) target(ctx context.Context, id, network string) (targetState, error) {
	return withDocker(ctx, func(docker *testcontainers.DockerClient) (targetState, error) {
		inspection, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
		if err != nil {
			return targetState{}, err
		}
		var state targetState
		if current := inspection.Container.State; current != nil {
			state.running, state.paused = current.Running && !current.Restarting, current.Paused
			state.startedAt = current.StartedAt
		}
		if inspection.Container.HostConfig != nil {
			state.networkMode = inspection.Container.HostConfig.NetworkMode
		}
		if settings := inspection.Container.NetworkSettings; settings != nil {
			state.sandboxID = settings.SandboxID
			if endpoint := settings.Networks[network]; endpoint != nil {
				state.mac = endpoint.MacAddress.String()
				network, err := docker.NetworkInspect(ctx, network, mobycl.NetworkInspectOptions{})
				if err != nil {
					return targetState{}, fmt.Errorf("inspect network: %w", err)
				}
				state.driver = network.Network.Driver
			}
		}
		return state, nil
	})
}

// helperOptions run argv as the helper's process in networkMode, as the
// image's own user, with NET_RAW as its only capability and without
// no-new-privileges, which would discard tcpdump's file capability.
func helperOptions(networkMode string, argv []string) []testcontainers.ContainerCustomizer {
	return []testcontainers.ContainerCustomizer{
		testcontainers.WithEntrypoint(argv[0]),
		testcontainers.WithCmd(argv[1:]...),
		testcontainers.WithHostConfigModifier(func(host *container.HostConfig) {
			host.NetworkMode = container.NetworkMode(networkMode)
			host.CapDrop = []string{"ALL"}
			host.CapAdd = []string{"NET_RAW"}
		}),
		testcontainers.WithLabels(map[string]string{"org.testcontainers.ceph.role": "capture-helper"}),
	}
}

func (dockerBackend) run(ctx context.Context, image, networkMode string, argv []string) (string, error) {
	helper, err := testcontainers.Run(ctx, image, helperOptions(networkMode, argv)...)
	if helper == nil {
		return "", err
	}
	return helper.GetContainerID(), err
}

func (b dockerBackend) probe(ctx context.Context, image, networkMode string, argv []string) (probeResult, error) {
	id, err := b.run(ctx, image, networkMode, argv)
	if err != nil {
		return probeResult{}, errors.Join(err, b.removeFresh(ctx, id))
	}
	result, err := func() (probeResult, error) {
		probeCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		for {
			state, err := b.state(probeCtx, id)
			if err != nil {
				return probeResult{}, err
			}
			if !state.running {
				var result probeResult
				result.exitCode = state.exitCode
				result.stdout, result.stderr, err = b.logs(probeCtx, id, 1<<20)
				if err != nil {
					return result, err
				}
				result.imageID, err = withDocker(probeCtx, func(docker *testcontainers.DockerClient) (string, error) {
					inspection, err := docker.ContainerInspect(probeCtx, id, mobycl.ContainerInspectOptions{})
					return inspection.Container.Image, err
				})
				return result, err
			}
			select {
			case <-probeCtx.Done():
				return probeResult{}, fmt.Errorf("the capture probe did not exit: %w", probeCtx.Err())
			case <-time.After(pollInterval):
			}
		}
	}()
	return result, errors.Join(err, b.removeFresh(ctx, id))
}

// removeFresh removes a probe also after ctx ended.
func (b dockerBackend) removeFresh(ctx context.Context, id string) error {
	if id == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finishTimeout)
	defer cancel()
	return b.remove(ctx, id)
}

func (dockerBackend) platform(ctx context.Context, imageID string) (string, error) {
	return withDocker(ctx, func(docker *testcontainers.DockerClient) (string, error) {
		inspection, err := docker.ImageInspect(ctx, imageID)
		if err != nil {
			return "", err
		}
		platform := inspection.Os + "/" + inspection.Architecture
		if inspection.Variant != "" {
			platform += "/" + inspection.Variant
		}
		return platform, nil
	})
}

func (dockerBackend) state(ctx context.Context, id string) (helperState, error) {
	return withDocker(ctx, func(docker *testcontainers.DockerClient) (helperState, error) {
		inspection, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
		if err != nil {
			return helperState{}, err
		}
		current := inspection.Container.State
		if current == nil {
			return helperState{}, errors.New("the helper has no state")
		}
		state := helperState{running: current.Running || current.Restarting, exitCode: current.ExitCode}
		if finished, err := time.Parse(time.RFC3339Nano, current.FinishedAt); err == nil && !state.running {
			state.finishedAt = finished.UTC()
		}
		return state, nil
	})
}

// cappedBuffer keeps the first limit bytes written to it.
type cappedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if room := b.limit - b.Len(); room > 0 {
		b.Buffer.Write(p[:min(room, len(p))])
	}
	return len(p), nil
}

func (dockerBackend) logs(ctx context.Context, id string, limit int) ([]byte, []byte, error) {
	type output struct{ stdout, stderr []byte }
	result, err := withDocker(ctx, func(docker *testcontainers.DockerClient) (output, error) {
		stream, err := docker.ContainerLogs(ctx, id, mobycl.ContainerLogsOptions{ShowStdout: true, ShowStderr: true})
		if err != nil {
			return output{}, err
		}
		stdout, stderr := &cappedBuffer{limit: limit}, &cappedBuffer{limit: limit}
		_, err = stdcopy.StdCopy(stdout, stderr, stream)
		return output{stdout.Bytes(), stderr.Bytes()}, errors.Join(err, stream.Close())
	})
	return result.stdout, result.stderr, err
}

func (dockerBackend) exists(ctx context.Context, id, path string) (bool, error) {
	return withDocker(ctx, func(docker *testcontainers.DockerClient) (bool, error) {
		stat, err := docker.ContainerStatPath(ctx, id, mobycl.ContainerStatPathOptions{Path: path})
		if err != nil {
			if errdefs.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		return stat.Stat.Mode.IsRegular(), nil
	})
}

func (dockerBackend) signal(ctx context.Context, id, signal string) error {
	_, err := withDocker(ctx, func(docker *testcontainers.DockerClient) (struct{}, error) {
		_, err := docker.ContainerKill(ctx, id, mobycl.ContainerKillOptions{Signal: signal})
		return struct{}{}, err
	})
	return err
}

// tarFile is the single regular file of a copied archive.
type tarFile struct {
	io.Reader
	closers []io.Closer
}

func (f tarFile) Close() error {
	var errs []error
	for _, closer := range f.closers {
		errs = append(errs, closer.Close())
	}
	return errors.Join(errs...)
}

func (dockerBackend) copy(ctx context.Context, id, path string) (io.ReadCloser, error) {
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return nil, err
	}
	result, err := docker.CopyFromContainer(ctx, id, mobycl.CopyFromContainerOptions{SourcePath: path})
	if err != nil {
		return nil, errors.Join(err, docker.Close())
	}
	archive := tar.NewReader(result.Content)
	header, err := archive.Next()
	if err == nil && header.Typeflag != tar.TypeReg {
		err = fmt.Errorf("the capture file is not a regular file")
	}
	if err != nil {
		return nil, errors.Join(err, result.Content.Close(), docker.Close())
	}
	return tarFile{Reader: archive, closers: []io.Closer{result.Content, docker}}, nil
}

func (dockerBackend) remove(ctx context.Context, id string) error {
	_, err := withDocker(ctx, func(docker *testcontainers.DockerClient) (struct{}, error) {
		_, err := docker.ContainerRemove(ctx, id, mobycl.ContainerRemoveOptions{Force: true, RemoveVolumes: true})
		return struct{}{}, err
	})
	return err
}
