package monstore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	mobycl "github.com/moby/moby/client"
)

// This file is the Docker translation of the monStoreInspectEngine port of
// inspect_container.go: one request becomes one create call, and the
// start, wait, log and remove calls follow. It holds no lock and no state
// between calls (so it adds no entry to the lock order of
// docs/internals/mon-volume-storage.md): every call opens a client through the
// injected constructor and closes it before it returns.
//
// What is checked Docker-free is the translation (the create options, the
// forced removal that never takes a volume, the demultiplexing and bounding of
// the log stream, the exit code that a context end cannot forge). What is not
// established: no Docker daemon ran any of it, so whether the daemon accepts
// the create options, whether the image carries python3 and runs as the given
// owner, and how the log stream ends on a daemon are native questions. Nothing
// constructs this engine in production, and there is no default client: the
// first caller supplies the constructor.

// monStoreInspectDocker is the Docker surface of the inspector engine, the
// calls of one inspection.
type monStoreInspectDocker interface {
	ContainerCreate(context.Context, mobycl.ContainerCreateOptions) (mobycl.ContainerCreateResult, error)
	ContainerStart(context.Context, string, mobycl.ContainerStartOptions) (mobycl.ContainerStartResult, error)
	ContainerWait(context.Context, string, mobycl.ContainerWaitOptions) mobycl.ContainerWaitResult
	ContainerLogs(context.Context, string, mobycl.ContainerLogsOptions) (mobycl.ContainerLogsResult, error)
	ContainerRemove(context.Context, string, mobycl.ContainerRemoveOptions) (mobycl.ContainerRemoveResult, error)
	Close() error
}

// monStoreMobyEngine implements monStoreInspectEngine on the moby client.
type monStoreMobyEngine struct {
	// newClient opens the Docker client of one call. There is no default.
	newClient func(context.Context) (monStoreInspectDocker, error)
}

var _ monStoreInspectEngine = monStoreMobyEngine{}

// with runs fn on a fresh client and closes the client afterwards; a close
// failure is reported next to fn's error.
func (e monStoreMobyEngine) with(ctx context.Context, fn func(monStoreInspectDocker) error) (err error) {
	if e.newClient == nil {
		return errors.New("the MON store inspector engine has no Docker client constructor")
	}
	docker, err := e.newClient(ctx)
	if err != nil {
		return fmt.Errorf("open the Docker client: %w", err)
	}
	if isNilPort(docker) {
		return errors.New("the MON store inspector engine got a nil Docker client")
	}
	defer func() { err = errors.Join(err, docker.Close()) }()
	return fn(docker)
}

// monStoreInspectCreateOptions translates a validated request into the create
// call. It copies the request's slices and map, so the options own their data.
func monStoreInspectCreateOptions(req monStoreInspectRequest) mobycl.ContainerCreateOptions {
	labels := make(map[string]string, len(req.labels))
	for k, v := range req.labels {
		labels[k] = v
	}
	var security []string
	if req.noNewPrivileges {
		security = []string{"no-new-privileges:true"}
	}
	return mobycl.ContainerCreateOptions{
		Config: &container.Config{
			Image:      req.image,
			User:       req.user,
			Entrypoint: append([]string(nil), req.entrypoint...),
			Cmd:        append([]string(nil), req.cmd...),
			Labels:     labels,
			// No terminal: the log stream is then always multiplexed, which
			// Logs depends on.
			Tty: false,
		},
		HostConfig: &container.HostConfig{
			// A named volume with NoCopy, so that neither the image's content
			// nor anything else is copied into the store on mount.
			Mounts: []mount.Mount{{
				Type:          mount.TypeVolume,
				Source:        req.volume,
				Target:        req.target,
				ReadOnly:      req.readOnlyMount,
				VolumeOptions: &mount.VolumeOptions{NoCopy: true},
			}},
			NetworkMode:    container.NetworkMode(req.networkMode),
			CapDrop:        append([]string(nil), req.capDrop...),
			SecurityOpt:    security,
			ReadonlyRootfs: req.readOnlyRootfs,
			Privileged:     req.privileged,
			AutoRemove:     req.autoRemove,
		},
	}
}

// Create validates the request, then creates the container without starting
// it. Any error leaves the outcome unknown: a lost reply can leave a container
// that the cluster's labelled-container scan finds, so Create never retries and
// never removes by name.
func (e monStoreMobyEngine) Create(ctx context.Context, req monStoreInspectRequest) (id string, err error) {
	if err := req.validate(); err != nil {
		return "", err
	}
	err = e.with(ctx, func(docker monStoreInspectDocker) error {
		created, err := docker.ContainerCreate(ctx, monStoreInspectCreateOptions(req))
		if err != nil {
			return fmt.Errorf("create the inspector container: %w", err)
		}
		id = created.ID
		return nil
	})
	return id, err
}

// Start starts the created container.
func (e monStoreMobyEngine) Start(ctx context.Context, id string) error {
	return e.with(ctx, func(docker monStoreInspectDocker) error {
		if _, err := docker.ContainerStart(ctx, id, mobycl.ContainerStartOptions{}); err != nil {
			return fmt.Errorf("start the inspector container: %w", err)
		}
		return nil
	})
}

// Wait returns the exit code of the stopped container. A context end, a failed
// wait request, a wait response that carries an error and a closed channel are
// errors; none of them is reported as an exit code.
func (e monStoreMobyEngine) Wait(ctx context.Context, id string) (code int64, err error) {
	err = e.with(ctx, func(docker monStoreInspectDocker) error {
		waited := docker.ContainerWait(ctx, id, mobycl.ContainerWaitOptions{Condition: container.WaitConditionNotRunning})
		select {
		case res, ok := <-waited.Result:
			if !ok {
				return errors.New("the wait reply ended without an exit status")
			}
			if res.Error != nil {
				return fmt.Errorf("the wait reply carries an error: %s", res.Error.Message)
			}
			code = res.StatusCode
			return nil
		case werr, ok := <-waited.Error:
			if !ok || werr == nil {
				return errors.New("the wait reply ended without an exit status")
			}
			return fmt.Errorf("wait for the inspector container: %w", werr)
		case <-ctx.Done():
			return fmt.Errorf("wait for the inspector container: %w", ctx.Err())
		}
	})
	if err != nil {
		return 0, err
	}
	return code, nil
}

// Logs reads the output of the stopped container, at most limit bytes of each
// stream. A stream with more than limit bytes sets truncated and reading stops,
// so the result never holds a prefix presented as the whole. A stream that ends
// inside a frame, a frame that is neither stdout nor stderr and a context end
// are errors.
func (e monStoreMobyEngine) Logs(ctx context.Context, id string, limit int) (logs monStoreInspectLogs, err error) {
	if limit <= 0 {
		return monStoreInspectLogs{}, errors.New("the log limit must be positive")
	}
	err = e.with(ctx, func(docker monStoreInspectDocker) error {
		reader, err := docker.ContainerLogs(ctx, id, mobycl.ContainerLogsOptions{ShowStdout: true, ShowStderr: true})
		if err != nil {
			return fmt.Errorf("read the inspector container logs: %w", err)
		}
		defer func() { _ = reader.Close() }()
		stop := context.AfterFunc(ctx, func() { _ = reader.Close() })
		defer stop()
		logs, err = readMonStoreInspectFrames(reader, limit)
		return errors.Join(err, ctx.Err())
	})
	if err != nil {
		return monStoreInspectLogs{}, err
	}
	return logs, nil
}

// readMonStoreInspectFrames demultiplexes a Docker log stream into at most
// limit bytes per stream. It never allocates from a frame's advertised size:
// each payload is copied through a fixed buffer.
func readMonStoreInspectFrames(r io.Reader, limit int) (monStoreInspectLogs, error) {
	var logs monStoreInspectLogs
	header := make([]byte, 8)
	chunk := make([]byte, 32<<10)
	for {
		if _, err := io.ReadFull(r, header); err != nil {
			if err == io.EOF {
				return logs, nil
			}
			return monStoreInspectLogs{}, fmt.Errorf("read a log frame header: %w", err)
		}
		if (header[0] != 1 && header[0] != 2) || header[1] != 0 || header[2] != 0 || header[3] != 0 {
			return monStoreInspectLogs{}, errors.New("invalid Docker log frame")
		}
		dst := &logs.stdout
		if header[0] == 2 {
			dst = &logs.stderr
		}
		for left := int64(binary.BigEndian.Uint32(header[4:])); left > 0; {
			n, err := r.Read(chunk[:min(int64(len(chunk)), left)])
			if n > 0 {
				left -= int64(n)
				if len(*dst)+n > limit {
					// Keep exactly the limit, drop the rest and stop.
					*dst = append(*dst, chunk[:limit-len(*dst)]...)
					logs.truncated = true
					return logs, nil
				}
				*dst = append(*dst, chunk[:n]...)
			}
			if err != nil {
				if err == io.EOF && left > 0 {
					err = io.ErrUnexpectedEOF
				}
				if err == io.EOF {
					break
				}
				return monStoreInspectLogs{}, fmt.Errorf("read a log frame: %w", err)
			}
		}
	}
}

// Remove removes the container, forced, and never takes a volume with it. A
// container that is already gone is not an error.
func (e monStoreMobyEngine) Remove(ctx context.Context, id string) error {
	return e.with(ctx, func(docker monStoreInspectDocker) error {
		_, err := docker.ContainerRemove(ctx, id, mobycl.ContainerRemoveOptions{Force: true})
		if err != nil && !errdefs.IsNotFound(err) {
			return fmt.Errorf("remove the inspector container: %w", err)
		}
		return nil
	})
}
