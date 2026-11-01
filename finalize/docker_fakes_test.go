package finalize

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/diagnostics"
	"github.com/containerd/errdefs"
	dockercontainer "github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// The fakes of this file stand for the Docker engine that the crash directory
// reader and the log reader of a verification call: the same shape as the
// collector fakes of package diagnostics, kept here because test files are
// not shared between packages.

// crashEntry is one entry of a fake crash directory archive.
type crashEntry struct {
	name, body, link string
	kind             byte
	// size, when set, is the header's size, to model a large file without
	// its bytes.
	size int64
}

// crashArchive builds the tar stream that Docker's archive API returns for
// /var/lib/ceph/crash: entries named below "crash".
func crashArchive(t *testing.T, entries ...crashEntry) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	for _, entry := range entries {
		header := &tar.Header{Name: entry.name, Mode: 0o644, Typeflag: entry.kind, Linkname: entry.link, ModTime: time.Unix(1, 0)}
		switch entry.kind {
		case 0:
			header.Typeflag = tar.TypeReg
			header.Size = int64(len(entry.body))
		case tar.TypeDir:
			header.Mode = 0o755
		}
		if entry.size > 0 {
			header.Size = entry.size
		}
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		body := []byte(entry.body)
		if entry.size > 0 {
			body = bytes.Repeat([]byte("x"), int(entry.size))
		}
		if _, err := writer.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

// fakeCrashContainer is one container as Docker sees it: whether it exists,
// its state, and its crash directory, or the error that reading fails with.
type fakeCrashContainer struct {
	removed bool
	// removedAfterInspect removes the container between the inspection and
	// the archive request.
	removedAfterInspect bool
	inspectErr          error
	// inspectErrAfter fails every inspection after that many.
	inspectErrAfter int
	inspections     int
	noCrashDir      bool
	archive         []byte
	copyErr         error
	state           dockercontainer.State
	copies          int
}

type fakeCrashDocker struct {
	containers map[string]*fakeCrashContainer
}

func (d *fakeCrashDocker) ContainerInspect(ctx context.Context, id string, _ mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error) {
	if err := ctx.Err(); err != nil {
		return mobycl.ContainerInspectResult{}, err
	}
	ctr := d.containers[id]
	switch {
	case ctr == nil || ctr.removed:
		return mobycl.ContainerInspectResult{}, fmt.Errorf("no such container %s: %w", id, errdefs.ErrNotFound)
	case ctr.inspectErr != nil:
		return mobycl.ContainerInspectResult{}, ctr.inspectErr
	case ctr.inspectErrAfter > 0 && ctr.inspections >= ctr.inspectErrAfter:
		return mobycl.ContainerInspectResult{}, errors.New("engine stopped answering")
	}
	ctr.inspections++
	state := ctr.state
	return mobycl.ContainerInspectResult{Container: dockercontainer.InspectResponse{ID: id, Image: "sha256:image-" + id, State: &state}}, nil
}

func (d *fakeCrashDocker) CopyFromContainer(ctx context.Context, id string, options mobycl.CopyFromContainerOptions) (mobycl.CopyFromContainerResult, error) {
	if err := ctx.Err(); err != nil {
		return mobycl.CopyFromContainerResult{}, err
	}
	ctr := d.containers[id]
	if ctr != nil && ctr.removedAfterInspect {
		ctr.removed = true
	}
	switch {
	case options.SourcePath != ceph.CrashDir:
		return mobycl.CopyFromContainerResult{}, fmt.Errorf("unexpected path %s", options.SourcePath)
	case ctr == nil || ctr.removed || ctr.noCrashDir:
		return mobycl.CopyFromContainerResult{}, fmt.Errorf("could not find the file %s in container %s: %w", options.SourcePath, id, errdefs.ErrNotFound)
	case ctr.copyErr != nil:
		return mobycl.CopyFromContainerResult{}, ctr.copyErr
	}
	ctr.copies++
	return mobycl.CopyFromContainerResult{Content: io.NopCloser(bytes.NewReader(ctr.archive))}, nil
}

type crashTarget struct {
	testcontainers.Container
	id string
}

func (c crashTarget) GetContainerID() string { return c.id }

func crashContainer(role, name, id string) diagnostics.Target {
	return diagnostics.Target{Role: role, Name: name, Container: crashTarget{id: id}}
}

// diagnosticsFrame is one frame of Docker's multiplexed log stream.
func diagnosticsFrame(kind byte, data string) []byte {
	header := make([]byte, 8)
	header[0] = kind
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	return append(header, data...)
}
