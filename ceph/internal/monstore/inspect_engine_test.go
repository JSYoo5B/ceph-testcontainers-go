package monstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	mobycl "github.com/moby/moby/client"
)

// monStoreInspectDockerFake is a small Docker model for the inspector engine.
// Its answers are synthetic: it says nothing about what a Docker daemon does.
// It records the calls, and keeps the labels of every container that exists,
// including one whose create reply was lost.
type monStoreInspectDockerFake struct {
	calls      []string
	createOpts []mobycl.ContainerCreateOptions
	// lostCreateReply makes ContainerCreate create the container and then fail.
	lostCreateReply bool
	createErr       error
	startErr        error
	wait            func(context.Context) mobycl.ContainerWaitResult
	logs            func(context.Context) (io.ReadCloser, error)
	logsOpts        []mobycl.ContainerLogsOptions
	removeOpts      []mobycl.ContainerRemoveOptions
	removeCtxErr    []error
	removeErr       error
	closeErr        error
	closed          int
	// existing maps the ID of every container to its labels.
	existing map[string]map[string]string
}

const monStoreFakeContainerID = "inspector-1"

func (d *monStoreInspectDockerFake) ContainerCreate(_ context.Context, opts mobycl.ContainerCreateOptions) (mobycl.ContainerCreateResult, error) {
	d.calls = append(d.calls, "create")
	d.createOpts = append(d.createOpts, opts)
	if d.createErr != nil {
		return mobycl.ContainerCreateResult{}, d.createErr
	}
	if d.existing == nil {
		d.existing = map[string]map[string]string{}
	}
	d.existing[monStoreFakeContainerID] = maps.Clone(opts.Config.Labels)
	if d.lostCreateReply {
		return mobycl.ContainerCreateResult{}, context.DeadlineExceeded
	}
	return mobycl.ContainerCreateResult{ID: monStoreFakeContainerID}, nil
}

func (d *monStoreInspectDockerFake) ContainerStart(_ context.Context, id string, _ mobycl.ContainerStartOptions) (mobycl.ContainerStartResult, error) {
	d.calls = append(d.calls, "start:"+id)
	return mobycl.ContainerStartResult{}, d.startErr
}

func (d *monStoreInspectDockerFake) ContainerWait(ctx context.Context, id string, opts mobycl.ContainerWaitOptions) mobycl.ContainerWaitResult {
	d.calls = append(d.calls, "wait:"+id+":"+string(opts.Condition))
	if d.wait != nil {
		return d.wait(ctx)
	}
	return monStoreWaitExit(0)
}

func (d *monStoreInspectDockerFake) ContainerLogs(ctx context.Context, id string, opts mobycl.ContainerLogsOptions) (mobycl.ContainerLogsResult, error) {
	d.calls = append(d.calls, "logs:"+id)
	d.logsOpts = append(d.logsOpts, opts)
	if d.logs == nil {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	return d.logs(ctx)
}

func (d *monStoreInspectDockerFake) ContainerRemove(ctx context.Context, id string, opts mobycl.ContainerRemoveOptions) (mobycl.ContainerRemoveResult, error) {
	d.calls = append(d.calls, "remove:"+id)
	d.removeOpts = append(d.removeOpts, opts)
	d.removeCtxErr = append(d.removeCtxErr, ctx.Err())
	if d.removeErr != nil {
		return mobycl.ContainerRemoveResult{}, d.removeErr
	}
	delete(d.existing, id)
	return mobycl.ContainerRemoveResult{}, nil
}

func (d *monStoreInspectDockerFake) Close() error {
	d.closed++
	return d.closeErr
}

func (d *monStoreInspectDockerFake) engine() monStoreMobyEngine {
	return monStoreMobyEngine{newClient: func(context.Context) (monStoreInspectDocker, error) { return d, nil }}
}

func (d *monStoreInspectDockerFake) count(prefix string) (n int) {
	for _, c := range d.calls {
		if strings.HasPrefix(c, prefix) {
			n++
		}
	}
	return n
}

func monStoreWaitExit(code int64) mobycl.ContainerWaitResult {
	res := make(chan container.WaitResponse, 1)
	res <- container.WaitResponse{StatusCode: code}
	return mobycl.ContainerWaitResult{Result: res, Error: make(chan error)}
}

func monStoreFrame(stream byte, data []byte) []byte {
	header := make([]byte, 8)
	header[0] = stream
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	return append(header, data...)
}

func monStoreLogReader(frames ...[]byte) func(context.Context) (io.ReadCloser, error) {
	return func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(bytes.Join(frames, nil))), nil
	}
}

const monStoreEmptyListing = `{"format":"tc-mon-store-listing-v1"}` + "\n" + `{"end":true,"count":0,"kv_backend":null}` + "\n"

func monStoreEngineRequest(t *testing.T) monStoreInspectRequest {
	t.Helper()
	req, err := newMonStoreInspectRequest("registry.example/ceph:20.2.4", "167:167", "owner-1", "store-volume-a", "token-1")
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func TestMonStoreMobyEngineCreateTranslatesTheRequest(t *testing.T) {
	d := &monStoreInspectDockerFake{}
	req := monStoreEngineRequest(t)
	id, err := d.engine().Create(t.Context(), req)
	if err != nil || id != monStoreFakeContainerID {
		t.Fatalf("id %q err %v", id, err)
	}
	if len(d.createOpts) != 1 || d.closed != 1 || d.count("start") != 0 {
		t.Fatalf("calls %v closed %d", d.calls, d.closed)
	}
	cfg, host := d.createOpts[0].Config, d.createOpts[0].HostConfig
	if cfg.Image != req.image || cfg.User != "167:167" || cfg.Tty || !slices.Equal(cfg.Entrypoint, []string{"python3"}) || !slices.Equal(cfg.Cmd, req.cmd) {
		t.Errorf("config %+v", cfg)
	}
	if !maps.Equal(cfg.Labels, map[string]string{clusterutil.OwnerLabel: "owner-1", monStoreInspectorLabel: "token-1"}) {
		t.Errorf("labels %v", cfg.Labels)
	}
	want := mount.Mount{Type: mount.TypeVolume, Source: "store-volume-a", Target: "/store", ReadOnly: true, VolumeOptions: &mount.VolumeOptions{NoCopy: true}}
	if len(host.Mounts) != 1 || len(host.Binds) != 0 || host.Mounts[0].Type != want.Type || host.Mounts[0].Source != want.Source ||
		host.Mounts[0].Target != want.Target || !host.Mounts[0].ReadOnly || host.Mounts[0].VolumeOptions == nil || !host.Mounts[0].VolumeOptions.NoCopy {
		t.Errorf("mounts %+v binds %v", host.Mounts, host.Binds)
	}
	if host.NetworkMode != "none" || !slices.Equal(host.CapDrop, []string{"ALL"}) || !slices.Equal(host.SecurityOpt, []string{"no-new-privileges:true"}) ||
		!host.ReadonlyRootfs || host.Privileged || host.AutoRemove {
		t.Errorf("host config %+v", host)
	}
	// The options own their data: a later change of the request changes nothing.
	req.labels[clusterutil.OwnerLabel] = "changed"
	req.cmd[0] = "changed"
	if cfg.Labels[clusterutil.OwnerLabel] != "owner-1" || cfg.Cmd[0] != "-I" {
		t.Error("the create options alias the request")
	}
}

func TestMonStoreMobyEngineCreateRefusesAnInvalidRequestBeforeDocker(t *testing.T) {
	cases := map[string]func(*monStoreInspectRequest){
		"writable mount":    func(r *monStoreInspectRequest) { r.readOnlyMount = false },
		"network":           func(r *monStoreInspectRequest) { r.networkMode = "host" },
		"privileged":        func(r *monStoreInspectRequest) { r.privileged = true },
		"auto remove":       func(r *monStoreInspectRequest) { r.autoRemove = true },
		"capabilities":      func(r *monStoreInspectRequest) { r.capDrop = nil },
		"writable rootfs":   func(r *monStoreInspectRequest) { r.readOnlyRootfs = false },
		"removal of volume": func(r *monStoreInspectRequest) { r.keepVolumesOnRemove = false },
		"no owned label":    func(r *monStoreInspectRequest) { delete(r.labels, clusterutil.OwnerLabel) },
	}
	for name, mutate := range cases {
		d := &monStoreInspectDockerFake{}
		req := monStoreEngineRequest(t)
		mutate(&req)
		if id, err := d.engine().Create(t.Context(), req); err == nil || id != "" || len(d.calls) != 0 {
			t.Errorf("%s: id %q err %v calls %v", name, id, err, d.calls)
		}
	}
}

func TestMonStoreMobyEngineClientFailuresAreErrors(t *testing.T) {
	req := monStoreEngineRequest(t)
	if _, err := (monStoreMobyEngine{}).Create(t.Context(), req); err == nil {
		t.Error("an engine without a constructor created a container")
	}
	failing := monStoreMobyEngine{newClient: func(context.Context) (monStoreInspectDocker, error) { return nil, errors.New("no daemon") }}
	if err := failing.Start(t.Context(), "x"); err == nil || !strings.Contains(err.Error(), "no daemon") {
		t.Errorf("constructor failure: %v", err)
	}
	var typedNil *monStoreInspectDockerFake
	nilClient := monStoreMobyEngine{newClient: func(context.Context) (monStoreInspectDocker, error) { return typedNil, nil }}
	if err := nilClient.Remove(t.Context(), "x"); err == nil {
		t.Error("a typed-nil client was used")
	}
	// The client is closed on every outcome, and a close failure is reported.
	d := &monStoreInspectDockerFake{closeErr: errors.New("close failed"), startErr: errors.New("start refused")}
	err := d.engine().Start(t.Context(), "x")
	if d.closed != 1 || err == nil || !strings.Contains(err.Error(), "start refused") || !strings.Contains(err.Error(), "close failed") {
		t.Errorf("closed %d err %v", d.closed, err)
	}
	if _, err := d.engine().Create(t.Context(), req); err == nil || !strings.Contains(err.Error(), "close failed") || d.closed != 2 {
		t.Errorf("a close failure after a successful create was lost: %v", err)
	}
}

func TestMonStoreMobyEngineWaitReturnsTheExitCode(t *testing.T) {
	for _, code := range []int64{0, 3, 137} {
		d := &monStoreInspectDockerFake{wait: func(context.Context) mobycl.ContainerWaitResult { return monStoreWaitExit(code) }}
		got, err := d.engine().Wait(t.Context(), monStoreFakeContainerID)
		if err != nil || got != code {
			t.Errorf("code %d: got %d err %v", code, got, err)
		}
		if want := "wait:inspector-1:not-running"; d.calls[0] != want {
			t.Errorf("call %q", d.calls[0])
		}
	}
}

func TestMonStoreMobyEngineWaitNeverForgesAnExitCode(t *testing.T) {
	closedResult := func(context.Context) mobycl.ContainerWaitResult {
		res := make(chan container.WaitResponse)
		close(res)
		return mobycl.ContainerWaitResult{Result: res, Error: make(chan error)}
	}
	closedError := func(context.Context) mobycl.ContainerWaitResult {
		errc := make(chan error)
		close(errc)
		return mobycl.ContainerWaitResult{Result: make(chan container.WaitResponse), Error: errc}
	}
	failedWait := func(context.Context) mobycl.ContainerWaitResult {
		errc := make(chan error, 1)
		errc <- errors.New("daemon lost")
		return mobycl.ContainerWaitResult{Result: make(chan container.WaitResponse), Error: errc}
	}
	errorReply := func(context.Context) mobycl.ContainerWaitResult {
		res := make(chan container.WaitResponse, 1)
		res <- container.WaitResponse{StatusCode: 0, Error: &container.WaitExitError{Message: "cannot wait"}}
		return mobycl.ContainerWaitResult{Result: res, Error: make(chan error)}
	}
	blocked := func(context.Context) mobycl.ContainerWaitResult {
		return mobycl.ContainerWaitResult{Result: make(chan container.WaitResponse), Error: make(chan error)}
	}
	cases := map[string]func(context.Context) mobycl.ContainerWaitResult{
		"closed result channel": closedResult, "closed error channel": closedError,
		"wait request failed": failedWait, "reply carries an error": errorReply,
	}
	for name, wait := range cases {
		d := &monStoreInspectDockerFake{wait: wait}
		if code, err := d.engine().Wait(t.Context(), monStoreFakeContainerID); err == nil || code != 0 {
			t.Errorf("%s: code %d err %v", name, code, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	d := &monStoreInspectDockerFake{wait: blocked}
	if code, err := d.engine().Wait(ctx, monStoreFakeContainerID); !errors.Is(err, context.Canceled) || code != 0 {
		t.Errorf("a context end: code %d err %v", code, err)
	}
}

func TestMonStoreMobyEngineLogsDemultiplexAndBound(t *testing.T) {
	d := &monStoreInspectDockerFake{logs: monStoreLogReader(
		monStoreFrame(1, []byte("out-1 ")), monStoreFrame(2, []byte("err")), monStoreFrame(1, []byte("out-2")), monStoreFrame(1, nil))}
	logs, err := d.engine().Logs(t.Context(), monStoreFakeContainerID, 64)
	if err != nil || string(logs.stdout) != "out-1 out-2" || string(logs.stderr) != "err" || logs.truncated {
		t.Fatalf("logs %+v err %v", logs, err)
	}
	if len(d.logsOpts) != 1 || !d.logsOpts[0].ShowStdout || !d.logsOpts[0].ShowStderr || d.logsOpts[0].Follow || d.logsOpts[0].Tail != "" {
		t.Errorf("log options %+v", d.logsOpts)
	}
	// Exactly the limit is not truncation.
	d.logs = monStoreLogReader(monStoreFrame(1, []byte("0123456789")))
	if logs, err := d.engine().Logs(t.Context(), monStoreFakeContainerID, 10); err != nil || logs.truncated || len(logs.stdout) != 10 {
		t.Errorf("at the limit: %+v err %v", logs, err)
	}
	// One byte over, in one frame or across frames, truncates at the limit.
	for name, frames := range map[string][][]byte{
		"one frame":   {monStoreFrame(1, []byte("0123456789A"))},
		"two frames":  {monStoreFrame(1, []byte("01234567")), monStoreFrame(1, []byte("89A"))},
		"other first": {monStoreFrame(2, []byte("e")), monStoreFrame(1, []byte("0123456789A")), monStoreFrame(2, []byte("late"))},
	} {
		d.logs = monStoreLogReader(frames...)
		logs, err := d.engine().Logs(t.Context(), monStoreFakeContainerID, 10)
		if err != nil || !logs.truncated || len(logs.stdout) != 10 || len(logs.stderr) > 1 {
			t.Errorf("%s: %+v err %v", name, logs, err)
		}
	}
	// A frame beyond the chunk size is read through a fixed buffer.
	big := bytes.Repeat([]byte("x"), 100<<10)
	d.logs = monStoreLogReader(monStoreFrame(1, big))
	if logs, err := d.engine().Logs(t.Context(), monStoreFakeContainerID, 1<<20); err != nil || !bytes.Equal(logs.stdout, big) {
		t.Errorf("large frame: %d bytes err %v", len(logs.stdout), err)
	}
	if _, err := d.engine().Logs(t.Context(), monStoreFakeContainerID, 0); err == nil {
		t.Error("a zero limit was accepted")
	}
}

type monStoreFailingReader struct{ err error }

func (r monStoreFailingReader) Read([]byte) (int, error) { return 0, r.err }

func TestMonStoreMobyEngineLogsRefuseAnUnreadableStream(t *testing.T) {
	hugeHeader := []byte{1, 0, 0, 0, 0xff, 0xff, 0xff, 0xff}
	cases := map[string]func(context.Context) (io.ReadCloser, error){
		"open fails":           func(context.Context) (io.ReadCloser, error) { return nil, errors.New("no logs") },
		"cut inside a header":  monStoreLogReader(monStoreFrame(1, []byte("ok")), []byte{1, 0, 0}),
		"cut inside a payload": monStoreLogReader(monStoreFrame(1, []byte("complete")), monStoreFrame(1, []byte("0123456789"))[:12]),
		"stdin frame":          monStoreLogReader(monStoreFrame(0, []byte("x"))),
		"unknown stream":       monStoreLogReader(monStoreFrame(3, []byte("x"))),
		"reserved bytes":       monStoreLogReader([]byte{1, 1, 0, 0, 0, 0, 0, 1, 'x'}),
		"huge frame, no bytes": monStoreLogReader(hugeHeader),
		"read error": func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(io.MultiReader(bytes.NewReader(monStoreFrame(1, []byte("ok"))), monStoreFailingReader{errors.New("reset")})), nil
		},
		"read error in payload": func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(io.MultiReader(bytes.NewReader([]byte{1, 0, 0, 0, 0, 0, 0, 9, 'a'}), monStoreFailingReader{errors.New("reset")})), nil
		},
	}
	for name, open := range cases {
		d := &monStoreInspectDockerFake{logs: open}
		logs, err := d.engine().Logs(t.Context(), monStoreFakeContainerID, 1<<20)
		if err == nil || len(logs.stdout) != 0 || len(logs.stderr) != 0 || logs.truncated {
			t.Errorf("%s: logs %+v err %v", name, logs, err)
		}
	}
}

type monStoreBlockingReader struct {
	closed chan struct{}
	once   *sync.Once
}

func (r monStoreBlockingReader) Read([]byte) (int, error) { <-r.closed; return 0, errors.New("closed") }
func (r monStoreBlockingReader) Close() error             { r.once.Do(func() { close(r.closed) }); return nil }

func TestMonStoreMobyEngineLogsStopWithTheContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	d := &monStoreInspectDockerFake{logs: func(context.Context) (io.ReadCloser, error) {
		cancel()
		return monStoreBlockingReader{closed: make(chan struct{}), once: new(sync.Once)}, nil
	}}
	if _, err := d.engine().Logs(ctx, monStoreFakeContainerID, 1<<20); !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v", err)
	}
}

func TestMonStoreMobyEngineRemoveIsForcedKeepsVolumesAndToleratesMissing(t *testing.T) {
	d := &monStoreInspectDockerFake{}
	if err := d.engine().Remove(t.Context(), monStoreFakeContainerID); err != nil {
		t.Fatal(err)
	}
	if got := d.removeOpts; len(got) != 1 || !got[0].Force || got[0].RemoveVolumes || got[0].RemoveLinks {
		t.Fatalf("remove options %+v", got)
	}
	d.removeErr = errdefs.ErrNotFound
	if err := d.engine().Remove(t.Context(), monStoreFakeContainerID); err != nil {
		t.Errorf("a missing container is an error: %v", err)
	}
	d.removeErr = errors.New("device busy")
	if err := d.engine().Remove(t.Context(), monStoreFakeContainerID); err == nil || !strings.Contains(err.Error(), "device busy") {
		t.Errorf("a failed removal was hidden: %v", err)
	}
}

// ---- the driver on the Docker engine, with the fake client ----

func monStoreDockerInspector(d *monStoreInspectDockerFake) monStoreContainerInspector {
	return monStoreContainerInspector{engine: d.engine(), image: "registry.example/ceph:20.2.4", user: "167:167", owner: "owner-1",
		newToken: func() string { return "token-1" }}
}

func TestMonStoreInspectorOnTheDockerEngineReadsAListing(t *testing.T) {
	d := &monStoreInspectDockerFake{logs: monStoreLogReader(monStoreFrame(1, []byte(monStoreEmptyListing)))}
	files, err := monStoreDockerInspector(d).Inspect(t.Context(), "store-volume-a")
	if err != nil || len(files.entries) != 0 || files.kvBackend != nil {
		t.Fatalf("files %+v err %v", files, err)
	}
	want := []string{"create", "start:inspector-1", "wait:inspector-1:not-running", "logs:inspector-1", "remove:inspector-1"}
	if !slices.Equal(d.calls, want) || len(d.existing) != 0 || d.closed != 5 {
		t.Fatalf("calls %v existing %v closed %d", d.calls, d.existing, d.closed)
	}
}

func TestMonStoreInspectorOnTheDockerEngineKeepsALostCreateFindable(t *testing.T) {
	d := &monStoreInspectDockerFake{lostCreateReply: true, logs: monStoreLogReader(monStoreFrame(1, []byte(monStoreEmptyListing)))}
	files, err := monStoreDockerInspector(d).Inspect(t.Context(), "store-volume-a")
	if err == nil || !strings.Contains(err.Error(), "outcome unknown") || len(files.entries) != 0 {
		t.Fatalf("files %+v err %v", files, err)
	}
	// Not retried, not started, not removed by a guessed identity.
	if !slices.Equal(d.calls, []string{"create"}) {
		t.Fatalf("calls %v", d.calls)
	}
	// The container exists and carries the label that Terminate's scan selects.
	labels, ok := d.existing[monStoreFakeContainerID]
	if !ok || labels[clusterutil.OwnerLabel] != "owner-1" || labels[monStoreInspectorLabel] != "token-1" {
		t.Fatalf("existing %v", d.existing)
	}
}

func TestMonStoreInspectorOnTheDockerEngineReportsFailuresAndRemoves(t *testing.T) {
	listing := monStoreLogReader(monStoreFrame(1, []byte(monStoreEmptyListing)))
	type setup func(*monStoreInspectDockerFake)
	cases := map[string]struct {
		setup setup
		want  string
	}{
		"non-zero exit": {func(d *monStoreInspectDockerFake) {
			d.wait = func(context.Context) mobycl.ContainerWaitResult { return monStoreWaitExit(3) }
		}, "exited with code 3"},
		"start fails": {func(d *monStoreInspectDockerFake) { d.startErr = errors.New("start refused") }, "start refused"},
		"wait fails": {func(d *monStoreInspectDockerFake) {
			d.wait = func(context.Context) mobycl.ContainerWaitResult {
				errc := make(chan error, 1)
				errc <- errors.New("daemon lost")
				return mobycl.ContainerWaitResult{Result: make(chan container.WaitResponse), Error: errc}
			}
		}, "daemon lost"},
		"logs fail": {func(d *monStoreInspectDockerFake) {
			d.logs = func(context.Context) (io.ReadCloser, error) { return nil, errors.New("no logs") }
		}, "no logs"},
		"truncated frame": {func(d *monStoreInspectDockerFake) {
			d.logs = monStoreLogReader(monStoreFrame(1, []byte(monStoreEmptyListing))[:20])
		}, "log frame"},
		"standard error": {func(d *monStoreInspectDockerFake) {
			d.logs = monStoreLogReader(monStoreFrame(1, []byte(monStoreEmptyListing)), monStoreFrame(2, []byte("warning")))
		}, "standard error"},
		"output over the bound": {func(d *monStoreInspectDockerFake) {
			d.logs = func(context.Context) (io.ReadCloser, error) {
				header := monStoreFrame(1, nil)
				binary.BigEndian.PutUint32(header[4:], monStoreInspectOutputLimit+1)
				return io.NopCloser(io.MultiReader(bytes.NewReader(header), io.LimitReader(zeroReader{}, monStoreInspectOutputLimit+1))), nil
			}
		}, "beyond the limit"},
	}
	for name, tc := range cases {
		d := &monStoreInspectDockerFake{logs: listing}
		tc.setup(d)
		files, err := monStoreDockerInspector(d).Inspect(t.Context(), "store-volume-a")
		if err == nil || !strings.Contains(err.Error(), tc.want) || len(files.entries) != 0 || files.kvBackend != nil {
			t.Errorf("%s: files %+v err %v", name, files, err)
		}
		if d.count("remove:") != 1 || len(d.existing) != 0 {
			t.Errorf("%s: calls %v existing %v", name, d.calls, d.existing)
		}
		if got := d.removeOpts; len(got) != 1 || !got[0].Force || got[0].RemoveVolumes {
			t.Errorf("%s: remove options %+v", name, got)
		}
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) { clear(p); return len(p), nil }

func TestMonStoreInspectorOnTheDockerEngineWithholdsTheListingWhenRemovalFails(t *testing.T) {
	d := &monStoreInspectDockerFake{removeErr: errors.New("device busy"), logs: monStoreLogReader(monStoreFrame(1, []byte(monStoreEmptyListing)))}
	files, err := monStoreDockerInspector(d).Inspect(t.Context(), "store-volume-a")
	if err == nil || !strings.Contains(err.Error(), "device busy") || len(files.entries) != 0 || files.kvBackend != nil {
		t.Fatalf("files %+v err %v", files, err)
	}
	// The container is left in place, labelled, for the cluster's teardown scan.
	if labels, ok := d.existing[monStoreFakeContainerID]; !ok || labels[clusterutil.OwnerLabel] != "owner-1" {
		t.Fatalf("existing %v", d.existing)
	}
	// A failed removal beside a failed run keeps both errors.
	d.startErr = errors.New("start refused")
	if _, err := monStoreDockerInspector(d).Inspect(t.Context(), "store-volume-a"); err == nil ||
		!strings.Contains(err.Error(), "start refused") || !strings.Contains(err.Error(), "device busy") {
		t.Fatalf("err %v", err)
	}
}

func TestMonStoreInspectorOnTheDockerEngineRemovesAfterTheCallerContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	d := &monStoreInspectDockerFake{wait: func(context.Context) mobycl.ContainerWaitResult {
		cancel()
		return mobycl.ContainerWaitResult{Result: make(chan container.WaitResponse), Error: make(chan error)}
	}}
	if _, err := monStoreDockerInspector(d).Inspect(ctx, "store-volume-a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v", err)
	}
	if d.count("remove:") != 1 || len(d.existing) != 0 || d.count("logs") != 0 || len(d.removeCtxErr) != 1 || d.removeCtxErr[0] != nil {
		t.Fatalf("calls %v existing %v", d.calls, d.existing)
	}
}

func TestMonStoreMobyEngineCreateErrorIsReturnedWithoutAnID(t *testing.T) {
	d := &monStoreInspectDockerFake{createErr: errors.New("image missing")}
	id, err := d.engine().Create(t.Context(), monStoreEngineRequest(t))
	if err == nil || !strings.Contains(err.Error(), "image missing") || id != "" || !slices.Equal(d.calls, []string{"create"}) {
		t.Fatalf("id %q err %v calls %v", id, err, d.calls)
	}
}
