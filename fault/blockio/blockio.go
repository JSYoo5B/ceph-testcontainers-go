// Package blockio prepares disposable device-mapper storage for one BlueStore
// OSD and injects bounded main-device delay or EIO. The main device also carries
// BlueFS traffic. Restoring its table does not restart or recover the OSD.
package blockio

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	control "github.com/JSYoo5B/ceph-testcontainers-go/internal/blockio"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

const (
	// DefaultSize is the logical size of a zero Config.Size.
	DefaultSize int64 = 1 << 30
	// MinSize and MaxSize bound the disposable backing file, in bytes.
	MinSize int64 = 64 << 20
	MaxSize int64 = 4 << 30
	// MaxDelay bounds Profile.Delay.
	MaxDelay = 10 * time.Second
)

const operationTimeout = 2 * time.Minute

const ownerLabel = "org.testcontainers.ceph.blockio-owner"

// Config selects the logical backing size. Size is sector aligned, between
// MinSize and MaxSize; zero selects DefaultSize. The helper holds the sparse
// file on its own bounded tmpfs. Written pages consume Docker-host memory.
type Config struct{ Size int64 }

// Option carries Config to Run; its Customize changes no container settings.
type Option struct{ config Config }

// Customize implements testcontainers.ContainerCustomizer.
func (Option) Customize(*testcontainers.GenericContainerRequest) error { return nil }

// WithConfig carries the one optional Config to Run.
//
//purpose:construction
func WithConfig(config Config) Option { return Option{config: config} }

// Profile selects exactly one main-device fault. Delay is a positive whole
// number of milliseconds, at most MaxDelay. EIO makes every sector return an
// I/O error. It can abort the OSD; client operations may time out instead of
// returning EIO. A profile never implies independent observation of its effect.
type Profile struct {
	Delay time.Duration
	EIO   bool
}

// Identity identifies the mapping observed for this disposable backend. It is
// observation only: Run accepts no existing mapping, loop or physical disk.
type Identity struct {
	Path, UUID   string
	Major, Minor uint32
	Size         int64
}

// Tables is a native observation of an identified device-mapper node, not an
// assertion about the client's I/O, daemon health or data recovery.
type Tables struct {
	Live, Inactive                string
	Suspended                     bool
	Openers                       int
	ReadsInFlight, WritesInFlight int64
}

// Status keeps requested settings separate from independently read kernel
// tables. Identity and Tables are nil until observed; PendingStep names an
// operation whose completion has not been confirmed. Released means that the
// owned mapping, loop, file and helper have all been released.
type Status struct {
	Requested   Profile
	Identity    *Identity
	Tables      *Tables
	PendingStep string
	Released    bool
}

// Container owns one backing file, loop, mapping and privileged keeper. Keep
// a non-nil value returned with an error. Terminate retains the keeper and its
// backing when a consumer or control step may remain active. Do not stop,
// restart or remove the embedded keeper while the backend is held.
type Container struct {
	testcontainers.Container
	mu sync.Mutex
	// nativeCommand substitutes command receipts for cancellation/identity tests.
	// Production uses the supervised keeper command when nil.
	//testdouble:nil-default-hook
	nativeCommand                                    func(context.Context, ...string) ([]byte, error)
	name, instance                                   string
	size                                             int64
	docker                                           *testcontainers.DockerClient
	creationSent, ready, closing, released           bool
	loop                                             control.LoopIdentity
	fileDev                                          uint64
	device                                           faulthook.BlockDevice
	record                                           control.Record
	linear                                           string
	requested                                        Profile
	cluster                                          *ceph.Container
	osd                                              *ceph.OSDContainer
	attachmentSent                                   bool
	token                                            faulthook.Token
	registered                                       bool
	excluded, mappingRemoved, detachSent, unlinkSent bool
	loopReleased, removeSent, backingReleased        bool
	detachID                                         string
	// creationStopped, detachStopped and stoppedStep mark a dispatch that
	// failed without a reply while its keeper invocation was confirmed
	// stopped; withdrawUnstarted and settle then consult its started marker.
	creationStopped, detachStopped bool
	stoppedStep                    string
}

func failure(kind error, format string, args ...any) error {
	return &ceph.OperationError{Kind: kind, Op: "block I/O backend", Err: fmt.Errorf(format, args...)}
}

func configOf(opts []testcontainers.ContainerCustomizer) (Config, error) {
	var cfg Config
	seen := false
	for _, opt := range opts {
		var value Option
		switch typed := opt.(type) {
		case Option:
			value = typed
		case *Option:
			if typed == nil {
				return cfg, failure(ceph.ErrInvalidRequest, "nil WithConfig")
			}
			value = *typed
		default:
			continue
		}
		if seen {
			return cfg, failure(ceph.ErrInvalidRequest, "provide at most one WithConfig")
		}
		cfg, seen = value.config, true
	}
	if cfg.Size == 0 {
		cfg.Size = DefaultSize
	}
	if cfg.Size < MinSize || cfg.Size > MaxSize || cfg.Size%512 != 0 {
		return cfg, failure(ceph.ErrInvalidRequest, "size must be sector aligned and between %d and %d bytes", MinSize, MaxSize)
	}
	return cfg, nil
}

func (p Profile) table(sectors int64, lower string) (string, error) {
	if p.EIO && p.Delay == 0 {
		return fmt.Sprintf("0 %d error", sectors), nil
	}
	if !p.EIO && p.Delay > 0 && p.Delay <= MaxDelay && p.Delay%time.Millisecond == 0 {
		return fmt.Sprintf("0 %d delay %s 0 %d", sectors, lower, p.Delay.Milliseconds()), nil
	}
	return "", failure(ceph.ErrInvalidRequest, "select EIO or a whole-millisecond delay between 1ms and %s", MaxDelay)
}

// Run prepares one disposable backend with the explicitly selected helper
// image. The image supplies sh, sleep, python3, dmsetup, losetup and blockdev;
// the Docker host grants privileged /dev access and registered linear support.
// Individual faults check their own registered target before use. No host
// module is loaded. Customizers reach the keeper; its identity, private tmpfs,
// command and image must remain intact. Register its CleanupContainer before
// the cluster's so cluster teardown precedes backing release.
//
//purpose:construction
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Container, error) {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(img) == "" {
		return nil, failure(ceph.ErrImageCapability, "select a block I/O helper image")
	}
	cfg, err := configOf(opts)
	if err != nil {
		return nil, err
	}
	b := &Container{name: "tcblk-" + uuid.NewString(), size: cfg.Size}
	b.docker, err = testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return b, err
	}
	request := testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: img, Name: b.name, Entrypoint: []string{"sleep"}, Cmd: []string{"infinity"}, User: "0",
		Files: []testcontainers.ContainerFile{{Reader: strings.NewReader(ownerScript), ContainerFilePath: ownerProgram, FileMode: 0o500}},
		HostConfigModifier: func(h *container.HostConfig) {
			h.Privileged = true
			h.Binds = append(h.Binds, "/dev:/dev")
			h.Tmpfs = map[string]string{"/tc-blk": fmt.Sprintf("size=%d,mode=0700", cfg.Size+(64<<20))}
			h.NetworkMode = "none"
		},
	}, Started: true}
	for _, opt := range opts {
		if err := opt.Customize(&request); err != nil {
			return b, err
		}
	}
	if request.Image != img || request.Name != b.name || strings.Join(request.Entrypoint, " ") != "sleep" || strings.Join(request.Cmd, " ") != "infinity" {
		return b, failure(ceph.ErrInvalidRequest, "customizers must retain the helper image, identity and idle command")
	}
	if request.Labels == nil {
		request.Labels = map[string]string{}
	}
	request.Labels[ownerLabel] = b.name
	b.Container, err = testcontainers.GenericContainer(ctx, request)
	if err != nil {
		return b, faulthook.ClassifyDocker(ctx, err)
	}
	probe, err := b.owner(ctx, "probe")
	if err != nil {
		return b, err
	}
	b.instance = probe.Instance
	if !contains(probe.Targets, "linear") {
		return b, failure(ceph.ErrEnvironmentUnavailable, "device-mapper linear target is not registered")
	}
	if err := b.startCreation(ctx); err != nil {
		return b, err
	}
	for {
		done, err := b.prepare(ctx)
		if err != nil {
			return b, err
		}
		if done {
			return b, nil
		}
		select {
		case <-ctx.Done():
			return b, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// GetContainerID implements the container contract for partial startup too.
//
//purpose:observation
func (b *Container) GetContainerID() string {
	if b.Container == nil {
		return ""
	}
	return b.Container.GetContainerID()
}

func contains(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}

// AttachOSD creates and starts one OSD on this backend. It accepts only a ready
// linear backend and allows one attachment attempt, including partial startup.
// The cluster restores its registered block fault before stopping daemons;
// this separately owned keeper stays until the caller terminates the backend.
//
//purpose:construction
func (b *Container) AttachOSD(ctx context.Context, cluster *ceph.Container) (*ceph.OSDContainer, error) {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	if err := clusterutil.LockMutex(ctx, &b.mu); err != nil {
		return nil, err
	}
	defer b.mu.Unlock()
	if cluster == nil || !b.ready || b.closing || b.attachmentSent {
		return nil, failure(ceph.ErrInvalidRequest, "attach a ready backend to exactly one cluster")
	}
	view, err := b.observe(ctx)
	if err != nil {
		return nil, err
	}
	if control.Classify(b.record, b.linear, view) != control.Applied {
		return nil, failure(ceph.ErrInvalidRequest, "restore the backend before attaching an OSD")
	}
	err = faulthook.WithTopology(ctx, cluster, func() error {
		if faulthook.Closed(cluster) || !faulthook.BlueStore(cluster) {
			return failure(ceph.ErrInvalidRequest, "a live BlueStore cluster is required")
		}
		token, _, err := faulthook.Reserve(ctx, cluster, faulthook.BlockIO, b.name)
		if err != nil {
			return err
		}
		b.cluster, b.token, b.registered = cluster, token, true
		_, err = faulthook.Settle(cluster, token, faulthook.Held{Handle: b, Undo: b.Restore})
		return err
	})
	if err != nil {
		return nil, err
	}
	b.attachmentSent = true
	b.osd, err = faulthook.AddBlockDeviceOSD(ctx, cluster, b.device, b.name)
	return b.osd, err
}

// Apply installs one bounded main-device fault. A successful return confirms
// the requested table is active; it does not prove a client observed it. An
// error may leave an owned inactive table, suspension or pending kernel step.
// Inspect Status and retry Restore instead of replaying an uncertain request.
//
//purpose:condition
func (b *Container) Apply(ctx context.Context, profile Profile) error {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	if err := clusterutil.LockMutex(ctx, &b.mu); err != nil {
		return err
	}
	defer b.mu.Unlock()
	want, err := profile.table(b.size/512, b.loop.Dev)
	if err != nil {
		return err
	}
	if !b.ready || b.closing {
		return failure(ceph.ErrInvalidRequest, "backend is not available for a new fault")
	}
	if b.cluster != nil {
		if err := faulthook.WithTopology(ctx, b.cluster, func() error {
			if faulthook.Closed(b.cluster) {
				return failure(ceph.ErrInvalidRequest, "cluster is terminated")
			}
			return nil
		}); err != nil {
			return err
		}
	}
	probe, err := b.owner(ctx, "probe")
	if err != nil {
		return err
	}
	target := "delay"
	if profile.EIO {
		target = "error"
	}
	if !contains(probe.Targets, target) {
		return failure(ceph.ErrEnvironmentUnavailable, "device-mapper %s target is not registered", target)
	}
	b.requested = profile
	return b.transition(ctx, want)
}

// Restore returns only this backend's recorded tables to its linear mapping.
// It settles prior steps before new ones, refuses foreign identity/tables, and
// remains retryable after cancellation or partial failure. OSD restart and
// acknowledged-data recovery are separate caller operations.
//
//purpose:condition,lifetime
func (b *Container) Restore(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	if err := clusterutil.LockMutex(ctx, &b.mu); err != nil {
		return err
	}
	defer b.mu.Unlock()
	return b.restore(ctx)
}

func (b *Container) restore(ctx context.Context) error {
	if b.released || b.mappingRemoved {
		return nil
	}
	if !b.ready {
		if err := b.withdrawUnstarted(ctx); err != nil {
			return err
		}
		if !b.creationSent {
			return nil
		}
		done, err := b.prepare(ctx)
		if err != nil {
			return err
		}
		if !done {
			return errPending
		}
	}
	if err := b.settle(ctx); err != nil {
		return err
	}
	view, err := b.observe(ctx)
	if err != nil {
		return err
	}
	if view.Inactive != nil {
		if view.UUID != b.record.UUID || view.Major != b.record.Major || view.Minor != b.record.Minor || control.Normalize(*view.Inactive) != control.Normalize(b.record.Loaded) || view.Live == nil || control.Normalize(*view.Live) != control.Normalize(b.record.Live) {
			return failure(ceph.ErrNativeFailure, "foreign inactive table or device identity; backend retained")
		}
		if err := b.step(ctx, "clear", ""); err != nil {
			return err
		}
	}
	if err := b.transition(ctx, b.linear); err != nil {
		return err
	}
	b.requested = Profile{}
	return nil
}

// Status reads the owned identity and live/inactive tables separately from the
// last requested profile. Polling may confirm a recorded step, but never
// dispatches another mutation. A pending creation has no invented identity.
//
//purpose:observation
func (b *Container) Status(ctx context.Context) (Status, error) {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	if err := clusterutil.LockMutex(ctx, &b.mu); err != nil {
		return Status{}, err
	}
	defer b.mu.Unlock()
	status := Status{Requested: b.requested, Released: b.released}
	if b.released {
		return status, nil
	}
	if b.mappingRemoved {
		status.PendingStep = "release"
		if b.backingReleased {
			status.PendingStep = "keeper-release"
		}
		return status, nil
	}
	if !b.ready {
		if err := b.withdrawUnstarted(ctx); err != nil {
			return status, err
		}
		if !b.creationSent {
			return status, nil
		}
		done, err := b.prepare(ctx)
		if err != nil {
			return status, err
		}
		if !done {
			status.PendingStep = "create"
			return status, nil
		}
	}
	err := b.settle(ctx)
	if err != nil && !errors.Is(err, errPending) {
		return status, err
	}
	status.PendingStep = b.record.Unsettled
	view, err := b.observe(ctx)
	if err != nil {
		return status, err
	}
	if !view.Exists {
		return status, failure(ceph.ErrNativeFailure, "owned mapping is absent")
	}
	if view.UUID != b.record.UUID || view.Major != b.record.Major || view.Minor != b.record.Minor {
		return status, failure(ceph.ErrNativeFailure, "mapping identity changed; no action taken")
	}
	if view.State != "ACTIVE" && view.State != "SUSPENDED" {
		return status, failure(ceph.ErrNativeFailure, "mapping activity state is unavailable")
	}
	status.Identity = &Identity{Path: b.device.HostPath, UUID: view.UUID, Major: view.Major, Minor: view.Minor, Size: b.size}
	status.Tables = &Tables{Suspended: view.State == "SUSPENDED", Openers: view.Open, ReadsInFlight: view.InFlight[0], WritesInFlight: view.InFlight[1]}
	if view.Live != nil {
		status.Tables.Live = *view.Live
	}
	if view.Inactive != nil {
		status.Tables.Inactive = *view.Inactive
	}
	return status, nil
}
