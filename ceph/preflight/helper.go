package preflight

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	control "github.com/JSYoo5B/ceph-testcontainers-go/internal/blockio"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

// ownerLabel marks the helper containers of Observe.
const ownerLabel = "org.testcontainers.ceph.preflight-owner"

const cleanupTimeout = 30 * time.Second

// helperRunner starts, probes and removes one helper container.
//
//testdouble:dependency-port
type helperRunner interface {
	// Start creates and starts the helper. It returns the container ID once
	// the engine created one, also with an error.
	Start(ctx context.Context, image string) (string, error)
	// Probe runs control.ProbeScript in the started helper.
	Probe(ctx context.Context) ([]byte, error)
	// Terminate removes the helper, when one was created.
	Terminate(ctx context.Context) error
}

type dockerHelper struct{ ctr testcontainers.Container }

// Start runs the helper with the privileges and device access of the block
// I/O keeper: root, privileged, the engine host's /dev and no network.
func (h *dockerHelper) Start(ctx context.Context, image string) (string, error) {
	name := "tcpre-" + uuid.NewString()
	request := testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: image, Name: name, Entrypoint: []string{"sleep"}, Cmd: []string{"infinity"}, User: "0",
		Labels: map[string]string{ownerLabel: name},
		HostConfigModifier: func(h *container.HostConfig) {
			h.Privileged = true
			h.Binds = append(h.Binds, "/dev:/dev")
			h.NetworkMode = "none"
		},
	}, Started: true}
	ctr, err := testcontainers.GenericContainer(ctx, request)
	if ctr != nil {
		h.ctr = ctr
		return ctr.GetContainerID(), err
	}
	return "", err
}

func (h *dockerHelper) Probe(ctx context.Context) ([]byte, error) {
	result, err := supervised.Command(ctx, h.ctr, "preflight helper", 30*time.Second, 64<<10, 8<<10, "python3", "-I", "-c", control.ProbeScript)
	if err != nil {
		return nil, err
	}
	if result.ExitCode != 0 {
		return nil, &supervised.ExitError{Label: "preflight helper", Code: result.ExitCode}
	}
	return result.Stdout, nil
}

func (h *dockerHelper) Terminate(ctx context.Context) error {
	if h.ctr == nil {
		return nil
	}
	return h.ctr.Terminate(ctx)
}

const (
	toolsGuidance   = "use a helper image that meets contract 1 of the blockio extension, such as blockio.HelperImage"
	kernelGuidance  = "make the engine kernel provide loop devices and device-mapper; on Docker Desktop, prepare the engine kernel as the desktop setup guide of this module describes"
	targetGuidance  = "register the device-mapper %s target in the engine kernel (kernel feature %s of the desktop setup guide); the library loads no kernel module"
	refusedGuidance = "allow privileged containers with the engine host's /dev on this engine; rootless engines and authorization plugins can refuse them"
)

// runHelper runs the helper level checks of block I/O and records the
// helper's lifetime. It always removes a created helper, with a fresh
// bounded context when ctx has ended.
func (c checker) runHelper(ctx context.Context, engine engineClient, image string, report *Report) []error {
	var helper helperRunner = &dockerHelper{}
	if c.helper != nil {
		helper = c.helper(engine)
	}
	run := &HelperRun{}
	report.Helper = run
	var errs []error
	id, err := helper.Start(ctx, image)
	run.ContainerID = id
	if err == nil {
		run.Started = true
		raw, probeErr := helper.Probe(ctx)
		if probeErr != nil {
			errs = append(errs, probeFailure(ctx, probeErr, report))
		} else if probe, parseErr := control.ParseProbe(raw); parseErr != nil {
			report.set(keyTools, Unknown, "", "helper-probe", "the helper probe returned invalid output", "")
			errs = append(errs, failure(ceph.ErrNativeFailure, "the helper probe returned invalid output"))
		} else {
			record(probe, report)
		}
	} else {
		errs = append(errs, startFailure(ctx, err, report))
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	if err := helper.Terminate(cleanupCtx); err != nil {
		run.Cleanup = "the removal of the helper container was not confirmed"
		errs = append(errs, failure(ceph.ErrEnvironmentUnavailable, "remove helper container %s: removal was not confirmed", id))
	} else {
		run.Released = true
	}
	return errs
}

// startFailure classifies a helper that did not start. A refusal of the
// helper's configuration is conclusive for that configuration and leaves
// the kernel checks not run.
func startFailure(ctx context.Context, err error, report *Report) error {
	switch {
	case ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		report.set(keyHelperStart, Unknown, "", "helper-start", "the helper start was interrupted", "")
		return cause(ctx, err, ceph.ErrEnvironmentUnavailable, "start the helper")
	case errdefs.IsNotFound(err):
		report.set(keyHelperStart, Unavailable, ImageKind, "helper-start", "the engine could not find or pull the helper image", "pass a helper image that the engine can pull, such as blockio.HelperImage")
		return nil
	case errdefs.IsPermissionDenied(err) || errdefs.IsUnauthorized(err):
		report.set(keyHelperStart, Unavailable, EnvironmentKind, "helper-start", "the engine refused the helper's privileged configuration", refusedGuidance)
		return nil
	}
	report.set(keyHelperStart, Unknown, "", "helper-start", "the helper did not start", "")
	return failure(ceph.ErrEnvironmentUnavailable, "start the helper: the helper did not start")
}

// probeFailure classifies a probe that returned no output. A helper that
// cannot run python3 lacks an image tool.
func probeFailure(ctx context.Context, err error, report *Report) error {
	report.set(keyHelperStart, Observed, "", "helper-start", "", "")
	var exit *supervised.ExitError
	if ctx.Err() == nil && errors.As(err, &exit) && (exit.Code == 126 || exit.Code == 127) {
		report.set(keyTools, Unavailable, ImageKind, "helper-probe", "the helper image cannot run python3", toolsGuidance)
		return nil
	}
	report.set(keyTools, Unknown, "", "helper-probe", "the helper probe did not complete", "")
	return cause(ctx, err, ceph.ErrNativeFailure, "probe the helper")
}

// record maps the probe's gates onto the report. The targets are
// conclusive only when the probe read the registered target list.
func record(probe control.Probe, report *Report) {
	report.set(keyHelperStart, Observed, "", "helper-start", "", "")
	keys := map[string]string{"tools": keyTools, "drivers": keyDrivers, "control": keyControl}
	reasons := map[string]string{
		"tools":   "the helper image lacks a required tool",
		"drivers": "the engine kernel registers no device-mapper or loop driver",
		"control": "the device-mapper or loop control device is not accessible",
	}
	guidance := map[string]string{"tools": toolsGuidance, "drivers": kernelGuidance, "control": kernelGuidance}
	for _, gate := range probe.Gates {
		key, ok := keys[gate.Key]
		if !ok {
			continue
		}
		switch gate.State {
		case "observed":
			report.set(key, Observed, "", "helper-probe", "", "")
		case "unavailable":
			reason := reasons[gate.Key]
			if len(gate.Missing) > 0 {
				reason = "the helper image lacks " + joinTools(gate.Missing)
			}
			report.set(key, Unavailable, Kind(gate.Kind), "helper-probe", reason, guidance[gate.Key])
		case "unknown":
			report.set(key, Unknown, "", "helper-probe", "the probe could not observe this gate", "")
		}
	}
	targets := probe.Gates[len(probe.Gates)-1]
	for key, target := range map[string]string{keyTargetLinear: "linear", keyTargetDelay: "delay", keyTargetError: "error"} {
		switch targets.State {
		case "observed":
			if slices.Contains(probe.Targets, target) {
				report.set(key, Observed, "", "helper-probe", "", "")
			} else {
				feature := map[string]string{"linear": "dm", "delay": "dm-delay", "error": "dm"}[target]
				report.set(key, Unavailable, EnvironmentKind, "helper-probe", "dmsetup targets does not list "+target, fmt.Sprintf(targetGuidance, target, feature))
			}
		case "unknown":
			report.set(key, Unknown, "", "helper-probe", "dmsetup targets could not be read", "")
		}
	}
}

// joinTools names the missing tools of the fixed tool list only.
func joinTools(missing []string) string {
	var names []string
	for _, name := range missing {
		if slices.Contains([]string{"sh", "sleep", "dmsetup", "losetup", "blockdev"}, name) {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return "a required tool"
	}
	return strings.Join(names, ", ")
}
