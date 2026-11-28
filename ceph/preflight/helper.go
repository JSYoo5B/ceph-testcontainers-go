package preflight

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	control "github.com/JSYoo5B/ceph-testcontainers-go/internal/blockio"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/containerd/errdefs"
	"github.com/google/uuid"
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

// helperContainerRequest is the block I/O keeper's helper request, which runs image
// with the keeper's privileges and device access: root, privileged, the
// engine host's /dev and no network.
func helperContainerRequest(image string) testcontainers.GenericContainerRequest {
	name := "tcpre-" + uuid.NewString()
	request := control.HelperRequest(image, name)
	request.Labels = map[string]string{ownerLabel: name}
	return testcontainers.GenericContainerRequest{ContainerRequest: request, Started: true}
}

// Start runs the helper from helperContainerRequest.
func (h *dockerHelper) Start(ctx context.Context, image string) (string, error) {
	ctr, err := testcontainers.GenericContainer(ctx, helperContainerRequest(image))
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
			report.set(CheckKeyBlockIOImageTools, StateUnknown, "", SourceHelperProbe, "the helper probe returned invalid output", "")
			errs = append(errs, failure(ceph.ErrNativeFailure, "the helper probe returned invalid output"))
		} else if err := record(probe, report); err != nil {
			errs = append(errs, err)
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
	case interrupted(ctx, err):
		report.set(CheckKeyBlockIOHelperStart, StateUnknown, "", SourceHelperStart, "the helper start was interrupted", "")
		return cause(ctx, err, ceph.ErrEnvironmentUnavailable, "start the helper")
	case errdefs.IsNotFound(err):
		report.set(CheckKeyBlockIOHelperStart, StateUnavailable, KindImage, SourceHelperStart, "the engine could not find or pull the helper image", "pass a helper image that the engine can pull, such as blockio.HelperImage")
		return nil
	case errdefs.IsPermissionDenied(err) || errdefs.IsUnauthorized(err):
		report.set(CheckKeyBlockIOHelperStart, StateUnavailable, KindEnvironment, SourceHelperStart, "the engine refused the helper's privileged configuration", refusedGuidance)
		return nil
	}
	report.set(CheckKeyBlockIOHelperStart, StateUnknown, "", SourceHelperStart, "the helper did not start", "")
	return failure(ceph.ErrEnvironmentUnavailable, "start the helper: the helper did not start")
}

// probeFailure classifies a probe that returned no output. A helper that
// cannot run python3 lacks an image tool.
func probeFailure(ctx context.Context, err error, report *Report) error {
	report.set(CheckKeyBlockIOHelperStart, StateObserved, "", SourceHelperStart, "", "")
	if ctx.Err() == nil && supervised.MissingPython(err) {
		report.set(CheckKeyBlockIOImageTools, StateUnavailable, KindImage, SourceHelperProbe, "the helper image cannot run python3", toolsGuidance)
		return nil
	}
	report.set(CheckKeyBlockIOImageTools, StateUnknown, "", SourceHelperProbe, "the helper probe did not complete", "")
	return cause(ctx, err, ceph.ErrNativeFailure, "probe the helper")
}

// probeChecks are the checks of the probe's gates before the targets, with
// the reason and guidance of an unavailable gate.
var probeChecks = []struct {
	gate             control.GateKey
	key              CheckKey
	reason, guidance string
}{
	{control.GateTools, CheckKeyBlockIOImageTools, "the helper image lacks a required tool", toolsGuidance},
	{control.GateDrivers, CheckKeyBlockIODrivers, "the engine kernel registers no device-mapper or loop driver", kernelGuidance},
	{control.GateControl, CheckKeyBlockIOControlAccess, "the device-mapper or loop control device is not accessible", kernelGuidance},
}

// probeTargets are the target checks, with the device-mapper target each
// needs and the kernel feature of the desktop setup guide that provides it.
var probeTargets = []struct {
	key             CheckKey
	target, feature string
}{
	{CheckKeyBlockIOTargetLinear, "linear", "dm"},
	{CheckKeyBlockIOTargetDelay, "delay", "dm-delay"},
	{CheckKeyBlockIOTargetError, "error", "dm"},
}

var gateKinds = map[control.GateKind]Kind{control.GateImage: KindImage, control.GateEnvironment: KindEnvironment}

// record maps the probe's gates onto the report, by gate key. The targets
// are conclusive only when the probe read the registered target list. A gate
// whose observation failed is unknown and returns an error.
func record(probe control.Probe, report *Report) error {
	var failed []string
	report.set(CheckKeyBlockIOHelperStart, StateObserved, "", SourceHelperStart, "", "")
	for _, check := range probeChecks {
		gate := probe.Gate(check.gate)
		switch gate.State {
		case control.GateObserved:
			report.set(check.key, StateObserved, "", SourceHelperProbe, "", "")
		case control.GateUnavailable:
			reason := check.reason
			if len(gate.Missing) > 0 {
				reason = "the helper image lacks " + joinTools(gate.Missing)
			}
			report.set(check.key, StateUnavailable, gateKinds[gate.Kind], SourceHelperProbe, reason, check.guidance)
		case control.GateUnknown:
			report.set(check.key, StateUnknown, "", SourceHelperProbe, "the probe could not observe this gate", "")
			failed = append(failed, string(check.key))
		}
	}
	targets := probe.Gate(control.GateTargets)
	for _, check := range probeTargets {
		switch targets.State {
		case control.GateObserved:
			if slices.Contains(probe.Targets, check.target) {
				report.set(check.key, StateObserved, "", SourceHelperProbe, "", "")
			} else {
				report.set(check.key, StateUnavailable, KindEnvironment, SourceHelperProbe, "dmsetup targets does not list "+check.target, fmt.Sprintf(targetGuidance, check.target, check.feature))
			}
		case control.GateUnknown:
			report.set(check.key, StateUnknown, "", SourceHelperProbe, "dmsetup targets could not be read", "")
		}
	}
	if targets.State == control.GateUnknown {
		failed = append(failed, "the device-mapper target list")
	}
	if len(failed) > 0 {
		return failure(ceph.ErrNativeFailure, "the helper probe could not observe %s", strings.Join(failed, ", "))
	}
	return nil
}

// joinTools names the missing tools that the probe requires only, so that
// the report holds no text of the helper.
func joinTools(missing []string) string {
	var names []string
	for _, name := range missing {
		if slices.Contains(control.ProbeTools, name) {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return "a required tool"
	}
	return strings.Join(names, ", ")
}
