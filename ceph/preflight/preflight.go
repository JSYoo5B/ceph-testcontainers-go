package preflight

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// SchemaVersion is the Report.SchemaVersion of this package's reports. It
// changes when a field or check key changes its meaning.
const SchemaVersion = 1

const operationTimeout = 2 * time.Minute

// Capability names one fixture capability that Observe checks the
// prerequisites of.
type Capability string

const (
	// CapabilityEngine is the Docker engine that testcontainers selects for
	// this process, which every fixture uses. Observe always observes it.
	CapabilityEngine Capability = "engine"
	// CapabilityBlockIODelay is a blockio.Profile with a Delay: the
	// prerequisites of blockio.Run and of applying the device-mapper delay
	// target.
	CapabilityBlockIODelay Capability = "blockio.delay"
	// CapabilityBlockIOEIO is a blockio.Profile with EIO: the prerequisites of
	// blockio.Run and of applying the device-mapper error target.
	CapabilityBlockIOEIO Capability = "blockio.eio"
)

// Level selects how far Observe looks.
type Level string

const (
	// LevelInspect reads the engine's metadata only. It starts no container,
	// pulls no image and changes nothing; the checks that need a helper
	// container are StateNotRun. The zero Level is LevelInspect.
	LevelInspect Level = "inspect"
	// LevelHelper also starts one owned temporary container from
	// Request.HelperImage, pulling it when the engine lacks it, with the
	// privileges and device access of the operation it checks, runs a
	// read-only probe in it and removes it.
	LevelHelper Level = "helper"
)

// Request selects what Observe checks. Capabilities lists the capabilities
// to check, each at most once; CapabilityEngine is always checked.
// HelperImage is the image a LevelHelper call runs, such as
// blockio.HelperImage; it is used as given, and is required at LevelHelper
// with a block I/O capability and refused otherwise.
type Request struct {
	Capabilities []Capability
	Level        Level
	HelperImage  string
}

// State is the conclusion of one check or one capability.
type State string

const (
	// StateObserved means that the prerequisite was observed. It does not
	// certify that the fixture's operation, its effect or a recovery will
	// succeed.
	StateObserved State = "observed"
	// StateUnavailable means that an observation proved the prerequisite
	// missing.
	StateUnavailable State = "unavailable"
	// StateUnknown means that the observation failed or was inconclusive: it
	// proves neither presence nor absence.
	StateUnknown State = "unknown"
	// StateNotRun means that the check was not run, at this Level or because
	// a gate it depends on was not observed.
	StateNotRun State = "not-run"
)

// Kind is the error category of a StateUnavailable check.
type Kind string

const (
	// KindEnvironment is a missing engine, kernel or privilege prerequisite,
	// which an operation reports as ceph.ErrEnvironmentUnavailable.
	KindEnvironment Kind = "environment"
	// KindImage is a missing prerequisite of the helper image, which an
	// operation reports as ceph.ErrImageCapability.
	KindImage Kind = "image"
)

// CheckKey names one check. A key is stable across releases of the same
// SchemaVersion. The engine check belongs to every capability; the other
// checks belong to the block I/O capabilities, the delay target to
// CapabilityBlockIODelay only and the error target to CapabilityBlockIOEIO
// only.
type CheckKey string

// The checks of a report, in report order.
const (
	// CheckKeyEngineInfo is the selection of the engine and the read of its
	// information.
	CheckKeyEngineInfo CheckKey = "engine.info"
	// CheckKeyBlockIOEngineOS is a Linux engine.
	CheckKeyBlockIOEngineOS CheckKey = "blockio.engine-os"
	// CheckKeyBlockIOHelperStart is the start of the helper with the
	// keeper's privileged configuration.
	CheckKeyBlockIOHelperStart CheckKey = "blockio.helper-start"
	// CheckKeyBlockIOImageTools is the tools of the helper image.
	CheckKeyBlockIOImageTools CheckKey = "blockio.image-tools"
	// CheckKeyBlockIODrivers is the device-mapper and loop drivers of the
	// engine kernel.
	CheckKeyBlockIODrivers CheckKey = "blockio.drivers"
	// CheckKeyBlockIOControlAccess is read and write access to the
	// device-mapper and loop control devices.
	CheckKeyBlockIOControlAccess CheckKey = "blockio.control-access"
	// CheckKeyBlockIOTargetLinear is the registered device-mapper linear
	// target.
	CheckKeyBlockIOTargetLinear CheckKey = "blockio.target.linear"
	// CheckKeyBlockIOTargetDelay is the registered device-mapper delay
	// target.
	CheckKeyBlockIOTargetDelay CheckKey = "blockio.target.delay"
	// CheckKeyBlockIOTargetError is the registered device-mapper error
	// target.
	CheckKeyBlockIOTargetError CheckKey = "blockio.target.error"
)

// Source names the observation that concluded a check.
type Source string

const (
	// SourceEngineInfo is the engine's information.
	SourceEngineInfo Source = "engine-info"
	// SourceHelperStart is the start of the helper container.
	SourceHelperStart Source = "helper-start"
	// SourceHelperProbe is the probe run in the helper container.
	SourceHelperProbe Source = "helper-probe"
)

// Check is the result of one prerequisite check. Capabilities are the
// requested capabilities that depend on it. Reason and Guidance are fixed
// texts of this package; they hold no engine or host detail.
type Check struct {
	Key          CheckKey     `json:"key"`
	Capabilities []Capability `json:"capabilities"`
	State        State        `json:"state"`
	Kind         Kind         `json:"kind,omitempty"`
	Source       Source       `json:"source,omitempty"`
	Reason       string       `json:"reason,omitempty"`
	Guidance     string       `json:"guidance,omitempty"`
}

// Platform is an operating system and architecture.
type Platform struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
}

// EngineFacts are the allowlisted facts of the selected engine. An empty
// string or a nil pointer is unknown. Transport is the scheme of the engine
// endpoint (unix, npipe, tcp, ssh, http or https), never the endpoint itself.
// Desktop is set once the engine information was read: true when the engine
// reports Docker Desktop, false otherwise. Backend is wsl2 when the kernel
// reports a WSL 2 kernel. Rootless and UserNamespaces come from the engine's
// security options.
type EngineFacts struct {
	Transport       string `json:"transport,omitempty"`
	ServerVersion   string `json:"server_version,omitempty"`
	OSType          string `json:"os_type,omitempty"`
	Architecture    string `json:"architecture,omitempty"`
	KernelVersion   string `json:"kernel_version,omitempty"`
	OperatingSystem string `json:"operating_system,omitempty"`
	CgroupVersion   string `json:"cgroup_version,omitempty"`
	Desktop         *bool  `json:"desktop,omitempty"`
	Backend         string `json:"backend,omitempty"`
	Rootless        *bool  `json:"rootless,omitempty"`
	UserNamespaces  *bool  `json:"user_namespaces,omitempty"`
}

// HelperRun records the helper container of a LevelHelper call.
// ContainerID is set once the engine created it. Released means that its
// removal was confirmed; Cleanup is a fixed reason when it was not.
type HelperRun struct {
	Started     bool   `json:"started"`
	ContainerID string `json:"container_id,omitempty"`
	Released    bool   `json:"released"`
	Cleanup     string `json:"cleanup,omitempty"`
}

// Report is one JSON-serializable snapshot of an Observe call. Process is the
// platform of the calling Go process, which can differ from the engine's.
// Checks hold every check of the requested capabilities, in a stable order.
// A Report holds no endpoint, host name, path, credential, environment or
// raw engine response, so it can be shared as it is.
type Report struct {
	SchemaVersion int          `json:"schema_version"`
	Level         Level        `json:"level"`
	Capabilities  []Capability `json:"capabilities"`
	Process       Platform     `json:"process"`
	Engine        EngineFacts  `json:"engine"`
	Checks        []Check      `json:"checks"`
	Helper        *HelperRun   `json:"helper,omitempty"`
}

// Verdict is the conclusion for capability: StateUnavailable when a check it
// depends on is StateUnavailable, otherwise StateUnknown when one is
// StateUnknown or StateNotRun, otherwise StateObserved. A capability that was
// not requested is StateNotRun.
//
//purpose:observation
func (r *Report) Verdict(capability Capability) State {
	if r == nil || !slices.Contains(r.Capabilities, capability) {
		return StateNotRun
	}
	verdict := StateObserved
	for _, check := range r.Checks {
		if !slices.Contains(check.Capabilities, capability) {
			continue
		}
		switch check.State {
		case StateUnavailable:
			return StateUnavailable
		case StateUnknown, StateNotRun:
			verdict = StateUnknown
		}
	}
	return verdict
}

// Lookup returns the check named key.
//
//purpose:observation
func (r *Report) Lookup(key CheckKey) (Check, bool) {
	if r == nil {
		return Check{}, false
	}
	for _, check := range r.Checks {
		if check.Key == key {
			return check, true
		}
	}
	return Check{}, false
}

func failure(kind error, format string, args ...any) error {
	return &ceph.OperationError{Kind: kind, Op: "preflight", Err: fmt.Errorf(format, args...)}
}

// Observe observes the prerequisites of the requested capabilities on the
// engine that testcontainers selects for this process, the one a fixture
// started next would use, before any fixture starts. It returns a report,
// also a partial one, with an error when an observation could not be made
// or the helper could not be removed. Every StateUnknown check that a failed
// observation left comes with such an error; a StateUnavailable or
// StateNotRun check does not. A cancelled or expired ctx is kept in the error, which holds no
// engine detail. Observe gives its work a two-minute deadline; an earlier
// caller deadline takes precedence. Each call observes again: the effective
// environment can change between calls.
//
// The caller decides from Report.Verdict whether to fail, skip or go on, and
// owns logging, files and uploads of the report.
//
//purpose:observation
func Observe(ctx context.Context, request Request) (*Report, error) {
	return checker{}.check(ctx, request)
}

// checker holds the substitutes of Observe's engine and helper.
type checker struct {
	// engine replaces the engine client in tests; nil uses testcontainers.
	//
	//testdouble:nil-default-hook
	engine func(context.Context) (engineClient, error)
	// helper replaces the helper container in tests; nil uses testcontainers.
	//
	//testdouble:nil-default-hook
	helper func(engineClient) helperRunner
}

func (c checker) check(ctx context.Context, request Request) (*Report, error) {
	if err := validate(&request); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	report := &Report{
		SchemaVersion: SchemaVersion,
		Level:         request.Level,
		Capabilities:  append([]Capability{CapabilityEngine}, request.Capabilities...),
		Process:       Platform{OS: runtime.GOOS, Architecture: runtime.GOARCH},
	}
	blockIO := blockIOCapabilities(request.Capabilities)
	report.Checks = plan(blockIO)
	var errs []error
	engine, info, err := c.inspect(ctx, report)
	if engine != nil {
		defer func() { _ = engine.Close() }()
	}
	if err != nil {
		errs = append(errs, err)
	}
	if info && len(blockIO) > 0 {
		if err := engineOS(report); err != nil {
			errs = append(errs, err)
		}
		if request.Level == LevelHelper && report.state(CheckKeyBlockIOEngineOS) == StateObserved {
			errs = append(errs, c.runHelper(ctx, engine, request.HelperImage, report)...)
		}
	}
	notRun(report)
	return report, errors.Join(errs...)
}

func validate(request *Request) error {
	if request.Level == "" {
		request.Level = LevelInspect
	}
	if request.Level != LevelInspect && request.Level != LevelHelper {
		return failure(ceph.ErrInvalidRequest, "unknown level %q", request.Level)
	}
	seen := map[Capability]bool{}
	var capabilities []Capability
	for _, capability := range request.Capabilities {
		if capability == CapabilityEngine {
			continue
		}
		if capability != CapabilityBlockIODelay && capability != CapabilityBlockIOEIO {
			return failure(ceph.ErrInvalidRequest, "unknown capability %q", capability)
		}
		if seen[capability] {
			return failure(ceph.ErrInvalidRequest, "capability %q is requested twice", capability)
		}
		seen[capability] = true
		capabilities = append(capabilities, capability)
	}
	request.Capabilities = capabilities
	needsImage := request.Level == LevelHelper && len(blockIOCapabilities(request.Capabilities)) > 0
	switch {
	case needsImage && request.HelperImage == "":
		return failure(ceph.ErrInvalidRequest, "a helper level block I/O call needs a helper image")
	case !needsImage && request.HelperImage != "":
		return failure(ceph.ErrInvalidRequest, "a helper image is used only by a helper level block I/O call")
	}
	return nil
}

func (r *Report) set(key CheckKey, state State, kind Kind, source Source, reason, guidance string) {
	for i := range r.Checks {
		if r.Checks[i].Key == key {
			r.Checks[i].State, r.Checks[i].Kind, r.Checks[i].Source = state, kind, source
			r.Checks[i].Reason, r.Checks[i].Guidance = reason, guidance
			return
		}
	}
}

func (r *Report) state(key CheckKey) State {
	check, _ := r.Lookup(key)
	return check.State
}

// notRun gives every check that no observation reached its reason.
func notRun(r *Report) {
	for i := range r.Checks {
		check := &r.Checks[i]
		if check.State != "" {
			continue
		}
		check.State = StateNotRun
		switch {
		case r.Level == LevelInspect && check.Key != CheckKeyEngineInfo && check.Key != CheckKeyBlockIOEngineOS:
			check.Reason = "needs a helper level check"
		default:
			check.Reason = "a gate it depends on was not observed"
		}
	}
}

// interrupted reports whether ctx ended or err is a context error.
func interrupted(ctx context.Context, err error) bool {
	return ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// cause keeps ctx's error, or err's when it is a context error, and replaces
// any other error by a fixed text, so that no engine detail leaves Observe.
func cause(ctx context.Context, err error, kind error, text string) error {
	if !interrupted(ctx, err) {
		return failure(kind, "%s", text)
	}
	ctxErr := ctx.Err()
	switch {
	case ctxErr != nil:
	case errors.Is(err, context.Canceled):
		ctxErr = context.Canceled
	default:
		ctxErr = context.DeadlineExceeded
	}
	return fmt.Errorf("preflight: %s: %w", text, ctxErr)
}
