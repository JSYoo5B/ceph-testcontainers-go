package preflight

import (
	"context"
	"errors"
	"net/url"
	"slices"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/api/types/system"
	"github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// Check keys, in report order.
const (
	keyEngine       = "engine.info"
	keyEngineOS     = "blockio.engine-os"
	keyHelperStart  = "blockio.helper-start"
	keyTools        = "blockio.image-tools"
	keyDrivers      = "blockio.drivers"
	keyControl      = "blockio.control-access"
	keyTargetLinear = "blockio.target.linear"
	keyTargetDelay  = "blockio.target.delay"
	keyTargetError  = "blockio.target.error"
)

// engineClient is the part of the Docker client that Observe uses.
//
//testdouble:dependency-port
type engineClient interface {
	Info(context.Context) (system.Info, error)
	DaemonHost() string
	Close() error
}

// clientEngine reads the engine through the Docker client directly, past
// testcontainers' cached and logged Info.
type clientEngine struct{ docker *client.Client }

func (e clientEngine) Info(ctx context.Context) (system.Info, error) {
	result, err := e.docker.Info(ctx, client.InfoOptions{})
	return result.Info, err
}

func (e clientEngine) DaemonHost() string { return e.docker.DaemonHost() }

func (e clientEngine) Close() error { return e.docker.Close() }

func blockIOCapabilities(capabilities []Capability) []Capability {
	var selected []Capability
	for _, capability := range []Capability{BlockIODelay, BlockIOEIO} {
		if slices.Contains(capabilities, capability) {
			selected = append(selected, capability)
		}
	}
	return selected
}

// plan lists the checks of the requested capabilities. Engine reachability
// is a gate of every capability.
func plan(blockIO []Capability) []Check {
	all := append([]Capability{Engine}, blockIO...)
	checks := []Check{{Key: keyEngine, Capabilities: all}}
	if len(blockIO) == 0 {
		return checks
	}
	for _, key := range []string{keyEngineOS, keyHelperStart, keyTools, keyDrivers, keyControl, keyTargetLinear} {
		checks = append(checks, Check{Key: key, Capabilities: slices.Clone(blockIO)})
	}
	if slices.Contains(blockIO, BlockIODelay) {
		checks = append(checks, Check{Key: keyTargetDelay, Capabilities: []Capability{BlockIODelay}})
	}
	if slices.Contains(blockIO, BlockIOEIO) {
		checks = append(checks, Check{Key: keyTargetError, Capabilities: []Capability{BlockIOEIO}})
	}
	return checks
}

const engineGuidance = "start the Docker engine that testcontainers selects, or select another one through DOCKER_HOST, the Docker context or the testcontainers configuration"

// inspect resolves the engine the way a fixture does and reads its facts.
// It reports whether the facts were read.
func (c checker) inspect(ctx context.Context, report *Report) (engineClient, bool, error) {
	newEngine := c.engine
	if newEngine == nil {
		newEngine = func(ctx context.Context) (engineClient, error) {
			docker, err := testcontainers.NewDockerClientWithOpts(ctx)
			if err != nil {
				return nil, err
			}
			return clientEngine{docker.Client}, nil
		}
	}
	engine, err := newEngine(ctx)
	switch {
	case err == nil:
	case ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		report.set(keyEngine, Unknown, "", "engine-info", "the engine selection was interrupted", engineGuidance)
		return nil, false, cause(ctx, err, ceph.ErrEnvironmentUnavailable, "select the engine")
	case onlyConnectionFailures(err):
		// testcontainers reads the information of each candidate endpoint
		// and caches the result for the process, so a fixture started next
		// fails the same way.
		report.set(keyEngine, Unavailable, EnvironmentKind, "engine-info", "testcontainers found no reachable engine", engineGuidance)
		return nil, false, nil
	default:
		// A candidate answered with an error, no candidate was found, or the
		// runtime provider failed: nothing proves the engine unreachable.
		report.set(keyEngine, Unknown, "", "engine-info", "testcontainers could not select an engine", engineGuidance)
		return nil, false, cause(ctx, err, ceph.ErrEnvironmentUnavailable, "select the engine")
	}
	report.Engine.Transport = transport(engine.DaemonHost())
	info, err := engine.Info(ctx)
	switch {
	case err == nil:
	case ctx.Err() == nil && client.IsErrConnectionFailed(err):
		report.set(keyEngine, Unavailable, EnvironmentKind, "engine-info", "the selected engine is not reachable", engineGuidance)
		return engine, false, nil
	default:
		report.set(keyEngine, Unknown, "", "engine-info", "the engine did not return its information", engineGuidance)
		return engine, false, cause(ctx, err, ceph.ErrEnvironmentUnavailable, "read the engine information")
	}
	report.Engine = facts(info, report.Engine.Transport)
	report.set(keyEngine, Observed, "", "engine-info", "", "")
	return engine, true, nil
}

// onlyConnectionFailures reports whether every failure that err joins is a
// failed connection to an engine. testcontainers wraps the failure of each
// candidate endpoint and joins them, so one candidate that answered with an
// error keeps the selection inconclusive.
func onlyConnectionFailures(err error) bool {
	switch e := err.(type) {
	case nil:
		return false
	case interface{ Unwrap() []error }:
		children := e.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !onlyConnectionFailures(child) {
				return false
			}
		}
		return true
	}
	if inner := errors.Unwrap(err); inner != nil && joins(inner) {
		return onlyConnectionFailures(inner)
	}
	return client.IsErrConnectionFailed(err)
}

// joins reports whether err or an error it wraps joins several errors.
func joins(err error) bool {
	for ; err != nil; err = errors.Unwrap(err) {
		if _, ok := err.(interface{ Unwrap() []error }); ok {
			return true
		}
	}
	return false
}

// transport keeps only the scheme of an engine endpoint.
func transport(host string) string {
	parsed, err := url.Parse(host)
	if err != nil {
		return ""
	}
	switch scheme := strings.ToLower(parsed.Scheme); scheme {
	case "unix", "npipe", "tcp", "ssh", "http", "https":
		return scheme
	}
	return ""
}

func facts(info system.Info, transport string) EngineFacts {
	f := EngineFacts{
		Transport:       transport,
		ServerVersion:   info.ServerVersion,
		OSType:          info.OSType,
		Architecture:    info.Architecture,
		KernelVersion:   info.KernelVersion,
		OperatingSystem: info.OperatingSystem,
		CgroupVersion:   info.CgroupVersion,
	}
	if strings.Contains(info.OperatingSystem, "Docker Desktop") {
		f.Desktop = new(true)
	}
	if strings.Contains(strings.ToLower(info.KernelVersion), "microsoft-standard-wsl2") {
		f.Backend = "wsl2"
	}
	if info.SecurityOptions != nil {
		rootless, userns := false, false
		for _, option := range info.SecurityOptions {
			for field := range strings.SplitSeq(option, ",") {
				switch field {
				case "name=rootless":
					rootless = true
				case "name=userns":
					userns = true
				}
			}
		}
		f.Rootless, f.UserNamespaces = &rootless, &userns
	}
	return f
}

// engineOS is the common gate that block I/O needs a Linux engine.
func engineOS(report *Report) {
	switch report.Engine.OSType {
	case "linux":
		report.set(keyEngineOS, Observed, "", "engine-info", "", "")
	case "":
		report.set(keyEngineOS, Unknown, "", "engine-info", "the engine did not report its operating system", "")
	default:
		report.set(keyEngineOS, Unavailable, EnvironmentKind, "engine-info", "block I/O faults need a Linux engine", "select a Linux engine; on Docker Desktop for Windows, switch to Linux containers")
	}
}
