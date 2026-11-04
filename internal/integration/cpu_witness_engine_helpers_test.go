//go:build all || integration

package integration_test

import (
	"errors"
	"fmt"

	"github.com/moby/moby/api/types/container"
)

// This file is the engine adapter for the cgroup witness binder. It converts
// one engine inspection into the cpuEngineState the fence compares. The
// conversion is pure, so the Docker-free fixtures drive it with
// synthetic inspection responses. Nothing here mutates a container.

var errCPUEngineState = errors.New("cpu engine state is not usable")

// cpuEngineChecks disables one conversion rule per negative control.
type cpuEngineChecks struct {
	// lenient turns missing inspection fields into empty values instead of
	// refusing them.
	lenient bool
}

// cpuEngineStateFromInspect copies the fence fields out of an inspection. A
// missing state block, ID or start time is a refusal: it must not decode into
// an empty state that could later compare equal to another empty state.
func cpuEngineStateFromInspect(c container.InspectResponse, checks cpuEngineChecks) (cpuEngineState, error) {
	if c.State == nil && !checks.lenient {
		return cpuEngineState{}, fmt.Errorf("%w: inspection has no state", errCPUEngineState)
	}
	var s cpuEngineState
	s.ID = c.ID
	s.RestartCount = c.RestartCount
	if c.State != nil {
		s.StartedAt = c.State.StartedAt
		s.PID = c.State.Pid
		s.Running = c.State.Running
		s.Paused = c.State.Paused
		s.Restarting = c.State.Restarting
	}
	if !checks.lenient && (s.ID == "" || s.StartedAt == "") {
		return cpuEngineState{}, fmt.Errorf("%w: inspection lacks identity", errCPUEngineState)
	}
	return s, nil
}
