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

// errEngineUpdateStale refuses a dispatch before any engine call was made.
var errEngineUpdateStale = errors.New("engine update refused: target is not the running captured generation")

// guardedEngineUpdate inspects the target and dispatches a resource update
// only when it is still the running captured generation (same ID and start
// time, not paused, restarting or restarted). A refusal means zero update
// calls were made. This stops dispatch to an already-stale target; it does
// not make the engine's update atomic against a restart that happens after
// the check, which the caller's post-update fences still have to catch.
func guardedEngineUpdate(gen containerGeneration, inspectNow func() (container.InspectResponse, error), dispatch func() ([]string, error)) (warnings []string, err error) {
	current, err := inspectNow()
	if err != nil {
		return nil, fmt.Errorf("%w: inspection failed: %v", errEngineUpdateStale, err)
	}
	state, err := cpuEngineStateFromInspect(current, cpuEngineChecks{})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errEngineUpdateStale, err)
	}
	if err := state.Check(gen, 0); err != nil {
		return nil, fmt.Errorf("%w: %v", errEngineUpdateStale, err)
	}
	return dispatch()
}

// engineUpdateReply maps one update call result to the record fields. Any
// error, including a failure while decoding the reply body, makes the
// outcome uncertain: the caller settles it by inspecting the same generation
// and never by replaying the request.
func engineUpdateReply(warnings []string, err error) (replyReceived bool, copied []string) {
	copied = append([]string(nil), warnings...)
	return err == nil, copied
}
