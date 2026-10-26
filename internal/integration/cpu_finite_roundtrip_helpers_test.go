//go:build all || integration

package integration_test

import (
	"errors"
	"fmt"

	"github.com/moby/moby/api/types/container"
)

// This file assembles the transition record of one CPU quota update from
// already-collected observations. It dispatches nothing: the native scenario
// calls the engine and passes the raw results here, so the Docker-free
// fixtures drive the same assembly over synthetic values.

var errCPURoundTripRecord = errors.New("cpu round trip record is not usable")

// cpuUpdateReply maps one update call result to the record fields. Any error,
// including a failure while decoding the reply body, makes the outcome
// uncertain: the caller settles it by inspecting the same generation and never
// by replaying the request.
func cpuUpdateReply(warnings []string, err error) (replyReceived bool, copied []string) {
	copied = append([]string(nil), warnings...)
	return err == nil, copied
}

// assembleCPUTransition builds the record of one update from the reply fields,
// a fresh inspection of the same container ID and the kernel reading taken
// after the update. A missing inspection state or host configuration is a
// refusal, never an all-zero reading. A missing kernel reading stays nil, which
// the classifier treats as unavailable evidence.
func assembleCPUTransition(gen containerGeneration, requested int64, replyReceived bool, warnings []string, inspected container.InspectResponse, kernel *cpuWitness) (cpuTransitionRecord, error) {
	state, err := cpuEngineStateFromInspect(inspected, cpuEngineChecks{})
	if err != nil {
		return cpuTransitionRecord{}, fmt.Errorf("%w: %v", errCPURoundTripRecord, err)
	}
	limits, err := cpuEngineLimitsFromInspect(inspected)
	if err != nil {
		return cpuTransitionRecord{}, fmt.Errorf("%w: %v", errCPURoundTripRecord, err)
	}
	return cpuTransitionRecord{
		Generation:    gen,
		RequestedNano: requested,
		ReplyReceived: replyReceived,
		Warnings:      append([]string(nil), warnings...),
		AfterState:    state,
		AfterLimits:   limits,
		Kernel:        kernel,
	}, nil
}

// errCPUUpdateStale refuses a dispatch before any engine call was made.
var errCPUUpdateStale = errors.New("cpu update refused: target is not the running captured generation")

// guardedCPUUpdate inspects the target and dispatches the update only when it
// is still the running captured generation (same ID and start time, not
// paused, restarting or restarted). A refusal means zero update calls were
// made. This stops dispatch to an already-stale target; it does not make the
// engine's update atomic against a restart that happens after the check, which
// the caller's post-update fences still have to catch.
func guardedCPUUpdate(gen containerGeneration, inspectNow func() (container.InspectResponse, error), dispatch func() ([]string, error)) (warnings []string, err error) {
	current, err := inspectNow()
	if err != nil {
		return nil, fmt.Errorf("%w: inspection failed: %v", errCPUUpdateStale, err)
	}
	state, err := cpuEngineStateFromInspect(current, cpuEngineChecks{})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errCPUUpdateStale, err)
	}
	if err := state.check(gen, 0); err != nil {
		return nil, fmt.Errorf("%w: %v", errCPUUpdateStale, err)
	}
	return dispatch()
}
