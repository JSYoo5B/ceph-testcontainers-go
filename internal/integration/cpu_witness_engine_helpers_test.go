//go:build all || integration

package integration_test

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/moby/moby/api/types/container"
)

// This file is the engine adapter for the CPU witness binder. It converts one
// engine inspection into the cpuEngineState the fence compares and
// cross-checks an engine-configured CPU limit against the kernel's cpu.max.
// The conversion is pure, so the Docker-free fixtures drive it with
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

// cpuMaxMatchesNanoCPUs checks that the kernel's cpu.max expresses the engine's
// NanoCPUs setting exactly (quota/period equals nano/1e9). Docker derives the
// quota by integer division, so a value that does not divide evenly is
// reported as a mismatch rather than rounded. An unlimited kernel setting or a
// nonpositive engine setting never matches.
func cpuMaxMatchesNanoCPUs(max cpuMax, nano int64) error {
	if nano <= 0 {
		return fmt.Errorf("%w: engine CPU limit is not finite", errCPUWitnessInconclusive)
	}
	if max.Unlimited || max.Period == 0 || max.Quota == 0 {
		return fmt.Errorf("%w: kernel CPU limit is not finite", errCPUWitnessInconclusive)
	}
	left := new(big.Int).Mul(new(big.Int).SetUint64(max.Quota), big.NewInt(1_000_000_000))
	right := new(big.Int).Mul(big.NewInt(nano), new(big.Int).SetUint64(max.Period))
	if left.Cmp(right) != 0 {
		return fmt.Errorf("%w: kernel cpu.max does not match the engine CPU limit", errCPUWitnessInconclusive)
	}
	return nil
}
