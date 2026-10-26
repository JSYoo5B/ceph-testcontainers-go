//go:build all || integration

package integration_test

import (
	"errors"
	"fmt"

	"github.com/moby/moby/api/types/container"
)

// This file classifies one CPU quota update transition of a frozen container
// generation. It separates four things that an update call conflates: the
// requested field, the reply (error or warnings), the engine's inspected
// settings and the effective kernel cpu.max. It is a pure model over
// already-collected records, driven by Docker-free fixtures. No update is
// dispatched here, no engine is called and no native cgroup is read.
//
// Source basis (issue #65 comments, unconfirmed against any installed engine):
// the engine ignores zero-valued CPU fields on update, rejects mixing a
// positive NanoCPUs with already-set quota/period fields, and can answer
// success for a stopped or restarting target without applying the change to a
// running task. So restoring an originally unlimited (all-zero) baseline is
// not modeled, and an HTTP success or an inspected setting alone is never
// effective-limit evidence.

var (
	errCPUTransition = errors.New("cpu update transition is not established")
)

// cpuEngineLimits are the inspected CPU bandwidth settings. Exactly one
// encoding is used by this model: NanoCPUs with zero CPUQuota and CPUPeriod.
type cpuEngineLimits struct {
	NanoCPUs  int64
	CPUQuota  int64
	CPUPeriod int64
}

// cpuEngineLimitsFromInspect copies the settings. A missing host
// configuration is a refusal, not an all-zero (unlimited) reading.
func cpuEngineLimitsFromInspect(c container.InspectResponse) (cpuEngineLimits, error) {
	if c.HostConfig == nil {
		return cpuEngineLimits{}, fmt.Errorf("%w: inspection has no host configuration", errCPUTransition)
	}
	r := c.HostConfig.Resources
	return cpuEngineLimits{NanoCPUs: r.NanoCPUs, CPUQuota: r.CPUQuota, CPUPeriod: r.CPUPeriod}, nil
}

// cpuTransitionRecord is everything retained about one update.
type cpuTransitionRecord struct {
	Generation containerGeneration
	// RequestedNano is the only requested CPU field. Zero is not a request
	// this model accepts: the engine ignores it.
	RequestedNano int64

	// ReplyReceived is false when the call returned an error or the reply was
	// lost: the outcome is then uncertain and must be settled by inspection of
	// the same generation, never by replaying the request.
	ReplyReceived bool
	Warnings      []string

	// After* are taken after the reply, from the same frozen container ID.
	AfterState  cpuEngineState
	AfterLimits cpuEngineLimits
	// Kernel is the post-update local reading; nil when it was not taken.
	Kernel *cpuWitness
}

type cpuTransitionVerdict string

const (
	// cpuTransitionApplied: requested value is in the engine settings and in
	// the kernel cpu.max of the same running generation, reply clean.
	cpuTransitionApplied cpuTransitionVerdict = "applied"
	// cpuTransitionAppliedUncertainReply: effective evidence shows the request
	// took effect although the reply was lost. It is not a clean update.
	cpuTransitionAppliedUncertainReply cpuTransitionVerdict = "applied-uncertain-reply"
	// cpuTransitionConfiguredOnly: the engine records the value but the
	// target is not a running task or the kernel does not show it.
	cpuTransitionConfiguredOnly cpuTransitionVerdict = "configured-only"
	// cpuTransitionNotApplied: the settings and kernel show another value.
	cpuTransitionNotApplied cpuTransitionVerdict = "not-applied"
	// cpuTransitionUncertain: the evidence cannot decide.
	cpuTransitionUncertain cpuTransitionVerdict = "uncertain"
)

// cpuTransitionChecks disables one rule per negative control.
type cpuTransitionChecks struct {
	skipGeneration bool
	skipRunning    bool
	skipWarnings   bool
	skipEncoding   bool
	skipKernel     bool
	skipReply      bool
}

// classifyCPUTransition decides what a recorded update established.
func classifyCPUTransition(tr cpuTransitionRecord, checks cpuTransitionChecks) (cpuTransitionVerdict, error) {
	if tr.RequestedNano <= 0 {
		return cpuTransitionUncertain, fmt.Errorf("%w: request is not a positive finite limit", errCPUTransition)
	}
	if !checks.skipGeneration {
		if !tr.Generation.valid() || tr.AfterState.ID != tr.Generation.ID || tr.AfterState.StartedAt != tr.Generation.StartedAt {
			return cpuTransitionUncertain, fmt.Errorf("%w: generation changed or incomplete", errCPUTransition)
		}
		if tr.Kernel != nil && tr.Kernel.Generation != tr.Generation {
			return cpuTransitionUncertain, fmt.Errorf("%w: kernel reading belongs to another generation", errCPUTransition)
		}
	}
	if !checks.skipWarnings && len(tr.Warnings) > 0 {
		return cpuTransitionUncertain, fmt.Errorf("%w: update returned warnings", errCPUTransition)
	}
	if !checks.skipEncoding && (tr.AfterLimits.CPUQuota != 0 || tr.AfterLimits.CPUPeriod != 0) {
		return cpuTransitionUncertain, fmt.Errorf("%w: mixed CPU encoding in settings", errCPUTransition)
	}
	if tr.AfterLimits.NanoCPUs != tr.RequestedNano {
		return cpuTransitionNotApplied, nil
	}
	s := tr.AfterState
	if !checks.skipRunning && (!s.Running || s.Paused || s.Restarting) {
		return cpuTransitionConfiguredOnly, nil
	}
	if !checks.skipKernel {
		if tr.Kernel == nil || !tr.Kernel.ControllerListed {
			return cpuTransitionConfiguredOnly, nil
		}
		if err := cpuMaxMatchesNanoCPUs(tr.Kernel.Max, tr.RequestedNano); err != nil {
			return cpuTransitionConfiguredOnly, nil
		}
	}
	if !tr.ReplyReceived && !checks.skipReply {
		return cpuTransitionAppliedUncertainReply, nil
	}
	return cpuTransitionApplied, nil
}

// cpuRoundTripChecks disables one rule per negative control.
type cpuRoundTripChecks struct {
	skipFinite  bool
	skipTighter bool
	skipRestore bool
	skipSameGen bool
}

// verifyCPURoundTrip checks a finite baseline -> tighter -> same baseline
// sequence. baseline is the originally installed finite limit; both
// transitions must be clean applications on one generation, with the second
// request equal to the baseline exactly. Disposal is not restoration.
func verifyCPURoundTrip(baseline int64, tighten, restore cpuTransitionRecord, checks cpuRoundTripChecks) error {
	if !checks.skipFinite && baseline <= 0 {
		return fmt.Errorf("%w: baseline is not a finite limit", errCPUTransition)
	}
	if !checks.skipTighter && !(tighten.RequestedNano > 0 && tighten.RequestedNano < baseline) {
		return fmt.Errorf("%w: first request is not strictly tighter than the baseline", errCPUTransition)
	}
	if !checks.skipRestore && restore.RequestedNano != baseline {
		return fmt.Errorf("%w: restore request differs from the baseline", errCPUTransition)
	}
	if !checks.skipSameGen && tighten.Generation != restore.Generation {
		return fmt.Errorf("%w: transitions belong to different generations", errCPUTransition)
	}
	for name, tr := range map[string]cpuTransitionRecord{"tighten": tighten, "restore": restore} {
		v, err := classifyCPUTransition(tr, cpuTransitionChecks{})
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		if v != cpuTransitionApplied {
			return fmt.Errorf("%w: %s is %s", errCPUTransition, name, v)
		}
	}
	return nil
}
