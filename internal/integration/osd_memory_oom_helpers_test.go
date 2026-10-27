//go:build all || integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"
)

// This file is the model of the first OSD memory-OOM step. One owned OSD, the
// acting primary of a single-PG replicated pool, is moved to a finite memory
// ceiling, then once to a predeclared lower ceiling that its resident memory
// exceeds. Everything is joined to one frozen container generation, daemon
// process and cgroup: the update transitions, the target-local kernel memory
// counters before and after, the engine's record of the daemon's exit, the
// engine's record of what happens next (no automatic restart), the OSD map and
// PG observations, the client writer's ledger and the explicit restart.
//
// The result of the pressure step is judged by its own verdict. An OSD that is
// killed by the cgroup OOM killer is reported as osd-oom-killed and is never
// labelled a survivable pressure scenario; a hit limit with a surviving OSD,
// an unreached limit, another or unknown victim, an unsupported controller and
// every shortfall of evidence keep their own distinct verdicts. Exit status
// 137 alone is never evidence. The counters memory.events.local are
// NON-hierarchical and do not name the victim, so the OSD is the proven victim
// only when it was the cgroup's only process, it is gone, the engine recorded
// an OOM exit of the same container generation inside the observed window and
// exactly one kill was counted.
//
// The model dispatches nothing and runs no workload; the Docker-free fixtures
// drive it with literal records. It does not decide what an OSD should do
// under a limit. It does not cover a MON or any other role, memory.high,
// smaller ceilings, a search for the smallest killing limit, supported
// defaults, a public API, other engines, or stable behavior across runs.

const (
	// Provisional experiment values, not supported Ceph defaults. There is one
	// pressure step and never a search for the limit at which something dies.
	osdOOMBaseline int64 = 2 << 30
	osdOOMCeiling  int64 = 64 << 20

	osdOOMExecutable = "ceph-osd"
	// osdOOMHoldMin is the shortest time the stopped container must be seen
	// staying stopped before any explicit start.
	osdOOMHoldMin = 5 * time.Second
	// osdOOMHeadroom: the ceiling must be this many times below the daemon's
	// resident memory for the step to be eligible, so that reclaim of page
	// cache alone does not decide the outcome.
	osdOOMHeadroom = 2
)

var errOSDOOM = errors.New("osd oom record is not usable")

// osdMemLimits are the inspected memory settings and restart policy.
type osdMemLimits struct {
	Memory, Swap  int64
	RestartPolicy string
}

// osdMemLimitsFromInspect copies the settings. A missing host configuration is
// a refusal, not an all-zero (unlimited) reading.
func osdMemLimitsFromInspect(c container.InspectResponse) (osdMemLimits, error) {
	if c.HostConfig == nil {
		return osdMemLimits{}, fmt.Errorf("%w: inspection has no host configuration", errOSDOOM)
	}
	return osdMemLimits{Memory: c.HostConfig.Memory, Swap: c.HostConfig.MemorySwap, RestartPolicy: string(c.HostConfig.RestartPolicy.Name)}, nil
}

// osdMemTransition is everything retained about one memory update: the
// request (memory and swap-plus-memory are always requested equal, which is
// no container swap), the reply, a fresh inspection and, when the task was
// running, the kernel reading of the same generation.
type osdMemTransition struct {
	Generation containerGeneration
	Requested  int64
	// At is the controller time the reply or error was received.
	At time.Time
	// ReplyReceived is false when the call returned an error or the reply was
	// lost: the outcome is then uncertain and is settled by inspecting the same
	// generation, never by replaying the request.
	ReplyReceived bool
	Warnings      []string
	AfterState    cpuEngineState
	AfterLimits   osdMemLimits
	Kernel        *memoryWitness
}

type osdMemVerdict string

const (
	osdMemApplied              osdMemVerdict = "applied"
	osdMemAppliedUncertainRply osdMemVerdict = "applied-uncertain-reply"
	// osdMemConfiguredOnly: the engine records the request and the task is not
	// running, or a readable kernel limit of the running generation does not
	// show it. Saved configuration is not an effective limit.
	osdMemConfiguredOnly osdMemVerdict = "configured-only"
	osdMemNotApplied     osdMemVerdict = "not-applied"
	osdMemUncertain      osdMemVerdict = "uncertain"
)

// osdMemChecks disables one rule per negative control.
type osdMemChecks struct {
	skipTime       bool
	skipGeneration bool
	skipWarnings   bool
	skipSwap       bool
	skipRunning    bool
	skipKernel     bool
	skipReply      bool
}

// classifyOSDMemTransition decides what a recorded update established.
func classifyOSDMemTransition(tr osdMemTransition, checks osdMemChecks) (osdMemVerdict, error) {
	if tr.Requested <= 0 {
		return osdMemUncertain, fmt.Errorf("%w: request is not a positive finite limit", errOSDOOM)
	}
	if !checks.skipTime && tr.At.IsZero() {
		return osdMemUncertain, fmt.Errorf("%w: the reply time is not recorded", errOSDOOM)
	}
	if !checks.skipGeneration {
		if !tr.Generation.valid() || tr.AfterState.ID != tr.Generation.ID || tr.AfterState.StartedAt != tr.Generation.StartedAt {
			return osdMemUncertain, fmt.Errorf("%w: generation changed or incomplete", errOSDOOM)
		}
		if tr.Kernel != nil && tr.Kernel.Generation != tr.Generation {
			return osdMemUncertain, fmt.Errorf("%w: kernel reading belongs to another generation", errOSDOOM)
		}
	}
	if !checks.skipWarnings && len(tr.Warnings) > 0 {
		return osdMemUncertain, fmt.Errorf("%w: update returned warnings", errOSDOOM)
	}
	if !checks.skipSwap && tr.AfterLimits.Memory == tr.Requested && tr.AfterLimits.Swap != tr.Requested {
		return osdMemUncertain, fmt.Errorf("%w: settings allow container swap", errOSDOOM)
	}
	s := tr.AfterState
	running := s.Running && !s.Paused && !s.Restarting
	inspected := tr.AfterLimits.Memory == tr.Requested && tr.AfterLimits.Swap == tr.Requested
	kernel := tr.Kernel != nil && tr.Kernel.ControllerListed && !tr.Kernel.At.IsZero()
	kernelMatches := kernel && tr.Kernel.Max == memoryLimit{Bytes: uint64(tr.Requested)} && tr.Kernel.SwapMax == memoryLimit{}
	if !inspected {
		if !running && !checks.skipRunning {
			return osdMemUncertain, fmt.Errorf("%w: no running task to read an effective limit from", errOSDOOM)
		}
		if !kernel && !checks.skipKernel {
			return osdMemUncertain, fmt.Errorf("%w: inspection lacks the request and no kernel reading is usable", errOSDOOM)
		}
		if kernelMatches && !checks.skipKernel {
			return osdMemUncertain, fmt.Errorf("%w: inspection and kernel disagree", errOSDOOM)
		}
		return osdMemNotApplied, nil
	}
	if !running && !checks.skipRunning {
		return osdMemConfiguredOnly, nil
	}
	if !checks.skipKernel {
		if !kernel {
			return osdMemUncertain, fmt.Errorf("%w: no usable kernel reading to settle a configured value", errOSDOOM)
		}
		if !kernelMatches {
			return osdMemConfiguredOnly, nil
		}
	}
	if !tr.ReplyReceived && !checks.skipReply {
		return osdMemAppliedUncertainRply, nil
	}
	return osdMemApplied, nil
}

// osdOOMFinal is the last reading of the target-local counters. Source says
// when it was taken relative to the end of the window: the cgroup of a dead
// daemon is normally removed, so an already-open event handle may or may not
// still be readable. A last in-window sample is weaker evidence than a reading
// after the stop and is recorded as such.
type osdOOMFinal struct {
	Source string
	Events memoryEvents
	At     time.Time
}

const (
	osdOOMFinalPostStop   = "post-stop-reading"
	osdOOMFinalLastSample = "last-in-window-sample"
)

// osdOOMStep is the one pressure step.
type osdOOMStep struct {
	Generation   containerGeneration
	PID          int
	ProcessStart string
	// Executable is the base name of the frozen process's executable as read
	// outside the target before the step.
	Executable    string
	ProcsBefore   memoryProcs
	OtherProcSeen bool

	// Baseline is the finite memory.max that must be installed before the step
	// and Ceiling the predeclared lower one.
	Baseline, Ceiling memoryLimit
	Before            *memoryWitness

	// ExecStart is taken immediately before the update is dispatched and
	// ExecEnd after the reply was handled and the stop (or its deadline) was
	// observed.
	ExecStart, ExecEnd time.Time
	// EffectiveAt is the first time the Ceiling and zero swap were read from
	// the target's own cgroup. Zero means the ceiling was never seen in force.
	EffectiveAt time.Time
	Final       *osdOOMFinal
	Exit        *memoryDaemonExit
}

type osdOOMVerdict string

const (
	// osdOOMKilled: every attribution rule held.
	osdOOMKilled osdOOMVerdict = "osd-oom-killed"
	// osdOOMNotReached: no counter advanced and the same OSD kept running.
	osdOOMNotReached osdOOMVerdict = "limit-not-reached"
	// osdOOMHitSurvived: the limit was hit without OOM handling and the OSD
	// survived. It is the only survivable outcome of this step.
	osdOOMHitSurvived osdOOMVerdict = "limit-hit-osd-survived"
	// osdOOMOther: a kill was counted but the OSD is not the proven victim.
	osdOOMOther       osdOOMVerdict = "other-or-unknown-victim"
	osdOOMUnsupported osdOOMVerdict = "unsupported"
	osdOOMIncomplete  osdOOMVerdict = "incomplete"
)

// osdOOMStepChecks disables one rule per negative control.
type osdOOMStepChecks struct {
	skipExecutable bool
	skipJoin       bool
	skipStamp      bool
	skipBracket    bool
	skipLimit      bool
	skipSwap       bool
	skipHeadroom   bool
	skipSingleton  bool
	skipExitJoin   bool
	skipEffective  bool
	skipFinalTime  bool
	skipRollback   bool
	skipConsist    bool
	skipExitWindow bool
	skipOOMFlag    bool
	skipKillCount  bool
	skipExitCode   bool
}

func osdOOMIncompleteStep(format string, args ...any) (osdOOMVerdict, error) {
	return osdOOMIncomplete, fmt.Errorf("%w: "+format, append([]any{errOSDOOM}, args...)...)
}

// classifyOSDOOMStep judges the pressure step. Missing or unobserved evidence
// is incomplete or unsupported and never an empty value that counts as
// evidence.
func classifyOSDOOMStep(s osdOOMStep, checks osdOOMStepChecks) (osdOOMVerdict, error) {
	if !s.Generation.valid() || s.PID <= 0 || s.ProcessStart == "" {
		return osdOOMIncompleteStep("frozen generation or process identity is incomplete")
	}
	if !checks.skipExecutable && s.Executable != osdOOMExecutable {
		return osdOOMIncompleteStep("the frozen process is not observed to be %s", osdOOMExecutable)
	}
	if s.Before == nil {
		return osdOOMUnsupported, nil
	}
	if !checks.skipJoin && s.Before.Generation != s.Generation {
		return osdOOMIncompleteStep("the first reading does not belong to the frozen generation")
	}
	if !s.Before.ControllerListed {
		return osdOOMUnsupported, nil
	}
	if s.Before.Source != memoryWitnessSource {
		return osdOOMIncompleteStep("reading source is not the target-local memory witness")
	}
	// Every time must exist before any ordering is judged: the zero time
	// orders before every real one.
	if !checks.skipStamp && (s.Before.At.IsZero() || s.ExecStart.IsZero() || s.ExecEnd.IsZero()) {
		return osdOOMIncompleteStep("a reading or window time is not recorded")
	}
	if !checks.skipBracket && (s.Before.At.After(s.ExecStart) || !s.ExecEnd.After(s.ExecStart)) {
		return osdOOMIncompleteStep("the first reading was not taken before the step or the window is not ordered")
	}
	if !checks.skipLimit {
		if s.Baseline.Unlimited || s.Baseline.Bytes == 0 || s.Ceiling.Unlimited || s.Ceiling.Bytes == 0 || s.Ceiling.Bytes >= s.Baseline.Bytes {
			return osdOOMIncompleteStep("baseline and ceiling are not finite with the ceiling below the baseline")
		}
		if s.Before.Max != s.Baseline {
			return osdOOMIncompleteStep("installed memory.max before the step differs from the admitted baseline")
		}
	}
	if !checks.skipSwap && s.Before.SwapMax != (memoryLimit{}) {
		return osdOOMIncompleteStep("swap is not bounded to zero before the step")
	}
	if !checks.skipHeadroom && s.Before.Current < osdOOMHeadroom*s.Ceiling.Bytes {
		return osdOOMIncompleteStep("resident memory %d is below %d times the ceiling %d", s.Before.Current, osdOOMHeadroom, s.Ceiling.Bytes)
	}
	if s.Exit == nil {
		return osdOOMIncompleteStep("the daemon outcome is unobserved")
	}
	if !checks.skipExitJoin && s.Exit.Generation != s.Generation {
		return osdOOMIncompleteStep("the daemon outcome belongs to another container generation")
	}
	if !checks.skipEffective && (s.EffectiveAt.IsZero() || s.EffectiveAt.Before(s.ExecStart) || s.EffectiveAt.After(s.ExecEnd)) {
		return osdOOMIncompleteStep("the ceiling was not observed in force inside the window")
	}
	f := s.Final
	if f == nil {
		return osdOOMIncompleteStep("the final counters are unobserved")
	}
	if !checks.skipFinalTime {
		if f.At.IsZero() {
			return osdOOMIncompleteStep("the final counter reading has no time")
		}
		switch f.Source {
		case osdOOMFinalLastSample:
			if f.At.Before(s.ExecStart) || f.At.After(s.ExecEnd) {
				return osdOOMIncompleteStep("the last sample is outside the window")
			}
		case osdOOMFinalPostStop:
			if f.At.Before(s.ExecEnd) {
				return osdOOMIncompleteStep("a post-stop reading precedes the end of the window")
			}
		default:
			return osdOOMIncompleteStep("unknown final reading source %q", f.Source)
		}
	}
	b, a := s.Before.Events, f.Events
	if !checks.skipRollback && (a.Max < b.Max || a.OOM < b.OOM || a.OOMKill < b.OOMKill) {
		return osdOOMIncompleteStep("a local counter decreased")
	}
	dMax, dOOM, dKill := a.Max-b.Max, a.OOM-b.OOM, a.OOMKill-b.OOMKill
	if !checks.skipConsist && ((dKill > 0 && dOOM == 0) || (dOOM > 0 && dMax == 0)) {
		return osdOOMIncompleteStep("local counters advanced inconsistently")
	}
	running := s.Exit.Running
	if running && s.Exit.OOMKilled && !checks.skipOOMFlag {
		return osdOOMIncompleteStep("the engine reports an OOM exit of a running daemon")
	}
	if dKill == 0 {
		switch {
		case running && dMax == 0 && dOOM == 0:
			return osdOOMNotReached, nil
		case running && dOOM == 0:
			return osdOOMHitSurvived, nil
		}
		// A stopped daemon, or OOM handling without a counted kill, is not a
		// classified outcome of this step; the last readable sample may simply
		// precede the kill.
		return osdOOMIncompleteStep("the daemon stopped or OOM handling ran without a counted kill")
	}
	if running {
		return osdOOMOther, nil
	}
	if !checks.skipExitWindow && (s.Exit.FinishedAt.IsZero() || s.Exit.FinishedAt.Before(s.ExecStart) || s.Exit.FinishedAt.After(s.ExecEnd)) {
		return osdOOMIncompleteStep("the daemon exit time is missing or outside the observed window")
	}
	if !s.ProcsBefore.Observed {
		return osdOOMIncompleteStep("cgroup membership before the step is unobserved")
	}
	daemon := checks.skipOOMFlag || s.Exit.OOMKilled
	if !checks.skipSingleton && (len(s.ProcsBefore.PIDs) != 1 || s.ProcsBefore.PIDs[0] != s.PID || s.OtherProcSeen) {
		daemon = false
	}
	if !checks.skipKillCount && dKill != 1 {
		daemon = false
	}
	if !checks.skipExitCode && s.Exit.ExitCode != 137 {
		daemon = false
	}
	if daemon {
		return osdOOMKilled, nil
	}
	return osdOOMOther, nil
}

// osdOOMHoldSample is one engine observation of the stopped container before
// any explicit start.
type osdOOMHoldSample struct {
	At           time.Time
	ID           string
	StartedAt    string
	Running      bool
	Restarting   bool
	RestartCount int
}

// osdOOMHeldStopped reports why the container is not shown to stay stopped on
// its own: the engine must not have restarted it, which is separate from the
// restart policy it was admitted with.
func osdOOMHeldStopped(samples []osdOOMHoldSample, gen containerGeneration, after time.Time) []string {
	var out []string
	if len(samples) < 3 {
		return []string{"fewer than three stopped-state observations"}
	}
	for _, s := range samples {
		if s.At.IsZero() {
			return []string{"a stopped-state observation has no time"}
		}
	}
	for _, s := range samples {
		switch {
		case s.ID != gen.ID || s.StartedAt != gen.StartedAt:
			out = append(out, "a stopped-state observation names another generation")
		case s.Running || s.Restarting || s.RestartCount != 0:
			out = append(out, "the engine restarted the daemon by itself")
		case s.At.Before(after):
			out = append(out, "a stopped-state observation precedes the end of the window")
		}
	}
	if span := samples[len(samples)-1].At.Sub(samples[0].At); span < osdOOMHoldMin {
		out = append(out, "the stopped state was not observed for the minimum span")
	}
	return out
}

// osdOOMEffects are the cluster and client observations around the outage.
type osdOOMEffects struct {
	// MapBefore is the target's OSD map entry before the step.
	MapBefore *osdMapEntry
	// Views were taken after the window ended and before the explicit start.
	Views []osdSlowLiveView
	// AckedBefore and AckedDuring are the live writer's acknowledged counts
	// at the start of the step and after the outage was observed.
	AckedBefore, AckedDuring int
	AckedProgressed          bool
}

// osdOOMRecovery is the explicit restart and what followed.
type osdOOMRecovery struct {
	Started     bool
	StartedAt   time.Time
	New         containerGeneration
	NewPID      int
	NewProcess  string
	Kernel      *memoryWitness
	MapAfter    *osdMapEntry
	Clean       bool
	AckedAfter  bool
	LedgerFinal bool
}

type osdOOMIdentity struct {
	Generation   containerGeneration
	PID          int
	ProcessStart string
}

// osdOOMRun is everything the scenario retains.
type osdOOMRun struct {
	Identity osdOOMIdentity
	Baseline int64
	Ceiling  int64
	// Admit moves the OSD from the unlimited creation state to the finite
	// baseline. Pressure is the one step. Restore re-saves the baseline on the
	// stopped container before the explicit start; it is configuration, never
	// an effective limit.
	Admit, Pressure, Restore osdMemTransition
	Step                     osdOOMStep
	Hold                     []osdOOMHoldSample
	Effects                  osdOOMEffects
	Recovery                 osdOOMRecovery
}

type osdOOMRunVerdict string

const (
	// osdOOMRunOOMKilled: the OSD was the proven OOM victim and every effect,
	// restart and recovery check holds. It is not a slow-live or survivable
	// pressure outcome.
	osdOOMRunOOMKilled osdOOMRunVerdict = "osd-oom-killed"
	// osdOOMRunOOMKilledUnfinished: the step attributed the OOM kill to the OSD
	// but a join, effect, restart or recovery check is missing.
	osdOOMRunOOMKilledUnfinished osdOOMRunVerdict = "osd-oom-killed-unfinished"
	osdOOMRunNotReached          osdOOMRunVerdict = "limit-not-reached"
	osdOOMRunHitSurvived         osdOOMRunVerdict = "limit-hit-osd-survived"
	osdOOMRunOther               osdOOMRunVerdict = "other-or-unknown-victim"
	osdOOMRunUnsupported         osdOOMRunVerdict = "unsupported"
	osdOOMRunInconclusive        osdOOMRunVerdict = "inconclusive"
)

func strictlyOrdered(times ...time.Time) bool {
	for i, t := range times {
		if t.IsZero() || (i > 0 && !t.After(times[i-1])) {
			return false
		}
	}
	return true
}

// osdOOMJoin refuses a run whose records do not all describe one target in one
// order. Every record is joined as a whole: generation, container and process
// identity across admission, mutation, window, hold and recovery, and every
// time is present before any ordering is judged.
func osdOOMJoin(r osdOOMRun) []string {
	var out []string
	id := r.Identity
	if !id.Generation.valid() || id.PID <= 0 || id.ProcessStart == "" {
		return []string{"frozen identity is incomplete"}
	}
	for name, tr := range map[string]osdMemTransition{"admit": r.Admit, "pressure": r.Pressure, "restore": r.Restore} {
		if tr.Generation != id.Generation || tr.AfterState.ID != id.Generation.ID || tr.AfterState.StartedAt != id.Generation.StartedAt {
			out = append(out, name+" transition does not name the frozen generation")
		}
		if name != "restore" && tr.AfterState.PID != id.PID && tr.AfterState.Running {
			out = append(out, name+" transition names another process")
		}
		if tr.Kernel != nil && tr.Kernel.Generation != id.Generation {
			out = append(out, name+" kernel reading names another generation")
		}
	}
	if r.Admit.Kernel == nil {
		out = append(out, "admission has no kernel reading")
	}
	if r.Admit.Requested != r.Baseline {
		out = append(out, "admission did not request the baseline")
	}
	if r.Pressure.Requested != r.Ceiling || r.Ceiling >= r.Baseline {
		out = append(out, "the pressure step did not request the predeclared lower ceiling")
	}
	if r.Restore.Requested != r.Baseline {
		out = append(out, "restoration did not request the admitted baseline")
	}
	s := r.Step
	if s.Generation != id.Generation || s.PID != id.PID || s.ProcessStart != id.ProcessStart {
		out = append(out, "the pressure window does not name the frozen process")
	}
	if s.Exit != nil && s.Exit.Generation != id.Generation {
		out = append(out, "the daemon outcome names another generation")
	}
	if s.Baseline != (memoryLimit{Bytes: uint64(r.Baseline)}) || s.Ceiling != (memoryLimit{Bytes: uint64(r.Ceiling)}) {
		out = append(out, "the pressure window expects limits other than the run's")
	}
	if s.Before != nil && s.Before.Generation != id.Generation {
		out = append(out, "the first reading names another generation")
	}
	if r.Admit.Kernel != nil && s.Before != nil {
		if !strictlyOrdered(r.Admit.At, r.Admit.Kernel.At, s.Before.At) || s.Before.At.After(s.ExecStart) {
			out = append(out, "admission and the first reading are not in the declared order")
		}
	} else {
		out = append(out, "admission or the first reading is missing")
	}
	if !strictlyOrdered(s.ExecStart, r.Pressure.At) || r.Pressure.At.After(s.ExecEnd) || !s.ExecEnd.After(s.ExecStart) {
		out = append(out, "the pressure reply is not inside the window")
	}
	if !strictlyOrdered(s.ExecEnd, r.Restore.At) {
		out = append(out, "restoration does not follow the window")
	}
	if p := r.Admit.AfterLimits.RestartPolicy; p != "" && p != string(container.RestartPolicyDisabled) {
		out = append(out, "the daemon was admitted with an automatic restart policy")
	}
	out = append(out, osdOOMHeldStopped(r.Hold, id.Generation, s.ExecEnd)...)
	if len(r.Hold) > 0 && r.Hold[len(r.Hold)-1].At.After(r.Restore.At) {
		out = append(out, "stopped-state observations continue after restoration")
	}
	return out
}

// osdOOMEffectReasons lists what the cluster and client observations lack.
func osdOOMEffectReasons(r osdOOMRun) []string {
	var out []string
	e := r.Effects
	if e.MapBefore == nil || !e.MapBefore.Up {
		out = append(out, "the OSD was not observed up in the map before the step")
	}
	down, pgSeen, pgAffected := false, false, false
	for _, v := range e.Views {
		if v.At.IsZero() {
			return append(out, "an outage observation has no time")
		}
		if !v.At.After(r.Step.ExecEnd) {
			out = append(out, "an outage observation precedes the end of the window")
			continue
		}
		if i := v.Instance; i != nil && (i.Running || i.StartedAt != r.Identity.Generation.StartedAt) {
			out = append(out, "an outage observation shows another or running instance")
		}
		if m := v.Map; m != nil && !m.Up {
			down = true
		}
		if v.PGRead {
			pgSeen = true
			if v.PGState != "active+clean" {
				pgAffected = true
			}
		}
	}
	if !down {
		out = append(out, "the OSD was never observed down in the map")
	}
	if !pgSeen || !pgAffected {
		out = append(out, "the PG was never observed leaving active+clean")
	}
	if !e.AckedProgressed || e.AckedDuring < e.AckedBefore+3 {
		out = append(out, "the live writer was not acknowledged again while the OSD was down")
	}
	return out
}

// osdOOMRecoveryReasons lists what the explicit restart did not establish.
func osdOOMRecoveryReasons(r osdOOMRun) []string {
	var out []string
	v := r.Recovery
	if !v.Started {
		return []string{"no explicit restart was dispatched"}
	}
	id := r.Identity
	if !strictlyOrdered(r.Restore.At, v.StartedAt) {
		out = append(out, "the new generation was not observed after restoration")
	}
	if v.New.ID != id.Generation.ID || !v.New.valid() || v.New.StartedAt == id.Generation.StartedAt {
		out = append(out, "the restart is not a new generation of the same container")
	}
	if v.NewPID <= 0 || v.NewPID == id.PID || v.NewProcess == "" || v.NewProcess == id.ProcessStart {
		out = append(out, "the restart did not produce a new process")
	}
	switch k := v.Kernel; {
	case k == nil || k.Generation != v.New || !k.ControllerListed || !strictlyOrdered(v.StartedAt, k.At):
		out = append(out, "the restarted generation has no joined kernel reading")
	case k.Max != (memoryLimit{Bytes: uint64(r.Baseline)}) || k.SwapMax != (memoryLimit{}):
		out = append(out, "the restarted generation does not run under the restored baseline and zero swap")
	}
	if m, b := v.MapAfter, r.Effects.MapBefore; m == nil || b == nil || !m.Up || m.UpFrom <= b.UpFrom {
		out = append(out, "the OSD did not rejoin the map with a newer boot epoch")
	}
	for name, ok := range map[string]bool{
		"cluster clean on every OSD":                 v.Clean,
		"live writer acknowledged after the restart": v.AckedAfter,
		"every acknowledged object read back intact": v.LedgerFinal,
	} {
		if !ok {
			out = append(out, "missing: "+name)
		}
	}
	return out
}

// classifyOSDOOMRun combines the step with the joins, effects, restart
// behavior and recovery. A killed OSD is never reported with a survivable
// label: osd-oom-killed needs every check, and anything short of it is
// osd-oom-killed-unfinished with its reasons.
func classifyOSDOOMRun(r osdOOMRun) (osdOOMRunVerdict, []string) {
	step, err := classifyOSDOOMStep(r.Step, osdOOMStepChecks{})
	var reasons []string
	if err != nil {
		reasons = append(reasons, err.Error())
	}
	switch step {
	case osdOOMNotReached:
		return osdOOMRunNotReached, reasons
	case osdOOMHitSurvived:
		return osdOOMRunHitSurvived, reasons
	case osdOOMOther:
		return osdOOMRunOther, reasons
	case osdOOMUnsupported:
		return osdOOMRunUnsupported, reasons
	case osdOOMIncomplete:
		return osdOOMRunInconclusive, reasons
	}
	reasons = append(reasons, osdOOMJoin(r)...)
	if v, err := classifyOSDMemTransition(r.Admit, osdMemChecks{}); v != osdMemApplied {
		reasons = append(reasons, fmt.Sprintf("admission is %s: %v", v, err))
	}
	if v, err := classifyOSDMemTransition(r.Pressure, osdMemChecks{}); v != osdMemApplied && v != osdMemAppliedUncertainRply && v != osdMemConfiguredOnly {
		reasons = append(reasons, fmt.Sprintf("pressure update is %s: %v", v, err))
	}
	if v, err := classifyOSDMemTransition(r.Restore, osdMemChecks{}); v != osdMemConfiguredOnly {
		reasons = append(reasons, fmt.Sprintf("restoration of the stopped container is %s, not saved configuration: %v", v, err))
	}
	reasons = append(reasons, osdOOMEffectReasons(r)...)
	reasons = append(reasons, osdOOMRecoveryReasons(r)...)
	slices.Sort(reasons)
	if len(reasons) == 0 {
		return osdOOMRunOOMKilled, nil
	}
	return osdOOMRunOOMKilledUnfinished, reasons
}

// osdOOMExecutableOf reads the executable base name of a process from a proc
// root. Permission and absence stay distinct from a wrong executable.
func osdOOMExecutableOf(procRoot string, pid int) (string, error) {
	exe, err := os.Readlink(filepath.Join(procRoot, strconv.Itoa(pid), "exe"))
	switch {
	case errors.Is(err, os.ErrPermission):
		return "", fmt.Errorf("%w: the controller may not read the executable", errOSDOOM)
	case err != nil:
		return "", fmt.Errorf("%w: executable unavailable", errOSDOOM)
	}
	return filepath.Base(exe), nil
}

// osdOOMObservation is what the recorder retained.
type osdOOMObservation struct {
	// Reads are the successful reads through the already-open event handle, in
	// order; the cgroup path may be gone while the handle still answers.
	Reads       []osdOOMRead
	EffectiveAt time.Time
	OtherProc   bool
	// Errors keeps the distinct failure messages, not one per tick.
	Errors []string
}

type osdOOMRead struct {
	At     time.Time
	Events memoryEvents
}

// final picks the closing reading for a window that ended at end: a read at or
// after the end is a post-stop reading, otherwise the last read is only the
// last in-window sample. No reads is nil, never zero counters.
func (o osdOOMObservation) final(end time.Time) *osdOOMFinal {
	if len(o.Reads) == 0 {
		return nil
	}
	last := o.Reads[len(o.Reads)-1]
	source := osdOOMFinalLastSample
	if !last.At.Before(end) {
		source = osdOOMFinalPostStop
	}
	return &osdOOMFinal{Source: source, Events: last.Events, At: last.At}
}

// recordOSDOOM samples the target from outside its cgroup until ctx ends: the
// open event handle (so teardown of the path does not hide the counters), the
// limit files by path, and the member list. It runs no process inside the
// target.
func recordOSDOOM(ctx context.Context, group string, handle *os.File, ceiling uint64, pid int, every time.Duration, now func() time.Time) <-chan osdOOMObservation {
	out := make(chan osdOOMObservation, 1)
	go func() {
		var obs osdOOMObservation
		note := func(err error) {
			if msg := err.Error(); !slices.Contains(obs.Errors, msg) && len(obs.Errors) < 8 {
				obs.Errors = append(obs.Errors, msg)
			}
		}
		tick := time.NewTicker(every)
		defer tick.Stop()
		for {
			at := now()
			if events, err := readOSDOOMHandle(handle); err == nil {
				obs.Reads = append(obs.Reads, osdOOMRead{At: at, Events: events})
			} else {
				note(err)
			}
			if obs.EffectiveAt.IsZero() {
				maxData, errMax := os.ReadFile(filepath.Join(group, "memory.max"))
				swapData, errSwap := os.ReadFile(filepath.Join(group, "memory.swap.max"))
				if errMax == nil && errSwap == nil {
					m, mErr := parseMemoryLimit(string(maxData), false)
					sw, sErr := parseMemoryLimit(string(swapData), true)
					if mErr == nil && sErr == nil && m == (memoryLimit{Bytes: ceiling}) && sw == (memoryLimit{}) {
						obs.EffectiveAt = at
					}
				}
			}
			if data, err := os.ReadFile(filepath.Join(group, "cgroup.procs")); err == nil {
				if procs, perr := parseCgroupProcs(string(data)); perr != nil {
					note(perr)
				} else if slices.ContainsFunc(procs.PIDs, func(p int) bool { return p != pid }) {
					obs.OtherProc = true
				}
			}
			select {
			case <-ctx.Done():
				out <- obs
				return
			case <-tick.C:
			}
		}
	}()
	return out
}

func readOSDOOMHandle(handle *os.File) (memoryEvents, error) {
	if _, err := handle.Seek(0, io.SeekStart); err != nil {
		return memoryEvents{}, errors.New("event handle unreadable")
	}
	data, err := io.ReadAll(io.LimitReader(handle, 4097))
	if err != nil || len(data) > 4096 {
		return memoryEvents{}, errors.New("event handle unreadable or oversized")
	}
	return parseMemoryEventsLocal(string(data))
}

// osdOOMEligible refuses the step before any update is dispatched.
func osdOOMEligible(before *memoryWitness, procs memoryProcs, pid int, baseline, ceiling int64) error {
	switch {
	case before == nil || !before.ControllerListed:
		return errors.New("no memory controller reading")
	case before.Max != (memoryLimit{Bytes: uint64(baseline)}) || before.SwapMax != (memoryLimit{}):
		return errors.New("the admitted finite baseline with zero swap is not installed")
	case !procs.Observed || len(procs.PIDs) != 1 || procs.PIDs[0] != pid:
		return errors.New("the daemon is not the cgroup's only process")
	case before.Current < osdOOMHeadroom*uint64(ceiling):
		return fmt.Errorf("resident memory %d does not exceed %d times the ceiling %d", before.Current, osdOOMHeadroom, ceiling)
	case before.Current >= uint64(baseline)/2:
		return errors.New("the baseline leaves too little headroom for the daemon")
	}
	return nil
}

func osdOOMReceiptStep(s osdOOMStep) map[string]any {
	out := map[string]any{"pid": s.PID, "process_start": s.ProcessStart, "executable": s.Executable, "other_process_seen": s.OtherProcSeen}
	if s.Before != nil {
		out["before_events"], out["before_current"] = s.Before.Events, s.Before.Current
	}
	if s.Final != nil {
		out["final_source"], out["final_events"] = s.Final.Source, s.Final.Events
	}
	if s.Exit != nil {
		out["exit"] = map[string]any{"running": s.Exit.Running, "oom_killed": s.Exit.OOMKilled, "exit_code": s.Exit.ExitCode}
	}
	out["effective_observed"] = !s.EffectiveAt.IsZero()
	return out
}

func osdOOMReceiptTransition(tr osdMemTransition) map[string]any {
	verdict, err := classifyOSDMemTransition(tr, osdMemChecks{})
	out := map[string]any{"requested": tr.Requested, "reply_received": tr.ReplyReceived, "warnings": len(tr.Warnings), "verdict": verdict,
		"inspected_memory": tr.AfterLimits.Memory, "inspected_swap": tr.AfterLimits.Swap, "restart_policy": tr.AfterLimits.RestartPolicy}
	if err != nil {
		out["error"] = err.Error()
	}
	if tr.Kernel != nil {
		out["kernel_max"], out["kernel_swap_max"] = tr.Kernel.Max, tr.Kernel.SwapMax
	}
	return out
}

// osdOOMViewSummary renders one observation for the receipt.
func osdOOMViewSummary(v osdSlowLiveView) string {
	parts := []string{v.At.UTC().Format("15:04:05.000")}
	if v.Instance != nil {
		parts = append(parts, fmt.Sprintf("running=%t oom=%t", v.Instance.Running, v.Instance.OOMKilled))
	}
	if v.Map != nil {
		parts = append(parts, fmt.Sprintf("up=%t in=%t up_from=%d down_at=%d", v.Map.Up, v.Map.In, v.Map.UpFrom, v.Map.DownAt))
	}
	if v.PGRead {
		parts = append(parts, fmt.Sprintf("pg=%s primary=%d", v.PGState, v.ActingPrimary))
	}
	if v.HealthRead {
		parts = append(parts, "health="+strings.Join(v.HealthCodes, ","))
	}
	if len(v.Errs) > 0 {
		parts = append(parts, "errors="+strings.Join(v.Errs, "; "))
	}
	return strings.Join(parts, " ")
}
