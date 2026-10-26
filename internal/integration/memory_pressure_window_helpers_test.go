//go:build all || integration

package integration_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// This file judges one bounded memory-limit window on one frozen container
// generation: two target-local kernel readings (memory.max, memory.swap.max,
// memory.current and the NON-hierarchical memory.events.local) taken before and
// after one predeclared limit step, plus the independently recorded fate of the
// frozen daemon process. It dispatches nothing, reads no native cgroup of its
// own accord and runs no workload, so the Docker-free fixtures drive it with
// synthetic files and literal records. It is not a Ceph recovery oracle and it
// does not decide what a daemon should do under a limit.
//
// Counter semantics follow the cgroup v2 interface: "max" counts how often the
// limit was hit, "oom" how often the cgroup reached OOM handling, and
// "oom_kill" how many processes any OOM killer killed in it. None of them names
// the victim. A daemon is the proven victim only when its frozen process
// identity was the cgroup's only process, it is gone, the engine recorded an
// OOM exit for the same container generation inside the observed window, and
// exactly one kill was counted. Anything else with an advancing oom_kill is
// reported as another or unknown victim, never as a daemon OOM. Exit status
// 137 alone is not evidence.

const memoryWitnessSource = "cgroup-v2-local:memory"

var (
	errMemoryPressure          = errors.New("memory pressure window is not usable")
	errMemoryWitnessMalformed  = errors.New("memory witness file is malformed")
	errMemoryWitnessUnavail    = errors.New("memory witness file is unavailable")
	errMemoryWitnessGeneration = errors.New("memory witness generation is not comparable")
)

// memoryLimit is a memory.max or memory.swap.max setting.
type memoryLimit struct {
	Unlimited bool
	Bytes     uint64 // zero when Unlimited
}

// memoryEvents are the three required keys of memory.events.local.
type memoryEvents struct {
	Max, OOM, OOMKill uint64
}

// memoryWitness is one reading of one frozen container generation's cgroup.
type memoryWitness struct {
	Generation containerGeneration
	At         time.Time
	Source     string

	// ControllerListed reports that "memory" is in cgroup.controllers of the
	// target directory. Without it the memory.* files do not exist, so absence
	// is ineligibility and never a zero.
	ControllerListed bool
	Max, SwapMax     memoryLimit
	Current          uint64
	Events           memoryEvents
}

// parseMemoryLimit decodes "max" or an unsigned integer. A zero value is
// refused: the experiment only ever admits a finite positive ceiling, and
// memory.swap.max "0" is handled by the caller through allowZero.
func parseMemoryLimit(data string, allowZero bool) (memoryLimit, error) {
	if !strings.HasSuffix(data, "\n") || strings.Count(data, "\n") != 1 {
		return memoryLimit{}, fmt.Errorf("%w: limit needs exactly one line", errMemoryWitnessMalformed)
	}
	field := strings.TrimSuffix(data, "\n")
	if field == "max" {
		return memoryLimit{Unlimited: true}, nil
	}
	n, err := strconv.ParseUint(field, 10, 64)
	if err != nil || fmt.Sprint(n) != field || (n == 0 && !allowZero) {
		return memoryLimit{}, fmt.Errorf("%w: limit value", errMemoryWitnessMalformed)
	}
	return memoryLimit{Bytes: n}, nil
}

func parseMemoryCurrent(data string) (uint64, error) {
	field := strings.TrimSuffix(data, "\n")
	n, err := strconv.ParseUint(field, 10, 64)
	if !strings.HasSuffix(data, "\n") || err != nil || fmt.Sprint(n) != field {
		return 0, fmt.Errorf("%w: memory.current", errMemoryWitnessMalformed)
	}
	return n, nil
}

// parseMemoryEventsLocal reuses the merged local counter parser, which refuses
// duplicate, malformed and non-integer lines and requires max, oom and
// oom_kill. Extra keys (low, high, oom_group_kill) are ignored.
func parseMemoryEventsLocal(data string) (memoryEvents, error) {
	counters, err := daemonMemoryCounters(data)
	if err != nil {
		return memoryEvents{}, fmt.Errorf("%w: %v", errMemoryWitnessMalformed, err)
	}
	return memoryEvents{Max: counters["max"], OOM: counters["oom"], OOMKill: counters["oom_kill"]}, nil
}

// readMemoryWitness reads the files of one cgroup directory. The hierarchical
// memory.events is never read as a substitute for memory.events.local: a
// directory that only has the hierarchical file is unavailable.
func readMemoryWitness(gen containerGeneration, group string, now time.Time) (memoryWitness, error) {
	if !gen.valid() {
		return memoryWitness{}, fmt.Errorf("%w: frozen generation is incomplete", errMemoryWitnessGeneration)
	}
	read := func(name string) (string, error) {
		data, err := os.ReadFile(filepath.Join(group, name))
		if err != nil {
			return "", fmt.Errorf("%w: %s", errMemoryWitnessUnavail, name)
		}
		return string(data), nil
	}
	w := memoryWitness{Generation: gen, At: now, Source: memoryWitnessSource}
	text, err := read("cgroup.controllers")
	if err != nil {
		return memoryWitness{}, err
	}
	controllers, err := parseCgroupControllers(text)
	if err != nil {
		return memoryWitness{}, err
	}
	if !controllers["memory"] {
		return w, nil
	}
	w.ControllerListed = true
	if text, err = read("memory.max"); err != nil {
		return memoryWitness{}, err
	}
	if w.Max, err = parseMemoryLimit(text, false); err != nil {
		return memoryWitness{}, err
	}
	if text, err = read("memory.swap.max"); err != nil {
		return memoryWitness{}, err
	}
	if w.SwapMax, err = parseMemoryLimit(text, true); err != nil {
		return memoryWitness{}, err
	}
	if text, err = read("memory.current"); err != nil {
		return memoryWitness{}, err
	}
	if w.Current, err = parseMemoryCurrent(text); err != nil {
		return memoryWitness{}, err
	}
	if text, err = read("memory.events.local"); err != nil {
		return memoryWitness{}, err
	}
	if w.Events, err = parseMemoryEventsLocal(text); err != nil {
		return memoryWitness{}, err
	}
	return w, nil
}

// memoryProcs is the member list of cgroup.procs at one instant. Observed
// false means it was not read, which is different from an empty list.
type memoryProcs struct {
	Observed bool
	PIDs     []int
}

// memoryDaemonExit is the engine's separate record of the frozen container.
// It is nil on the window record when the outcome was not observed.
type memoryDaemonExit struct {
	Generation containerGeneration
	Running    bool
	OOMKilled  bool
	ExitCode   int
	FinishedAt time.Time
}

// memoryPressureRecord is one window.
type memoryPressureRecord struct {
	Generation containerGeneration
	// Expected is the finite memory.max that must be installed on both
	// readings. Swap must be bounded to zero on both.
	Expected memoryLimit

	// Daemon identity frozen before the step: the process id and its start
	// time as read outside the victim, and the cgroup members at that time.
	PID           int
	ProcessStart  string
	ProcsBefore   memoryProcs
	OtherProcSeen bool // any other member was seen at any sample of the window

	// ExecStart and ExecEnd are controller-clock times taken immediately
	// before the limit step is dispatched and after the window was observed.
	ExecStart, ExecEnd time.Time

	Before, After *memoryWitness
	Exit          *memoryDaemonExit
}

type memoryPressureVerdict string

const (
	// memoryLimitNotReached: no local counter advanced and the daemon is
	// still the running process of the same generation.
	memoryLimitNotReached memoryPressureVerdict = "limit-not-reached"
	// memoryLimitHitSurvived: the limit was hit without OOM handling and the
	// daemon survived (reclaim sufficed).
	memoryLimitHitSurvived memoryPressureVerdict = "limit-hit-daemon-survived"
	// memoryDaemonVictim: every attribution rule held.
	memoryDaemonVictim memoryPressureVerdict = "daemon-oom-victim"
	// memoryOtherVictim: a kill was counted but the daemon is not the proven
	// victim (helper killed, daemon alive, several kills, other exit cause).
	memoryOtherVictim memoryPressureVerdict = "other-or-unknown-victim"
	// memoryPressureUnsupported: no admitted memory-controller witness.
	memoryPressureUnsupported memoryPressureVerdict = "unsupported"
	// memoryPressureIncomplete: anything else.
	memoryPressureIncomplete memoryPressureVerdict = "incomplete"
)

// memoryPressureChecks disables one rule per negative control.
type memoryPressureChecks struct {
	skipJoin       bool
	skipBracket    bool
	skipLimit      bool
	skipSwap       bool
	skipRollback   bool
	skipConsist    bool
	skipExitJoin   bool
	skipExitWindow bool
	skipSingleton  bool
	skipOOMFlag    bool
	skipGone       bool
	skipKillCount  bool
}

func memoryIncomplete(format string, args ...any) (memoryPressureVerdict, error) {
	return memoryPressureIncomplete, fmt.Errorf("%w: "+format, append([]any{errMemoryPressure}, args...)...)
}

// classifyMemoryPressure judges one window. Missing or unobserved evidence is
// incomplete or unsupported and never an empty value that counts as evidence.
func classifyMemoryPressure(r memoryPressureRecord, checks memoryPressureChecks) (memoryPressureVerdict, error) {
	if !r.Generation.valid() {
		return memoryIncomplete("frozen generation is incomplete")
	}
	if r.Before == nil {
		return memoryPressureUnsupported, nil
	}
	if !checks.skipJoin && (r.Before.Generation != r.Generation || (r.After != nil && r.After.Generation != r.Generation)) {
		return memoryIncomplete("readings do not belong to the declared daemon generation")
	}
	if !r.Before.ControllerListed || (r.After != nil && !r.After.ControllerListed) {
		return memoryPressureUnsupported, nil
	}
	if r.After == nil {
		// A cgroup removed with the daemon can make the post-window reading
		// unreadable; that is an unobserved outcome, not a zero delta.
		return memoryIncomplete("post-window reading is unavailable")
	}
	if r.Before.Source != memoryWitnessSource || r.After.Source != memoryWitnessSource {
		return memoryIncomplete("reading source is not the target-local memory witness")
	}
	if !checks.skipBracket {
		switch {
		case r.ExecStart.IsZero() || !r.ExecEnd.After(r.ExecStart):
			return memoryIncomplete("window is not ordered")
		case r.Before.At.After(r.ExecStart):
			return memoryIncomplete("first reading was taken after the step was dispatched")
		case r.After.At.Before(r.ExecEnd):
			return memoryIncomplete("second reading was taken before the window ended")
		}
	}
	if !checks.skipLimit {
		if r.Expected.Unlimited || r.Expected.Bytes == 0 {
			return memoryIncomplete("expected limit is not finite")
		}
		if r.Before.Max != r.Expected || r.After.Max != r.Expected {
			return memoryIncomplete("installed memory.max differs from the expected one")
		}
	}
	if !checks.skipSwap && (r.Before.SwapMax != (memoryLimit{}) || r.After.SwapMax != (memoryLimit{})) {
		return memoryIncomplete("swap is not bounded to zero on both readings")
	}
	b, a := r.Before.Events, r.After.Events
	if !checks.skipRollback && (a.Max < b.Max || a.OOM < b.OOM || a.OOMKill < b.OOMKill) {
		return memoryIncomplete("local counter decreased")
	}
	dMax, dOOM, dKill := a.Max-b.Max, a.OOM-b.OOM, a.OOMKill-b.OOMKill
	// The kernel raises "max" when the limit is hit before it enters OOM
	// handling and counts a kill only inside OOM handling.
	if !checks.skipConsist && ((dKill > 0 && dOOM == 0) || (dOOM > 0 && dMax == 0)) {
		return memoryIncomplete("local counters advanced inconsistently")
	}
	if r.Exit == nil {
		return memoryIncomplete("daemon outcome is unobserved")
	}
	if !checks.skipExitJoin && r.Exit.Generation != r.Generation {
		return memoryIncomplete("daemon outcome belongs to another container generation")
	}

	if dKill == 0 {
		switch {
		case r.Exit.Running && dMax == 0 && dOOM == 0:
			return memoryLimitNotReached, nil
		case r.Exit.Running && dOOM == 0:
			return memoryLimitHitSurvived, nil
		}
		// A stopped daemon or OOM handling without a counted kill is not a
		// classified outcome of this window.
		return memoryIncomplete("daemon stopped or OOM handling ran without a counted kill")
	}

	// A kill was counted. Attribute it to the daemon only with every rule.
	if !checks.skipExitWindow && !r.Exit.Running && (r.Exit.FinishedAt.Before(r.ExecStart) || r.Exit.FinishedAt.After(r.ExecEnd)) {
		return memoryIncomplete("daemon exit time is outside the observed window")
	}
	if r.PID <= 0 || r.ProcessStart == "" || !r.ProcsBefore.Observed {
		return memoryIncomplete("daemon process identity or cgroup membership is unobserved")
	}
	daemon := checks.skipGone || !r.Exit.Running
	if !checks.skipOOMFlag && !r.Exit.OOMKilled {
		daemon = false
	}
	if !checks.skipSingleton && (len(r.ProcsBefore.PIDs) != 1 || r.ProcsBefore.PIDs[0] != r.PID || r.OtherProcSeen) {
		daemon = false
	}
	if !checks.skipKillCount && dKill != 1 {
		daemon = false
	}
	if daemon {
		return memoryDaemonVictim, nil
	}
	return memoryOtherVictim, nil
}
