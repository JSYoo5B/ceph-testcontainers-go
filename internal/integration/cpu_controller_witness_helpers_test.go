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

// This file is a test-only, target-local CPU-controller eligibility witness
// for one cgroup v2 directory. It exists because the Docker stats projection
// cannot say whether CPU throttling counters were available: the engine
// conversion fills value-typed throttling fields only when its producer has
// CPU input, and an absent producer input encodes the same all-zero JSON as an
// observed zero (see the container accounting adapter and issue #65).
//
// The reader works on a cgroup directory path, so the Docker-free fixtures
// drive the real reader over synthetic files. The kernel file formats follow
// the cgroup v2 interface documentation (cgroup.controllers, cpu.max and
// cpu.stat). No native cgroup was read by this change and no live binding to a
// container cgroup is provided here: binding must reuse the frozen
// container/start/PID generation fence of the memory qualification.

const cpuWitnessSource = "cgroup-v2-local:cpu"

// cpuMax is the installed bandwidth setting. A finite quota is only a
// request; it is never throttling evidence by itself.
type cpuMax struct {
	Unlimited bool
	Quota     uint64 // microseconds per period, zero when Unlimited
	Period    uint64 // microseconds
}

// cpuStatCounters are the throttling keys of cpu.stat. The units are the
// kernel's: periods and throttled periods are counts, throttled time is
// microseconds (the API projection multiplies it into nanoseconds).
type cpuStatCounters struct {
	Periods, Throttled, ThrottledUsec uint64
}

// cpuWitness is one reading of one frozen container generation's cgroup.
type cpuWitness struct {
	Generation containerGeneration
	At         time.Time
	Source     string

	// ControllerListed reports that "cpu" is in cgroup.controllers of the
	// target directory. Without it cpu.max and the throttling keys of cpu.stat
	// do not exist, so absence of the keys is ineligibility, not zero.
	ControllerListed bool
	Max              cpuMax
	Stat             cpuStatCounters
}

var (
	errCPUWitnessMalformed    = errors.New("cpu witness file is malformed")
	errCPUWitnessIneligible   = errors.New("cpu controller is not enabled for the target cgroup")
	errCPUWitnessUnavailable  = errors.New("cpu witness file is unavailable")
	errCPUWitnessGeneration   = errors.New("cpu witness generation is not comparable")
	errCPUWitnessInconclusive = errors.New("cpu witness is inconclusive")
)

// parseCgroupControllers decodes cgroup.controllers: space separated names,
// possibly empty. An empty list is a valid, ineligible reading.
func parseCgroupControllers(data string) (map[string]bool, error) {
	if data != "" && !strings.HasSuffix(data, "\n") {
		return nil, fmt.Errorf("%w: controllers without a final newline", errCPUWitnessMalformed)
	}
	out := map[string]bool{}
	for _, name := range strings.Fields(data) {
		if out[name] {
			return nil, fmt.Errorf("%w: duplicate controller %q", errCPUWitnessMalformed, name)
		}
		out[name] = true
	}
	return out, nil
}

// parseCPUMax decodes "<quota|max> <period>". Anything else, including a zero
// period, a zero quota, a single field or trailing fields, is refused.
func parseCPUMax(data string) (cpuMax, error) {
	fields := strings.Fields(data)
	if len(fields) != 2 || !strings.HasSuffix(data, "\n") {
		return cpuMax{}, fmt.Errorf("%w: cpu.max needs two fields", errCPUWitnessMalformed)
	}
	period, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil || period == 0 {
		return cpuMax{}, fmt.Errorf("%w: cpu.max period", errCPUWitnessMalformed)
	}
	if fields[0] == "max" {
		return cpuMax{Unlimited: true, Period: period}, nil
	}
	quota, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil || quota == 0 {
		return cpuMax{}, fmt.Errorf("%w: cpu.max quota", errCPUWitnessMalformed)
	}
	return cpuMax{Quota: quota, Period: period}, nil
}

// parseCPUStat decodes cpu.stat. The three throttling keys must all be present
// exactly once and be unsigned integers. When none of them is present the
// controller is not exposing bandwidth statistics and the result is
// errCPUWitnessIneligible; a partial set is malformed.
func parseCPUStat(data string) (cpuStatCounters, error) {
	seen := map[string]uint64{}
	for _, line := range strings.Split(strings.TrimSuffix(data, "\n"), "\n") {
		if line == "" {
			continue
		}
		f := strings.Fields(line)
		if len(f) != 2 {
			return cpuStatCounters{}, fmt.Errorf("%w: cpu.stat line", errCPUWitnessMalformed)
		}
		n, err := strconv.ParseUint(f[1], 10, 64)
		if err != nil {
			return cpuStatCounters{}, fmt.Errorf("%w: cpu.stat value", errCPUWitnessMalformed)
		}
		if _, dup := seen[f[0]]; dup {
			return cpuStatCounters{}, fmt.Errorf("%w: duplicate cpu.stat key", errCPUWitnessMalformed)
		}
		seen[f[0]] = n
	}
	keys := []string{"nr_periods", "nr_throttled", "throttled_usec"}
	present := 0
	for _, k := range keys {
		if _, ok := seen[k]; ok {
			present++
		}
	}
	switch present {
	case 0:
		return cpuStatCounters{}, errCPUWitnessIneligible
	case len(keys):
		return cpuStatCounters{Periods: seen["nr_periods"], Throttled: seen["nr_throttled"], ThrottledUsec: seen["throttled_usec"]}, nil
	}
	return cpuStatCounters{}, fmt.Errorf("%w: partial throttling keys", errCPUWitnessMalformed)
}

// readCPUWitness reads the three files of one cgroup directory. A missing
// file is errCPUWitnessUnavailable, never a zero. When cpu is not listed in
// cgroup.controllers the witness is returned with ControllerListed false and
// no counters, which classifies as ineligible.
func readCPUWitness(gen containerGeneration, group string, now time.Time) (cpuWitness, error) {
	if !gen.valid() {
		return cpuWitness{}, fmt.Errorf("%w: frozen generation is incomplete", errCPUWitnessGeneration)
	}
	read := func(name string) (string, error) {
		data, err := os.ReadFile(filepath.Join(group, name))
		if err != nil {
			return "", fmt.Errorf("%w: %s", errCPUWitnessUnavailable, name)
		}
		return string(data), nil
	}
	w := cpuWitness{Generation: gen, At: now, Source: cpuWitnessSource}
	text, err := read("cgroup.controllers")
	if err != nil {
		return cpuWitness{}, err
	}
	controllers, err := parseCgroupControllers(text)
	if err != nil {
		return cpuWitness{}, err
	}
	if !controllers["cpu"] {
		return w, nil
	}
	w.ControllerListed = true
	if text, err = read("cpu.max"); err != nil {
		return cpuWitness{}, err
	}
	if w.Max, err = parseCPUMax(text); err != nil {
		return cpuWitness{}, err
	}
	if text, err = read("cpu.stat"); err != nil {
		return cpuWitness{}, err
	}
	if w.Stat, err = parseCPUStat(text); err != nil {
		return cpuWitness{}, err
	}
	return w, nil
}

// cpuThrottleVerdict is the combined classification of an interval.
type cpuThrottleVerdict string

const (
	// cpuVerdictUnsupported: no admitted controller witness. API zeros fall
	// here and are never reported as "not throttled".
	cpuVerdictUnsupported cpuThrottleVerdict = "unsupported"
	// cpuVerdictIncomplete: a witness exists but cannot be tied to one
	// generation, one ordered interval or an installed finite quota.
	cpuVerdictIncomplete cpuThrottleVerdict = "incomplete"
	// cpuVerdictObservedZero: admitted controller, finite quota installed on
	// both readings, counters readable, no throttling counter advanced.
	cpuVerdictObservedZero cpuThrottleVerdict = "observed-zero"
	// cpuVerdictThrottled: admitted controller, finite quota installed, and
	// both nr_throttled and throttled_usec advanced.
	cpuVerdictThrottled cpuThrottleVerdict = "throttled"
)

// cpuClassifyChecks disables one rule per negative control.
type cpuClassifyChecks struct {
	skipGeneration bool
	skipController bool
	skipOrder      bool
	skipQuota      bool
	skipRollback   bool
}

// classifyCPUThrottle compares two local readings of the same generation. The
// API sample pair is accepted only as supplementary context and never changes
// the verdict, in particular all-zero API throttling data without a witness is
// unsupported, not "observed zero".
func classifyCPUThrottle(before, after *cpuWitness, apiZero bool, checks cpuClassifyChecks) (cpuThrottleVerdict, error) {
	_ = apiZero // deliberately unused: it cannot establish availability
	if before == nil || after == nil {
		return cpuVerdictUnsupported, nil
	}
	if !checks.skipGeneration && (!before.Generation.valid() || before.Generation != after.Generation) {
		return cpuVerdictIncomplete, fmt.Errorf("%w: %+v vs %+v", errCPUWitnessGeneration, before.Generation, after.Generation)
	}
	if before.Source != after.Source || before.Source != cpuWitnessSource {
		return cpuVerdictIncomplete, fmt.Errorf("%w: source", errCPUWitnessInconclusive)
	}
	if !checks.skipController && (!before.ControllerListed || !after.ControllerListed) {
		return cpuVerdictUnsupported, errCPUWitnessIneligible
	}
	if !checks.skipOrder && !after.At.After(before.At) {
		return cpuVerdictIncomplete, fmt.Errorf("%w: reading times are not ordered", errCPUWitnessInconclusive)
	}
	if !checks.skipQuota && (before.Max.Unlimited || after.Max.Unlimited) {
		return cpuVerdictIncomplete, fmt.Errorf("%w: no finite quota on both readings", errCPUWitnessInconclusive)
	}
	b, a := before.Stat, after.Stat
	if !checks.skipRollback && (a.Periods < b.Periods || a.Throttled < b.Throttled || a.ThrottledUsec < b.ThrottledUsec) {
		return cpuVerdictIncomplete, fmt.Errorf("%w: counter decreased", errCPUWitnessInconclusive)
	}
	if a.Throttled > b.Throttled && a.ThrottledUsec > b.ThrottledUsec {
		return cpuVerdictThrottled, nil
	}
	if a.Throttled != b.Throttled || a.ThrottledUsec != b.ThrottledUsec {
		// One of the two advanced without the other: not a clean zero.
		return cpuVerdictIncomplete, fmt.Errorf("%w: throttling counters advanced inconsistently", errCPUWitnessInconclusive)
	}
	return cpuVerdictObservedZero, nil
}
