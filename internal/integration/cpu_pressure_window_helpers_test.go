//go:build all || integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// This file judges one bounded CPU-pressure window on one frozen container
// generation: two target-local kernel readings (the merged CPU-controller
// witness) taken before and after a bounded worker ran inside the target's
// cgroup, plus the worker's own completion report. It dispatches nothing and
// reads no cgroup itself, so the Docker-free fixtures drive it with synthetic
// records. It does not decide what a throttled daemon does, and it is not a
// Ceph, OOM or recovery oracle.
//
// A quota that was installed is never pressure evidence, and neither is a
// worker that merely ran. The verdict "throttled" needs an admitted
// controller, an installed finite quota that equals the expected one on both
// readings, readings that bracket the worker's execution window, a worker that
// reported its own completed progress, and advancing kernel throttling
// counters. A worker that completed under the same finite quota without any
// counter advance is "demand not throttled": the mechanism was not
// reproduced, which is a result and not a pass of the pressure claim.

var errCPUPressure = errors.New("cpu pressure window is not usable")

// cpuBurnReport is the completion report printed by the bounded worker. Every
// field is required and typed; a missing, null or wrong-typed field refuses the
// report instead of decoding to a zero that would count as progress.
type cpuBurnReport struct {
	WallNanos  uint64 // worker-observed monotonic duration
	CPUNanos   uint64 // CPU time the worker process consumed
	Iterations uint64 // loop iterations, proof of forward progress
}

// parseCPUBurnReport decodes the worker's single JSON line. The report is
// accepted only when completed is the JSON value true and all numbers are
// non-negative integers. Trailing bytes after the single object are refused.
func parseCPUBurnReport(data []byte) (cpuBurnReport, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var raw map[string]json.RawMessage
	if err := dec.Decode(&raw); err != nil {
		return cpuBurnReport{}, fmt.Errorf("%w: burn report is not a JSON object: %v", errCPUPressure, err)
	}
	if rest := bytes.TrimSpace(data[dec.InputOffset():]); len(rest) != 0 {
		return cpuBurnReport{}, fmt.Errorf("%w: trailing bytes after the burn report", errCPUPressure)
	}
	completed, ok := raw["completed"]
	if !ok || string(bytes.TrimSpace(completed)) != "true" {
		return cpuBurnReport{}, fmt.Errorf("%w: burn report is not marked completed", errCPUPressure)
	}
	num := func(key string) (uint64, error) {
		v, ok := raw[key]
		if !ok {
			return 0, fmt.Errorf("%w: burn report field %q is missing", errCPUPressure, key)
		}
		s := string(bytes.TrimSpace(v))
		if s == "" || strings.ContainsAny(s, ".eE-+\"") || s == "null" {
			return 0, fmt.Errorf("%w: burn report field %q is not a non-negative integer", errCPUPressure, key)
		}
		var n uint64
		if _, err := fmt.Sscanf(s, "%d", &n); err != nil || fmt.Sprint(n) != s {
			return 0, fmt.Errorf("%w: burn report field %q is not a non-negative integer", errCPUPressure, key)
		}
		if n > math.MaxInt64 {
			return 0, fmt.Errorf("%w: burn report field %q is out of range", errCPUPressure, key)
		}
		return n, nil
	}
	var r cpuBurnReport
	var err error
	if r.WallNanos, err = num("wall_ns"); err != nil {
		return cpuBurnReport{}, err
	}
	if r.CPUNanos, err = num("cpu_ns"); err != nil {
		return cpuBurnReport{}, err
	}
	if r.Iterations, err = num("iterations"); err != nil {
		return cpuBurnReport{}, err
	}
	return r, nil
}

// cpuPressureRecord is one window. Burn is nil when the worker's outcome is
// unknown (exec error, nonzero exit, unparsable output, deadline): that
// outcome is classified as incomplete and is never promoted by the counters.
type cpuPressureRecord struct {
	Generation containerGeneration
	// Expected is the finite cpu.max that must be installed on both readings.
	Expected cpuMax

	// ExecStart and ExecEnd are controller-clock times taken immediately
	// before the exec request and after its result.
	ExecStart, ExecEnd time.Time
	Burn               *cpuBurnReport

	Before, After *cpuWitness
}

type cpuPressureVerdict string

const (
	// cpuPressureThrottled: all admission rules held and both throttling
	// counters advanced during the bracketed window.
	cpuPressureThrottled cpuPressureVerdict = "throttled"
	// cpuPressureDemandNotThrottled: all admission rules held, the worker
	// completed with progress, and no throttling counter advanced.
	cpuPressureDemandNotThrottled cpuPressureVerdict = "demand-not-throttled"
	// cpuPressureUnsupported: no admitted controller witness.
	cpuPressureUnsupported cpuPressureVerdict = "unsupported"
	// cpuPressureIncomplete: anything else, including an unknown worker
	// outcome, readings that do not bracket the window or a changed quota.
	cpuPressureIncomplete cpuPressureVerdict = "incomplete"
)

// cpuPressureChecks disables one rule per negative control.
type cpuPressureChecks struct {
	skipBurn    bool
	skipBracket bool
	skipQuota   bool
	skipWall    bool
	skipJoin    bool
	skipPeriods bool
}

// classifyCPUPressure judges one window. Missing evidence is incomplete or
// unsupported and never an empty value that counts as evidence.
func classifyCPUPressure(r cpuPressureRecord, checks cpuPressureChecks) (cpuPressureVerdict, error) {
	if !r.Generation.Valid() {
		return cpuPressureIncomplete, fmt.Errorf("%w: frozen generation is incomplete", errCPUPressure)
	}
	if r.Before == nil || r.After == nil {
		return cpuPressureUnsupported, nil
	}
	if !checks.skipJoin && (r.Before.Generation != r.Generation || r.After.Generation != r.Generation) {
		return cpuPressureIncomplete, fmt.Errorf("%w: readings do not belong to the declared worker generation", errCPUPressure)
	}
	if !checks.skipBurn {
		if r.Burn == nil {
			return cpuPressureIncomplete, fmt.Errorf("%w: worker outcome is unknown", errCPUPressure)
		}
		if r.Burn.WallNanos == 0 || r.Burn.CPUNanos == 0 || r.Burn.Iterations == 0 {
			return cpuPressureIncomplete, fmt.Errorf("%w: worker reported no progress", errCPUPressure)
		}
	}
	if !checks.skipBracket {
		switch {
		case r.ExecStart.IsZero() || !r.ExecEnd.After(r.ExecStart):
			return cpuPressureIncomplete, fmt.Errorf("%w: execution window is not ordered", errCPUPressure)
		case r.Before.At.After(r.ExecStart):
			return cpuPressureIncomplete, fmt.Errorf("%w: first reading was taken after the worker started", errCPUPressure)
		case r.After.At.Before(r.ExecEnd):
			return cpuPressureIncomplete, fmt.Errorf("%w: second reading was taken before the worker ended", errCPUPressure)
		}
	}
	// Compared as unsigned values: converting an oversized report to a signed
	// duration would wrap it negative. The window is positive here unless the
	// bracket rule was disabled, in which case a non-positive window refuses.
	if window := r.ExecEnd.Sub(r.ExecStart); !checks.skipWall && r.Burn != nil && (window <= 0 || r.Burn.WallNanos > uint64(window)) {
		return cpuPressureIncomplete, fmt.Errorf("%w: worker duration exceeds the observed execution window", errCPUPressure)
	}
	if !checks.skipQuota {
		if r.Expected.Unlimited || r.Expected.Quota == 0 || r.Expected.Period == 0 {
			return cpuPressureIncomplete, fmt.Errorf("%w: expected quota is not finite", errCPUPressure)
		}
		if r.Before.Max != r.Expected || r.After.Max != r.Expected {
			return cpuPressureIncomplete, fmt.Errorf("%w: installed quota differs from the expected one", errCPUPressure)
		}
	}
	verdict, err := classifyCPUThrottle(r.Before, r.After, false, cpuClassifyChecks{})
	switch verdict {
	case cpuVerdictUnsupported:
		return cpuPressureUnsupported, err
	case cpuVerdictObservedZero:
		return cpuPressureDemandNotThrottled, nil
	case cpuVerdictThrottled:
		b, a := r.Before.Stat, r.After.Stat
		if !checks.skipPeriods && (a.Periods <= b.Periods || a.Throttled-b.Throttled > a.Periods-b.Periods) {
			return cpuPressureIncomplete, fmt.Errorf("%w: throttled periods are not within the elapsed periods", errCPUPressure)
		}
		return cpuPressureThrottled, nil
	}
	return cpuPressureIncomplete, err
}
