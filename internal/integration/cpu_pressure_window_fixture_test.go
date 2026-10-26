//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"errors"
	"testing"
	"time"
)

// Docker-free fixtures for the bounded CPU-pressure window judgement. All
// readings are synthetic files in the cgroup v2 format and all worker reports
// are literal strings; no engine, container or native cgroup was involved.
//
// Not covered: no native CPU load, no real throttling observation, no daemon
// behavior under a quota, no OOM, no recovery and no engine or kernel
// qualification. A fixture pass shows the classifier's rules, not a native
// effect.

var cpuPressureQuota = cpuMax{Quota: 25000, Period: 100000}

const cpuBurnGood = `{"wall_ns": 5000000000, "cpu_ns": 1300000000, "iterations": 4200000, "completed": true}` + "\n"

func cpuPressureWindow(t *testing.T, afterStat, afterMax string) cpuPressureRecord {
	t.Helper()
	burn, err := parseCPUBurnReport([]byte(cpuBurnGood))
	if err != nil {
		t.Fatal(err)
	}
	return cpuPressureRecord{
		Generation: cpuGen,
		Expected:   cpuPressureQuota,
		ExecStart:  cpuT0.Add(time.Second),
		ExecEnd:    cpuT0.Add(7 * time.Second),
		Burn:       &burn,
		Before:     cpuRead(t, cpuGroup(t, "cpu\n", "25000 100000\n", cpuStatZero), cpuT0),
		After:      cpuRead(t, cpuGroup(t, "cpu\n", afterMax, afterStat), cpuT0.Add(8*time.Second)),
	}
}

func TestCPUBurnReportRequiresEveryTypedField(t *testing.T) {
	r, err := parseCPUBurnReport([]byte(cpuBurnGood))
	if err != nil || r.WallNanos != 5000000000 || r.CPUNanos != 1300000000 || r.Iterations != 4200000 {
		t.Fatalf("good report: %+v %v", r, err)
	}
	bad := map[string]string{
		"empty":             "",
		"not an object":     "[]\n",
		"missing completed": `{"wall_ns":1,"cpu_ns":1,"iterations":1}`,
		"completed false":   `{"wall_ns":1,"cpu_ns":1,"iterations":1,"completed":false}`,
		"completed string":  `{"wall_ns":1,"cpu_ns":1,"iterations":1,"completed":"true"}`,
		"completed null":    `{"wall_ns":1,"cpu_ns":1,"iterations":1,"completed":null}`,
		"missing wall":      `{"cpu_ns":1,"iterations":1,"completed":true}`,
		"null cpu":          `{"wall_ns":1,"cpu_ns":null,"iterations":1,"completed":true}`,
		"string iterations": `{"wall_ns":1,"cpu_ns":1,"iterations":"7","completed":true}`,
		"negative wall":     `{"wall_ns":-1,"cpu_ns":1,"iterations":1,"completed":true}`,
		"float cpu":         `{"wall_ns":1,"cpu_ns":1.5,"iterations":1,"completed":true}`,
		"exponent":          `{"wall_ns":1e3,"cpu_ns":1,"iterations":1,"completed":true}`,
		"leading zero":      `{"wall_ns":01,"cpu_ns":1,"iterations":1,"completed":true}`,
		"wall at 2^63":      `{"wall_ns":9223372036854775808,"cpu_ns":1,"iterations":1,"completed":true}`,
		"wall at 2^64-1":    `{"wall_ns":18446744073709551615,"cpu_ns":1,"iterations":1,"completed":true}`,
		"cpu at 2^63":       `{"wall_ns":1,"cpu_ns":9223372036854775808,"iterations":1,"completed":true}`,
		"trailing object":   cpuBurnGood + cpuBurnGood,
		"trailing garbage":  cpuBurnGood + "killed",
	}
	for name, in := range bad {
		if _, err := parseCPUBurnReport([]byte(in)); !errors.Is(err, errCPUPressure) {
			t.Errorf("%s: accepted or wrong error: %v", name, err)
		}
	}
}

func TestCPUPressureThrottledNeedsEveryRule(t *testing.T) {
	r := cpuPressureWindow(t, cpuStatHit, "25000 100000\n")
	if v, err := classifyCPUPressure(r, cpuPressureChecks{}); v != cpuPressureThrottled || err != nil {
		t.Fatalf("complete window: %v %v", v, err)
	}
	// A completed worker under the installed quota without counter advance is
	// not pressure evidence.
	quiet := cpuPressureWindow(t, cpuStatZero, "25000 100000\n")
	if v, err := classifyCPUPressure(quiet, cpuPressureChecks{}); v != cpuPressureDemandNotThrottled || err != nil {
		t.Fatalf("no counter advance: %v %v", v, err)
	}

	cases := map[string]func(*cpuPressureRecord){
		"worker outcome unknown":      func(r *cpuPressureRecord) { r.Burn = nil },
		"worker made no progress":     func(r *cpuPressureRecord) { r.Burn.Iterations = 0 },
		"worker consumed no cpu":      func(r *cpuPressureRecord) { r.Burn.CPUNanos = 0 },
		"first reading after start":   func(r *cpuPressureRecord) { r.Before.At = r.ExecStart.Add(time.Millisecond) },
		"second reading before end":   func(r *cpuPressureRecord) { r.After.At = r.ExecEnd.Add(-time.Millisecond) },
		"window not ordered":          func(r *cpuPressureRecord) { r.ExecEnd = r.ExecStart },
		"window start missing":        func(r *cpuPressureRecord) { r.ExecStart = time.Time{} },
		"worker longer than window":   func(r *cpuPressureRecord) { r.Burn.WallNanos = uint64(7 * time.Second) },
		"quota differs from expected": func(r *cpuPressureRecord) { r.Expected.Quota = 50000 },
		"quota removed after":         func(r *cpuPressureRecord) { r.After.Max = cpuMax{Unlimited: true, Period: 100000} },
		"expected quota unlimited":    func(r *cpuPressureRecord) { r.Expected = cpuMax{Unlimited: true, Period: 100000} },
		"generation incomplete":       func(r *cpuPressureRecord) { r.Generation = containerGeneration{} },
		"record id differs":           func(r *cpuPressureRecord) { r.Generation.ID = "c2" },
		"record start differs":        func(r *cpuPressureRecord) { r.Generation.StartedAt = "2026-10-25T00:01:00Z" },
		"pair from another container": func(r *cpuPressureRecord) {
			r.Before.Generation.ID, r.After.Generation.ID = "c2", "c2"
		},
		"pair from another start": func(r *cpuPressureRecord) {
			r.Before.Generation.StartedAt, r.After.Generation.StartedAt = "2026-10-25T00:01:00Z", "2026-10-25T00:01:00Z"
		},
		"wall wraps negative": func(r *cpuPressureRecord) { r.Burn.WallNanos = 1 << 63 },
		"wall max uint64":     func(r *cpuPressureRecord) { r.Burn.WallNanos = ^uint64(0) },
		"generation replaced": func(r *cpuPressureRecord) { r.After.Generation.StartedAt = "2026-10-25T00:01:00Z" },
	}
	for name, mutate := range cases {
		w := cpuPressureWindow(t, cpuStatHit, "25000 100000\n")
		mutate(&w)
		v, err := classifyCPUPressure(w, cpuPressureChecks{})
		if v != cpuPressureIncomplete || err == nil {
			t.Errorf("%s: %v %v", name, v, err)
		}
	}

	// Missing readings or a missing controller are unsupported, never zero.
	missing := cpuPressureWindow(t, cpuStatHit, "25000 100000\n")
	missing.After = nil
	if v, _ := classifyCPUPressure(missing, cpuPressureChecks{}); v != cpuPressureUnsupported {
		t.Errorf("missing reading: %v", v)
	}
	noCtl := cpuPressureWindow(t, cpuStatHit, "25000 100000\n")
	noCtl.After.ControllerListed = false
	if v, _ := classifyCPUPressure(noCtl, cpuPressureChecks{}); v != cpuPressureUnsupported {
		t.Errorf("controller dropped: %v", v)
	}
}

func TestCPUPressureRefusesThrottledPeriodsBeyondElapsed(t *testing.T) {
	// 30 throttled periods out of 5 elapsed periods is not a consistent window.
	impossible := cpuStatBase + "nr_periods 15\nnr_throttled 30\nthrottled_usec 1500\n"
	r := cpuPressureWindow(t, impossible, "25000 100000\n")
	if v, err := classifyCPUPressure(r, cpuPressureChecks{}); v != cpuPressureIncomplete || err == nil {
		t.Fatalf("throttled beyond elapsed: %v %v", v, err)
	}
	// Throttling counters that advanced while nr_periods did not.
	frozen := cpuStatBase + "nr_periods 10\nnr_throttled 5\nthrottled_usec 1500\n"
	r = cpuPressureWindow(t, frozen, "25000 100000\n")
	if v, err := classifyCPUPressure(r, cpuPressureChecks{}); v != cpuPressureIncomplete || err == nil {
		t.Fatalf("periods did not advance: %v %v", v, err)
	}
}

// Each rule is switched off in turn; the matching scenario must then be
// accepted as pressure evidence, proving the tests above exercise the rule.
func TestCPUPressureNegativeControlsBreakEachRule(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*cpuPressureRecord)
		stat   string
		checks cpuPressureChecks
	}{
		{"worker outcome", func(r *cpuPressureRecord) { r.Burn = nil }, cpuStatHit, cpuPressureChecks{skipBurn: true}},
		{"bracket", func(r *cpuPressureRecord) { r.Before.At = r.ExecStart.Add(time.Second) }, cpuStatHit, cpuPressureChecks{skipBracket: true}},
		{"quota", func(r *cpuPressureRecord) { r.Expected.Quota = 50000 }, cpuStatHit, cpuPressureChecks{skipQuota: true}},
		{"wall", func(r *cpuPressureRecord) { r.Burn.WallNanos = uint64(7 * time.Second) }, cpuStatHit, cpuPressureChecks{skipWall: true}},
		{"join id", func(r *cpuPressureRecord) {
			r.Generation.ID = "c2"
		}, cpuStatHit, cpuPressureChecks{skipJoin: true}},
		{"join start", func(r *cpuPressureRecord) {
			r.Generation.StartedAt = "2026-10-25T00:01:00Z"
		}, cpuStatHit, cpuPressureChecks{skipJoin: true}},
		{"wall wraps", func(r *cpuPressureRecord) { r.Burn.WallNanos = 1 << 63 }, cpuStatHit, cpuPressureChecks{skipWall: true}},
		{"periods", func(r *cpuPressureRecord) {}, cpuStatBase + "nr_periods 15\nnr_throttled 30\nthrottled_usec 1500\n", cpuPressureChecks{skipPeriods: true}},
	}
	for _, c := range cases {
		r := cpuPressureWindow(t, c.stat, "25000 100000\n")
		c.mutate(&r)
		strict, _ := classifyCPUPressure(r, cpuPressureChecks{})
		broken, _ := classifyCPUPressure(r, c.checks)
		if strict != cpuPressureIncomplete {
			t.Errorf("%s: strict classifier produced %v", c.name, strict)
		}
		if broken != cpuPressureThrottled {
			t.Errorf("%s: disabled rule still refused (%v); control proves nothing", c.name, broken)
		}
	}
}
