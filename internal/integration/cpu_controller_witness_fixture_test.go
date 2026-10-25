//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Docker-free fixtures for the target-local CPU-controller witness. The files
// are synthetic and follow the cgroup v2 interface format; no engine or native
// cgroup was read.
//
// Not covered: no live binding to a container cgroup, no native CPU pressure,
// no throttling observation, no quota update and no installed-engine or
// kernel qualification.

var cpuGen = containerGeneration{ID: "c1", StartedAt: "2026-10-25T00:00:00.000000001Z"}

const (
	cpuStatBase = "usage_usec 10\nuser_usec 6\nsystem_usec 4\n"
	cpuStatZero = cpuStatBase + "nr_periods 10\nnr_throttled 0\nthrottled_usec 0\n"
	cpuStatHit  = cpuStatBase + "nr_periods 60\nnr_throttled 20\nthrottled_usec 1500\n"
)

func cpuGroup(t *testing.T, controllers, max, stat string) string {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{"cgroup.controllers": controllers, "cpu.max": max, "cpu.stat": stat}
	for name, data := range files {
		if data == "-" {
			continue
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

var cpuT0 = time.Date(2026, 10, 25, 0, 0, 10, 0, time.UTC)

func cpuRead(t *testing.T, group string, at time.Time) *cpuWitness {
	t.Helper()
	w, err := readCPUWitness(cpuGen, group, at)
	if err != nil {
		t.Fatal(err)
	}
	return &w
}

func TestCPUWitnessParsesRealFormats(t *testing.T) {
	if c, err := parseCgroupControllers("cpuset cpu io memory pids\n"); err != nil || !c["cpu"] || !c["memory"] {
		t.Fatalf("controllers: %v %v", c, err)
	}
	if c, err := parseCgroupControllers(""); err != nil || len(c) != 0 {
		t.Fatalf("empty controllers must be a valid ineligible reading: %v %v", c, err)
	}
	if m, err := parseCPUMax("25000 100000\n"); err != nil || m.Unlimited || m.Quota != 25000 || m.Period != 100000 {
		t.Fatalf("finite max: %+v %v", m, err)
	}
	if m, err := parseCPUMax("max 100000\n"); err != nil || !m.Unlimited || m.Period != 100000 {
		t.Fatalf("unlimited max: %+v %v", m, err)
	}
	if s, err := parseCPUStat(cpuStatHit); err != nil || s != (cpuStatCounters{Periods: 60, Throttled: 20, ThrottledUsec: 1500}) {
		t.Fatalf("stat: %+v %v", s, err)
	}
}

func TestCPUWitnessRefusesMalformedAndPartialInput(t *testing.T) {
	for name, data := range map[string]string{
		"no newline": "cpu",
		"duplicate":  "cpu cpu\n",
	} {
		if _, err := parseCgroupControllers(data); !errors.Is(err, errCPUWitnessMalformed) {
			t.Errorf("controllers %s accepted: %v", name, err)
		}
	}
	for name, data := range map[string]string{
		"empty":        "",
		"one field":    "max\n",
		"three fields": "1 100000 1\n",
		"zero period":  "max 0\n",
		"zero quota":   "0 100000\n",
		"negative":     "-1 100000\n",
		"float":        "0.5 100000\n",
		"no newline":   "max 100000",
	} {
		if _, err := parseCPUMax(data); !errors.Is(err, errCPUWitnessMalformed) {
			t.Errorf("cpu.max %s accepted: %v", name, err)
		}
	}
	for name, data := range map[string]string{
		"partial":    cpuStatBase + "nr_periods 1\nnr_throttled 0\n",
		"duplicate":  cpuStatZero + "nr_periods 11\n",
		"string":     cpuStatBase + "nr_periods x\nnr_throttled 0\nthrottled_usec 0\n",
		"negative":   cpuStatBase + "nr_periods 1\nnr_throttled -1\nthrottled_usec 0\n",
		"three cols": cpuStatBase + "nr_periods 1 2\nnr_throttled 0\nthrottled_usec 0\n",
	} {
		if _, err := parseCPUStat(data); !errors.Is(err, errCPUWitnessMalformed) {
			t.Errorf("cpu.stat %s accepted: %v", name, err)
		}
	}
}

// Without the controller the base cpu.stat keys remain but the throttling keys
// are absent. That is ineligibility and must never decode into zero counters.
func TestCPUWitnessAbsentThrottlingKeysAreIneligibleNotZero(t *testing.T) {
	if _, err := parseCPUStat(cpuStatBase); !errors.Is(err, errCPUWitnessIneligible) {
		t.Fatalf("absent keys: %v", err)
	}
	if _, err := parseCPUStat(""); !errors.Is(err, errCPUWitnessIneligible) {
		t.Fatalf("empty cpu.stat: %v", err)
	}
	// The paired control: present keys with observed zeros parse as evidence.
	s, err := parseCPUStat(cpuStatZero)
	if err != nil || s.Throttled != 0 || s.ThrottledUsec != 0 || s.Periods != 10 {
		t.Fatalf("observed zeros: %+v %v", s, err)
	}
}

func TestCPUWitnessReaderMissingFilesAndController(t *testing.T) {
	// cpu not listed: reading succeeds but is not eligible, and no other file is needed.
	w, err := readCPUWitness(cpuGen, cpuGroup(t, "memory pids\n", "-", cpuStatBase), cpuT0)
	if err != nil || w.ControllerListed {
		t.Fatalf("controller-less reading: %+v %v", w, err)
	}
	for _, missing := range []string{"cgroup.controllers", "cpu.max", "cpu.stat"} {
		files := map[string]string{"cgroup.controllers": "cpu memory\n", "cpu.max": "25000 100000\n", "cpu.stat": cpuStatZero}
		files[missing] = "-"
		group := cpuGroup(t, files["cgroup.controllers"], files["cpu.max"], files["cpu.stat"])
		if _, err := readCPUWitness(cpuGen, group, cpuT0); !errors.Is(err, errCPUWitnessUnavailable) {
			t.Errorf("missing %s: %v", missing, err)
		}
	}
	if _, err := readCPUWitness(containerGeneration{}, t.TempDir(), cpuT0); !errors.Is(err, errCPUWitnessGeneration) {
		t.Fatalf("incomplete generation: %v", err)
	}
}

func TestCPUWitnessClassifiesAPIZeroWithoutWitnessAsUnsupported(t *testing.T) {
	// Identical all-zero API throttling data, with and without an admitted
	// controller witness: only the witness separates the two.
	v, err := classifyCPUThrottle(nil, nil, true, cpuClassifyChecks{})
	if err != nil || v != cpuVerdictUnsupported {
		t.Fatalf("no witness: %v %v", v, err)
	}
	without := cpuRead(t, cpuGroup(t, "memory\n", "-", cpuStatBase), cpuT0)
	withoutLater := cpuRead(t, cpuGroup(t, "memory\n", "-", cpuStatBase), cpuT0.Add(time.Second))
	if v, err := classifyCPUThrottle(without, withoutLater, true, cpuClassifyChecks{}); v != cpuVerdictUnsupported || !errors.Is(err, errCPUWitnessIneligible) {
		t.Fatalf("controller not listed: %v %v", v, err)
	}
	b := cpuRead(t, cpuGroup(t, "cpu\n", "25000 100000\n", cpuStatZero), cpuT0)
	a := cpuRead(t, cpuGroup(t, "cpu\n", "25000 100000\n", cpuStatZero), cpuT0.Add(5*time.Second))
	if v, err := classifyCPUThrottle(b, a, true, cpuClassifyChecks{}); v != cpuVerdictObservedZero || err != nil {
		t.Fatalf("admitted observed zero: %v %v", v, err)
	}
}

func TestCPUWitnessThrottledVerdict(t *testing.T) {
	b := cpuRead(t, cpuGroup(t, "cpu\n", "25000 100000\n", cpuStatZero), cpuT0)
	a := cpuRead(t, cpuGroup(t, "cpu\n", "25000 100000\n", cpuStatHit), cpuT0.Add(5*time.Second))
	if v, err := classifyCPUThrottle(b, a, false, cpuClassifyChecks{}); v != cpuVerdictThrottled || err != nil {
		t.Fatalf("throttled: %v %v", v, err)
	}
	// An installed quota alone is not throttling.
	same := cpuRead(t, cpuGroup(t, "cpu\n", "25000 100000\n", cpuStatZero), cpuT0.Add(5*time.Second))
	if v, _ := classifyCPUThrottle(b, same, false, cpuClassifyChecks{}); v == cpuVerdictThrottled {
		t.Fatal("quota without counter advance classified as throttled")
	}
	// One counter advancing without the other is not a clean verdict either way.
	half := cpuRead(t, cpuGroup(t, "cpu\n", "25000 100000\n", cpuStatBase+"nr_periods 60\nnr_throttled 20\nthrottled_usec 0\n"), cpuT0.Add(5*time.Second))
	if v, err := classifyCPUThrottle(b, half, false, cpuClassifyChecks{}); v != cpuVerdictIncomplete || err == nil {
		t.Fatalf("inconsistent advance: %v %v", v, err)
	}
}

func TestCPUWitnessRefusesIncomparableReadings(t *testing.T) {
	b := cpuRead(t, cpuGroup(t, "cpu\n", "25000 100000\n", cpuStatZero), cpuT0)
	hit := func(max string, at time.Time) *cpuWitness {
		return cpuRead(t, cpuGroup(t, "cpu\n", max, cpuStatHit), at)
	}
	later := cpuT0.Add(5 * time.Second)

	other := hit("25000 100000\n", later)
	other.Generation.StartedAt = "2026-10-25T00:01:00Z"
	if v, err := classifyCPUThrottle(b, other, false, cpuClassifyChecks{}); v != cpuVerdictIncomplete || !errors.Is(err, errCPUWitnessGeneration) {
		t.Errorf("replaced generation: %v %v", v, err)
	}
	if v, err := classifyCPUThrottle(b, hit("25000 100000\n", cpuT0), false, cpuClassifyChecks{}); v != cpuVerdictIncomplete || err == nil {
		t.Errorf("unordered readings: %v %v", v, err)
	}
	if v, err := classifyCPUThrottle(b, hit("max 100000\n", later), false, cpuClassifyChecks{}); v != cpuVerdictIncomplete || err == nil {
		t.Errorf("quota removed: %v %v", v, err)
	}
	dropped := cpuRead(t, cpuGroup(t, "cpu\n", "25000 100000\n", cpuStatBase+"nr_periods 5\nnr_throttled 0\nthrottled_usec 0\n"), later)
	if v, err := classifyCPUThrottle(hit("25000 100000\n", cpuT0), dropped, false, cpuClassifyChecks{}); v != cpuVerdictIncomplete || err == nil {
		t.Errorf("counter rollback: %v %v", v, err)
	}
	noCtl := cpuRead(t, cpuGroup(t, "memory\n", "-", cpuStatBase), later)
	if v, _ := classifyCPUThrottle(b, noCtl, false, cpuClassifyChecks{}); v != cpuVerdictUnsupported {
		t.Errorf("controller dropped: %v", v)
	}
}

// Each rule is switched off in turn; the matching scenario must then be
// misclassified, proving the regression tests above exercise the rule.
func TestCPUWitnessNegativeControlsBreakEachRule(t *testing.T) {
	good := cpuRead(t, cpuGroup(t, "cpu\n", "25000 100000\n", cpuStatZero), cpuT0)
	later := cpuT0.Add(5 * time.Second)
	mk := func(controllers, max, stat string, at time.Time) *cpuWitness {
		return cpuRead(t, cpuGroup(t, controllers, max, stat), at)
	}
	replaced := mk("cpu\n", "25000 100000\n", cpuStatHit, later)
	replaced.Generation.ID = "c2"
	noController := mk("cpu\n", "25000 100000\n", cpuStatZero, later)
	noController.ControllerListed = false
	cases := []struct {
		name   string
		after  *cpuWitness
		checks cpuClassifyChecks
	}{
		{"generation", replaced, cpuClassifyChecks{skipGeneration: true}},
		{"controller", noController, cpuClassifyChecks{skipController: true}},
		{"order", mk("cpu\n", "25000 100000\n", cpuStatHit, cpuT0), cpuClassifyChecks{skipOrder: true}},
		{"quota", mk("cpu\n", "max 100000\n", cpuStatHit, later), cpuClassifyChecks{skipQuota: true}},
	}
	for _, c := range cases {
		healthy, _ := classifyCPUThrottle(good, c.after, false, cpuClassifyChecks{})
		broken, _ := classifyCPUThrottle(good, c.after, false, c.checks)
		if healthy == cpuVerdictThrottled || healthy == cpuVerdictObservedZero {
			t.Errorf("%s: strict classifier produced evidence %v", c.name, healthy)
		}
		if broken != cpuVerdictThrottled && broken != cpuVerdictObservedZero {
			t.Errorf("%s: disabled rule still refused (%v); control proves nothing", c.name, broken)
		}
	}
	hit := mk("cpu\n", "25000 100000\n", cpuStatHit, cpuT0)
	rolled := mk("cpu\n", "25000 100000\n", cpuStatBase+"nr_periods 5\nnr_throttled 20\nthrottled_usec 1500\n", later)
	strict, _ := classifyCPUThrottle(hit, rolled, false, cpuClassifyChecks{})
	broken, _ := classifyCPUThrottle(hit, rolled, false, cpuClassifyChecks{skipRollback: true})
	if strict != cpuVerdictIncomplete || broken == cpuVerdictIncomplete {
		t.Errorf("rollback control: strict=%v broken=%v", strict, broken)
	}
}
