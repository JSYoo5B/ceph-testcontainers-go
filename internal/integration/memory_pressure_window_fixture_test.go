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

// Docker-free fixtures for the bounded memory-limit window judgement. All
// readings are synthetic files in the cgroup v2 format and all engine and
// process records are literals; no engine, container or native cgroup was
// involved.
//
// Not covered: no native memory pressure, no real OOM or victim observation,
// no event-handle survival through cgroup teardown, no recovery, no OSD data
// durability, and no engine or kernel qualification. A fixture pass shows the
// classifier's rules, not a native effect.

var memGen = containerGeneration{ID: "m1", StartedAt: "2026-10-25T00:00:00.000000001Z"}

var memT0 = time.Date(2026, 10, 25, 0, 0, 10, 0, time.UTC)

const memCeiling = 33554432 // 32 MiB, a fixture value and not a product default

const (
	memEventsBase = "low 0\nhigh 0\n"
	memEventsZero = memEventsBase + "max 3\noom 1\noom_kill 0\noom_group_kill 0\n"
	memEventsHit  = memEventsBase + "max 9\noom 1\noom_kill 0\noom_group_kill 0\n"
	memEventsKill = memEventsBase + "max 9\noom 2\noom_kill 1\noom_group_kill 0\n"
)

func memGroup(t *testing.T, controllers, max, swap, current, events string) string {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"cgroup.controllers":  controllers,
		"memory.max":          max,
		"memory.swap.max":     swap,
		"memory.current":      current,
		"memory.events.local": events,
	}
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

func memRead(t *testing.T, group string, at time.Time) *memoryWitness {
	t.Helper()
	w, err := readMemoryWitness(memGen, group, at)
	if err != nil {
		t.Fatal(err)
	}
	return &w
}

// memWindow builds a complete daemon-victim window; each case mutates it.
func memWindow(t *testing.T, afterEvents string) memoryPressureRecord {
	t.Helper()
	return memoryPressureRecord{
		Generation:   memGen,
		Expected:     memoryLimit{Bytes: memCeiling},
		PID:          7,
		ProcessStart: "12345",
		ProcsBefore:  memoryProcs{Observed: true, PIDs: []int{7}},
		ExecStart:    memT0.Add(time.Second),
		ExecEnd:      memT0.Add(7 * time.Second),
		Before:       memRead(t, memGroup(t, "cpu memory\n", "33554432\n", "0\n", "1000\n", memEventsZero), memT0),
		After:        memRead(t, memGroup(t, "cpu memory\n", "33554432\n", "0\n", "0\n", afterEvents), memT0.Add(8*time.Second)),
		Exit: &memoryDaemonExit{
			Generation: memGen, OOMKilled: true, ExitCode: 137,
			FinishedAt: memT0.Add(4 * time.Second),
		},
	}
}

func TestMemoryWitnessParsesRealFormats(t *testing.T) {
	if l, err := parseMemoryLimit("max\n", false); err != nil || !l.Unlimited {
		t.Fatalf("max: %+v %v", l, err)
	}
	if l, err := parseMemoryLimit("0\n", true); err != nil || l.Unlimited || l.Bytes != 0 {
		t.Fatalf("swap zero: %+v %v", l, err)
	}
	if l, err := parseMemoryLimit("33554432\n", false); err != nil || l.Bytes != memCeiling {
		t.Fatalf("finite: %+v %v", l, err)
	}
	for name, in := range map[string]string{
		"empty": "", "no newline": "100", "zero max": "0\n", "negative": "-1\n", "two lines": "1\n2\n",
		"leading zero": "01\n", "text": "lots\n", "two fields": "1 2\n", "overflow": "18446744073709551616\n",
	} {
		if _, err := parseMemoryLimit(in, false); !errors.Is(err, errMemoryWitnessMalformed) {
			t.Errorf("%s: accepted or wrong error: %v", name, err)
		}
	}
	if _, err := parseMemoryLimit("-\n", true); err == nil {
		t.Error("dash accepted")
	}
	if e, err := parseMemoryEventsLocal(memEventsKill); err != nil || e != (memoryEvents{Max: 9, OOM: 2, OOMKill: 1}) {
		t.Fatalf("events: %+v %v", e, err)
	}
	for name, in := range map[string]string{
		"empty":           "",
		"missing oom":     "max 1\noom_kill 0\n",
		"missing max":     "oom 1\noom_kill 0\n",
		"missing kill":    "max 1\noom 1\n",
		"duplicate":       "max 1\nmax 2\noom 1\noom_kill 0\n",
		"non integer":     "max 1\noom x\noom_kill 0\n",
		"negative":        "max 1\noom -1\noom_kill 0\n",
		"extra field":     "max 1 2\noom 1\noom_kill 0\n",
		"empty value key": "max\noom 1\noom_kill 0\n",
	} {
		if _, err := parseMemoryEventsLocal(in); !errors.Is(err, errMemoryWitnessMalformed) {
			t.Errorf("%s: accepted or wrong error: %v", name, err)
		}
	}
}

func TestMemoryWitnessReaderRefusesMissingAndHierarchicalEvidence(t *testing.T) {
	good := func() string { return memGroup(t, "memory\n", "33554432\n", "0\n", "5\n", memEventsZero) }
	if w, err := readMemoryWitness(memGen, good(), memT0); err != nil || !w.ControllerListed || w.Current != 5 {
		t.Fatalf("good: %+v %v", w, err)
	}
	// An unlisted controller is a valid, ineligible reading without files.
	if w, err := readMemoryWitness(memGen, memGroup(t, "cpu\n", "-", "-", "-", "-"), memT0); err != nil || w.ControllerListed {
		t.Fatalf("controller unlisted: %+v %v", w, err)
	}
	for name, dir := range map[string]string{
		"no controllers file": memGroup(t, "-", "33554432\n", "0\n", "5\n", memEventsZero),
		"no max":              memGroup(t, "memory\n", "-", "0\n", "5\n", memEventsZero),
		"no swap":             memGroup(t, "memory\n", "33554432\n", "-", "5\n", memEventsZero),
		"no current":          memGroup(t, "memory\n", "33554432\n", "0\n", "-", memEventsZero),
		"no local events":     memGroup(t, "memory\n", "33554432\n", "0\n", "5\n", "-"),
	} {
		if _, err := readMemoryWitness(memGen, dir, memT0); !errors.Is(err, errMemoryWitnessUnavail) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// The hierarchical memory.events must never stand in for the local file.
	hier := memGroup(t, "memory\n", "33554432\n", "0\n", "5\n", "-")
	if err := os.WriteFile(filepath.Join(hier, "memory.events"), []byte(memEventsKill), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readMemoryWitness(memGen, hier, memT0); !errors.Is(err, errMemoryWitnessUnavail) {
		t.Errorf("hierarchical events accepted: %v", err)
	}
	for name, dir := range map[string]string{
		"null-like max":  memGroup(t, "memory\n", "\n", "0\n", "5\n", memEventsZero),
		"bad current":    memGroup(t, "memory\n", "33554432\n", "0\n", "x\n", memEventsZero),
		"bad controller": memGroup(t, "memory memory\n", "33554432\n", "0\n", "5\n", memEventsZero),
		"bad events":     memGroup(t, "memory\n", "33554432\n", "0\n", "5\n", "max 1\n"),
	} {
		if _, err := readMemoryWitness(memGen, dir, memT0); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if _, err := readMemoryWitness(containerGeneration{}, good(), memT0); !errors.Is(err, errMemoryWitnessGeneration) {
		t.Errorf("incomplete generation: %v", err)
	}
}

func TestMemoryPressureOutcomes(t *testing.T) {
	r := memWindow(t, memEventsKill)
	if v, err := classifyMemoryPressure(r, memoryPressureChecks{}); v != memoryDaemonVictim || err != nil {
		t.Fatalf("complete victim window: %v %v", v, err)
	}
	running := &memoryDaemonExit{Generation: memGen, Running: true}
	for _, c := range []struct {
		name  string
		after string
		exit  *memoryDaemonExit
		want  memoryPressureVerdict
	}{
		{"limit not reached", memEventsZero, running, memoryLimitNotReached},
		{"limit hit, daemon survived", memEventsHit, running, memoryLimitHitSurvived},
		{"kill counted, daemon alive", memEventsKill, running, memoryOtherVictim},
		{"kill counted, exit not an OOM exit", memEventsKill, &memoryDaemonExit{Generation: memGen, ExitCode: 137, FinishedAt: memT0.Add(4 * time.Second)}, memoryOtherVictim},
	} {
		w := memWindow(t, c.after)
		w.Exit = c.exit
		if v, err := classifyMemoryPressure(w, memoryPressureChecks{}); v != c.want || err != nil {
			t.Errorf("%s: %v %v", c.name, v, err)
		}
	}
	// Several kills, a helper in the cgroup, a helper seen later and a daemon
	// that was not the only member are all other-or-unknown, never the daemon.
	for name, mutate := range map[string]func(*memoryPressureRecord){
		"two kills": func(r *memoryPressureRecord) {
			r.After.Events.OOMKill = r.Before.Events.OOMKill + 2
		},
		"helper present before": func(r *memoryPressureRecord) { r.ProcsBefore.PIDs = []int{7, 9} },
		"helper seen later":     func(r *memoryPressureRecord) { r.OtherProcSeen = true },
		"other pid is the only": func(r *memoryPressureRecord) { r.ProcsBefore.PIDs = []int{9} },
		"empty membership":      func(r *memoryPressureRecord) { r.ProcsBefore.PIDs = nil },
		"exit not an OOM exit":  func(r *memoryPressureRecord) { r.Exit.OOMKilled = false },
		"daemon still running":  func(r *memoryPressureRecord) { r.Exit.Running = true },
	} {
		w := memWindow(t, memEventsKill)
		mutate(&w)
		if v, err := classifyMemoryPressure(w, memoryPressureChecks{}); v != memoryOtherVictim || err != nil {
			t.Errorf("%s: %v %v", name, v, err)
		}
	}
}

func TestMemoryPressureRefusesIncompleteEvidence(t *testing.T) {
	cases := map[string]func(*memoryPressureRecord){
		"generation incomplete":     func(r *memoryPressureRecord) { r.Generation = containerGeneration{} },
		"record id differs":         func(r *memoryPressureRecord) { r.Generation.ID = "m2" },
		"record start differs":      func(r *memoryPressureRecord) { r.Generation.StartedAt = "2026-10-25T00:01:00Z" },
		"after from another start":  func(r *memoryPressureRecord) { r.After.Generation.StartedAt = "2026-10-25T00:01:00Z" },
		"before from another id":    func(r *memoryPressureRecord) { r.Before.Generation.ID = "m2" },
		"post-window reading gone":  func(r *memoryPressureRecord) { r.After = nil },
		"first reading too late":    func(r *memoryPressureRecord) { r.Before.At = r.ExecStart.Add(time.Millisecond) },
		"second reading too early":  func(r *memoryPressureRecord) { r.After.At = r.ExecEnd.Add(-time.Millisecond) },
		"window not ordered":        func(r *memoryPressureRecord) { r.ExecEnd = r.ExecStart },
		"window start missing":      func(r *memoryPressureRecord) { r.ExecStart = time.Time{} },
		"expected limit unlimited":  func(r *memoryPressureRecord) { r.Expected = memoryLimit{Unlimited: true} },
		"expected limit zero":       func(r *memoryPressureRecord) { r.Expected = memoryLimit{} },
		"limit differs from expect": func(r *memoryPressureRecord) { r.Expected.Bytes *= 2 },
		"limit removed after":       func(r *memoryPressureRecord) { r.After.Max = memoryLimit{Unlimited: true} },
		"swap unlimited before":     func(r *memoryPressureRecord) { r.Before.SwapMax = memoryLimit{Unlimited: true} },
		"swap finite after":         func(r *memoryPressureRecord) { r.After.SwapMax = memoryLimit{Bytes: 4096} },
		"max counter decreased":     func(r *memoryPressureRecord) { r.After.Events.Max = 1 },
		"oom counter decreased":     func(r *memoryPressureRecord) { r.After.Events.OOM = 0 },
		"kill counter decreased":    func(r *memoryPressureRecord) { r.Before.Events.OOMKill = 5 },
		"kill without oom":          func(r *memoryPressureRecord) { r.After.Events.OOM = r.Before.Events.OOM },
		"oom without max":           func(r *memoryPressureRecord) { r.After.Events.Max = r.Before.Events.Max },
		"daemon outcome unobserved": func(r *memoryPressureRecord) { r.Exit = nil },
		"exit of another start":     func(r *memoryPressureRecord) { r.Exit.Generation.StartedAt = "2026-10-25T00:01:00Z" },
		"exit of another id":        func(r *memoryPressureRecord) { r.Exit.Generation.ID = "m2" },
		"exit before the window":    func(r *memoryPressureRecord) { r.Exit.FinishedAt = r.ExecStart.Add(-time.Millisecond) },
		"exit after the window":     func(r *memoryPressureRecord) { r.Exit.FinishedAt = r.ExecEnd.Add(time.Millisecond) },
		"exit time missing":         func(r *memoryPressureRecord) { r.Exit.FinishedAt = time.Time{} },
		"membership unobserved":     func(r *memoryPressureRecord) { r.ProcsBefore = memoryProcs{} },
		"pid missing":               func(r *memoryPressureRecord) { r.PID = 0 },
		"process start missing":     func(r *memoryPressureRecord) { r.ProcessStart = "" },
		"stopped without a kill": func(r *memoryPressureRecord) {
			r.After.Events = r.Before.Events
		},
		"oom handling without a kill": func(r *memoryPressureRecord) {
			r.After.Events = memoryEvents{Max: 9, OOM: 2, OOMKill: r.Before.Events.OOMKill}
		},
	}
	for name, mutate := range cases {
		w := memWindow(t, memEventsKill)
		mutate(&w)
		v, err := classifyMemoryPressure(w, memoryPressureChecks{})
		if v != memoryPressureIncomplete || !errors.Is(err, errMemoryPressure) {
			t.Errorf("%s: %v %v", name, v, err)
		}
	}

	// Missing readings or a missing controller are unsupported, never zero.
	noBefore := memWindow(t, memEventsKill)
	noBefore.Before = nil
	if v, _ := classifyMemoryPressure(noBefore, memoryPressureChecks{}); v != memoryPressureUnsupported {
		t.Errorf("missing first reading: %v", v)
	}
	noCtl := memWindow(t, memEventsKill)
	noCtl.After.ControllerListed = false
	if v, _ := classifyMemoryPressure(noCtl, memoryPressureChecks{}); v != memoryPressureUnsupported {
		t.Errorf("controller dropped: %v", v)
	}
	hier := memWindow(t, memEventsKill)
	hier.After.Source = "cgroup-v2-hierarchical:memory"
	if v, err := classifyMemoryPressure(hier, memoryPressureChecks{}); v != memoryPressureIncomplete || err == nil {
		t.Errorf("foreign source: %v %v", v, err)
	}
}

// Each rule is switched off in turn; the matching scenario must then be
// accepted as a daemon OOM, proving the tests above exercise the rule.
func TestMemoryPressureNegativeControlsBreakEachRule(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*memoryPressureRecord)
		checks memoryPressureChecks
	}{
		{"join", func(r *memoryPressureRecord) { r.After.Generation.ID = "m2" }, memoryPressureChecks{skipJoin: true}},
		{"bracket", func(r *memoryPressureRecord) { r.Before.At = r.ExecStart.Add(time.Second) }, memoryPressureChecks{skipBracket: true}},
		{"limit", func(r *memoryPressureRecord) { r.Expected.Bytes *= 2 }, memoryPressureChecks{skipLimit: true}},
		{"swap", func(r *memoryPressureRecord) { r.After.SwapMax = memoryLimit{Unlimited: true} }, memoryPressureChecks{skipSwap: true}},
		{"rollback", func(r *memoryPressureRecord) { r.Before.Events.Max = 20 }, memoryPressureChecks{skipRollback: true}},
		{"consistency", func(r *memoryPressureRecord) { r.After.Events.OOM = r.Before.Events.OOM }, memoryPressureChecks{skipConsist: true}},
		{"exit join", func(r *memoryPressureRecord) { r.Exit.Generation.ID = "m2" }, memoryPressureChecks{skipExitJoin: true}},
		{"exit window", func(r *memoryPressureRecord) { r.Exit.FinishedAt = r.ExecEnd.Add(time.Hour) }, memoryPressureChecks{skipExitWindow: true}},
		{"singleton", func(r *memoryPressureRecord) { r.ProcsBefore.PIDs = []int{7, 9} }, memoryPressureChecks{skipSingleton: true}},
		{"later helper", func(r *memoryPressureRecord) { r.OtherProcSeen = true }, memoryPressureChecks{skipSingleton: true}},
		{"oom flag", func(r *memoryPressureRecord) { r.Exit.OOMKilled = false }, memoryPressureChecks{skipOOMFlag: true}},
		{"process gone", func(r *memoryPressureRecord) { r.Exit.Running = true }, memoryPressureChecks{skipGone: true}},
		{"kill count", func(r *memoryPressureRecord) { r.After.Events.OOMKill = r.Before.Events.OOMKill + 2 }, memoryPressureChecks{skipKillCount: true}},
	}
	for _, c := range cases {
		r := memWindow(t, memEventsKill)
		c.mutate(&r)
		strict, _ := classifyMemoryPressure(r, memoryPressureChecks{})
		if strict == memoryDaemonVictim {
			t.Errorf("%s: strict classifier accepted the broken window", c.name)
		}
		if broken, _ := classifyMemoryPressure(r, c.checks); broken != memoryDaemonVictim {
			t.Errorf("%s: disabled rule still refused (%v); control proves nothing", c.name, broken)
		}
	}
}
