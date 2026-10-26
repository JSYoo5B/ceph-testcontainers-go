//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
)

// Docker-free fixtures for the scratch memory-ceiling control. Allocator
// reports are literal strings, cgroup readings are synthetic files in the
// cgroup v2 format and engine records are literals; no engine, container,
// allocator or native cgroup was involved.
//
// Not covered: no native memory pressure, no real allocator kill, no Ceph
// daemon, no daemon OOM, no event-handle survival, no recovery and no engine or
// kernel qualification. A fixture pass shows the parser and judge rules, not a
// native effect.

const (
	scratchCeiling   = 134217728 // 128 MiB, a provisional scratch value and not a product default
	scratchInitPID   = bindPID
	scratchBelow     = 67108864  // 64 MiB request of the within-ceiling control
	scratchAbove     = 201326592 // 192 MiB request of the kill window
	scratchEventsOK  = "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n"
	scratchEventsHit = "low 0\nhigh 0\nmax 12\noom 1\noom_kill 1\noom_group_kill 0\n"
	// A limit hit without OOM handling.
	scratchEventsMaxOnly = "low 0\nhigh 0\nmax 5\noom 0\noom_kill 0\noom_group_kill 0\n"
	scratchEventsTwoKill = "low 0\nhigh 0\nmax 12\noom 2\noom_kill 2\noom_group_kill 0\n"
)

const (
	scratchProgress = `{"progress_bytes": 8388608}` + "\n" + `{"progress_bytes": 16777216}` + "\n"
	scratchDone     = `{"progress_bytes": 8388608}` + "\n" + `{"progress_bytes": 16777216}` + "\n" +
		`{"completed": true, "requested_bytes": 16777216, "allocated_bytes": 16777216}` + "\n"
)

func TestMemoryAllocReportRequiresEveryTypedField(t *testing.T) {
	r, err := parseMemoryAllocReport([]byte(scratchDone))
	if err != nil || r.Final == nil || r.Final.Requested != 16777216 || r.Final.Allocated != 16777216 || r.Progress != 16777216 || r.Chunks != 2 {
		t.Fatalf("complete report: %+v %v", r, err)
	}
	// A killed allocator leaves progress lines and no completion line, and one
	// that was killed before the first chunk leaves nothing. Both are valid
	// reports of an allocator that did not complete.
	if r, err := parseMemoryAllocReport([]byte(scratchProgress)); err != nil || r.Final != nil || r.Progress != 16777216 {
		t.Fatalf("partial report: %+v %v", r, err)
	}
	if r, err := parseMemoryAllocReport(nil); err != nil || r.Final != nil || r.Progress != 0 || r.Chunks != 0 {
		t.Fatalf("empty report: %+v %v", r, err)
	}
	line := func(s string) string { return s + "\n" }
	p1 := line(`{"progress_bytes":10}`)
	bad := map[string]string{
		"no trailing newline":     `{"progress_bytes":10}`,
		"unterminated fragment":   p1 + `{"progress_by`,
		"blank line":              p1 + "\n",
		"not an object":           line(`[10]`),
		"progress string":         line(`{"progress_bytes":"10"}`),
		"progress null":           line(`{"progress_bytes":null}`),
		"progress float":          line(`{"progress_bytes":1.5}`),
		"progress exponent":       line(`{"progress_bytes":1e3}`),
		"progress negative":       line(`{"progress_bytes":-1}`),
		"progress leading zero":   line(`{"progress_bytes":01}`),
		"progress zero":           line(`{"progress_bytes":0}`),
		"progress at 2^63":        line(`{"progress_bytes":9223372036854775808}`),
		"progress repeats":        p1 + p1,
		"progress decreases":      p1 + line(`{"progress_bytes":5}`),
		"progress extra key":      line(`{"progress_bytes":10,"x":1}`),
		"duplicate key":           line(`{"progress_bytes":10,"progress_bytes":11}`),
		"trailing bytes":          line(`{"progress_bytes":10} x`),
		"unknown line":            line(`{"hello":1}`),
		"completed false":         p1 + line(`{"completed":false,"requested_bytes":10,"allocated_bytes":10}`),
		"completed string":        p1 + line(`{"completed":"true","requested_bytes":10,"allocated_bytes":10}`),
		"completed null":          p1 + line(`{"completed":null,"requested_bytes":10,"allocated_bytes":10}`),
		"completed missing":       p1 + line(`{"requested_bytes":10,"allocated_bytes":10,"x":1}`),
		"requested missing":       p1 + line(`{"completed":true,"allocated_bytes":10,"x":1}`),
		"requested null":          p1 + line(`{"completed":true,"requested_bytes":null,"allocated_bytes":10}`),
		"allocated missing":       p1 + line(`{"completed":true,"requested_bytes":10,"x":1}`),
		"allocated string":        p1 + line(`{"completed":true,"requested_bytes":10,"allocated_bytes":"10"}`),
		"allocated float":         p1 + line(`{"completed":true,"requested_bytes":10,"allocated_bytes":10.0}`),
		"allocated at 2^64-1":     p1 + line(`{"completed":true,"requested_bytes":10,"allocated_bytes":18446744073709551615}`),
		"allocated != progress":   p1 + line(`{"completed":true,"requested_bytes":10,"allocated_bytes":9}`),
		"completion without work": line(`{"completed":true,"requested_bytes":10,"allocated_bytes":10}`),
		"extra key in completion": p1 + line(`{"completed":true,"requested_bytes":10,"allocated_bytes":10,"x":1}`),
		"output after completion": p1 + line(`{"completed":true,"requested_bytes":10,"allocated_bytes":10}`) + p1,
	}
	for name, in := range bad {
		if _, err := parseMemoryAllocReport([]byte(in)); !errors.Is(err, errMemoryScratch) {
			t.Errorf("%s: accepted or wrong error: %v", name, err)
		}
	}
}

func TestCgroupProcsParserRefusesAmbiguousMembership(t *testing.T) {
	if p, err := parseCgroupProcs("7\n9\n"); err != nil || !p.Observed || len(p.PIDs) != 2 {
		t.Fatalf("members: %+v %v", p, err)
	}
	if p, err := parseCgroupProcs(""); err != nil || !p.Observed || len(p.PIDs) != 0 {
		t.Fatalf("empty cgroup: %+v %v", p, err)
	}
	for name, in := range map[string]string{
		"no newline": "7", "blank line": "7\n\n", "zero": "0\n", "negative": "-3\n", "text": "init\n",
		"leading zero": "07\n", "two fields": "7 8\n", "duplicate": "7\n7\n",
	} {
		if _, err := parseCgroupProcs(in); !errors.Is(err, errMemoryScratch) {
			t.Errorf("%s: accepted or wrong error: %v", name, err)
		}
	}
}

func TestMemoryScratchEngineRecordsAreStrict(t *testing.T) {
	host := func(mem, swap int64, policy container.RestartPolicyMode) container.InspectResponse {
		return container.InspectResponse{HostConfig: &container.HostConfig{Resources: container.Resources{Memory: mem, MemorySwap: swap}, RestartPolicy: container.RestartPolicy{Name: policy}}}
	}
	if err := memoryScratchEngineMatches(host(scratchCeiling, scratchCeiling, container.RestartPolicyDisabled), scratchCeiling); err != nil {
		t.Fatalf("good host config: %v", err)
	}
	for name, c := range map[string]container.InspectResponse{
		"nil host config": {},
		"other ceiling":   host(scratchCeiling*2, scratchCeiling*2, container.RestartPolicyDisabled),
		"swap allowed":    host(scratchCeiling, scratchCeiling*2, container.RestartPolicyDisabled),
		"unlimited swap":  host(scratchCeiling, -1, container.RestartPolicyDisabled),
		"unset swap":      host(scratchCeiling, 0, container.RestartPolicyDisabled),
		"restart policy":  host(scratchCeiling, scratchCeiling, container.RestartPolicyAlways),
	} {
		if err := memoryScratchEngineMatches(c, scratchCeiling); !errors.Is(err, errMemoryScratchState) {
			t.Errorf("%s: accepted or wrong error: %v", name, err)
		}
	}
	if err := memoryScratchEngineMatches(host(0, 0, container.RestartPolicyDisabled), 0); err == nil {
		t.Error("a non-finite ceiling matched")
	}

	state := func(s container.State) container.InspectResponse {
		return container.InspectResponse{ID: cpuGen.ID, State: &s}
	}
	run, err := memoryExitFromInspect(state(container.State{Running: true, StartedAt: cpuGen.StartedAt}))
	if err != nil || !run.Running || run.Generation != cpuGen {
		t.Fatalf("running: %+v %v", run, err)
	}
	if paused, err := memoryExitFromInspect(state(container.State{Running: true, Paused: true, StartedAt: cpuGen.StartedAt})); err != nil || paused.Running {
		t.Fatalf("paused counts as running: %+v %v", paused, err)
	}
	stopped, err := memoryExitFromInspect(state(container.State{StartedAt: cpuGen.StartedAt, FinishedAt: "2026-10-25T00:00:14.5Z", ExitCode: 137, OOMKilled: true}))
	if err != nil || stopped.Running || !stopped.OOMKilled || stopped.ExitCode != 137 || stopped.FinishedAt.IsZero() {
		t.Fatalf("stopped: %+v %v", stopped, err)
	}
	for name, c := range map[string]container.InspectResponse{
		"nil state":          {ID: cpuGen.ID},
		"no id":              {State: &container.State{Running: true, StartedAt: cpuGen.StartedAt}},
		"no start":           state(container.State{Running: true}),
		"stopped no finish":  state(container.State{StartedAt: cpuGen.StartedAt}),
		"stopped zero time":  state(container.State{StartedAt: cpuGen.StartedAt, FinishedAt: "0001-01-01T00:00:00Z"}),
		"stopped bad finish": state(container.State{StartedAt: cpuGen.StartedAt, FinishedAt: "yesterday"}),
	} {
		if _, err := memoryExitFromInspect(c); !errors.Is(err, errMemoryScratchState) {
			t.Errorf("%s: accepted or wrong error: %v", name, err)
		}
	}
}

func scratchGroup(t *testing.T, controllers, max, swap, current, events, procs string) string {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"cgroup.controllers": controllers, "memory.max": max, "memory.swap.max": swap,
		"memory.current": current, "memory.events.local": events, "cgroup.procs": procs,
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

func scratchRead(t *testing.T, group string, at time.Time) *memoryWitness {
	t.Helper()
	w, err := readMemoryWitness(cpuGen, group, at)
	if err != nil {
		t.Fatal(err)
	}
	return &w
}

// scratchWindow builds a complete window of the given request and after-events;
// each case mutates it. Defaults describe the allocator-kill window.
func scratchWindow(t *testing.T, requested uint64, afterEvents string) memoryScratchRecord {
	t.Helper()
	exit := 137
	return memoryScratchRecord{
		Window: memoryPressureRecord{
			Generation:   cpuGen,
			Expected:     memoryLimit{Bytes: scratchCeiling},
			PID:          scratchInitPID,
			ProcessStart: "777",
			ProcsBefore:  memoryProcs{Observed: true, PIDs: []int{scratchInitPID}},
			ExecStart:    cpuT0.Add(time.Second),
			ExecEnd:      cpuT0.Add(7 * time.Second),
			Before:       scratchRead(t, scratchGroup(t, "cpu memory\n", "134217728\n", "0\n", "1000\n", scratchEventsOK, "4242\n"), cpuT0),
			After:        scratchRead(t, scratchGroup(t, "cpu memory\n", "134217728\n", "0\n", "2000\n", afterEvents, "4242\n"), cpuT0.Add(8*time.Second)),
			Exit:         &memoryDaemonExit{Generation: cpuGen, Running: true},
		},
		Requested: requested,
		ExecExit:  &exit,
		Report:    &memoryAllocReport{Progress: 100663296, Chunks: 12},
	}
}

func scratchControl(t *testing.T) memoryScratchRecord {
	t.Helper()
	r := scratchWindow(t, scratchBelow, scratchEventsOK)
	zero := 0
	r.ExecExit = &zero
	r.Report = &memoryAllocReport{Progress: scratchBelow, Chunks: 8, Final: &memoryAllocFinal{Requested: scratchBelow, Allocated: scratchBelow}}
	return r
}

func TestMemoryScratchOutcomes(t *testing.T) {
	if v, err := classifyMemoryScratch(scratchControl(t), memoryScratchChecks{}); v != memoryScratchWithinCeiling || err != nil {
		t.Fatalf("control window: %v %v", v, err)
	}
	if v, err := classifyMemoryScratch(scratchWindow(t, scratchAbove, scratchEventsHit), memoryScratchChecks{}); v != memoryScratchAllocatorOOM || err != nil {
		t.Fatalf("kill window: %v %v", v, err)
	}
	// An allocator that holds the whole request above the ceiling without any
	// counter advance means the ceiling was not reproduced: a result, not a pass.
	r := scratchControl(t)
	r.Requested = scratchAbove
	r.Report = &memoryAllocReport{Progress: scratchAbove, Chunks: 24, Final: &memoryAllocFinal{Requested: scratchAbove, Allocated: scratchAbove}}
	if v, err := classifyMemoryScratch(r, memoryScratchChecks{}); v != memoryScratchCeilingNotEnforced || err != nil {
		t.Fatalf("unenforced ceiling: %v %v", v, err)
	}
	// An unlisted memory controller is unsupported, not a zero.
	r = scratchControl(t)
	r.Window.Before = scratchRead(t, scratchGroup(t, "cpu\n", "-", "-", "-", "-", "-"), cpuT0)
	if v, err := classifyMemoryScratch(r, memoryScratchChecks{}); v != memoryScratchUnsupported || err != nil {
		t.Fatalf("no controller: %v %v", v, err)
	}
}

func TestMemoryScratchRefusesIncompleteEvidence(t *testing.T) {
	type tweak func(*memoryScratchRecord)
	kill := func(f tweak) memoryScratchRecord {
		r := scratchWindow(t, scratchAbove, scratchEventsHit)
		f(&r)
		return r
	}
	control := func(f tweak) memoryScratchRecord {
		r := scratchControl(t)
		f(&r)
		return r
	}
	nonzero, killed := 1, 137
	cases := map[string]memoryScratchRecord{
		"no declared request":         kill(func(r *memoryScratchRecord) { r.Requested = 0 }),
		"allocator output unobserved": kill(func(r *memoryScratchRecord) { r.Report = nil }),
		"exec outcome unobserved":     kill(func(r *memoryScratchRecord) { r.ExecExit = nil }),
		"daemon outcome unobserved":   kill(func(r *memoryScratchRecord) { r.Window.Exit = nil }),
		"init gone": kill(func(r *memoryScratchRecord) {
			r.Window.Exit = &memoryDaemonExit{Generation: cpuGen, OOMKilled: true, ExitCode: 137, FinishedAt: cpuT0.Add(4 * time.Second)}
		}),
		"init of another generation": kill(func(r *memoryScratchRecord) {
			r.Window.Exit.Generation = containerGeneration{ID: "other", StartedAt: cpuGen.StartedAt}
		}),
		"two kills counted": kill(func(r *memoryScratchRecord) {
			r.Window.After = scratchRead(t, scratchGroup(t, "memory\n", "134217728\n", "0\n", "5\n", scratchEventsTwoKill, "4242\n"), cpuT0.Add(8*time.Second))
		}),
		"kill without exceeding request": kill(func(r *memoryScratchRecord) { r.Requested = scratchBelow }),
		"kill but allocator completed": kill(func(r *memoryScratchRecord) {
			r.Report.Final = &memoryAllocFinal{Requested: scratchAbove, Allocated: scratchAbove}
		}),
		"kill but exit zero":           kill(func(r *memoryScratchRecord) { r.ExecExit = new(int) }),
		"kill but exit one":            kill(func(r *memoryScratchRecord) { r.ExecExit = &nonzero }),
		"progress reached the ceiling": kill(func(r *memoryScratchRecord) { r.Report.Progress = scratchCeiling }),
		"limit hit without a kill": kill(func(r *memoryScratchRecord) {
			r.Window.After = scratchRead(t, scratchGroup(t, "memory\n", "134217728\n", "0\n", "5\n", scratchEventsMaxOnly, "4242\n"), cpuT0.Add(8*time.Second))
		}),
		"control did not complete":    control(func(r *memoryScratchRecord) { r.Report.Final = nil }),
		"control exit nonzero":        control(func(r *memoryScratchRecord) { r.ExecExit = &killed }),
		"control completion mismatch": control(func(r *memoryScratchRecord) { r.Report.Final.Allocated = scratchBelow / 2 }),
		"control requested mismatch":  control(func(r *memoryScratchRecord) { r.Report.Final.Requested = scratchBelow * 2 }),
		"control counters advanced": control(func(r *memoryScratchRecord) {
			r.Window.After = scratchRead(t, scratchGroup(t, "memory\n", "134217728\n", "0\n", "5\n", scratchEventsMaxOnly, "4242\n"), cpuT0.Add(8*time.Second))
		}),
		"swap not bounded": control(func(r *memoryScratchRecord) {
			r.Window.After = scratchRead(t, scratchGroup(t, "memory\n", "134217728\n", "max\n", "5\n", scratchEventsOK, "4242\n"), cpuT0.Add(8*time.Second))
		}),
		"ceiling changed mid-window": control(func(r *memoryScratchRecord) {
			r.Window.After = scratchRead(t, scratchGroup(t, "memory\n", "268435456\n", "0\n", "5\n", scratchEventsOK, "4242\n"), cpuT0.Add(8*time.Second))
		}),
		"second reading too early": control(func(r *memoryScratchRecord) { r.Window.After.At = cpuT0.Add(2 * time.Second) }),
		"first reading unstamped":  control(func(r *memoryScratchRecord) { r.Window.Before.At = time.Time{} }),
		"readings of another cgroup": control(func(r *memoryScratchRecord) {
			r.Window.After.Generation = containerGeneration{ID: "other", StartedAt: cpuGen.StartedAt}
		}),
		"second reading unavailable":  control(func(r *memoryScratchRecord) { r.Window.After = nil }),
		"members before not a single": kill(func(r *memoryScratchRecord) { r.Window.ProcsBefore.PIDs = []int{scratchInitPID, 9} }),
		"members before not observed": kill(func(r *memoryScratchRecord) { r.Window.ProcsBefore = memoryProcs{} }),
	}
	for name, r := range cases {
		v, err := classifyMemoryScratch(r, memoryScratchChecks{})
		if v != memoryScratchInconclusive || err == nil || (!errors.Is(err, errMemoryScratch) && !errors.Is(err, errMemoryPressure)) {
			t.Errorf("%s: %v %v", name, v, err)
		}
	}
}

func TestMemoryScratchNegativeControlsBreakEachRule(t *testing.T) {
	var zero, killedExit = 0, 137
	type tc struct {
		name   string
		record func() memoryScratchRecord
		checks memoryScratchChecks
		want   memoryScratchVerdict
	}
	kill := func(f func(*memoryScratchRecord)) func() memoryScratchRecord {
		return func() memoryScratchRecord {
			r := scratchWindow(t, scratchAbove, scratchEventsHit)
			f(&r)
			return r
		}
	}
	control := func(f func(*memoryScratchRecord)) func() memoryScratchRecord {
		return func() memoryScratchRecord {
			r := scratchControl(t)
			f(&r)
			return r
		}
	}
	for _, c := range []tc{
		{"members before", kill(func(r *memoryScratchRecord) { r.Window.ProcsBefore.PIDs = []int{scratchInitPID, 9} }), memoryScratchChecks{skipMembers: true}, memoryScratchAllocatorOOM},
		{"output presence", kill(func(r *memoryScratchRecord) { r.Report = nil }), memoryScratchChecks{skipReport: true}, memoryScratchInconclusive},
		{"exec presence", kill(func(r *memoryScratchRecord) { r.ExecExit = nil }), memoryScratchChecks{skipExec: true}, memoryScratchInconclusive},
		{"init alive", kill(func(r *memoryScratchRecord) {
			r.Window.Exit = &memoryDaemonExit{Generation: cpuGen, Running: false, FinishedAt: cpuT0.Add(4 * time.Second)}
			r.Window.OtherProcSeen = true
		}), memoryScratchChecks{skipInitAlive: true}, memoryScratchAllocatorOOM},
		{"kill count", kill(func(r *memoryScratchRecord) {
			r.Window.After = scratchRead(t, scratchGroup(t, "memory\n", "134217728\n", "0\n", "5\n", scratchEventsTwoKill, "4242\n"), cpuT0.Add(8*time.Second))
		}), memoryScratchChecks{skipKillCount: true}, memoryScratchAllocatorOOM},
		{"request bound", kill(func(r *memoryScratchRecord) { r.Requested = scratchBelow }), memoryScratchChecks{skipBound: true}, memoryScratchAllocatorOOM},
		{"kill completion", kill(func(r *memoryScratchRecord) { r.ExecExit = &zero }), memoryScratchChecks{skipComplete: true}, memoryScratchAllocatorOOM},
		{"progress below ceiling", kill(func(r *memoryScratchRecord) { r.Report.Progress = scratchCeiling }), memoryScratchChecks{skipProgress: true}, memoryScratchAllocatorOOM},
		{"control completion", control(func(r *memoryScratchRecord) { r.ExecExit = &killedExit }), memoryScratchChecks{skipComplete: true}, memoryScratchWithinCeiling},
		{"control join", control(func(r *memoryScratchRecord) { r.Report.Final.Allocated = 1 }), memoryScratchChecks{skipJoin: true}, memoryScratchWithinCeiling},
	} {
		if v, err := classifyMemoryScratch(c.record(), memoryScratchChecks{}); v != memoryScratchInconclusive || err == nil {
			t.Errorf("%s: intact rules accepted the broken record: %v %v", c.name, v, err)
		}
		if v, err := classifyMemoryScratch(c.record(), c.checks); v != c.want {
			t.Errorf("%s: disabling the rule gave %v %v, want %v", c.name, v, err, c.want)
		}
	}
}

func TestMemoryScratchWitnessReadIsFencedAndListsMembers(t *testing.T) {
	b := newBindTree(t)
	dir := filepath.Join(b.roots.Cgroup, b.rel)
	write := func(files map[string]string) {
		t.Helper()
		for name, data := range files {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	write(map[string]string{"memory.max": "134217728\n", "memory.swap.max": "0\n", "memory.current": "5\n", "memory.events.local": scratchEventsOK, "cgroup.procs": "4242\n"})
	target := bound(t, b, cpuBindChecks{})
	w, procs, err := readMemoryScratchWitness(target, fixedState(goodState()), tick())
	if err != nil || !w.ControllerListed || w.Max.Bytes != scratchCeiling || !procs.Observed || len(procs.PIDs) != 1 || procs.PIDs[0] != bindPID {
		t.Fatalf("read: %+v %+v %v", w, procs, err)
	}
	if w.Generation != cpuGen {
		t.Fatalf("witness generation %+v", w.Generation)
	}

	// A drifted engine record is refused on both sides of the read.
	moved := goodState()
	moved.StartedAt = "later"
	if _, _, err := readMemoryScratchWitness(target, fixedState(moved), tick()); !errors.Is(err, errCPUBinding) {
		t.Fatalf("replaced generation read: %v", err)
	}
	// A cgroup that disappears between the fences is a refusal, not a zero.
	target.betweenFences = func() { _ = os.RemoveAll(dir) }
	if _, _, err := readMemoryScratchWitness(target, fixedState(goodState()), tick()); !errors.Is(err, errCPUBinding) {
		t.Fatalf("cgroup removed mid-read: %v", err)
	}
	// The closing fence is what refuses it: the negative control reads through.
	b2 := newBindTree(t)
	dir2 := filepath.Join(b2.roots.Cgroup, b2.rel)
	for name, data := range map[string]string{"memory.max": "134217728\n", "memory.swap.max": "0\n", "memory.current": "5\n", "memory.events.local": scratchEventsOK, "cgroup.procs": "4242\n"} {
		if err := os.WriteFile(filepath.Join(dir2, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	loose := bound(t, b2, cpuBindChecks{skipPostFence: true})
	loose.betweenFences = func() { _ = os.RemoveAll(dir2) }
	if _, _, err := readMemoryScratchWitness(loose, fixedState(goodState()), tick()); err != nil {
		t.Fatalf("without the closing fence the removed cgroup was refused: %v", err)
	}

	// Unreadable or malformed membership is unavailable evidence.
	b3 := newBindTree(t)
	dir3 := filepath.Join(b3.roots.Cgroup, b3.rel)
	files := map[string]string{"memory.max": "134217728\n", "memory.swap.max": "0\n", "memory.current": "5\n", "memory.events.local": scratchEventsOK}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir3, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	target3 := bound(t, b3, cpuBindChecks{})
	if _, _, err := readMemoryScratchWitness(target3, fixedState(goodState()), tick()); !errors.Is(err, errMemoryWitnessUnavail) {
		t.Fatalf("missing cgroup.procs: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir3, "cgroup.procs"), []byte("4242"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readMemoryScratchWitness(target3, fixedState(goodState()), tick()); !errors.Is(err, errMemoryScratch) {
		t.Fatalf("malformed cgroup.procs: %v", err)
	}
	if strings.Contains(target3.receipt(), b3.roots.Cgroup) {
		t.Fatal("receipt leaks the cgroup path")
	}
}
