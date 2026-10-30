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
)

// Docker-free fixtures for binding the CPU witness to a frozen generation over
// a synthetic proc and cgroup tree. Not covered: no engine call, no native
// cgroup, no CPU pressure, no quota update and no kernel qualification.

const bindPID = 4242

// procStat has the comm field with spaces and a parenthesis and a start time
// at field 22.
func procStat(start string) string {
	return "4242 (ceph (mon) x) S 1 4242 4242 0 -1 4194560 100 0 0 0 1 1 0 0 20 0 1 0 " + start + " 1000 100 18446744073709551615\n"
}

type bindTree struct {
	roots cpuFSRoots
	rel   string
}

func newBindTree(t *testing.T) *bindTree {
	t.Helper()
	root := t.TempDir()
	b := &bindTree{roots: cpuFSRoots{Proc: filepath.Join(root, "proc"), Cgroup: filepath.Join(root, "cg")}, rel: "system.slice/docker-" + cpuGen.ID + ".scope"}
	b.setProc(t, procStat("777"), "0::/"+b.rel+"\n")
	b.writeGroup(t, b.rel, "cpu memory\n", "25000 100000\n", cpuStatZero)
	return b
}

func (b *bindTree) setProc(t *testing.T, stat, cgroup string) {
	t.Helper()
	dir := filepath.Join(b.roots.Proc, "4242")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string]string{"stat": stat, "cgroup": cgroup} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func (b *bindTree) writeGroup(t *testing.T, rel, controllers, max, stat string) {
	t.Helper()
	dir := filepath.Join(b.roots.Cgroup, rel)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string]string{"cgroup.controllers": controllers, "cpu.max": max, "cpu.stat": stat} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func goodState() cpuEngineState {
	return cpuEngineState{ID: cpuGen.ID, StartedAt: cpuGen.StartedAt, PID: bindPID, Running: true}
}

func fixedState(s cpuEngineState) func() (cpuEngineState, error) {
	return func() (cpuEngineState, error) { return s, nil }
}

func tick() func() time.Time {
	at := cpuT0
	return func() time.Time { at = at.Add(time.Second); return at }
}

func bound(t *testing.T, b *bindTree, checks cpuBindChecks) *cpuWitnessTarget {
	t.Helper()
	target, err := bindCPUWitnessTarget(b.roots, cpuGen, goodState(), checks)
	if err != nil {
		t.Fatal(err)
	}
	return target
}

func TestCPUBindingReadsThrottlingForTheFrozenGeneration(t *testing.T) {
	b := newBindTree(t)
	target := bound(t, b, cpuBindChecks{})
	if target.ProcessStart != "777" {
		t.Fatalf("process start %q", target.ProcessStart)
	}
	now := tick()
	before, err := target.read(fixedState(goodState()), now)
	if err != nil || before.Stat.Throttled != 0 || !before.ControllerListed {
		t.Fatalf("first reading: %+v %v", before, err)
	}
	b.writeGroup(t, b.rel, "cpu memory\n", "25000 100000\n", cpuStatHit)
	after, err := target.read(fixedState(goodState()), now)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := classifyCPUThrottle(before, after, false, cpuClassifyChecks{}); v != cpuVerdictThrottled || err != nil {
		t.Fatalf("verdict %v %v", v, err)
	}
	if r := target.Receipt(); strings.Contains(r, b.roots.Cgroup) || strings.Contains(r, "system.slice") || !strings.Contains(r, "process_start=777") {
		t.Fatalf("receipt leaks a path or lacks identity: %q", r)
	}
}

func TestCPUBindingWithoutControllerStaysIneligible(t *testing.T) {
	b := newBindTree(t)
	b.writeGroup(t, b.rel, "memory pids\n", "-", "-")
	target := bound(t, b, cpuBindChecks{})
	now := tick()
	first, err := target.read(fixedState(goodState()), now)
	if err != nil || first.ControllerListed {
		t.Fatalf("%+v %v", first, err)
	}
	second, _ := target.read(fixedState(goodState()), now)
	if v, err := classifyCPUThrottle(first, second, true, cpuClassifyChecks{}); v != cpuVerdictUnsupported || !errors.Is(err, errCPUWitnessIneligible) {
		t.Fatalf("verdict %v %v", v, err)
	}
}

func TestCPUBindingRefusesUnadmittedEngineState(t *testing.T) {
	b := newBindTree(t)
	cases := map[string]func(*cpuEngineState){
		"replaced":   func(s *cpuEngineState) { s.ID = "other" },
		"restarted":  func(s *cpuEngineState) { s.StartedAt = "2026-10-25T00:00:09Z" },
		"stopped":    func(s *cpuEngineState) { s.Running = false },
		"paused":     func(s *cpuEngineState) { s.Paused = true },
		"restarting": func(s *cpuEngineState) { s.Restarting = true },
		"no pid":     func(s *cpuEngineState) { s.PID = 0 },
		"restarts":   func(s *cpuEngineState) { s.RestartCount = 1 },
	}
	for name, mutate := range cases {
		s := goodState()
		mutate(&s)
		if _, err := bindCPUWitnessTarget(b.roots, cpuGen, s, cpuBindChecks{}); !errors.Is(err, errCPUBinding) {
			t.Errorf("%s: want refusal, got %v", name, err)
		}
	}
	if _, err := bindCPUWitnessTarget(b.roots, containerGeneration{ID: "c1"}, goodState(), cpuBindChecks{}); !errors.Is(err, errCPUBinding) {
		t.Errorf("incomplete generation: %v", err)
	}
}

func TestCPUBindingRefusesUnsafeOrAmbiguousCgroupMembership(t *testing.T) {
	good := "0::/system.slice/docker-" + cpuGen.ID + ".scope\n"
	cases := map[string]string{
		"hybrid":        "1:cpu:/x\n0::/system.slice/docker-" + cpuGen.ID + ".scope\n",
		"v1 only":       "4:cpu:/docker/" + cpuGen.ID + "\n",
		"two lines":     good + good,
		"root":          "0::/\n",
		"relative":      "0::system.slice/docker-" + cpuGen.ID + ".scope\n",
		"escape":        "0::/../docker-" + cpuGen.ID + "\n",
		"unclean":       "0::/a//docker-" + cpuGen.ID + "\n",
		"other name":    "0::/system.slice/docker-other.scope\n",
		"empty":         "",
		"missing group": "0::/system.slice/docker-" + cpuGen.ID + "-gone.scope\n",
	}
	for name, content := range cases {
		b := newBindTree(t)
		b.setProc(t, procStat("777"), content)
		if _, err := bindCPUWitnessTarget(b.roots, cpuGen, goodState(), cpuBindChecks{}); !errors.Is(err, errCPUBinding) {
			t.Errorf("%s: want refusal, got %v", name, err)
		}
	}
	b := newBindTree(t)
	b.setProc(t, "garbage", good)
	if _, err := bindCPUWitnessTarget(b.roots, cpuGen, goodState(), cpuBindChecks{}); !errors.Is(err, errCPUBinding) {
		t.Errorf("malformed process stat: %v", err)
	}
	// A regular file where the cgroup directory must be is not a directory.
	b = newBindTree(t)
	file := filepath.Join(b.roots.Cgroup, "plain-"+cpuGen.ID)
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	b.setProc(t, procStat("777"), "0::/plain-"+cpuGen.ID+"\n")
	if _, err := bindCPUWitnessTarget(b.roots, cpuGen, goodState(), cpuBindChecks{}); !errors.Is(err, errCPUBinding) {
		t.Errorf("file as cgroup: %v", err)
	}
}

// drift describes a change that must be caught by exactly one fence rule.
type drift struct {
	name   string
	apply  func(t *testing.T, b *bindTree) // changes the local tree
	state  func(*cpuEngineState)           // changes the engine state
	skip   func(*cpuBindChecks)            // negative control
	errHas string
}

func replaceGroup(t *testing.T, b *bindTree) {
	t.Helper()
	// Create the replacement before removing the original so the two never
	// share an inode.
	tmp := b.rel + ".new"
	b.writeGroup(t, tmp, "cpu memory\n", "25000 100000\n", cpuStatZero)
	if err := os.RemoveAll(filepath.Join(b.roots.Cgroup, b.rel)); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(b.roots.Cgroup, tmp), filepath.Join(b.roots.Cgroup, b.rel)); err != nil {
		t.Fatal(err)
	}
}

var drifts = []drift{
	{name: "process start", apply: func(t *testing.T, b *bindTree) { b.setProc(t, procStat("888"), "0::/"+b.rel+"\n") },
		skip: func(c *cpuBindChecks) { c.skipProcessStart = true }, errHas: "process start"},
	{name: "cgroup moved", apply: func(t *testing.T, b *bindTree) {
		other := "system.slice/docker-" + cpuGen.ID + "-b.scope"
		b.writeGroup(t, other, "cpu\n", "25000 100000\n", cpuStatZero)
		b.setProc(t, procStat("777"), "0::/"+other+"\n")
	}, skip: func(c *cpuBindChecks) { c.skipCgroupPath = true }, errHas: "another cgroup"},
	{name: "cgroup replaced", apply: replaceGroup,
		skip: func(c *cpuBindChecks) { c.skipIdentity = true }, errHas: "replaced"},
	{name: "engine restart", state: func(s *cpuEngineState) { s.StartedAt = "2026-10-25T00:00:09Z" }, errHas: "generation"},
	{name: "engine pid", state: func(s *cpuEngineState) { s.PID = 4243 }, errHas: "PID"},
	{name: "engine paused", state: func(s *cpuEngineState) { s.Paused = true }, errHas: "not running"},
}

func TestCPUBindingFenceCatchesDriftBeforeAndAfterTheRead(t *testing.T) {
	for _, d := range drifts {
		// Before the first fence.
		b := newBindTree(t)
		target := bound(t, b, cpuBindChecks{})
		state := goodState()
		if d.apply != nil {
			d.apply(t, b)
		}
		if d.state != nil {
			d.state(&state)
		}
		if w, err := target.read(fixedState(state), tick()); err == nil || w != nil || !strings.Contains(err.Error(), d.errHas) {
			t.Errorf("%s before: want %q refusal, got %v %v", d.name, d.errHas, w, err)
		}
		// Between the reads and the closing fence: the first fence passes.
		b = newBindTree(t)
		target = bound(t, b, cpuBindChecks{})
		current := goodState()
		calls := 0
		target.betweenFences = func() {
			if d.apply != nil {
				d.apply(t, b)
			}
			if d.state != nil {
				d.state(&current)
			}
		}
		inspect := func() (cpuEngineState, error) { calls++; return current, nil }
		if w, err := target.read(inspect, tick()); err == nil || w != nil || !strings.Contains(err.Error(), d.errHas) || calls != 2 {
			t.Errorf("%s after: want %q refusal after 2 inspections, got %v %v (calls %d)", d.name, d.errHas, w, err, calls)
		}
	}
}

func TestCPUBindingEngineInspectionFailureIsRefusal(t *testing.T) {
	b := newBindTree(t)
	target := bound(t, b, cpuBindChecks{})
	fail := func() (cpuEngineState, error) { return cpuEngineState{}, errors.New("engine unreachable") }
	if w, err := target.read(fail, tick()); w != nil || !errors.Is(err, errCPUBinding) {
		t.Fatalf("%v %v", w, err)
	}
}

func TestCPUBindingNegativeControlsBreakEachRule(t *testing.T) {
	for _, d := range drifts {
		if d.skip == nil {
			continue
		}
		var checks cpuBindChecks
		d.skip(&checks)
		b := newBindTree(t)
		target := bound(t, b, checks)
		d.apply(t, b)
		if _, err := target.read(fixedState(goodState()), tick()); err != nil {
			t.Errorf("%s: rule switched off must accept the drift, got %v", d.name, err)
		}
	}
	// Dropping the closing fence accepts a drift introduced during the read.
	b := newBindTree(t)
	target := bound(t, b, cpuBindChecks{skipPostFence: true})
	target.betweenFences = func() { b.setProc(t, procStat("888"), "0::/"+b.rel+"\n") }
	if _, err := target.read(fixedState(goodState()), tick()); err != nil {
		t.Errorf("post fence switched off must accept the mid-read drift, got %v", err)
	}
}
