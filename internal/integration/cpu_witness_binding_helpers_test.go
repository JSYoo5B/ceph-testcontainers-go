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

// This file binds the CPU-controller witness to the cgroup of one frozen
// container generation. It reuses the identity fence of the memory
// qualification (container ID, engine start time, init PID, kernel process
// start time, cgroup directory identity) and adds a re-resolution of the
// cgroup path from the PID, so a migrated or replaced cgroup cannot be read as
// the frozen target.
//
// Roots are injectable so the Docker-free fixtures drive the real binder over a
// synthetic proc and cgroup tree. No engine call is made here: the caller
// supplies the engine state it inspected, and no native cgroup was read by the
// change that introduced this file.

var errCPUBinding = errors.New("cpu witness binding refused")

// cpuFSRoots are the proc and cgroup v2 mount points.
type cpuFSRoots struct{ Proc, Cgroup string }

// cpuEngineState is the subset of the engine's container state the fence
// compares. It is a copy; the caller inspects the same container ID again for
// every fence.
type cpuEngineState struct {
	ID           string
	StartedAt    string
	PID          int
	Running      bool
	Paused       bool
	Restarting   bool
	RestartCount int
}

func (s cpuEngineState) Check(gen containerGeneration, pid int) error {
	switch {
	case s.ID != gen.ID:
		return fmt.Errorf("%w: container was replaced", errCPUBinding)
	case s.StartedAt != gen.StartedAt:
		return fmt.Errorf("%w: container generation changed", errCPUBinding)
	case !s.Running || s.Paused || s.Restarting:
		return fmt.Errorf("%w: target is not running normally", errCPUBinding)
	case s.PID <= 0 || (pid != 0 && s.PID != pid):
		return fmt.Errorf("%w: init PID changed", errCPUBinding)
	case s.RestartCount != 0:
		return fmt.Errorf("%w: target was restarted", errCPUBinding)
	}
	return nil
}

// cpuBindChecks disables one fence rule per negative control.
type cpuBindChecks struct {
	skipProcessStart bool
	skipCgroupPath   bool
	skipIdentity     bool
	skipPostFence    bool
}

type cpuWitnessTarget struct {
	Generation   containerGeneration
	PID          int
	ProcessStart string

	roots    cpuFSRoots
	group    string // never included in a receipt
	identity os.FileInfo
	checks   cpuBindChecks

	// betweenFences runs after the first fence and the reads, before the
	// closing fence. Tests use it to change the tree mid-read.
	betweenFences func()
}

// resolveCPUGroup maps the init PID to its unified cgroup directory. Only a
// single "0::" line is accepted, the path must stay below the cgroup root and
// one of its elements must contain the container ID.
func resolveCPUGroup(roots cpuFSRoots, pid int, id string) (string, error) {
	data, err := os.ReadFile(filepath.Join(roots.Proc, strconv.Itoa(pid), "cgroup"))
	if err != nil {
		return "", fmt.Errorf("%w: target cgroup unavailable", errCPUBinding)
	}
	text := strings.TrimSuffix(string(data), "\n")
	if strings.Contains(text, "\n") || !strings.HasPrefix(text, "0::/") {
		return "", fmt.Errorf("%w: target is not one cgroup v2 membership", errCPUBinding)
	}
	rel := strings.TrimPrefix(text, "0::/")
	if rel == "" || filepath.Clean(rel) != rel || rel == ".." || strings.HasPrefix(rel, "../") {
		return "", fmt.Errorf("%w: unsafe cgroup path", errCPUBinding)
	}
	named := false
	for _, element := range strings.Split(rel, "/") {
		if id != "" && strings.Contains(element, id) {
			named = true
		}
	}
	if !named {
		return "", fmt.Errorf("%w: cgroup path does not name the container", errCPUBinding)
	}
	return filepath.Join(roots.Cgroup, rel), nil
}

func cpuProcessStart(roots cpuFSRoots, pid int) (string, error) {
	data, err := os.ReadFile(filepath.Join(roots.Proc, strconv.Itoa(pid), "stat"))
	if err != nil {
		return "", fmt.Errorf("%w: process identity unavailable", errCPUBinding)
	}
	start, err := daemonProcessStart(string(data))
	if err != nil {
		return "", fmt.Errorf("%w: %v", errCPUBinding, err)
	}
	return start, nil
}

// bindCPUWitnessTarget freezes the identity of one running generation.
func bindCPUWitnessTarget(roots cpuFSRoots, gen containerGeneration, state cpuEngineState, checks cpuBindChecks) (*cpuWitnessTarget, error) {
	if !gen.Valid() {
		return nil, fmt.Errorf("%w: %v", errCPUBinding, errCPUWitnessGeneration)
	}
	if err := state.Check(gen, 0); err != nil {
		return nil, err
	}
	start, err := cpuProcessStart(roots, state.PID)
	if err != nil {
		return nil, err
	}
	group, err := resolveCPUGroup(roots, state.PID, gen.ID)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(group)
	if err != nil || !info.IsDir() {
		return nil, fmt.Errorf("%w: cgroup directory identity unavailable", errCPUBinding)
	}
	return &cpuWitnessTarget{Generation: gen, PID: state.PID, ProcessStart: start, roots: roots, group: group, identity: info, checks: checks}, nil
}

// fence compares a fresh engine state and the local tree with the frozen one.
func (t *cpuWitnessTarget) fence(state cpuEngineState) error {
	if err := state.Check(t.Generation, t.PID); err != nil {
		return err
	}
	if !t.checks.skipProcessStart {
		start, err := cpuProcessStart(t.roots, t.PID)
		if err != nil {
			return err
		}
		if start != t.ProcessStart {
			return fmt.Errorf("%w: process start changed", errCPUBinding)
		}
	}
	if !t.checks.skipCgroupPath {
		group, err := resolveCPUGroup(t.roots, t.PID, t.Generation.ID)
		if err != nil {
			return err
		}
		if group != t.group {
			return fmt.Errorf("%w: process moved to another cgroup", errCPUBinding)
		}
	}
	if !t.checks.skipIdentity {
		info, err := os.Stat(t.group)
		if err != nil || !os.SameFile(t.identity, info) {
			return fmt.Errorf("%w: cgroup directory replaced or removed", errCPUBinding)
		}
	}
	return nil
}

// read takes one witness bracketed by two fences. The state function is called
// once per fence so both inspections happen on either side of the file reads.
func (t *cpuWitnessTarget) read(state func() (cpuEngineState, error), now func() time.Time) (*cpuWitness, error) {
	inspect := func() error {
		s, err := state()
		if err != nil {
			return fmt.Errorf("%w: engine inspection: %v", errCPUBinding, err)
		}
		return t.fence(s)
	}
	if err := inspect(); err != nil {
		return nil, err
	}
	w, err := readCPUWitness(t.Generation, t.group, now())
	if err != nil {
		return nil, err
	}
	if t.betweenFences != nil {
		t.betweenFences()
	}
	if !t.checks.skipPostFence {
		if err := inspect(); err != nil {
			return nil, err
		}
	}
	return &w, nil
}

// receipt is the public description of the binding. It carries identity
// fields only and never the cgroup path.
func (t *cpuWitnessTarget) Receipt() string {
	return fmt.Sprintf("CPU_WITNESS_BINDING container=%s started=%s pid=%d process_start=%s source=%s", t.Generation.ID, t.Generation.StartedAt, t.PID, t.ProcessStart, cpuWitnessSource)
}
