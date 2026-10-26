//go:build all || integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"
)

// This file is the Ceph-free scratch memory-ceiling control of the memory
// qualification: a bounded allocator runs with docker exec inside one scratch
// container whose hard memory ceiling is finite and whose swap is bounded to
// zero. Its pure parts parse the allocator report, convert engine records and
// judge one window on top of the merged memory window judge
// (classifyMemoryPressure), which is reused unchanged: the scratch init process
// plays the role of the target process, so a counted kill with a surviving init
// is exactly that judge's "other or unknown victim" outcome.
//
// The allocator runs in the target's own cgroup, so a kill is "allocator OOM in
// the target cgroup" and never a daemon OOM. This file dispatches nothing and
// reads no cgroup of its own accord beyond the injectable roots, so the
// Docker-free fixtures drive it with synthetic files and literal records. The
// ceilings are provisional scratch values, not supported Ceph defaults.

var (
	errMemoryScratch      = errors.New("memory scratch window is not usable")
	errMemoryScratchState = errors.New("memory scratch engine record is not usable")
)

// memoryAllocFinal is the completion line of the allocator.
type memoryAllocFinal struct {
	Requested, Allocated uint64
}

// memoryAllocReport is the parsed allocator output. Progress is the last
// progress value (zero when the allocator printed none) and Final is nil when
// no completion line was printed.
type memoryAllocReport struct {
	Progress uint64
	Chunks   int
	Final    *memoryAllocFinal
}

// memoryAllocObject decodes one JSON object with no duplicate keys into raw
// values. Anything after the object on the line is refused.
func memoryAllocObject(line []byte) (map[string]json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(line))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return nil, fmt.Errorf("%w: allocator line is not a JSON object", errMemoryScratch)
	}
	out := map[string]json.RawMessage{}
	for dec.More() {
		key, err := dec.Token()
		name, ok := key.(string)
		if err != nil || !ok {
			return nil, fmt.Errorf("%w: allocator line has a malformed key", errMemoryScratch)
		}
		if _, dup := out[name]; dup {
			return nil, fmt.Errorf("%w: allocator line repeats key %q", errMemoryScratch, name)
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return nil, fmt.Errorf("%w: allocator line has a malformed value", errMemoryScratch)
		}
		out[name] = raw
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return nil, fmt.Errorf("%w: allocator line is not closed", errMemoryScratch)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: trailing bytes on an allocator line", errMemoryScratch)
	}
	return out, nil
}

// memoryAllocUint accepts only a plain non-negative decimal integer. null,
// strings, floats, exponents, signs and leading zeros are refused instead of
// decoding to a zero that would count as progress.
func memoryAllocUint(raw json.RawMessage, key string) (uint64, error) {
	s := string(raw)
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil || strconv.FormatUint(n, 10) != s || n > math.MaxInt64 {
		return 0, fmt.Errorf("%w: allocator field %q is not a non-negative integer", errMemoryScratch, key)
	}
	return n, nil
}

// parseMemoryAllocReport decodes the allocator's output. Empty output is a
// valid report of an allocator that printed nothing before it ended. Every line
// must be newline-terminated, progress values must strictly increase, the
// completion line must be last with completed equal to the JSON value true and
// its allocated value must equal the last progress value.
func parseMemoryAllocReport(data []byte) (memoryAllocReport, error) {
	var r memoryAllocReport
	if len(data) == 0 {
		return r, nil
	}
	if data[len(data)-1] != '\n' {
		return memoryAllocReport{}, fmt.Errorf("%w: allocator output ends inside a line", errMemoryScratch)
	}
	for _, line := range bytes.Split(data[:len(data)-1], []byte("\n")) {
		if r.Final != nil {
			return memoryAllocReport{}, fmt.Errorf("%w: output continues after the completion line", errMemoryScratch)
		}
		obj, err := memoryAllocObject(line)
		if err != nil {
			return memoryAllocReport{}, err
		}
		if raw, ok := obj["progress_bytes"]; ok {
			if len(obj) != 1 {
				return memoryAllocReport{}, fmt.Errorf("%w: progress line has extra keys", errMemoryScratch)
			}
			n, err := memoryAllocUint(raw, "progress_bytes")
			if err != nil {
				return memoryAllocReport{}, err
			}
			if n == 0 || n <= r.Progress {
				return memoryAllocReport{}, fmt.Errorf("%w: progress does not increase", errMemoryScratch)
			}
			r.Progress = n
			r.Chunks++
			continue
		}
		if len(obj) != 3 {
			return memoryAllocReport{}, fmt.Errorf("%w: completion line needs exactly its three keys", errMemoryScratch)
		}
		if completed, ok := obj["completed"]; !ok || string(completed) != "true" {
			return memoryAllocReport{}, fmt.Errorf("%w: line is neither progress nor a completion", errMemoryScratch)
		}
		var f memoryAllocFinal
		if raw, ok := obj["requested_bytes"]; !ok {
			return memoryAllocReport{}, fmt.Errorf("%w: requested_bytes is missing", errMemoryScratch)
		} else if f.Requested, err = memoryAllocUint(raw, "requested_bytes"); err != nil {
			return memoryAllocReport{}, err
		}
		if raw, ok := obj["allocated_bytes"]; !ok {
			return memoryAllocReport{}, fmt.Errorf("%w: allocated_bytes is missing", errMemoryScratch)
		} else if f.Allocated, err = memoryAllocUint(raw, "allocated_bytes"); err != nil {
			return memoryAllocReport{}, err
		}
		if f.Allocated != r.Progress {
			return memoryAllocReport{}, fmt.Errorf("%w: completion disagrees with the last progress value", errMemoryScratch)
		}
		r.Final = &f
	}
	return r, nil
}

// parseCgroupProcs decodes cgroup.procs: zero or more positive, distinct
// process ids, one per newline-terminated line.
func parseCgroupProcs(data string) (memoryProcs, error) {
	out := memoryProcs{Observed: true, PIDs: []int{}}
	if data == "" {
		return out, nil
	}
	if !strings.HasSuffix(data, "\n") {
		return memoryProcs{}, fmt.Errorf("%w: cgroup.procs ends inside a line", errMemoryScratch)
	}
	seen := map[int]bool{}
	for _, field := range strings.Split(strings.TrimSuffix(data, "\n"), "\n") {
		pid, err := strconv.Atoi(field)
		if err != nil || pid <= 0 || strconv.Itoa(pid) != field || seen[pid] {
			return memoryProcs{}, fmt.Errorf("%w: cgroup.procs line", errMemoryScratch)
		}
		seen[pid] = true
		out.PIDs = append(out.PIDs, pid)
	}
	return out, nil
}

// readMemoryScratchWitness takes one memory witness and the cgroup member list
// of the frozen generation, bracketed by two identity fences exactly like the
// CPU witness read. The member list is read after the memory files.
func readMemoryScratchWitness(t *cpuWitnessTarget, state func() (cpuEngineState, error), now func() time.Time) (*memoryWitness, memoryProcs, error) {
	inspect := func() error {
		s, err := state()
		if err != nil {
			return fmt.Errorf("%w: engine inspection: %v", errCPUBinding, err)
		}
		return t.fence(s)
	}
	if err := inspect(); err != nil {
		return nil, memoryProcs{}, err
	}
	w, err := readMemoryWitness(t.Generation, t.group, now())
	if err != nil {
		return nil, memoryProcs{}, err
	}
	var procs memoryProcs
	if w.ControllerListed {
		data, err := os.ReadFile(filepath.Join(t.group, "cgroup.procs"))
		if err != nil {
			return nil, memoryProcs{}, fmt.Errorf("%w: cgroup.procs unavailable", errMemoryWitnessUnavail)
		}
		if procs, err = parseCgroupProcs(string(data)); err != nil {
			return nil, memoryProcs{}, err
		}
	}
	if t.betweenFences != nil {
		t.betweenFences()
	}
	if !t.checks.skipPostFence {
		if err := inspect(); err != nil {
			return nil, memoryProcs{}, err
		}
	}
	return &w, procs, nil
}

// memoryScratchEngineMatches checks that the engine was created with exactly
// the expected hard ceiling and the same swap-plus-memory value (no container
// swap) and without a restart policy. A missing HostConfig is a refusal.
func memoryScratchEngineMatches(c container.InspectResponse, ceiling int64) error {
	if ceiling <= 0 {
		return fmt.Errorf("%w: ceiling is not finite", errMemoryScratchState)
	}
	h := c.HostConfig
	switch {
	case h == nil:
		return fmt.Errorf("%w: inspection has no host configuration", errMemoryScratchState)
	case h.Memory != ceiling:
		return fmt.Errorf("%w: engine memory ceiling differs", errMemoryScratchState)
	case h.MemorySwap != ceiling:
		return fmt.Errorf("%w: engine swap setting allows container swap", errMemoryScratchState)
	case h.RestartPolicy.Name != container.RestartPolicyDisabled:
		return fmt.Errorf("%w: restart policy is enabled", errMemoryScratchState)
	}
	return nil
}

// memoryExitFromInspect copies the engine's current record of the frozen
// container. A missing state, identity or (for a stopped container) finish time
// is a refusal and never decodes into an empty record.
func memoryExitFromInspect(c container.InspectResponse) (*memoryDaemonExit, error) {
	if c.State == nil || c.ID == "" || c.State.StartedAt == "" {
		return nil, fmt.Errorf("%w: inspection lacks state or identity", errMemoryScratchState)
	}
	exit := &memoryDaemonExit{
		Generation: containerGeneration{ID: c.ID, StartedAt: c.State.StartedAt},
		Running:    c.State.Running && !c.State.Paused && !c.State.Restarting,
		OOMKilled:  c.State.OOMKilled,
		ExitCode:   c.State.ExitCode,
	}
	if !c.State.Running {
		at, err := time.Parse(time.RFC3339Nano, c.State.FinishedAt)
		if err != nil || at.IsZero() || at.Year() <= 1 {
			return nil, fmt.Errorf("%w: stopped container has no finish time", errMemoryScratchState)
		}
		exit.FinishedAt = at
	}
	return exit, nil
}

type memoryScratchVerdict string

const (
	// memoryScratchWithinCeiling: control. The allocator held a request below
	// the ceiling, completed, and no local counter advanced.
	memoryScratchWithinCeiling memoryScratchVerdict = "within-ceiling"
	// memoryScratchAllocatorOOM: the allocator, which asked for more than the
	// ceiling, was killed exactly once while the scratch init survived.
	memoryScratchAllocatorOOM memoryScratchVerdict = "allocator-oom-in-target-cgroup"
	// memoryScratchCeilingNotEnforced: the allocator held at least the ceiling
	// and finished with no counter advance. The mechanism was not reproduced.
	memoryScratchCeilingNotEnforced memoryScratchVerdict = "ceiling-not-enforced"
	memoryScratchUnsupported        memoryScratchVerdict = "unsupported"
	memoryScratchInconclusive       memoryScratchVerdict = "incomplete"
)

// memoryScratchRecord is one allocator window. Window.PID is the scratch init,
// Window.Exit the engine's record of it after the window, and the exec fields
// are the separate outcome of the allocator itself.
type memoryScratchRecord struct {
	Window memoryPressureRecord
	// Requested is the bound the controller passed to the allocator.
	Requested uint64
	// ExecExit is nil when the exec outcome is unknown.
	ExecExit *int
	// Report is nil when the allocator output was unread or unparsable.
	Report *memoryAllocReport
}

// memoryScratchChecks disables one rule per negative control.
type memoryScratchChecks struct {
	skipReport    bool
	skipExec      bool
	skipJoin      bool
	skipComplete  bool
	skipBound     bool
	skipProgress  bool
	skipKillCount bool
	skipInitAlive bool
	skipMembers   bool
}

func memoryScratchRefuse(format string, args ...any) (memoryScratchVerdict, error) {
	return memoryScratchInconclusive, fmt.Errorf("%w: "+format, append([]any{errMemoryScratch}, args...)...)
}

// classifyMemoryScratch judges one allocator window. The merged window judge
// decides the shared admission rules (generation join, ordered bracket,
// recorded reading times, finite ceiling on both readings, zero swap, counter
// monotonicity and consistency, observed init outcome). Missing or unobserved
// evidence is incomplete and never an empty value that counts as evidence.
func classifyMemoryScratch(r memoryScratchRecord, checks memoryScratchChecks) (memoryScratchVerdict, error) {
	if r.Requested == 0 {
		return memoryScratchRefuse("declared request is not set")
	}
	base, err := classifyMemoryPressure(r.Window, memoryPressureChecks{})
	switch base {
	case memoryPressureUnsupported:
		return memoryScratchUnsupported, nil
	case memoryPressureIncomplete:
		return memoryScratchInconclusive, err
	}
	ceiling := r.Window.Expected.Bytes
	// Before the allocator starts the scratch init must be the only member, so
	// that a counted kill with a surviving init can only be the allocator.
	if p := r.Window.ProcsBefore; !checks.skipMembers && (!p.Observed || len(p.PIDs) != 1 || p.PIDs[0] != r.Window.PID) {
		return memoryScratchRefuse("the scratch init was not the only cgroup member before the window")
	}
	if !checks.skipReport && r.Report == nil {
		return memoryScratchRefuse("allocator output is unobserved")
	}
	if !checks.skipExec && r.ExecExit == nil {
		return memoryScratchRefuse("allocator exec outcome is unobserved")
	}
	if r.Report == nil || r.ExecExit == nil {
		// Only reachable when a negative control disabled the presence rules.
		return memoryScratchRefuse("allocator outcome is unobserved")
	}
	switch base {
	case memoryLimitNotReached:
		f := r.Report.Final
		if !checks.skipComplete && (f == nil || *r.ExecExit != 0) {
			return memoryScratchRefuse("allocator did not complete although no kill was counted")
		}
		if f == nil {
			return memoryScratchRefuse("completion line is missing")
		}
		if !checks.skipJoin && (f.Requested != r.Requested || f.Allocated != r.Requested) {
			return memoryScratchRefuse("completion does not match the declared request")
		}
		if r.Requested >= ceiling {
			return memoryScratchCeilingNotEnforced, nil
		}
		return memoryScratchWithinCeiling, nil
	case memoryOtherVictim:
		b, a := r.Window.Before.Events, r.Window.After.Events
		if !checks.skipInitAlive && !r.Window.Exit.Running {
			return memoryScratchRefuse("scratch init is not running after the window")
		}
		if !checks.skipKillCount && a.OOMKill-b.OOMKill != 1 {
			return memoryScratchRefuse("the window counted %d kills, not exactly one", a.OOMKill-b.OOMKill)
		}
		if !checks.skipBound && r.Requested <= ceiling {
			return memoryScratchRefuse("a kill was counted although the request does not exceed the ceiling")
		}
		if !checks.skipComplete && (r.Report.Final != nil || *r.ExecExit != 137) {
			return memoryScratchRefuse("allocator did not end by SIGKILL without a completion line")
		}
		if !checks.skipProgress && r.Report.Progress >= ceiling {
			return memoryScratchRefuse("allocator held the whole ceiling before it was killed")
		}
		return memoryScratchAllocatorOOM, nil
	}
	// A hit limit without a kill, or the scratch init itself being the victim,
	// is not one of the two classified outcomes of this control.
	return memoryScratchRefuse("window outcome %s is not a classified scratch outcome", base)
}
