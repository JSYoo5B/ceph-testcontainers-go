//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"errors"
	"testing"

	"github.com/moby/moby/api/types/container"
)

// Docker-free fixtures for the engine adapter of the CPU witness. Synthetic
// inspection responses only. Not covered: no engine call, no native cgroup and
// no claim about the installed engine or kernel.

func goodInspect() container.InspectResponse {
	return container.InspectResponse{
		ID:    "c1",
		State: &container.State{Running: true, Pid: 4242, StartedAt: "2026-10-25T00:00:00.000000001Z"},
	}
}

func TestCPUEngineStateCopiesTheFenceFields(t *testing.T) {
	in := goodInspect()
	in.RestartCount = 2
	in.State.Paused, in.State.Restarting = true, true
	s, err := cpuEngineStateFromInspect(in, cpuEngineChecks{})
	want := cpuEngineState{ID: "c1", StartedAt: in.State.StartedAt, PID: 4242, Running: true, Paused: true, Restarting: true, RestartCount: 2}
	if err != nil || s != want {
		t.Fatalf("got %+v %v, want %+v", s, err, want)
	}
	if err := s.Check(containerGeneration{ID: "c1", StartedAt: in.State.StartedAt}, 4242); !errors.Is(err, errCPUBinding) {
		t.Fatalf("paused restarted state must be refused by the fence: %v", err)
	}
}

func TestCPUEngineStateRefusesMissingFieldsInsteadOfDecodingEmpty(t *testing.T) {
	cases := map[string]func(*container.InspectResponse){
		"nil state":        func(c *container.InspectResponse) { c.State = nil },
		"empty id":         func(c *container.InspectResponse) { c.ID = "" },
		"empty start time": func(c *container.InspectResponse) { c.State.StartedAt = "" },
	}
	for name, mutate := range cases {
		in := goodInspect()
		mutate(&in)
		if _, err := cpuEngineStateFromInspect(in, cpuEngineChecks{}); !errors.Is(err, errCPUEngineState) {
			t.Errorf("%s: want refusal, got %v", name, err)
		}
		// Negative control: the lenient conversion accepts the same input.
		if _, err := cpuEngineStateFromInspect(in, cpuEngineChecks{lenient: true}); err != nil {
			t.Errorf("%s: lenient control should not refuse: %v", name, err)
		}
	}
}

func TestCPUEngineEmptyStatesCannotSatisfyTheFence(t *testing.T) {
	// A lenient empty state still fails the fence, but only because the frozen
	// generation is nonempty. Two empty states compare equal, which is why the
	// adapter must refuse them first.
	a, _ := cpuEngineStateFromInspect(container.InspectResponse{}, cpuEngineChecks{lenient: true})
	b, _ := cpuEngineStateFromInspect(container.InspectResponse{}, cpuEngineChecks{lenient: true})
	if a != b {
		t.Fatal("control: empty states are expected to compare equal")
	}
	if _, err := cpuEngineStateFromInspect(container.InspectResponse{}, cpuEngineChecks{}); !errors.Is(err, errCPUEngineState) {
		t.Fatalf("strict conversion must refuse an empty inspection: %v", err)
	}
}

func TestCPUMaxMatchesNanoCPUs(t *testing.T) {
	ok := []struct {
		max  cpuMax
		nano int64
	}{
		{cpuMax{Quota: 100000, Period: 100000}, 1_000_000_000},
		{cpuMax{Quota: 25000, Period: 100000}, 250_000_000},
		{cpuMax{Quota: 200000, Period: 100000}, 2_000_000_000},
		{cpuMax{Quota: 50000, Period: 50000}, 1_000_000_000},
	}
	for _, c := range ok {
		if err := cpuMaxMatchesNanoCPUs(c.max, c.nano); err != nil {
			t.Errorf("%+v %d: %v", c.max, c.nano, err)
		}
	}
	bad := map[string]struct {
		max  cpuMax
		nano int64
	}{
		"different quota":     {cpuMax{Quota: 50000, Period: 100000}, 1_000_000_000},
		"unlimited":           {cpuMax{Unlimited: true, Period: 100000}, 1_000_000_000},
		"zero engine limit":   {cpuMax{Quota: 100000, Period: 100000}, 0},
		"negative engine":     {cpuMax{Quota: 100000, Period: 100000}, -1},
		"zero value max":      {cpuMax{}, 1_000_000_000},
		"inexact division":    {cpuMax{Quota: 33333, Period: 100000}, 333_333_333},
		"huge values no wrap": {cpuMax{Quota: 1 << 63, Period: 1 << 62}, 1_000_000_000},
	}
	for name, c := range bad {
		if err := cpuMaxMatchesNanoCPUs(c.max, c.nano); !errors.Is(err, errCPUWitnessInconclusive) {
			t.Errorf("%s: want inconclusive, got %v", name, err)
		}
	}
}
