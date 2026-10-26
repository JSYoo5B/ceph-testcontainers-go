//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"errors"
	"testing"

	"github.com/moby/moby/api/types/container"
)

// Docker-free fixtures for the record assembly of the finite CPU round trip.
// Synthetic inspections only. Not covered: no update call, no engine and no
// native cgroup.

func rtInspect(nano int64) container.InspectResponse {
	g := tGen()
	return container.InspectResponse{
		ID:         g.ID,
		State:      &container.State{StartedAt: g.StartedAt, Pid: 7, Running: true},
		HostConfig: &container.HostConfig{Resources: container.Resources{NanoCPUs: nano}},
	}
}

func TestCPUUpdateReplyErrorIsUncertain(t *testing.T) {
	if ok, _ := cpuUpdateReply(nil, errors.New("connection reset")); ok {
		t.Fatal("an error reply must not count as received")
	}
	ok, w := cpuUpdateReply([]string{"w"}, nil)
	if !ok || len(w) != 1 {
		t.Fatalf("clean reply lost: %v %v", ok, w)
	}
}

func TestCPURoundTripAssemblyFeedsTheClassifier(t *testing.T) {
	g := tGen()
	k := tRecord(tTight).Kernel
	rec, err := assembleCPUTransition(g, tTight, true, nil, rtInspect(tTight), k)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := classifyCPUTransition(rec, cpuTransitionChecks{}); err != nil || v != cpuTransitionApplied {
		t.Fatalf("got %s %v", v, err)
	}
	// A lost reply with effective evidence stays distinguishable.
	rec, _ = assembleCPUTransition(g, tTight, false, nil, rtInspect(tTight), k)
	if v, _ := classifyCPUTransition(rec, cpuTransitionChecks{}); v != cpuTransitionAppliedUncertainReply {
		t.Fatalf("lost reply classified %s", v)
	}
	// A missing kernel reading is unavailable evidence, not a clean update.
	rec, _ = assembleCPUTransition(g, tTight, true, nil, rtInspect(tTight), nil)
	if v, err := classifyCPUTransition(rec, cpuTransitionChecks{}); err == nil || v != cpuTransitionUncertain {
		t.Fatalf("no kernel reading classified %s %v", v, err)
	}
	// Warnings are copied, so later mutation of the caller's slice is not seen.
	w := []string{"x"}
	rec, _ = assembleCPUTransition(g, tTight, true, w, rtInspect(tTight), k)
	w[0] = "y"
	if rec.Warnings[0] != "x" {
		t.Fatal("warnings alias the caller's slice")
	}
}

func TestCPURoundTripAssemblyRefusesIncompleteInspection(t *testing.T) {
	g := tGen()
	noHost := rtInspect(tTight)
	noHost.HostConfig = nil
	noState := rtInspect(tTight)
	noState.State = nil
	for name, in := range map[string]container.InspectResponse{"no host config": noHost, "no state": noState} {
		if _, err := assembleCPUTransition(g, tTight, true, nil, in, nil); !errors.Is(err, errCPURoundTripRecord) {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

func TestGuardedCPUUpdateDispatchesNothingToAStaleTarget(t *testing.T) {
	g := tGen()
	restarted := rtInspect(tTight)
	restarted.State.StartedAt = "2026-10-25T01:00:00.000000001Z"
	replaced := rtInspect(tTight)
	replaced.ID = "c2"
	paused := rtInspect(tTight)
	paused.State.Paused = true
	count := rtInspect(tTight)
	count.RestartCount = 1
	cases := map[string]container.InspectResponse{"restarted": restarted, "replaced": replaced, "paused": paused, "restart count": count}
	for name, in := range cases {
		calls := 0
		_, err := guardedCPUUpdate(g, func() (container.InspectResponse, error) { return in, nil }, func() ([]string, error) { calls++; return nil, nil })
		if calls != 0 || !errors.Is(err, errCPUUpdateStale) {
			t.Fatalf("%s: dispatched %d updates, err=%v", name, calls, err)
		}
	}
	calls := 0
	_, err := guardedCPUUpdate(g, func() (container.InspectResponse, error) { return container.InspectResponse{}, errors.New("down") }, func() ([]string, error) { calls++; return nil, nil })
	if calls != 0 || !errors.Is(err, errCPUUpdateStale) {
		t.Fatalf("inspection failure: dispatched %d, err=%v", calls, err)
	}
	calls = 0
	if _, err := guardedCPUUpdate(g, func() (container.InspectResponse, error) { return rtInspect(tBase), nil }, func() ([]string, error) { calls++; return nil, nil }); err != nil || calls != 1 {
		t.Fatalf("the running captured generation must dispatch once: %d %v", calls, err)
	}
}
