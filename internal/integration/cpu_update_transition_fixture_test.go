//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"errors"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
)

// Docker-free fixtures for the CPU update transition model. Synthetic records
// only. Not covered: no update call, no engine, no native cgroup and no claim
// about the installed engine or kernel.

const (
	tBase  int64 = 1_000_000_000
	tTight int64 = 250_000_000
)

func tGen() containerGeneration {
	return containerGeneration{ID: "c1", StartedAt: "2026-10-25T00:00:00.000000001Z"}
}

func tRecord(nano int64) cpuTransitionRecord {
	g := tGen()
	return cpuTransitionRecord{
		Generation:    g,
		RequestedNano: nano,
		ReplyReceived: true,
		AfterState:    cpuEngineState{ID: g.ID, StartedAt: g.StartedAt, PID: 7, Running: true},
		AfterLimits:   cpuEngineLimits{NanoCPUs: nano},
		Kernel: &cpuWitness{
			Generation: g, At: time.Unix(10, 0), Source: cpuWitnessSource, ControllerListed: true,
			Max: cpuMax{Quota: uint64(nano / 10_000), Period: 100000},
		},
	}
}

func TestCPUTransitionAppliedNeedsEngineAndKernelAgreement(t *testing.T) {
	v, err := classifyCPUTransition(tRecord(tTight), cpuTransitionChecks{})
	if err != nil || v != cpuTransitionApplied {
		t.Fatalf("got %s %v", v, err)
	}
}

// Each case is a record that must not classify as applied, paired with the
// check whose removal (negative control) lets it through.
func TestCPUTransitionRefusesWhatIsNotEffective(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*cpuTransitionRecord)
		want    cpuTransitionVerdict
		wantErr bool
		skip    cpuTransitionChecks
	}{
		{"stopped target saved config only", func(r *cpuTransitionRecord) { r.AfterState.Running = false }, cpuTransitionConfiguredOnly, false, cpuTransitionChecks{skipRunning: true}},
		{"paused target", func(r *cpuTransitionRecord) { r.AfterState.Paused = true }, cpuTransitionConfiguredOnly, false, cpuTransitionChecks{skipRunning: true}},
		{"restarting target", func(r *cpuTransitionRecord) { r.AfterState.Restarting = true }, cpuTransitionConfiguredOnly, false, cpuTransitionChecks{skipRunning: true}},
		{"no kernel reading", func(r *cpuTransitionRecord) { r.Kernel = nil }, cpuTransitionUncertain, true, cpuTransitionChecks{skipKernel: true}},
		{"controller not listed", func(r *cpuTransitionRecord) { r.Kernel.ControllerListed = false }, cpuTransitionUncertain, true, cpuTransitionChecks{skipKernel: true}},
		{"kernel still shows old quota", func(r *cpuTransitionRecord) { r.Kernel.Max.Quota = 100000 }, cpuTransitionConfiguredOnly, false, cpuTransitionChecks{skipKernel: true}},
		{"kernel unlimited", func(r *cpuTransitionRecord) { r.Kernel.Max = cpuMax{Unlimited: true, Period: 100000} }, cpuTransitionConfiguredOnly, false, cpuTransitionChecks{skipKernel: true}},
		{"warnings returned", func(r *cpuTransitionRecord) { r.Warnings = []string{"w"} }, cpuTransitionUncertain, true, cpuTransitionChecks{skipWarnings: true}},
		{"mixed encoding", func(r *cpuTransitionRecord) { r.AfterLimits.CPUQuota = 25000 }, cpuTransitionUncertain, true, cpuTransitionChecks{skipEncoding: true}},
		{"replaced generation", func(r *cpuTransitionRecord) { r.AfterState.StartedAt = "later" }, cpuTransitionUncertain, true, cpuTransitionChecks{skipGeneration: true}},
		{"kernel reading of another generation", func(r *cpuTransitionRecord) { r.Kernel.Generation.ID = "c2" }, cpuTransitionUncertain, true, cpuTransitionChecks{skipGeneration: true}},
		{"lost reply", func(r *cpuTransitionRecord) { r.ReplyReceived = false }, cpuTransitionAppliedUncertainReply, false, cpuTransitionChecks{skipReply: true}},
	}
	for _, c := range cases {
		r := tRecord(tTight)
		c.mutate(&r)
		v, err := classifyCPUTransition(r, cpuTransitionChecks{})
		if v == cpuTransitionApplied || v != c.want || (err != nil) != c.wantErr || (err != nil && !errors.Is(err, errCPUTransition)) {
			t.Errorf("%s: got %s %v, want %s (error=%t)", c.name, v, err, c.want, c.wantErr)
		}
		// Negative control: with the rule disabled the same record is applied.
		if v, _ := classifyCPUTransition(r, c.skip); v != cpuTransitionApplied {
			t.Errorf("%s: control with the rule disabled should reach applied, got %s", c.name, v)
		}
	}
}

// An inspected value that lacks the request is "not-applied" only when the
// same-generation kernel reading also lacks it. Each other combination keeps
// the uncertainty. The skipKernel control shows the earlier single-source
// behavior (inspection alone decided) for every case.
func TestCPUTransitionNotAppliedNeedsKernelAgreement(t *testing.T) {
	old := func() cpuTransitionRecord {
		r := tRecord(tTight)
		r.AfterLimits.NanoCPUs = tBase
		return r
	}
	cases := []struct {
		name   string
		mutate func(*cpuTransitionRecord)
		want   cpuTransitionVerdict
	}{
		{"inspected old, kernel requested", func(r *cpuTransitionRecord) {}, cpuTransitionUncertain},
		{"inspected old, kernel missing", func(r *cpuTransitionRecord) { r.Kernel = nil }, cpuTransitionUncertain},
		{"inspected old, controller not listed", func(r *cpuTransitionRecord) { r.Kernel.ControllerListed = false }, cpuTransitionUncertain},
		{"inspected old, kernel unreadable zero value", func(r *cpuTransitionRecord) { r.Kernel.Max = cpuMax{} }, cpuTransitionUncertain},
		{"inspected old, task not running", func(r *cpuTransitionRecord) {
			r.AfterState.Running = false
			r.Kernel.Max = cpuMax{Quota: uint64(tBase / 10_000), Period: 100000}
		}, cpuTransitionUncertain},
		{"inspected old, kernel old", func(r *cpuTransitionRecord) { r.Kernel.Max = cpuMax{Quota: uint64(tBase / 10_000), Period: 100000} }, cpuTransitionNotApplied},
		{"inspected old, kernel unlimited", func(r *cpuTransitionRecord) { r.Kernel.Max = cpuMax{Unlimited: true, Period: 100000} }, cpuTransitionNotApplied},
	}
	for _, c := range cases {
		r := old()
		c.mutate(&r)
		v, err := classifyCPUTransition(r, cpuTransitionChecks{})
		if v != c.want || (c.want == cpuTransitionUncertain) != (err != nil) {
			t.Errorf("%s: got %s %v, want %s", c.name, v, err, c.want)
		}
		if c.want == cpuTransitionUncertain {
			// Negative control: ignoring the effective evidence (or the
			// running state) collapses the case into a definitive verdict.
			if v, _ := classifyCPUTransition(r, cpuTransitionChecks{skipKernel: true, skipRunning: true}); v != cpuTransitionNotApplied {
				t.Errorf("%s: single-source control should reach not-applied, got %s", c.name, v)
			}
		}
	}
}

func TestCPUTransitionRefusesNonPositiveRequests(t *testing.T) {
	for _, n := range []int64{0, -1} {
		r := tRecord(tTight)
		r.RequestedNano = n
		r.AfterLimits.NanoCPUs = n
		if v, err := classifyCPUTransition(r, cpuTransitionChecks{}); !errors.Is(err, errCPUTransition) || v != cpuTransitionUncertain {
			t.Errorf("request %d: got %s %v", n, v, err)
		}
	}
}

func TestCPUEngineLimitsFromInspectRefusesMissingHostConfig(t *testing.T) {
	if _, err := cpuEngineLimitsFromInspect(container.InspectResponse{}); !errors.Is(err, errCPUTransition) {
		t.Fatalf("missing host configuration must be refused, got %v", err)
	}
	var hc container.HostConfig
	hc.NanoCPUs = tTight
	got, err := cpuEngineLimitsFromInspect(container.InspectResponse{HostConfig: &hc})
	if err != nil || got != (cpuEngineLimits{NanoCPUs: tTight}) {
		t.Fatalf("got %+v %v", got, err)
	}
	// Control: an explicit empty host configuration is a real zero reading.
	got, err = cpuEngineLimitsFromInspect(container.InspectResponse{HostConfig: &container.HostConfig{}})
	if err != nil || got != (cpuEngineLimits{}) {
		t.Fatalf("explicit empty config: %+v %v", got, err)
	}
}

func TestCPURoundTripRequiresExactBaselineOnOneGeneration(t *testing.T) {
	ok := func() (cpuTransitionRecord, cpuTransitionRecord) { return tRecord(tTight), tRecord(tBase) }
	a, b := ok()
	if err := verifyCPURoundTrip(tBase, a, b, cpuRoundTripChecks{}); err != nil {
		t.Fatalf("clean round trip refused: %v", err)
	}
	cases := []struct {
		name     string
		baseline int64
		mutate   func(a, b *cpuTransitionRecord)
		skip     cpuRoundTripChecks
	}{
		{"unlimited baseline", 0, func(a, b *cpuTransitionRecord) {}, cpuRoundTripChecks{skipFinite: true, skipTighter: true, skipRestore: true}},
		{"not tighter", tBase, func(a, b *cpuTransitionRecord) { *a = tRecord(tBase) }, cpuRoundTripChecks{skipTighter: true}},
		{"restored to other value", tBase, func(a, b *cpuTransitionRecord) { *b = tRecord(tBase / 2) }, cpuRoundTripChecks{skipRestore: true}},
		{"other generation", tBase, func(a, b *cpuTransitionRecord) {
			*b = tRecord(tBase)
			b.Generation.ID = "c2"
			b.AfterState.ID = "c2"
			b.Kernel.Generation.ID = "c2"
		}, cpuRoundTripChecks{skipSameGen: true}},
		{"no kernel reading after restore", tBase, func(a, b *cpuTransitionRecord) { b.Kernel = nil }, cpuRoundTripChecks{}},
	}
	for _, c := range cases {
		a, b := ok()
		c.mutate(&a, &b)
		if err := verifyCPURoundTrip(c.baseline, a, b, cpuRoundTripChecks{}); !errors.Is(err, errCPUTransition) {
			t.Errorf("%s: want refusal, got %v", c.name, err)
		}
		// Negative control: with the rule disabled the same sequence passes.
		if c.skip != (cpuRoundTripChecks{}) {
			if err := verifyCPURoundTrip(c.baseline, a, b, c.skip); err != nil {
				t.Errorf("%s: control with the rule disabled should pass: %v", c.name, err)
			}
		}
	}
	// A transition that is only configured must also fail the sequence.
	a, b = ok()
	b.AfterState.Running = false
	if err := verifyCPURoundTrip(tBase, a, b, cpuRoundTripChecks{}); !errors.Is(err, errCPUTransition) {
		t.Errorf("configured-only restore must fail: %v", err)
	}
}
