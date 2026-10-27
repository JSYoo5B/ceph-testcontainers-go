//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
)

// Docker-free controls for the OSD memory-OOM model. Every record is a
// literal, the cgroup tree of the recorder is a temporary directory of
// synthetic files and the process root is a temporary symbolic link; no
// engine, container, Ceph daemon, native cgroup or OOM killer was involved. A
// fixture pass shows the classifiers' rules, not a native effect.
//
// Not covered: a native OOM kill, whether the open event handle survives the
// removal of a real cgroup, a MON or any other role, smaller or larger
// ceilings, recovery under other conditions, or any engine or kernel.

var (
	oomGen = containerGeneration{ID: "osd1", StartedAt: "2026-10-27T00:00:00.000000001Z"}
	oomNew = containerGeneration{ID: "osd1", StartedAt: "2026-10-27T00:05:00.000000001Z"}
	oomT0  = time.Date(2026, 10, 27, 0, 0, 10, 0, time.UTC)
)

const (
	oomPID      = 7
	oomProcess  = "12345"
	oomBaseline = 2 << 30
	oomCeiling  = 64 << 20
)

func oomAt(seconds float64) time.Time {
	return oomT0.Add(time.Duration(seconds * float64(time.Second)))
}

func oomWitness(gen containerGeneration, at time.Time, max uint64, current uint64, events memoryEvents) *memoryWitness {
	return &memoryWitness{Generation: gen, At: at, Source: memoryWitnessSource, ControllerListed: true,
		Max: memoryLimit{Bytes: max}, Current: current, Events: events}
}

func oomState(gen containerGeneration, pid int, running bool) cpuEngineState {
	return cpuEngineState{ID: gen.ID, StartedAt: gen.StartedAt, PID: pid, Running: running}
}

// goodOOMStep is a complete osd-oom-killed window whose closing reading is the
// last sample before the stop.
func goodOOMStep() osdOOMStep {
	return osdOOMStep{
		Generation: oomGen, PID: oomPID, ProcessStart: oomProcess, Executable: "ceph-osd",
		ProcsBefore: memoryProcs{Observed: true, PIDs: []int{oomPID}},
		Baseline:    memoryLimit{Bytes: oomBaseline}, Ceiling: memoryLimit{Bytes: oomCeiling},
		Before:    oomWitness(oomGen, oomAt(3), oomBaseline, 300<<20, memoryEvents{Max: 0, OOM: 0, OOMKill: 0}),
		ExecStart: oomAt(4), ExecEnd: oomAt(6), EffectiveAt: oomAt(4.5),
		Final: &osdOOMFinal{Source: osdOOMFinalLastSample, At: oomAt(5.2), Events: memoryEvents{Max: 44, OOM: 1, OOMKill: 1}},
		Exit:  &memoryDaemonExit{Generation: oomGen, OOMKilled: true, ExitCode: 137, FinishedAt: oomAt(5.1)},
	}
}

func TestOSDOOMStepVerdicts(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*osdOOMStep)
		want   osdOOMVerdict
	}{
		{"last sample before the stop", func(s *osdOOMStep) {}, osdOOMKilled},
		{"post-stop reading", func(s *osdOOMStep) {
			s.Final.Source, s.Final.At = osdOOMFinalPostStop, oomAt(7)
		}, osdOOMKilled},
		{"nothing advanced and the daemon runs", func(s *osdOOMStep) {
			s.Final.Events, s.Exit = memoryEvents{}, &memoryDaemonExit{Generation: oomGen, Running: true}
		}, osdOOMNotReached},
		{"limit hit without OOM handling and the daemon survives", func(s *osdOOMStep) {
			s.Final.Events, s.Exit = memoryEvents{Max: 5}, &memoryDaemonExit{Generation: oomGen, Running: true}
		}, osdOOMHitSurvived},
		{"a kill was counted and the daemon still runs", func(s *osdOOMStep) {
			s.Exit = &memoryDaemonExit{Generation: oomGen, Running: true}
		}, osdOOMOther},
		{"two kills", func(s *osdOOMStep) { s.Final.Events = memoryEvents{Max: 44, OOM: 2, OOMKill: 2} }, osdOOMOther},
		{"engine does not report an OOM exit", func(s *osdOOMStep) { s.Exit.OOMKilled = false }, osdOOMOther},
		{"exit status is not 137", func(s *osdOOMStep) { s.Exit.ExitCode = 1 }, osdOOMOther},
		{"another process was a member before the step", func(s *osdOOMStep) { s.ProcsBefore.PIDs = []int{oomPID, 8} }, osdOOMOther},
		{"another process appeared during the window", func(s *osdOOMStep) { s.OtherProcSeen = true }, osdOOMOther},
		{"the daemon was not the member", func(s *osdOOMStep) { s.ProcsBefore.PIDs = []int{8} }, osdOOMOther},
		{"no memory reading", func(s *osdOOMStep) { s.Before = nil }, osdOOMUnsupported},
		{"controller not listed", func(s *osdOOMStep) { s.Before.ControllerListed = false }, osdOOMUnsupported},
		{"stopped without a counted kill", func(s *osdOOMStep) { s.Final.Events = memoryEvents{} }, osdOOMIncomplete},
		{"running with an OOM exit flag", func(s *osdOOMStep) {
			s.Final.Events, s.Exit = memoryEvents{}, &memoryDaemonExit{Generation: oomGen, Running: true, OOMKilled: true}
		}, osdOOMIncomplete},
		{"membership before the step unobserved", func(s *osdOOMStep) { s.ProcsBefore = memoryProcs{} }, osdOOMIncomplete},
		{"final counters unobserved", func(s *osdOOMStep) { s.Final = nil }, osdOOMIncomplete},
		{"daemon outcome unobserved", func(s *osdOOMStep) { s.Exit = nil }, osdOOMIncomplete},
		{"identity incomplete: no process start", func(s *osdOOMStep) { s.ProcessStart = "" }, osdOOMIncomplete},
		{"identity incomplete: no pid", func(s *osdOOMStep) { s.PID = 0 }, osdOOMIncomplete},
		{"identity incomplete: no generation", func(s *osdOOMStep) { s.Generation = containerGeneration{} }, osdOOMIncomplete},
		{"unknown closing source", func(s *osdOOMStep) { s.Final.Source = "" }, osdOOMIncomplete},
		{"first reading without a time", func(s *osdOOMStep) { s.Before.At = time.Time{} }, osdOOMIncomplete},
		{"window start without a time", func(s *osdOOMStep) { s.ExecStart = time.Time{} }, osdOOMIncomplete},
		{"window end without a time", func(s *osdOOMStep) { s.ExecEnd = time.Time{} }, osdOOMIncomplete},
		{"closing reading without a time", func(s *osdOOMStep) { s.Final.At = time.Time{} }, osdOOMIncomplete},
		{"reading source is not the target-local witness", func(s *osdOOMStep) { s.Before.Source = "stats-api" }, osdOOMIncomplete},
	}
	for _, c := range cases {
		s := goodOOMStep()
		c.mutate(&s)
		if got, err := classifyOSDOOMStep(s, osdOOMStepChecks{}); got != c.want {
			t.Errorf("%s: %s (%v), want %s", c.name, got, err, c.want)
		}
	}
}

// TestOSDOOMStepRulesEachHaveANegativeControl disables exactly one rule at a
// time: the mutated record is refused with the rule and accepted as an OSD OOM
// without it, which shows the rule is what refuses it.
func TestOSDOOMStepRulesEachHaveANegativeControl(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*osdOOMStep)
		skip   func(*osdOOMStepChecks)
		want   osdOOMVerdict // verdict with the rule disabled; killed unless set
		def    osdOOMVerdict // verdict with every rule; incomplete unless set
	}{
		{name: "executable is not ceph-osd", mutate: func(s *osdOOMStep) { s.Executable = "sh" }, skip: func(c *osdOOMStepChecks) { c.skipExecutable = true }},
		{name: "first reading from another generation", mutate: func(s *osdOOMStep) { s.Before.Generation = oomNew }, skip: func(c *osdOOMStepChecks) { c.skipJoin = true }},
		{name: "unrecorded reading time", mutate: func(s *osdOOMStep) { s.Before.At = time.Time{} }, skip: func(c *osdOOMStepChecks) { c.skipStamp = true }},
		{name: "first reading after the dispatch", mutate: func(s *osdOOMStep) { s.Before.At = oomAt(4.5) }, skip: func(c *osdOOMStepChecks) { c.skipBracket = true }},
		{name: "window not ordered", mutate: func(s *osdOOMStep) { s.ExecEnd = s.ExecStart }, skip: func(c *osdOOMStepChecks) { c.skipBracket = true }, want: osdOOMIncomplete},
		{name: "baseline differs from the installed limit", mutate: func(s *osdOOMStep) { s.Before.Max = memoryLimit{Bytes: 1 << 30} }, skip: func(c *osdOOMStepChecks) { c.skipLimit = true }},
		{name: "swap is not zero", mutate: func(s *osdOOMStep) { s.Before.SwapMax = memoryLimit{Unlimited: true} }, skip: func(c *osdOOMStepChecks) { c.skipSwap = true }},
		{name: "resident memory below the headroom", mutate: func(s *osdOOMStep) { s.Before.Current = 100 << 20 }, skip: func(c *osdOOMStepChecks) { c.skipHeadroom = true }},
		{name: "another member before the step", mutate: func(s *osdOOMStep) { s.ProcsBefore.PIDs = []int{oomPID, 9} }, skip: func(c *osdOOMStepChecks) { c.skipSingleton = true }, def: osdOOMOther},
		{name: "outcome from another generation", mutate: func(s *osdOOMStep) { s.Exit.Generation = oomNew }, skip: func(c *osdOOMStepChecks) { c.skipExitJoin = true }},
		{name: "ceiling never observed in force", mutate: func(s *osdOOMStep) { s.EffectiveAt = time.Time{} }, skip: func(c *osdOOMStepChecks) { c.skipEffective = true }},
		{name: "ceiling observed outside the window", mutate: func(s *osdOOMStep) { s.EffectiveAt = oomAt(3.5) }, skip: func(c *osdOOMStepChecks) { c.skipEffective = true }},
		{name: "last sample after the window", mutate: func(s *osdOOMStep) { s.Final.At = oomAt(6.5) }, skip: func(c *osdOOMStepChecks) { c.skipFinalTime = true }},
		{name: "post-stop reading before the end", mutate: func(s *osdOOMStep) { s.Final.Source = osdOOMFinalPostStop }, skip: func(c *osdOOMStepChecks) { c.skipFinalTime = true }},
		{name: "counter decreased", mutate: func(s *osdOOMStep) {
			s.Before.Events = memoryEvents{Max: 50, OOM: 3, OOMKill: 2}
		}, skip: func(c *osdOOMStepChecks) { c.skipRollback = true; c.skipKillCount = true; c.skipConsist = true }},
		{name: "kill counted without OOM handling", mutate: func(s *osdOOMStep) { s.Final.Events = memoryEvents{Max: 44, OOM: 0, OOMKill: 1} }, skip: func(c *osdOOMStepChecks) { c.skipConsist = true }},
		{name: "OOM handling without a limit hit", mutate: func(s *osdOOMStep) { s.Final.Events = memoryEvents{Max: 0, OOM: 1, OOMKill: 1} }, skip: func(c *osdOOMStepChecks) { c.skipConsist = true }},
		{name: "exit before the window", mutate: func(s *osdOOMStep) { s.Exit.FinishedAt = oomAt(3.5) }, skip: func(c *osdOOMStepChecks) { c.skipExitWindow = true }},
		{name: "exit after the window", mutate: func(s *osdOOMStep) { s.Exit.FinishedAt = oomAt(6.5) }, skip: func(c *osdOOMStepChecks) { c.skipExitWindow = true }},
		{name: "exit without a time", mutate: func(s *osdOOMStep) { s.Exit.FinishedAt = time.Time{} }, skip: func(c *osdOOMStepChecks) { c.skipExitWindow = true }},
		{name: "engine reports no OOM exit", mutate: func(s *osdOOMStep) { s.Exit.OOMKilled = false }, skip: func(c *osdOOMStepChecks) { c.skipOOMFlag = true }, def: osdOOMOther},
		{name: "two kills", mutate: func(s *osdOOMStep) { s.Final.Events = memoryEvents{Max: 44, OOM: 2, OOMKill: 2} }, skip: func(c *osdOOMStepChecks) { c.skipKillCount = true }, def: osdOOMOther},
		{name: "exit status is not 137", mutate: func(s *osdOOMStep) { s.Exit.ExitCode = 255 }, skip: func(c *osdOOMStepChecks) { c.skipExitCode = true }, def: osdOOMOther},
	}
	for _, c := range cases {
		want, def := c.want, c.def
		if want == "" {
			want = osdOOMKilled
		}
		if def == "" {
			def = osdOOMIncomplete
		}
		s := goodOOMStep()
		c.mutate(&s)
		if got, err := classifyOSDOOMStep(s, osdOOMStepChecks{}); got != def {
			t.Errorf("%s: with every rule %s (%v), want %s", c.name, got, err, def)
		}
		var checks osdOOMStepChecks
		c.skip(&checks)
		if got, err := classifyOSDOOMStep(s, checks); got != want {
			t.Errorf("%s: with the rule disabled %s (%v), want %s", c.name, got, err, want)
		}
	}
}

func goodOOMTransition(requested int64, at float64, running bool) osdMemTransition {
	tr := osdMemTransition{
		Generation: oomGen, Requested: requested, At: oomAt(at), ReplyReceived: true,
		AfterState:  oomState(oomGen, oomPID, running),
		AfterLimits: osdMemLimits{Memory: requested, Swap: requested, RestartPolicy: "no"},
	}
	if running {
		tr.Kernel = oomWitness(oomGen, oomAt(at+0.5), uint64(requested), 300<<20, memoryEvents{})
	}
	return tr
}

func TestOSDMemTransitionVerdicts(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*osdMemTransition)
		want   osdMemVerdict
	}{
		{"clean update", func(tr *osdMemTransition) {}, osdMemApplied},
		{"lost reply with matching evidence", func(tr *osdMemTransition) { tr.ReplyReceived = false }, osdMemAppliedUncertainRply},
		{"saved on a stopped container", func(tr *osdMemTransition) {
			tr.AfterState.Running, tr.Kernel = false, nil
		}, osdMemConfiguredOnly},
		{"kernel limit unchanged", func(tr *osdMemTransition) { tr.Kernel.Max = memoryLimit{Bytes: 1 << 30} }, osdMemConfiguredOnly},
		{"kernel swap not zero", func(tr *osdMemTransition) { tr.Kernel.SwapMax = memoryLimit{Unlimited: true} }, osdMemConfiguredOnly},
		{"engine and kernel both show another value", func(tr *osdMemTransition) {
			tr.AfterLimits.Memory, tr.AfterLimits.Swap = 1<<30, 1<<30
			tr.Kernel.Max = memoryLimit{Bytes: 1 << 30}
		}, osdMemNotApplied},
		{"engine lacks the request but the kernel has it", func(tr *osdMemTransition) {
			tr.AfterLimits.Memory, tr.AfterLimits.Swap = 1<<30, 1<<30
		}, osdMemUncertain},
		{"engine lacks the request and the task is stopped", func(tr *osdMemTransition) {
			tr.AfterLimits.Memory, tr.AfterLimits.Swap = 1<<30, 1<<30
			tr.AfterState.Running, tr.Kernel = false, nil
		}, osdMemUncertain},
		{"running without a kernel reading", func(tr *osdMemTransition) { tr.Kernel = nil }, osdMemUncertain},
		{"controller not listed", func(tr *osdMemTransition) { tr.Kernel.ControllerListed = false }, osdMemUncertain},
		{"kernel reading has no time", func(tr *osdMemTransition) { tr.Kernel.At = time.Time{} }, osdMemUncertain},
		{"reply has no time", func(tr *osdMemTransition) { tr.At = time.Time{} }, osdMemUncertain},
		{"warnings", func(tr *osdMemTransition) { tr.Warnings = []string{"x"} }, osdMemUncertain},
		{"container swap allowed", func(tr *osdMemTransition) { tr.AfterLimits.Swap = 2 * tr.Requested }, osdMemUncertain},
		{"another generation", func(tr *osdMemTransition) { tr.AfterState.StartedAt = oomNew.StartedAt }, osdMemUncertain},
		{"kernel reading from another generation", func(tr *osdMemTransition) { tr.Kernel.Generation = oomNew }, osdMemUncertain},
		{"empty generation", func(tr *osdMemTransition) { tr.Generation = containerGeneration{} }, osdMemUncertain},
		{"zero request", func(tr *osdMemTransition) { tr.Requested = 0 }, osdMemUncertain},
		{"paused task", func(tr *osdMemTransition) { tr.AfterState.Paused = true }, osdMemConfiguredOnly},
	}
	for _, c := range cases {
		tr := goodOOMTransition(oomBaseline, 1, true)
		c.mutate(&tr)
		if got, err := classifyOSDMemTransition(tr, osdMemChecks{}); got != c.want {
			t.Errorf("%s: %s (%v), want %s", c.name, got, err, c.want)
		}
	}
}

func TestOSDMemTransitionRulesEachHaveANegativeControl(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*osdMemTransition)
		skip   func(*osdMemChecks)
		want   osdMemVerdict
	}{
		{"reply time", func(tr *osdMemTransition) { tr.At = time.Time{} }, func(c *osdMemChecks) { c.skipTime = true }, osdMemApplied},
		{"generation", func(tr *osdMemTransition) { tr.AfterState.StartedAt = "other" }, func(c *osdMemChecks) { c.skipGeneration = true }, osdMemApplied},
		{"warnings", func(tr *osdMemTransition) { tr.Warnings = []string{"x"} }, func(c *osdMemChecks) { c.skipWarnings = true }, osdMemApplied},
		{"kernel", func(tr *osdMemTransition) { tr.Kernel = nil }, func(c *osdMemChecks) { c.skipKernel = true }, osdMemApplied},
		{"reply", func(tr *osdMemTransition) { tr.ReplyReceived = false }, func(c *osdMemChecks) { c.skipReply = true }, osdMemApplied},
		{"running", func(tr *osdMemTransition) { tr.AfterState.Running = false }, func(c *osdMemChecks) { c.skipRunning = true }, osdMemApplied},
	}
	for _, c := range cases {
		tr := goodOOMTransition(oomBaseline, 1, true)
		c.mutate(&tr)
		if got, _ := classifyOSDMemTransition(tr, osdMemChecks{}); got == osdMemApplied {
			t.Errorf("%s: accepted as a clean application", c.name)
		}
		var checks osdMemChecks
		c.skip(&checks)
		if got, err := classifyOSDMemTransition(tr, checks); got != c.want {
			t.Errorf("%s: with the rule disabled %s (%v), want %s", c.name, got, err, c.want)
		}
	}
}

func oomMark(gen containerGeneration, at float64, acked int, running, mapDown bool) osdOOMLedgerMark {
	return osdOOMLedgerMark{Acked: acked, At: oomAt(at), Generation: gen, Running: running, MapDown: mapDown}
}

func goodOOMRun() osdOOMRun {
	step := goodOOMStep()
	r := osdOOMRun{
		Identity: osdOOMIdentity{Generation: oomGen, PID: oomPID, ProcessStart: oomProcess},
		Baseline: oomBaseline, Ceiling: oomCeiling,
		Admit:    goodOOMTransition(oomBaseline, 1, true),
		Pressure: goodOOMTransition(oomCeiling, 5, false),
		Restore:  goodOOMTransition(oomBaseline, 14, false),
		Step:     step,
		Effects: osdOOMEffects{
			MapBefore:   &osdMapEntry{Epoch: 40, Up: true, In: true, UpFrom: 5},
			PreStep:     oomMark(oomGen, 2.5, 10, true, false),
			OutageFloor: oomMark(oomGen, 8.5, 12, false, true), OutageLater: oomMark(oomGen, 12, 15, false, true),
		},
	}
	r.Admit.Kernel.At = oomAt(2)
	r.Pressure.Kernel = nil
	for _, sec := range []float64{7, 9, 11, 13} {
		r.Hold = append(r.Hold, osdOOMHoldSample{At: oomAt(sec), ID: oomGen.ID, StartedAt: oomGen.StartedAt})
	}
	r.Effects.Views = []osdSlowLiveView{
		{At: oomAt(8), Instance: &osdInstanceView{StartedAt: oomGen.StartedAt}, Map: &osdMapEntry{Epoch: 41, Up: false, In: true, UpFrom: 5, DownAt: 41}, PGRead: true, PGState: "active+undersized+degraded"},
	}
	r.Recovery = osdOOMRecovery{
		Started: true, StartedAt: oomAt(16), New: oomNew, NewPID: 99, NewProcess: "777",
		Kernel:   oomWitness(oomNew, oomAt(17), oomBaseline, 200<<20, memoryEvents{}),
		MapAfter: &osdMapEntry{Epoch: 60, Up: true, In: true, UpFrom: 58},
		Clean:    true, LedgerFinal: true,
		AckedFloor: oomMark(oomNew, 16.5, 20, true, false), AckedLater: oomMark(oomNew, 19, 24, true, false),
	}
	return r
}

func TestOSDOOMRunEstablishedAndNeverSurvivable(t *testing.T) {
	v, reasons := classifyOSDOOMRun(goodOOMRun())
	if v != osdOOMRunOOMKilled || len(reasons) != 0 {
		t.Fatalf("complete run: %s %v", v, reasons)
	}
	for _, survivable := range []osdOOMRunVerdict{osdOOMRunNotReached, osdOOMRunHitSurvived} {
		if v == survivable {
			t.Fatalf("an OOM-killed OSD carries the survivable label %s", survivable)
		}
	}
}

func TestOSDOOMRunShortfallsKeepTheKilledVerdictDistinct(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*osdOOMRun)
		reason string
	}{
		{"admission names another generation", func(r *osdOOMRun) { r.Admit.AfterState.StartedAt = oomNew.StartedAt }, "admit transition does not name the frozen generation"},
		{"pressure transition of another container", func(r *osdOOMRun) {
			r.Pressure.Generation = containerGeneration{ID: "osd2", StartedAt: oomGen.StartedAt}
		}, "pressure transition does not name the frozen generation"},
		{"restoration of another generation", func(r *osdOOMRun) { r.Restore.Generation = oomNew; r.Restore.AfterState = oomState(oomNew, 0, false) }, "restore transition does not name the frozen generation"},
		{"admission kernel reading of another generation", func(r *osdOOMRun) { r.Admit.Kernel.Generation = oomNew }, "admit kernel reading names another generation"},
		{"admission names another process", func(r *osdOOMRun) { r.Admit.AfterState.PID = 8 }, "admit transition names another process"},
		{"admission has no kernel reading", func(r *osdOOMRun) { r.Admit.Kernel = nil }, "admission has no kernel reading"},
		{"window names another process", func(r *osdOOMRun) { r.Step.PID = 8; r.Step.ProcsBefore.PIDs = []int{8} }, "the pressure window does not name the frozen process"},
		{"window names another process start", func(r *osdOOMRun) { r.Step.ProcessStart = "1" }, "the pressure window does not name the frozen process"},
		{"admission did not request the baseline", func(r *osdOOMRun) {
			r.Admit.Requested = 1 << 30
			r.Admit.AfterLimits = osdMemLimits{Memory: 1 << 30, Swap: 1 << 30}
		}, "admission did not request the baseline"},
		{"pressure step is not the declared ceiling", func(r *osdOOMRun) { r.Ceiling = 32 << 20 }, "the pressure step did not request the predeclared lower ceiling"},
		{"restoration is not the baseline", func(r *osdOOMRun) {
			r.Restore.Requested = 1 << 30
			r.Restore.AfterLimits = osdMemLimits{Memory: 1 << 30, Swap: 1 << 30}
		}, "restoration did not request the admitted baseline"},
		{"restoration claims an effective limit", func(r *osdOOMRun) {
			r.Restore.AfterState.Running = true
			r.Restore.AfterState.PID = oomPID
			r.Restore.Kernel = oomWitness(oomGen, oomAt(14.5), oomBaseline, 1, memoryEvents{})
		}, "restoration of the stopped container is applied"},
		{"admission time missing", func(r *osdOOMRun) { r.Admit.At = time.Time{} }, "admission is uncertain"},
		{"admission kernel reading time missing", func(r *osdOOMRun) { r.Admit.Kernel.At = time.Time{} }, "not in the declared order"},
		{"first reading precedes admission", func(r *osdOOMRun) { r.Step.Before.At = oomAt(1.5) }, "not in the declared order"},
		{"pressure reply before the dispatch", func(r *osdOOMRun) { r.Pressure.At = oomAt(3.5) }, "the pressure reply is not inside the window"},
		{"pressure reply time missing", func(r *osdOOMRun) { r.Pressure.At = time.Time{} }, "the pressure reply is not inside the window"},
		{"restoration before the window ended", func(r *osdOOMRun) { r.Restore.At = oomAt(5.5) }, "restoration does not follow the window"},
		{"restoration time missing", func(r *osdOOMRun) { r.Restore.At = time.Time{} }, "restoration does not follow the window"},
		{"restart policy enabled", func(r *osdOOMRun) { r.Admit.AfterLimits.RestartPolicy = "always" }, "automatic restart policy"},
		{"engine restarted the daemon by itself", func(r *osdOOMRun) { r.Hold[2].Running = true }, "the engine restarted the daemon by itself"},
		{"engine restart count advanced", func(r *osdOOMRun) { r.Hold[1].RestartCount = 1 }, "the engine restarted the daemon by itself"},
		{"hold observation of another generation", func(r *osdOOMRun) { r.Hold[0].StartedAt = oomNew.StartedAt }, "names another generation"},
		{"hold observations too few", func(r *osdOOMRun) { r.Hold = r.Hold[:2] }, "fewer than three"},
		{"hold span too short", func(r *osdOOMRun) {
			for i := range r.Hold {
				r.Hold[i].At = oomAt(7 + float64(i)/2)
			}
		}, "minimum span"},
		{"hold observation without a time", func(r *osdOOMRun) { r.Hold[1].At = time.Time{} }, "has no time"},
		{"hold precedes the window end", func(r *osdOOMRun) { r.Hold[0].At = oomAt(5.5) }, "precedes the end of the window"},
		{"OSD map never shows it down", func(r *osdOOMRun) { r.Effects.Views[0].Map.Up = true }, "never observed down"},
		{"map before the step missing", func(r *osdOOMRun) { r.Effects.MapBefore = nil }, "not observed up in the map before the step"},
		{"PG never leaves clean", func(r *osdOOMRun) { r.Effects.Views[0].PGState = "active+clean" }, "never observed leaving active+clean"},
		{"PG state unread", func(r *osdOOMRun) { r.Effects.Views[0].PGRead = false }, "never observed leaving active+clean"},
		{"outage view without a time", func(r *osdOOMRun) { r.Effects.Views[0].At = time.Time{} }, "has no time"},
		{"outage view before the window end", func(r *osdOOMRun) { r.Effects.Views[0].At = oomAt(5.9) }, "precedes the end of the window"},
		{"outage view shows a running instance", func(r *osdOOMRun) { r.Effects.Views[0].Instance.Running = true }, "another or running instance"},
		{"ledger advanced before the kill and stayed constant through the outage", func(r *osdOOMRun) {
			// The pre-step mark is far below the floor; a cumulative comparison
			// against it would have passed.
			r.Effects.PreStep.Acked = 1
			r.Effects.OutageFloor.Acked, r.Effects.OutageLater.Acked = 15, 15
		}, "outage progress: the writer was not acknowledged again after the phase floor"},
		{"outage progress too small", func(r *osdOOMRun) { r.Effects.OutageLater.Acked = 14 }, "outage progress: the writer was not acknowledged again"},
		{"outage advance only after the restart", func(r *osdOOMRun) {
			r.Effects.OutageLater.Acked = r.Effects.OutageFloor.Acked
			r.Recovery.AckedLater.Acked = r.Effects.OutageFloor.Acked + 30
		}, "outage progress: the writer was not acknowledged again"},
		{"outage floor taken while the target ran", func(r *osdOOMRun) { r.Effects.OutageFloor.Running = true }, "outage progress: floor ledger mark was taken in another run state"},
		{"outage floor taken while the map showed it up", func(r *osdOOMRun) { r.Effects.OutageFloor.MapDown = false }, "not down in the map"},
		{"outage floor of another generation", func(r *osdOOMRun) { r.Effects.OutageFloor.Generation = oomNew }, "names another generation"},
		{"outage floor without a time", func(r *osdOOMRun) { r.Effects.OutageFloor.At = time.Time{} }, "has no time"},
		{"outage floor before the window end", func(r *osdOOMRun) { r.Effects.OutageFloor.At = oomAt(5.5) }, "outage ledger floor precedes the end of the window"},
		{"outage marks unordered", func(r *osdOOMRun) { r.Effects.OutageLater.At = r.Effects.OutageFloor.At }, "ledger marks are not ordered"},
		{"outage later mark after restoration", func(r *osdOOMRun) { r.Effects.OutageLater.At = oomAt(15) }, "follows the restoration"},
		{"recovery floor precedes the restart", func(r *osdOOMRun) { r.Recovery.AckedFloor.At = oomAt(15.5) }, "recovery ledger floor precedes the observed restart"},
		{"recovery no progress after its floor", func(r *osdOOMRun) { r.Recovery.AckedLater.Acked = r.Recovery.AckedFloor.Acked }, "recovery progress: the writer was not acknowledged again"},
		{"recovery marks of the old generation", func(r *osdOOMRun) { r.Recovery.AckedFloor.Generation = oomGen }, "recovery progress: floor ledger mark names another generation"},
		{"recovery floor while stopped", func(r *osdOOMRun) { r.Recovery.AckedFloor.Running = false }, "another run state"},
		{"no explicit restart", func(r *osdOOMRun) { r.Recovery = osdOOMRecovery{} }, "no explicit restart"},
		{"same generation after the restart", func(r *osdOOMRun) { r.Recovery.New = oomGen }, "not a new generation"},
		{"another container after the restart", func(r *osdOOMRun) { r.Recovery.New = containerGeneration{ID: "osd9", StartedAt: oomNew.StartedAt} }, "not a new generation"},
		{"same pid after the restart", func(r *osdOOMRun) { r.Recovery.NewPID = oomPID }, "did not produce a new process"},
		{"same process start after the restart", func(r *osdOOMRun) { r.Recovery.NewProcess = oomProcess }, "did not produce a new process"},
		{"new generation observed before restoration", func(r *osdOOMRun) { r.Recovery.StartedAt = oomAt(13) }, "not observed after restoration"},
		{"new generation observation without a time", func(r *osdOOMRun) { r.Recovery.StartedAt = time.Time{} }, "not observed after restoration"},
		{"restarted kernel reading of the old generation", func(r *osdOOMRun) { r.Recovery.Kernel.Generation = oomGen }, "no joined kernel reading"},
		{"restarted kernel reading missing", func(r *osdOOMRun) { r.Recovery.Kernel = nil }, "no joined kernel reading"},
		{"restarted kernel reading without a time", func(r *osdOOMRun) { r.Recovery.Kernel.At = time.Time{} }, "no joined kernel reading"},
		{"restarted under the pressure ceiling", func(r *osdOOMRun) { r.Recovery.Kernel.Max = memoryLimit{Bytes: oomCeiling} }, "does not run under the restored baseline"},
		{"restarted with swap", func(r *osdOOMRun) { r.Recovery.Kernel.SwapMax = memoryLimit{Unlimited: true} }, "does not run under the restored baseline"},
		{"map shows the old boot epoch", func(r *osdOOMRun) { r.Recovery.MapAfter.UpFrom = 5 }, "newer boot epoch"},
		{"map entry still down", func(r *osdOOMRun) { r.Recovery.MapAfter.Up = false }, "newer boot epoch"},
		{"map after the restart missing", func(r *osdOOMRun) { r.Recovery.MapAfter = nil }, "newer boot epoch"},
		{"cluster not clean", func(r *osdOOMRun) { r.Recovery.Clean = false }, "cluster clean"},
		{"ledger not verified", func(r *osdOOMRun) { r.Recovery.LedgerFinal = false }, "read back intact"},
	}
	for _, c := range cases {
		r := goodOOMRun()
		c.mutate(&r)
		v, reasons := classifyOSDOOMRun(r)
		if v != osdOOMRunOOMKilledUnfinished {
			t.Errorf("%s: verdict %s, want %s", c.name, v, osdOOMRunOOMKilledUnfinished)
			continue
		}
		if !strings.Contains(strings.Join(reasons, "\n"), c.reason) {
			t.Errorf("%s: reasons %q lack %q", c.name, reasons, c.reason)
		}
	}
}

func TestOSDOOMRunOtherStepOutcomesKeepTheirOwnVerdict(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*osdOOMRun)
		want   osdOOMRunVerdict
	}{
		{"survived", func(r *osdOOMRun) {
			r.Step.Final.Events, r.Step.Exit = memoryEvents{Max: 4}, &memoryDaemonExit{Generation: oomGen, Running: true}
		}, osdOOMRunHitSurvived},
		{"not reached", func(r *osdOOMRun) {
			r.Step.Final.Events, r.Step.Exit = memoryEvents{}, &memoryDaemonExit{Generation: oomGen, Running: true}
		}, osdOOMRunNotReached},
		{"another victim", func(r *osdOOMRun) { r.Step.ProcsBefore.PIDs = []int{oomPID, 8} }, osdOOMRunOther},
		{"unsupported", func(r *osdOOMRun) { r.Step.Before.ControllerListed = false }, osdOOMRunUnsupported},
		{"unobserved final counters", func(r *osdOOMRun) { r.Step.Final = nil }, osdOOMRunInconclusive},
		{"outcome of another generation", func(r *osdOOMRun) { r.Step.Exit.Generation = oomNew }, osdOOMRunInconclusive},
	}
	for _, c := range cases {
		r := goodOOMRun()
		c.mutate(&r)
		if v, _ := classifyOSDOOMRun(r); v != c.want {
			t.Errorf("%s: %s, want %s", c.name, v, c.want)
		}
	}
}

func TestOSDOOMEligibilityRefusesBeforeDispatch(t *testing.T) {
	good := func() (*memoryWitness, memoryProcs) {
		return oomWitness(oomGen, oomAt(3), oomBaseline, 300<<20, memoryEvents{}), memoryProcs{Observed: true, PIDs: []int{oomPID}}
	}
	if w, p := good(); osdOOMEligible(w, p, oomPID, oomBaseline, oomCeiling) != nil {
		t.Fatal("eligible target refused")
	}
	cases := map[string]func(*memoryWitness, *memoryProcs){
		"controller":     func(w *memoryWitness, p *memoryProcs) { w.ControllerListed = false },
		"baseline":       func(w *memoryWitness, p *memoryProcs) { w.Max = memoryLimit{Unlimited: true} },
		"swap":           func(w *memoryWitness, p *memoryProcs) { w.SwapMax = memoryLimit{Unlimited: true} },
		"members":        func(w *memoryWitness, p *memoryProcs) { p.PIDs = []int{oomPID, 3} },
		"members unread": func(w *memoryWitness, p *memoryProcs) { *p = memoryProcs{} },
		"not the member": func(w *memoryWitness, p *memoryProcs) { p.PIDs = []int{3} },
		"usage":          func(w *memoryWitness, p *memoryProcs) { w.Current = 100 << 20 },
		"headroom":       func(w *memoryWitness, p *memoryProcs) { w.Current = oomBaseline },
	}
	for name, mutate := range cases {
		w, p := good()
		mutate(w, &p)
		if osdOOMEligible(w, p, oomPID, oomBaseline, oomCeiling) == nil {
			t.Errorf("%s: step admitted", name)
		}
	}
	if osdOOMEligible(nil, memoryProcs{}, oomPID, oomBaseline, oomCeiling) == nil {
		t.Error("missing reading admitted")
	}
}

func TestOSDOOMExecutableIsReadFromTheProcessRoot(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "7"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/usr/bin/ceph-osd", filepath.Join(root, "7", "exe")); err != nil {
		t.Fatal(err)
	}
	if exe, err := osdOOMExecutableOf(root, 7); err != nil || exe != "ceph-osd" {
		t.Fatalf("executable: %q %v", exe, err)
	}
	if _, err := osdOOMExecutableOf(root, 8); err == nil {
		t.Fatal("a missing process has an executable")
	}
}

// oomTree builds a synthetic cgroup directory and returns it with an open
// event handle.
func oomTree(t *testing.T, events, procs string) (string, *os.File) {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{"memory.max": "2147483648\n", "memory.swap.max": "0\n", "cgroup.procs": procs, "memory.events.local": events}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	handle, err := os.Open(filepath.Join(dir, "memory.events.local"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handle.Close() })
	return dir, handle
}

func TestOSDOOMRecorderKeepsLastReadableCountersAndEffectiveCeiling(t *testing.T) {
	dir, handle := oomTree(t, "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n", "7\n")
	ctx, cancel := context.WithCancel(t.Context())
	out := recordOSDOOM(ctx, dir, handle, 2<<30, 7, time.Millisecond, time.Now)
	time.Sleep(20 * time.Millisecond)
	if err := os.WriteFile(filepath.Join(dir, "memory.events.local"), []byte("low 0\nhigh 0\nmax 9\noom 1\noom_kill 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	cancel()
	obs := <-out
	if obs.EffectiveAt.IsZero() || obs.OtherProc || len(obs.Reads) < 2 {
		t.Fatalf("observation: %+v", obs)
	}
	if last := obs.Reads[len(obs.Reads)-1].Events; last != (memoryEvents{Max: 9, OOM: 1, OOMKill: 1}) {
		t.Fatalf("last reading: %+v", last)
	}
	end := obs.Reads[len(obs.Reads)-1].At.Add(time.Hour)
	if f := obs.final(end); f == nil || f.Source != osdOOMFinalLastSample {
		t.Fatalf("a read before the end is only the last in-window sample: %+v", f)
	}
	if f := obs.final(obs.Reads[0].At); f == nil || f.Source != osdOOMFinalPostStop {
		t.Fatalf("a read after the end is a post-stop reading: %+v", f)
	}
	if (osdOOMObservation{}).final(end) != nil {
		t.Fatal("no reads must not decode to zero counters")
	}
}

func TestOSDOOMRecorderNeverInventsEvidence(t *testing.T) {
	// A ceiling that never matches, a foreign member and an unreadable event
	// handle: no effective time, the foreign process is flagged and no read is
	// kept.
	dir, handle := oomTree(t, "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n", "7\n8\n")
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	out := recordOSDOOM(ctx, dir, handle, 64<<20, 7, time.Millisecond, time.Now)
	time.Sleep(15 * time.Millisecond)
	cancel()
	obs := <-out
	if !obs.EffectiveAt.IsZero() || !obs.OtherProc || len(obs.Reads) != 0 || len(obs.Errors) == 0 {
		t.Fatalf("observation: %+v", obs)
	}
	if len(obs.Errors) > 2 {
		t.Fatalf("one message per tick instead of per distinct failure: %q", obs.Errors)
	}
}

func TestOSDOOMHandleSurvivesUnlinkedCounters(t *testing.T) {
	// The path disappears while the open handle still answers. This shows the
	// recorder's use of the handle on a regular file; whether a real cgroup
	// file keeps answering after the cgroup is removed is a native question.
	dir, handle := oomTree(t, "low 0\nhigh 0\nmax 4\noom 1\noom_kill 1\n", "7\n")
	if err := os.Remove(filepath.Join(dir, "memory.events.local")); err != nil {
		t.Fatal(err)
	}
	if e, err := readOSDOOMHandle(handle); err != nil || e != (memoryEvents{Max: 4, OOM: 1, OOMKill: 1}) {
		t.Fatalf("handle read: %+v %v", e, err)
	}
}

func TestOSDMemTransitionContainerSwapRuleHasANegativeControl(t *testing.T) {
	tr := goodOOMTransition(oomBaseline, 1, true)
	tr.AfterLimits.Swap = 4 * tr.Requested
	if _, err := classifyOSDMemTransition(tr, osdMemChecks{}); err == nil || !strings.Contains(err.Error(), "container swap") {
		t.Fatalf("container swap not refused as such: %v", err)
	}
	if _, err := classifyOSDMemTransition(tr, osdMemChecks{skipSwap: true}); err != nil && strings.Contains(err.Error(), "container swap") {
		t.Fatalf("the swap rule still refuses with the rule disabled: %v", err)
	}
}

func TestStrictOrderingRequiresEveryTimeFirst(t *testing.T) {
	if !strictlyOrdered(oomAt(1), oomAt(2), oomAt(3)) {
		t.Fatal("ordered times refused")
	}
	for name, times := range map[string][]time.Time{
		"zero first":   {{}, oomAt(1)},
		"zero middle":  {oomAt(1), {}, oomAt(3)},
		"zero last":    {oomAt(1), {}},
		"equal":        {oomAt(1), oomAt(1)},
		"out of order": {oomAt(2), oomAt(1)},
		"single zero":  {{}},
	} {
		if strictlyOrdered(times...) {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestOSDOOMViewSummaryKeepsErrorsAndMissingParts(t *testing.T) {
	v := osdSlowLiveView{At: oomAt(1), Errs: []string{"osd dump: boom"}}
	got := osdOOMViewSummary(v)
	if !strings.Contains(got, "errors=osd dump: boom") || strings.Contains(got, "up=") || strings.Contains(got, "pg=") {
		t.Fatalf("unreadable parts decoded as healthy: %q", got)
	}
}

func TestOSDMemLimitsFromInspectRefusesMissingHostConfig(t *testing.T) {
	if l, err := osdMemLimitsFromInspect(container.InspectResponse{}); err == nil {
		t.Fatalf("a missing host configuration decoded as %+v", l)
	}
	c := container.InspectResponse{HostConfig: &container.HostConfig{}}
	c.HostConfig.Memory, c.HostConfig.MemorySwap, c.HostConfig.RestartPolicy.Name = oomBaseline, oomBaseline, container.RestartPolicyDisabled
	if l, err := osdMemLimitsFromInspect(c); err != nil || l != (osdMemLimits{Memory: oomBaseline, Swap: oomBaseline, RestartPolicy: "no"}) {
		t.Fatalf("limits: %+v %v", l, err)
	}
}

func TestOSDOOMReceiptsAreSerializableAndPathFree(t *testing.T) {
	r := goodOOMRun()
	for name, v := range map[string]any{
		"step": osdOOMReceiptStep(r.Step), "admit": osdOOMReceiptTransition(r.Admit), "restore": osdOOMReceiptTransition(r.Restore),
	} {
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if strings.Contains(string(data), "/") {
			t.Errorf("%s receipt carries a path: %s", name, data)
		}
	}
	step := osdOOMReceiptStep(osdOOMStep{})
	if step["effective_observed"] != false {
		t.Fatalf("an empty step claims an observed ceiling: %v", step)
	}
}

func TestOSDOOMFixtureLimitsMatchThePredeclaredScenario(t *testing.T) {
	if osdOOMBaseline != oomBaseline || osdOOMCeiling != oomCeiling || osdOOMCeiling >= osdOOMBaseline {
		t.Fatalf("fixture limits %d/%d differ from the scenario's %d/%d", oomBaseline, oomCeiling, osdOOMBaseline, osdOOMCeiling)
	}
}
