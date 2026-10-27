//go:build integration && ci_optional && ci_batch_osd_memory_oom

//ci: timeout=30m job-timeout=40 image-roles=essential,client

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

const osdOOMPool = "tc-oom"

// TestOSDMemoryOOMStep is the first OSD memory-OOM step, separate from the CPU
// scenario and run on its own fresh disposable cluster. One owned OSD, the
// acting primary of a single-PG replicated pool, is moved by one explicit
// update from its unlimited creation state to a finite memory ceiling with no
// container swap, observed there, and then given ONE predeclared lower
// ceiling that its resident memory exceeds. There is no search for the
// smallest limit that kills something and no second pressure step.
//
// The observer, the event handle and the client stay outside the OSD's
// cgroup: no allocator, debug command or stats exec runs in it. The result of
// the step is judged by classifyOSDOOMStep and kept as its own verdict: an OSD
// killed by the cgroup OOM killer is reported as osd-oom-killed and is never
// presented as a survivable pressure scenario, and a surviving OSD, an
// unreached limit, another or unknown victim and every shortfall of evidence
// are reported as such. Only an osd-oom-killed run that also shows the engine
// not restarting the daemon by itself, the OSD down in the map, the PG leaving
// active+clean, a live writer acknowledged again while the OSD is down, the
// restored configuration, an explicit restart into a new process generation
// under the restored limit, the OSD rejoining and every acknowledged object
// read back passes.
//
// The pressure update has an uncertain outcome by nature (it can kill the
// daemon during the call); its reply is recorded, the same generation is
// inspected afterwards and the request is never replayed. The receipt is
// emitted before the cluster is destroyed on every outcome. This is the OSD
// step only. It does not cover a MON, memory.high, other ceilings, supported
// defaults, a public API, other engines or stable behavior across runs.
func TestOSDMemoryOOMStep(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("eligibility: Linux local cgroup-v2 observer required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 27*time.Minute)
	defer cancel()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	if !strings.HasPrefix(docker.DaemonHost(), "unix://") {
		t.Skip("eligibility: a local Unix Docker endpoint is required")
	}
	info, err := docker.Info(ctx, mobycl.InfoOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, option := range info.Info.SecurityOptions {
		if strings.Contains(option, "rootless") {
			t.Skip("eligibility: rootful local Docker engine required")
		}
	}
	if info.Info.CgroupVersion != "2" || !info.Info.MemoryLimit || !info.Info.SwapLimit {
		t.Skip("eligibility: cgroup v2 memory limit and swap limit support required")
	}
	// The experiment puts no pressure on the host, only on the one cgroup.
	// Refuse when the runner cannot reserve the finite baseline of the victim.
	var available uint64
	if mem, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(mem), "\n") {
			if strings.HasPrefix(line, "MemAvailable:") {
				_, _ = fmt.Sscanf(line, "MemAvailable: %d kB", &available)
			}
		}
	}
	if available < 6<<20 {
		t.Skip("eligibility: less than 6 GiB available runner memory")
	}
	t.Logf("OSD_OOM_CONTROLLER euid=%d engine=%s arch=%s ncpu=%d", os.Geteuid(), info.Info.ServerVersion, runtime.GOARCH, info.Info.NCPU)

	image, opts := integrationImages(t)
	cluster, client := newServiceClusterWithOptions(t, image, append(opts, ceph.WithNoInitialOSDs())...)
	if _, err := cluster.OverrideConfig(ctx, ceph.ConfigSetting{Section: "osd", Name: "osd_mclock_skip_benchmark", Value: "true"}); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := cluster.AddOSD(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: osdOOMPool, PGNum: 1, Replicas: 3, MinSize: 2, Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	pool, err := cluster.Pool(ctx, osdOOMPool)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := pool.PGs(ctx)
	if err != nil || len(snapshot.PGs) != 1 || snapshot.PGs[0].State != "active+clean" || len(snapshot.PGs[0].Acting) != 3 {
		t.Fatalf("placement before any mutation: %+v: %v", snapshot, err)
	}
	pg := snapshot.PGs[0]
	var osd *ceph.OSDContainer
	for _, candidate := range clusteraccess.OSDs(t, cluster) {
		if candidate.ID == pg.ActingPrimary {
			osd = candidate
		}
	}
	if osd == nil {
		t.Fatalf("acting primary osd.%d is not an owned OSD", pg.ActingPrimary)
	}

	id := osd.GetContainerID()
	rawInspect := func(call context.Context) (container.InspectResponse, error) {
		c, done := context.WithTimeout(call, 10*time.Second)
		defer done()
		r, err := docker.ContainerInspect(c, id, mobycl.ContainerInspectOptions{})
		return r.Container, err
	}
	inspect := func() (cpuEngineState, error) {
		c, err := rawInspect(ctx)
		if err != nil {
			return cpuEngineState{}, err
		}
		return cpuEngineStateFromInspect(c, cpuEngineChecks{})
	}
	state, err := inspect()
	if err != nil {
		t.Fatalf("engine inspection before binding: %v", err)
	}
	gen := containerGeneration{ID: id, StartedAt: state.StartedAt}
	roots := cpuFSRoots{Proc: "/proc", Cgroup: "/sys/fs/cgroup"}
	target, err := bindCPUWitnessTarget(roots, gen, state, cpuBindChecks{})
	if err != nil {
		t.Skipf("eligibility incomplete before any mutation: %v", err)
	}
	t.Log(target.receipt())
	process, err := osdOOMProcessOf(roots.Proc, target.PID)
	if err != nil {
		t.Skipf("unsupported before any mutation, no OOM was attempted: %v", err)
	}
	if err := osdOOMProcessIsOSD(process, osd.ID); err != nil {
		t.Fatalf("the frozen process is not the selected OSD; nothing was dispatched: %v", err)
	}
	creation, creationProcs, err := readMemoryScratchWitness(target, inspect, time.Now)
	if err != nil {
		t.Fatalf("creation-state witness: %v", err)
	}
	if !creation.ControllerListed {
		t.Skip("eligibility: the memory controller is not listed on the target cgroup; recorded as unsupported")
	}
	t.Logf("OSD_OOM_TARGET osd=%d pg=%s acting=%v comm=%s exe_state=%s creation_memory_max_unlimited=%t members=%v", osd.ID, pg.PGID, pg.Acting, process.Comm, process.ExeState, creation.Max.Unlimited, creationProcs.PIDs)

	// The receipt is registered first so that it is emitted after the
	// restoration and writer cleanups and before the cluster is destroyed.
	run := osdOOMRun{Baseline: osdOOMBaseline, Ceiling: osdOOMCeiling, Identity: osdOOMIdentity{Generation: gen, PID: target.PID, ProcessStart: target.ProcessStart}}
	details := map[string]any{
		"osd": osd.ID, "pg": pg.PGID.String(), "acting": pg.Acting, "container": id, "started_at": gen.StartedAt,
		"pid": target.PID, "process_start": target.ProcessStart, "comm": process.Comm, "exe_state": process.ExeState, "engine": info.Info.ServerVersion,
		"baseline": osdOOMBaseline, "ceiling": osdOOMCeiling,
	}
	var viewSummaries []string
	var recorderErrors []string
	t.Cleanup(func() {
		verdict, reasons := classifyOSDOOMRun(run)
		details["verdict"], details["reasons"] = verdict, reasons
		details["transitions"] = map[string]any{"admit": osdOOMReceiptTransition(run.Admit), "pressure": osdOOMReceiptTransition(run.Pressure), "restore": osdOOMReceiptTransition(run.Restore)}
		details["step"], details["recorder_errors"] = osdOOMReceiptStep(run.Step), recorderErrors
		details["outage_views"], details["hold_samples"] = viewSummaries, len(run.Hold)
		details["recovery"] = map[string]any{"started": run.Recovery.Started, "new_started_at": run.Recovery.New.StartedAt, "new_pid": run.Recovery.NewPID,
			"clean": run.Recovery.Clean, "acked_floor": run.Recovery.AckedFloor.Acked, "acked_later": run.Recovery.AckedLater.Acked, "ledger_final": run.Recovery.LedgerFinal}
		data, err := json.Marshal(details)
		if err != nil {
			t.Errorf("receipt: %v", err)
			return
		}
		t.Logf("OSD_OOM_RECEIPT %s", data)
	})

	// update dispatches one request and records the transition from the reply,
	// a fresh inspection and, while the task runs, the kernel reading of the
	// same generation. It never retries. A running update is refused before
	// dispatch unless the target is still the running captured generation; an
	// update of the stopped container is refused unless it is still the
	// captured generation, stopped and not restarted.
	dispatchedSteps := map[string]bool{}
	update := func(limit int64, stopped bool) (osdMemTransition, bool, error) {
		var updateErr error
		var replyWarnings []string
		dispatched := false
		send := func() ([]string, error) {
			dispatched = true
			c, done := context.WithTimeout(ctx, 20*time.Second)
			defer done()
			result, err := docker.ContainerUpdate(c, id, mobycl.ContainerUpdateOptions{Resources: &container.Resources{Memory: limit, MemorySwap: limit}})
			updateErr, replyWarnings = err, result.Warnings
			return result.Warnings, err
		}
		var guardErr error
		if stopped {
			current, err := rawInspect(ctx)
			s, serr := cpuEngineStateFromInspect(current, cpuEngineChecks{})
			switch {
			case err != nil || serr != nil:
				guardErr = fmt.Errorf("stale: inspection failed: %v %v", err, serr)
			case s.ID != gen.ID || s.StartedAt != gen.StartedAt || s.Running || s.Restarting || s.RestartCount != 0:
				guardErr = errors.New("stale: the target is not the captured, stopped, unrestarted generation")
			default:
				_, _ = send()
			}
		} else {
			_, guardErr = guardedCPUUpdate(gen, func() (container.InspectResponse, error) { return rawInspect(ctx) }, send)
		}
		if !dispatched {
			return osdMemTransition{}, false, guardErr
		}
		at := time.Now()
		received, warnings := cpuUpdateReply(replyWarnings, updateErr)
		after, err := rawInspect(ctx)
		if err != nil {
			return osdMemTransition{}, true, err
		}
		afterState, err := cpuEngineStateFromInspect(after, cpuEngineChecks{})
		if err != nil {
			return osdMemTransition{}, true, err
		}
		limits, err := osdMemLimitsFromInspect(after)
		if err != nil {
			return osdMemTransition{}, true, err
		}
		var kernel *memoryWitness
		if afterState.Running {
			if w, _, kerr := readMemoryScratchWitness(target, inspect, time.Now); kerr != nil {
				t.Logf("OSD_OOM_KERNEL_UNAVAILABLE requested=%d error=%v", limit, kerr)
			} else {
				kernel = w
			}
		}
		t.Logf("OSD_OOM_UPDATE requested=%d stopped_container=%t reply_received=%t update_error=%v warnings=%d", limit, stopped, received, updateErr, len(warnings))
		return osdMemTransition{Generation: gen, Requested: limit, At: at, ReplyReceived: received, Warnings: warnings, AfterState: afterState, AfterLimits: limits, Kernel: kernel}, true, nil
	}
	// Cleanup restoration of the admitted finite baseline happens only when the
	// pressure request was dispatched and no restoration was: the captured
	// generation is restored where it is, a replaced generation never receives
	// the old restoration, and an uncertain outcome is inspected, not replayed.
	t.Cleanup(func() {
		if !dispatchedSteps["pressure"] || dispatchedSteps["restore"] {
			return
		}
		call, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		current, err := docker.ContainerInspect(call, id, mobycl.ContainerInspectOptions{})
		if err != nil || current.Container.State == nil || current.Container.State.StartedAt != gen.StartedAt || current.Container.RestartCount != 0 {
			t.Error("memory restoration incomplete: the captured generation is unavailable or replaced", err)
			return
		}
		if _, err := docker.ContainerUpdate(call, id, mobycl.ContainerUpdateOptions{Resources: &container.Resources{Memory: osdOOMBaseline, MemorySwap: osdOOMBaseline}}); err != nil {
			t.Error("memory restoration independently failed", err)
		}
	})

	observe := func() osdSlowLiveView {
		var v osdSlowLiveView
		c, done := context.WithTimeout(ctx, 30*time.Second)
		defer done()
		if st, err := osd.State(c); err != nil || st == nil {
			v.Errs = append(v.Errs, "state unreadable")
		} else {
			v.Instance = &osdInstanceView{Running: st.Running, OOMKilled: st.OOMKilled, PID: st.Pid, StartedAt: st.StartedAt}
		}
		if raw, err := cluster.Ceph(c, "osd", "dump", "--format", "json"); err != nil {
			v.Errs = append(v.Errs, "osd dump: "+err.Error())
		} else if m, err := parseOSDMapEntry(raw, osd.ID); err != nil {
			v.Errs = append(v.Errs, err.Error())
		} else {
			v.Map = &m
		}
		if snap, err := pool.PGs(c); err != nil || len(snap.PGs) != 1 {
			v.Errs = append(v.Errs, "pg state unreadable")
		} else {
			v.PGState, v.ActingPrimary, v.PGRead = snap.PGs[0].State, snap.PGs[0].ActingPrimary, true
		}
		if h, err := cluster.HealthDetails(c); err != nil {
			v.Errs = append(v.Errs, "health unreadable")
		} else {
			v.HealthCodes, v.HealthRead = []string{}, true
			for code := range h.Checks {
				v.HealthCodes = append(v.HealthCodes, code)
			}
			slices.Sort(v.HealthCodes)
		}
		v.At = time.Now()
		return v
	}

	// The live writer keeps one connection and one ledger for the whole run.
	const ledger, stopFile = "/tmp/tc-oom-step.jsonl", "/tmp/tc-oom-step.stop"
	actor := startNative(ctx, cluster, client, secondFaultCommand("oom actor", 25*time.Minute, "actor", osdOOMPool, ledger, stopFile, "1", "900"))
	actorDone := false
	t.Cleanup(func() {
		if actorDone {
			return
		}
		actor.cancel()
		select {
		case outcome := <-actor.done:
			var unstopped *ceph.NativeCommandUnstoppedError
			if errors.As(outcome.err, &unstopped) {
				stopCtx, stop := context.WithTimeout(context.Background(), time.Minute)
				defer stop()
				if err := unstopped.Stop(stopCtx); err != nil {
					t.Errorf("stop the live writer: %v", err)
				}
			}
		case <-time.After(time.Minute):
			t.Error("the live writer did not stop")
		}
	})
	// ackedWait polls the ledger without failing the run: a writer that makes
	// no progress is a recorded outcome, not a reason to lose the receipt.
	ackedWait := func(since secondFaultMark, n int, limit time.Duration) (secondFaultMark, bool) {
		deadline := time.Now().Add(limit)
		for {
			mark, _ := secondFaultMarkNow(t, ctx, cluster, client, ledger)
			if secondFaultAdvanced(since, mark, n) {
				return mark, true
			}
			if time.Now().After(deadline) {
				return mark, false
			}
			time.Sleep(time.Second)
		}
	}
	// ledgerMark joins the cumulative acknowledged count to the engine's state
	// of the target before and after the ledger read, and to the map.
	ledgerMark := func() osdOOMLedgerMark {
		var m osdOOMLedgerMark
		first, _ := rawInspect(ctx)
		mark, _ := secondFaultMarkNow(t, ctx, cluster, client, ledger)
		last, err := rawInspect(ctx)
		if err != nil || last.State == nil || first.State == nil {
			return m
		}
		v := observe()
		m.Acked, m.At = mark.Acked, time.Now()
		m.Generation = containerGeneration{ID: last.ID, StartedAt: last.State.StartedAt}
		m.Running = first.State.Running || last.State.Running
		if first.State.StartedAt != last.State.StartedAt || first.State.Running != last.State.Running {
			m.Generation = containerGeneration{}
		}
		m.MapDown = v.Map != nil && !v.Map.Up
		return m
	}
	healthy := secondFaultAcks(t, ctx, cluster, client, ledger, secondFaultMark{}, 3, "while healthy")

	// Admission: one explicit update from the unlimited creation state to the
	// finite baseline with no container swap. A failed or uncertain admission
	// stops the run here, before any pressure.
	record, dispatched, err := update(osdOOMBaseline, false)
	if dispatched {
		run.Admit = record
		dispatchedSteps["admission"] = true
	}
	if err != nil {
		t.Fatalf("admission: dispatched=%t, record not usable: %v", dispatched, err)
	}
	if v, verr := classifyOSDMemTransition(run.Admit, osdMemChecks{}); v != osdMemApplied {
		t.Fatalf("admission is %s, not a clean application: %v", v, verr)
	}
	mapBefore := observe()
	if mapBefore.Map == nil || !mapBefore.Map.Up {
		t.Fatalf("the OSD is not observed up before the step: %+v", mapBefore)
	}
	run.Effects.MapBefore = mapBefore.Map
	viewSummaries = append(viewSummaries, "before: "+osdOOMViewSummary(mapBefore))

	// Eligibility is judged on a fresh reading after the baseline is installed
	// and before anything is dispatched. A refusal here changes nothing.
	before, procs, err := readMemoryScratchWitness(target, inspect, time.Now)
	if err != nil {
		t.Fatalf("pre-step witness: %v", err)
	}
	details["pre_step"] = map[string]any{"current": before.Current, "events": before.Events, "members": procs.PIDs}
	if err := osdOOMEligible(before, procs, target.PID, osdOOMBaseline, osdOOMCeiling); err != nil {
		t.Fatalf("the pressure step is not eligible; nothing was dispatched: %v", err)
	}
	run.Step = osdOOMStep{
		Generation: gen, PID: target.PID, ProcessStart: target.ProcessStart, OSD: osd.ID, Process: process, ProcsBefore: procs,
		Baseline: memoryLimit{Bytes: uint64(osdOOMBaseline)}, Ceiling: memoryLimit{Bytes: uint64(osdOOMCeiling)}, Before: before,
	}
	run.Effects.PreStep = ledgerMark()

	// The event handle is opened outside the victim before the step so that the
	// counters can still be read if the cgroup path is removed with the daemon.
	handle, err := os.Open(filepath.Join(target.group, "memory.events.local"))
	if err != nil {
		t.Fatalf("target-local memory events unavailable; nothing was dispatched: %v", err)
	}
	t.Cleanup(func() { _ = handle.Close() })
	recording, stopRecording := context.WithCancel(ctx)
	defer stopRecording()
	observed := recordOSDOOM(recording, target.group, handle, uint64(osdOOMCeiling), target.PID, 2*time.Millisecond, time.Now)

	// The one pressure step.
	run.Step.ExecStart = time.Now()
	pressure, dispatched, perr := update(osdOOMCeiling, false)
	if dispatched {
		run.Pressure = pressure
		dispatchedSteps["pressure"] = true
	}
	if perr != nil {
		t.Logf("OSD_OOM_PRESSURE_RECORD dispatched=%t error=%v", dispatched, perr)
	}
	var exit *memoryDaemonExit
	for deadline := time.Now().Add(30 * time.Second); ; {
		c, err := rawInspect(ctx)
		if err == nil {
			if e, eerr := memoryExitFromInspect(c); eerr == nil {
				exit = e
				if !e.Running || e.Generation != gen {
					break
				}
			}
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	run.Step.ExecEnd = time.Now()
	time.Sleep(300 * time.Millisecond) // post-stop reads through the open handle
	stopRecording()
	obs := <-observed
	recorderErrors = obs.Errors
	run.Step.EffectiveAt, run.Step.OtherProcSeen = obs.EffectiveAt, obs.OtherProc
	run.Step.Final = obs.final(run.Step.ExecEnd)
	run.Step.Exit = exit
	stepVerdict, stepErr := classifyOSDOOMStep(run.Step, osdOOMStepChecks{})
	t.Logf("OSD_OOM_STEP verdict=%s error=%v reads=%d final=%+v exit=%+v", stepVerdict, stepErr, len(obs.Reads), run.Step.Final, run.Step.Exit)
	if stepVerdict != osdOOMKilled {
		t.Fatalf("the step is not an OSD OOM kill: %s: %v (the admitted baseline is restored by cleanup)", stepVerdict, stepErr)
	}

	// The engine must leave the daemon stopped on its own, and the cluster and
	// the client are observed while it is down. This is the outage window.
	down, affected := false, false
	for deadline := time.Now().Add(2 * time.Minute); time.Now().Before(deadline); time.Sleep(time.Second) {
		if c, err := rawInspect(ctx); err == nil && c.State != nil {
			run.Hold = append(run.Hold, osdOOMHoldSample{At: time.Now(), ID: c.ID, StartedAt: c.State.StartedAt,
				Running: c.State.Running, Restarting: c.State.Restarting, RestartCount: c.RestartCount})
		}
		if len(run.Hold)%2 == 1 {
			v := observe()
			run.Effects.Views = append(run.Effects.Views, v)
			viewSummaries = append(viewSummaries, "outage: "+osdOOMViewSummary(v))
			down = down || (v.Map != nil && !v.Map.Up)
			affected = affected || (v.PGRead && v.PGState != "active+clean")
		}
		if n := len(run.Hold); n >= 3 && run.Hold[n-1].At.Sub(run.Hold[0].At) >= osdOOMHoldMin && down && affected {
			break
		}
	}
	// A fresh ledger floor is taken only now, after the target is established
	// stopped and down. Acknowledgements made before the kill, during the
	// update or during the death poll are below it and prove nothing.
	run.Effects.OutageFloor = ledgerMark()
	_, _ = ackedWait(secondFaultMark{Acked: run.Effects.OutageFloor.Acked}, 3, 2*time.Minute)
	run.Effects.OutageLater = ledgerMark()
	t.Logf("OSD_OOM_OUTAGE down=%t pg_affected=%t pre_step=%d floor=%d later=%d healthy=%+v", down, affected, run.Effects.PreStep.Acked, run.Effects.OutageFloor.Acked, run.Effects.OutageLater.Acked, healthy)

	// Restore the admitted finite baseline BEFORE the explicit start. On a
	// stopped container this is saved configuration, not an effective limit.
	restore, dispatched, rerr := update(osdOOMBaseline, true)
	if dispatched {
		run.Restore = restore
		dispatchedSteps["restore"] = true
	}
	if rerr != nil {
		t.Fatalf("restoration before the restart: dispatched=%t: %v", dispatched, rerr)
	}
	startErr := osd.Start(ctx)
	run.Recovery.Started = startErr == nil
	if startErr != nil {
		t.Fatalf("explicit restart: %v", startErr)
	}
	var newState cpuEngineState
	for deadline := time.Now().Add(90 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		s, err := inspect()
		if err == nil && s.Running && s.StartedAt != gen.StartedAt {
			newState = s
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("a new daemon generation was not observed: %v", err)
		}
	}
	run.Recovery.StartedAt = time.Now()
	run.Recovery.New = containerGeneration{ID: newState.ID, StartedAt: newState.StartedAt}
	run.Recovery.NewPID = newState.PID
	if recovered, err := bindCPUWitnessTarget(roots, run.Recovery.New, newState, cpuBindChecks{}); err != nil {
		t.Errorf("new generation could not be bound: %v", err)
	} else {
		run.Recovery.NewProcess = recovered.ProcessStart
		recoveredInspect := func() (cpuEngineState, error) { return inspect() }
		if w, _, err := readMemoryScratchWitness(recovered, recoveredInspect, time.Now); err != nil {
			t.Errorf("new generation kernel reading: %v", err)
		} else {
			run.Recovery.Kernel = w
		}
	}
	for deadline := time.Now().Add(2 * time.Minute); ; time.Sleep(time.Second) {
		v := observe()
		if v.Map != nil && v.Map.Up && v.Map.UpFrom > run.Effects.MapBefore.UpFrom {
			run.Recovery.MapAfter = v.Map
			viewSummaries = append(viewSummaries, "rejoined: "+osdOOMViewSummary(v))
			break
		}
		if time.Now().After(deadline) {
			t.Logf("OSD_OOM_REJOIN not observed: %s", osdOOMViewSummary(v))
			break
		}
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Logf("cluster did not return to clean: %v", err)
	} else {
		run.Recovery.Clean = true
	}
	run.Recovery.AckedFloor = ledgerMark()
	_, _ = ackedWait(secondFaultMark{Acked: run.Recovery.AckedFloor.Acked}, 3, 2*time.Minute)
	run.Recovery.AckedLater = ledgerMark()

	// Close the writer and the data oracle: every acknowledged object must read
	// back byte for byte from an independent client, and unknown outcomes are
	// inspected, never replayed.
	execCommand(t, ctx, client, "touch", stopFile)
	select {
	case outcome := <-actor.done:
		actorDone = true
		if outcome.err != nil || outcome.result.ExitCode != 0 || !outcome.result.Complete() {
			t.Fatalf("live writer shutdown: %v exit %d: %.1024s", outcome.err, outcome.result.ExitCode, outcome.result.Stderr)
		}
	case <-time.After(2 * time.Minute):
		t.Fatal("the live writer did not stop after its stop file appeared")
	}
	ops, err := secondFaultOps(secondFaultLedger(t, ctx, cluster, client, ledger), true)
	if err != nil {
		t.Fatal("writer ledger refused:", err)
	}
	final := secondFaultCount(ops)
	names := make([]string, len(ops))
	for i, op := range ops {
		names[i] = op.Name
	}
	encodedNames, err := json.Marshal(names)
	if err != nil {
		t.Fatal(err)
	}
	read, err := cluster.RunNativeCommand(ctx, toolkitContainer(t, cluster), secondFaultCommand("oom independent read", 3*time.Minute, "read", osdOOMPool, string(encodedNames)))
	if err != nil || read.ExitCode != 0 || !read.Complete() {
		t.Fatalf("independent read of the writer's objects: exit %d: %v: %.2048s", read.ExitCode, err, read.Stderr)
	}
	var objects map[string]secondFaultObject
	if err := json.Unmarshal(read.Stdout, &objects); err != nil {
		t.Fatalf("independent read %.512s: %v", read.Stdout, err)
	}
	verifyErr := secondFaultVerify(ops, objects)
	run.Recovery.LedgerFinal = verifyErr == nil && final.Acked >= run.Recovery.AckedLater.Acked && final.Acked > 0 && final.Refused == 0
	details["ledger"] = map[string]any{"healthy": healthy, "outage_floor": run.Effects.OutageFloor.Acked, "outage_later": run.Effects.OutageLater.Acked, "final": final}
	if verifyErr != nil {
		t.Errorf("data oracle: %v", verifyErr)
	}

	verdict, reasons := classifyOSDOOMRun(run)
	t.Logf("OSD_OOM_VERDICT verdict=%s reasons=%v", verdict, reasons)
	if verdict != osdOOMRunOOMKilled {
		t.Fatalf("not an established OSD OOM kill with recovery: %s: %v", verdict, reasons)
	}
	t.Log("OSD_OOM established for one OSD, one engine and one run: after one finite ceiling was admitted, one lower ceiling killed the frozen OSD process once by the cgroup OOM killer, the engine left it stopped, the OSD was marked down and the PG left active+clean, the writer was acknowledged again, the restored configuration and an explicit restart gave a new process generation that rejoined under the restored limit and every acknowledged object read back")
}
