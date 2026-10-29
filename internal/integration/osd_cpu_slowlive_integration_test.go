//go:build integration && ci_optional && ci_batch_osd_cpu_slowlive

//ci: timeout=30m job-timeout=40 image-roles=essential,client

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

const (
	// Provisional scratch values, not supported Ceph defaults. The baseline is
	// established by one explicit update from the unlimited creation state and
	// is the limit that is restored; the unlimited state itself is never
	// restored. The workload is predeclared: eight writers overwrite 32 fixed
	// 64 KiB names each for ten seconds, one attempt per operation.
	osdCPUBaseline int64 = 2_000_000_000
	osdCPUTight    int64 = 250_000_000
	osdCPUPool           = "tc-cpu"
)

var osdCPUWorkload = []string{"10", "8", "65536"} // seconds, writers, object bytes

// TestOSDCPUPressureSlowLive is the first native Ceph-daemon CPU-pressure
// step: one owned OSD, the primary of a single-PG replicated pool, is moved to
// a finite CPU ceiling, tightened with the merged update judge while a bounded
// client workload runs, and restored. Each of three windows (baseline,
// pressure, restored) is bracketed by the target cgroup's own cpu.stat
// throttling counters and joined to the frozen container/process generation,
// the OSD map, the PG state and the workload's own progress. A live writer
// keeps a ledger for the whole run and every acknowledged object is read back
// by an independent client afterwards.
//
// If the OSD exits or is marked down the run is reported as such and is not a
// slow-live success; failed and inconclusive outcomes keep their receipt, and
// no mutation is replayed. The scenario sets up the first CPU step only. It
// does not cover OOM, memory pressure, MON attribution, other roles, recovery
// under pressure, resource defaults, a public API or other engines.
func TestOSDCPUPressureSlowLive(t *testing.T) {
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
	if info.Info.CgroupVersion != "2" || !info.Info.CPUCfsQuota || info.Info.NCPU < 4 {
		t.Skip("eligibility: cgroup v2 CFS quota support and at least four CPUs required")
	}
	t.Logf("OSD_CPU_CONTROLLER euid=%d engine=%s arch=%s ncpu=%d", os.Geteuid(), info.Info.ServerVersion, runtime.GOARCH, info.Info.NCPU)

	image, opts := integrationImages(t)
	cluster, client := newServiceClusterWithOptions(t, image, opts.WithCustomizers(ceph.WithNoInitialOSDs()))
	if _, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "osd", Name: "osd_mclock_skip_benchmark", Value: "true"}); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := cluster.AddOSD(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: osdCPUPool, PGNum: 1, Replicas: 3, MinSize: 2, Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	pool, err := cluster.Pool(ctx, osdCPUPool)
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
	target, err := bindCPUWitnessTarget(cpuFSRoots{Proc: "/proc", Cgroup: "/sys/fs/cgroup"}, gen, state, cpuBindChecks{})
	if err != nil {
		t.Skipf("eligibility incomplete before any mutation: %v", err)
	}
	t.Log(target.receipt())
	unlimited, err := target.read(inspect, time.Now)
	if err != nil {
		t.Fatalf("creation-state witness: %v", err)
	}
	if !unlimited.ControllerListed {
		t.Skip("eligibility: the cpu controller is not listed on the target cgroup; recorded as unsupported")
	}
	t.Logf("OSD_CPU_TARGET osd=%d pg=%s acting=%v creation_cpu_max_unlimited=%t", osd.ID, pg.PGID, pg.Acting, unlimited.Max.Unlimited)

	// The receipt is registered first so that it is emitted after the
	// restoration cleanup and before the cluster is destroyed.
	run := osdCPURun{Baseline: osdCPUBaseline, Tight: osdCPUTight, Identity: osdCPUIdentity{Generation: gen, PID: target.PID}}
	details := map[string]any{
		"osd": osd.ID, "pg": pg.PGID.String(), "acting": pg.Acting, "container": id, "started_at": gen.StartedAt,
		"pid": target.PID, "process_start": target.ProcessStart, "engine": info.Info.ServerVersion,
		"workload": map[string]any{"seconds": osdCPUWorkload[0], "writers": osdCPUWorkload[1], "object_bytes": osdCPUWorkload[2]},
	}
	var windowReceipts []map[string]any
	t.Cleanup(func() {
		verdict, reasons := classifyOSDCPURun(run)
		details["verdict"], details["reasons"], details["windows"] = verdict, reasons, windowReceipts
		details["transitions"] = map[string]any{"admit": transitionReceipt(run.Admit), "tighten": transitionReceipt(run.Tighten), "restore": transitionReceipt(run.Restore)}
		data, err := json.Marshal(details)
		if err != nil {
			t.Errorf("receipt: %v", err)
			return
		}
		t.Logf("OSD_CPU_SLOWLIVE_RECEIPT %s", data)
	})

	// update dispatches one request after the stale-generation guard and
	// records the transition from the reply, a fresh inspection and the kernel
	// reading of the same generation. It never retries.
	update := func(nano int64) (cpuTransitionRecord, bool, error) {
		var updateErr error
		var replyWarnings []string
		dispatched := false
		_, guardErr := guardedCPUUpdate(gen, func() (container.InspectResponse, error) { return rawInspect(ctx) }, func() ([]string, error) {
			dispatched = true
			c, done := context.WithTimeout(ctx, 20*time.Second)
			defer done()
			result, err := docker.ContainerUpdate(c, id, mobycl.ContainerUpdateOptions{Resources: &container.Resources{NanoCPUs: nano}})
			updateErr, replyWarnings = err, result.Warnings
			return result.Warnings, err
		})
		if !dispatched {
			return cpuTransitionRecord{}, false, guardErr
		}
		received, warnings := cpuUpdateReply(replyWarnings, updateErr)
		after, err := rawInspect(ctx)
		if err != nil {
			return cpuTransitionRecord{}, true, err
		}
		kernel, kernelErr := target.read(inspect, time.Now)
		if kernelErr != nil {
			t.Logf("OSD_CPU_KERNEL_UNAVAILABLE requested=%d error=%v", nano, kernelErr)
			kernel = nil
		}
		t.Logf("OSD_CPU_UPDATE requested=%d reply_received=%t update_error=%v warnings=%d", nano, received, updateErr, len(warnings))
		record, err := assembleCPUTransition(gen, nano, received, warnings, after, kernel)
		return record, true, err
	}
	dispatchedSteps := map[string]bool{}
	applied := func(step string, nano int64, dst *cpuTransitionRecord) cpuTransitionRecord {
		t.Helper()
		record, dispatched, err := update(nano)
		if dispatched {
			*dst = record
			dispatchedSteps[step] = true
		}
		if err != nil {
			t.Fatalf("%s: dispatched=%t, record not usable: %v", step, dispatched, err)
		}
		if v, verr := classifyCPUTransition(record, cpuTransitionChecks{}); v != cpuTransitionApplied {
			t.Fatalf("%s is %s, not a clean application: %v", step, v, verr)
		}
		return record
	}

	// Restoration of the admitted limit happens only when a tighten request
	// was dispatched and no restore request was: a restore whose outcome is
	// uncertain is inspected, never replayed. This is cleanup, not evidence.
	t.Cleanup(func() {
		if !dispatchedSteps["tighten"] || dispatchedSteps["restore"] {
			return
		}
		call, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		_, err := guardedCPUUpdate(gen, func() (container.InspectResponse, error) { return rawInspect(call) }, func() ([]string, error) {
			result, err := docker.ContainerUpdate(call, id, mobycl.ContainerUpdateOptions{Resources: &container.Resources{NanoCPUs: osdCPUBaseline}})
			return result.Warnings, err
		})
		if err != nil {
			t.Error("CPU restoration incomplete or independently failed", err)
		}
	})

	// observe reads the target and the cluster around it. Each part that
	// cannot be read stays absent and keeps its error.
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
	failureLine := func() string {
		c, done := context.WithTimeout(ctx, 30*time.Second)
		defer done()
		logs, err := cluster.Ceph(c, "log", "last", "300", "info", "cluster")
		if err != nil {
			return "unreadable: " + err.Error()
		}
		if f, ok := parseOSDFailure(string(logs), osd.ID); ok {
			return f.Line
		}
		return ""
	}

	// window runs the predeclared workload between two kernel readings and
	// samples the target while it runs.
	window := func(label string, expected cpuMax, ref *osdCPUWindowRef) (osdCPUWindowVerdict, error) {
		t.Helper()
		w := osdCPUWindow{Label: label, Generation: gen, PID: target.PID, Expected: expected}
		failureBefore := failureLine()
		if b, err := target.read(inspect, time.Now); err != nil {
			w.BeforeErr = err.Error()
		} else {
			w.Before = b
		}
		baseline := observe()
		w.Baseline = &baseline
		command := ceph.NativeCommand{
			Argv:    append([]string{"python3", "-c", osdCPULoadScript, osdCPUPool}, append(slices.Clone(osdCPUWorkload), label)...),
			Timeout: 2 * time.Minute, MaxStdout: 4 << 10, MaxStderr: 4 << 10, Label: "osd cpu load " + label,
		}
		w.ExecStart = time.Now()
		call := startNative(ctx, cluster, client, command)
		var outcome nativeOutcome
		for waiting := true; waiting; {
			select {
			case outcome = <-call.done:
				w.ExecEnd = time.Now()
				waiting = false
			case <-time.After(2 * time.Second):
				w.Samples = append(w.Samples, observe())
			}
		}
		if b, err := target.read(inspect, time.Now); err != nil {
			w.AfterErr = err.Error()
		} else {
			w.After = b
		}
		w.Samples = append(w.Samples, observe())
		switch {
		case outcome.err != nil:
			w.LoadErr = "workload call: " + outcome.err.Error()
		case outcome.result.ExitCode != 0 || !outcome.result.Complete():
			w.LoadErr = "workload exit or incomplete output"
		default:
			if report, err := parseOSDCPULoadReport([]byte(lastJSONLine(string(outcome.result.Stdout)))); err != nil {
				w.LoadErr = err.Error()
			} else {
				w.Load = &report
			}
		}
		*ref = osdCPUWindowRef{Identity: osdCPUIdentity{Generation: w.Generation, PID: w.PID}, Expected: w.Expected, ExecStart: w.ExecStart, ExecEnd: w.ExecEnd}
		verdict, verr := classifyOSDCPUWindow(w, osdCPUWindowChecks{})
		receipt := windowReceipt(w, verdict, verr)
		receipt["heartbeat_failure_line_before"], receipt["heartbeat_failure_line_after"] = failureBefore, failureLine()
		windowReceipts = append(windowReceipts, receipt)
		data, _ := json.Marshal(receipt)
		t.Logf("OSD_CPU_WINDOW %s", data)
		var unstopped *ceph.NativeCommandUnstoppedError
		if errors.As(outcome.err, &unstopped) {
			t.Fatalf("%s window: the workload could not be confirmed stopped: %v", label, outcome.err)
		}
		return verdict, verr
	}

	// The live writer keeps one connection and one ledger for the whole run.
	const ledger, stopFile = "/tmp/tc-cpu-slowlive.jsonl", "/tmp/tc-cpu-slowlive.stop"
	actor := startNative(ctx, cluster, client, secondFaultCommand("cpu actor", 25*time.Minute, "actor", osdCPUPool, ledger, stopFile, "1", "900"))
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
	healthy := secondFaultAcks(t, ctx, cluster, client, ledger, secondFaultMark{}, 3, "while healthy")

	// Admission: one explicit update from the unlimited creation state to the
	// finite baseline. A failed or uncertain admission stops the run here.
	admit := applied("admission", osdCPUBaseline, &run.Admit)
	baselineMax := admit.Kernel.Max
	if err := cpuMaxMatchesNanoCPUs(baselineMax, osdCPUBaseline); err != nil {
		t.Fatalf("admitted kernel limit: %v", err)
	}

	// Control window under the admitted baseline: the workload must not
	// throttle the OSD there, or the pressure window would prove nothing.
	var verr error
	if run.Control, verr = window("baseline", baselineMax, &run.ControlRef); run.Control != osdWindowNotThrottled {
		t.Fatalf("baseline control window is %s, not a clean no-throttle interval: %v", run.Control, verr)
	}

	tighten := applied("tighten", osdCPUTight, &run.Tighten)
	tightMax := tighten.Kernel.Max
	if err := cpuMaxMatchesNanoCPUs(tightMax, osdCPUTight); err != nil {
		t.Fatalf("tightened kernel limit: %v", err)
	}
	// The progress baseline is taken after the tightened limit is confirmed.
	confirmed, _ := secondFaultMarkNow(t, ctx, cluster, client, ledger)

	run.Pressure, verr = window("pressure", tightMax, &run.PressureRef)
	underPressure, _ := secondFaultMarkNow(t, ctx, cluster, client, ledger)
	t.Logf("OSD_CPU_LEDGER healthy=%+v tightened=%+v after_pressure=%+v pressure_verdict=%s", healthy, confirmed, underPressure, run.Pressure)
	if run.Pressure == osdWindowExited || run.Pressure == osdWindowMarkedDown {
		t.Fatalf("the OSD did not stay a live member: %s: %v", run.Pressure, verr)
	}

	restore := applied("restore", osdCPUBaseline, &run.Restore)
	if err := verifyCPURoundTrip(osdCPUBaseline, run.Tighten, restore, cpuRoundTripChecks{}); err != nil {
		t.Fatalf("finite round trip not established: %v", err)
	}
	restoredMark, _ := secondFaultMarkNow(t, ctx, cluster, client, ledger)

	if run.Back, verr = window("restored", restore.Kernel.Max, &run.BackRef); run.Back != osdWindowNotThrottled {
		t.Logf("restored window is %s: %v", run.Back, verr)
	}
	afterRestore := secondFaultAcks(t, ctx, cluster, client, ledger, restoredMark, 3, "after the baseline was restored")
	run.LedgerAcksAfterRestore = true
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Logf("cluster did not return to clean: %v", err)
	} else {
		run.ClusterClean = true
	}
	if s, err := inspect(); err == nil && target.fence(s) == nil {
		run.SameInstanceAtEnd = true
	}

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
	read, err := cluster.RunNativeCommand(ctx, toolkitContainer(t, cluster), secondFaultCommand("cpu independent read", 3*time.Minute, "read", osdCPUPool, string(encodedNames)))
	if err != nil || read.ExitCode != 0 || !read.Complete() {
		t.Fatalf("independent read of the writer's objects: exit %d: %v: %.2048s", read.ExitCode, err, read.Stderr)
	}
	var objects map[string]secondFaultObject
	if err := json.Unmarshal(read.Stdout, &objects); err != nil {
		t.Fatalf("independent read %.512s: %v", read.Stdout, err)
	}
	verifyErr := secondFaultVerify(ops, objects)
	run.LedgerVerified = verifyErr == nil && final.Acked >= afterRestore.Acked && final.Acked > 0 && final.Refused == 0
	details["ledger"] = map[string]any{"healthy": healthy, "tightened": confirmed, "after_pressure": underPressure, "restored": restoredMark, "after_restore": afterRestore, "final": final}
	if verifyErr != nil {
		t.Errorf("data oracle: %v", verifyErr)
	}

	verdict, reasons := classifyOSDCPURun(run)
	t.Logf("OSD_CPU_SLOWLIVE_VERDICT verdict=%s reasons=%v", verdict, reasons)
	if verdict != osdRunSlowLive {
		t.Fatalf("not a slow-live scenario: %s: %v", verdict, reasons)
	}
	t.Log("OSD_CPU_SLOWLIVE established for one OSD, one engine and one run: finite limit tightened and restored on one process generation, kernel throttling counters advanced under the tight limit and did not under the baseline or restored limit, the OSD stayed up in the map, client writes were acknowledged in every window and every acknowledged object read back")
}

func transitionReceipt(tr cpuTransitionRecord) map[string]any {
	verdict, err := classifyCPUTransition(tr, cpuTransitionChecks{})
	out := map[string]any{"requested_nano": tr.RequestedNano, "reply_received": tr.ReplyReceived, "warnings": len(tr.Warnings), "verdict": verdict}
	if err != nil {
		out["error"] = err.Error()
	}
	if tr.Kernel != nil {
		out["kernel_quota"], out["kernel_period"] = tr.Kernel.Max.Quota, tr.Kernel.Max.Period
	}
	return out
}

func windowReceipt(w osdCPUWindow, verdict osdCPUWindowVerdict, verr error) map[string]any {
	out := map[string]any{"label": w.Label, "verdict": verdict, "expected_quota": w.Expected.Quota, "expected_period": w.Expected.Period}
	if verr != nil {
		out["error"] = verr.Error()
	}
	if w.Before != nil && w.After != nil {
		b, a := w.Before.Stat, w.After.Stat
		out["d_periods"], out["d_throttled"], out["d_throttled_usec"] = a.Periods-b.Periods, a.Throttled-b.Throttled, a.ThrottledUsec-b.ThrottledUsec
		out["installed_before"], out["installed_after"] = w.Before.Max, w.After.Max
	}
	if w.BeforeErr != "" || w.AfterErr != "" {
		out["reading_errors"] = []string{w.BeforeErr, w.AfterErr}
	}
	if w.Load != nil {
		out["load"] = *w.Load
	}
	if w.LoadErr != "" {
		out["load_error"] = w.LoadErr
	}
	if !w.ExecEnd.IsZero() {
		out["exec_ns"] = w.ExecEnd.Sub(w.ExecStart).Nanoseconds()
	}
	if w.Baseline != nil {
		out["baseline_observation"] = w.Baseline.summary()
	}
	var samples []string
	for _, s := range w.Samples {
		samples = append(samples, s.summary())
	}
	out["observations"] = samples
	return out
}

func (v osdSlowLiveView) summary() string {
	parts := []string{v.At.UTC().Format("15:04:05.000")}
	if v.Instance != nil {
		parts = append(parts, fmt.Sprintf("running=%t oom=%t", v.Instance.Running, v.Instance.OOMKilled))
	}
	if v.Map != nil {
		parts = append(parts, fmt.Sprintf("up=%t in=%t up_from=%d down_at=%d", v.Map.Up, v.Map.In, v.Map.UpFrom, v.Map.DownAt))
	}
	if v.PGRead {
		parts = append(parts, fmt.Sprintf("pg=%s primary=%d", v.PGState, v.ActingPrimary))
	}
	if v.HealthRead {
		parts = append(parts, "health="+strings.Join(v.HealthCodes, ","))
	}
	if len(v.Errs) > 0 {
		parts = append(parts, "errors="+strings.Join(v.Errs, "; "))
	}
	return strings.Join(parts, " ")
}
