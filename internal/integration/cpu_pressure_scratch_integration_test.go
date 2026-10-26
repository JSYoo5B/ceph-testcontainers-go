//go:build integration && ci_optional && ci_batch_cpu_pressure_scratch

//ci: timeout=15m job-timeout=25 image-roles=essential

package integration_test

import (
	"context"
	"io"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const (
	// Provisional scratch values, not supported Ceph defaults. The worker is a
	// single thread, so it demands at most one CPU: that exceeds the tight
	// ceiling and stays far below the baseline.
	cpuPressureBaseline int64 = 2_000_000_000
	cpuPressureTight    int64 = 250_000_000
	cpuPressureSeconds        = "5"
)

// cpuBurnScript is the bounded worker, run with python3 -I -c inside the
// target. argv[1] is the duration in whole seconds. It prints one JSON object
// after the loop; a killed or timed-out worker prints nothing and therefore
// has no report.
const cpuBurnScript = `import json,sys,time
secs=int(sys.argv[1])
t0=time.monotonic_ns()
end=t0+secs*1000000000
n=0
while time.monotonic_ns()<end:
    n+=1
print(json.dumps({"wall_ns":time.monotonic_ns()-t0,"cpu_ns":time.process_time_ns(),"iterations":n,"completed":True}))
`

// TestCPUPressureScratchMechanism is the mechanism control of the CPU-pressure
// qualification: one disposable container without Ceph runs a bounded
// single-thread worker under a tightened CPU ceiling, and the kernel
// cpu.stat throttling counters of that container's cgroup must advance across
// the bracketed window. The same worker is then run under the restored
// baseline ceiling, where its demand is far below the quota, as a control that
// the counters do not advance on their own. It uses no Ceph daemon and says
// nothing about a throttled MON, OSD recovery, OOM or other engines.
func TestCPUPressureScratchMechanism(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("eligibility: Linux local cgroup-v2 observer required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
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
	if info.Info.CgroupVersion != "2" || !info.Info.CPUCfsQuota || info.Info.NCPU < 2 {
		t.Skip("eligibility: cgroup v2 CFS quota support and at least two CPUs required")
	}
	t.Logf("CPU_PRESSURE_CONTROLLER euid=%d engine=%s arch=%s ncpu=%d", os.Geteuid(), info.Info.ServerVersion, runtime.GOARCH, info.Info.NCPU)

	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: controlImage(t), Entrypoint: []string{"sleep"}, Cmd: []string{"infinity"},
		HostConfigModifier: func(h *container.HostConfig) {
			h.NetworkMode = "none"
			h.NanoCPUs = cpuPressureBaseline
			h.RestartPolicy = container.RestartPolicy{Name: container.RestartPolicyDisabled}
		},
	}, Started: true})
	if ctr != nil {
		t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), time.Minute)
			defer done()
			if err := ctr.Terminate(cleanup); err != nil {
				t.Error("scratch container cleanup", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	id := ctr.GetContainerID()

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
		t.Skipf("eligibility incomplete before any update: %v", err)
	}
	t.Log(target.receipt())

	// update dispatches one request after the stale-generation guard and
	// records the transition from the reply, a fresh inspection and the kernel
	// reading of the same generation.
	update := func(nano int64) (cpuTransitionRecord, error) {
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
			return cpuTransitionRecord{}, guardErr
		}
		received, warnings := cpuUpdateReply(replyWarnings, updateErr)
		after, err := rawInspect(ctx)
		if err != nil {
			return cpuTransitionRecord{}, err
		}
		kernel, kernelErr := target.read(inspect, time.Now)
		if kernelErr != nil {
			t.Logf("CPU_PRESSURE_KERNEL_UNAVAILABLE requested=%d error=%v", nano, kernelErr)
			kernel = nil
		}
		t.Logf("CPU_PRESSURE_UPDATE requested=%d reply_received=%t update_error=%v warnings=%d", nano, received, updateErr, len(warnings))
		return assembleCPUTransition(gen, nano, received, warnings, after, kernel)
	}

	// window runs the bounded worker between two kernel readings. The worker
	// outcome stays nil unless the exec returned zero and its report parsed.
	window := func(label string, expected cpuMax) cpuPressureRecord {
		t.Helper()
		rec := cpuPressureRecord{Generation: gen, Expected: expected}
		var err error
		if rec.Before, err = target.read(inspect, time.Now); err != nil {
			t.Fatalf("%s: first reading: %v", label, err)
		}
		call, done := context.WithTimeout(ctx, 60*time.Second)
		defer done()
		rec.ExecStart = time.Now()
		code, reader, execErr := ctr.Exec(call, []string{"python3", "-I", "-c", cpuBurnScript, cpuPressureSeconds}, tcexec.Multiplexed())
		var out []byte
		var readErr error
		if execErr == nil {
			out, readErr = io.ReadAll(reader)
		}
		rec.ExecEnd = time.Now()
		if execErr == nil && readErr == nil && code == 0 {
			if burn, perr := parseCPUBurnReport(out); perr == nil {
				rec.Burn = &burn
			} else {
				t.Logf("CPU_PRESSURE_WORKER label=%s unparsable report: %v", label, perr)
			}
		} else {
			t.Logf("CPU_PRESSURE_WORKER label=%s exec_error=%v read_error=%v exit=%d", label, execErr, readErr, code)
		}
		if rec.After, err = target.read(inspect, time.Now); err != nil {
			t.Fatalf("%s: second reading: %v", label, err)
		}
		b, a := rec.Before.Stat, rec.After.Stat
		t.Logf("CPU_PRESSURE_WINDOW label=%s quota=%d period=%d d_periods=%d d_throttled=%d d_throttled_usec=%d",
			label, rec.After.Max.Quota, rec.After.Max.Period, a.Periods-b.Periods, a.Throttled-b.Throttled, a.ThrottledUsec-b.ThrottledUsec)
		if rec.Burn != nil {
			t.Logf("CPU_PRESSURE_BURN label=%s wall_ns=%d cpu_ns=%d iterations=%d", label, rec.Burn.WallNanos, rec.Burn.CPUNanos, rec.Burn.Iterations)
		}
		return rec
	}

	// Baseline admission: engine and kernel show the finite creation ceiling.
	baseline, err := target.read(inspect, time.Now)
	if err != nil {
		t.Fatalf("baseline witness: %v", err)
	}
	if !baseline.ControllerListed {
		t.Skip("eligibility: the cpu controller is not listed on the target cgroup; recorded as unsupported")
	}
	if err := cpuMaxMatchesNanoCPUs(baseline.Max, cpuPressureBaseline); err != nil {
		t.Fatalf("baseline kernel and engine limits disagree: %v", err)
	}

	tighten, err := update(cpuPressureTight)
	if err != nil {
		t.Fatalf("tighten record: %v", err)
	}
	if v, verr := classifyCPUTransition(tighten, cpuTransitionChecks{}); v != cpuTransitionApplied {
		t.Fatalf("tighten not applied: %s %v", v, verr)
	}
	tightMax := tighten.Kernel.Max
	if err := cpuMaxMatchesNanoCPUs(tightMax, cpuPressureTight); err != nil {
		t.Fatalf("tightened kernel limit: %v", err)
	}

	pressure := window("tight", tightMax)
	pv, perr := classifyCPUPressure(pressure, cpuPressureChecks{})
	t.Logf("CPU_PRESSURE_VERDICT label=tight verdict=%s error=%v", pv, perr)

	restore, err := update(cpuPressureBaseline)
	if err != nil {
		t.Fatalf("restore record: %v", err)
	}
	if err := verifyCPURoundTrip(cpuPressureBaseline, tighten, restore, cpuRoundTripChecks{}); err != nil {
		t.Fatalf("finite round trip not established: %v", err)
	}

	control := window("baseline", restore.Kernel.Max)
	cv, cerr := classifyCPUPressure(control, cpuPressureChecks{})
	t.Logf("CPU_PRESSURE_VERDICT label=baseline verdict=%s error=%v", cv, cerr)

	if pv != cpuPressureThrottled {
		t.Fatalf("tight window did not establish throttling: %s %v", pv, perr)
	}
	if cv != cpuPressureDemandNotThrottled {
		t.Fatalf("baseline control window is not a clean no-throttle interval: %s %v", cv, cerr)
	}
	t.Log("CPU_PRESSURE qualified (scratch mechanism only): kernel throttling counters advanced under the tight ceiling and did not under the restored baseline, same container generation")
}
