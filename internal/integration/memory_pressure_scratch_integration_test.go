//go:build integration && ci_optional && ci_batch_memory_pressure_scratch

//ci: timeout=15m job-timeout=25 image-roles=essential

package integration_test

import (
	"context"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const (
	// Provisional scratch values, not supported Ceph defaults. The control
	// request stays far below the ceiling and the kill request exceeds it by a
	// bounded amount. The container is created with the ceiling and zero
	// container swap; there is no update and no search for a smaller limit.
	memoryScratchCeilingBytes int64  = 128 << 20
	memoryScratchControlBytes uint64 = 64 << 20
	memoryScratchKillBytes    uint64 = 192 << 20
	memoryScratchChunkBytes   uint64 = 8 << 20
)

// memoryAllocScript is the bounded allocator, run with python3 -I -c inside the
// target. argv[1] is the total request in bytes and argv[2] the chunk size.
// Every chunk is filled so its pages are resident, and one progress line is
// flushed after each chunk. The completion line is printed only after the whole
// request is held, so a killed allocator leaves progress lines and no
// completion line.
const memoryAllocScript = `import json,sys
req=int(sys.argv[1]); chunk=int(sys.argv[2])
held=[]; n=0
while n<req:
    s=min(chunk,req-n)
    held.append(b"\xa5"*s)
    n+=s
    print(json.dumps({"progress_bytes":n}),flush=True)
print(json.dumps({"completed":True,"requested_bytes":req,"allocated_bytes":n}),flush=True)
`

// TestMemoryPressureScratchMechanism is the memory mechanism control of the
// pressure qualification: one disposable container without Ceph has a finite
// hard memory ceiling and no container swap. A bounded allocator first holds a
// request well below the ceiling, which must complete without any local memory
// counter advancing. The same allocator then requests more than the ceiling
// once, which must be killed exactly once by the cgroup OOM killer while the
// scratch init survives. The allocator runs in the target's own cgroup, so the
// result is an allocator OOM in the target cgroup and never a daemon OOM. It
// uses no Ceph daemon and says nothing about a MON or OSD under a memory limit,
// recovery, other ceilings or other engines.
func TestMemoryPressureScratchMechanism(t *testing.T) {
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
	if info.Info.CgroupVersion != "2" || !info.Info.MemoryLimit || !info.Info.SwapLimit {
		t.Skip("eligibility: cgroup v2 memory limit and swap limit support required")
	}
	t.Logf("MEMORY_PRESSURE_CONTROLLER euid=%d engine=%s arch=%s", os.Geteuid(), info.Info.ServerVersion, runtime.GOARCH)

	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: harness.EssentialImage(t), Entrypoint: []string{"sleep"}, Cmd: []string{"infinity"},
		HostConfigModifier: func(h *container.HostConfig) {
			h.NetworkMode = "none"
			h.Memory = memoryScratchCeilingBytes
			h.MemorySwap = memoryScratchCeilingBytes
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
	first, err := rawInspect(ctx)
	if err != nil {
		t.Fatalf("engine inspection before binding: %v", err)
	}
	if err := memoryScratchEngineMatches(first, memoryScratchCeilingBytes); err != nil {
		t.Fatalf("engine configuration: %v", err)
	}
	state, err := cpuEngineStateFromInspect(first, cpuEngineChecks{})
	if err != nil {
		t.Fatalf("engine state before binding: %v", err)
	}
	gen := containerGeneration{ID: id, StartedAt: state.StartedAt}
	target, err := bindCPUWitnessTarget(cpuFSRoots{Proc: "/proc", Cgroup: "/sys/fs/cgroup"}, gen, state, cpuBindChecks{})
	if err != nil {
		t.Skipf("eligibility incomplete before any allocation: %v", err)
	}
	t.Log(target.Receipt())

	expected := memoryLimit{Bytes: uint64(memoryScratchCeilingBytes)}
	// window runs the bounded allocator between two fenced kernel readings and
	// records the allocator's own outcome separately from the engine's record of
	// the scratch init. Unknown outcomes stay nil.
	window := func(label string, requested uint64) memoryScratchRecord {
		t.Helper()
		rec := memoryScratchRecord{Requested: requested}
		rec.Window = memoryPressureRecord{Generation: gen, Expected: expected, PID: target.PID, ProcessStart: target.ProcessStart}
		before, procs, err := readMemoryScratchWitness(target, inspect, time.Now)
		if err != nil {
			t.Fatalf("%s: first reading: %v", label, err)
		}
		rec.Window.Before, rec.Window.ProcsBefore = before, procs
		call, done := context.WithTimeout(ctx, 60*time.Second)
		defer done()
		rec.Window.ExecStart = time.Now()
		code, reader, execErr := ctr.Exec(call, []string{"python3", "-I", "-c", memoryAllocScript, strconv.FormatUint(requested, 10), strconv.FormatUint(memoryScratchChunkBytes, 10)}, tcexec.Multiplexed())
		var out []byte
		var readErr error
		if execErr == nil {
			out, readErr = io.ReadAll(reader)
			rec.ExecExit = &code
		}
		rec.Window.ExecEnd = time.Now()
		if execErr == nil && readErr == nil {
			if report, perr := parseMemoryAllocReport(out); perr == nil {
				rec.Report = &report
			} else {
				t.Logf("MEMORY_PRESSURE_ALLOCATOR label=%s unparsable report: %v", label, perr)
			}
		}
		t.Logf("MEMORY_PRESSURE_ALLOCATOR label=%s requested=%d exec_error=%v read_error=%v exit=%v", label, requested, execErr, readErr, code)
		if c, err := rawInspect(ctx); err != nil {
			t.Logf("MEMORY_PRESSURE_INIT label=%s inspection error: %v", label, err)
		} else if exit, err := memoryExitFromInspect(c); err != nil {
			t.Logf("MEMORY_PRESSURE_INIT label=%s record error: %v", label, err)
		} else {
			rec.Window.Exit = exit
			t.Logf("MEMORY_PRESSURE_INIT label=%s running=%t exit_code=%d engine_oom_flag=%t", label, exit.Running, exit.ExitCode, exit.OOMKilled)
		}
		if after, _, err := readMemoryScratchWitness(target, inspect, time.Now); err != nil {
			t.Logf("MEMORY_PRESSURE_KERNEL_UNAVAILABLE label=%s after-reading error: %v", label, err)
		} else {
			rec.Window.After = after
		}
		if rec.Report != nil {
			t.Logf("MEMORY_PRESSURE_REPORT label=%s progress=%d chunks=%d completed=%t", label, rec.Report.Progress, rec.Report.Chunks, rec.Report.Final != nil)
		}
		if b, a := rec.Window.Before, rec.Window.After; b != nil && a != nil {
			t.Logf("MEMORY_PRESSURE_WINDOW label=%s max=%d swap_max=%d d_max=%d d_oom=%d d_oom_kill=%d",
				label, a.Max.Bytes, a.SwapMax.Bytes, a.Events.Max-b.Events.Max, a.Events.OOM-b.Events.OOM, a.Events.OOMKill-b.Events.OOMKill)
		}
		return rec
	}

	baseline, _, err := readMemoryScratchWitness(target, inspect, time.Now)
	if err != nil {
		t.Fatalf("baseline witness: %v", err)
	}
	if !baseline.ControllerListed {
		t.Skip("eligibility: the memory controller is not listed on the target cgroup; recorded as unsupported")
	}

	control := window("control", memoryScratchControlBytes)
	cv, cerr := classifyMemoryScratch(control, memoryScratchChecks{})
	t.Logf("MEMORY_PRESSURE_VERDICT label=control verdict=%s error=%v", cv, cerr)

	pressure := window("kill", memoryScratchKillBytes)
	pv, perr := classifyMemoryScratch(pressure, memoryScratchChecks{})
	t.Logf("MEMORY_PRESSURE_VERDICT label=kill verdict=%s error=%v", pv, perr)

	if cv != memoryScratchWithinCeiling {
		t.Fatalf("control window is not a clean within-ceiling interval: %s %v", cv, cerr)
	}
	if pv != memoryScratchAllocatorOOM {
		t.Fatalf("kill window did not establish a single allocator OOM: %s %v", pv, perr)
	}
	t.Log("MEMORY_PRESSURE qualified (scratch mechanism only): the bounded allocator completed below the ceiling with no counter advance and was killed once above it while the scratch init survived, same container generation")
}
