//go:build integration && ci_optional && ci_batch_cpu_finite_roundtrip

//ci: timeout=15m job-timeout=25 image-roles=essential,client

package integration_test

import (
	"context"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

const (
	// cpuRoundTripBaseline is the finite ceiling installed at creation and
	// restored exactly. cpuRoundTripTight is strictly below it. Both are
	// provisional scratch values, not supported Ceph defaults, and neither is
	// meant to cause throttling.
	cpuRoundTripBaseline int64 = 1_000_000_000
	cpuRoundTripTight    int64 = 500_000_000
)

// TestCPUFiniteBaselineRoundTrip updates one frozen MON generation from a
// finite creation-time CPU limit to a stricter one and back to the identical
// limit, reading the kernel cpu.max after each update. It applies no CPU load
// and does not qualify throttling, a CPU-starved daemon, OOM or reset of an
// unlimited baseline.
func TestCPUFiniteBaselineRoundTrip(t *testing.T) {
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
	t.Logf("CPU_ROUNDTRIP_CONTROLLER euid=%d engine=%s arch=%s ncpu=%d", os.Geteuid(), info.Info.ServerVersion, runtime.GOARCH, info.Info.NCPU)

	image, opts := integrationImages(t)
	opts = append(opts, ceph.WithNoInitialManagers(), ceph.WithNoInitialOSDs(),
		testcontainers.WithHostConfigModifier(func(h *container.HostConfig) {
			h.NanoCPUs = cpuRoundTripBaseline
			h.RestartPolicy = container.RestartPolicy{Name: container.RestartPolicyDisabled}
		}))
	cluster, client := newServiceClusterWithOptions(t, image, opts...)
	id := cluster.GetContainerID()
	inspect := func() (cpuEngineState, error) {
		call, done := context.WithTimeout(ctx, 10*time.Second)
		defer done()
		raw, err := docker.ContainerInspect(call, id, mobycl.ContainerInspectOptions{})
		if err != nil {
			return cpuEngineState{}, err
		}
		return cpuEngineStateFromInspect(raw.Container, cpuEngineChecks{})
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

	rawInspect := func(call context.Context) (container.InspectResponse, error) {
		c, done := context.WithTimeout(call, 10*time.Second)
		defer done()
		r, err := docker.ContainerInspect(c, id, mobycl.ContainerInspectOptions{})
		return r.Container, err
	}
	// update dispatches one request and records the transition. The reply is
	// retained as received or uncertain; an uncertain outcome is settled only
	// by the post-update inspection and kernel reading of the same generation.
	update := func(call context.Context, nano int64) (cpuTransitionRecord, error) {
		var updateErr error
		var replyWarnings []string
		dispatched := false
		_, guardErr := guardedCPUUpdate(gen, func() (container.InspectResponse, error) { return rawInspect(call) }, func() ([]string, error) {
			dispatched = true
			c, done := context.WithTimeout(call, 20*time.Second)
			defer done()
			result, err := docker.ContainerUpdate(c, id, mobycl.ContainerUpdateOptions{Resources: &container.Resources{NanoCPUs: nano}})
			updateErr, replyWarnings = err, result.Warnings
			return result.Warnings, err
		})
		if !dispatched {
			return cpuTransitionRecord{}, guardErr
		}
		received, warnings := cpuUpdateReply(replyWarnings, updateErr)
		after, err := rawInspect(call)
		if err != nil {
			return cpuTransitionRecord{}, err
		}
		kernel, kernelErr := target.read(inspect, time.Now)
		if kernelErr != nil {
			t.Logf("CPU_ROUNDTRIP_KERNEL_UNAVAILABLE requested=%d error=%v", nano, kernelErr)
			kernel = nil
		}
		t.Logf("CPU_ROUNDTRIP_UPDATE requested=%d reply_received=%t update_error=%v warnings=%d", nano, received, updateErr, len(warnings))
		return assembleCPUTransition(gen, nano, received, warnings, after, kernel)
	}

	// Restoration has its own deadline and never touches another generation.
	restored := false
	t.Cleanup(func() {
		if restored {
			return
		}
		call, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		_, err := guardedCPUUpdate(gen, func() (container.InspectResponse, error) { return rawInspect(call) }, func() ([]string, error) {
			result, err := docker.ContainerUpdate(call, id, mobycl.ContainerUpdateOptions{Resources: &container.Resources{NanoCPUs: cpuRoundTripBaseline}})
			return result.Warnings, err
		})
		if err != nil {
			t.Error("CPU restoration incomplete or independently failed", err)
		}
	})

	quorum := func(label string) {
		t.Helper()
		result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{Argv: []string{"ceph", "--connect-timeout", "5", "quorum_status", "--format", "json"}, Timeout: 10 * time.Second, MaxStdout: 64 << 10, MaxStderr: 4096, Label: label})
		if err != nil || !result.Complete() || result.ExitCode != 0 {
			t.Fatalf("independent native observer %q: exit=%d complete=%t error=%v", label, result.ExitCode, result.Complete(), err)
		}
	}

	// Baseline admission: engine and kernel both show the finite creation limit.
	baseline, err := target.read(inspect, time.Now)
	if err != nil {
		t.Fatalf("baseline witness: %v", err)
	}
	if !baseline.ControllerListed {
		t.Skip("eligibility: the cpu controller is not listed on the target cgroup; recorded as unsupported")
	}
	if err := cpuMaxMatchesNanoCPUs(baseline.Max, cpuRoundTripBaseline); err != nil {
		t.Fatalf("baseline kernel and engine limits disagree: %v", err)
	}
	t.Logf("CPU_ROUNDTRIP_BASELINE quota=%d period=%d", baseline.Max.Quota, baseline.Max.Period)
	quorum("cpu round trip baseline observer")

	tighten, err := update(ctx, cpuRoundTripTight)
	if err != nil {
		t.Fatalf("tighten record: %v", err)
	}
	quorum("cpu round trip tightened observer")
	restore, err := update(ctx, cpuRoundTripBaseline)
	if err != nil {
		t.Fatalf("restore record: %v", err)
	}
	for name, tr := range map[string]cpuTransitionRecord{"tighten": tighten, "restore": restore} {
		v, verr := classifyCPUTransition(tr, cpuTransitionChecks{})
		t.Logf("CPU_ROUNDTRIP_TRANSITION step=%s verdict=%s error=%v", name, v, verr)
	}
	// On a failing verdict the cleanup above still attempts the baseline on
	// the same generation. That is cleanup, not restoration evidence.
	if err := verifyCPURoundTrip(cpuRoundTripBaseline, tighten, restore, cpuRoundTripChecks{}); err != nil {
		t.Fatalf("finite round trip not established: %v", err)
	}
	restored = true
	quorum("cpu round trip restored observer")
	t.Log("CPU_ROUNDTRIP qualified: finite baseline -> stricter limit -> identical baseline; same container generation, engine and kernel agree at each step")
}
