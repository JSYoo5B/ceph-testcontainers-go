//go:build integration && ci_optional && ci_batch_cpu_witness_readonly

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

// cpuReadonlyNano is a generous finite CPU limit installed at container
// creation. It is not a pressure setting and is never changed afterwards.
const cpuReadonlyNano int64 = 1_000_000_000

// TestCPUWitnessReadonly reads the target-local CPU controller witness of one
// live MON generation twice and classifies the interval. It installs a
// finite quota only through the container's creation request, never updates
// it and applies no CPU pressure. It does not qualify throttling.
func TestCPUWitnessReadonly(t *testing.T) {
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
	t.Logf("CPU_WITNESS_CONTROLLER euid=%d engine=%s arch=%s ncpu=%d", os.Geteuid(), info.Info.ServerVersion, runtime.GOARCH, info.Info.NCPU)

	image, opts := integrationImages(t)
	opts = append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialManagers().WithNoInitialOSDs()),
		testcontainers.WithHostConfigModifier(func(h *container.HostConfig) {
			h.NanoCPUs = cpuReadonlyNano
			h.RestartPolicy = container.RestartPolicy{Name: container.RestartPolicyDisabled}
		}))
	cluster, client := newServiceClusterWithOptions(t, image, opts...)
	id := cluster.GetContainerID()

	inspect := cpuEngineInspector(ctx, docker.Client, id)
	state, err := inspect()
	if err != nil {
		t.Fatalf("engine inspection before binding: %v", err)
	}
	raw, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
	if err != nil || raw.Container.HostConfig == nil {
		t.Fatalf("engine configuration unavailable: %v", err)
	}
	if raw.Container.HostConfig.NanoCPUs != cpuReadonlyNano {
		t.Fatalf("creation-time CPU limit was not installed: %d", raw.Container.HostConfig.NanoCPUs)
	}
	target, err := bindCPUWitnessTarget(cpuFSRoots{Proc: "/proc", Cgroup: "/sys/fs/cgroup"},
		containerGeneration{ID: id, StartedAt: state.StartedAt}, state, cpuBindChecks{})
	if err != nil {
		t.Skipf("eligibility incomplete before any reading: %v", err)
	}
	t.Log(target.receipt())

	before, err := target.read(inspect, time.Now)
	if err != nil {
		t.Fatalf("first witness reading: %v", err)
	}
	// Independent native activity between the readings; its result is not
	// part of the CPU verdict.
	result, runErr := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{Argv: []string{"ceph", "--connect-timeout", "5", "quorum_status", "--format", "json"}, Timeout: 10 * time.Second, MaxStdout: 64 << 10, MaxStderr: 4096, Label: "cpu witness activity"})
	if runErr != nil || !result.Complete() || result.ExitCode != 0 {
		t.Fatalf("native activity between readings: exit=%d complete=%t error=%v", result.ExitCode, result.Complete(), runErr)
	}
	time.Sleep(time.Second)
	after, err := target.read(inspect, time.Now)
	if err != nil {
		t.Fatalf("second witness reading: %v", err)
	}
	t.Logf("CPU_WITNESS_READING controller=%t max_unlimited=%t quota=%d period=%d periods=%d throttled=%d throttled_usec=%d",
		before.ControllerListed, before.Max.Unlimited, before.Max.Quota, before.Max.Period, before.Stat.Periods, before.Stat.Throttled, before.Stat.ThrottledUsec)
	t.Logf("CPU_WITNESS_READING controller=%t max_unlimited=%t quota=%d period=%d periods=%d throttled=%d throttled_usec=%d",
		after.ControllerListed, after.Max.Unlimited, after.Max.Quota, after.Max.Period, after.Stat.Periods, after.Stat.Throttled, after.Stat.ThrottledUsec)
	if !before.ControllerListed || !after.ControllerListed {
		t.Skip("eligibility: the cpu controller is not listed on the target cgroup; recorded as unsupported, not as zero throttling")
	}
	if err := cpuMaxMatchesNanoCPUs(after.Max, raw.Container.HostConfig.NanoCPUs); err != nil {
		t.Fatalf("kernel and engine CPU limits disagree: %v", err)
	}
	verdict, err := classifyCPUThrottle(before, after, false, cpuClassifyChecks{})
	t.Logf("CPU_WITNESS_VERDICT verdict=%s error=%v", verdict, err)
	if err != nil {
		t.Fatalf("interval is not classifiable: %v", err)
	}
	// Throttling is not expected at this limit and is not asserted either way.
	if verdict != cpuVerdictObservedZero && verdict != cpuVerdictThrottled {
		t.Fatalf("unexpected verdict %s", verdict)
	}
	// The generation must still be the frozen one after the whole scenario.
	if _, err := target.read(inspect, time.Now); err != nil {
		t.Fatalf("closing fence: %v", err)
	}
}

// cpuEngineInspector returns the per-fence inspection function of one frozen
// container ID. Each call has its own deadline and inspects the same ID.
func cpuEngineInspector(ctx context.Context, docker *mobycl.Client, id string) func() (cpuEngineState, error) {
	return func() (cpuEngineState, error) {
		call, done := context.WithTimeout(ctx, 10*time.Second)
		defer done()
		raw, err := docker.ContainerInspect(call, id, mobycl.ContainerInspectOptions{})
		if err != nil {
			return cpuEngineState{}, err
		}
		return cpuEngineStateFromInspect(raw.Container, cpuEngineChecks{})
	}
}
