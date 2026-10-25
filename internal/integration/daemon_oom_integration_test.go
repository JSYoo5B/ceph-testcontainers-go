//go:build integration && ci_optional && ci_batch_daemon_oom

//ci: timeout=15m job-timeout=25 image-roles=essential,client

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
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

// TestDaemonMemoryQualification is an opt-in, local Linux/cgroup-v2 experiment.
// An eligible but unobserved OOM fails; it is never relabeled as a native pass.
func TestDaemonMemoryQualification(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("eligibility: Linux local cgroup-v2 observer required")
	}
	t.Logf("DAEMON_MEMORY_CONTROLLER euid=%d local_process_observer=true", os.Geteuid())
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
		t.Skip("eligibility: cgroup v2 memory and swap controllers required")
	}
	// No pressure is put on the host. Refuse when its current available capacity
	// cannot reserve the experiment's finite victim budget plus other MONs.
	mem, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		t.Skip("eligibility: host headroom unavailable")
	}
	var available uint64
	for _, line := range strings.Split(string(mem), "\n") {
		if strings.HasPrefix(line, "MemAvailable:") {
			_, _ = fmt.Sscanf(line, "MemAvailable: %d kB", &available)
		}
	}
	if available < 4<<20 {
		t.Skip("eligibility: less than 4 GiB available runner memory")
	}
	image, opts := integrationImages(t)
	opts = append(opts, ceph.WithMonitorCount(3), ceph.WithNoInitialManagers(), ceph.WithNoInitialOSDs(),
		testcontainers.WithHostConfigModifier(func(h *container.HostConfig) {
			h.Memory = daemonMemoryBaseline
			h.MemorySwap = daemonMemoryBaseline
			h.RestartPolicy = container.RestartPolicy{Name: container.RestartPolicyDisabled}
		}))
	cluster, client := newServiceClusterWithOptions(t, image, opts...)
	id := cluster.GetContainerID()
	target, err := daemonMemoryBind(ctx, docker.Client, id)
	if err != nil {
		t.Fatalf("eligibility incomplete before any update: %v", err)
	}
	t.Cleanup(func() { _ = target.Events.Close() })
	raw, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("DAEMON_MEMORY_RUNTIME engine=%s arch=%s ceph=%s image=%s container=%s started=%s pid=%d process_start=%s", info.Info.ServerVersion, runtime.GOARCH, cluster.CephVersion(), raw.Container.Image, id, target.Started, target.PID, target.ProcessStart)
	// Restoration has its own deadline even if the workload is canceled. Do not
	// apply old settings to an unexpectedly restarted or replaced generation.
	restored := false
	t.Cleanup(func() {
		if restored {
			return
		}
		cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		current, err := docker.ContainerInspect(cleanup, id, mobycl.ContainerInspectOptions{})
		if err != nil || current.Container.State == nil || current.Container.State.StartedAt != target.Started {
			t.Error("memory restoration incomplete: captured generation unavailable")
			return
		}
		if err := daemonMemoryUpdate(cleanup, docker.Client, id, daemonMemoryBaseline); err != nil {
			t.Error("memory restoration independently failed", err)
		}
	})
	admin := func(want int) {
		t.Helper()
		result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{Argv: []string{"ceph", "--connect-timeout", "5", "quorum_status", "--format", "json"}, Timeout: 10 * time.Second, MaxStdout: 64 << 10, MaxStderr: 4096, Label: "independent memory experiment observer"})
		if err != nil || !result.Complete() || result.ExitCode != 0 {
			t.Fatalf("independent native observer: exit=%d complete=%t error=%v", result.ExitCode, result.Complete(), err)
		}
		var status struct {
			Quorum []string `json:"quorum_names"`
		}
		if json.Unmarshal(result.Stdout, &status) != nil || len(status.Quorum) != want {
			t.Fatalf("native quorum: want %d members, got %v", want, status.Quorum)
		}
	}
	same := func(limit int64) daemonMemorySample {
		t.Helper()
		if err := target.fence(ctx, docker.Client); err != nil {
			t.Fatal(err)
		}
		s, err := target.sample()
		if err != nil {
			t.Fatal(err)
		}
		if s.Max != uint64(limit) || s.Swap != 0 {
			t.Fatalf("effective limit/no-swap mismatch: max=%d swap=%d", s.Max, s.Swap)
		}
		inspect, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
		if err != nil || inspect.Container.HostConfig.Memory != limit || inspect.Container.HostConfig.MemorySwap != limit {
			t.Fatal("inspect/effective resource settings disagree", err)
		}
		return s
	}
	admin(3)
	before := same(daemonMemoryBaseline)
	if before.Current >= uint64(daemonMemoryBenign)/2 {
		t.Fatal("eligibility: benign transition lacks required memory headroom")
	}
	for _, limit := range []int64{daemonMemoryBenign, daemonMemoryBaseline} {
		if err := target.fence(ctx, docker.Client); err != nil {
			t.Fatal(err)
		}
		if err := daemonMemoryUpdate(ctx, docker.Client, id, limit); err != nil {
			t.Fatal(err)
		}
		current := same(limit)
		if current.Events["oom"] != before.Events["oom"] || current.Events["oom_kill"] != before.Events["oom_kill"] {
			t.Fatal("benign round trip encountered memory OOM")
		}
		admin(3)
	}
	t.Log("DAEMON_MEMORY_ROUNDTRIP qualified: finite baseline -> benign ceiling -> identical baseline; same process/cgroup, native quorum and no-swap observed")
	// One predeclared step. The observer and client stay outside this cgroup;
	// no allocator, debug command, stats exec or other worker is created in it.
	baseline := same(daemonMemoryBaseline)
	if baseline.Current <= uint64(daemonMemoryPressure) {
		t.Fatal("OOM pressure step is not eligible at current usage")
	}
	if err := target.fence(ctx, docker.Client); err != nil {
		t.Fatal(err)
	}
	recording, stopRecording := context.WithCancel(ctx)
	windowResult := target.record(recording, baseline.Events)
	defer stopRecording()
	updateErr := daemonMemoryUpdate(ctx, docker.Client, id, daemonMemoryPressure)
	t.Logf("DAEMON_MEMORY_PRESSURE requested=%d update_error=%v", daemonMemoryPressure, updateErr)
	var stopped, oomKilled, unchanged bool
	for deadline := time.Now().Add(10 * time.Second); ; {
		state, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
		if err != nil || state.Container.State == nil {
			t.Fatal("independent exit observation unavailable", err)
		}
		s := state.Container.State
		unchanged = state.Container.ID == id && s.StartedAt == target.Started && state.Container.RestartCount == 0
		stopped, oomKilled = !s.Running, s.OOMKilled
		if !unchanged {
			t.Fatal("pressure target generation changed unexpectedly")
		}
		if stopped || time.Now().After(deadline) {
			break
		}
		if err := target.fence(ctx, docker.Client); err != nil {
			t.Fatal("pressure process/cgroup fence", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	stopRecording()
	window := <-windowResult
	oomErr := daemonMemoryOOM(baseline.Events, window.Events, unchanged, !window.OtherProcess, oomKilled, stopped)
	if !window.PressureObserved {
		oomErr = fmt.Errorf("effective pressure ceiling was not observed (OOM evidence: %v)", oomErr)
	}
	t.Logf("DAEMON_MEMORY_OOM before=%v after=%v stopped=%t docker_oom=%t local_window_error=%v verdict=%v", baseline.Events, window.Events, stopped, oomKilled, window.Error, oomErr)
	// Restore the finite configuration before an explicit restart. A successful
	// update on a stopped container is only saved configuration, not runtime proof.
	cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
	err = daemonMemoryUpdate(cleanup, docker.Client, id, daemonMemoryBaseline)
	done()
	if err != nil {
		t.Fatal("restore before restart", err)
	}
	restored = true
	if !stopped {
		t.Fatal("OOM unobserved after the single step; baseline restored without retry")
	}
	if err := cluster.Container.Start(ctx); err != nil {
		t.Fatal("explicit restart", err)
	}
	deadline := time.Now().Add(90 * time.Second)
	for {
		restarted, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
		if err == nil && restarted.Container.State != nil && restarted.Container.State.Running && restarted.Container.State.StartedAt != target.Started {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("new daemon generation not observed", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	recovered, err := daemonMemoryBind(ctx, docker.Client, id)
	if err != nil {
		t.Fatal("new generation restored limits unavailable", err)
	}
	defer func() { _ = recovered.Events.Close() }()
	effective, err := recovered.sample()
	if err != nil || effective.Max != uint64(daemonMemoryBaseline) || effective.Swap != 0 {
		t.Fatal("restarted effective finite baseline/no-swap missing", err)
	}
	// MON rejoin takes time; use a fresh client command on each attempt.
	for {
		result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{Argv: []string{"ceph", "--connect-timeout", "5", "quorum_status", "--format", "json"}, Timeout: 10 * time.Second, MaxStdout: 64 << 10, MaxStderr: 4096, Label: "memory experiment recovery"})
		var status struct {
			Quorum []string `json:"quorum_names"`
		}
		if err == nil && result.Complete() && result.ExitCode == 0 && json.Unmarshal(result.Stdout, &status) == nil && len(status.Quorum) == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("restarted MON did not rejoin quorum", err)
		}
		time.Sleep(time.Second)
	}
	t.Logf("DAEMON_MEMORY_RECOVERY new_started=%s new_process_start=%s baseline=%d swap=%d independent_quorum=3", recovered.Started, recovered.ProcessStart, effective.Max, effective.Swap)
	if oomErr != nil {
		t.Fatal("daemon OOM qualification incomplete; recovery is a separate observation", oomErr)
	}
}
