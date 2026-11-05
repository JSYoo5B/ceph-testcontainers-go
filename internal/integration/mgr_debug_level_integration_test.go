//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_daemon_basics))))

//ci: timeout=45m job-timeout=55 image-roles=essential,client,rbd-aux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/finalize"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

// TestMGRDebugLevel opens runtime debug level windows on an active and a
// standby MGR and checks each state against the daemon's own config get: a
// verified apply and restore that records the MgrMap GID, a preserved outside
// edit, a paused MGR that is restored only after it resumes, and a failover by
// ceph mgr fail. The failed MGR respawns in its container with a new GID, so
// its window ends target-changed without a set, while the standby that took
// over keeps its process and its window is restored.
func TestMGRDebugLevel(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	cluster, err := ceph.Run(ctx, image, append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithManagerCount(2).WithOSDCount(1).WithPoolDefaults(1, 1)))...)
	if cluster != nil {
		if _, registerErr := finalize.TerminateOnCleanup(t, cluster, finalize.Config{Archive: true, Scenario: "mgr-debug-level"}); registerErr != nil {
			_ = cluster.Terminate(context.WithoutCancel(ctx))
			t.Fatal(registerErr)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	active, standby := mgrRoles(t, ctx, cluster)
	get := func(name, option string) string {
		t.Helper()
		out, err := harness.ManagerHandle(t, ctx, cluster, name).AdminSocket(ctx, "config", "get", option)
		var value map[string]string
		if err != nil || json.Unmarshal(out, &value) != nil || value[option] == "" {
			t.Fatalf("mgr.%s config get %s: %q %v", name, option, out, err)
		}
		return value[option]
	}
	set := func(name, option, value string) {
		t.Helper()
		if _, err := harness.ManagerHandle(t, ctx, cluster, name).AdminSocket(ctx, "config", "set", option, value); err != nil {
			t.Fatal(err)
		}
	}
	logReceipt := func(h *override.DebugLevelHandle) {
		t.Helper()
		data, _ := json.Marshal(h.Receipt())
		t.Logf("receipt: %s", data)
	}
	open := func(name, option, level string) *override.DebugLevelHandle {
		t.Helper()
		h, err := override.SetDebugLevel(ctx, cluster, override.DebugLevelSetting{Daemon: "mgr." + name, Name: option, Level: level})
		if err != nil {
			t.Fatal(err)
		}
		return h
	}

	// A verified window on the active MGR.
	prior := get(active.name, "debug_mgr")
	window := open(active.name, "debug_mgr", "10")
	if r := window.Receipt(); r.State != override.DebugLevelApplied || r.Prior != prior || r.Applied != "10/10" || r.GID != active.gid ||
		!strings.Contains(r.Generation, "/gid=") || get(active.name, "debug_mgr") != "10/10" {
		t.Fatalf("applied receipt %+v", r)
	}
	if _, err := override.SetDebugLevel(ctx, cluster, override.DebugLevelSetting{Daemon: "mgr." + active.name, Name: "debug_mgr", Level: "5"}); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("overlapping window: %v", err)
	}
	if err := window.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	logReceipt(window)
	if window.Receipt().State != override.DebugLevelRestored || get(active.name, "debug_mgr") != prior {
		t.Fatalf("restored receipt %+v", window.Receipt())
	}

	// An outside edit is preserved, then put back by the test itself.
	priorMS := get(standby.name, "debug_ms")
	edited := open(standby.name, "debug_ms", "1")
	set(standby.name, "debug_ms", "3/3")
	if err := edited.Restore(ctx); err == nil || edited.Receipt().State != override.DebugLevelConflict || get(standby.name, "debug_ms") != "3/3" {
		t.Fatalf("restore over an outside edit: %v %+v", err, edited.Receipt())
	}
	logReceipt(edited)
	set(standby.name, "debug_ms", priorMS)

	// A paused MGR is not restored; after it resumes, the retry is.
	paused := open(standby.name, "debug_ms", "1/1")
	pause, err := fault.Pause(ctx, cluster, harness.ManagerHandle(t, ctx, cluster, standby.name))
	if err != nil {
		if pause != nil {
			_ = pause.Resume(ctx)
		}
		t.Fatal(err)
	}
	restoreErr := paused.Restore(ctx)
	if err := pause.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	if restoreErr == nil || !strings.Contains(restoreErr.Error(), "paused") || paused.Receipt().State != override.DebugLevelUnconfirmed || get(standby.name, "debug_ms") != "1/1" {
		t.Fatalf("restore of a paused MGR: %v %+v", restoreErr, paused.Receipt())
	}
	if err := paused.Restore(ctx); err != nil || get(standby.name, "debug_ms") != priorMS {
		t.Fatalf("restore after resume: %v %+v", err, paused.Receipt())
	}
	logReceipt(paused)

	// Failover: the active MGR respawns, the standby takes over in place.
	failed := open(active.name, "debug_mgr", "10/10")
	kept := open(standby.name, "debug_mgr", "10/10")
	priorStandby := kept.Receipt().Prior
	harness.CephCommand(t, ctx, cluster, "mgr", "fail", active.name)
	respawned := waitMGRFailover(t, ctx, cluster, active, standby)
	if err := failed.Restore(ctx); err == nil || failed.Receipt().State != override.DebugLevelTargetChanged {
		t.Fatalf("restore into a respawned MGR: %v %+v", err, failed.Receipt())
	}
	logReceipt(failed)
	for _, transition := range failed.Receipt().Transitions {
		if transition.Readback == "10/10" && transition.State == override.DebugLevelRestored {
			t.Fatalf("a respawned MGR was reported restored: %+v", failed.Receipt())
		}
	}
	// The respawned process runs without the window's level, and nothing
	// was set in it.
	if level := get(active.name, "debug_mgr"); level == "10/10" {
		t.Fatalf("respawned mgr.%s (GID %d) still shows the window's level", active.name, respawned)
	}
	if err := kept.Restore(ctx); err != nil || kept.Receipt().State != override.DebugLevelRestored || get(standby.name, "debug_mgr") != priorStandby {
		t.Fatalf("restore of the promoted standby: %v %+v", err, kept.Receipt())
	}
	logReceipt(kept)
}

type mgrRole struct {
	name string
	gid  uint64
}

// mgrRoles reads the active and the one standby MGR from the MgrMap.
func mgrRoles(t *testing.T, ctx context.Context, cluster *ceph.Container) (active, standby mgrRole) {
	t.Helper()
	seen, err := harness.Observe(ctx, 2*time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) (ceph.ManagerStatus, bool, error) {
		status, err := harness.ObserveCluster(cluster).ManagerStatus(probeCtx)
		if err != nil {
			return status, false, nil
		}
		return status, status.Available && status.ActiveGID != 0 && len(status.Standbys) == 1, nil
	})
	if err != nil {
		t.Fatalf("want an available active MGR and one standby: last %+v: %v", seen.Value, err)
	}
	status := seen.Value
	return mgrRole{status.ActiveName, status.ActiveGID}, mgrRole{status.Standbys[0].Name, status.Standbys[0].GID}
}

// waitMGRFailover waits until the former standby is active with its own GID
// and the failed MGR is back as a standby with a new GID, which it returns.
func waitMGRFailover(t *testing.T, ctx context.Context, cluster *ceph.Container, failed, promoted mgrRole) uint64 {
	t.Helper()
	seen, err := harness.Observe(ctx, 3*time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) (ceph.ManagerStatus, bool, error) {
		status, err := harness.ObserveCluster(cluster).ManagerStatus(probeCtx)
		if err != nil {
			return status, false, nil
		}
		back := false
		for _, standby := range status.Standbys {
			back = back || (standby.Name == failed.name && standby.GID != failed.gid)
		}
		return status, status.ActiveName == promoted.name && status.ActiveGID == promoted.gid && back, nil
	})
	if err != nil {
		t.Fatalf("mgr.%s did not take over from respawned mgr.%s: last %+v: %v", promoted.name, failed.name, seen.Value, err)
	}
	for _, standby := range seen.Value.Standbys {
		if standby.Name == failed.name {
			return standby.GID
		}
	}
	return 0
}
