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
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

// TestOSDDebugLevel opens runtime debug level windows on one surviving OSD
// and checks each state against the daemon's own config get: a verified
// apply and restore, a refused overlapping window, a preserved outside edit,
// a paused daemon that is not restored until it resumes, and a window that
// the finalization restores before Terminate.
func TestOSDDebugLevel(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	cluster, err := ceph.Run(ctx, image, append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1)))...)
	var finalization *finalize.Finalization
	if cluster != nil {
		var registerErr error
		finalization, registerErr = finalize.TerminateOnCleanup(t, cluster, finalize.Config{Archive: true, Scenario: "osd-debug-level"})
		if registerErr != nil {
			_ = cluster.Terminate(context.WithoutCancel(ctx))
			t.Fatal(registerErr)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	get := func(name string) string {
		t.Helper()
		out, err := harness.OSDHandle(t, ctx, cluster, 0).AdminSocket(ctx, "config", "get", name)
		var value map[string]string
		if err != nil || json.Unmarshal(out, &value) != nil || value[name] == "" {
			t.Fatalf("osd.0 config get %s: %q %v", name, out, err)
		}
		return value[name]
	}
	logReceipt := func(h *override.DebugLevelHandle) {
		t.Helper()
		data, _ := json.Marshal(h.Receipt())
		t.Logf("receipt: %s", data)
	}

	// A verified window: the normalized readback, then the prior value.
	priorOSD := get("debug_osd")
	window, err := override.SetDebugLevel(ctx, cluster, override.DebugLevelSetting{Daemon: "osd.0", Name: "debug_osd", Level: "20"})
	if err != nil {
		t.Fatal(err)
	}
	receipt := window.Receipt()
	if receipt.State != override.DebugLevelApplied || receipt.Prior != priorOSD || receipt.Applied != "20/20" || get("debug_osd") != "20/20" {
		t.Fatalf("applied receipt %+v", receipt)
	}
	if _, err := override.SetDebugLevel(ctx, cluster, override.DebugLevelSetting{Daemon: "osd.0", Name: "debug_osd", Level: "5"}); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("overlapping window: %v", err)
	}
	// Some OSD I/O while the level is raised.
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-debug-level"}); err != nil {
		t.Fatal(err)
	}
	if err := window.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	logReceipt(window)
	if window.Receipt().State != override.DebugLevelRestored || get("debug_osd") != priorOSD {
		t.Fatalf("restored receipt %+v", window.Receipt())
	}

	// An outside edit is preserved, then put back by the test itself.
	priorMS := get("debug_ms")
	edited, err := override.SetDebugLevel(ctx, cluster, override.DebugLevelSetting{Daemon: "osd.0", Name: "debug_ms", Level: "1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.OSDHandle(t, ctx, cluster, 0).AdminSocket(ctx, "config", "set", "debug_ms", "3/3"); err != nil {
		t.Fatal(err)
	}
	if err := edited.Restore(ctx); err == nil || edited.Receipt().State != override.DebugLevelConflict || get("debug_ms") != "3/3" {
		t.Fatalf("restore over an outside edit: %v %+v", err, edited.Receipt())
	}
	logReceipt(edited)
	if _, err := harness.OSDHandle(t, ctx, cluster, 0).AdminSocket(ctx, "config", "set", "debug_ms", priorMS); err != nil {
		t.Fatal(err)
	}

	// A paused OSD is not restored; after it resumes, the retry is.
	paused, err := override.SetDebugLevel(ctx, cluster, override.DebugLevelSetting{Daemon: "osd.0", Name: "debug_ms", Level: "1/1"})
	if err != nil {
		t.Fatal(err)
	}
	osd := clusteraccess.OSDs(t, cluster)[0]
	pause, err := fault.Pause(ctx, cluster, osd)
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
	if restoreErr == nil || !strings.Contains(restoreErr.Error(), "paused") || paused.Receipt().State != override.DebugLevelUnconfirmed || get("debug_ms") != "1/1" {
		t.Fatalf("restore of a paused OSD: %v %+v", restoreErr, paused.Receipt())
	}
	if err := paused.Restore(ctx); err != nil || get("debug_ms") != priorMS {
		t.Fatalf("restore after resume: %v %+v", err, paused.Receipt())
	}
	logReceipt(paused)

	// The finalization restores the last window before Terminate; a failure
	// would fail this test.
	last, err := override.SetDebugLevel(ctx, cluster, override.DebugLevelSetting{Daemon: "osd.0", Name: "debug_osd", Level: "10/10"})
	if err != nil {
		t.Fatal(err)
	}
	finalization.Cleanup("restore osd.0 debug_osd", func(ctx context.Context) error {
		err := last.Restore(ctx)
		logReceipt(last)
		if err == nil && last.Receipt().State != override.DebugLevelRestored {
			err = errors.New("restoration was not verified")
		}
		return err
	})
}
