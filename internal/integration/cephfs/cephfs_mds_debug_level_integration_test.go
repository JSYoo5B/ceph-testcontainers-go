//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_fixtures_admin))))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,cephfs

package cephfs_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

// TestCephFSMDSDebugLevel opens runtime debug level windows on an active and
// a standby MDS and reads every value back through ceph tell, the Messenger
// path that does not use the admin socket the window writes: a verified
// apply and restore that records the GID the MDS reports, a preserved outside
// edit, and a failover by ceph mds fail. The failed MDS respawns in its
// container with a new GID, so its window ends target-changed without a set,
// while the standby that takes rank 0 keeps its process and its window is
// restored.
func TestCephFSMDSDebugLevel(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	m := startCephFSStandbyFailover(t, ctx)
	active, standby := m.MDS, m.standby
	tell := func(name, option string) string {
		t.Helper()
		out, err := m.Cluster.Ceph(ctx, "tell", "mds."+name, "config", "get", option)
		var values map[string]string
		if err != nil || json.Unmarshal(out, &values) != nil || values[option] == "" {
			t.Fatalf("config get %s on mds.%s: %v: %.256s", option, name, err, out)
		}
		return values[option]
	}
	set := func(name, option, value string) {
		t.Helper()
		if _, err := m.Cluster.Ceph(ctx, "tell", "mds."+name, "config", "set", option, value); err != nil {
			t.Fatal(err)
		}
	}
	open := func(name, option, level string) *override.DebugLevelHandle {
		t.Helper()
		h, err := override.SetDebugLevel(ctx, m.Cluster, override.DebugLevelSetting{Daemon: "mds." + name, Name: option, Level: level})
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	logReceipt := func(h *override.DebugLevelHandle) {
		t.Helper()
		data, _ := json.Marshal(h.Receipt())
		t.Logf("receipt: %s", data)
	}

	// A verified window on the active MDS.
	prior := tell(active.Name, "debug_mds")
	window := open(active.Name, "debug_mds", "10")
	if r := window.Receipt(); r.State != override.DebugLevelApplied || r.Prior != prior || r.Applied != "10/10" || r.GID != active.GID || tell(active.Name, "debug_mds") != "10/10" {
		t.Fatalf("applied receipt %+v, active %+v", r, active)
	}
	if _, err := override.SetDebugLevel(ctx, m.Cluster, override.DebugLevelSetting{Daemon: "mds." + active.Name, Name: "debug_mds", Level: "5"}); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("overlapping window: %v", err)
	}
	if err := window.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	logReceipt(window)
	if window.Receipt().State != override.DebugLevelRestored || tell(active.Name, "debug_mds") != prior {
		t.Fatalf("restored receipt %+v", window.Receipt())
	}

	// An outside edit is preserved, then put back by the test itself.
	priorMS := tell(active.Name, "debug_ms")
	edited := open(active.Name, "debug_ms", "1")
	if _, err := m.Cluster.Ceph(ctx, "tell", "mds."+active.Name, "config", "set", "debug_ms", "3/3"); err != nil {
		t.Fatal(err)
	}
	if err := edited.Restore(ctx); err == nil || edited.Receipt().State != override.DebugLevelConflict || tell(active.Name, "debug_ms") != "3/3" {
		t.Fatalf("restore over an outside edit: %v %+v", err, edited.Receipt())
	}
	logReceipt(edited)
	if _, err := m.Cluster.Ceph(ctx, "tell", "mds."+active.Name, "config", "set", "debug_ms", priorMS); err != nil {
		t.Fatal(err)
	}

	// Failover: the active MDS respawns, the standby takes rank 0 in place.
	originalStandby := tell(standby.Name, "debug_mds")
	const activeMarker, standbyMarker, respawnMarker = "2/2", "4/4", "7/7"
	set(active.Name, "debug_mds", activeMarker)
	if got := tell(active.Name, "debug_mds"); got != activeMarker {
		t.Fatalf("active marker read back as %q", got)
	}
	set(standby.Name, "debug_mds", standbyMarker)
	priorStandby := tell(standby.Name, "debug_mds")
	if priorStandby != standbyMarker {
		t.Fatalf("standby marker read back as %q", priorStandby)
	}
	failed := open(active.Name, "debug_mds", "10/10")
	if r := failed.Receipt(); r.Prior != activeMarker || r.GID != active.GID {
		t.Fatalf("active window %+v, independently observed prior %q", r, activeMarker)
	}
	kept := open(standby.Name, "debug_mds", "10/10")
	if r := kept.Receipt(); r.Prior != priorStandby || r.GID != standby.GID || tell(standby.Name, "debug_mds") != "10/10" {
		t.Fatalf("standby window %+v, independently observed prior %q", r, priorStandby)
	}
	if _, err := m.Cluster.Ceph(ctx, "mds", "fail", strconv.FormatUint(active.GID, 10)); err != nil {
		t.Fatal(err)
	}
	respawned := waitMDSRespawned(t, ctx, m.FS, active, standby)
	respawnPrior := tell(active.Name, "debug_mds")
	if respawnPrior == "10/10" {
		t.Fatalf("respawned MDS GID %d kept the old window's level", respawned.GID)
	}
	set(active.Name, "debug_mds", respawnMarker)
	if got := tell(active.Name, "debug_mds"); got != respawnMarker {
		t.Fatalf("respawned MDS marker read back as %q", got)
	}
	if err := failed.Restore(ctx); err == nil || failed.Receipt().State != override.DebugLevelTargetChanged {
		t.Fatalf("restore into a respawned MDS: %v %+v", err, failed.Receipt())
	}
	logReceipt(failed)
	if got := tell(active.Name, "debug_mds"); got != respawnMarker {
		t.Fatalf("restore changed the marker on respawned MDS GID %d: got %q, want %q", respawned.GID, got, respawnMarker)
	}
	set(active.Name, "debug_mds", respawnPrior)
	if err := kept.Restore(ctx); err != nil || kept.Receipt().State != override.DebugLevelRestored || tell(standby.Name, "debug_mds") != priorStandby {
		t.Fatalf("restore of the promoted standby: %v %+v", err, kept.Receipt())
	}
	logReceipt(kept)
	set(standby.Name, "debug_mds", originalStandby)
}

// waitMDSRespawned waits until the former standby serves rank 0 with its own
// GID and the failed MDS is back as an owned standby with a new GID, which it
// returns.
func waitMDSRespawned(t *testing.T, ctx context.Context, fs *cephfs.Filesystem, failed, promoted cephfs.MDSStatus) cephfs.MDSStatus {
	t.Helper()
	var back cephfs.MDSStatus
	seen, err := harness.Observe(ctx, 3*time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) (*cephfs.FilesystemStatus, bool, error) {
		status, err := fs.MDSStatus(probeCtx)
		if err != nil {
			return status, false, nil
		}
		current, ok := activeRank(status, 0)
		back = cephfs.MDSStatus{}
		for _, standby := range status.Standby {
			if standby.Name == failed.Name && standby.GID != failed.GID && standby.Owned {
				back = standby
			}
		}
		return status, ok && current.Name == promoted.Name && current.GID == promoted.GID && back.GID != 0, nil
	})
	if err != nil {
		t.Fatalf("mds.%s did not take rank 0 from respawned mds.%s: last %+v: %v", promoted.Name, failed.Name, seen.Value, err)
	}
	return back
}
