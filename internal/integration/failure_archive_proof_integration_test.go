//go:build integration && ci_optional && ci_batch_failure_archive

//ci: timeout=15m job-timeout=25 image-roles=essential

package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

// TestFailureArchiveProof deliberately fails real tests, not recorder stand-ins.
// Only an explicit optional batch selects it; neither all nor required CI does.
// Keep its failed process/job result when inspecting the uploaded archive.
func TestFailureArchiveProof(t *testing.T) {
	for _, mode := range []string{"assertion", "verification"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
			defer cancel()
			image, opts := integrationImages(t)
			cluster, err := ceph.Run(ctx, opts.WithOptions(ceph.WithOSDCount(1), ceph.WithPoolDefaults(1, 1)))
			var finalization *ceph.Finalization
			if cluster != nil {
				var registerErr error
				finalization, registerErr = cluster.TerminateOnCleanup(t, ceph.FinalizationConfig{
					Archive: true, Scenario: "failure-archive-proof/" + mode,
					ArchiveTimeout: 2 * time.Minute,
					Diagnostics:    ceph.DiagnosticsConfig{MaxOutputBytes: 128 << 10, LogTail: 100},
				})
				if registerErr != nil {
					_ = cluster.Terminate(context.WithoutCancel(ctx))
					t.Fatal(registerErr)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			fsid, err := cluster.Ceph(ctx, "fsid", "--format=json")
			if err != nil {
				t.Fatal(err)
			}
			var identity struct {
				FSID string `json:"fsid"`
			}
			if err := json.Unmarshal(fsid, &identity); err != nil || identity.FSID == "" {
				t.Fatalf("native FSID: %q %v", fsid, err)
			}
			out, err := cluster.Ceph(ctx, "osd", "ls", "--format=json")
			if err != nil {
				t.Fatal(err)
			}
			var nativeOSDs []int
			if err := json.Unmarshal(out, &nativeOSDs); err != nil || len(nativeOSDs) != 1 || nativeOSDs[0] != 0 {
				t.Fatalf("native one-OSD control: %q %v", out, err)
			}
			window, applyErr := override.SetDebugLevel(ctx, cluster, ceph.DebugLevelSetting{Daemon: "osd.0", Name: "debug_osd", Level: "1/1"})
			finalization.TrackDebugLevel(window)
			finalization.Cleanup("restore controlled debug window", window.Restore)
			if applyErr != nil {
				t.Fatal(applyErr)
			}
			if mode == "verification" {
				if _, err := finalization.Verify(ctx, ceph.VerificationConfig{
					Name:    "absent-crash-control",
					Crashes: []ceph.CrashExpectation{{Name: "required-but-absent", Required: true, Daemon: "osd.0", Process: "ceph-osd"}},
				}); err != nil {
					t.Fatal(err)
				}
			}
			ready, err := json.Marshal(map[string]any{
				"mode": mode, "fsid": identity.FSID, "osds": nativeOSDs,
				"osd_container": clusteraccess.OSDs(t, cluster)[0].GetContainerID(), "ceph_version": staticFacts(cluster).CephVersion,
				"debug_state": window.Receipt().State,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("FAILURE_ARCHIVE_PROOF_READY %s", ready)
			// The finalization must collect and restore using fresh contexts.
			cancel()
			if mode == "assertion" && len(nativeOSDs) != 2 {
				t.Errorf("CONTROLLED_ARCHIVE_ASSERTION: native OSD count = %d, deliberately want 2", len(nativeOSDs))
			}
		})
	}
}
