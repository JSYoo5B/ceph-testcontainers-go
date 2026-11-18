//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_daemon_basics))))

//ci: timeout=45m job-timeout=55 image-roles=essential,client,rbd-aux

package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/finalize"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

// TestVerificationGenerationRestart observes a native restart between
// activation and evaluation. Deterministic unit tests cover transitions during
// evidence reads; this scenario does not claim to reproduce those race windows.
func TestVerificationGenerationRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	cluster, err := ceph.Run(ctx, image, append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1)))...)
	recorder := &jsonFileFinalizer{TB: t}
	var finalization *finalize.Finalization
	var owned []string
	wantIncomplete := false
	if cluster != nil {
		var registerErr error
		finalization, registerErr = finalize.TerminateOnCleanup(recorder, cluster, finalize.Config{Scenario: "verification-generation-restart"})
		if cluster.Container != nil {
			owned = append(owned, cluster.GetContainerID())
		}
		t.Cleanup(func() {
			if len(owned) != 0 {
				verificationAssertRemoved(t, owned)
			}
		})
		if registerErr != nil {
			cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
			defer cancelCleanup()
			if err := cluster.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate cluster after finalization registration failed: %v", err)
			}
			t.Fatal(registerErr)
		}
		t.Cleanup(func() {
			recorder.finalize()
			expectRestartVerification(t, recorder.failures(), wantIncomplete)
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	owned = append(owned, clusteraccess.ControlContainer(t, cluster).GetContainerID())
	for _, mgr := range clusteraccess.Managers(t, cluster) {
		owned = append(owned, mgr.GetContainerID())
	}
	osd := clusteraccess.OSDs(t, cluster)[0]
	owned = append(owned, osd.GetContainerID())
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	// Keep the OSD registered during the restart so verification can isolate
	// the evidence gap caused by the container generation change.
	nodown, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoDown, true)
	finalization.Cleanup("restore nodown", nodown.Restore)
	if err != nil {
		t.Fatal(err)
	}
	verification, err := finalization.Verify(ctx, finalize.VerificationConfig{Name: "restart"})
	if err != nil {
		t.Fatal(err)
	}
	before, err := verification.Evaluate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Logs.Gaps) != 0 || len(before.Crashes.Gaps) != 0 {
		t.Fatalf("unchanged generation has coverage gaps: logs=%v crashes=%v", before.Logs.Gaps, before.Crashes.Gaps)
	}
	grace := 5 * time.Second
	if err := osd.Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
	if err := osd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := verification.Evaluate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for name, gaps := range map[string][]finalize.VerificationGap{"logs": after.Logs.Gaps, "crashes": after.Crashes.Gaps} {
		found := false
		for _, gap := range gaps {
			if gap.Source == "osd.0" && strings.Contains(gap.Reason, "generation") {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s has no restart coverage gap: %v", name, gaps)
		}
	}
	if after.Outcome != finalize.VerificationIncomplete {
		t.Fatalf("restarted activation source reported %s, want incomplete: logs=%+v crashes=%+v", after.Outcome, after.Logs.Findings, after.Crashes.Findings)
	}
	wantIncomplete = true
	t.Log("unchanged native generation has no coverage gaps; restarted OSD has log and crash generation gaps")
}

// expectRestartVerification accepts only the incomplete result from the
// restart verification. Other checks and termination must still succeed.
func expectRestartVerification(t *testing.T, failures []string, wantIncomplete bool) {
	t.Helper()
	const expected = "ceph finalization check verification restart: post-test verification restart: incomplete; "
	for _, failure := range failures {
		if wantIncomplete && strings.HasPrefix(failure, expected) {
			wantIncomplete = false
			continue
		}
		t.Errorf("unexpected finalization failure: %s", failure)
	}
	if wantIncomplete {
		t.Error("finalization did not report incomplete restart verification")
	}
}
