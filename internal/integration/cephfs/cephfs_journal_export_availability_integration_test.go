//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_journal_export_availability))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,cephfs,rbd-aux,rgw

package cephfs_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"os"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/testcontainers/testcontainers-go"
)

// The first real-export characterization needs `cephfs-journal-tool` in an
// existing image role. This scenario asks every published role image whether
// it has the tool, read-only, with no cluster. The recorded answer is the
// result: absent in all roles, present in some, or inconclusive. Only an
// inconclusive answer fails the test, because then the question was not
// answered. When the tool is absent the journal export is not attempted and
// no image is invented; when it is present the export is still not attempted
// by this slice and the receipt says so.
func TestCephFSJournalExportToolAvailability(t *testing.T) {
	testCephFSJournalExportToolAvailability(t)
}

var jeRoleEnv = []struct{ role, env string }{
	{"essential", "CEPH_TEST_ESSENTIAL_IMAGE"},
	{"client", "CEPH_TEST_CLIENT_IMAGE"},
	{"cephfs", "CEPH_TEST_CEPHFS_IMAGE"},
	{"rbd-aux", "CEPH_TEST_RBD_AUX_IMAGE"},
	{"rgw", "CEPH_TEST_RGW_IMAGE"},
}

func testCephFSJournalExportToolAvailability(t *testing.T) {
	var receipts []harness.JeRoleReceipt
	overall := func() {
		verdict, reason := harness.JeOverall(receipts)
		data, err := json.Marshal(map[string]any{
			"kind":             "cephfs-journal-export-availability",
			"verdict":          verdict,
			"reason":           reason,
			"export_attempted": false,
			"roles":            receipts,
		})
		if err != nil {
			t.Errorf("encode availability receipt: %v", err)
			return
		}
		t.Logf("journal-export-availability-receipt %s", data)
	}
	// Emitted on every path, before any later cleanup of this test.
	defer overall()

	var firstErr error
	for _, entry := range jeRoleEnv {
		receipt, err := jeProbeRole(t, entry.role, entry.env)
		receipts = append(receipts, receipt)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		t.Errorf("first probe failure: %v", firstErr)
	}
	if verdict, reason := harness.JeOverall(receipts); verdict == harness.JeInconclusive {
		t.Fatalf("availability not established: %s", reason)
	}
}

// jeProbeRole starts an idle container of one role image, runs the read-only
// probe under supervision, logs the role receipt, and only then removes the
// container. The returned error is the probe failure, never the cleanup one.
func jeProbeRole(t *testing.T, role, env string) (receipt harness.JeRoleReceipt, probeErr error) {
	t.Helper()
	receipt = harness.JeRoleReceipt{Role: role, Verdict: harness.JeInconclusive}
	image := os.Getenv(env)
	if image == "" {
		receipt.Reason = env + " is not set"
		t.Logf("journal-export-availability-role %s", jeJSON(receipt))
		return receipt, errors.New(receipt.Reason)
	}
	receipt.Configured, receipt.ImageRef = true, image

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	ctr, err := testcontainers.Run(ctx, image, ceph.WithIdleEntrypoint())
	defer func() {
		if terr := testcontainers.TerminateContainer(ctr); terr != nil {
			t.Errorf("remove %s probe container: %v", role, terr)
		}
	}()
	// Registered last, so it runs first: the role receipt precedes cleanup.
	defer func() { t.Logf("journal-export-availability-role %s", jeJSON(receipt)) }()
	if err != nil {
		receipt.Reason = fmt.Sprintf("start container: %v", err)
		return receipt, errors.New(receipt.Reason)
	}
	if inspect, err := ctr.Inspect(ctx); err == nil && inspect != nil {
		receipt.ImageID = inspect.Image
	}

	report, err := supervised.RunShell(ctx, ctr, "journal tool probe", 120, "sh", "-c", harness.JeProbeScript)
	if err != nil {
		receipt.Reason = fmt.Sprintf("run probe: %v", err)
		return receipt, errors.New(receipt.Reason)
	}
	stdout := string(report.Stdout)
	receipt.ExitCode, receipt.Stdout, receipt.Stderr = report.ExitCode, harness.JeClip(stdout), harness.JeClip(string(report.Stderr))
	receipt.StdoutSHA256 = harness.JeHash(stdout)
	parsed := harness.JeParseProbe(stdout, report.ExitCode, report.Stopped)
	receipt.Verdict, receipt.Reason = parsed.Verdict, parsed.Reason
	receipt.Tools, receipt.Hits = parsed.Tools, parsed.Hits
	receipt.CephVersion, receipt.CephCommit, receipt.CephRelease = parsed.CephVersion, parsed.CephCommit, parsed.CephRelease
	if parsed.Verdict == harness.JeInconclusive {
		return receipt, errors.New(role + ": " + parsed.Reason)
	}
	return receipt, nil
}

func jeJSON(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("{\"encode_error\":%q}", err.Error())
	}
	return string(data)
}
