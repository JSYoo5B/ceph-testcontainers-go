//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_sync_lease_lapse_test

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// These cases feed synthetic engine reports to judgeGateway. They are not
// native evidence of what an engine reports.
func TestJudgeGatewayTellsAStoppedContainerFromAResumedOne(t *testing.T) {
	running := gatewayRun{Running: true, Status: "running"}
	frozen := gatewayRun{Running: true, Paused: true, Status: "paused"}
	exited := gatewayRun{Status: "exited", ExitCode: 137}
	for _, tc := range []struct {
		name       string
		wantPaused bool
		got        gatewayRun
		owed       bool
		err        string
	}{
		{name: "paused as asked", wantPaused: true, got: frozen, owed: true},
		{name: "resumed as asked", got: running},
		{name: "still paused after a resume", got: frozen, owed: true, err: "is paused=true (paused), want paused=false"},
		{name: "not paused after a pause", wantPaused: true, got: running, err: "is paused=false (running), want paused=true"},
		// An engine rejects the unpause of a container that exited and
		// reports it as neither running nor paused.
		{name: "exited before a resume", got: exited, err: "condition not established: the gateway container of zone b is not running (exited, exit code 137)"},
		{name: "exited before a pause", wantPaused: true, got: exited, err: "condition not established: the gateway container of zone b is not running (exited, exit code 137)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owed, err := judgeGateway("b", tc.wantPaused, tc.got)
			if owed != tc.owed {
				t.Errorf("owed %t, want %t", owed, tc.owed)
			}
			if tc.err == "" && err != nil || tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err)) {
				t.Errorf("error %v, want %q", err, tc.err)
			}
		})
	}
}

// leaseLapseT records what reportLeaseLapse does to its test.
type leaseLapseT struct{ logs, errors []string }

func (f *leaseLapseT) Helper() {}
func (f *leaseLapseT) Logf(format string, args ...any) {
	f.logs = append(f.logs, fmt.Sprintf(format, args...))
}
func (f *leaseLapseT) Errorf(format string, args ...any) {
	f.errors = append(f.errors, fmt.Sprintf(format, args...))
}
func (f *leaseLapseT) FailNow()       { panic("FailNow") }
func (f *leaseLapseT) Cleanup(func()) {}
func (f *leaseLapseT) Skipped() bool  { return false }
func (f *leaseLapseT) Name() string   { return "TestMultiClusterRGWDataSyncAfterSyncLeaseLapse" }

// A matching REPRODUCED verdict followed by a failed restart keeps the
// verdict and still fails the test for the gateway operation.
func TestReportLeaseLapseFailsAFailedRestartBesideAMatchingVerdict(t *testing.T) {
	data, err := os.ReadFile(harness.ReproductionsFile)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := harness.ParseReproductionCatalog(data)
	if err != nil {
		t.Fatal(err)
	}
	affected := catalog.Combinations[0]
	if affected.Verdicts["rgw_sync_lease_lapse"] != harness.Reproduced {
		t.Fatalf("the first combination %s does not expect a reproduction", affected.ID)
	}
	env := map[string]string{}
	for role, variable := range map[string]string{"essential": "CEPH_TEST_ESSENTIAL_IMAGE", "client": "CEPH_TEST_CLIENT_IMAGE", "rgw": "CEPH_TEST_RGW_IMAGE"} {
		env[variable] = affected.Images[role]
	}
	for _, c := range []struct {
		name    string
		restart func() (string, error)
		errors  int
	}{
		{"restarted", func() (string, error) { return "caught up true after 4s", nil }, 0},
		{"start failed", func() (string, error) { return "", errors.New("start zone b's gateway: container exited") }, 1},
	} {
		f := &leaseLapseT{}
		repro := harness.NewReproduction(f, catalog, func(name string) string { return env[name] }, affected.Platform, "rgw_sync_lease_lapse", "lapse")
		reportLeaseLapse(f, repro, "zone b is behind on shards [2]", c.restart)
		line := ""
		for _, l := range f.logs {
			if strings.HasPrefix(l, harness.VerdictLinePrefix) {
				line = l
			}
		}
		if !strings.Contains(line, "verdict=REPRODUCED expected=REPRODUCED") {
			t.Errorf("%s: verdict line %q", c.name, line)
		}
		if len(f.errors) != c.errors || (c.errors > 0 && !strings.Contains(f.errors[0], "container exited")) {
			t.Errorf("%s: failures %q, want %d naming the restart", c.name, f.errors, c.errors)
		}
	}
}
