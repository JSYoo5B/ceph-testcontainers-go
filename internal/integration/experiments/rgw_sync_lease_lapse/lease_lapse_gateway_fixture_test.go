//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_sync_lease_lapse_test

import (
	"strings"
	"testing"
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
