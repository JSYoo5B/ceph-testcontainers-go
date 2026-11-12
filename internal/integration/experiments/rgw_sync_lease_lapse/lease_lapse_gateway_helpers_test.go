//go:build all || integration

package rgw_sync_lease_lapse_test

import (
	"fmt"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// gatewayRun is what the engine reports of a gateway container.
type gatewayRun struct {
	Running  bool
	Paused   bool
	Status   string
	ExitCode int
}

// judgeGateway reads the engine's report of a zone's gateway container after
// a pause (wantPaused) or a resume was asked for. owed says whether the
// container is frozen and so still needs a resume. The error is nil only for
// a running container in the state asked for: a container that stopped is
// not frozen and owes nothing, but it is neither a frozen gateway nor one
// that answers again, so a round cannot go on with it.
func judgeGateway(zone string, wantPaused bool, got gatewayRun) (owed bool, err error) {
	switch {
	case !got.Running && !got.Paused:
		return false, fmt.Errorf("condition not established: the gateway container of zone %s is not running (%s, exit code %d)", zone, got.Status, got.ExitCode)
	case got.Paused != wantPaused:
		return got.Paused, fmt.Errorf("the gateway container of zone %s is paused=%t (%s), want paused=%t", zone, got.Paused, got.Status, wantPaused)
	}
	return got.Paused, nil
}

// reportLeaseLapse records the observed symptom with the diagnostic of a
// gateway restart and concludes the unit. The restart is a requested
// operation, not an observation: when it fails, the symptom and its verdict
// stand and t fails on its own, so a matching verdict cannot hide a gateway
// left stopped.
func reportLeaseLapse(t harness.ReproductionT, repro *harness.Reproduction, symptom string, restart func() (string, error)) {
	t.Helper()
	diagnostic, err := restart()
	if err != nil {
		diagnostic = "the restart failed: " + err.Error()
	}
	repro.Symptom(t, "lapse", "%s. After a restart of zone b's gateway: %s", symptom, diagnostic)
	repro.Conclude()
	if err != nil {
		t.Errorf("restart after the observed defect: %v", err)
	}
}
