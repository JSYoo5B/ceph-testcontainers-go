//go:build all || integration

package integration_test

import (
	"context"
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// osdSelection selects fixture OSD handles by native ID, for an expectation
// taken before any OSDStates baseline exists.
func osdSelection(osds []*ceph.OSDContainer) []ceph.OSDState {
	selection := make([]ceph.OSDState, len(osds))
	for i, osd := range osds {
		selection[i].ID = osd.ID
	}
	return selection
}

// requireOSDs takes one OSDStates observation and fails the test unless want
// matches it, keeping a failed query apart from a mismatch. It returns the
// evaluated observation.
func requireOSDs(t *testing.T, ctx context.Context, cluster *ceph.Container, what string, want ceph.OSDExpectation) []ceph.OSDState {
	t.Helper()
	states, err := harness.ObserveCluster(cluster).OSDStates(ctx)
	if err != nil {
		t.Fatalf("%s: OSD states unavailable: %v", what, err)
	}
	result, err := want.Evaluate(states)
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	if !result.Matched {
		t.Fatalf("%s: %v", what, result)
	}
	return result.Observed
}
