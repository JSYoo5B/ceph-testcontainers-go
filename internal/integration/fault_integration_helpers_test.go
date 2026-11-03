//go:build all || integration

package integration_test

import (
	"context"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func waitHealthCode(t *testing.T, ctx context.Context, cluster *ceph.Container, fsid, code string, present bool) {
	t.Helper()
	phaseCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	for {
		state := integrationHealthDetails(t, phaseCtx, cluster, fsid, code)
		if check, found := state.Checks[code]; found == present {
			t.Logf("HEALTH_CODE code=%s present=%v details=%q", code, present, check.Details)
			return
		}
		select {
		case <-phaseCtx.Done():
			t.Fatalf("native %s present=%v not observed: %v", code, present, phaseCtx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}
