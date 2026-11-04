//go:build all || integration

package integration_test

import (
	"context"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/expect"
)

func waitHealthCode(t *testing.T, ctx context.Context, cluster *ceph.Container, fsid, code string, present bool) {
	t.Helper()
	phaseCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	want := healthCodeExpectation(code, present)
	var last expect.HealthEvaluation
	for {
		state := integrationHealthDetails(t, phaseCtx, cluster, fsid, code)
		result, err := want.Evaluate(state)
		if err != nil {
			t.Fatal(err)
		}
		if result.Matched {
			t.Logf("HEALTH_CODE code=%s present=%v details=%q", code, present, state.Checks[code].Details)
			return
		}
		last = result
		select {
		case <-phaseCtx.Done():
			t.Fatalf("native %s present=%v not observed: %v; last %v", code, present, phaseCtx.Err(), last)
		case <-time.After(2 * time.Second):
		}
	}
}

// healthCodeExpectation states that check code is present or absent,
// whatever its severity, count or mute state.
func healthCodeExpectation(code string, present bool) expect.Health {
	if present {
		return expect.Health{Require: []expect.HealthCheck{{Code: code}}}
	}
	return expect.Health{Forbid: []string{code}}
}
