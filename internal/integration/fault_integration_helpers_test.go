//go:build all || integration

package integration_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// Helpers shared by fault fixtures that run in separate CI batches.

func mustCeph(t *testing.T, ctx context.Context, cluster *ceph.Container, args ...string) []byte {
	t.Helper()
	data, err := cluster.Ceph(ctx, args...)
	if err != nil {
		t.Fatal(args, err)
	}
	return data
}

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

func execOutput(t *testing.T, ctx context.Context, ctr testcontainers.Container, args ...string) string {
	t.Helper()
	execCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	code, r, err := ctr.Exec(execCtx, args, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("%s exited %d: %s", args[0], code, out)
	}
	return strings.TrimSpace(string(out))
}
