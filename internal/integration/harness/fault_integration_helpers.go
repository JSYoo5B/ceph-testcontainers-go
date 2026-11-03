//go:build all || integration

package harness

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// Helpers shared by fault fixtures that run in separate CI batches.

func MustCeph(t *testing.T, ctx context.Context, cluster *ceph.Container, args ...string) []byte {
	t.Helper()
	data, err := cluster.Ceph(ctx, args...)
	if err != nil {
		t.Fatal(args, err)
	}
	return data
}

func ExecOutput(t *testing.T, ctx context.Context, ctr testcontainers.Container, args ...string) string {
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

// NativeBinaryRelease is the Ceph release that binary in ctr reports with
// --version, the value a role's timing qualification judges, which can differ
// from the cluster's own release when role images are pinned separately.
func NativeBinaryRelease(t *testing.T, ctx context.Context, ctr testcontainers.Container, binary string) string {
	t.Helper()
	words := strings.Fields(ExecOutput(t, ctx, ctr, binary, "--version"))
	if len(words) < 3 || words[0] != "ceph" || words[1] != "version" {
		t.Fatalf("%s --version did not report a Ceph release: %q", binary, strings.Join(words, " "))
	}
	return words[2]
}
