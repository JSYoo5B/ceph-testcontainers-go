//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rgw_fixtures))))

//ci: timeout=120m job-timeout=130 image-roles=essential,client,rgw

package rgw_test

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// rgwAdminShim stands in for radosgw-admin, ahead of it on the gateway's
// PATH. It records its PID and then, by the mode file, sleeps (sleep),
// ignores SIGTERM while it sleeps (ignore-term), or ignores SIGTERM and
// answers after a second (complete). It sends no request to Ceph.
const rgwAdminShim = `#!/bin/sh
mode=$(cat /tmp/tc-shim.mode)
echo $$ > /tmp/tc-shim.pid.tmp && mv /tmp/tc-shim.pid.tmp /tmp/tc-shim.pid
case $mode in
ignore-term) trap '' TERM; exec sleep 60 ;;
complete) trap '' TERM; sleep 1; echo '{"shim":true}' ;;
*) exec sleep 60 ;;
esac
`

// TestRGWAdminCommandLifetime checks through the public Admin, in a real
// gateway, that no radosgw-admin outlives its call: a caller that cancels,
// and a deadline shorter than SIGTERM's grace with a command that ignores
// SIGTERM, return only after the command is gone. A command that ignores
// SIGTERM but ends in time keeps its output, and a real radosgw-admin runs
// before and after.
func TestRGWAdminCommandLifetime(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	cluster, _ := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{SkipUserCreation: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.Admin(ctx, "user", "list"); err != nil {
		t.Fatalf("real radosgw-admin: %v", err)
	}
	exec := func(args ...string) (int, string) {
		t.Helper()
		execCtx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		code, r, err := gateway.Exec(execCtx, args, tcexec.Multiplexed())
		if err != nil {
			t.Fatal(err)
		}
		out, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		return code, string(out)
	}
	if err := gateway.CopyToContainer(ctx, []byte(rgwAdminShim), "/usr/local/sbin/radosgw-admin", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, out := exec("sh", "-c", "command -v radosgw-admin"); strings.TrimSpace(out) != "/usr/local/sbin/radosgw-admin" {
		t.Fatalf("the shim is not first on the gateway's PATH: %q", out)
	}
	setMode := func(mode string) {
		t.Helper()
		if code, out := exec("sh", "-c", "rm -f /tmp/tc-shim.pid && echo "+mode+" > /tmp/tc-shim.mode"); code != 0 {
			t.Fatalf("set the shim mode: %s", out)
		}
	}
	shimPID := func() int {
		t.Helper()
		for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
			if code, out := exec("cat", "/tmp/tc-shim.pid"); code == 0 {
				if pid, err := strconv.Atoi(strings.TrimSpace(out)); err == nil {
					return pid
				}
			}
		}
		t.Fatal("the shim did not start")
		return 0
	}
	// /proc/<pid>/stat names a live process, or a zombie (Z) that ended.
	gone := func(pid int) bool {
		t.Helper()
		code, out := exec("sh", "-c", "awk '{sub(/.*\\) /, \"\"); print $1}' /proc/"+strconv.Itoa(pid)+"/stat 2>/dev/null")
		return code != 0 || strings.TrimSpace(out) == "" || strings.TrimSpace(out) == "Z"
	}
	call := func(ctx context.Context, act func()) (int, time.Duration, error) {
		t.Helper()
		started := time.Now()
		result := make(chan error, 1)
		go func() {
			_, err := gateway.Admin(ctx, "user", "stats", "--uid", "tc-shim", "--sync-stats")
			result <- err
		}()
		pid := shimPID()
		act()
		select {
		case err := <-result:
			return pid, time.Since(started), err
		case <-time.After(time.Minute):
			t.Fatal("Admin did not return")
			return pid, 0, nil
		}
	}

	setMode("sleep")
	callerCtx, cancelCaller := context.WithTimeout(ctx, 30*time.Second)
	pid, took, err := call(callerCtx, cancelCaller)
	if !errors.Is(err, context.Canceled) || !gone(pid) {
		t.Fatalf("a canceled call returned %v after %s with the command gone %t", err, took, gone(pid))
	}
	t.Logf("a canceled call returned after %s with its radosgw-admin gone", took)

	setMode("ignore-term")
	shortCtx, cancelShort := context.WithTimeout(ctx, 1500*time.Millisecond)
	pid, took, err = call(shortCtx, func() {})
	cancelShort()
	if !errors.Is(err, context.DeadlineExceeded) || !gone(pid) {
		t.Fatalf("a 1.5 s call returned %v after %s with the command gone %t", err, took, gone(pid))
	}
	t.Logf("a 1.5 s call with a radosgw-admin that ignores SIGTERM returned after %s with it gone", took)

	setMode("complete")
	out, err := gateway.Admin(ctx, "user", "stats", "--uid", "tc-shim")
	if err != nil || strings.TrimSpace(string(out)) != `{"shim":true}` {
		t.Fatalf("a command that ignores SIGTERM and ends: %q, %v", out, err)
	}
	if code, out := exec("sh", "-c", "ls -A /tmp/tc-supervised-sh"); code != 0 || strings.TrimSpace(out) != "" {
		t.Fatalf("the supervisor left markers: %q", out)
	}
	if code, out := exec("rm", "/usr/local/sbin/radosgw-admin"); code != 0 {
		t.Fatalf("remove the shim: %s", out)
	}
	if _, err := gateway.Admin(ctx, "user", "list"); err != nil {
		t.Fatalf("real radosgw-admin after the shim: %v", err)
	}

	// Terminate reaches a running Admin whose own caller stays alive: the
	// call returns, its command stopped, before the teardown removes the
	// gateway.
	if err := gateway.CopyToContainer(ctx, []byte(rgwAdminShim), "/usr/local/sbin/radosgw-admin", 0o755); err != nil {
		t.Fatal(err)
	}
	setMode("ignore-term")
	terminated := make(chan error, 1)
	pid, took, err = call(ctx, func() {
		go func() {
			terminateCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Minute)
			defer stop()
			terminated <- cluster.Terminate(terminateCtx)
		}()
	})
	if err == nil || !strings.Contains(err.Error(), "terminated") {
		t.Fatalf("an Admin call that Terminate ended returned %v after %s", err, took)
	}
	// Terminate stops the command before it waits for the cluster mutex
	// that Admin held, so the gateway still runs when Admin returns.
	select {
	case err := <-terminated:
		t.Fatalf("Terminate returned before the gateway was checked: %v", err)
	default:
	}
	if !gone(pid) {
		t.Fatal("Admin returned while its radosgw-admin still ran")
	}
	t.Logf("Terminate ended a running Admin after %s with its radosgw-admin gone", took)
	// The test's client container still uses the cluster network, so this
	// Terminate may leave the network to the cleanup's.
	t.Logf("Terminate: %v", <-terminated)
}
