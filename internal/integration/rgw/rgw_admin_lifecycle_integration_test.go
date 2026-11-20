//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rgw_fixtures))))

//ci: timeout=120m job-timeout=130 image-roles=essential,client,rgw

package rgw_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// rgwAdminShim stands in for radosgw-admin, ahead of it on the gateway's
// PATH. It records its PID and then, by the mode file, sleeps (sleep),
// ignores the stop signals while it sleeps (ignore-term), or ignores them and
// answers after a second (complete). The stop signals are SIGTERM and
// SIGABRT, the first signal at the supervisor's own limit. It sends no
// request to Ceph.
const rgwAdminShim = `#!/bin/sh
mode=$(cat /tmp/tc-shim.mode)
echo $$ > /tmp/tc-shim.pid.tmp && mv /tmp/tc-shim.pid.tmp /tmp/tc-shim.pid
case $mode in
ignore-term) trap '' TERM ABRT; exec sleep 60 ;;
complete) trap '' TERM ABRT; sleep 1; echo '{"shim":true}' ;;
*) exec sleep 60 ;;
esac
`

// TestRGWAdminCommandLifetime checks through the public Admin, in a real
// gateway, that no radosgw-admin outlives its call: a caller that cancels,
// and a deadline shorter than the stop's grace with a command that ignores
// the stop signals, return only after the command is gone. A command that
// ignores them but ends in time keeps its output, and a real radosgw-admin
// runs before and after.
func TestRGWAdminCommandLifetime(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	cluster, _ := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	var terminatingPID atomic.Int64
	stopEvidence := make(chan error, 1)
	observeBeforeRemoval := func(ctx context.Context, ctr testcontainers.Container) error {
		pid := terminatingPID.Swap(0)
		if pid == 0 {
			return nil
		}
		code, reader, err := ctr.Exec(ctx, []string{"sh", "-c", rgwAdminProcessState, "rgw-admin-state", strconv.FormatInt(pid, 10)}, tcexec.Multiplexed())
		if err == nil {
			var out []byte
			out, err = io.ReadAll(reader)
			if err == nil {
				var gone bool
				gone, err = rgwAdminProcessGone(code, string(out))
				if err == nil && !gone {
					err = fmt.Errorf("admin PID %d still runs before gateway removal", pid)
				}
			}
		}
		stopEvidence <- err
		return err
	}
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{SkipUserCreation: true},
		testcontainers.WithAdditionalLifecycleHooks(testcontainers.ContainerLifecycleHooks{
			PreStops: []testcontainers.ContainerHook{observeBeforeRemoval},
		}))
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
		code, out := exec("sh", "-c", rgwAdminProcessState, "rgw-admin-state", strconv.Itoa(pid))
		exited, err := rgwAdminProcessGone(code, out)
		if err != nil {
			t.Fatal(err)
		}
		return exited
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
	t.Logf("a 1.5 s call with a radosgw-admin that ignores its stop signals returned after %s with it gone", took)

	setMode("complete")
	out, err := gateway.Admin(ctx, "user", "stats", "--uid", "tc-shim")
	if err != nil || strings.TrimSpace(string(out)) != `{"shim":true}` {
		t.Fatalf("a command that ignores its stop signals and ends: %q, %v", out, err)
	}
	if code, out := exec("ls", "-A", supervised.MarkerDir); code != 0 || strings.TrimSpace(out) != "" {
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
	result := make(chan error, 1)
	started := time.Now()
	go func() {
		_, err := gateway.Admin(ctx, "user", "stats", "--uid", "tc-shim")
		result <- err
	}()
	terminatingPID.Store(int64(shimPID()))
	terminateCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Minute)
	defer stop()
	// The pre-stop hook observes the process while its container
	// still exists. Deliberately consume Admin's result only after teardown,
	// so a fast removal cannot race a later Docker Exec from this goroutine.
	terminateErr := cluster.Terminate(terminateCtx)
	select {
	case observed := <-stopEvidence:
		if observed != nil {
			t.Fatal(observed)
		}
	default:
		t.Fatal("gateway removal did not record command stop evidence")
	}
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "terminated") {
			t.Fatalf("an Admin call that Terminate ended returned %v", err)
		}
	case <-time.After(time.Minute):
		t.Fatal("Admin did not return after teardown")
	}
	t.Logf("Terminate ended Admin after %s; the pre-stop observation confirmed its process gone", time.Since(started))
	// The test's client still uses the network, which final cleanup removes.
	t.Logf("Terminate: %v", terminateErr)
}

// TestRGWAdminStoppedCommandSaysWhereItWas checks with a real radosgw-admin
// that a command which hangs ends at its caller's bound with an error that
// says where it waited. The cluster's only OSD is paused, so the command's
// first RADOS read never completes; the gateway's supervisor stops it at its
// own limit, and the error names frames of the command's main thread. After
// the OSD resumes, the next command completes.
func TestRGWAdminStoppedCommandSaysWhereItWas(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	cluster, _ := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{SkipUserCreation: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rgwAdminStep(ctx, gateway, "user", "list"); err != nil {
		t.Fatalf("radosgw-admin before the pause: %v", err)
	}
	osds, err := cluster.OSDs(ctx)
	if err != nil || len(osds) != 1 {
		t.Fatalf("the cluster's OSD: %d, %v", len(osds), err)
	}
	pause, err := fault.Pause(ctx, cluster, osds[0])
	if err != nil {
		t.Fatal(err)
	}
	resumed := false
	resume := func() {
		if resumed {
			return
		}
		resumed = true
		resumeCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer stop()
		if err := pause.Resume(resumeCtx); err != nil {
			t.Errorf("resume the OSD: %v", err)
		}
	}
	t.Cleanup(resume)

	// A 30 s bound leaves the supervisor about 15 s, in whole seconds of
	// what remains at the call, before it stops the command.
	const bound = 30 * time.Second
	step, stop := context.WithTimeout(ctx, bound)
	started := time.Now()
	_, err = gateway.Admin(step, "user", "list")
	took := time.Since(started)
	stop()
	const stopped, where = "radosgw-admin still ran after 1", "s and was stopped; when it was stopped, thread radosgw-admin was in "
	if !errors.Is(err, ceph.ErrNativeFailure) || !strings.Contains(err.Error(), stopped) || !strings.Contains(err.Error(), where) || took >= bound {
		t.Fatalf("a radosgw-admin that waits for a paused OSD returned after %s: %v", took, err)
	}
	_, frames, _ := strings.Cut(err.Error(), where)
	if len(strings.Split(frames, " < ")) < 4 || strings.Contains(frames, "[0x") || strings.Contains(frames, "/") {
		t.Fatalf("the stop report is not a bounded list of frames: %q", frames)
	}
	t.Logf("a hung radosgw-admin returned after %s: %v", took, err)

	resume()
	if _, err := rgwAdminStep(ctx, gateway, "user", "list"); err != nil {
		t.Fatalf("radosgw-admin after the OSD resumed: %v", err)
	}
	if code, reader, err := gateway.Exec(ctx, []string{"ls", "-A", supervised.MarkerDir}, tcexec.Multiplexed()); err != nil || code != 0 {
		t.Fatalf("list the supervisor's markers: %d, %v", code, err)
	} else if out, _ := io.ReadAll(reader); strings.TrimSpace(string(out)) != "" {
		t.Fatalf("the supervisor left markers: %q", out)
	}
}
