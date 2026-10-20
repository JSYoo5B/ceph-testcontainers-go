package supervised

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// installedPython returns the host's python3, which runs the exact scripts
// without Docker.
func installedPython(t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("POSIX pipes and process groups are unavailable on Windows")
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("local Python supervisor regression requires installed python3; native image validation covers the runtime")
	}
	return python
}

// supervisorInvocation is one production supervisor running on the test host
// with a child that publishes its PID and then waits, so that only a cancel
// or the supervisor's deadline ends it. The child expires on its own after
// 20 seconds, which bounds a failed test without signaling a recorded PID.
type supervisorInvocation struct {
	marker, pidPath string
	cmd             *exec.Cmd
	// done is closed when the supervisor has exited.
	done chan struct{}
}

const supervisorWaitingChild = `import os,sys,time
deadline=time.monotonic()+20
with open(sys.argv[1]+'.tmp','w') as f: f.write(str(os.getpid()))
os.rename(sys.argv[1]+'.tmp',sys.argv[1])
while time.monotonic()<deadline: time.sleep(0.01)
`

func startSupervisor(t *testing.T, python, dir, name string) *supervisorInvocation {
	t.Helper()
	return startSupervisorScript(t, python, Script, dir, name)
}

func startSupervisorScript(t *testing.T, python, script, dir, name string) *supervisorInvocation {
	t.Helper()
	invocation := &supervisorInvocation{marker: filepath.Join(dir, "markers", name), pidPath: filepath.Join(dir, name+".pid"), done: make(chan struct{})}
	invocation.cmd = exec.Command(python, "-c", script, "4096", "1024", "30", python, "-c", supervisorWaitingChild, invocation.pidPath)
	invocation.cmd.Env = append(os.Environ(), MarkerVariable+"="+invocation.marker)
	if err := invocation.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = invocation.cmd.Wait()
		close(invocation.done)
	}()
	t.Cleanup(func() {
		select {
		case <-invocation.done:
		case <-time.After(30 * time.Second):
			t.Errorf("supervisor %s did not exit", name)
		}
	})
	return invocation
}

// childPID waits until the invocation's child has published its PID.
func (invocation *supervisorInvocation) childPID(t *testing.T) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if pid, err := os.ReadFile(invocation.pidPath); err == nil {
			return string(pid)
		}
		if time.Now().After(deadline) {
			t.Fatal("supervised child did not start")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// exited waits for the supervisor to exit and checks that it left no marker.
func (invocation *supervisorInvocation) exited(t *testing.T, within time.Duration) {
	t.Helper()
	select {
	case <-invocation.done:
	case <-time.After(within):
		t.Fatalf("supervisor did not exit within %s", within)
	}
	for _, path := range []string{invocation.marker, invocation.marker + ".cancel"} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("supervisor left %s: %v", path, err)
		}
	}
}

func runSupervisorCancel(t *testing.T, python, marker string) {
	t.Helper()
	output, err := exec.Command(python, "-c", CancelScript, marker, "5").CombinedOutput()
	if err != nil {
		t.Fatalf("canceler failed: %v; %s", err, output)
	}
}

func supervisedProcessExists(t *testing.T, python, pid string) bool {
	t.Helper()
	probe, err := exec.Command(python, "-c", "import os,sys\ntry:\n os.kill(int(sys.argv[1]),0); print('alive')\nexcept ProcessLookupError: print('reaped')", pid).Output()
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(probe)) == "alive"
}

// A cancel that claims the marker first is final: a supervisor that Docker
// starts afterwards starts no command and removes the claim.
func TestSupervisorCanceledBeforeStartRunsNothing(t *testing.T) {
	python := installedPython(t)
	dir := t.TempDir()
	marker := filepath.Join(dir, "markers", "late")
	runSupervisorCancel(t, python, marker)
	late := &supervisorInvocation{marker: marker, pidPath: filepath.Join(dir, "late.pid"), done: make(chan struct{})}
	late.cmd = exec.Command(python, "-c", Script, "4096", "1024", "30", python, "-c", supervisorWaitingChild, late.pidPath)
	late.cmd.Env = append(os.Environ(), MarkerVariable+"="+marker)
	output, err := late.cmd.CombinedOutput()
	close(late.done)
	if err != nil || len(output) != 0 {
		t.Fatalf("late supervisor: %v; %q", err, output)
	}
	late.exited(t, time.Second)
	if _, err := os.Stat(late.pidPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("a supervisor started after its cancellation ran its command")
	}
}

// Canceling one invocation, including one that has already finished, leaves
// every other invocation running. The canceler signals nothing itself: the
// owning supervisor kills its unreaped child's group, and the cancel returns
// only after that child has been reaped.
func TestSupervisorCancelStopsOnlyItsOwnInvocation(t *testing.T) {
	python := installedPython(t)
	dir := t.TempDir()
	// An invocation that has already ended, whose child's PID is free for
	// reuse by the invocations that start after it.
	finished := startSupervisor(t, python, dir, "finished")
	finished.childPID(t)
	runSupervisorCancel(t, python, finished.marker)
	finished.exited(t, 5*time.Second)
	a, b := startSupervisor(t, python, dir, "a"), startSupervisor(t, python, dir, "b")
	pidA, pidB := a.childPID(t), b.childPID(t)

	runSupervisorCancel(t, python, finished.marker)
	if !supervisedProcessExists(t, python, pidA) || !supervisedProcessExists(t, python, pidB) {
		t.Fatal("canceling a finished invocation touched a running one")
	}
	runSupervisorCancel(t, python, a.marker)
	if supervisedProcessExists(t, python, pidA) {
		t.Fatal("cancel returned before the owning supervisor reaped its child")
	}
	if !supervisedProcessExists(t, python, pidB) {
		t.Fatal("canceling one invocation stopped another")
	}
	a.exited(t, 5*time.Second)
	runSupervisorCancel(t, python, b.marker)
	if supervisedProcessExists(t, python, pidB) {
		t.Fatal("cancel returned before the owning supervisor reaped its child")
	}
	b.exited(t, 5*time.Second)
}

// A cancel that races the supervisor's start, before or after it claims the
// marker and starts its child, always ends with no command left running.
func TestSupervisorCancelRacingStartupLeavesNothingRunning(t *testing.T) {
	python := installedPython(t)
	dir := t.TempDir()
	started := 0
	for attempt := range 20 {
		name := "race-" + strconv.Itoa(attempt)
		invocation := startSupervisor(t, python, dir, name)
		time.Sleep(time.Duration(attempt%5) * 10 * time.Millisecond)
		runSupervisorCancel(t, python, invocation.marker)
		if pid, err := os.ReadFile(invocation.pidPath); err == nil && supervisedProcessExists(t, python, string(pid)) {
			t.Fatalf("attempt %d: cancel returned while child %s runs", attempt, pid)
		}
		invocation.exited(t, 5*time.Second)
		pid, err := os.ReadFile(invocation.pidPath)
		if err == nil && supervisedProcessExists(t, python, string(pid)) {
			t.Fatalf("attempt %d: child %s runs after its supervisor exited", attempt, pid)
		}
		if err == nil {
			started++
		}
	}
	t.Logf("the child had started in %d of 20 canceled attempts", started)
}

// A program that cannot start ends the supervisor with LaunchFailedExit and
// leaves no marker.
func TestSupervisorReportsAProgramItCannotStart(t *testing.T) {
	python := installedPython(t)
	dir := t.TempDir()
	marker := filepath.Join(dir, "markers", "missing")
	for _, program := range []string{filepath.Join(dir, "missing-program"), dir} {
		cmd := exec.Command(python, "-c", Script, "4096", "1024", "30", program)
		cmd.Env = append(os.Environ(), MarkerVariable+"="+marker)
		output, err := cmd.Output()
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != LaunchFailedExit || len(output) != 0 {
			t.Fatalf("%s: supervisor returned %v with %q", program, err, output)
		}
		for _, path := range []string{marker, marker + ".cancel"} {
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("supervisor left %s: %v", path, err)
			}
		}
	}
}

// A command that the supervisor cannot fork, for want of a process, ends it
// with LaunchUnavailableExit, not as a program that it cannot start.
func TestSupervisorReportsAnUnavailableLaunch(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("RLIMIT_NPROC does not bind root")
	}
	python := installedPython(t)
	marker := filepath.Join(t.TempDir(), "markers", "unavailable")
	// The supervisor runs with a process limit that its fork exceeds.
	limited := "import os,resource; resource.setrlimit(resource.RLIMIT_NPROC,(1,1)); exec(os.environ['TC_TEST_SCRIPT'])"
	cmd := exec.Command(python, "-c", limited, "4096", "1024", "30", "true")
	cmd.Env = append(os.Environ(), MarkerVariable+"="+marker, "TC_TEST_SCRIPT="+Script)
	output, err := cmd.Output()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != LaunchUnavailableExit || len(output) != 0 {
		t.Fatalf("supervisor returned %v with %q", err, output)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("supervisor left its marker: %v", err)
	}
}

// fakeProc gives the scripts a /proc below dir with one container instance:
// a boot ID and the start time of PID 1, which restart changes.
type fakeProc struct {
	dir    string
	starts int
}

func newFakeProc(t *testing.T, dir string) *fakeProc {
	t.Helper()
	proc := &fakeProc{dir: dir, starts: 1}
	if err := os.MkdirAll(filepath.Join(dir, "proc", "sys", "kernel", "random"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "proc", "1"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "proc", "sys", "kernel", "random", "boot_id"), []byte("boot\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	proc.write(t)
	return proc
}

func (proc *fakeProc) write(t *testing.T) {
	t.Helper()
	fields := append([]string{"S"}, slices.Repeat([]string{"0"}, 18)...)
	stat := "1 (init) " + strings.Join(append(fields, strconv.Itoa(proc.starts)), " ") + " 0 0\n"
	if err := os.WriteFile(filepath.Join(proc.dir, "proc", "1", "stat"), []byte(stat), 0o600); err != nil {
		t.Fatal(err)
	}
}

func (proc *fakeProc) restart(t *testing.T) {
	t.Helper()
	proc.starts++
	proc.write(t)
}

func (proc *fakeProc) instance() string { return "boot/" + strconv.Itoa(proc.starts) }

// script makes s read the fake /proc.
func (proc *fakeProc) script(s string) string {
	return strings.ReplaceAll(s, "/proc/", proc.dir+"/proc/")
}

// cancelWith runs a canceler with a limit in seconds and returns its exit
// code and run time.
func cancelWith(t *testing.T, python, script, marker, limit string) (int, time.Duration) {
	t.Helper()
	started := time.Now()
	err := exec.Command(python, "-c", script, marker, limit).Run()
	var exitErr *exec.ExitError
	if err != nil && !errors.As(err, &exitErr) {
		t.Fatal(err)
	}
	if exitErr != nil {
		return exitErr.ExitCode(), time.Since(started)
	}
	return 0, time.Since(started)
}

// The supervisor records its container instance in its marker, and the
// canceler stops it through the usual protocol in the same instance.
func TestSupervisorRecordsItsInstance(t *testing.T) {
	python := installedPython(t)
	dir := t.TempDir()
	proc := newFakeProc(t, dir)
	invocation := startSupervisorScript(t, python, proc.script(Script), dir, "recorded")
	pid := invocation.childPID(t)
	if owner, err := os.ReadFile(invocation.marker); err != nil || string(owner) != "run "+proc.instance()+"\n" {
		t.Fatalf("marker holds %q, %v", owner, err)
	}
	if code, _ := cancelWith(t, python, proc.script(CancelScript), invocation.marker, "5"); code != 0 || supervisedProcessExists(t, python, pid) {
		t.Fatalf("cancel in the same instance exited %d", code)
	}
	invocation.exited(t, 5*time.Second)
}

// A supervisor that a signal killed leaves its marker. The canceler cannot
// confirm the invocation in the same instance, and confirms it at once after
// a restart. The marker then still keeps a late start from running.
func TestCancelConfirmsAnInvocationOfAnEarlierInstance(t *testing.T) {
	python := installedPython(t)
	dir := t.TempDir()
	proc := newFakeProc(t, dir)
	invocation := startSupervisorScript(t, python, proc.script(Script), dir, "killed")
	pid, err := strconv.Atoi(invocation.childPID(t))
	if err != nil {
		t.Fatal(err)
	}
	// The instance ends: the supervisor and its child's group die.
	_ = invocation.cmd.Process.Kill()
	<-invocation.done
	_ = syscall.Kill(-pid, syscall.SIGKILL)
	if code, _ := cancelWith(t, python, proc.script(CancelScript), invocation.marker, "0.3"); code == 0 {
		t.Fatal("cancel confirmed a dead supervisor's invocation in the same instance")
	}
	proc.restart(t)
	if code, took := cancelWith(t, python, proc.script(CancelScript), invocation.marker, "5"); code != 0 || took > 3*time.Second {
		t.Fatalf("cancel after a restart exited %d after %s", code, took)
	}
	if _, err := os.Stat(invocation.marker); err != nil {
		t.Fatalf("cancel removed the marker that blocks a late start: %v", err)
	}
	late := exec.Command(python, "-c", proc.script(Script), "4096", "1024", "30", python, "-c", supervisorWaitingChild, filepath.Join(dir, "late.pid"))
	late.Env = append(os.Environ(), MarkerVariable+"="+invocation.marker)
	if output, err := late.CombinedOutput(); err != nil || len(output) != 0 {
		t.Fatalf("late supervisor: %v; %q", err, output)
	}
	if _, err := os.Stat(filepath.Join(dir, "late.pid")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("a late supervisor ran its command")
	}
}

// Members of the killed group whose parent died are reaped by the supervisor
// itself, so they leave no zombie in a container whose PID 1 never reaps.
func TestSupervisorReapsItsKilledGroup(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("child subreapers are a Linux feature")
	}
	python := installedPython(t)
	dir := t.TempDir()
	pidPath := filepath.Join(dir, "descendant.pid")
	const tree = `import subprocess,sys,time
subprocess.Popen([sys.executable,"-c","import os,sys,time\nwith open(sys.argv[1],'w') as f: f.write(str(os.getpid()))\ntime.sleep(30)",sys.argv[1]])
time.sleep(30)`
	cmd := exec.Command(python, "-c", Script, "4096", "1024", "1", python, "-c", tree, pidPath)
	cmd.Env = append(os.Environ(), MarkerVariable+"="+filepath.Join(dir, "markers", "reap"))
	if output, err := cmd.Output(); err != nil || !strings.Contains(string(output), `"timed_out": true`) {
		t.Fatalf("supervisor: %v; %q", err, output)
	}
	pid, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat("/proc/" + string(pid)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("descendant %s remains after the supervisor exited: %v", pid, err)
	}
}

// Only a complete record, which ends with a newline, names another instance.
func TestCancelIgnoresAPartialRecord(t *testing.T) {
	python := installedPython(t)
	dir := t.TempDir()
	proc := newFakeProc(t, dir)
	marker := filepath.Join(dir, "markers", "partial")
	if err := os.MkdirAll(filepath.Dir(marker), 0o700); err != nil {
		t.Fatal(err)
	}
	for record, confirmed := range map[string]bool{"run b": false, "run boot/9": false, "run boot/9\n": true, "run \n": false} {
		if err := os.WriteFile(marker, []byte(record), 0o600); err != nil {
			t.Fatal(err)
		}
		if code, _ := cancelWith(t, python, proc.script(CancelScript), marker, "0.3"); (code == 0) != confirmed {
			t.Fatalf("record %q: canceler exited %d", record, code)
		}
		_ = os.Remove(marker + ".cancel")
	}
}

// A command that exits after starting a descendant in its own session does
// not keep the supervisor waiting for that descendant.
func TestSupervisorDoesNotWaitForADetachedDescendant(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the supervisor's group kill after a command exits needs Linux process groups")
	}
	python := installedPython(t)
	dir := t.TempDir()
	pidPath := filepath.Join(dir, "detached.pid")
	const detach = `import subprocess,sys
p=subprocess.Popen([sys.executable,"-c","import time; time.sleep(30)"],start_new_session=True,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
open(sys.argv[1],"w").write(str(p.pid))`
	cmd := exec.Command(python, "-c", Script, "4096", "1024", "30", python, "-c", detach, pidPath)
	cmd.Env = append(os.Environ(), MarkerVariable+"="+filepath.Join(dir, "markers", "detach"))
	started := time.Now()
	output, err := cmd.Output()
	took := time.Since(started)
	if data, readErr := os.ReadFile(pidPath); readErr == nil {
		if pid, convErr := strconv.Atoi(string(data)); convErr == nil {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
	if err != nil || !strings.Contains(string(output), `"exit_code": 0`) {
		t.Fatalf("supervisor: %v; %q", err, output)
	}
	if took > 800*time.Millisecond {
		t.Fatalf("the supervisor waited %s for a descendant outside the group", took)
	}
}

// killedSupervisorContainer reports a supervisor that a signal killed just
// as the caller's context ended.
type killedSupervisorContainer struct {
	testcontainers.Container
	cancel context.CancelFunc
}

func (ctr *killedSupervisorContainer) Exec(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
	ctr.cancel()
	return 137, bytes.NewReader(nil), nil
}

func TestRunReportsASupervisorKilledAsTheContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, err := Run(ctx, &killedSupervisorContainer{cancel: cancel}, "probe", time.Minute, 1024, 1024, "true")
	marker, unconfirmed := Unconfirmed(err)
	if !unconfirmed || marker == "" || !errors.Is(err, context.Canceled) {
		t.Fatalf("a killed supervisor returned %v", err)
	}
}

// A supervisor that cannot signal its command's group, for example under a
// seccomp profile that denies kill, keeps its marker and exits
// CleanupFailedExit, so that no cancellation is confirmed while the command
// may run.
func TestSupervisorKeepsItsMarkerWhenCleanupFails(t *testing.T) {
	python := installedPython(t)
	dir := t.TempDir()
	script := "import os\ndef deny(*args): raise PermissionError(1, 'denied')\nos.killpg=deny\n" + Script
	invocation := startSupervisorScript(t, python, script, dir, "denied")
	pid, err := strconv.Atoi(invocation.childPID(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
	if code, _ := cancelWith(t, python, CancelScript, invocation.marker, "1"); code == 0 {
		t.Fatal("a cancellation was confirmed although the command could not be stopped")
	}
	<-invocation.done
	if code := invocation.cmd.ProcessState.ExitCode(); code != CleanupFailedExit {
		t.Fatalf("supervisor exited %d", code)
	}
	if _, err := os.Stat(invocation.marker); err != nil {
		t.Fatalf("the supervisor removed its marker: %v", err)
	}
	if !supervisedProcessExists(t, python, strconv.Itoa(pid)) {
		t.Fatal("the model lost the command that the supervisor could not stop")
	}
	if marker, unconfirmed := Unconfirmed(&ExitError{Code: CleanupFailedExit, Marker: "m"}); !unconfirmed || marker != "m" {
		t.Fatal("a failed cleanup is not reported as unconfirmed")
	}
}

// A canceler that finds its own claim confirms again at once.
func TestCancelRetriesItsOwnClaim(t *testing.T) {
	python := installedPython(t)
	marker := filepath.Join(t.TempDir(), "markers", "claimed")
	for attempt := range 2 {
		if code, took := cancelWith(t, python, CancelScript, marker, "5"); code != 0 || took > 3*time.Second {
			t.Fatalf("attempt %d exited %d after %s", attempt, code, took)
		}
	}
	if owner, err := os.ReadFile(marker); err != nil || string(owner) != "claim" {
		t.Fatalf("claim holds %q, %v", owner, err)
	}
}

// cancelFailingContainer never answers the supervisor and fails every cancel.
// It calls started when the supervisor's exec arrives.
type cancelFailingContainer struct {
	testcontainers.Container
	marker  string
	started func()
}

func (ctr *cancelFailingContainer) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if len(args) > 3 && args[2] == CancelScript {
		ctr.marker = args[3]
		return 3, bytes.NewReader(nil), nil
	}
	if ctr.started != nil {
		ctr.started()
	}
	<-ctx.Done()
	return 0, nil, ctx.Err()
}

func TestCommandReportsAnInvocationItCouldNotStop(t *testing.T) {
	// The deadline passes once the supervisor's exec started, rather than
	// after a wall-clock interval that a slow (race-instrumented) run can
	// spend before Run even sizes the supervisor's timeout.
	ctx := clustertest.NewSignalDeadline(t.Context())
	ctr := &cancelFailingContainer{started: ctx.Expire}
	_, err := Command(ctx, ctr, "probe", time.Minute, 1024, 1024, "true")
	var notStopped *NotStoppedError
	if !errors.As(err, &notStopped) || notStopped.Marker == "" || notStopped.Marker != ctr.marker {
		t.Fatalf("a failed cancel returned %v", err)
	}
}
