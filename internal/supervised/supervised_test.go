package supervised

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
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
	invocation := &supervisorInvocation{marker: filepath.Join(dir, "markers", name), pidPath: filepath.Join(dir, name+".pid"), done: make(chan struct{})}
	invocation.cmd = exec.Command(python, "-c", Script, "4096", "1024", "30", python, "-c", supervisorWaitingChild, invocation.pidPath)
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
