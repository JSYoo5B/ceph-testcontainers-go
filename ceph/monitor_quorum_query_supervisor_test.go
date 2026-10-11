package ceph

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func monitorQuorumRunPython(t *testing.T, python string, stdoutLimit, stderrLimit int, seconds string, child string, args ...string) monitorQuorumTestEnvelope {
	t.Helper()
	return monitorQuorumRunPythonScript(t, python, monitorQuorumCommandScript, stdoutLimit, stderrLimit, seconds, child, args...)
}

func monitorQuorumRunPythonScript(t *testing.T, python, script string, stdoutLimit, stderrLimit int, seconds string, child string, args ...string) monitorQuorumTestEnvelope {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	argv := []string{"-c", script, strconv.Itoa(stdoutLimit), strconv.Itoa(stderrLimit), seconds, python, "-c", child}
	argv = append(argv, args...)
	cmd := exec.CommandContext(ctx, python, argv...)
	cmd.Env = append(os.Environ(), supervisedMarkerVariable+"="+filepath.Join(t.TempDir(), "markers", "invocation"))
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("actual quorum supervisor failed: %v; %s", err, output)
	}
	var result monitorQuorumTestEnvelope
	if err := json.Unmarshal(output, &result); err != nil || result.SchemaVersion != 1 {
		t.Fatalf("actual quorum supervisor returned invalid envelope: %v; %s", err, output)
	}
	return result
}

const monitorQuorumHeartbeatDescendant = `import sys,time
with open(sys.argv[1],'ab',buffering=0) as f:
 while True:
  f.write(b'x'); time.sleep(0.01)
`

func monitorQuorumAssertDescendantStopped(t *testing.T, heartbeatPath string) {
	t.Helper()
	before, err := os.ReadFile(heartbeatPath)
	if err != nil || len(before) == 0 {
		t.Fatalf("owned descendant never demonstrated activity: bytes=%d err=%v", len(before), err)
	}
	time.Sleep(100 * time.Millisecond)
	after, err := os.ReadFile(heartbeatPath)
	if err != nil || len(after) != len(before) {
		t.Fatalf("owned descendant remained active after return: before=%d after=%d err=%v", len(before), len(after), err)
	}
}

func TestMonitorQuorumPythonKeepsNativeStdoutSeparate(t *testing.T) {
	python := diagnosticsInstalledPython(t)
	heartbeatPath := filepath.Join(t.TempDir(), "stdout-descendant")
	const child = `import fcntl,os,subprocess,sys,time
subprocess.Popen([sys.executable,'-c',sys.argv[3],sys.argv[2]],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
while not os.path.exists(sys.argv[2]) or os.path.getsize(sys.argv[2])==0: time.sleep(0.01)
fcntl.fcntl(2,fcntl.F_SETFL,fcntl.fcntl(2,fcntl.F_GETFL)|os.O_NONBLOCK)
sys.stdout.write(sys.argv[1]); sys.stdout.flush()
os.write(2,b'native warning on separate stderr\n')
`
	result := monitorQuorumRunPython(t, python, 4096, 1024, "2", child, monitorConfigQuorum, heartbeatPath, monitorQuorumHeartbeatDescendant)
	stdout, stdoutErr := base64.StdEncoding.DecodeString(result.Stdout)
	stderr, stderrErr := base64.StdEncoding.DecodeString(result.Stderr)
	if stdoutErr != nil || stderrErr != nil || string(stdout) != monitorConfigQuorum || string(stderr) != "native warning on separate stderr\n" || result.ExitCode != 0 || result.TimedOut || result.StdoutTruncated || result.StderrTruncated {
		t.Fatalf("native JSON and independently nonblocking stderr were mixed or lost: %+v; %v, %v", result, stdoutErr, stderrErr)
	}
	monitorQuorumAssertDescendantStopped(t, heartbeatPath)
}

func TestMonitorQuorumPythonDrainsBeyondSeparateOutputCaps(t *testing.T) {
	python := diagnosticsInstalledPython(t)
	heartbeatPath := filepath.Join(t.TempDir(), "capped-output-descendant")
	const child = `import fcntl,os,select,subprocess,sys,time
subprocess.Popen([sys.executable,'-c',sys.argv[2],sys.argv[1]],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
while not os.path.exists(sys.argv[1]) or os.path.getsize(sys.argv[1])==0: time.sleep(0.01)
fcntl.fcntl(2,fcntl.F_SETFL,fcntl.fcntl(2,fcntl.F_GETFL)|os.O_NONBLOCK)
sys.stdout.buffer.write(b'O'*(4*1024*1024)); sys.stdout.buffer.flush()
pending=b'E'*(256*1024)
while pending:
 try:
  count=os.write(2,pending); pending=pending[count:]
 except BlockingIOError:
  select.select([],[2],[],0.1)
`
	result := monitorQuorumRunPython(t, python, 512, 64, "3", child, heartbeatPath, monitorQuorumHeartbeatDescendant)
	stdout, stdoutErr := base64.StdEncoding.DecodeString(result.Stdout)
	stderr, stderrErr := base64.StdEncoding.DecodeString(result.Stderr)
	if stdoutErr != nil || stderrErr != nil || string(stdout) != strings.Repeat("O", 512) || string(stderr) != strings.Repeat("E", 64) || result.ExitCode != 0 || result.TimedOut || !result.StdoutTruncated || !result.StderrTruncated {
		t.Fatalf("caps stopped pipe drain or did not retain separate truncation: %+v; %v, %v", result, stdoutErr, stderrErr)
	}
	monitorQuorumAssertDescendantStopped(t, heartbeatPath)
}

func TestMonitorQuorumPythonFullJSONThenHangKillsAndReapsOwnedGroup(t *testing.T) {
	python := diagnosticsInstalledPython(t)
	pidPath := filepath.Join(t.TempDir(), "child-pid")
	heartbeatPath := filepath.Join(t.TempDir(), "descendant-heartbeat")
	// Intrinsic expiry bounds failed fixtures without signaling a recorded PID
	// after the supervisor may already have released its ownership by reaping.
	const child = `import os,subprocess,sys,time
deadline=time.monotonic()+2
with open(sys.argv[1],'w') as f: f.write(str(os.getpid()))
descendant="import sys,time\ndeadline=time.monotonic()+2\nwith open(sys.argv[1],'ab',buffering=0) as f:\n while time.monotonic()<deadline:\n  f.write(b'x'); time.sleep(0.01)"
subprocess.Popen([sys.executable,'-c',descendant,sys.argv[2]],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
sys.stdout.write(sys.argv[3]); sys.stdout.flush()
os.close(1); os.close(2)
while time.monotonic()<deadline: time.sleep(0.01)
`
	markers := t.TempDir()
	control := &diagnosticsTestContainer{exec: func(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
		if !monitorQuorumTestCommand(args) {
			t.Fatal("native read did not use the fixed quorum supervisor")
		}
		// Execute the exact production supervisor, replacing only its fixed Ceph
		// child with this local native-process fixture and moving its marker
		// below a test directory. No Docker is involved.
		argv := append([]string{}, args[1:6]...)
		argv = append(argv, python, "-c", child, pidPath, heartbeatPath, monitorConfigQuorum)
		cmd := exec.CommandContext(ctx, python, argv...)
		cmd.Env = append(os.Environ(), supervisorTestEnvironment(opts, markers)...)
		output, err := cmd.Output()
		if exitErr, ok := err.(*exec.ExitError); ok {
			t.Logf("actual quorum supervisor stderr: %s", exitErr.Stderr)
		}
		return 0, bytes.NewReader(output), err
	}}
	ctx, cancel := context.WithTimeout(t.Context(), 900*time.Millisecond)
	defer cancel()
	started := time.Now()
	status, err := queryMonitorQuorum(ctx, control)
	if !errors.Is(err, context.DeadlineExceeded) || len(status.QuorumNames) != 0 || ctx.Err() != nil || time.Since(started) >= 900*time.Millisecond {
		t.Fatalf("full JSON before a stuck process became success or exceeded caller budget: %+v, %v, caller=%v elapsed=%s", status, err, ctx.Err(), time.Since(started))
	}
	pid, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatal("child did not publish its owned identity", err)
	}
	// kill(pid, 0) must fail after p.wait(): killing without reaping is insufficient.
	probe, err := exec.CommandContext(t.Context(), python, "-c", "import os,sys\ntry:\n os.kill(int(sys.argv[1]),0); print('alive')\nexcept ProcessLookupError: print('reaped')", string(pid)).Output()
	if err != nil || strings.TrimSpace(string(probe)) != "reaped" {
		t.Fatalf("supervisor returned before reaping its direct child: %q, %v", probe, err)
	}
	before, err := os.ReadFile(heartbeatPath)
	if err != nil || len(before) == 0 {
		t.Fatalf("descendant did not demonstrate activity before timeout: bytes=%d err=%v", len(before), err)
	}
	time.Sleep(100 * time.Millisecond)
	after, err := os.ReadFile(heartbeatPath)
	if err != nil || len(after) != len(before) {
		t.Fatalf("owned descendant remained active after return: before=%d after=%d err=%v", len(before), len(after), err)
	}
	if elapsed := time.Since(started); elapsed >= 1500*time.Millisecond {
		t.Fatalf("descendant observation approached the fixture's intrinsic expiry: %s", elapsed)
	}
}

func TestMonitorQuorumPythonSignalsOwnedGroupBeforeReapingSuccessfulChild(t *testing.T) {
	python := diagnosticsInstalledPython(t)
	heartbeatPath := filepath.Join(t.TempDir(), "successful-child-descendant")
	// Instrument the actual supervisor's syscalls. The observed child must still
	// be waitable without reaping when its group is signaled, including exit 0.
	const observe = `import os,signal,subprocess
original_killpg=os.killpg; original_wait=subprocess.Popen.wait; signaled=False
def observed_killpg(pid,sig):
 global signaled
 observed=os.waitid(os.P_PID,pid,os.WEXITED|os.WNOHANG|os.WNOWAIT)
 assert observed is not None and observed.si_pid==pid, 'owned child identity was reaped before group cleanup'
 original_killpg(pid,sig)
 signaled=True
def observed_wait(self,*args,**kwargs):
 assert signaled, 'reap preceded owned group signal'
 return original_wait(self,*args,**kwargs)
os.killpg=observed_killpg; subprocess.Popen.wait=observed_wait
`
	const child = `import os,subprocess,sys,time
descendant="import sys,time\nwith open(sys.argv[1],'ab',buffering=0) as f:\n while True:\n  f.write(b'x'); time.sleep(0.01)"
subprocess.Popen([sys.executable,'-c',descendant,sys.argv[1]],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
while not os.path.exists(sys.argv[1]) or os.path.getsize(sys.argv[1])==0: time.sleep(0.01)
sys.stdout.write(sys.argv[2]); sys.stdout.flush()
`
	result := monitorQuorumRunPythonScript(t, python, observe+monitorQuorumCommandScript, 4096, 1024, "2", child, heartbeatPath, monitorConfigQuorum)
	stdout, err := base64.StdEncoding.DecodeString(result.Stdout)
	if err != nil || string(stdout) != monitorConfigQuorum || result.ExitCode != 0 || result.TimedOut || result.StdoutTruncated || result.StderrTruncated {
		t.Fatalf("safe cleanup changed the completed native observation: %+v, %v", result, err)
	}
	before, err := os.ReadFile(heartbeatPath)
	if err != nil || len(before) == 0 {
		t.Fatalf("normal-exit descendant never demonstrated activity: bytes=%d err=%v", len(before), err)
	}
	time.Sleep(100 * time.Millisecond)
	after, err := os.ReadFile(heartbeatPath)
	if err != nil || len(after) != len(before) {
		t.Fatalf("normal-completion group cleanup retained a descendant: before=%d after=%d err=%v", len(before), len(after), err)
	}
}

// supervisorTestEnvironment returns the environment that supervisedCommand
// passes to Exec, with the invocation marker moved from the container's
// marker directory into dir.
func supervisorTestEnvironment(opts []tcexec.ProcessOption, dir string) []string {
	processOptions := tcexec.NewProcessOptions(nil)
	for _, opt := range opts {
		opt.Apply(processOptions)
	}
	env := slices.Clone(processOptions.ExecConfig.Env)
	for i, value := range env {
		if marker, ok := strings.CutPrefix(value, supervisedMarkerVariable+"="+supervisedMarkerDir+"/"); ok {
			env[i] = supervisedMarkerVariable + "=" + filepath.Join(dir, marker)
		}
	}
	return env
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
	invocation.cmd = exec.Command(python, "-c", monitorQuorumCommandScript, "4096", "1024", "30", python, "-c", supervisorWaitingChild, invocation.pidPath)
	invocation.cmd.Env = append(os.Environ(), supervisedMarkerVariable+"="+invocation.marker)
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
	output, err := exec.Command(python, "-c", supervisedCancelScript, marker, "5").CombinedOutput()
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
	python := diagnosticsInstalledPython(t)
	dir := t.TempDir()
	marker := filepath.Join(dir, "markers", "late")
	runSupervisorCancel(t, python, marker)
	late := &supervisorInvocation{marker: marker, pidPath: filepath.Join(dir, "late.pid"), done: make(chan struct{})}
	late.cmd = exec.Command(python, "-c", monitorQuorumCommandScript, "4096", "1024", "30", python, "-c", supervisorWaitingChild, late.pidPath)
	late.cmd.Env = append(os.Environ(), supervisedMarkerVariable+"="+marker)
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
	python := diagnosticsInstalledPython(t)
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
	python := diagnosticsInstalledPython(t)
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
