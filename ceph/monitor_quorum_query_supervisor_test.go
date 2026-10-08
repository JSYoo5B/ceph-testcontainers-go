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
	output, err := exec.CommandContext(ctx, python, argv...).CombinedOutput()
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
	control := &diagnosticsTestContainer{exec: func(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
		if !monitorQuorumTestCommand(args) {
			t.Fatal("native read did not use the fixed quorum supervisor")
		}
		// Execute the exact production supervisor, replacing only its fixed Ceph
		// child with this local native-process fixture. No Docker is involved.
		argv := append([]string{}, args[1:6]...)
		argv = append(argv, python, "-c", child, pidPath, heartbeatPath, monitorConfigQuorum)
		output, err := exec.CommandContext(ctx, python, argv...).Output()
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
