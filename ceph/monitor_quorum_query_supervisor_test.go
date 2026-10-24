package ceph

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func monitorQuorumRunPython(t *testing.T, python string, stdoutLimit, stderrLimit int, seconds string, child string, args ...string) monitorQuorumTestEnvelope {
	t.Helper()
	return monitorQuorumRunPythonScript(t, python, supervised.Script, stdoutLimit, stderrLimit, seconds, child, args...)
}

func monitorQuorumRunPythonScript(t *testing.T, python, script string, stdoutLimit, stderrLimit int, seconds string, child string, args ...string) monitorQuorumTestEnvelope {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	argv := []string{"-c", script, strconv.Itoa(stdoutLimit), strconv.Itoa(stderrLimit), seconds, python, "-c", child}
	argv = append(argv, args...)
	cmd := exec.CommandContext(ctx, python, argv...)
	cmd.Env = append(os.Environ(), supervised.MarkerVariable+"="+filepath.Join(t.TempDir(), "markers", "invocation"))
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

// monitorQuorumHangOverallBound is the explicit bound on one hang fixture
// run. It sits below the fixture child's intrinsic expiry (2s), so a run that
// reaches it has stopped depending on the supervisor to settle the group.
const monitorQuorumHangOverallBound = 1500 * time.Millisecond

// monitorQuorumHangCaller is the caller deadline of the hang fixture. It
// shortens the supervisor's child duration to a fraction of itself.
const monitorQuorumHangCaller = 900 * time.Millisecond

// monitorQuorumHangObservation records the phases of one hang fixture run so
// a failure shows which phase used the wall-clock allowance.
type monitorQuorumHangObservation struct {
	StartupDelay           time.Duration // injected before the interpreter is launched
	Effective              string        // child duration the caller side handed to the supervisor
	ExecEntered            time.Duration // since the start of the run
	SupervisorUp           time.Duration // interpreter launched
	SupervisorEnd          time.Duration // supervisor exited and its report was complete
	Returned               time.Duration // queryMonitorQuorum returned
	CallerErr              error         // caller context state at return
	Report                 monitorQuorumTestEnvelope
	ReportErr              error
	Status                 QuorumStatus
	Err                    error
	PIDPath, HeartbeatPath string
}

func (o monitorQuorumHangObservation) String() string {
	return fmt.Sprintf("delay=%s effective=%ss exec=%s up=%s end=%s returned=%s caller=%v timedOut=%v exit=%d stdoutBytes=%d reportErr=%v status=%+v err=%v",
		o.StartupDelay, o.Effective, o.ExecEntered, o.SupervisorUp, o.SupervisorEnd, o.Returned, o.CallerErr, o.Report.TimedOut, o.Report.ExitCode, len(o.Report.Stdout), o.ReportErr, o.Status, o.Err)
}

// runMonitorQuorumHang executes the production supervisor around a child that
// prints complete quorum JSON and then hangs with a descendant. The
// supervisor runs under its own bounded context, not the caller's, so a caller
// deadline cannot cut it off before it settles the group. descendant is the
// Python source of the descendant; a variant may leave the owned group.
func runMonitorQuorumHang(t *testing.T, startupDelay time.Duration, descendant string) monitorQuorumHangObservation {
	t.Helper()
	python := diagnosticsInstalledPython(t)
	obs := monitorQuorumHangObservation{
		StartupDelay:  startupDelay,
		PIDPath:       filepath.Join(t.TempDir(), "child-pid"),
		HeartbeatPath: filepath.Join(t.TempDir(), "descendant-heartbeat"),
	}
	// Intrinsic expiry bounds failed fixtures without signaling a recorded PID
	// after the supervisor may already have released its ownership by reaping.
	const child = `import os,subprocess,sys,time
deadline=time.monotonic()+2
with open(sys.argv[1],'w') as f: f.write(str(os.getpid()))
subprocess.Popen([sys.executable,'-c',sys.argv[4],sys.argv[2]],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
sys.stdout.write(sys.argv[3]); sys.stdout.flush()
os.close(1); os.close(2)
while time.monotonic()<deadline: time.sleep(0.01)
`
	markers := t.TempDir()
	started := time.Now()
	control := &diagnosticsTestContainer{exec: func(_ context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
		obs.ExecEntered = time.Since(started)
		if !monitorQuorumTestCommand(args) {
			t.Fatal("native read did not use the fixed quorum supervisor")
		}
		obs.Effective = args[5]
		// Execute the exact production supervisor, replacing only its fixed Ceph
		// child with this local native-process fixture and moving its marker
		// below a test directory. No Docker is involved.
		argv := append([]string{}, args[1:6]...)
		argv = append(argv, python, "-c", child, obs.PIDPath, obs.HeartbeatPath, monitorConfigQuorum, descendant)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		time.Sleep(startupDelay)
		cmd := exec.CommandContext(ctx, python, argv...)
		cmd.Env = append(os.Environ(), supervisorTestEnvironment(opts, markers)...)
		obs.SupervisorUp = time.Since(started)
		output, err := cmd.Output()
		obs.SupervisorEnd = time.Since(started)
		if exitErr, ok := err.(*exec.ExitError); ok {
			t.Logf("actual quorum supervisor stderr: %s", exitErr.Stderr)
		}
		obs.ReportErr = json.Unmarshal(output, &obs.Report)
		return 0, bytes.NewReader(output), err
	}}
	ctx, cancel := context.WithTimeout(t.Context(), monitorQuorumHangCaller)
	defer cancel()
	obs.Status, obs.Err = queryMonitorQuorum(ctx, control)
	obs.Returned = time.Since(started)
	obs.CallerErr = ctx.Err()
	return obs
}

// monitorQuorumVerifyNoFalseSuccess checks the semantic contract: complete
// JSON from a child that never exits is a deadline error with an empty status,
// and the supervisor itself reported the timeout.
func monitorQuorumVerifyNoFalseSuccess(obs monitorQuorumHangObservation) error {
	if !errors.Is(obs.Err, context.DeadlineExceeded) || len(obs.Status.QuorumNames) != 0 {
		return fmt.Errorf("full JSON before a stuck process became success: %s", obs)
	}
	stdout, err := base64.StdEncoding.DecodeString(obs.Report.Stdout)
	if obs.ReportErr != nil || err != nil || !obs.Report.TimedOut || string(stdout) != monitorConfigQuorum {
		return fmt.Errorf("supervisor did not report the stuck child as timed out with its full output: %s", obs)
	}
	return nil
}

// monitorQuorumVerifyOverallBound is the explicit wall-clock bound; it is not
// the admission margin against the caller deadline.
func monitorQuorumVerifyOverallBound(obs monitorQuorumHangObservation) error {
	if obs.Returned >= monitorQuorumHangOverallBound {
		return fmt.Errorf("run exceeded the explicit overall bound %s: %s", monitorQuorumHangOverallBound, obs)
	}
	return nil
}

// monitorQuorumVerifyDirectChildReaped checks the recorded child PID
// independently of the returned error. kill(pid, 0) must fail after p.wait():
// killing without reaping is insufficient.
func monitorQuorumVerifyDirectChildReaped(t *testing.T, python, pidPath string) error {
	t.Helper()
	pid, err := os.ReadFile(pidPath)
	if err != nil {
		return fmt.Errorf("child did not publish its owned identity: %w", err)
	}
	probe, err := exec.CommandContext(t.Context(), python, "-c", "import os,sys\ntry:\n os.kill(int(sys.argv[1]),0); print('alive')\nexcept ProcessLookupError: print('reaped')", string(pid)).Output()
	if err != nil || strings.TrimSpace(string(probe)) != "reaped" {
		return fmt.Errorf("supervisor returned before reaping its direct child: %q, %v", probe, err)
	}
	return nil
}

func monitorQuorumVerifyDescendantSettled(heartbeatPath string) error {
	before, err := os.ReadFile(heartbeatPath)
	if err != nil || len(before) == 0 {
		return fmt.Errorf("descendant did not demonstrate activity before timeout: bytes=%d err=%v", len(before), err)
	}
	time.Sleep(100 * time.Millisecond)
	after, err := os.ReadFile(heartbeatPath)
	if err != nil || len(after) != len(before) {
		return fmt.Errorf("owned descendant remained active after return: before=%d after=%d err=%v", len(before), len(after), err)
	}
	return nil
}

const monitorQuorumOwnedDescendant = `import sys,time
deadline=time.monotonic()+2
with open(sys.argv[1],'ab',buffering=0) as f:
 while time.monotonic()<deadline:
  f.write(b'x'); time.sleep(0.01)
`

func TestMonitorQuorumPythonFullJSONThenHangKillsAndReapsOwnedGroup(t *testing.T) {
	python := diagnosticsInstalledPython(t)
	obs := runMonitorQuorumHang(t, 0, monitorQuorumOwnedDescendant)
	t.Logf("phases: %s", obs)
	// Each contract is checked independently so one failure does not hide the
	// others, and none is inferred from the returned error.
	if err := monitorQuorumVerifyNoFalseSuccess(obs); err != nil {
		t.Error(err)
	}
	if err := monitorQuorumVerifyDirectChildReaped(t, python, obs.PIDPath); err != nil {
		t.Error(err)
	}
	if err := monitorQuorumVerifyDescendantSettled(obs.HeartbeatPath); err != nil {
		t.Error(err)
	}
	if err := monitorQuorumVerifyOverallBound(obs); err != nil {
		t.Error(err)
	}
}

// With interpreter admission delayed past the caller-relative allowance, the
// former strict condition (elapsed below the caller deadline) is violated by
// construction, while the semantic contract and the explicit bound still hold.
func TestMonitorQuorumHangControlStartupDelayBreaksOnlyWallClockMargin(t *testing.T) {
	python := diagnosticsInstalledPython(t)
	obs := runMonitorQuorumHang(t, 300*time.Millisecond, monitorQuorumOwnedDescendant)
	t.Logf("phases: %s", obs)
	if obs.Returned < monitorQuorumHangCaller {
		t.Fatalf("injected delay did not cross the former strict margin: %s", obs)
	}
	if err := errors.Join(monitorQuorumVerifyNoFalseSuccess(obs), monitorQuorumVerifyDirectChildReaped(t, python, obs.PIDPath),
		monitorQuorumVerifyDescendantSettled(obs.HeartbeatPath), monitorQuorumVerifyOverallBound(obs)); err != nil {
		t.Fatalf("delayed admission broke a semantic contract: %v", err)
	}
}

// Negative controls: the verifiers must fail on outcomes they exist to catch.
func TestMonitorQuorumHangVerifiersRejectBrokenOutcomes(t *testing.T) {
	python := diagnosticsInstalledPython(t)
	good := runMonitorQuorumHang(t, 0, monitorQuorumOwnedDescendant)

	falseSuccess := good
	falseSuccess.Err = nil
	falseSuccess.Status = QuorumStatus{QuorumNames: []string{"a"}}
	if monitorQuorumVerifyNoFalseSuccess(falseSuccess) == nil {
		t.Error("accepted full JSON from a stuck child as success")
	}
	notTimedOut := good
	notTimedOut.Report.TimedOut = false
	if monitorQuorumVerifyNoFalseSuccess(notTimedOut) == nil {
		t.Error("accepted a report that does not show the supervisor timeout")
	}
	slow := good
	slow.Returned = monitorQuorumHangOverallBound
	if monitorQuorumVerifyOverallBound(slow) == nil {
		t.Error("accepted a run at the explicit overall bound")
	}

	// A live process in place of the recorded child: the direct child is not reaped.
	sleeper := exec.CommandContext(t.Context(), python, "-c", "import time;time.sleep(5)")
	if err := sleeper.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sleeper.Wait() }()
	alivePath := filepath.Join(t.TempDir(), "alive-pid")
	if err := os.WriteFile(alivePath, []byte(strconv.Itoa(sleeper.Process.Pid)), 0o600); err != nil {
		t.Fatal(err)
	}
	if monitorQuorumVerifyDirectChildReaped(t, python, alivePath) == nil {
		t.Error("accepted a live direct child as reaped")
	}
	_ = sleeper.Process.Kill()

	// A descendant that left the owned process group survives the group kill.
	escaped := runMonitorQuorumHang(t, 0, "import os\nos.setsid()\n"+monitorQuorumOwnedDescendant)
	if monitorQuorumVerifyDescendantSettled(escaped.HeartbeatPath) == nil {
		t.Error("accepted an active descendant outside the killed group as settled")
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
	result := monitorQuorumRunPythonScript(t, python, observe+supervised.Script, 4096, 1024, "2", child, heartbeatPath, monitorConfigQuorum)
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

// supervisorTestEnvironment returns the environment that supervised.Command
// passes to Exec, with the invocation marker moved from the container's
// marker directory into dir.
func supervisorTestEnvironment(opts []tcexec.ProcessOption, dir string) []string {
	processOptions := tcexec.NewProcessOptions(nil)
	for _, opt := range opts {
		opt.Apply(processOptions)
	}
	env := slices.Clone(processOptions.ExecConfig.Env)
	for i, value := range env {
		if marker, ok := strings.CutPrefix(value, supervised.MarkerVariable+"="+supervised.MarkerDir+"/"); ok {
			env[i] = supervised.MarkerVariable + "=" + filepath.Join(dir, marker)
		}
	}
	return env
}
