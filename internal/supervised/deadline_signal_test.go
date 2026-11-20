package supervised

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// reportingChild handles SIGABRT as a Ceph command does: it reports where it
// was on standard error and ends. With "ignore" it ignores the signal.
const reportingChild = `import os,signal,sys,time
def report(*_):
 os.write(2,b"stopped in wait\n"); os._exit(3)
signal.signal(signal.SIGABRT, signal.SIG_IGN if sys.argv[1]=="ignore" else report)
sys.stdout.write("before the wait\n"); sys.stdout.flush()
time.sleep(30)`

func runAtDeadline(t *testing.T, mode string, env ...string) (stdout, stderr string, exit int, elapsed time.Duration) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("signaling the group of an exited, unreaped child is refused outside Linux")
	}
	python := installedPython(t)
	cmd := exec.Command(python, "-c", Script, "4096", "4096", "0.5", python, "-c", reportingChild, mode)
	cmd.Env = append(append(os.Environ(), MarkerVariable+"="+filepath.Join(t.TempDir(), "markers", "deadline")), env...)
	started := time.Now()
	output, err := cmd.Output()
	elapsed = time.Since(started)
	if err != nil {
		t.Fatalf("supervisor: %v; %q", err, output)
	}
	var envelope struct {
		Stdout   string `json:"stdout"`
		Stderr   string `json:"stderr"`
		ExitCode int    `json:"exit_code"`
		TimedOut bool   `json:"timed_out"`
	}
	if err := json.Unmarshal(output, &envelope); err != nil || !envelope.TimedOut {
		t.Fatalf("envelope: %q, %v", output, err)
	}
	out, _ := base64.StdEncoding.DecodeString(envelope.Stdout)
	errOut, _ := base64.StdEncoding.DecodeString(envelope.Stderr)
	return string(out), string(errOut), envelope.ExitCode, elapsed
}

// A command stopped at the deadline with the deadline signal still delivers
// what its handler wrote; SIGTERM alone, the stop without that signal, ends
// the same command before it reports anything.
func TestSupervisorDeadlineSignalKeepsTheHandlersReport(t *testing.T) {
	stdout, stderr, exit, elapsed := runAtDeadline(t, "report", roleTermGrace+"=2", deadlineSignalVariable+"=6")
	if stdout != "before the wait\n" || stderr != "stopped in wait\n" || exit != 3 || elapsed > 2*time.Second {
		t.Fatalf("with the deadline signal: stdout=%q stderr=%q exit=%d after %s", stdout, stderr, exit, elapsed)
	}
	stdout, stderr, exit, _ = runAtDeadline(t, "report", roleTermGrace+"=2")
	if stdout != "before the wait\n" || stderr != "" || exit != -15 {
		t.Fatalf("without the deadline signal: stdout=%q stderr=%q exit=%d", stdout, stderr, exit)
	}
	// Without a grace there is no time to report: the stop is the kill.
	if _, stderr, exit, _ = runAtDeadline(t, "report", deadlineSignalVariable+"=6"); stderr != "" || exit != -9 {
		t.Fatalf("without a grace: stderr=%q exit=%d", stderr, exit)
	}
}

// A command that does not end on the deadline signal is killed after the grace.
func TestSupervisorKillsACommandThatIgnoresTheDeadlineSignal(t *testing.T) {
	stdout, stderr, exit, elapsed := runAtDeadline(t, "ignore", roleTermGrace+"=0.3", deadlineSignalVariable+"=6")
	if stdout != "before the wait\n" || stderr != "" || exit != -9 || elapsed < 800*time.Millisecond || elapsed > 5*time.Second {
		t.Fatalf("stdout=%q stderr=%q exit=%d after %s", stdout, stderr, exit, elapsed)
	}
}

type envContainer struct {
	testcontainers.Container
	env [][]string
}

func (ctr *envContainer) Exec(_ context.Context, _ []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	options := tcexec.NewProcessOptions(nil)
	for _, opt := range opts {
		opt.Apply(options)
	}
	ctr.env = append(ctr.env, options.ExecConfig.Env)
	return 0, strings.NewReader(`{"schema_version":1,"stdout":"","stderr":"","exit_code":0,"timed_out":false,"stdout_truncated":false,"stderr_truncated":false}`), nil
}

// Only a role command under WithDeadlineSignal asks the supervisor for it.
func TestDeadlineSignalReachesOnlyARoleCommandThatAsksForIt(t *testing.T) {
	ctr := &envContainer{}
	if _, err := RunRoleCommand(WithDeadlineSignal(t.Context(), 6), ctr, "probe", 5, 0, "true"); err != nil {
		t.Fatal(err)
	}
	if _, err := RunRoleCommand(t.Context(), ctr, "probe", 5, 0, "true"); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(WithDeadlineSignal(t.Context(), 6), ctr, "probe", time.Second, 64, 64, "true"); err != nil {
		t.Fatal(err)
	}
	const variable = deadlineSignalVariable + "=6"
	if len(ctr.env) != 3 || !slices.Contains(ctr.env[0], variable) || slices.Contains(ctr.env[1], variable) || slices.Contains(ctr.env[2], variable) {
		t.Fatalf("supervisor environments: %v", ctr.env)
	}
}
