package supervised

import (
	"context"
	"encoding/base64"
	"encoding/json"
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

// answeringChild handles SIGABRT as reportingChild does and answers one
// request on a Unix socket as a Ceph admin socket does: after the request
// and a zero byte it writes a four-byte length and the request, repeated
// sys.argv[3] times. With "silent" it accepts the connection and never
// answers; with "absent" it has no socket.
const answeringChild = `import os,signal,socket,sys,threading,time
def report(*_):
 os.write(2,b"stopped in wait\n"); os._exit(3)
signal.signal(signal.SIGABRT,report)
def serve(s):
 c,_=s.accept()
 if sys.argv[1]=="silent": time.sleep(30)
 request=b""
 while not request.endswith(b"\0"): request+=c.recv(64)
 body=request[:-1]*int(sys.argv[3])
 c.sendall(len(body).to_bytes(4,"big")+body); c.close()
if sys.argv[1]!="absent":
 s=socket.socket(socket.AF_UNIX); s.bind(sys.argv[2].replace("{pid}",str(os.getpid()))); s.listen(1)
 threading.Thread(target=serve,args=(s,),daemon=True).start()
sys.stdout.write("before the wait\n"); sys.stdout.flush()
time.sleep(30)`

// Before the deadline signal the supervisor asks the command over its socket
// and reports the answer, bounded in bytes; a command that never answers, or
// has no socket, costs at most the question's one second and still gets the
// signal. The socket's file does not outlive the command.
func TestSupervisorAsksTheCommandBeforeTheDeadlineSignal(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("signaling the group of an exited, unreaped child is refused outside Linux")
	}
	python := installedPython(t)
	const request = "where"
	for name, test := range map[string]struct {
		mode             string
		repeat           int
		reply            string
		size             int
		atLeast, atMost  time.Duration
		withoutTheSignal bool
	}{
		"answered":       {mode: "answer", repeat: 3, reply: "wherewherewhere", size: 15, atMost: 1500 * time.Millisecond},
		"more than fits": {mode: "answer", repeat: 20000, reply: strings.Repeat(request, DeadlineReplyLimit/len(request)+1)[:DeadlineReplyLimit], size: 100000, atMost: 1500 * time.Millisecond},
		"never answered": {mode: "silent", atLeast: 1400 * time.Millisecond, atMost: 3 * time.Second},
		"no socket":      {mode: "absent", atMost: 1500 * time.Millisecond},
		// Without the deadline signal there is no report to wait for, and
		// no question.
		"no signal": {mode: "answer", repeat: 1, atMost: 3500 * time.Millisecond, withoutTheSignal: true},
	} {
		t.Run(name, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "ask")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			socket := filepath.Join(dir, "command."+DeadlineQueryPID+".asok")
			cmd := exec.Command(python, "-c", Script, "4096", "4096", "0.5", python, "-c", answeringChild, test.mode, socket, strconv.Itoa(test.repeat))
			cmd.Env = append(os.Environ(), MarkerVariable+"="+filepath.Join(dir, "markers", "ask"), roleTermGrace+"=2",
				deadlineSocketVariable+"="+socket, deadlineRequestVariable+"="+request)
			if !test.withoutTheSignal {
				cmd.Env = append(cmd.Env, deadlineSignalVariable+"=6")
			}
			started := time.Now()
			output, err := cmd.Output()
			elapsed := time.Since(started)
			if err != nil {
				t.Fatalf("supervisor: %v; %.300q", err, output)
			}
			var envelope struct {
				Stderr   string  `json:"stderr"`
				ExitCode int     `json:"exit_code"`
				TimedOut bool    `json:"timed_out"`
				Reply    *string `json:"deadline_reply"`
				Size     int     `json:"deadline_reply_size"`
			}
			if err := json.Unmarshal(output, &envelope); err != nil || !envelope.TimedOut {
				t.Fatalf("envelope: %.300q, %v", output, err)
			}
			reply := ""
			if envelope.Reply != nil {
				data, err := base64.StdEncoding.DecodeString(*envelope.Reply)
				if err != nil {
					t.Fatal(err)
				}
				reply = string(data)
			}
			if reply != test.reply || envelope.Size != test.size || elapsed < test.atLeast || elapsed > test.atMost {
				t.Fatalf("reply %.40q of %d bytes (announced %d) after %s", reply, len(reply), envelope.Size, elapsed)
			}
			stderr, _ := base64.StdEncoding.DecodeString(envelope.Stderr)
			if wantErr, wantExit := "stopped in wait\n", 3; !test.withoutTheSignal && (string(stderr) != wantErr || envelope.ExitCode != wantExit) {
				t.Fatalf("after the question the command got no deadline signal: stderr=%q exit=%d", stderr, envelope.ExitCode)
			}
			if left, err := filepath.Glob(filepath.Join(dir, "*.asok")); err != nil || len(left) != 0 {
				t.Fatalf("the supervisor left the command's socket: %v, %v", left, err)
			}
		})
	}
}

// Only a role command that has a deadline signal carries the question.
func TestDeadlineQueryReachesOnlyARoleCommandWithADeadlineSignal(t *testing.T) {
	ctr := &envContainer{}
	asked := WithDeadlineQuery(t.Context(), "/run/command."+DeadlineQueryPID+".asok", `{"prefix":"where"}`)
	if _, err := RunRoleCommand(WithDeadlineSignal(asked, 6), ctr, "probe", 5, 0, "true"); err != nil {
		t.Fatal(err)
	}
	if _, err := RunRoleCommand(asked, ctr, "probe", 5, 0, "true"); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(WithDeadlineSignal(asked, 6), ctr, "probe", time.Second, 64, 64, "true"); err != nil {
		t.Fatal(err)
	}
	socket, request := deadlineSocketVariable+"=/run/command.{pid}.asok", deadlineRequestVariable+`={"prefix":"where"}`
	if len(ctr.env) != 3 || !slices.Contains(ctr.env[0], socket) || !slices.Contains(ctr.env[0], request) {
		t.Fatalf("supervisor environments: %v", ctr.env)
	}
	for _, env := range ctr.env[1:] {
		if slices.ContainsFunc(env, func(v string) bool {
			return strings.HasPrefix(v, deadlineSocketVariable) || strings.HasPrefix(v, deadlineRequestVariable)
		}) {
			t.Fatalf("supervisor environments: %v", ctr.env)
		}
	}
}

// The report carries the answer and whether it was cut, and refuses an
// envelope whose answer does not fit what it announces.
func TestReportHoldsTheDeadlineReply(t *testing.T) {
	envelope := func(extra string) []byte {
		return []byte(`{"schema_version":1,"stdout":"","stderr":"","exit_code":-6,"timed_out":true,"stdout_truncated":false,"stderr_truncated":false` + extra + `}`)
	}
	answer := base64.StdEncoding.EncodeToString([]byte("answer"))
	for name, test := range map[string]struct {
		extra string
		reply string
		cut   bool
		fails bool
	}{
		"none":                  {},
		"whole":                 {extra: `,"deadline_reply":"` + answer + `","deadline_reply_size":6`, reply: "answer"},
		"cut":                   {extra: `,"deadline_reply":"` + answer + `","deadline_reply_size":7`, reply: "answer", cut: true},
		"empty":                 {extra: `,"deadline_reply":"","deadline_reply_size":0`},
		"without a size":        {extra: `,"deadline_reply":"` + answer + `"`, fails: true},
		"longer than announced": {extra: `,"deadline_reply":"` + answer + `","deadline_reply_size":5`, fails: true},
		"not base64":            {extra: `,"deadline_reply":"?","deadline_reply_size":1`, fails: true},
		"over the limit":        {extra: `,"deadline_reply":"` + base64.StdEncoding.EncodeToString(make([]byte, DeadlineReplyLimit+1)) + `","deadline_reply_size":70000`, fails: true},
	} {
		t.Run(name, func(t *testing.T) {
			report, err := parseReport(t.Context(), "probe", "marker", 0, envelope(test.extra), -1, -1, nil)
			if (err != nil) != test.fails || string(report.DeadlineReply) != test.reply || report.DeadlineReplyCut != test.cut {
				t.Fatalf("report %q cut=%t, %v", report.DeadlineReply, report.DeadlineReplyCut, err)
			}
		})
	}
}

// escapedWriter starts a descendant in a session of its own, which the
// supervisor's group kill does not reach, that writes to both inherited pipes
// without pause, records the descendant's PID and waits.
const escapedWriter = `import os,subprocess,sys,time
writer="import os\nwhile True:\n os.write(1,b'o'*4096); os.write(2,b'e'*4096)"
child=subprocess.Popen([sys.executable,"-c",writer],start_new_session=True)
with open(sys.argv[1]+".tmp","w") as f: f.write(str(child.pid))
os.rename(sys.argv[1]+".tmp",sys.argv[1])
time.sleep(30)`

// A descendant that left the command's group and keeps writing into the
// command's pipes does not hold the supervisor: at the deadline it stops the
// command, reads what is there for a bounded time, reports and removes its
// marker.
func TestSupervisorDeadlineStopIsBoundedUnderAnEscapedWriter(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("signaling the group of an exited, unreaped child is refused outside Linux")
	}
	python := installedPython(t)
	for name, env := range map[string][]string{
		"deadline signal": {roleTermGrace + "=0.3", deadlineSignalVariable + "=6"},
		"signal ignored":  {roleTermGrace + "=0.3", deadlineSignalVariable + "=28"},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			pidPath, marker := filepath.Join(dir, "writer.pid"), filepath.Join(dir, "markers", "writer")
			// The writer ends on its own once the supervisor closed its
			// pipes; a supervisor that failed to return leaves it running.
			t.Cleanup(func() {
				if data, err := os.ReadFile(pidPath); err == nil {
					if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil && pid > 0 {
						_ = syscall.Kill(pid, syscall.SIGKILL)
					}
				}
			})
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, python, "-c", Script, "4096", "4096", "0.5", python, "-c", escapedWriter, pidPath)
			cmd.Env = append(append(os.Environ(), MarkerVariable+"="+marker), env...)
			started := time.Now()
			output, err := cmd.Output()
			took := time.Since(started)
			// Deadline 0.5 s, grace 0.3 s, drain and reaping 0.2 s each.
			if err != nil || took > 3*time.Second {
				t.Fatalf("supervisor returned after %s: %v; %.200q", took, err, output)
			}
			var envelope struct {
				Stdout, Stderr  string
				TimedOut        bool `json:"timed_out"`
				StdoutTruncated bool `json:"stdout_truncated"`
			}
			if err := json.Unmarshal(output, &envelope); err != nil || !envelope.TimedOut || !envelope.StdoutTruncated ||
				len(envelope.Stdout) > 8192 || len(envelope.Stderr) > 8192 {
				t.Fatalf("envelope: %.300q, %v", output, err)
			}
			if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("the supervisor kept its marker: %v", err)
			}
			if _, err := os.Stat(pidPath); err != nil {
				t.Fatalf("the writer did not start: %v", err)
			}
		})
	}
}
