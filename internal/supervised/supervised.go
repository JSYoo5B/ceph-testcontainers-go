// Package supervised runs a command inside a container under a Python
// supervisor that bounds its run time and output, and that a canceled caller
// can stop without leaving the command behind. The container needs python3.
package supervised

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const (
	// MarkerVariable passes the supervisor its invocation's marker path, below
	// MarkerDir; see CancelScript.
	MarkerVariable = "TC_SUPERVISED"
	MarkerDir      = "/tmp/tc-supervised"
	// CancelTimeout bounds one Cancel.
	CancelTimeout = 10 * time.Second
	// ExecCushion is the time Command leaves after the supervisor's deadline
	// for it to kill and reap its child and report.
	ExecCushion = 250 * time.Millisecond
	// LaunchFailedExit is the supervisor's exit code when it could not start
	// the command, for example because the program does not exist or is not
	// executable.
	LaunchFailedExit = 120
	// CleanupFailedExit is the supervisor's exit code when it could not kill
	// and reap the command's group; it then keeps its marker, so that no
	// cancellation is confirmed and the command may still run.
	CleanupFailedExit = 121
)

// Script is the supervisor. It runs its arguments after the three limits
// (stdout bytes, stderr bytes, seconds) as a child in a new process group,
// bounds each output stream, kills only that group at the deadline, and prints
// one JSON envelope. Native stdout stays separate from Ceph's independently
// nonblocking stderr pipe. When it cannot start the command it exits
// LaunchFailedExit without an envelope, and when it cannot kill or reap the
// started command's group it exits CleanupFailedExit and keeps its marker.
//
// The supervisor claims its invocation's marker before it starts the child,
// writing the container instance into it, and stops at once if CancelScript
// claimed it first. While the child runs, a cancel marker makes the
// supervisor kill the child's group, which it still owns because the child is
// not yet reaped. Removing the marker after the reap acknowledges the
// cancellation.
//
// On Linux the supervisor is a child subreaper: members of the killed group
// whose parent died become its children, and it reaps them before it
// reports, for at most 200 ms while members remain. It does not wait for a
// descendant that left the group.
//
// The marker record ends with a newline, so that a canceler never takes a
// partly written record for one of another instance.
const Script = `import base64,json,os,select,signal,subprocess,sys,time
` + instanceFunction + `limits=[int(sys.argv[1]),int(sys.argv[2])]; deadline=time.monotonic()+float(sys.argv[3])
marker=os.environ["TC_SUPERVISED"]; cancel_marker=marker+".cancel"
os.makedirs(os.path.dirname(marker),mode=0o700,exist_ok=True)
try: fd=os.open(marker,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
except FileExistsError:
 os.unlink(marker)
 sys.exit(0)
os.write(fd,b"run "+instance()+b"\n"); os.close(fd)
try:
 import ctypes
 ctypes.CDLL(None).prctl(36,1,0,0,0)
except Exception: pass
def reap_descendants(group):
 until=time.monotonic()+0.2
 while True:
  try:
   while os.waitpid(-1,os.WNOHANG)[0]: pass
  except ChildProcessError: return
  try: os.killpg(group,0)
  except OSError: return
  if time.monotonic()>=until: return
  time.sleep(0.005)
output=[bytearray(),bytearray()]; clipped=[False,False]; timed_out=False; group_signaled=False; launched=False; cleaned=False
def kill_owned_group():
 global group_signaled
 if group_signaled: return
 try: os.killpg(p.pid,signal.SIGKILL)
 except ProcessLookupError: pass
 group_signaled=True
try:
 try: p=subprocess.Popen(sys.argv[4:],stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
 except OSError: sys.exit(120)
 launched=True
 streams={p.stdout:0,p.stderr:1}
 try:
  while streams:
   remaining=deadline-time.monotonic()
   if remaining<=0:
    timed_out=True
    kill_owned_group()
    break
   if os.path.exists(cancel_marker):
    kill_owned_group()
    break
   for stream in select.select(list(streams),[],[],min(remaining,0.1))[0]:
    data=os.read(stream.fileno(),32768)
    if not data:
     del streams[stream]
     continue
    channel=streams[stream]; room=max(0,limits[channel]-len(output[channel]))
    output[channel].extend(data[:room]); clipped[channel]=clipped[channel] or len(data)>room
  # Observe exit without releasing the child's PID/PGID identity. Reaping or
  # Popen.poll() before group cleanup could signal a subsequently reused group.
  while not group_signaled:
   remaining=deadline-time.monotonic()
   if remaining<=0:
    timed_out=True
    break
   if os.path.exists(cancel_marker):
    break
   if os.waitid(os.P_PID,p.pid,os.WEXITED|os.WNOHANG|os.WNOWAIT) is not None:
    break
   time.sleep(min(remaining,0.01))
  kill_owned_group()
  code=p.wait()
 finally:
  kill_owned_group()
  p.wait()
  reap_descendants(p.pid)
  cleaned=True
  p.stdout.close()
  p.stderr.close()
finally:
 if launched and not cleaned: os._exit(121)
 for path in (marker,cancel_marker):
  try: os.unlink(path)
  except FileNotFoundError: pass
print(json.dumps({'schema_version':1,'stdout':base64.b64encode(output[0]).decode(),'stderr':base64.b64encode(output[1]).decode(),'exit_code':code,'timed_out':timed_out,'stdout_truncated':clipped[0],'stderr_truncated':clipped[1]}))
`

// instanceFunction defines instance(), which identifies the running container
// instance by the host's boot ID and the start time of the container's PID 1.
// A restart starts a new PID 1, and every process of the old instance has
// ended. It returns b"" when /proc does not tell.
const instanceFunction = `def instance():
 try:
  with open("/proc/sys/kernel/random/boot_id","rb") as f: boot=f.read().strip()
  with open("/proc/1/stat","rb") as f: stat=f.read()
  return boot+b"/"+stat[stat.rindex(b")")+2:].split()[19]
 except (OSError,ValueError,IndexError): return b""
`

// NotStoppedError reports an invocation that Command could not confirm as
// stopped after Exec ended early: its command may still run in the container.
// A later Cancel with Marker that succeeds confirms that it no longer can.
type NotStoppedError struct {
	Marker string
	Err    error
}

func (e *NotStoppedError) Error() string { return e.Err.Error() }

func (e *NotStoppedError) Unwrap() error { return e.Err }

// ExitError reports a supervisor that exited without a report. Code 126 or
// 127 means that the container could not run python3, and LaunchFailedExit
// that the supervisor could not start the command. A code above 128 means
// that a signal killed the supervisor before its cleanup, and
// CleanupFailedExit that its cleanup failed; then its command may still run,
// and Cancel with Marker can confirm that only after the container restarted.
type ExitError struct {
	Label     string
	Code      int
	Oversized bool
	Marker    string
}

func (e *ExitError) Error() string {
	return fmt.Sprintf("%s supervisor failed: exit=%d oversized=%t", e.Label, e.Code, e.Oversized)
}

// Unconfirmed returns the marker of the invocation whose end err leaves
// unconfirmed: Cancel could not stop it, or a signal killed its supervisor
// before its cleanup. Cancel with the marker confirms it later.
func Unconfirmed(err error) (string, bool) {
	var notStopped *NotStoppedError
	if errors.As(err, &notStopped) {
		return notStopped.Marker, true
	}
	var exit *ExitError
	if errors.As(err, &exit) && (exit.Code < 0 || exit.Code > 128 || exit.Code == CleanupFailedExit) {
		return exit.Marker, true
	}
	return "", false
}

// BackendError is an error of Docker or of a container implementation during
// Op. Its text holds the cause's text, which this package does not control.
type BackendError struct {
	Op  string
	Err error
}

func (e *BackendError) Error() string { return e.Op + ": " + e.Err.Error() }

func (e *BackendError) Unwrap() error { return e.Err }

// Result is the complete output of one supervised command.
type Result struct {
	Stdout, Stderr []byte
	ExitCode       int
}

// Report is the supervisor's complete report of one run.
type Report struct {
	Stdout, Stderr []byte
	ExitCode       int
	// TimedOut means that the supervisor killed the command's process group
	// at its deadline; the output is what the command wrote until then.
	TimedOut bool
	// StdoutTruncated and StderrTruncated mean that a stream exceeded its
	// limit; the output holds its first limit bytes.
	StdoutTruncated, StderrTruncated bool
}

// Command runs argv like Run and returns its result only for a run that
// completed within its deadline and output limits; a timed-out or truncated
// run is an error. A nonzero exit is returned in the result.
func Command(ctx context.Context, ctr testcontainers.Container, label string, processLimit time.Duration, stdoutLimit, stderrLimit int, argv ...string) (Result, error) {
	report, err := Run(ctx, ctr, label, processLimit, stdoutLimit, stderrLimit, argv...)
	if err != nil {
		return Result{}, err
	}
	if report.TimedOut {
		return Result{}, fmt.Errorf("%s process timed out: %w", label, context.DeadlineExceeded)
	}
	if report.StdoutTruncated || report.StderrTruncated {
		return Result{}, fmt.Errorf("%s process output was truncated", label)
	}
	return Result{Stdout: report.Stdout, Stderr: report.Stderr, ExitCode: report.ExitCode}, nil
}

// Run runs argv in ctr under Script, which kills the command's process group
// at its deadline and bounds each output stream. The deadline is
// processLimit, shortened to the caller's deadline. When Exec ends before the
// supervisor does, which includes a manual cancellation, Cancel stops this
// invocation inside the container before Run returns, whether its supervisor
// is running or has not started yet; when Cancel fails, the error holds a
// NotStoppedError. label names the command in errors. Run returns the
// supervisor's report, also for a run that timed out or was truncated, and an
// error only when it has no complete report.
func Run(ctx context.Context, ctr testcontainers.Container, label string, processLimit time.Duration, stdoutLimit, stderrLimit int, argv ...string) (Report, error) {
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	if ctr == nil {
		return Report{}, errors.New("ceph control container is unavailable")
	}
	// The child expires first, leaving time to kill/reap and return its envelope.
	// WithTimeout also preserves an earlier caller deadline without extending it.
	attempt, cancel := context.WithTimeout(ctx, processLimit+ExecCushion)
	defer cancel()
	deadline, _ := attempt.Deadline()
	remaining := time.Until(deadline)
	cushion := min(ExecCushion, remaining/4)
	seconds := min(processLimit, remaining-cushion).Seconds()
	if seconds <= 0 {
		return Report{}, context.DeadlineExceeded
	}
	command := append([]string{"python3", "-c", Script,
		strconv.Itoa(stdoutLimit), strconv.Itoa(stderrLimit), strconv.FormatFloat(seconds, 'f', 9, 64)}, argv...)
	marker := MarkerDir + "/" + uuid.NewString()
	code, reader, err := ctr.Exec(attempt, command, tcexec.Multiplexed(), tcexec.WithEnv([]string{MarkerVariable + "=" + marker}))
	if err != nil {
		// Exec stops waiting when attempt ends, but Docker may already have
		// accepted the exec and start or keep running the supervisor.
		err = &BackendError{Op: "exec " + label + " supervisor", Err: err}
		if cancelErr := Cancel(ctx, ctr, label, marker); cancelErr != nil {
			return Report{}, errors.Join(err, &NotStoppedError{Marker: marker, Err: cancelErr})
		}
		return Report{}, err
	}
	// A signal killed the supervisor, or its cleanup failed, so its command
	// may still run, whatever happened to attempt meanwhile.
	if code < 0 || code > 128 || code == CleanupFailedExit {
		return Report{}, errors.Join(&ExitError{Label: label, Code: code, Marker: marker}, attempt.Err())
	}
	if err := attempt.Err(); err != nil {
		return Report{}, err
	}
	if reader == nil {
		return Report{}, fmt.Errorf("%s supervisor returned no output", label)
	}
	if closer, ok := reader.(io.Closer); ok {
		defer func() { _ = closer.Close() }()
	}
	// Both streams are bounded inside the container before Testcontainers buffers.
	envelopeLimit := (stdoutLimit+stderrLimit)*2 + 4096
	data, err := io.ReadAll(io.LimitReader(reader, int64(envelopeLimit)+1))
	if ctxErr := attempt.Err(); ctxErr != nil {
		return Report{}, ctxErr
	}
	if err != nil {
		return Report{}, &BackendError{Op: "read " + label + " supervisor", Err: err}
	}
	if len(data) > envelopeLimit || code != 0 {
		return Report{}, &ExitError{Label: label, Code: code, Oversized: len(data) > envelopeLimit, Marker: marker}
	}
	var envelope struct {
		SchemaVersion   *int    `json:"schema_version"`
		Stdout          *string `json:"stdout"`
		Stderr          *string `json:"stderr"`
		ExitCode        *int    `json:"exit_code"`
		TimedOut        *bool   `json:"timed_out"`
		StdoutTruncated *bool   `json:"stdout_truncated"`
		StderrTruncated *bool   `json:"stderr_truncated"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil || envelope.SchemaVersion == nil || *envelope.SchemaVersion != 1 || envelope.Stdout == nil || envelope.Stderr == nil || envelope.ExitCode == nil || envelope.TimedOut == nil || envelope.StdoutTruncated == nil || envelope.StderrTruncated == nil {
		return Report{}, fmt.Errorf("invalid %s supervisor protocol", label)
	}
	stdout, stdoutErr := base64.StdEncoding.DecodeString(*envelope.Stdout)
	stderr, stderrErr := base64.StdEncoding.DecodeString(*envelope.Stderr)
	if stdoutErr != nil || stderrErr != nil {
		return Report{}, fmt.Errorf("invalid %s supervisor encoding", label)
	}
	if err := attempt.Err(); err != nil {
		return Report{}, err
	}
	report := Report{Stdout: stdout, Stderr: stderr, ExitCode: *envelope.ExitCode, TimedOut: *envelope.TimedOut,
		StdoutTruncated: *envelope.StdoutTruncated || len(stdout) > stdoutLimit, StderrTruncated: *envelope.StderrTruncated || len(stderr) > stderrLimit}
	if len(report.Stdout) > stdoutLimit {
		report.Stdout = report.Stdout[:stdoutLimit]
	}
	if len(report.Stderr) > stderrLimit {
		report.Stderr = report.Stderr[:stderrLimit]
	}
	return report, nil
}

// CancelScript cancels one invocation through its marker and
// signals no process itself. If it claims the marker first, the supervisor has
// not started, and if Docker starts it later it finds the claim, removes it and
// starts nothing. A marker that holds its own claim, or that a supervisor of an
// earlier container instance wrote, needs nothing more: that supervisor and
// its command ended with their instance, and the file still blocks a late
// start. Otherwise it writes the cancel marker and waits until the
// supervisor removes the marker, which it does only after killing and reaping
// its child's group, or after its child ended on its own. A claim made after
// the supervisor finished stays as a small file in MarkerDir.
const CancelScript = `import os,sys,time
` + instanceFunction + `marker=sys.argv[1]; cancel_marker=marker+".cancel"; deadline=time.monotonic()+float(sys.argv[2])
os.makedirs(os.path.dirname(marker),mode=0o700,exist_ok=True)
try:
 fd=os.open(marker,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
 os.write(fd,b"claim"); os.close(fd)
 sys.exit(0)
except FileExistsError: pass
try:
 with open(marker,"rb") as f: owner=f.read()
except FileNotFoundError: sys.exit(0)
current=instance()
recorded=owner[4:-1] if owner.startswith(b"run ") and owner.endswith(b"\n") else b""
if owner==b"claim" or (recorded and current and recorded!=current): sys.exit(0)
os.close(os.open(cancel_marker,os.O_CREAT|os.O_WRONLY,0o600))
while os.path.exists(marker):
 if time.monotonic()>deadline: sys.exit(3)
 time.sleep(0.01)
try: os.unlink(cancel_marker)
except FileNotFoundError: pass
`

// Cancel stops the invocation that owns marker in ctr and returns once it can
// no longer run its command. It uses its own bounded context, because the
// caller's may have ended.
func Cancel(ctx context.Context, ctr testcontainers.Container, label, marker string) error {
	cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), CancelTimeout)
	defer cancel()
	limit := strconv.FormatFloat((CancelTimeout / 2).Seconds(), 'f', -1, 64)
	code, reader, err := ctr.Exec(cancelCtx, []string{"python3", "-c", CancelScript, marker, limit}, tcexec.Multiplexed())
	if err == nil && reader != nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(reader, 1<<16))
	}
	if err != nil {
		return &BackendError{Op: "cancel " + label + " command", Err: err}
	}
	if code != 0 {
		return fmt.Errorf("cancel %s command: canceler exited %d", label, code)
	}
	return nil
}
