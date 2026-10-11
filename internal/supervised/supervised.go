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
)

// Script is the supervisor. It runs its arguments after the three limits
// (stdout bytes, stderr bytes, seconds) as a child in a new process group,
// bounds each output stream, kills only that group at the deadline, and prints
// one JSON envelope. Native stdout stays separate from Ceph's independently
// nonblocking stderr pipe.
//
// The supervisor claims its invocation's marker before it starts the child,
// and stops at once if CancelScript claimed it first. While the
// child runs, a cancel marker makes the supervisor kill the child's group,
// which it still owns because the child is not yet reaped. Removing the
// marker after the reap acknowledges the cancellation.
const Script = `import base64,json,os,select,signal,subprocess,sys,time
limits=[int(sys.argv[1]),int(sys.argv[2])]; deadline=time.monotonic()+float(sys.argv[3])
marker=os.environ["TC_SUPERVISED"]; cancel_marker=marker+".cancel"
os.makedirs(os.path.dirname(marker),mode=0o700,exist_ok=True)
try: os.close(os.open(marker,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600))
except FileExistsError:
 os.unlink(marker)
 sys.exit(0)
output=[bytearray(),bytearray()]; clipped=[False,False]; timed_out=False; group_signaled=False
def kill_owned_group():
 global group_signaled
 if group_signaled: return
 try: os.killpg(p.pid,signal.SIGKILL)
 except ProcessLookupError: pass
 group_signaled=True
try:
 p=subprocess.Popen(sys.argv[4:],stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
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
  p.stdout.close()
  p.stderr.close()
finally:
 for path in (marker,cancel_marker):
  try: os.unlink(path)
  except FileNotFoundError: pass
print(json.dumps({'schema_version':1,'stdout':base64.b64encode(output[0]).decode(),'stderr':base64.b64encode(output[1]).decode(),'exit_code':code,'timed_out':timed_out,'stdout_truncated':clipped[0],'stderr_truncated':clipped[1]}))
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

// Result is the complete output of one supervised command.
type Result struct {
	Stdout, Stderr []byte
	ExitCode       int
}

// Command runs argv in ctr under Script, which kills the command's process
// group at its deadline and bounds each output stream. The deadline is
// processLimit, shortened to the caller's deadline. When Exec ends before the
// supervisor does, which includes a manual cancellation, Cancel stops this
// invocation inside the container before Command returns, whether its
// supervisor is running or has not started yet; when Cancel fails, the error
// holds a NotStoppedError. label names the command in errors. A timed-out or
// truncated run is an error; a nonzero exit is returned in the result.
func Command(ctx context.Context, ctr testcontainers.Container, label string, processLimit time.Duration, stdoutLimit, stderrLimit int, argv ...string) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if ctr == nil {
		return Result{}, errors.New("ceph control container is unavailable")
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
		return Result{}, context.DeadlineExceeded
	}
	command := append([]string{"python3", "-c", Script,
		strconv.Itoa(stdoutLimit), strconv.Itoa(stderrLimit), strconv.FormatFloat(seconds, 'f', 9, 64)}, argv...)
	marker := MarkerDir + "/" + uuid.NewString()
	code, reader, err := ctr.Exec(attempt, command, tcexec.Multiplexed(), tcexec.WithEnv([]string{MarkerVariable + "=" + marker}))
	if err != nil {
		// Exec stops waiting when attempt ends, but Docker may already have
		// accepted the exec and start or keep running the supervisor.
		err = fmt.Errorf("exec %s supervisor: %w", label, err)
		if cancelErr := Cancel(ctx, ctr, label, marker); cancelErr != nil {
			return Result{}, errors.Join(err, &NotStoppedError{Marker: marker, Err: cancelErr})
		}
		return Result{}, err
	}
	if err := attempt.Err(); err != nil {
		return Result{}, err
	}
	if reader == nil {
		return Result{}, fmt.Errorf("%s supervisor returned no output", label)
	}
	if closer, ok := reader.(io.Closer); ok {
		defer func() { _ = closer.Close() }()
	}
	// Both streams are bounded inside the container before Testcontainers buffers.
	envelopeLimit := (stdoutLimit+stderrLimit)*2 + 4096
	data, err := io.ReadAll(io.LimitReader(reader, int64(envelopeLimit)+1))
	if ctxErr := attempt.Err(); ctxErr != nil {
		return Result{}, ctxErr
	}
	if err != nil {
		return Result{}, fmt.Errorf("read %s supervisor: %w", label, err)
	}
	if len(data) > envelopeLimit || code != 0 {
		return Result{}, fmt.Errorf("%s supervisor failed: exit=%d oversized=%t", label, code, len(data) > envelopeLimit)
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
		return Result{}, fmt.Errorf("invalid %s supervisor protocol", label)
	}
	stdout, stdoutErr := base64.StdEncoding.DecodeString(*envelope.Stdout)
	stderr, stderrErr := base64.StdEncoding.DecodeString(*envelope.Stderr)
	if stdoutErr != nil || stderrErr != nil {
		return Result{}, fmt.Errorf("invalid %s supervisor encoding", label)
	}
	if *envelope.TimedOut {
		return Result{}, fmt.Errorf("%s process timed out: %w", label, context.DeadlineExceeded)
	}
	if *envelope.StdoutTruncated || *envelope.StderrTruncated || len(stdout) > stdoutLimit || len(stderr) > stderrLimit {
		return Result{}, fmt.Errorf("%s process output was truncated", label)
	}
	if err := attempt.Err(); err != nil {
		return Result{}, err
	}
	return Result{Stdout: stdout, Stderr: stderr, ExitCode: *envelope.ExitCode}, nil
}

// CancelScript cancels one invocation through its marker and
// signals no process itself. If it claims the marker first, the supervisor has
// not started, and if Docker starts it later it finds the claim, removes it and
// starts nothing. Otherwise it writes the cancel marker and waits until the
// supervisor removes the marker, which it does only after killing and reaping
// its child's group, or after its child ended on its own. A claim made after
// the supervisor finished stays as an empty file in MarkerDir.
const CancelScript = `import os,sys,time
marker=sys.argv[1]; cancel_marker=marker+".cancel"; deadline=time.monotonic()+float(sys.argv[2])
os.makedirs(os.path.dirname(marker),mode=0o700,exist_ok=True)
try:
 os.close(os.open(marker,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600))
 sys.exit(0)
except FileExistsError: pass
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
		return fmt.Errorf("cancel %s command: %w", label, err)
	}
	if code != 0 {
		return fmt.Errorf("cancel %s command: canceler exited %d", label, code)
	}
	return nil
}
