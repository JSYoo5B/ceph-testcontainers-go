package ceph

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const (
	// supervisedMarkerVariable passes the supervisor its invocation's marker
	// path, below supervisedMarkerDir; see supervisedCancelScript.
	supervisedMarkerVariable    = "TC_SUPERVISED"
	supervisedMarkerDir         = "/tmp/tc-supervised"
	supervisedCancelTimeout     = 10 * time.Second
	monitorQuorumProcessTimeout = 5 * time.Second
	monitorQuorumExecCushion    = 250 * time.Millisecond
	monitorQuorumStdoutLimit    = 1 << 20
	monitorQuorumStderrLimit    = 64 << 10
)

// The CLI's connection timeout does not bound command discovery, execution or
// shutdown. Supervise only this read's child group, keeping native JSON stdout
// separate from Ceph's independently nonblocking stderr pipe.
//
// The supervisor claims its invocation's marker before it starts the child,
// and stops at once if supervisedCancelScript claimed it first. While the
// child runs, a cancel marker makes the supervisor kill the child's group,
// which it still owns because the child is not yet reaped. Removing the
// marker after the reap acknowledges the cancellation.
const monitorQuorumCommandScript = `import base64,json,os,select,signal,subprocess,sys,time
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

func queryMonitorQuorum(ctx context.Context, control testcontainers.Container) (QuorumStatus, error) {
	var status QuorumStatus
	data, err := monitorQuorumCommand(ctx, control, monitorQuorumProcessTimeout)
	if err != nil {
		return status, err
	}
	// Missing/null native arrays are not an observation of an empty membership.
	// Empty arrays remain a valid native observation for quorum wait predicates.
	var shape struct {
		QuorumNames *[]string `json:"quorum_names"`
		MonMap      *struct {
			Mons *[]struct {
				Name *string `json:"name"`
			} `json:"mons"`
		} `json:"monmap"`
	}
	if err := json.Unmarshal(data, &shape); err != nil || shape.QuorumNames == nil || shape.MonMap == nil || shape.MonMap.Mons == nil {
		return status, errors.New("invalid native monitor quorum schema")
	}
	members := make(map[string]bool)
	for _, member := range *shape.MonMap.Mons {
		if member.Name == nil || *member.Name == "" || members[*member.Name] {
			return status, errors.New("invalid native monitor membership names")
		}
		members[*member.Name] = true
	}
	quorum := make(map[string]bool)
	for _, name := range *shape.QuorumNames {
		if !members[name] || quorum[name] {
			return status, errors.New("invalid native monitor quorum names")
		}
		quorum[name] = true
	}
	if err := json.Unmarshal(data, &status); err != nil {
		return QuorumStatus{}, fmt.Errorf("decode native monitor quorum: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return QuorumStatus{}, err
	}
	return status, nil
}

func monitorQuorumCommand(ctx context.Context, control testcontainers.Container, processLimit time.Duration) ([]byte, error) {
	if processLimit <= 0 || processLimit > monitorQuorumProcessTimeout {
		return nil, errors.New("invalid monitor quorum process limit")
	}
	result, err := supervisedCommand(ctx, control, "monitor quorum", processLimit, monitorQuorumStdoutLimit, monitorQuorumStderrLimit,
		"ceph", "--connect-timeout", "5", "quorum_status", "--format", "json")
	if err != nil {
		return nil, err
	}
	if result.exitCode != 0 {
		return nil, fmt.Errorf("quorum_status exited %d: %s", result.exitCode, strings.TrimSpace(string(result.stdout)+string(result.stderr)))
	}
	return result.stdout, nil
}

// supervisedResult is the complete output of one supervised command.
type supervisedResult struct {
	stdout, stderr []byte
	exitCode       int
}

// supervisedCommand runs argv in ctr under the Python supervisor, which kills
// the command's process group at its deadline and bounds each output stream.
// The deadline is processLimit, shortened to the caller's deadline. When Exec
// ends before the supervisor does, which includes a manual cancellation,
// cancelSupervised stops this invocation inside the container before
// supervisedCommand returns, whether its supervisor is running or has not
// started yet. label names the command in errors. A timed-out or truncated run
// is an error; a nonzero exit is returned in the result.
func supervisedCommand(ctx context.Context, ctr testcontainers.Container, label string, processLimit time.Duration, stdoutLimit, stderrLimit int, argv ...string) (supervisedResult, error) {
	if err := ctx.Err(); err != nil {
		return supervisedResult{}, err
	}
	if ctr == nil {
		return supervisedResult{}, errors.New("ceph control container is unavailable")
	}
	// The child expires first, leaving time to kill/reap and return its envelope.
	// WithTimeout also preserves an earlier caller deadline without extending it.
	attempt, cancel := context.WithTimeout(ctx, processLimit+monitorQuorumExecCushion)
	defer cancel()
	deadline, _ := attempt.Deadline()
	remaining := time.Until(deadline)
	cushion := min(monitorQuorumExecCushion, remaining/4)
	seconds := min(processLimit, remaining-cushion).Seconds()
	if seconds <= 0 {
		return supervisedResult{}, context.DeadlineExceeded
	}
	command := append([]string{"python3", "-c", monitorQuorumCommandScript,
		strconv.Itoa(stdoutLimit), strconv.Itoa(stderrLimit), strconv.FormatFloat(seconds, 'f', 9, 64)}, argv...)
	marker := supervisedMarkerDir + "/" + uuid.NewString()
	code, reader, err := ctr.Exec(attempt, command, tcexec.Multiplexed(), tcexec.WithEnv([]string{supervisedMarkerVariable + "=" + marker}))
	if err != nil {
		// Exec stops waiting when attempt ends, but Docker may already have
		// accepted the exec and start or keep running the supervisor.
		return supervisedResult{}, errors.Join(fmt.Errorf("exec %s supervisor: %w", label, err), cancelSupervised(ctx, ctr, label, marker))
	}
	if err := attempt.Err(); err != nil {
		return supervisedResult{}, err
	}
	if reader == nil {
		return supervisedResult{}, fmt.Errorf("%s supervisor returned no output", label)
	}
	if closer, ok := reader.(io.Closer); ok {
		defer func() { _ = closer.Close() }()
	}
	// Both streams are bounded inside the container before Testcontainers buffers.
	envelopeLimit := (stdoutLimit+stderrLimit)*2 + 4096
	data, err := io.ReadAll(io.LimitReader(reader, int64(envelopeLimit)+1))
	if ctxErr := attempt.Err(); ctxErr != nil {
		return supervisedResult{}, ctxErr
	}
	if err != nil {
		return supervisedResult{}, fmt.Errorf("read %s supervisor: %w", label, err)
	}
	if len(data) > envelopeLimit || code != 0 {
		return supervisedResult{}, fmt.Errorf("%s supervisor failed: exit=%d oversized=%t", label, code, len(data) > envelopeLimit)
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
		return supervisedResult{}, fmt.Errorf("invalid %s supervisor protocol", label)
	}
	stdout, stdoutErr := base64.StdEncoding.DecodeString(*envelope.Stdout)
	stderr, stderrErr := base64.StdEncoding.DecodeString(*envelope.Stderr)
	if stdoutErr != nil || stderrErr != nil {
		return supervisedResult{}, fmt.Errorf("invalid %s supervisor encoding", label)
	}
	if *envelope.TimedOut {
		return supervisedResult{}, fmt.Errorf("%s process timed out: %w", label, context.DeadlineExceeded)
	}
	if *envelope.StdoutTruncated || *envelope.StderrTruncated || len(stdout) > stdoutLimit || len(stderr) > stderrLimit {
		return supervisedResult{}, fmt.Errorf("%s process output was truncated", label)
	}
	if err := attempt.Err(); err != nil {
		return supervisedResult{}, err
	}
	return supervisedResult{stdout: stdout, stderr: stderr, exitCode: *envelope.ExitCode}, nil
}

// supervisedCancelScript cancels one invocation through its marker and
// signals no process itself. If it claims the marker first, the supervisor has
// not started, and if Docker starts it later it finds the claim, removes it and
// starts nothing. Otherwise it writes the cancel marker and waits until the
// supervisor removes the marker, which it does only after killing and reaping
// its child's group, or after its child ended on its own. A claim made after
// the supervisor finished stays as an empty file in supervisedMarkerDir.
const supervisedCancelScript = `import os,sys,time
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

// cancelSupervised stops the invocation that owns marker in ctr and returns
// once it can no longer run its command. It uses its own bounded context,
// because the caller's may have ended.
func cancelSupervised(ctx context.Context, ctr testcontainers.Container, label, marker string) error {
	cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), supervisedCancelTimeout)
	defer cancel()
	limit := strconv.FormatFloat((supervisedCancelTimeout / 2).Seconds(), 'f', -1, 64)
	code, reader, err := ctr.Exec(cancelCtx, []string{"python3", "-c", supervisedCancelScript, marker, limit}, tcexec.Multiplexed())
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
