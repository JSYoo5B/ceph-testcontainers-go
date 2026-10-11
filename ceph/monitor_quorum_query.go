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
	// supervisedTokenVariable carries a per-invocation token into the
	// supervisor's environment, which its child inherits, so that a canceled
	// invocation's processes can be found and killed.
	supervisedTokenVariable     = "TC_SUPERVISED"
	supervisedCleanupTimeout    = 10 * time.Second
	monitorQuorumProcessTimeout = 5 * time.Second
	monitorQuorumExecCushion    = 250 * time.Millisecond
	monitorQuorumStdoutLimit    = 1 << 20
	monitorQuorumStderrLimit    = 64 << 10
)

// The CLI's connection timeout does not bound command discovery, execution or
// shutdown. Supervise only this read's child group, keeping native JSON stdout
// separate from Ceph's independently nonblocking stderr pipe.
const monitorQuorumCommandScript = `import base64,json,os,select,signal,subprocess,sys,time
limits=[int(sys.argv[1]),int(sys.argv[2])]; deadline=time.monotonic()+float(sys.argv[3])
output=[bytearray(),bytearray()]; clipped=[False,False]; timed_out=False; group_signaled=False
p=subprocess.Popen(sys.argv[4:],stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True,env=dict(os.environ,TC_SUPERVISED_ROLE="child"))
streams={p.stdout:0,p.stderr:1}
def kill_owned_group():
 global group_signaled
 if group_signaled: return
 try: os.killpg(p.pid,signal.SIGKILL)
 except ProcessLookupError: pass
 group_signaled=True
try:
 while streams:
  remaining=deadline-time.monotonic()
  if remaining<=0:
   timed_out=True
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
// The deadline is processLimit, shortened to the caller's deadline. When ctx
// ends before the supervisor reports, which includes a manual cancellation,
// a separate bounded cleanup kills this invocation's process group and
// supervisor inside the container, so canceled calls leave no processes
// behind. label names the command in errors. A timed-out or truncated run is
// an error; a nonzero exit is returned in the result.
func supervisedCommand(ctx context.Context, ctr testcontainers.Container, label string, processLimit time.Duration, stdoutLimit, stderrLimit int, argv ...string) (result supervisedResult, err error) {
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
	token := uuid.NewString()
	reported := false
	defer func() {
		// A complete envelope means the supervisor finished and killed its
		// child's group. Otherwise the invocation may still run in ctr.
		if err != nil && !reported {
			err = errors.Join(err, cleanupSupervised(ctx, ctr, label, token))
		}
	}()
	code, reader, err := ctr.Exec(attempt, command, tcexec.Multiplexed(), tcexec.WithEnv([]string{supervisedTokenVariable + "=" + token}))
	if err != nil {
		return supervisedResult{}, fmt.Errorf("exec %s supervisor: %w", label, err)
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
	reported = true
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

// supervisedCleanupScript kills the processes of one canceled invocation,
// found by its token in their environment: first the command's process groups,
// then, after a short grace in which the supervisor can reap them, the
// supervisor itself. It repeats until none remains or its deadline passes.
const supervisedCleanupScript = `import os,signal,sys,time
token=("TC_SUPERVISED="+sys.argv[1]).encode(); deadline=time.monotonic()+float(sys.argv[2]); grace=None
def scan():
 groups=set(); others=set()
 for name in os.listdir("/proc"):
  if not name.isdigit() or int(name)==os.getpid(): continue
  try:
   with open("/proc/"+name+"/environ","rb") as f: env=f.read().split(b"\0")
  except OSError: continue
  if token not in env: continue
  if b"TC_SUPERVISED_ROLE=child" in env:
   try: groups.add(os.getpgid(int(name)))
   except OSError: pass
  else: others.add(int(name))
 return groups,others
while True:
 groups,others=scan()
 if not groups and not others: sys.exit(0)
 for group in groups:
  try: os.killpg(group,signal.SIGKILL)
  except OSError: pass
 if not groups:
  grace=grace or time.monotonic()+1
  if time.monotonic()>=grace:
   for pid in others:
    try: os.kill(pid,signal.SIGKILL)
    except OSError: pass
 if time.monotonic()>deadline: sys.exit(3)
 time.sleep(0.02)
`

// cleanupSupervised kills a canceled invocation's processes in ctr. It uses
// its own bounded context, because the caller's may have ended.
func cleanupSupervised(ctx context.Context, ctr testcontainers.Container, label, token string) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), supervisedCleanupTimeout)
	defer cancel()
	limit := strconv.FormatFloat((supervisedCleanupTimeout / 2).Seconds(), 'f', -1, 64)
	code, reader, err := ctr.Exec(cleanupCtx, []string{"python3", "-c", supervisedCleanupScript, token, limit}, tcexec.Multiplexed())
	if err == nil && reader != nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(reader, 1<<16))
	}
	if err != nil {
		return fmt.Errorf("clean up canceled %s command: %w", label, err)
	}
	if code != 0 {
		return fmt.Errorf("clean up canceled %s command: cleanup exited %d", label, code)
	}
	return nil
}
