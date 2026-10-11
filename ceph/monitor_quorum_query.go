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

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const (
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
p=subprocess.Popen(sys.argv[4:],stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
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
// the command's process group when processLimit or ctx ends first and bounds
// each output stream. label names the command in errors. A timed-out or
// truncated run is an error; a nonzero exit is returned in the result.
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
	code, reader, err := ctr.Exec(attempt, command, tcexec.Multiplexed())
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
	var result struct {
		SchemaVersion   *int    `json:"schema_version"`
		Stdout          *string `json:"stdout"`
		Stderr          *string `json:"stderr"`
		ExitCode        *int    `json:"exit_code"`
		TimedOut        *bool   `json:"timed_out"`
		StdoutTruncated *bool   `json:"stdout_truncated"`
		StderrTruncated *bool   `json:"stderr_truncated"`
	}
	if err := json.Unmarshal(data, &result); err != nil || result.SchemaVersion == nil || *result.SchemaVersion != 1 || result.Stdout == nil || result.Stderr == nil || result.ExitCode == nil || result.TimedOut == nil || result.StdoutTruncated == nil || result.StderrTruncated == nil {
		return supervisedResult{}, fmt.Errorf("invalid %s supervisor protocol", label)
	}
	stdout, stdoutErr := base64.StdEncoding.DecodeString(*result.Stdout)
	stderr, stderrErr := base64.StdEncoding.DecodeString(*result.Stderr)
	if stdoutErr != nil || stderrErr != nil {
		return supervisedResult{}, fmt.Errorf("invalid %s supervisor encoding", label)
	}
	if *result.TimedOut {
		return supervisedResult{}, fmt.Errorf("%s process timed out: %w", label, context.DeadlineExceeded)
	}
	if *result.StdoutTruncated || *result.StderrTruncated || len(stdout) > stdoutLimit || len(stderr) > stderrLimit {
		return supervisedResult{}, fmt.Errorf("%s process output was truncated", label)
	}
	if err := attempt.Err(); err != nil {
		return supervisedResult{}, err
	}
	return supervisedResult{stdout: stdout, stderr: stderr, exitCode: *result.ExitCode}, nil
}
