package cluster

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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if control == nil {
		return nil, errors.New("ceph control container is unavailable")
	}
	if processLimit <= 0 || processLimit > monitorQuorumProcessTimeout {
		return nil, errors.New("invalid monitor quorum process limit")
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
		return nil, context.DeadlineExceeded
	}
	argv := []string{"python3", "-c", monitorQuorumCommandScript,
		strconv.Itoa(monitorQuorumStdoutLimit), strconv.Itoa(monitorQuorumStderrLimit),
		strconv.FormatFloat(seconds, 'f', 9, 64), "ceph", "--connect-timeout", "5", "quorum_status", "--format", "json"}
	code, reader, err := control.Exec(attempt, argv, tcexec.Multiplexed())
	if err != nil {
		return nil, fmt.Errorf("exec monitor quorum supervisor: %w", err)
	}
	if err := attempt.Err(); err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, errors.New("monitor quorum supervisor returned no output")
	}
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}
	// Both streams are bounded inside the container before Testcontainers buffers.
	envelopeLimit := (monitorQuorumStdoutLimit+monitorQuorumStderrLimit)*2 + 4096
	data, err := io.ReadAll(io.LimitReader(reader, int64(envelopeLimit)+1))
	if ctxErr := attempt.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if err != nil {
		return nil, fmt.Errorf("read monitor quorum supervisor: %w", err)
	}
	if len(data) > envelopeLimit || code != 0 {
		return nil, fmt.Errorf("monitor quorum supervisor failed: exit=%d oversized=%t", code, len(data) > envelopeLimit)
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
		return nil, errors.New("invalid monitor quorum supervisor protocol")
	}
	stdout, stdoutErr := base64.StdEncoding.DecodeString(*result.Stdout)
	stderr, stderrErr := base64.StdEncoding.DecodeString(*result.Stderr)
	if stdoutErr != nil || stderrErr != nil {
		return nil, errors.New("invalid monitor quorum supervisor encoding")
	}
	if *result.TimedOut {
		return nil, fmt.Errorf("monitor quorum process timed out: %w", context.DeadlineExceeded)
	}
	if *result.StdoutTruncated || *result.StderrTruncated || len(stdout) > monitorQuorumStdoutLimit || len(stderr) > monitorQuorumStderrLimit {
		return nil, errors.New("monitor quorum process output was truncated")
	}
	if *result.ExitCode != 0 {
		return nil, fmt.Errorf("quorum_status exited %d: %s", *result.ExitCode, strings.TrimSpace(string(stdout)+string(stderr)))
	}
	if err := attempt.Err(); err != nil {
		return nil, err
	}
	return stdout, nil
}
