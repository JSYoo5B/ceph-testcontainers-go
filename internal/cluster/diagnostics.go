package cluster

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	dockercontainer "github.com/moby/moby/api/types/container"
	dockernetwork "github.com/moby/moby/api/types/network"
	dockerclient "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// DiagnosticsConfig bounds a read-only collection. Zero fields select a one
// minute total timeout, ten seconds per operation, 64 KiB per artifact, the last
// 1000 log lines, four workers and at most 128 containers. Additional containers
// are caller-owned clients or mirror daemons; collection never adopts them.
// RedactValues removes additional opaque application secrets. Redaction is best
// effort: arbitrary log contents still require review before sharing a report.
type DiagnosticsConfig struct {
	Timeout              time.Duration
	OperationTimeout     time.Duration
	MaxOutputBytes       int
	LogTail              int
	Concurrency          int
	MaxContainers        int
	AdditionalContainers []DiagnosticsContainer
	RedactValues         []string
}

// DiagnosticsContainer explicitly adds an external container to a report.
// Name and Role describe it; Container is only inspected and its logs read.
type DiagnosticsContainer struct {
	Role      string
	Name      string
	Container testcontainers.Container
}

// DiagnosticArtifact retains one observation, including incomplete output or
// an individual failure. Data is text; inspect observations are JSON when not
// truncated. Names and order are stable, but observations are not transactional.
type DiagnosticArtifact struct {
	Kind        string `json:"kind"`
	Name        string `json:"name"`
	Role        string `json:"role,omitempty"`
	ContainerID string `json:"container_id,omitempty"`
	Data        string `json:"data,omitempty"`
	Truncated   bool   `json:"truncated,omitempty"`
	Error       string `json:"error,omitempty"`
}

// DiagnosticsReport is an independent, JSON-serializable snapshot. Complete
// means every requested observation succeeded without byte truncation, not that
// Ceph is healthy or that replication has completed. Logs cover only LogTail.
type DiagnosticsReport struct {
	SchemaVersion int                  `json:"schema_version"`
	StartedAt     time.Time            `json:"started_at"`
	FinishedAt    time.Time            `json:"finished_at"`
	HostNetwork   bool                 `json:"host_network"`
	Closed        bool                 `json:"closed"`
	Complete      bool                 `json:"complete"`
	Artifacts     []DiagnosticArtifact `json:"artifacts"`
}

// CollectDiagnostics collects all tracked MON/MGR/OSD/MDS/RGW/control handles,
// container state, network addresses, bounded log tails and fixed Ceph queries.
// It supports partial bootstrap and stopped/removed containers. Individual
// failures return both a usable report and an aggregate error; truncation alone
// does not return an error. A canceled caller needs a fresh context to collect.
// No daemon is started, stopped or reconfigured, and no file, environment,
// keyring, auth database, mount source or arbitrary command is collected.
func (c *Container) CollectDiagnostics(ctx context.Context, config DiagnosticsConfig) (*DiagnosticsReport, error) {
	config, err := normalizeDiagnosticsConfig(config)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, config.Timeout)
	defer cancel()
	report := &DiagnosticsReport{SchemaVersion: 1, StartedAt: time.Now().UTC(), Artifacts: []DiagnosticArtifact{}}
	snapshot, err := c.diagnosticsSnapshot(ctx, config)
	report.HostNetwork, report.Closed = snapshot.hostNetwork, snapshot.closed
	if err != nil {
		message, clipped := diagnosticErrorText(err, diagnosticsRedactor(snapshot.secrets), config.MaxOutputBytes)
		report.Artifacts = append(report.Artifacts, DiagnosticArtifact{Kind: "inventory", Name: "cluster", Error: message, Truncated: clipped})
		report.FinishedAt = time.Now().UTC()
		return report, errors.Join(errors.New(message), ctx.Err())
	}
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		redact := diagnosticsRedactor(snapshot.secrets)
		message, clipped := diagnosticErrorText(err, redact, config.MaxOutputBytes)
		report.Artifacts = append(report.Artifacts, DiagnosticArtifact{Kind: "inventory", Name: "docker", Error: message, Truncated: clipped})
		report.FinishedAt = time.Now().UTC()
		return report, errors.Join(fmt.Errorf("diagnostics Docker client: %s", message), ctx.Err())
	}
	report, err = collectDiagnostics(ctx, config, snapshot, docker, report)
	if closeErr := docker.Close(); closeErr != nil {
		redact := diagnosticsRedactor(snapshot.secrets)
		message, clipped := diagnosticErrorText(closeErr, redact, config.MaxOutputBytes)
		report.Artifacts = append(report.Artifacts, DiagnosticArtifact{Kind: "inventory", Name: "docker-close", Error: message, Truncated: clipped})
		report.Complete = false
		err = errors.Join(err, fmt.Errorf("close diagnostics Docker client: %s", message))
	}
	return report, err
}

func normalizeDiagnosticsConfig(config DiagnosticsConfig) (DiagnosticsConfig, error) {
	if config.Timeout == 0 {
		config.Timeout = time.Minute
	}
	if config.OperationTimeout == 0 {
		config.OperationTimeout = 10 * time.Second
	}
	if config.MaxOutputBytes == 0 {
		config.MaxOutputBytes = 64 << 10
	}
	if config.LogTail == 0 {
		config.LogTail = 1000
	}
	if config.Concurrency == 0 {
		config.Concurrency = 4
	}
	if config.MaxContainers == 0 {
		config.MaxContainers = 128
	}
	if config.Timeout < 0 || config.Timeout > 5*time.Minute || config.OperationTimeout < 0 || config.OperationTimeout > time.Minute ||
		config.MaxOutputBytes < 1 || config.MaxOutputBytes > 1<<20 || config.LogTail < 1 || config.LogTail > 10000 ||
		config.Concurrency < 1 || config.Concurrency > 8 || config.MaxContainers < 1 || config.MaxContainers > 128 ||
		len(config.AdditionalContainers) > 64 || len(config.RedactValues) > 128 {
		return config, errors.New("invalid diagnostics limits")
	}
	for _, target := range config.AdditionalContainers {
		if len(target.Role) > 64 || len(target.Name) > 128 || strings.ContainsAny(target.Role+target.Name, "\x00\r\n") {
			return config, errors.New("invalid diagnostics container description")
		}
	}
	for _, value := range config.RedactValues {
		if len(value) > 4096 {
			return config, errors.New("diagnostics redaction value exceeds 4096 bytes")
		}
	}
	return config, nil
}

type diagnosticNetwork struct{ id, name string }
type diagnosticsSnapshot struct {
	containers          []DiagnosticsContainer
	networks            []diagnosticNetwork
	control             testcontainers.Container
	hostNetwork, closed bool
	publicAddress       string
	secrets             []string
	omitted             int
}

func diagnosticContainerHandle(ctr testcontainers.Container) testcontainers.Container {
	if ctr != nil {
		value := reflect.ValueOf(ctr)
		switch value.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
			if value.IsNil() {
				return nil
			}
		}
	}
	return ctr
}

func diagnosticLock(ctx context.Context, try func() bool) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if try() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (c *Container) diagnosticsSnapshot(ctx context.Context, config DiagnosticsConfig) (diagnosticsSnapshot, error) {
	snapshot := diagnosticsSnapshot{secrets: slices.Clone(config.RedactValues)}
	if c == nil {
		return snapshot, errors.New("Ceph cluster is nil")
	}
	if err := diagnosticLock(ctx, c.mu.TryLock); err != nil {
		return snapshot, err
	}
	snapshot.closed, snapshot.hostNetwork, snapshot.publicAddress = c.closed, c.settings.hostNetwork, c.settings.publicAddress
	snapshot.control = diagnosticContainerHandle(c.Container)
	add := func(role, name string, ctr testcontainers.Container) {
		snapshot.containers = append(snapshot.containers, DiagnosticsContainer{Role: role, Name: name, Container: diagnosticContainerHandle(ctr)})
	}
	if c.Container != nil && (!c.monitorTerminated || c.closed) {
		add("mon", "a", c.Container)
	}
	for name, mon := range c.monitors {
		if mon != nil {
			add("mon", name, mon.Container)
		}
	}
	for name, mgr := range c.managers {
		if mgr != nil {
			add("mgr", name, mgr.Container)
		}
	}
	if c.manager != nil {
		add("mgr", "a", c.manager)
	}
	for id, osd := range c.osds {
		if osd != nil {
			add("osd", strconv.Itoa(id), osd.Container)
		}
	}
	for name, ctr := range c.services {
		role := "service"
		if strings.HasPrefix(name, "mds.") {
			role = "mds"
		}
		if name == "rgw" || strings.HasPrefix(name, "rgw:") {
			role = "rgw"
		}
		add(role, name, ctr)
	}
	for _, gateway := range c.gateways {
		if gateway != nil {
			snapshot.secrets = append(snapshot.secrets, gateway.AccessKey, gateway.SecretKey)
		}
	}
	for _, nw := range []*testcontainers.DockerNetwork{c.network, c.clusterNetwork} {
		if nw != nil {
			snapshot.networks = append(snapshot.networks, diagnosticNetwork{id: nw.ID, name: nw.Name})
		}
	}
	c.mu.Unlock()
	if err := diagnosticLock(ctx, c.controlMu.TryRLock); err != nil {
		return snapshot, err
	}
	if c.controlPlane != nil {
		snapshot.control = diagnosticContainerHandle(c.controlPlane)
		add("control", "cli", snapshot.control)
	}
	c.controlMu.RUnlock()
	if err := diagnosticLock(ctx, c.configMu.TryRLock); err != nil {
		return snapshot, err
	}
	for _, match := range diagnosticKeyringPattern.FindAllSubmatch(c.keyring, -1) {
		snapshot.secrets = append(snapshot.secrets, strings.Trim(string(match[1]), "\"'"))
	}
	c.configMu.RUnlock()
	for i, target := range config.AdditionalContainers {
		target.Container = diagnosticContainerHandle(target.Container)
		if target.Name == "" {
			target.Name = fmt.Sprintf("extra-%d", i)
		}
		if target.Role == "" {
			target.Role = "extra"
		}
		snapshot.containers = append(snapshot.containers, target)
	}
	seen := make(map[string]bool)
	unique := snapshot.containers[:0]
	for _, target := range snapshot.containers {
		identity := "missing:" + target.Role + "/" + target.Name
		if target.Container != nil && target.Container.GetContainerID() != "" {
			identity = target.Container.GetContainerID()
		}
		if !seen[identity] {
			seen[identity] = true
			unique = append(unique, target)
		}
	}
	snapshot.containers = unique
	slices.SortFunc(snapshot.containers, func(a, b DiagnosticsContainer) int {
		return strings.Compare(a.Role+"/"+a.Name, b.Role+"/"+b.Name)
	})
	if len(unique) > config.MaxContainers {
		snapshot.omitted = len(unique) - config.MaxContainers
		snapshot.containers = unique[:config.MaxContainers]
	}
	return snapshot, nil
}

type diagnosticsDocker interface {
	ContainerInspect(context.Context, string, dockerclient.ContainerInspectOptions) (dockerclient.ContainerInspectResult, error)
	ContainerLogs(context.Context, string, dockerclient.ContainerLogsOptions) (dockerclient.ContainerLogsResult, error)
	NetworkInspect(context.Context, string, dockerclient.NetworkInspectOptions) (dockerclient.NetworkInspectResult, error)
}

type diagnosticTask struct {
	artifact DiagnosticArtifact
	collect  func(context.Context) (string, bool, error)
}

func collectDiagnostics(ctx context.Context, config DiagnosticsConfig, snapshot diagnosticsSnapshot, docker diagnosticsDocker, report *DiagnosticsReport) (*DiagnosticsReport, error) {
	redact := diagnosticsRedactor(snapshot.secrets)
	tasks := []diagnosticTask{}
	ids := make(map[string]bool)
	for _, target := range snapshot.containers {
		id := ""
		if target.Container != nil {
			id = target.Container.GetContainerID()
		}
		ids[id] = id != ""
		for _, kind := range []string{"container-inspect", "container-logs"} {
			artifact := DiagnosticArtifact{Kind: kind, Name: target.Name, Role: target.Role, ContainerID: id}
			tasks = append(tasks, diagnosticTask{artifact: artifact, collect: func(probe context.Context) (string, bool, error) {
				if id == "" {
					return "", false, errors.New("container was not created")
				}
				if kind == "container-inspect" {
					response, err := docker.ContainerInspect(probe, id, dockerclient.ContainerInspectOptions{})
					if err != nil {
						return "", false, err
					}
					data, err := json.Marshal(diagnosticContainerInspection(response.Container))
					return string(data), false, err
				}
				return diagnosticLogs(probe, docker, id, config)
			}})
		}
	}
	if snapshot.hostNetwork {
		tasks = append(tasks, diagnosticTask{artifact: DiagnosticArtifact{Kind: "network-inspect", Name: "host"}, collect: func(context.Context) (string, bool, error) {
			data, err := json.Marshal(map[string]any{"name": "host", "driver": "host", "shared": true, "public_address": snapshot.publicAddress})
			return string(data), false, err
		}})
	} else {
		seen := make(map[string]bool)
		for _, nw := range snapshot.networks {
			if seen[nw.id+nw.name] {
				continue
			}
			seen[nw.id+nw.name] = true
			tasks = append(tasks, diagnosticTask{artifact: DiagnosticArtifact{Kind: "network-inspect", Name: nw.name}, collect: func(probe context.Context) (string, bool, error) {
				if nw.id == "" {
					return "", false, errors.New("network was not created")
				}
				response, err := docker.NetworkInspect(probe, nw.id, dockerclient.NetworkInspectOptions{})
				if err != nil {
					return "", false, err
				}
				data, err := json.Marshal(diagnosticNetworkInspection(response.Network, ids))
				return string(data), false, err
			}})
		}
	}
	for _, query := range diagnosticCephQueries {
		tasks = append(tasks, diagnosticTask{artifact: DiagnosticArtifact{Kind: "ceph", Name: query.name}, collect: func(probe context.Context) (string, bool, error) {
			return diagnosticCephCommand(probe, snapshot.control, query.args, config.MaxOutputBytes)
		}})
	}
	report.Artifacts = make([]DiagnosticArtifact, len(tasks))
	for i, task := range tasks {
		report.Artifacts[i] = task.artifact
	}
	jobs := make(chan int)
	var workers sync.WaitGroup
	for range config.Concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range jobs {
				artifact := &report.Artifacts[i]
				if err := ctx.Err(); err != nil {
					artifact.Error = err.Error()
					continue
				}
				probe, cancel := context.WithTimeout(ctx, config.OperationTimeout)
				data, clipped, err := tasks[i].collect(probe)
				if err == nil {
					err = probe.Err()
				}
				cancel()
				data, outputClipped := diagnosticBoundText(redact(data), config.MaxOutputBytes)
				artifact.Data, artifact.Truncated = data, clipped || outputClipped
				if err != nil {
					message, errorClipped := diagnosticBoundText(redact(err.Error()), config.MaxOutputBytes)
					if message == "" {
						message = "?"
					}
					artifact.Error = message
					artifact.Truncated = artifact.Truncated || errorClipped
				}
			}
		}()
	}
	for i := range tasks {
		jobs <- i
	}
	close(jobs)
	workers.Wait()
	if snapshot.omitted > 0 {
		report.Artifacts = append(report.Artifacts, DiagnosticArtifact{Kind: "inventory", Name: "container-limit", Error: fmt.Sprintf("%d containers omitted by MaxContainers", snapshot.omitted)})
	}
	report.Complete = true
	var errs []error
	for i := range report.Artifacts {
		artifact := &report.Artifacts[i]
		artifact.Name, artifact.Role, artifact.ContainerID = redact(artifact.Name), redact(artifact.Role), redact(artifact.ContainerID)
		if artifact.Error != "" {
			message, clipped := diagnosticErrorText(errors.New(artifact.Error), redact, config.MaxOutputBytes)
			artifact.Error, artifact.Truncated = message, artifact.Truncated || clipped
			errs = append(errs, fmt.Errorf("%s/%s: %s", artifact.Kind, artifact.Name, artifact.Error))
		}
		if artifact.Error != "" || artifact.Truncated {
			report.Complete = false
		}
	}
	if ctx.Err() != nil {
		report.Complete = false
		errs = append(errs, ctx.Err())
	}
	report.FinishedAt = time.Now().UTC()
	return report, errors.Join(errs...)
}

func diagnosticBoundText(data string, limit int) (string, bool) {
	data = strings.ToValidUTF8(data, "�")
	if len(data) <= limit {
		return data, false
	}
	end := limit
	for end > 0 && !utf8.RuneStart(data[end]) {
		end--
	}
	return data[:end], true
}

func diagnosticErrorText(err error, redact func(string) string, limit int) (string, bool) {
	message, clipped := diagnosticBoundText(redact(err.Error()), limit)
	if message == "" {
		message = "?"
	}
	return message, clipped
}

func diagnosticContainerInspection(inspection dockercontainer.InspectResponse) map[string]any {
	result := map[string]any{"id": inspection.ID, "name": inspection.Name, "image_id": inspection.Image, "restart_count": inspection.RestartCount}
	if inspection.State != nil {
		s := inspection.State
		result["state"] = map[string]any{"status": s.Status, "running": s.Running, "paused": s.Paused, "restarting": s.Restarting, "oom_killed": s.OOMKilled, "dead": s.Dead, "pid": s.Pid, "exit_code": s.ExitCode, "started_at": s.StartedAt, "finished_at": s.FinishedAt}
	}
	if inspection.Config != nil {
		result["image_ref"] = inspection.Config.Image
	}
	if inspection.HostConfig != nil {
		result["network_mode"] = string(inspection.HostConfig.NetworkMode)
	}
	if inspection.NetworkSettings != nil {
		result["ports"] = inspection.NetworkSettings.Ports
		endpoints := make(map[string]any)
		for name, endpoint := range inspection.NetworkSettings.Networks {
			if endpoint != nil {
				endpoints[name] = map[string]any{"network_id": endpoint.NetworkID, "endpoint_id": endpoint.EndpointID, "ipv4": endpoint.IPAddress, "ipv6": endpoint.GlobalIPv6Address, "gateway": endpoint.Gateway}
			}
		}
		result["networks"] = endpoints
	}
	return result
}

func diagnosticNetworkInspection(nw dockernetwork.Inspect, ids map[string]bool) map[string]any {
	result := map[string]any{"id": nw.ID, "name": nw.Name, "driver": nw.Driver, "internal": nw.Internal}
	subnets := []map[string]any{}
	for _, ipam := range nw.IPAM.Config {
		subnets = append(subnets, map[string]any{"subnet": ipam.Subnet, "gateway": ipam.Gateway})
	}
	result["subnets"] = subnets
	endpoints := make(map[string]any)
	for id, endpoint := range nw.Containers {
		if ids[id] {
			endpoints[id] = map[string]any{"name": endpoint.Name, "endpoint_id": endpoint.EndpointID, "ipv4": endpoint.IPv4Address, "ipv6": endpoint.IPv6Address}
		}
	}
	result["containers"] = endpoints
	return result
}

type diagnosticTail struct {
	data      []byte
	limit     int
	truncated bool
}

func (tail *diagnosticTail) Write(data []byte) (int, error) {
	n := len(data)
	if len(tail.data)+n > tail.limit {
		tail.truncated = true
	}
	if n >= tail.limit {
		tail.data = append(tail.data[:0], data[n-tail.limit:]...)
	} else {
		if excess := len(tail.data) + n - tail.limit; excess > 0 {
			tail.data = append(tail.data[:0], tail.data[excess:]...)
		}
		tail.data = append(tail.data, data...)
	}
	return n, nil
}

func diagnosticLogs(ctx context.Context, docker diagnosticsDocker, id string, config DiagnosticsConfig) (string, bool, error) {
	inspection, err := docker.ContainerInspect(ctx, id, dockerclient.ContainerInspectOptions{})
	if err != nil {
		return "", false, err
	}
	reader, err := docker.ContainerLogs(ctx, id, dockerclient.ContainerLogsOptions{ShowStdout: true, ShowStderr: true, Tail: strconv.Itoa(config.LogTail)})
	if err != nil {
		return "", false, err
	}
	defer reader.Close()
	stop := context.AfterFunc(ctx, func() { _ = reader.Close() })
	defer stop()
	tail := &diagnosticTail{limit: config.MaxOutputBytes}
	if inspection.Container.Config != nil && inspection.Container.Config.Tty {
		_, err = io.Copy(tail, reader)
	} else {
		// stdcopy allocates from the advertised frame size. Stream each frame
		// through a fixed-size copy buffer instead, even for a huge frame header.
		header := make([]byte, 8)
		for {
			_, err = io.ReadFull(reader, header)
			if err == io.EOF {
				err = nil
				break
			}
			if err != nil {
				break
			}
			if (header[0] != 1 && header[0] != 2) || header[1] != 0 || header[2] != 0 || header[3] != 0 {
				err = errors.New("invalid Docker log frame")
				break
			}
			_, err = io.CopyN(tail, reader, int64(binary.BigEndian.Uint32(header[4:])))
			if err != nil {
				break
			}
		}
	}
	return string(tail.data), tail.truncated, errors.Join(err, ctx.Err())
}

var diagnosticCephQueries = []struct {
	name string
	args []string
}{
	{"version", []string{"--version"}},
	{"status", []string{"status", "--format", "json"}},
	{"health", []string{"health", "detail", "--format", "json"}},
	{"pgs", []string{"pg", "dump", "pgs_brief", "--format", "json"}},
	{"quorum", []string{"quorum_status", "--format", "json"}},
	{"monmap", []string{"mon", "dump", "--format", "json"}},
	{"mgrmap", []string{"mgr", "dump", "--format", "json"}},
	{"osdmap", []string{"osd", "dump", "--format", "json"}},
	{"fsmap", []string{"fs", "dump", "--format", "json"}},
}

// Bound output inside the container: Testcontainers Exec buffers its stream
// before returning. A host LimitReader alone would not bound that allocation.
// Kill only this newly spawned read-only CLI process group at the deadline.
// Keep stderr on its own pipe: Ceph makes FIFO stderr nonblocking, which would
// also change stdout when both descriptors share the same open file description.
const diagnosticCommandScript = `import base64,json,os,select,signal,subprocess,sys,time
limit=int(sys.argv[1]); deadline=time.monotonic()+float(sys.argv[2]); output=bytearray(); truncated=False; timed_out=False
p=subprocess.Popen(sys.argv[3:],stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
streams=[p.stdout,p.stderr]
def kill_owned_group():
 try: os.killpg(p.pid,signal.SIGKILL)
 except ProcessLookupError: pass
try:
 while streams:
  remaining=deadline-time.monotonic()
  if remaining<=0:
   timed_out=True
   kill_owned_group()
   break
  for stream in select.select(streams,[],[],min(remaining,0.1))[0]:
   data=os.read(stream.fileno(),32768)
   if not data:
    streams.remove(stream)
    continue
   room=max(0,limit-len(output)); output.extend(data[:room]); truncated=truncated or len(data)>room
 code=p.wait(timeout=max(0.01,deadline-time.monotonic()))
except subprocess.TimeoutExpired:
 timed_out=True
 kill_owned_group()
 code=p.wait()
finally:
 if p.poll() is None:
  kill_owned_group()
  p.wait()
 p.stdout.close()
 p.stderr.close()
print(json.dumps({'output':base64.b64encode(output).decode(),'exit_code':code,'timed_out':timed_out,'truncated':truncated}))
`

func diagnosticCephCommand(ctx context.Context, control testcontainers.Container, args []string, limit int) (string, bool, error) {
	if control == nil {
		return "", false, errors.New("Ceph control container was not created")
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return "", false, errors.New("diagnostic command requires a deadline")
	}
	seconds := time.Until(deadline).Seconds() * 0.75
	if seconds <= 0 {
		return "", false, ctx.Err()
	}
	argv := []string{"python3", "-c", diagnosticCommandScript, strconv.Itoa(limit), strconv.FormatFloat(seconds, 'f', 6, 64), "ceph", "--connect-timeout", "2"}
	argv = append(argv, args...)
	code, reader, err := control.Exec(ctx, argv, tcexec.Multiplexed())
	if err != nil {
		return "", false, err
	}
	if reader == nil {
		return "", false, errors.New("diagnostic command returned no output")
	}
	data, err := io.ReadAll(io.LimitReader(reader, int64(limit)*2+4096))
	if err != nil {
		return "", false, err
	}
	if code != 0 {
		return "", false, fmt.Errorf("diagnostic supervisor exited %d", code)
	}
	var result struct {
		Output    *string `json:"output"`
		ExitCode  *int    `json:"exit_code"`
		TimedOut  *bool   `json:"timed_out"`
		Truncated *bool   `json:"truncated"`
	}
	if err := json.Unmarshal(data, &result); err != nil || result.Output == nil || result.ExitCode == nil || result.TimedOut == nil || result.Truncated == nil {
		return "", false, errors.New("invalid diagnostic supervisor result")
	}
	output, err := base64.StdEncoding.DecodeString(*result.Output)
	if err != nil {
		return "", false, errors.New("invalid diagnostic supervisor output")
	}
	if len(output) > limit {
		output = output[:limit]
		*result.Truncated = true
	}
	if *result.TimedOut {
		return string(output), *result.Truncated, context.DeadlineExceeded
	}
	if *result.ExitCode != 0 {
		return string(output), *result.Truncated, fmt.Errorf("Ceph diagnostic command exited %d", *result.ExitCode)
	}
	return string(output), *result.Truncated, nil
}

var diagnosticKeyringPattern = regexp.MustCompile(`(?m)^\s*key\s*=\s*(\S+)`)
var diagnosticSecretPattern = regexp.MustCompile(`(?i)((?:secret_key|access_key|password|private_key|authorization|credential|signature|token|secret|key)\s*["']?\s*[:=]\s*["']?)([^"'\s,;}\]]+)`)
var diagnosticBearerPattern = regexp.MustCompile(`(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+`)
var diagnosticPrivateKeyPattern = regexp.MustCompile(`(?s)-----BEGIN (?:[A-Z ]+)?PRIVATE KEY-----.*?-----END (?:[A-Z ]+)?PRIVATE KEY-----`)

func diagnosticsRedactor(values []string) func(string) string {
	values = slices.Clone(values)
	slices.SortFunc(values, func(a, b string) int { return len(b) - len(a) })
	return func(data string) string {
		for _, value := range values {
			if value != "" {
				data = strings.ReplaceAll(data, value, "[REDACTED]")
			}
		}
		data = diagnosticPrivateKeyPattern.ReplaceAllString(data, "[REDACTED PRIVATE KEY]")
		data = diagnosticSecretPattern.ReplaceAllString(data, "${1}[REDACTED]")
		return diagnosticBearerPattern.ReplaceAllString(data, "${1}[REDACTED]")
	}
}
