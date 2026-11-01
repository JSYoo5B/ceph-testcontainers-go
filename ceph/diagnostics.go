package ceph

import (
	"context"
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

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	dockercontainer "github.com/moby/moby/api/types/container"
	dockernetwork "github.com/moby/moby/api/types/network"
	dockerclient "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// DiagnosticsConfig bounds a read-only collection. Zero fields select a one
// minute total timeout, ten seconds per operation, 64 KiB per artifact, the last
// 1000 log lines, four workers and at most 128 containers. Additional containers
// are caller-owned clients or mirror daemons; collection never adopts them.
// RedactValues removes additional opaque application secrets. Redaction is best
// effort: arbitrary log contents still require review before sharing a report.
//
// The limits are 5 minutes total, 1 minute per operation, 1 MiB per artifact,
// 10000 log lines, 8 workers, 128 containers, 64 additional containers and 128
// redaction values of up to 4096 bytes; invalid limits are refused before
// collection, without a report. Artifact kinds are inventory, container-
// inspect, container-logs, network-inspect and ceph.
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

// CollectDiagnostics collects all tracked MON/MGR/OSD/MDS/RGW/control and client
// toolkit handles,
// container state, network addresses, bounded log tails and fixed Ceph queries.
// It supports partial bootstrap and stopped/removed containers. Individual
// failures return both a usable report and an aggregate error; truncation alone
// does not return an error. A canceled caller needs a fresh context to collect.
// No daemon is started, stopped or reconfigured, and no file, environment,
// keyring, auth database, mount source or arbitrary command is collected.
//
// It collects the state and selected metadata of every tracked MON, MGR, OSD,
// MDS, RGW, control and client toolkit container, network metadata, log tails and fixed Ceph
// queries (version, status, health, pgs, quorum, monmap, mgrmap, osdmap,
// fsmap); it never collects files, environment variables, keyrings, the auth
// database, mount sources or arbitrary commands. TerminateOnCleanup with
// Archive calls it on failure; otherwise call it before cleanup with its own
// short context. When some queries fail, the report and an aggregate error are
// both returned, so save the report first; truncation alone is not an error. A
// ceph artifact holds the query's stdout followed by its stderr. When ctx
// ends, every running query is stopped in the control container before the
// call returns. Known admin and RGW secrets and every client key the cluster
// generated (also replaced or attempted ones) are masked with RedactValues and
// named secret patterns.
//
//purpose:evidence
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
	inventoryFailure := func(name string, err error) string {
		message, clipped := diagnosticErrorText(err, diagnosticsRedactor(snapshot.secrets), config.MaxOutputBytes)
		report.Artifacts = append(report.Artifacts, DiagnosticArtifact{Kind: "inventory", Name: name, Error: message, Truncated: clipped})
		return message
	}
	if err != nil {
		message := inventoryFailure("cluster", err)
		report.FinishedAt = time.Now().UTC()
		return report, errors.Join(errors.New(message), ctx.Err())
	}
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		message := inventoryFailure("docker", err)
		report.FinishedAt = time.Now().UTC()
		return report, errors.Join(fmt.Errorf("diagnostics Docker client: %s", message), ctx.Err())
	}
	report, err = collectDiagnostics(ctx, config, snapshot, docker, report)
	if closeErr := docker.Close(); closeErr != nil {
		message := inventoryFailure("docker-close", closeErr)
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
		return config, invalid(errors.New("invalid diagnostics limits"))
	}
	for _, target := range config.AdditionalContainers {
		if len(target.Role) > 64 || len(target.Name) > 128 || strings.ContainsAny(target.Role+target.Name, "\x00\r\n") {
			return config, invalid(errors.New("invalid diagnostics container description"))
		}
	}
	for _, value := range config.RedactValues {
		if len(value) > 4096 {
			return config, invalid(errors.New("diagnostics redaction value exceeds 4096 bytes"))
		}
	}
	return config, nil
}

type diagnosticNetwork struct{ id, name string }
type diagnosticsSnapshot struct {
	containers []DiagnosticsContainer
	networks   []diagnosticNetwork
	control    testcontainers.Container
	// controlPlane is the separate control container that the snapshot
	// added itself, if the cluster has one.
	controlPlane        testcontainers.Container
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

// sameContainer reports whether a and b are the same container: by ID, or for
// containers without one, by the same pointer.
func sameContainer(a, b testcontainers.Container) bool {
	if a == nil || b == nil {
		return false
	}
	if id := b.GetContainerID(); id != "" {
		return a.GetContainerID() == id
	}
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	return va.Type() == vb.Type() && va.Kind() == reflect.Pointer && va.Pointer() == vb.Pointer()
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
	topology := c.monitorTopology()
	topology.diagnosticDaemons(c.closed, add)
	c.osdFleet().diagnosticOSDs(add)
	topology.diagnosticServices(add)
	for _, state := range extensionsWith[serviceSecrets](c) {
		snapshot.secrets = append(snapshot.secrets, state.Secrets()...)
	}
	// Every client key the module generated, including replaced, attempted
	// and revoked ones.
	snapshot.secrets = append(snapshot.secrets, c.ledger.authRecords.secretValues()...)
	for _, nw := range c.nets.bridges() {
		snapshot.networks = append(snapshot.networks, diagnosticNetwork{id: nw.ID, name: nw.Name})
	}
	c.mu.Unlock()
	control, err := c.controlPlane.diagnosticHandle(ctx)
	if err != nil {
		return snapshot, err
	}
	if control != nil {
		snapshot.control = diagnosticContainerHandle(control)
		snapshot.controlPlane = snapshot.control
		add("control", "cli", snapshot.control)
	}
	client, err := c.toolkit.diagnosticSnapshot(ctx)
	if err != nil {
		return snapshot, err
	}
	if client != nil {
		add("client", "toolkit", client)
	}
	keyring, err := c.bootstrap.diagnosticKeyring(ctx)
	if err != nil {
		return snapshot, err
	}
	for _, match := range diagnosticKeyringPattern.FindAllSubmatch(keyring, -1) {
		snapshot.secrets = append(snapshot.secrets, strings.Trim(string(match[1]), "\"'"))
	}
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

// diagnosticCollection is one collection run over a snapshot: it plans one
// task per observation, runs the tasks on the configured workers and finishes
// the report. It owns no cluster state; the snapshot was taken under the
// cluster's lock and is only read here.
type diagnosticCollection struct {
	config   DiagnosticsConfig
	snapshot diagnosticsSnapshot
	docker   diagnosticsDocker
	redact   func(string) string
}

func collectDiagnostics(ctx context.Context, config DiagnosticsConfig, snapshot diagnosticsSnapshot, docker diagnosticsDocker, report *DiagnosticsReport) (*DiagnosticsReport, error) {
	run := diagnosticCollection{config: config, snapshot: snapshot, docker: docker, redact: diagnosticsRedactor(snapshot.secrets)}
	tasks := run.plan()
	report.Artifacts = make([]DiagnosticArtifact, len(tasks))
	for i, task := range tasks {
		report.Artifacts[i] = task.artifact
	}
	run.execute(ctx, tasks, report.Artifacts)
	if snapshot.omitted > 0 {
		report.Artifacts = append(report.Artifacts, DiagnosticArtifact{Kind: "inventory", Name: "container-limit", Error: fmt.Sprintf("%d containers omitted by MaxContainers", snapshot.omitted)})
	}
	return run.finish(ctx, report)
}

// plan lists the tasks in report order: each container's inspection and logs,
// then the networks, then the fixed Ceph queries.
func (run diagnosticCollection) plan() []diagnosticTask {
	tasks, ids := run.containerTasks()
	tasks = append(tasks, run.networkTasks(ids)...)
	return append(tasks, run.cephTasks()...)
}

// containerTasks inspects each container and reads its logs. ids records the
// created containers, which the network inspection lists as members.
func (run diagnosticCollection) containerTasks() ([]diagnosticTask, map[string]bool) {
	tasks := []diagnosticTask{}
	ids := make(map[string]bool)
	for _, target := range run.snapshot.containers {
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
					response, err := run.docker.ContainerInspect(probe, id, dockerclient.ContainerInspectOptions{})
					if err != nil {
						return "", false, err
					}
					data, err := json.Marshal(diagnosticContainerInspection(response.Container))
					return string(data), false, err
				}
				return diagnosticLogs(probe, run.docker, id, run.config)
			}})
		}
	}
	return tasks, ids
}

// networkTasks describes the host namespace, or inspects each distinct bridge
// once.
func (run diagnosticCollection) networkTasks(ids map[string]bool) []diagnosticTask {
	if run.snapshot.hostNetwork {
		return []diagnosticTask{{artifact: DiagnosticArtifact{Kind: "network-inspect", Name: "host"}, collect: func(context.Context) (string, bool, error) {
			data, err := json.Marshal(map[string]any{"name": "host", "driver": "host", "shared": true, "public_address": run.snapshot.publicAddress})
			return string(data), false, err
		}}}
	}
	var tasks []diagnosticTask
	seen := make(map[string]bool)
	for _, nw := range run.snapshot.networks {
		if seen[nw.id+nw.name] {
			continue
		}
		seen[nw.id+nw.name] = true
		tasks = append(tasks, diagnosticTask{artifact: DiagnosticArtifact{Kind: "network-inspect", Name: nw.name}, collect: func(probe context.Context) (string, bool, error) {
			if nw.id == "" {
				return "", false, errors.New("network was not created")
			}
			response, err := run.docker.NetworkInspect(probe, nw.id, dockerclient.NetworkInspectOptions{})
			if err != nil {
				return "", false, err
			}
			data, err := json.Marshal(diagnosticNetworkInspection(response.Network, ids))
			return string(data), false, err
		}})
	}
	return tasks
}

// cephTasks runs the fixed read-only Ceph queries through the control
// container.
func (run diagnosticCollection) cephTasks() []diagnosticTask {
	var tasks []diagnosticTask
	for _, query := range diagnosticCephQueries {
		tasks = append(tasks, diagnosticTask{artifact: DiagnosticArtifact{Kind: "ceph", Name: query.name}, collect: func(probe context.Context) (string, bool, error) {
			return diagnosticCephCommand(probe, run.snapshot.control, query.args, run.config.MaxOutputBytes)
		}})
	}
	return tasks
}

// execute runs the tasks on config.Concurrency workers, each writing only its
// own artifact.
func (run diagnosticCollection) execute(ctx context.Context, tasks []diagnosticTask, artifacts []DiagnosticArtifact) {
	jobs := make(chan int)
	var workers sync.WaitGroup
	for range run.config.Concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range jobs {
				run.collectOne(ctx, tasks[i], &artifacts[i])
			}
		}()
	}
	for i := range tasks {
		jobs <- i
	}
	close(jobs)
	workers.Wait()
}

// collectOne runs one task under the operation timeout and records its
// redacted, bounded output or error.
func (run diagnosticCollection) collectOne(ctx context.Context, task diagnosticTask, artifact *DiagnosticArtifact) {
	if err := ctx.Err(); err != nil {
		artifact.Error = err.Error()
		return
	}
	probe, cancel := context.WithTimeout(ctx, run.config.OperationTimeout)
	data, clipped, err := task.collect(probe)
	if err == nil {
		err = probe.Err()
	}
	cancel()
	data, outputClipped := diagnosticBoundText(run.redact(data), run.config.MaxOutputBytes)
	artifact.Data, artifact.Truncated = data, clipped || outputClipped
	if err != nil {
		message, errorClipped := diagnosticBoundText(run.redact(err.Error()), run.config.MaxOutputBytes)
		if message == "" {
			message = "?"
		}
		artifact.Error = message
		artifact.Truncated = artifact.Truncated || errorClipped
	}
}

// finish redacts every artifact's identity, decides completeness and joins the
// artifact errors with the run's own.
func (run diagnosticCollection) finish(ctx context.Context, report *DiagnosticsReport) (*DiagnosticsReport, error) {
	report.Complete = true
	var errs []error
	for i := range report.Artifacts {
		artifact := &report.Artifacts[i]
		artifact.Name, artifact.Role, artifact.ContainerID = run.redact(artifact.Name), run.redact(artifact.Role), run.redact(artifact.ContainerID)
		if artifact.Error != "" {
			message, clipped := diagnosticErrorText(errors.New(artifact.Error), run.redact, run.config.MaxOutputBytes)
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
	defer func() { _ = reader.Close() }()
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

// diagnosticCephCommand runs one read-only Ceph query under the supervisor,
// which bounds its output inside the container (Testcontainers Exec buffers
// the whole stream) and kills only its process group at 75% of ctx's time.
// Stdout precedes stderr in the output, which is clipped to limit. A timed-out
// or failed query keeps the output it produced. A canceled query leaves no
// process behind.
func diagnosticCephCommand(ctx context.Context, control testcontainers.Container, args []string, limit int) (string, bool, error) {
	if control == nil {
		return "", false, errors.New("Ceph control container was not created")
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return "", false, errors.New("diagnostic command requires a deadline")
	}
	budget := time.Until(deadline) * 3 / 4
	if budget <= 0 {
		return "", false, ctx.Err()
	}
	report, err := supervised.Run(ctx, control, "diagnostic", budget, limit, limit,
		append([]string{"ceph", "--connect-timeout", "2"}, args...)...)
	if err != nil {
		return "", false, err
	}
	output := append(report.Stdout, report.Stderr...)
	truncated := report.StdoutTruncated || report.StderrTruncated
	if len(output) > limit {
		output, truncated = output[:limit], true
	}
	if report.TimedOut {
		return string(output), truncated, context.DeadlineExceeded
	}
	if report.ExitCode != 0 {
		// The output is the artifact; the error repeats none of it.
		failure := nativeFailure("ceph", report.ExitCode, nil, report.Stderr)
		failure.Reason = ""
		return string(output), truncated, fmt.Errorf("Ceph diagnostic command: %w", failure)
	}
	return string(output), truncated, nil
}

var diagnosticKeyringPattern = regexp.MustCompile(`(?m)^\s*key\s*=\s*(\S+)`)
var diagnosticSecretPattern = regexp.MustCompile(`(?i)((?:secret_key|access_key|password|private_key|authorization|credential|signature|token|secret|key)\s*["']?\s*[:=]\s*["']?)([^"'\s,;}\]]+)`)
var diagnosticBearerPattern = regexp.MustCompile(`(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+`)
var diagnosticPrivateKeyPattern = regexp.MustCompile(`(?s)-----BEGIN (?:[A-Z ]+)?PRIVATE KEY-----.*?-----END (?:[A-Z ]+)?PRIVATE KEY-----`)

// diagnosticsRedactor masks values and recognized secrets. Values are
// replaced in one pass, longest first, so that a replacement is never
// rewritten again and the output grows at most linearly.
func diagnosticsRedactor(values []string) func(string) string {
	values = slices.DeleteFunc(slices.Clone(values), func(value string) bool { return value == "" })
	slices.SortFunc(values, func(a, b string) int {
		if len(a) != len(b) {
			return len(b) - len(a)
		}
		return strings.Compare(a, b)
	})
	values = slices.Compact(values)
	pairs := make([]string, 0, 2*len(values))
	for _, value := range values {
		pairs = append(pairs, value, "[REDACTED]")
	}
	replacer := strings.NewReplacer(pairs...)
	return func(data string) string {
		data = replacer.Replace(data)
		data = diagnosticPrivateKeyPattern.ReplaceAllString(data, "[REDACTED PRIVATE KEY]")
		// Bearer tokens first: the secret pattern takes only the word after
		// "Authorization:", which would leave the token behind it.
		data = diagnosticBearerPattern.ReplaceAllString(data, "${1}[REDACTED]")
		return diagnosticSecretPattern.ReplaceAllString(data, "${1}[REDACTED]")
	}
}
