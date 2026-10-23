package ceph

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// verificationDocker is the part of the Docker client that verification
// uses.
type verificationDocker interface {
	crashDocker
	ContainerLogs(context.Context, string, mobycl.ContainerLogsOptions) (mobycl.ContainerLogsResult, error)
	Info(context.Context, mobycl.InfoOptions) (mobycl.SystemInfoResult, error)
}

func dialVerificationDocker(ctx context.Context) (verificationDocker, func(), error) {
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return nil, nil, err
	}
	// The engine's own client: Testcontainers' Info caches the first answer,
	// whose clock would never advance.
	return docker.Client, func() { _ = docker.Close() }, nil
}

// verificationSourceName names a container as the report does: "osd.0",
// "mon.a", or the name itself when it already starts with its role.
func verificationSourceName(role, name string) string {
	if name == role || strings.HasPrefix(name, role+".") || strings.HasPrefix(name, role+":") {
		return name
	}
	return role + "." + name
}

// verificationTargets selects the daemon containers of a snapshot: every
// container but the separate control container and the toolkit client,
// unless the caller added them.
func verificationTargets(snapshot diagnosticsSnapshot, additional []DiagnosticsContainer) []verificationTarget {
	requested := func(target DiagnosticsContainer) bool {
		return slices.ContainsFunc(additional, func(extra DiagnosticsContainer) bool {
			return sameContainer(diagnosticContainerHandle(extra.Container), target.Container)
		})
	}
	var targets []verificationTarget
	for _, target := range snapshot.containers {
		cluster := sameContainer(target.Container, snapshot.controlPlane) || (target.Role == "client" && target.Name == "toolkit")
		if cluster && !requested(target) {
			continue
		}
		id := ""
		if target.Container != nil {
			id = target.Container.GetContainerID()
		}
		targets = append(targets, verificationTarget{DiagnosticsContainer: target, source: verificationSourceName(target.Role, target.Name), id: id})
	}
	return targets
}

func dockerClock(ctx context.Context, docker verificationDocker, timeout time.Duration) (time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	info, err := docker.Info(ctx, mobycl.InfoOptions{})
	if err != nil {
		return time.Time{}, errors.Join(err, ctx.Err())
	}
	clock, err := time.Parse(time.RFC3339Nano, info.Info.SystemTime)
	if err != nil {
		return time.Time{}, fmt.Errorf("the Docker engine reported its time as %q", info.Info.SystemTime)
	}
	return clock.UTC(), nil
}

// inspectGeneration identifies a container's current generation. A removed
// container returns errVerificationRemoved.
func inspectGeneration(ctx context.Context, docker verificationDocker, id string, timeout time.Duration) (*verificationGeneration, error) {
	if id == "" {
		return nil, errors.New("the container was not created")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	inspection, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
	switch {
	case errdefs.IsNotFound(err):
		return nil, errVerificationRemoved
	case err != nil:
		return nil, errors.Join(err, ctx.Err())
	}
	ctr := inspection.Container
	generation := &verificationGeneration{image: ctr.Image, restartCount: ctr.RestartCount}
	if ctr.State != nil {
		generation.startedAt, generation.state = ctr.State.StartedAt, string(ctr.State.Status)
	}
	if ctr.Config != nil {
		generation.tty = ctr.Config.Tty
	}
	if ctr.HostConfig != nil {
		generation.logDriver = ctr.HostConfig.LogConfig.Type
		if size := ctr.HostConfig.LogConfig.Config["max-size"]; size != "" && size != "-1" {
			generation.logRotation = size
		}
	}
	if generation.startedAt == "" {
		return nil, errors.New("the Docker engine reported no start time")
	}
	return generation, nil
}

var errVerificationRemoved = errors.New("the container was removed")

// recordVerificationBaseline inspects the selected containers, reads their
// crash directories and then reads the engine's clock, so that a crash
// between the two counts as new.
func recordVerificationBaseline(ctx context.Context, config VerificationConfig, snapshot diagnosticsSnapshot, docker verificationDocker) (*verificationBaseline, error) {
	b := &verificationBaseline{secrets: snapshot.secrets}
	redact := diagnosticsRedactor(b.secrets)
	both := func(source, reason string) {
		reason = verificationReason(reason, redact)
		b.logGaps = append(b.logGaps, VerificationGap{Source: source, Reason: "at activation: " + reason})
		b.crashGaps = append(b.crashGaps, VerificationGap{Source: source, Reason: "at activation: " + reason})
	}
	if snapshot.omitted > 0 {
		both("", fmt.Sprintf("%d containers exceed the container limit", snapshot.omitted))
	}
	b.targets = verificationTargets(snapshot, config.AdditionalContainers)
	for i := range b.targets {
		target := &b.targets[i]
		generation, err := inspectGeneration(ctx, docker, target.id, config.OperationTimeout)
		if err != nil {
			both(target.source, err.Error())
			continue
		}
		target.generation = generation
	}
	crashes, err := readVerificationCrashes(ctx, config, b.targets, docker)
	for i, result := range crashes.Containers {
		target := &b.targets[i]
		if target.generation == nil {
			continue
		}
		if reason := crashResultGap(result); reason != "" {
			b.crashGaps = append(b.crashGaps, VerificationGap{Source: target.source, Reason: "at activation: " + verificationReason(reason, redact)})
			continue
		}
		target.crashes = map[string]bool{}
		for _, record := range result.Records {
			target.crashes[record.CrashID] = true
		}
	}
	if err != nil && len(crashes.Containers) != len(b.targets) {
		b.crashGaps = append(b.crashGaps, VerificationGap{Reason: "at activation: " + verificationReason(err.Error(), redact)})
	}
	start, err := dockerClock(ctx, docker, config.OperationTimeout)
	if err != nil {
		return nil, fmt.Errorf("verification baseline: read the Docker engine's clock: %w", err)
	}
	b.start = start
	return b, nil
}

// readVerificationCrashes reads the crash directories of targets, one
// result per target in order.
func readVerificationCrashes(ctx context.Context, config VerificationConfig, targets []verificationTarget, docker verificationDocker) (*CrashMetadataReport, error) {
	crashConfig, _ := normalizeCrashMetadataConfig(CrashMetadataConfig{OperationTimeout: config.OperationTimeout, RedactValues: config.RedactValues})
	snapshot := diagnosticsSnapshot{}
	for _, target := range targets {
		snapshot.containers = append(snapshot.containers, target.DiagnosticsContainer)
	}
	report := &CrashMetadataReport{SchemaVersion: 1, StartedAt: time.Now().UTC(), Containers: []CrashMetadataContainer{}}
	return collectCrashMetadata(ctx, crashConfig, snapshot, docker, report)
}

// crashResultGap says why a container's crash directory does not establish
// which crashes it holds, or returns "".
func crashResultGap(result CrashMetadataContainer) string {
	switch result.Outcome {
	case CrashRecordsFound, CrashDirectoryEmpty:
		if len(result.Omitted) > 0 {
			return fmt.Sprintf("crash directory: %d metadata files were not accepted (first: %s, %s)", len(result.Omitted), result.Omitted[0].Path, result.Omitted[0].Reason)
		}
		return ""
	case CrashDirectoryAbsent:
		return "the container has no crash directory, so no crash can be recorded there"
	}
	reason := "crash directory " + string(result.Outcome)
	if result.Error != "" {
		reason += ": " + result.Error
	}
	return reason
}

func verificationReason(reason string, redact func(string) string) string {
	reason, _ = diagnosticBoundText(strings.ReplaceAll(redact(reason), "\n", "; "), 1024)
	if reason == "" {
		reason = "?"
	}
	return reason
}

// observeVerification observes the window from the baseline until now.
func observeVerification(ctx context.Context, config VerificationConfig, b *verificationBaseline, docker verificationDocker, o *verificationObservation) {
	redact := diagnosticsRedactor(b.secrets)
	logGap := func(source, reason string) {
		o.logGaps = append(o.logGaps, VerificationGap{Source: source, Reason: verificationReason(reason, redact)})
	}
	crashGap := func(source, reason string) {
		o.crashGap = append(o.crashGap, VerificationGap{Source: source, Reason: verificationReason(reason, redact)})
	}
	end, err := dockerClock(ctx, docker, config.OperationTimeout)
	if err != nil {
		o.gap("", verificationReason("the end of the window is unknown: "+err.Error(), redact))
	} else if !end.After(b.start) {
		o.gap("", fmt.Sprintf("the Docker engine's clock reads %s, not after the activation's %s, so the window is unknown",
			end.Format(time.RFC3339Nano), b.start.Format(time.RFC3339Nano)))
	} else {
		o.end = end
	}
	if o.current != nil {
		if o.current.omitted > 0 {
			o.gap("", fmt.Sprintf("%d containers exceed the container limit", o.current.omitted))
		}
		for _, target := range verificationTargets(*o.current, config.AdditionalContainers) {
			if !slices.ContainsFunc(b.targets, func(known verificationTarget) bool {
				return known.id == target.id && (target.id != "" || known.source == target.source)
			}) {
				o.gap(target.source, "added after the activation; containers without a baseline are not covered")
			}
		}
	}
	current := make([]*verificationGeneration, len(b.targets))
	for i, target := range b.targets {
		source := VerificationSource{Source: target.source, Role: target.Role, Name: target.Name, ContainerID: target.id, BaselineCrashes: []string{}}
		source.Role, source.Name, source.Source = redact(source.Role), redact(source.Name), redact(source.Source)
		if target.generation != nil {
			source.ImageID, source.StartedAt, source.RestartCount = target.generation.image, target.generation.startedAt, target.generation.restartCount
			source.LogDriver = target.generation.logDriver
		}
		for id := range target.crashes {
			source.BaselineCrashes = append(source.BaselineCrashes, redact(id))
		}
		slices.Sort(source.BaselineCrashes)
		if target.generation == nil {
			o.sources = append(o.sources, source)
			continue
		}
		generation, err := inspectGeneration(ctx, docker, target.id, config.OperationTimeout)
		switch {
		case err != nil:
			logGap(target.source, err.Error())
			crashGap(target.source, err.Error())
		case !generation.same(target.generation):
			reason := fmt.Sprintf("restarted since the activation (started %s, %d restarts; at activation %s, %d restarts); only the selected generation is covered",
				generation.startedAt, generation.restartCount, target.generation.startedAt, target.generation.restartCount)
			logGap(target.source, reason)
			crashGap(target.source, reason)
		default:
			current[i] = generation
			source.State = generation.state
		}
		if current[i] != nil {
			switch {
			case generation.logDriver != "json-file":
				logGap(target.source, fmt.Sprintf("log driver %q is not covered; only json-file without rotation is", generation.logDriver))
			case generation.logRotation != "":
				logGap(target.source, fmt.Sprintf("the json-file driver rotates at max-size %s and can drop lines", generation.logRotation))
			default:
				read, err := readVerificationLogs(ctx, docker, target, generation.tty, b.start, o.end, config, redact, func(e verificationEvent) {
					o.logs = append(o.logs, e)
				})
				source.LogBytes = read
				if err != nil {
					logGap(target.source, err.Error())
				}
			}
		}
		o.sources = append(o.sources, source)
	}
	crashes, err := readVerificationCrashes(ctx, config, b.targets, docker)
	if err != nil && len(crashes.Containers) != len(b.targets) {
		crashGap("", err.Error())
	}
	for i, result := range crashes.Containers {
		target := b.targets[i]
		if target.generation == nil {
			continue
		}
		if reason := crashResultGap(result); reason != "" && current[i] != nil {
			crashGap(target.source, reason)
		}
		if target.crashes == nil {
			if len(result.Records) > 0 {
				crashGap(target.source, fmt.Sprintf("%d crash records whose presence at activation is unknown", len(result.Records)))
			}
			continue
		}
		for _, record := range result.Records {
			if target.crashes[record.CrashID] {
				continue
			}
			o.crashes = append(o.crashes, crashEvent(target, record))
			if !record.Done {
				crashGap(target.source, fmt.Sprintf("crash %s has no done marker yet, so its metadata may be incomplete", record.CrashID))
			}
		}
	}
}

// crashEvent describes a new crash record. The collector already redacted
// its fields.
func crashEvent(target verificationTarget, record CrashRecord) verificationEvent {
	e := verificationEvent{source: target.source, containerID: target.id, event: "crash", time: record.Timestamp, crashID: record.CrashID, process: record.Process}
	for _, field := range []string{"stack_sig", "assert_condition", "assert_func", "assert_msg", "backtrace"} {
		switch value := record.Fields[field].(type) {
		case string:
			e.signature = append(e.signature, value)
		case []string:
			e.signature = append(e.signature, value...)
		}
	}
	summary := record.Entity + " (" + record.Process + ")"
	if message, _ := record.Fields["assert_msg"].(string); message != "" {
		summary += ": " + message
	} else if condition, _ := record.Fields["assert_condition"].(string); condition != "" {
		summary += ": assert " + condition
	} else if backtrace, _ := record.Fields["backtrace"].([]string); len(backtrace) > 0 {
		summary += ": " + strings.Join(backtrace[:min(3, len(backtrace))], " | ")
	}
	e.text, e.truncated = diagnosticBoundText(summary, maxFindingText)
	return e
}

func dockerTimestamp(t time.Time) string {
	return fmt.Sprintf("%d.%09d", t.Unix(), t.Nanosecond())
}

// maxVerificationLine bounds one log line; the rest of a longer line is
// skipped.
const maxVerificationLine = 64 << 10

// readVerificationLogs streams the container's log lines from start to end
// with Docker's timestamps and emits each line that is an event. It returns
// the bytes read and an error when the stream could not be read in full,
// exceeded MaxLogBytes, or held a line without a valid timestamp.
func readVerificationLogs(ctx context.Context, docker verificationDocker, target verificationTarget, tty bool, start, end time.Time, config VerificationConfig, redact func(string) string, emit func(verificationEvent)) (int64, error) {
	options := mobycl.ContainerLogsOptions{ShowStdout: true, ShowStderr: true, Timestamps: true, Since: dockerTimestamp(start)}
	if !end.IsZero() {
		options.Until = dockerTimestamp(end)
	}
	opCtx, cancel := context.WithTimeout(ctx, config.OperationTimeout)
	defer cancel()
	reader, err := docker.ContainerLogs(opCtx, target.id, options)
	if err != nil {
		return 0, errors.Join(err, opCtx.Err())
	}
	defer func() { _ = reader.Close() }()
	stop := context.AfterFunc(opCtx, func() { _ = reader.Close() })
	defer stop()
	counted := &countingReader{r: io.LimitReader(reader, config.MaxLogBytes+1)}
	malformed, outside := 0, 0
	line := func(data []byte, truncated bool) {
		stamp, text, _ := strings.Cut(string(data), " ")
		when, err := time.Parse(time.RFC3339Nano, stamp)
		if err != nil {
			malformed++
			return
		}
		if when.Before(start) || (!end.IsZero() && when.After(end)) {
			outside++
			return
		}
		for _, kind := range verificationLogEvents {
			if kind.pattern.MatchString(text) {
				e := verificationEvent{source: target.source, containerID: target.id, event: string(kind.event), time: when.UTC().Format(time.RFC3339Nano)}
				e.text, e.truncated = diagnosticBoundText(redact(strings.TrimRight(text, "\r")), maxFindingText)
				e.truncated = e.truncated || truncated
				emit(e)
				return
			}
		}
	}
	streams := map[byte]*verificationLines{1: {emit: line}, 2: {emit: line}}
	if tty {
		_, err = io.Copy(streams[1], counted)
	} else {
		header := make([]byte, 8)
		for {
			_, err = io.ReadFull(counted, header)
			if err == io.EOF {
				err = nil
				break
			}
			if err != nil {
				break
			}
			lines := streams[header[0]]
			if lines == nil || header[1] != 0 || header[2] != 0 || header[3] != 0 {
				err = errors.New("invalid Docker log frame")
				break
			}
			if _, err = io.CopyN(lines, counted, int64(binary.BigEndian.Uint32(header[4:]))); err != nil {
				break
			}
		}
	}
	if err == io.ErrUnexpectedEOF {
		err = errors.New("the log stream ends inside a frame")
	}
	for _, lines := range streams {
		lines.flush()
	}
	read := min(counted.n, config.MaxLogBytes)
	if counted.n > config.MaxLogBytes {
		// The limit ended the stream, whatever the reader made of that.
		err = fmt.Errorf("the log stream of the window exceeds %d bytes; later lines were not read", config.MaxLogBytes)
	}
	if malformed > 0 {
		err = errors.Join(err, fmt.Errorf("%d log lines have no valid timestamp", malformed))
	}
	return read, errors.Join(err, opCtx.Err())
}

// verificationLines splits a stream into lines of at most
// maxVerificationLine bytes.
type verificationLines struct {
	line      []byte
	truncated bool
	emit      func([]byte, bool)
}

func (l *verificationLines) Write(data []byte) (int, error) {
	n := len(data)
	for len(data) > 0 {
		end := bytes.IndexByte(data, '\n')
		chunk := data
		if end >= 0 {
			chunk = data[:end]
		}
		if room := maxVerificationLine - len(l.line); room < len(chunk) {
			l.line = append(l.line, chunk[:max(room, 0)]...)
			l.truncated = true
		} else {
			l.line = append(l.line, chunk...)
		}
		if end < 0 {
			break
		}
		l.flush()
		data = data[end+1:]
	}
	return n, nil
}

func (l *verificationLines) flush() {
	if len(l.line) > 0 || l.truncated {
		l.emit(l.line, l.truncated)
	}
	l.line, l.truncated = l.line[:0], false
}
