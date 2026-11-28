package finalize

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/internal/evidence"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/internal/evidencehook"
	"github.com/containerd/errdefs"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// clusterSnapshot is what a verification takes from the evidence inventory of
// its cluster: the selected containers, the separate control container and
// the secrets to redact, which start with the configured values.
type clusterSnapshot struct {
	containers   []diagnostics.Target
	controlPlane testcontainers.Container
	secrets      []string
	omitted      int
}

// takeClusterSnapshot takes the inventory of cluster and selects the
// cluster's containers and config.AdditionalContainers, at most 128.
func takeClusterSnapshot(ctx context.Context, cluster *ceph.Container, config VerificationConfig) (clusterSnapshot, error) {
	snapshot := clusterSnapshot{secrets: slices.Clone(config.RedactValues)}
	inv, err := evidencehook.Inventory(ctx, cluster)
	snapshot.secrets = append(snapshot.secrets, inv.Secrets...)
	if err != nil {
		return snapshot, err
	}
	snapshot.controlPlane = inv.ControlPlane
	snapshot.containers, snapshot.omitted = evidence.SelectAs(inv, config.AdditionalContainers, 128)
	return snapshot, nil
}

// verificationDocker is the part of the Docker client that verification
// uses.
type verificationDocker interface {
	evidence.CrashDocker
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
func verificationTargets(snapshot clusterSnapshot, additional []diagnostics.Target) []verificationTarget {
	requested := func(target diagnostics.Target) bool {
		return slices.ContainsFunc(additional, func(extra diagnostics.Target) bool {
			return evidence.SameContainer(evidence.Handle(extra.Container), target.Container)
		})
	}
	var targets []verificationTarget
	for _, target := range snapshot.containers {
		cluster := evidence.SameContainer(target.Container, snapshot.controlPlane) || (target.Role == "client" && target.Name == "toolkit")
		if cluster && !requested(target) {
			continue
		}
		id := ""
		if target.Container != nil {
			id = target.Container.GetContainerID()
		}
		targets = append(targets, verificationTarget{Target: target, source: verificationSourceName(target.Role, target.Name), id: id})
	}
	return targets
}

func dockerClock(ctx context.Context, docker verificationDocker, timeout time.Duration) (time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	info, err := docker.Info(ctx, mobycl.InfoOptions{})
	if err := errors.Join(err, ctx.Err()); err != nil {
		return time.Time{}, err
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
	case err != nil || ctx.Err() != nil:
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
func recordVerificationBaseline(ctx context.Context, config VerificationConfig, snapshot clusterSnapshot, docker verificationDocker) (*verificationBaseline, error) {
	b := &verificationBaseline{secrets: slices.Clone(snapshot.secrets)}
	redact := evidence.Redactor(b.secrets)
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
	crashes, err := readVerificationCrashes(ctx, config, b.targets, b.secrets, docker)
	for i, result := range crashes.Containers {
		target := &b.targets[i]
		if target.generation == nil {
			continue
		}
		if err := confirmVerificationGeneration(ctx, docker, *target, config.OperationTimeout); err != nil {
			both(target.source, "crash baseline: "+err.Error())
			continue
		}
		if reason := crashResultGap(result); reason != "" {
			b.crashGaps = append(b.crashGaps, VerificationGap{Source: target.source, Reason: "at activation: " + verificationReason(reason, redact)})
			continue
		}
		target.crashes = map[string]bool{}
		for _, record := range result.Records {
			// The raw identity, so that report redaction cannot merge
			// distinct crashes.
			target.crashes[record.Identity] = true
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
// result per target in order. The collector redacts with secrets, the
// configured values together with the cluster's own: it takes them from the
// snapshot, not from its RedactValues.
func readVerificationCrashes(ctx context.Context, config VerificationConfig, targets []verificationTarget, secrets []string, docker verificationDocker) (*evidence.CrashReport, error) {
	crashConfig, _ := evidence.NormalizeCrashConfig(evidence.CrashConfig[evidence.Member]{OperationTimeout: config.OperationTimeout})
	members := make([]evidence.Member, len(targets))
	for i, target := range targets {
		members[i] = evidence.Member(target.Target)
	}
	report := &evidence.CrashReport{SchemaVersion: 1, StartedAt: time.Now().UTC(), Containers: []evidence.CrashContainer{}}
	return evidence.CollectCrashes(ctx, crashConfig, members, nil, 0, slices.Clone(secrets), docker, report)
}

// crashResultGap says why a container's crash directory does not establish
// which crashes it holds, or returns "".
func crashResultGap(result evidence.CrashContainer) string {
	switch result.Outcome {
	case evidence.CrashRecordsFound, evidence.CrashDirectoryEmpty:
		if len(result.Omitted) > 0 {
			return fmt.Sprintf("crash directory: %d metadata files were not accepted (first: %s, %s)", len(result.Omitted), result.Omitted[0].Path, result.Omitted[0].Reason)
		}
		return ""
	case evidence.CrashDirectoryAbsent:
		return "the container has no crash directory, so no crash can be recorded there"
	}
	reason := "crash directory " + string(result.Outcome)
	if result.Error != "" {
		reason += ": " + result.Error
	}
	return reason
}

func verificationReason(reason string, redact func(string) string) string {
	reason, _ = evidence.BoundText(strings.ReplaceAll(redact(reason), "\n", "; "), 1024)
	if reason == "" {
		reason = "?"
	}
	return reason
}

// observeVerification observes the window from the baseline until now.
func observeVerification(ctx context.Context, config VerificationConfig, b *verificationBaseline, docker verificationDocker, o *verificationObservation) {
	w := verificationWindow{config: config, baseline: b, docker: docker, o: o, redact: evidence.Redactor(o.secrets), secrets: o.secrets}
	w.close(ctx)
	w.checkCoverage()
	current := make([]*verificationGeneration, len(b.targets))
	for i, target := range b.targets {
		current[i] = w.observeSource(ctx, target)
	}
	w.observeCrashes(ctx, current)
}

// verificationWindow is one observation of a verification's window: it
// records into o what the baseline's targets did between the activation and
// now. It reads the baseline and changes nothing in the cluster.
type verificationWindow struct {
	secrets  []string
	config   VerificationConfig
	baseline *verificationBaseline
	docker   verificationDocker
	o        *verificationObservation
	redact   func(string) string
}

func (w verificationWindow) logGap(source, reason string) {
	w.o.logGaps = append(w.o.logGaps, VerificationGap{Source: source, Reason: verificationReason(reason, w.redact)})
}

func (w verificationWindow) crashGap(source, reason string) {
	w.o.crashGap = append(w.o.crashGap, VerificationGap{Source: source, Reason: verificationReason(reason, w.redact)})
}

// close reads the end of the window on the Docker engine's clock; an
// unreadable or non-advancing clock leaves the window open, as a gap.
func (w verificationWindow) close(ctx context.Context) {
	end, err := dockerClock(ctx, w.docker, w.config.OperationTimeout)
	switch {
	case err != nil:
		w.o.gap("", verificationReason("the end of the window is unknown: "+err.Error(), w.redact))
	case !end.After(w.baseline.start):
		w.o.gap("", fmt.Sprintf("the Docker engine's clock reads %s, not after the activation's %s, so the window is unknown",
			end.Format(time.RFC3339Nano), w.baseline.start.Format(time.RFC3339Nano)))
	default:
		w.o.end = end
	}
}

// checkCoverage reports containers of the current snapshot that the baseline
// does not cover.
func (w verificationWindow) checkCoverage() {
	if w.o.current == nil {
		return
	}
	if w.o.current.omitted > 0 {
		w.o.gap("", fmt.Sprintf("%d containers exceed the container limit", w.o.current.omitted))
	}
	for _, target := range verificationTargets(*w.o.current, w.config.AdditionalContainers) {
		if !slices.ContainsFunc(w.baseline.targets, func(known verificationTarget) bool {
			return known.id == target.id && (target.id != "" || known.source == target.source)
		}) {
			w.o.gap(target.source, "added after the activation; containers without a baseline are not covered")
		}
	}
}

// observeSource records target's source entry and, when the generation
// selected at activation still runs, reads its logs. It returns that current
// generation, or nil when the target is not covered.
func (w verificationWindow) observeSource(ctx context.Context, target verificationTarget) *verificationGeneration {
	source := w.sourceOf(target)
	defer func() { w.o.sources = append(w.o.sources, source) }()
	if target.generation == nil {
		return nil
	}
	generation, err := inspectGeneration(ctx, w.docker, target.id, w.config.OperationTimeout)
	switch {
	case err != nil:
		w.logGap(target.source, err.Error())
		w.crashGap(target.source, err.Error())
		return nil
	case !generation.same(target.generation):
		reason := fmt.Sprintf("restarted since the activation (started %s, %d restarts; at activation %s, %d restarts); only the selected generation is covered",
			generation.startedAt, generation.restartCount, target.generation.startedAt, target.generation.restartCount)
		w.logGap(target.source, reason)
		w.crashGap(target.source, reason)
		return nil
	}
	source.State = generation.state
	switch {
	case generation.logDriver != "json-file":
		w.logGap(target.source, fmt.Sprintf("log driver %q is not covered; only json-file without rotation is", generation.logDriver))
	case generation.logRotation != "":
		w.logGap(target.source, fmt.Sprintf("the json-file driver rotates at max-size %s and can drop lines", generation.logRotation))
	default:
		var keep func(verificationLogLine)
		if w.config.ArchiveLogs {
			keep = func(line verificationLogLine) {
				line.Source, line.ContainerID = source.Source, source.ContainerID
				w.o.lines = append(w.o.lines, line)
			}
		}
		read, lines, err := readVerificationLogs(ctx, w.docker, target, generation.tty, w.baseline.start, w.o.end, w.config, evidence.LogLineRedactor(w.secrets, w.redact), func(e verificationEvent) {
			w.o.logs = append(w.o.logs, e)
		}, keep)
		source.LogBytes, source.LogLines = read, lines
		if err != nil {
			w.logGap(target.source, err.Error())
		}
	}
	if err := confirmVerificationGeneration(ctx, w.docker, target, w.config.OperationTimeout); err != nil {
		w.logGap(target.source, "after log collection: "+err.Error())
		w.crashGap(target.source, "after log collection: "+err.Error())
		return nil
	}
	return generation
}

// confirmVerificationGeneration brackets evidence reads with the activation
// generation. A later successful read cannot establish the earlier generation's
// coverage; already observed events remain useful alongside the resulting gap.
func confirmVerificationGeneration(ctx context.Context, docker verificationDocker, target verificationTarget, timeout time.Duration) error {
	generation, err := inspectGeneration(ctx, docker, target.id, timeout)
	if err != nil {
		return err
	}
	if !generation.same(target.generation) {
		return fmt.Errorf("container generation changed during evidence collection (started %s, %d restarts; at activation %s, %d restarts)", generation.startedAt, generation.restartCount, target.generation.startedAt, target.generation.restartCount)
	}
	return nil
}

// sourceOf is target's redacted source entry with its activation generation
// and baseline crashes.
func (w verificationWindow) sourceOf(target verificationTarget) VerificationSource {
	source := VerificationSource{Source: target.source, Role: target.Role, Name: target.Name, ContainerID: target.id, BaselineCrashes: []string{}}
	source.Role, source.Name, source.Source = w.redact(source.Role), w.redact(source.Name), w.redact(source.Source)
	if target.generation != nil {
		source.ImageID, source.StartedAt, source.RestartCount = target.generation.image, target.generation.startedAt, target.generation.restartCount
		source.LogDriver = target.generation.logDriver
	}
	for id := range target.crashes {
		source.BaselineCrashes = append(source.BaselineCrashes, w.redact(id))
	}
	slices.Sort(source.BaselineCrashes)
	return source
}

// observeCrashes reads the crash directories again and records each crash
// absent at activation; current holds each target's still-covered generation.
func (w verificationWindow) observeCrashes(ctx context.Context, current []*verificationGeneration) {
	b := w.baseline
	crashes, err := readVerificationCrashes(ctx, w.config, b.targets, w.secrets, w.docker)
	if err != nil && len(crashes.Containers) != len(b.targets) {
		w.crashGap("", err.Error())
	}
	for i, result := range crashes.Containers {
		target := b.targets[i]
		if target.generation == nil {
			continue
		}
		if current[i] != nil {
			if err := confirmVerificationGeneration(ctx, w.docker, target, w.config.OperationTimeout); err != nil {
				w.crashGap(target.source, "after crash collection: "+err.Error())
			}
		}
		if reason := crashResultGap(result); reason != "" && current[i] != nil {
			w.crashGap(target.source, reason)
		}
		if target.crashes == nil {
			if len(result.Records) > 0 {
				w.crashGap(target.source, fmt.Sprintf("%d crash records whose presence at activation is unknown", len(result.Records)))
			}
			continue
		}
		w.recordNewCrashes(target, result.Records)
	}
}

// recordNewCrashes records the crashes of records that target did not hold at
// activation. One crash can be seen twice, as unposted and as posted
// metadata: it counts once, and is complete when any sighting has its done
// marker. Crashes are compared by their raw identity; the reported ID is the
// redacted one.
func (w verificationWindow) recordNewCrashes(target verificationTarget, records []evidence.CrashRecord) {
	var order []string
	done, reported := map[string]bool{}, map[string]string{}
	for _, record := range records {
		if target.crashes[record.Identity] {
			continue
		}
		if _, ok := done[record.Identity]; !ok {
			order = append(order, record.Identity)
			reported[record.Identity] = record.CrashID
			w.o.crashes = append(w.o.crashes, crashEvent(target, record))
		}
		done[record.Identity] = done[record.Identity] || record.Done
	}
	for _, id := range order {
		if !done[id] {
			w.crashGap(target.source, fmt.Sprintf("crash %s has no done marker yet, so its metadata may be incomplete", reported[id]))
		}
	}
}

// crashEvent describes a new crash record. The collector already redacted
// its fields.
func crashEvent(target verificationTarget, record evidence.CrashRecord) verificationEvent {
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
	e.match = summary
	e.text, e.truncated = evidence.BoundText(summary, maxFindingText)
	return e
}

func dockerTimestamp(t time.Time) string {
	return fmt.Sprintf("%d.%09d", t.Unix(), t.Nanosecond())
}

// readVerificationLogs streams the container's log lines from start to end
// with Docker's timestamps and emits each line that is an event. keep, when
// not nil, receives every line of the window in the order received, with
// split cluster log entries restored (verificationDeinterleaver). Both run
// after the stream is read, since a private key's end can withhold earlier
// lines. It returns the bytes read, the lines of the window and an error
// when the stream could not be read in full, exceeded MaxLogBytes, or held a
// line without a valid timestamp.
func readVerificationLogs(ctx context.Context, docker verificationDocker, target verificationTarget, tty bool, start, end time.Time, config VerificationConfig, redact func(string, bool) string, emit func(verificationEvent), keep func(verificationLogLine)) (int64, int, error) {
	options := mobycl.ContainerLogsOptions{ShowStdout: true, ShowStderr: true, Timestamps: true, Since: dockerTimestamp(start)}
	if !end.IsZero() {
		options.Until = dockerTimestamp(end)
	}
	opCtx, cancel := context.WithTimeout(ctx, config.OperationTimeout)
	defer cancel()
	reader, err := docker.ContainerLogs(opCtx, target.id, options)
	if err != nil {
		return 0, 0, errors.Join(err, opCtx.Err())
	}
	defer func() { _ = reader.Close() }()
	stop := context.AfterFunc(opCtx, func() { _ = reader.Close() })
	defer stop()
	outside, unrecognized := 0, 0
	// held are the lines of the window, kept until the end of the read.
	var held []verificationHeldLine
	var blocks evidence.KeyBlockFollower
	// follow tracks the private key blocks of a line's stream, whether or
	// not the line belongs to the window. An end without a beginning
	// withholds the stream's held lines since the last end.
	follow := func(line evidence.LogLine) (string, int) {
		safe, number, withhold := blocks.Follow(line.Stream, line.Text)
		if withhold >= 0 {
			for i := range held {
				if held[i].stream == line.Stream && held[i].number >= withhold {
					held[i].withhold()
				}
			}
		}
		return safe, number
	}
	visit := func(line evidence.LogLine) {
		safe, number := follow(line)
		if line.Malformed || line.Time.Before(start) || (!end.IsZero() && line.Time.After(end)) {
			if !line.Malformed {
				outside++
			}
			return
		}
		truncated := line.Cut || line.Ended
		redacted := redact(safe, truncated)
		h := verificationHeldLine{stream: line.Stream, number: number}
		when := line.Time.Format(time.RFC3339Nano)
		if keep != nil {
			h.line = &verificationLogLine{Stream: line.Stream, Time: when, Text: redacted, Truncated: truncated}
		}
		kind, _, _, unclassified := classifyVerificationLine(line.Text)
		if unclassified {
			unrecognized++
		} else if kind != "" {
			e := verificationEvent{source: target.source, containerID: target.id, event: string(kind), time: when}
			// Expectations match the whole redacted line; only the text
			// that the report keeps is bounded.
			e.match = redacted
			e.text, e.truncated = evidence.BoundText(e.match, maxFindingText)
			e.truncated = e.truncated || truncated
			h.event = &e
		}
		held = append(held, h)
	}
	// A MON's cluster log entries can be split by its own daemon lines on
	// its standard error; the deinterleaver restores them before any line
	// is classified. Only that stream of a MON is read this way (with a TTY
	// it arrives as the one stream there is): the split is established for
	// no other daemon, and two lines of another one are two records.
	restore := newVerificationDeinterleaver(visit)
	read, err := evidence.ReadLogLines(reader, tty, config.MaxLogBytes, func(line evidence.LogLine) bool {
		if target.Role == verificationSplitRole && (line.Stream == "stderr" || tty) {
			restore.line(line)
		} else {
			visit(line)
		}
		return true
	})
	restore.finish()
	for _, h := range held {
		if h.line != nil {
			keep(*h.line)
		}
		if h.event != nil {
			emit(*h.event)
		}
	}
	if read.Exceeded {
		err = fmt.Errorf("the log stream of the window exceeds %d bytes; later lines were not read", config.MaxLogBytes)
	}
	if read.Malformed > 0 {
		err = errors.Join(err, fmt.Errorf("%d log lines have no valid timestamp", read.Malformed))
	}
	if unrecognized > 0 {
		err = errors.Join(err, fmt.Errorf("%d log lines carry an [ERR], [SEC] or [WRN] marker without a supported log envelope, so their severity was not classified", unrecognized))
	}
	if read.Cut > 0 {
		err = errors.Join(err, fmt.Errorf("%d log lines are longer than %d bytes; their rest was not read", read.Cut, evidence.MaxLogLine))
	}
	return read.Bytes, len(held), errors.Join(err, opCtx.Err())
}

// verificationHeldLine is one line of the window until the end of the read:
// its stream and number in the read, the line to keep, if kept, and its
// event, if any.
type verificationHeldLine struct {
	stream string
	number int
	line   *verificationLogLine
	event  *verificationEvent
}

// withhold replaces the line's text, and its event's, by
// evidence.PrivateKeyMarker.
func (h *verificationHeldLine) withhold() {
	if h.line != nil {
		h.line.Text = evidence.PrivateKeyMarker
	}
	if h.event != nil {
		h.event.match, h.event.text = evidence.PrivateKeyMarker, evidence.PrivateKeyMarker
	}
}
