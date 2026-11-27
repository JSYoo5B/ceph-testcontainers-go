package diagnostics

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/internal/evidence"
	dockerclient "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// LogStatus says how much of a log window one read covered. Only
// LogComplete means that a line missing from the result was not logged in
// the window.
type LogStatus string

const (
	// LogComplete: the read covered the whole window and the result holds
	// every line of it that it keeps.
	LogComplete LogStatus = "complete"
	// LogTruncated: the window holds more than LogConfig.MaxBytes. The
	// result covers its first MaxBytes; the rest was not read or not kept.
	LogTruncated LogStatus = "truncated"
	// LogStartNotFound: the log does not show where the window starts, so
	// earlier lines of it may be missing. A process log found no line where
	// a process starts after the container started; a MON log of
	// CollectManagerRefusals has no MGR process start to begin at, or its
	// log driver can drop lines.
	LogStartNotFound LogStatus = "start-not-found"
	// LogChanged: the container was started, stopped or restarted during
	// the read, so the lines may belong to another process than the one the
	// result names. Read again.
	LogChanged LogStatus = "changed"
)

// LogConfig bounds one CollectProcessLog or CollectManagerRefusals. MaxBytes
// bounds the text of each log window that is read, the lines' Docker
// timestamps excluded; zero selects 8 MiB, and at most 64 MiB is accepted.
// RedactValues are additional secrets to mask, as in Config: at most 128
// values of up to 4096 bytes. Invalid limits are refused with an error that
// matches ceph.ErrInvalidRequest.
type LogConfig struct {
	MaxBytes     int
	RedactValues []string
}

// LogLine is one log line, its text after redaction, with the time the
// Docker engine recorded for it. Containers of one engine share its clock,
// so the times of two containers' lines compare.
type LogLine struct {
	Time time.Time `json:"time"`
	Text string    `json:"text"`
}

// LogSource is the container log of one daemon that a read covered. Role
// and Name are the daemon's as Collect reports them (mon a, mgr b, osd 0).
// ContainerStartedAt and Running are the container's current run as Docker
// reported it before the read. Since is where the window starts: the
// process's Start, the container start when no process start was found,
// or, for a MON of CollectManagerRefusals, the MGR's process start.
type LogSource struct {
	Role               string    `json:"role"`
	Name               string    `json:"name"`
	ContainerID        string    `json:"container_id"`
	ContainerStartedAt time.Time `json:"container_started_at"`
	Running            bool      `json:"running"`
	Since              time.Time `json:"since"`
	Status             LogStatus `json:"status"`
}

// ProcessLog is the log of a daemon's latest process. Start is the line
// where that process begins, and it is the first of Lines: the line that the
// process logged when it started (ceph version ..., process ceph-mgr, pid
// N), or, for a process that a respawn began and that failed before it
// logged one, the last line of the process it replaced (mgr respawn
// exe_path ...). Without either, Start is zero, Status is LogStartNotFound
// and Lines are the lines since the container started, bounded alike.
type ProcessLog struct {
	LogSource
	Start LogLine   `json:"start"`
	Lines []LogLine `json:"lines"`
}

// CollectProcessLog reads the log of the latest process of daemon, a MON,
// MGR, OSD, MDS or RGW container that cluster tracks: the handle that
// Monitors, Managers or OSDs returned, or the container of a service.
//
// Every Ceph daemon logs a start line when its process starts, also when it
// respawns inside its container, as a MGR or MDS does. A respawned process
// can fail before it logs that line, for example when the MONs refuse its
// key, so the line that the replaced process logs right before it executes
// the new one also starts a process. The read takes the lines since the
// container's current run started and keeps those from the last such line
// on. Status reports what the result covers: LogComplete, or LogTruncated
// past LogConfig.MaxBytes, LogStartNotFound without a start, and LogChanged
// when the container's run changed during the read.
// The lines are redacted as Redact does, with the private key blocks that
// span lines masked whole; an oracle that needs the raw bytes reads the
// container's Logs itself.
//
// The read is bounded by ctx and the bytes it reads; nothing outlives the
// call. It reads a stopped container's last run as well. It never parses
// what a line means beyond where a process starts; that stays the test's
// judgment.
//
//purpose:observation,evidence
func CollectProcessLog(ctx context.Context, cluster *ceph.Container, daemon testcontainers.Container, config LogConfig) (*ProcessLog, error) {
	config, err := normalizeLogConfig(config)
	if err != nil {
		return nil, err
	}
	snapshot, member, err := logSnapshot(ctx, cluster, daemon, "", config)
	if err != nil {
		return nil, err
	}
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return nil, fmt.Errorf("process log Docker client: %w", err)
	}
	defer func() { _ = docker.Close() }()
	return readProcessLog(ctx, docker, member, config, evidence.Redactor(snapshot.secrets), nil)
}

// defaultLogBytes and maxLogBytes bound LogConfig.MaxBytes; maxLogLine
// bounds one line, which is cut beyond it.
const (
	defaultLogBytes = 8 << 20
	maxLogBytes     = 64 << 20
	maxLogLine      = 64 << 10
)

func normalizeLogConfig(config LogConfig) (LogConfig, error) {
	if config.MaxBytes == 0 {
		config.MaxBytes = defaultLogBytes
	}
	if config.MaxBytes < 1 || config.MaxBytes > maxLogBytes {
		return config, invalid(errors.New("invalid log read limits"))
	}
	if err := evidence.CheckCollectionInputs[Target](nil, config.RedactValues); err != nil {
		return config, invalid(fmt.Errorf("log read: %w", err))
	}
	return config, nil
}

// logDaemonRoles are the roles whose containers run a Ceph daemon process.
var logDaemonRoles = []string{"mon", "mgr", "osd", "mds", "rgw"}

// logSnapshot takes the inventory of cluster and finds daemon in it, a
// daemon container of the given role ("" for any). A container the cluster
// does not track, or one without a daemon, is an invalid request.
func logSnapshot(ctx context.Context, cluster *ceph.Container, daemon testcontainers.Container, role string, config LogConfig) (diagnosticsSnapshot, evidence.Member, error) {
	if evidence.IsNil(daemon) {
		return diagnosticsSnapshot{}, evidence.Member{}, invalid(errors.New("the daemon container is nil"))
	}
	snapshot, err := takeSnapshot(ctx, cluster, Config{RedactValues: config.RedactValues, MaxContainers: 128})
	if err != nil {
		return snapshot, evidence.Member{}, err
	}
	member, err := findLogMember(snapshot.containers, daemon.GetContainerID(), role)
	return snapshot, member, err
}

// findLogMember returns the daemon container of the given role ("" for
// any) with container ID id among containers.
func findLogMember(containers []Target, id, role string) (evidence.Member, error) {
	for _, member := range containers {
		if id != "" && member.Container != nil && member.Container.GetContainerID() == id {
			if slices.Contains(logDaemonRoles, member.Role) && (role == "" || member.Role == role) {
				return evidence.Member(member), nil
			}
			break
		}
	}
	want := "daemon"
	if role != "" {
		want = role
	}
	return evidence.Member{}, invalid(fmt.Errorf("the container is not a %s that the cluster tracks", want))
}

// processStartLine matches the line that every Ceph daemon logs when its
// process starts (global_init's output_ceph_version): the log entry's time,
// thread and level, then "ceph version <version> (<commit>) <release>
// (<type>), process <name>, pid <pid>".
var processStartLine = regexp.MustCompile(`^\S+ [0-9a-f]+ +-?\d+ ceph version \S+ \([0-9a-f]+\) \S+ \([^)]*\), process \S+, pid \d+$`)

// processRespawnLine matches the last line that a MGR or MDS logs before it
// executes itself again to respawn (the respawn function of MgrStandby and
// MDSDaemon logs " exe_path <path>" right before execv): the next line
// belongs to the new process.
var processRespawnLine = regexp.MustCompile(`^\S+ [0-9a-f]+ +-?\d+ (?:\S+ )?respawn +exe_path \S+$`)

// processBoundary reports whether text starts a process.
func processBoundary(text string) bool {
	return processStartLine.MatchString(text) || processRespawnLine.MatchString(text)
}

// containerRun is a container's current run as one inspection reports it.
type containerRun struct {
	startedAt string
	started   time.Time
	running   bool
	tty       bool
	// rotates is set when the log driver can drop the oldest lines: any
	// driver but json-file, or json-file with a max-size.
	rotates bool
}

func inspectRun(ctx context.Context, docker diagnosticsDocker, id string) (containerRun, error) {
	inspection, err := docker.ContainerInspect(ctx, id, dockerclient.ContainerInspectOptions{})
	if err != nil {
		return containerRun{}, err
	}
	ctr := inspection.Container
	var run containerRun
	if ctr.State != nil {
		run.startedAt, run.running = ctr.State.StartedAt, ctr.State.Running
	}
	started, err := time.Parse(time.RFC3339Nano, run.startedAt)
	if err != nil || started.IsZero() || started.Year() < 2 {
		return containerRun{}, fmt.Errorf("the container has not started (start time %q)", run.startedAt)
	}
	run.started = started.UTC()
	if ctr.Config != nil {
		run.tty = ctr.Config.Tty
	}
	if ctr.HostConfig != nil {
		size := ctr.HostConfig.LogConfig.Config["max-size"]
		run.rotates = ctr.HostConfig.LogConfig.Type != "json-file" || size != "" && size != "-1"
	}
	return run, nil
}

// readProcessLog reads member's latest process log. keep selects the lines
// of the process that the result keeps, nil for all of them; its Start is
// always kept.
func readProcessLog(ctx context.Context, docker diagnosticsDocker, member evidence.Member, config LogConfig, redact func(string) string, keep func(string) bool) (*ProcessLog, error) {
	id := member.Container.GetContainerID()
	before, err := inspectRun(ctx, docker, id)
	if err != nil {
		return nil, fmt.Errorf("inspect %s.%s: %w", member.Role, member.Name, err)
	}
	w := &logWindow{max: config.MaxBytes, process: true, keep: keep}
	if err := streamLog(ctx, docker, id, before.started, before.tty, w.add); err != nil {
		return nil, fmt.Errorf("read the log of %s.%s: %w", member.Role, member.Name, err)
	}
	after, err := inspectRun(ctx, docker, id)
	if err != nil {
		return nil, fmt.Errorf("inspect %s.%s again: %w", member.Role, member.Name, err)
	}
	result := &ProcessLog{LogSource: LogSource{Role: member.Role, Name: member.Name, ContainerID: id,
		ContainerStartedAt: before.started, Running: before.running, Since: before.started}}
	lines := redactLines(w.lines, redact)
	switch {
	case after.startedAt != before.startedAt || after.running != before.running:
		result.Status = LogChanged
	case !w.started:
		result.Status = LogStartNotFound
	case w.truncated():
		result.Status = LogTruncated
	default:
		result.Status = LogComplete
	}
	if w.started {
		result.Start, result.Since = lines[0], lines[0].Time
	}
	result.Lines = lines
	return result, nil
}

// logWindow keeps the lines of one log window up to max bytes of text. A
// process window starts again at each line where a process starts (see
// processBoundary), which it always keeps; keep, when set, selects the other
// lines it keeps. Every line counts towards max, kept or not. full is set
// once the window exceeds max, cut once a line of it was cut.
type logWindow struct {
	max       int
	process   bool
	keep      func(string) bool
	size      int
	started   bool
	full, cut bool
	lines     []LogLine
}

// add takes one line and reports whether the rest of the log can still
// change the window: a process window may start again, any other one stops
// once full.
func (w *logWindow) add(line LogLine, cut bool) bool {
	if w.process && processBoundary(line.Text) {
		w.started, w.full, w.cut, w.size, w.lines = true, false, cut, len(line.Text), []LogLine{line}
		return true
	}
	if w.full {
		return w.process
	}
	if w.size += len(line.Text); w.size > w.max {
		w.full = true
		return w.process
	}
	w.cut = w.cut || cut
	if w.keep == nil || w.keep(line.Text) {
		w.lines = append(w.lines, line)
	}
	return true
}

// truncated reports whether the window lost text to a bound.
func (w *logWindow) truncated() bool { return w.full || w.cut }

// streamLog reads id's stdout and stderr log entries recorded since since
// and passes each line to visit, in order, until visit returns false. A
// line longer than maxLogLine is cut, and visit is told so. The stream is
// closed when ctx ends.
func streamLog(ctx context.Context, docker diagnosticsDocker, id string, since time.Time, tty bool, visit func(LogLine, bool) bool) error {
	reader, err := docker.ContainerLogs(ctx, id, dockerclient.ContainerLogsOptions{ShowStdout: true, ShowStderr: true, Timestamps: true,
		Since: since.UTC().Format(time.RFC3339Nano)})
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = reader.Close() })
	defer stop()
	lines := &logLines{visit: visit}
	err = evidence.DemuxLogs(reader, tty, lines, lines)
	if errors.Is(err, errLogEnough) {
		err = nil
	}
	if err == nil {
		lines.flush()
	}
	return errors.Join(err, ctx.Err())
}

// errLogEnough ends a read whose window can no longer change.
var errLogEnough = errors.New("the log window is complete")

// logLines splits demultiplexed log bytes into lines with Docker's
// timestamp prefix.
type logLines struct {
	visit   func(LogLine, bool) bool
	pending []byte
	cut     bool
	done    bool
}

func (l *logLines) Write(data []byte) (int, error) {
	n := len(data)
	for len(data) > 0 && !l.done {
		end := slices.Index(data, '\n')
		chunk := data
		if end >= 0 {
			chunk = data[:end]
		}
		if room := maxLogLine - len(l.pending); len(chunk) > room {
			chunk, l.cut = chunk[:max(room, 0)], true
		}
		l.pending = append(l.pending, chunk...)
		if end < 0 {
			break
		}
		data = data[end+1:]
		l.emit()
	}
	if l.done {
		return n, errLogEnough
	}
	return n, nil
}

func (l *logLines) flush() {
	if len(l.pending) > 0 && !l.done {
		l.emit()
	}
}

func (l *logLines) emit() {
	text := strings.TrimSuffix(string(l.pending), "\r")
	line := LogLine{Text: text}
	if stamp, rest, ok := strings.Cut(text, " "); ok {
		if at, err := time.Parse(time.RFC3339Nano, stamp); err == nil {
			line = LogLine{Time: at.UTC(), Text: rest}
		}
	}
	if !l.visit(line, l.cut) {
		l.done = true
	}
	l.pending, l.cut = l.pending[:0], false
}

// privateKeyMarker replaces the lines, or parts of them, inside a private
// key block, as Redact writes it for a whole block.
const privateKeyMarker = "[REDACTED PRIVATE KEY]"

// redactLines redacts each line with redact, after masking what lies inside
// a private key block that spans lines: redact sees one line at a time and
// would miss the block's payload. An end without a beginning masks its line
// up to the end.
func redactLines(lines []LogLine, redact func(string) string) []LogLine {
	result := make([]LogLine, len(lines))
	open := false
	for i, line := range lines {
		var out strings.Builder
		from := 0
		evidence.VisitPrivateKeyDelimiters(line.Text, func(begin bool, start, end int) {
			if begin {
				if !open {
					out.WriteString(line.Text[from:start])
					open = true
				}
				return
			}
			if !open {
				out.Reset()
			}
			out.WriteString(privateKeyMarker)
			open, from = false, end
		})
		if open {
			out.WriteString(privateKeyMarker)
		} else {
			out.WriteString(line.Text[from:])
		}
		result[i] = LogLine{Time: line.Time, Text: redact(out.String())}
	}
	return result
}
