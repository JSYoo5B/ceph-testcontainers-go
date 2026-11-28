package diagnostics

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
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
	// LogTruncated: the result lost text of the window. The window holds
	// more than LogConfig.MaxBytes, and the result covers its first
	// MaxBytes; or a line of it was longer than 64 KiB, and its rest was
	// not read; or a line had no valid Docker timestamp, so whether it
	// belongs to the window is unknown, and it was not kept.
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
// timestamps excluded; zero selects 8 MiB, and at most 64 MiB is accepted. A
// process log keeps its start line even when that line alone exceeds
// MaxBytes, and then reports LogTruncated.
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
// exe_path ...). Without either, Start is zero and Lines are the lines since
// the container started, bounded alike; Status is then LogStartNotFound,
// unless the container's run changed during the read, which LogChanged
// reports first.
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
// on. Status reports what the result covers, in this order: LogChanged when
// the container's run changed during the read, LogStartNotFound without a
// start, LogTruncated when text of the window was lost (past
// LogConfig.MaxBytes, a line cut at 64 KiB, or a line without a valid
// timestamp), and otherwise LogComplete.
// The lines are redacted as Redact does. A private key block that spans
// lines is followed on its own stream, stdout or stderr, and masked whole;
// an end whose beginning the read did not see masks every earlier line of
// its stream since the stream's previous end, and a known secret split by
// the 64 KiB cut is masked too. An oracle that needs the raw bytes reads
// the container's Logs itself.
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
	return readProcessLog(ctx, docker, member, config, logRedactor(snapshot.Secrets), nil)
}

// defaultLogBytes and maxLogBytes bound LogConfig.MaxBytes.
const (
	defaultLogBytes = 8 << 20
	maxLogBytes     = 64 << 20
)

// logRedactor redacts one log line with secrets, also a secret that the
// missing end of a cut line split.
func logRedactor(secrets []string) func(string, bool) string {
	return evidence.LogLineRedactor(secrets, evidence.Redactor(secrets))
}

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
func logSnapshot(ctx context.Context, cluster *ceph.Container, daemon testcontainers.Container, role string, config LogConfig) (evidence.Snapshot[Target], evidence.Member, error) {
	if evidence.IsNil(daemon) {
		return evidence.Snapshot[Target]{}, evidence.Member{}, invalid(errors.New("the daemon container is nil"))
	}
	snapshot, err := takeSnapshot(ctx, cluster, nil, config.RedactValues, evidence.MaxContainers)
	if err != nil {
		return snapshot, evidence.Member{}, err
	}
	member, err := findLogMember(snapshot.Containers, daemon.GetContainerID(), role)
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

// inspectRun inspects a container's current run, which must have started.
func inspectRun(ctx context.Context, docker diagnosticsDocker, id string) (evidence.ContainerRun, error) {
	run, err := evidence.InspectRun(ctx, docker, id)
	if err == nil && run.Started.IsZero() {
		err = fmt.Errorf("the container has not started (start time %q)", run.StartedAt)
	}
	return run, err
}

// readProcessLog reads member's latest process log. keep selects the lines
// of the process that the result keeps, nil for all of them; its Start is
// always kept. redact redacts a kept line, which may lack its end.
func readProcessLog(ctx context.Context, docker diagnosticsDocker, member evidence.Member, config LogConfig, redact func(string, bool) string, keep func(string) bool) (*ProcessLog, error) {
	id := member.Container.GetContainerID()
	before, err := inspectRun(ctx, docker, id)
	if err != nil {
		return nil, fmt.Errorf("inspect %s.%s: %w", member.Role, member.Name, err)
	}
	w := &logWindow{max: config.MaxBytes, process: true, keep: keep}
	if err := streamLog(ctx, docker, id, before.Started, before.TTY, w.add); err != nil {
		return nil, fmt.Errorf("read the log of %s.%s: %w", member.Role, member.Name, err)
	}
	after, err := inspectRun(ctx, docker, id)
	if err != nil {
		return nil, fmt.Errorf("inspect %s.%s again: %w", member.Role, member.Name, err)
	}
	result := &ProcessLog{LogSource: LogSource{Role: member.Role, Name: member.Name, ContainerID: id,
		ContainerStartedAt: before.Started, Running: before.Running, Since: before.Started}}
	lines := w.result(redact)
	switch {
	case after.StartedAt != before.StartedAt || after.Running != before.Running:
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
// once the window exceeds max, lost once a line of it was cut or had no
// timestamp.
//
// The window follows the private key blocks of each stream through every
// line it reads, kept or not, and masks what lies inside one; an end
// without a beginning withholds the kept lines of its stream since the
// stream's last end (see evidence.KeyBlockFollower).
type logWindow struct {
	max        int
	process    bool
	keep       func(string) bool
	blocks     evidence.KeyBlockFollower
	size       int
	started    bool
	full, lost bool
	lines      []windowLine
}

// windowLine is one kept line: its stream and number in the read, its raw
// text, and its time and text with key blocks masked, not yet redacted.
type windowLine struct {
	stream     string
	number     int
	raw        string
	time       time.Time
	safe       string
	missingEnd bool
}

// add takes one line and reports whether the rest of the log can still
// change the window: a process window may start again, any other one stops
// once full.
func (w *logWindow) add(line evidence.LogLine) bool {
	safe, number, withhold := w.blocks.Follow(line.Stream, line.Text)
	if withhold >= 0 {
		for i := range w.lines {
			if w.lines[i].stream == line.Stream && w.lines[i].number >= withhold {
				w.lines[i].safe, w.lines[i].missingEnd = evidence.PrivateKeyMarker, false
			}
		}
	}
	if line.Malformed {
		// Its time is unknown, so it may belong to the window.
		w.lost = true
		return w.process || !w.full
	}
	kept := windowLine{stream: line.Stream, number: number, raw: line.Text, time: line.Time, safe: safe, missingEnd: line.Cut || line.Ended}
	if w.process && processBoundary(line.Text) {
		// The start line is kept even when it alone exceeds max; the window
		// is then truncated like any other that does.
		w.started, w.lost, w.size, w.lines = true, kept.missingEnd, len(line.Text), []windowLine{kept}
		w.full = w.size > w.max
		return true
	}
	if w.full {
		return w.process
	}
	if w.size += len(line.Text); w.size > w.max {
		w.full = true
		return w.process
	}
	w.lost = w.lost || kept.missingEnd
	if w.keep == nil || w.keep(line.Text) {
		w.lines = append(w.lines, kept)
	}
	return true
}

// truncated reports whether the window lost text to a bound or a line
// without a timestamp.
func (w *logWindow) truncated() bool { return w.full || w.lost }

// result returns the kept lines, redacted with redact.
func (w *logWindow) result(redact func(string, bool) string) []LogLine {
	lines := make([]LogLine, len(w.lines))
	for i, line := range w.lines {
		lines[i] = LogLine{Time: line.time, Text: redact(line.safe, line.missingEnd)}
	}
	return lines
}

// streamLog reads id's stdout and stderr log entries recorded since since
// and passes each line to visit, in order, until visit returns false. A
// line longer than evidence.MaxLogLine is cut, and visit is told so. The
// stream is closed when ctx ends.
func streamLog(ctx context.Context, docker diagnosticsDocker, id string, since time.Time, tty bool, visit func(evidence.LogLine) bool) error {
	reader, err := docker.ContainerLogs(ctx, id, dockerclient.ContainerLogsOptions{ShowStdout: true, ShowStderr: true, Timestamps: true,
		Since: since.UTC().Format(time.RFC3339Nano)})
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = reader.Close() })
	defer stop()
	_, err = evidence.ReadLogLines(reader, tty, 0, visit)
	return errors.Join(err, ctx.Err())
}
