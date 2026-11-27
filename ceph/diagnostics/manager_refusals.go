package diagnostics

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/internal/evidence"
	"github.com/testcontainers/testcontainers-go"
)

// ManagerRefusals is what a MGR's latest process and the owned MONs logged
// about the refused authentication of its MonClients.
//
// Manager and Start are the MGR's process log window, as CollectProcessLog
// reads it. Refusals are the MonClient lines of that process that report
// "(13) Permission denied"; a MGR runs several MonClients, its beacon and
// those of its modules, and the line does not say which one was refused.
// Monitors are the logs of the MONs that the cluster tracks since the
// process started, sorted by name, each with the refusals that name the
// MGR's entity.
type ManagerRefusals struct {
	Manager  LogSource         `json:"manager"`
	Start    LogLine           `json:"start"`
	Refusals []LogLine         `json:"refusals"`
	Monitors []MonitorRefusals `json:"monitors"`
}

// MonitorRefusals is one MON's log window and the refusals in it.
type MonitorRefusals struct {
	LogSource
	Refusals []MonitorRefusal `json:"refusals"`
}

// MonitorRefusal is one MON log line that refused an authentication of the
// MGR's entity. Reason is the native wording: "without presenting ticket",
// "using bad ticket" or "using mismatching ticket" for a refused global_id
// reclaim, which names GlobalID, and "unexpected key" for a key that the
// MON does not hold, which names no global ID. Compare GlobalID with the
// MgrMap GID to tell the beacon MonClient from a module's.
type MonitorRefusal struct {
	Reason   string  `json:"reason"`
	GlobalID uint64  `json:"global_id,omitempty"`
	Line     LogLine `json:"line"`
}

// CollectManagerRefusals reads, for one MGR that cluster tracks, the
// MonClient refusals of its latest process and what the MONs that cluster
// tracks logged about the same entity since that process started.
//
// Each source has its own Status. The MGR's is that of CollectProcessLog.
// A MON's covers its log since the MGR's process start: LogTruncated past
// LogConfig.MaxBytes, which bounds each source, and LogStartNotFound when the
// MGR's process start was not found (the MON is then read since the MGR
// container's start) or when the MON's log driver can drop lines. Only a
// source with LogComplete shows that a refusal it does not list was not
// logged. A read that fails returns its error and no result: no source is
// reported as read when it was not.
//
// The MON lines that count are the cephx server refusals logged at the
// default log level. The observation reports what was logged; whether a
// refusal is the SUT's, a module client's or a stale fixture's stays the
// test's judgment.
//
//purpose:observation,evidence
func CollectManagerRefusals(ctx context.Context, cluster *ceph.Container, manager *ceph.ManagerContainer, config LogConfig) (*ManagerRefusals, error) {
	config, err := normalizeLogConfig(config)
	if err != nil {
		return nil, err
	}
	var daemon testcontainers.Container
	if manager != nil {
		daemon = manager.Container
	}
	snapshot, member, err := logSnapshot(ctx, cluster, daemon, "mgr", config)
	if err != nil {
		return nil, err
	}
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return nil, fmt.Errorf("refusal Docker client: %w", err)
	}
	defer func() { _ = docker.Close() }()
	return readManagerRefusals(ctx, docker, snapshot, member, config)
}

func readManagerRefusals(ctx context.Context, docker diagnosticsDocker, snapshot diagnosticsSnapshot, manager evidence.Member, config LogConfig) (*ManagerRefusals, error) {
	redact := evidence.Redactor(snapshot.secrets)
	process, err := readProcessLog(ctx, docker, manager, config, redact, isMonClientRefusal)
	if err != nil {
		return nil, err
	}
	result := &ManagerRefusals{Manager: process.LogSource, Start: process.Start, Refusals: []LogLine{}, Monitors: []MonitorRefusals{}}
	if len(process.Lines) > 0 {
		result.Refusals = process.Lines
		if process.Start.Text != "" {
			result.Refusals = slices.Clone(process.Lines[1:])
		}
	}
	entity := "mgr." + manager.Name
	for _, member := range snapshot.containers {
		if member.Role != "mon" || member.Container == nil {
			continue
		}
		monitor, err := readMonitorRefusals(ctx, docker, evidence.Member(member), process, entity, config, redact)
		if err != nil {
			return nil, err
		}
		result.Monitors = append(result.Monitors, monitor)
	}
	slices.SortFunc(result.Monitors, func(a, b MonitorRefusals) int { return strings.Compare(a.Name, b.Name) })
	return result, nil
}

// readMonitorRefusals scans one MON's log since the MGR's process start for
// the refusals of entity.
func readMonitorRefusals(ctx context.Context, docker diagnosticsDocker, member evidence.Member, process *ProcessLog, entity string, config LogConfig, redact func(string) string) (MonitorRefusals, error) {
	id := member.Container.GetContainerID()
	run, err := inspectRun(ctx, docker, id)
	if err != nil {
		return MonitorRefusals{}, fmt.Errorf("inspect mon.%s: %w", member.Name, err)
	}
	source := LogSource{Role: member.Role, Name: member.Name, ContainerID: id, ContainerStartedAt: run.started, Running: run.running, Since: process.Since}
	w := &logWindow{max: config.MaxBytes, keep: func(text string) bool {
		_, ok := monitorRefusal(text, entity)
		return ok
	}}
	if err := streamLog(ctx, docker, id, process.Since, run.tty, w.add); err != nil {
		return MonitorRefusals{}, fmt.Errorf("read the log of mon.%s: %w", member.Name, err)
	}
	switch {
	case process.Start.Text == "" || run.rotates:
		source.Status = LogStartNotFound
	case w.truncated():
		source.Status = LogTruncated
	default:
		source.Status = LogComplete
	}
	// The window keeps only refusals; each is classified on its raw text and
	// reported with its redacted line.
	refusals := []MonitorRefusal{}
	for i, line := range redactLines(w.lines, redact) {
		refusal, _ := monitorRefusal(w.lines[i].Text, entity)
		refusal.Line = line
		refusals = append(refusals, refusal)
	}
	return MonitorRefusals{LogSource: source, Refusals: refusals}, nil
}

// monClientRefusal matches a MonClient's log entry that reports EACCES, as a
// MGR logs it when a MON refuses its authentication.
var monClientRefusal = regexp.MustCompile(`^\S+ [0-9a-f]+ +-?\d+ monclient(?:\([a-z]+\))?: .*\(13\) Permission denied$`)

func isMonClientRefusal(text string) bool { return monClientRefusal.MatchString(text) }

// cephxServerRefusal matches the cephx server refusals that a MON logs at
// level 0: the three refused global_id reclaims, which name the global ID,
// and a key that the MON does not hold.
var cephxServerRefusal = regexp.MustCompile(`^\S+ [0-9a-f]+ +-?\d+ cephx server (\S+):\s+(?:attempt to reclaim global_id (\d+) (without presenting ticket|using bad ticket|using mismatching ticket)|(unexpected key): .*)$`)

// monitorRefusal returns the refusal that text logs for entity.
func monitorRefusal(text, entity string) (MonitorRefusal, bool) {
	match := cephxServerRefusal.FindStringSubmatch(text)
	if match == nil || match[1] != entity {
		return MonitorRefusal{}, false
	}
	if match[4] != "" {
		return MonitorRefusal{Reason: match[4]}, true
	}
	id, err := strconv.ParseUint(match[2], 10, 64)
	if err != nil || id == 0 {
		return MonitorRefusal{}, false
	}
	return MonitorRefusal{Reason: match[3], GlobalID: id}, true
}
