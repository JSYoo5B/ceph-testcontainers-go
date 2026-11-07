package consumerevidence

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// GateSchema names the consumer's native MGR keyring gate receipt.
const GateSchema = "consumer-example/mgr-keyring-gate/v1"

// The state of one owned MGR's registered slot.
const (
	SlotRegistered = "registered"
	SlotRefused    = "registered-gid-refused"
	SlotAbsent     = "absent"
	SlotNoBeacon   = "no-beacon-session"
)

// GateBounds bounds a gate: each read, each MON log scan and the native
// control.
type GateBounds struct {
	Query, LogScan, Control time.Duration
	// LogBytes bounds the bytes of each MON log that are scanned.
	LogBytes int64
}

// monitorReclaimRefusal matches the three native MON log entries that refuse
// the global_id reclaim of a MonClient with EACCES: timestamp, thread, level
// and the cephx server message, which names the entity and the global ID. The
// same words quoted inside another entry, such as an audit line, do not
// match.
var monitorReclaimRefusal = regexp.MustCompile(`^\S+ [0-9a-f]+ +-?\d+ cephx server (\S+):\s+attempt to reclaim global_id (\d+) (without presenting ticket|using bad ticket|using mismatching ticket)$`)

// Refusal is one MON log line that refused a global_id reclaim.
type Refusal struct {
	Monitor  string `json:"monitor"`
	Entity   string `json:"entity"`
	GlobalID uint64 `json:"global_id"`
	Line     string `json:"line"`
}

// GateDaemon is one owned MGR as the gate saw it.
type GateDaemon struct {
	Daemon          string   `json:"daemon"`
	GID             uint64   `json:"gid"`
	Slot            string   `json:"slot"`
	BeaconMonitors  []string `json:"beacon_monitors"`
	ModuleGlobalIDs []uint64 `json:"module_global_ids,omitempty"`
	Stale           bool     `json:"stale"`
}

// Control is one native command that authenticates as a new native client.
type Control struct {
	Target   string `json:"target"`
	Command  string `json:"command"`
	ExitCode int    `json:"exit_code"`
	Complete bool   `json:"complete"`
}

// GateReceipt is one native MGR keyring gate. It never restarts or
// reconfigures a daemon: restarting would replace the condition it reports.
type GateReceipt struct {
	Schema             string            `json:"schema"`
	Label              string            `json:"label"`
	ObservedAt         time.Time         `json:"observed_at"`
	Map                ManagerMap        `json:"map"`
	Monitors           []MonitorSessions `json:"monitors"`
	Refusals           []Refusal         `json:"refusals"`
	Daemons            []GateDaemon      `json:"daemons"`
	Control            *Control          `json:"native_control,omitempty"`
	Queries            []Query           `json:"queries"`
	Complete           bool              `json:"complete"`
	GenerationEquality string            `json:"generation_equality"`
	FixtureError       string            `json:"fixture_error,omitempty"`
}

// maxRefusedSessions bounds the distinct refused entity and global ID pairs
// that one MON log scan keeps.
const maxRefusedSessions = 64

// scanRefusals returns the reclaim refusals of one MON log, one line for each
// distinct entity and global ID, reading at most limit bytes. Repeated
// refusals of one pair keep its first line, since the gate classifies pairs.
// A log longer than limit, or with more than maxRefusedSessions distinct
// pairs, is an error rather than a shorter list: a refusal that was not kept
// must not read as absent.
func scanRefusals(monitor string, logs io.Reader, limit int64) ([]Refusal, error) {
	counted := &io.LimitedReader{R: logs, N: limit + 1}
	scanner := bufio.NewScanner(counted)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	var found []Refusal
	seen := map[Session]bool{}
	for scanner.Scan() {
		match := monitorReclaimRefusal.FindStringSubmatch(strings.TrimSuffix(scanner.Text(), "\r"))
		if match == nil {
			continue
		}
		id, err := strconv.ParseUint(match[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: global_id %q", errMalformed, match[2])
		}
		key := Session{Entity: match[1], GlobalID: id}
		if seen[key] {
			continue
		}
		if len(found) == maxRefusedSessions {
			return nil, fmt.Errorf("%w: more than %d refused sessions", errOversized, maxRefusedSessions)
		}
		seen[key] = true
		found = append(found, Refusal{Monitor: monitor, Entity: match[1], GlobalID: id, Line: match[0]})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if counted.N <= 0 {
		return nil, errOversized
	}
	return found, nil
}

// readRefusals scans every MON's container log for reclaim refusals.
func readRefusals(ctx context.Context, monitors []*ceph.MonitorContainer, bounds GateBounds) ([]Refusal, []Query) {
	refusals := []Refusal{}
	var queries []Query
	for _, monitor := range monitors {
		queries = append(queries, bounded(ctx, bounds.LogScan, "log of mon."+monitor.DaemonName, func(ctx context.Context) error {
			logs, err := monitor.Logs(ctx)
			if err != nil {
				return err
			}
			stop := context.AfterFunc(ctx, func() { _ = logs.Close() })
			found, err := scanRefusals(monitor.DaemonName, logs, bounds.LogBytes)
			stop()
			if err = errors.Join(err, logs.Close()); ctx.Err() != nil {
				err = errors.Join(err, ctx.Err())
			}
			if err == nil {
				refusals = append(refusals, found...)
			}
			return err
		}))
	}
	return refusals, queries
}

// EvaluateGate classifies every owned MGR. An owned MGR is stale when it is
// absent from the MgrMap, when an owned MON refused the reclaim of the GID
// its registration holds, or when no owned MON holds its beacon session. A
// refusal of another global ID belongs to another MonClient of the process
// and leaves a registered slot with a live beacon healthy, so a SUT's
// failure is never excused by an unrelated refusal line. A failed native
// control means that native authentication of a new client failed too. Either
// is a fixture error, not evidence about the SUT. An incomplete read leaves
// the receipt incomplete, never clean. A complete gate requires an available
// active MGR and a native control targeting that registration.
func EvaluateGate(receipt GateReceipt, owned []string) GateReceipt {
	receipt.Complete = len(owned) > 0 && receipt.Map.Query.Complete() && len(receipt.Monitors) > 0 &&
		!slices.ContainsFunc(receipt.Monitors, func(m MonitorSessions) bool { return !m.Query.Complete() }) &&
		!slices.ContainsFunc(receipt.Queries, func(q Query) bool { return !q.Complete() })
	receipt.GenerationEquality = GenerationUnavailable
	receipt.Daemons, receipt.FixtureError = nil, ""
	if !receipt.Complete {
		return receipt
	}
	refused := map[Session]bool{}
	for _, refusal := range receipt.Refusals {
		refused[Session{Entity: refusal.Entity, GlobalID: refusal.GlobalID}] = true
	}
	var problems []string
	for _, name := range owned {
		gid, registered := receipt.Map.registered(name)
		daemon := GateDaemon{Daemon: "mgr." + name, GID: gid, BeaconMonitors: []string{}, ModuleGlobalIDs: moduleGlobalIDs(receipt.Monitors, name)}
		switch {
		case !registered:
			daemon.Slot = SlotAbsent
		case refused[Session{Entity: "mgr." + name, GlobalID: gid}]:
			daemon.Slot = SlotRefused
		default:
			daemon.BeaconMonitors = beaconMonitors(receipt.Monitors, Registration{Name: name, GID: gid})
			daemon.Slot = SlotRegistered
			if len(daemon.BeaconMonitors) == 0 {
				daemon.Slot = SlotNoBeacon
			}
		}
		daemon.Stale = daemon.Slot != SlotRegistered
		if daemon.Stale {
			problems = append(problems, fmt.Sprintf("%s (%s)", daemon.Daemon, daemon.Slot))
		}
		receipt.Daemons = append(receipt.Daemons, daemon)
	}
	switch control := receipt.Control; {
	case !receipt.Map.Available || receipt.Map.Active.Name == "" || receipt.Map.Active.GID == 0:
		receipt.Complete = false
		problems = append(problems, "no available active MGR")
	case control == nil:
		receipt.Complete = false
		problems = append(problems, "missing native control for the active MGR")
	case control.Target != "mgr."+receipt.Map.Active.Name || control.Command != "tell version":
		receipt.Complete = false
		problems = append(problems, "native control did not target the active MGR")
	case control.ExitCode != 0 || !control.Complete:
		problems = append(problems, fmt.Sprintf("native %s %s (exit %d, complete %t)", control.Command, control.Target, control.ExitCode, control.Complete))
	}
	if len(problems) > 0 {
		receipt.FixtureError = "fixture error: native " + strings.Join(problems, ", ") + "; a SUT authentication failure now is not product evidence"
	}
	return receipt
}

// CheckGate reads the MgrMap, every owned MON's sessions and the MgrMap again,
// taking another sample (at most three) while the two maps differ, so that
// the sessions belong to one map; then every owned MON's log for reclaim
// refusals and one native "tell" of the active MGR as a new native client.
// label is the consumer's name for the moment of the gate, such as the one
// before the SUT dials and the one after its failure.
func CheckGate(ctx context.Context, cluster *ceph.Container, label string, bounds GateBounds) (GateReceipt, error) {
	receipt := GateReceipt{Schema: GateSchema, Label: label, ObservedAt: time.Now().UTC(), Refusals: []Refusal{}, Queries: []Query{}}
	monitors, err := cluster.Monitors(ctx)
	if err != nil {
		return receipt, fmt.Errorf("list MONs: %w", err)
	}
	managers, err := cluster.Managers(ctx)
	if err != nil {
		return receipt, fmt.Errorf("list MGRs: %w", err)
	}
	owned := daemonNames(managers, func(m *ceph.ManagerContainer) string { return m.DaemonName })
	receipt.Map, receipt.Monitors, receipt.Queries = sampleOneMap(
		func() ManagerMap { return readManagerMap(ctx, cluster, bounds.Query) },
		func() []MonitorSessions { return readSessions(ctx, monitors, bounds.Query) })
	var queries []Query
	receipt.Refusals, queries = readRefusals(ctx, monitors, bounds)
	receipt.Queries = append(receipt.Queries, queries...)
	if receipt.Map.Query.Complete() && receipt.Map.Available && receipt.Map.Active.Name != "" {
		control, query, _, stopErr := tellVersion(ctx, cluster, "mgr."+receipt.Map.Active.Name, bounds.Control, 4<<10)
		receipt.Control = &control
		receipt.Queries = append(receipt.Queries, query)
		if stopErr != nil {
			receipt.Queries = append(receipt.Queries, Query{Name: "stop " + query.Name, Status: StatusFailed, Error: errorText(stopErr)})
		}
	}
	receipt = EvaluateGate(receipt, owned)
	if !receipt.Complete {
		return receipt, errors.New("native MGR keyring gate is incomplete")
	}
	return receipt, nil
}

// sampleOneMap reads the MgrMap, the sessions and the MgrMap again, taking
// another sample (at most three) while the two maps differ. It returns the
// second map, the sessions and the queries that make the bracket incomplete:
// a failed read on either side is kept, so a bracket that was never
// established cannot leave a gate complete.
func sampleOneMap(readMap func() ManagerMap, readSessions func() []MonitorSessions) (ManagerMap, []MonitorSessions, []Query) {
	queries := []Query{}
	for sample := 1; ; sample++ {
		before := readMap()
		sessions := readSessions()
		after := readMap()
		if !before.Query.Complete() {
			queries = append(queries, Query{Name: "MgrMap before the sessions", Status: before.Query.Status, Error: before.Query.Error})
		}
		if !before.Query.Complete() || !after.Query.Complete() || before.same(after) {
			return after, sessions, queries
		}
		if sample == 3 {
			queries = append(queries, Query{Name: "one MgrMap around the sessions", Status: StatusFailed,
				Error: fmt.Sprintf("the MgrMap moved from epoch %d to %d during each of %d samples", before.Epoch, after.Epoch, sample)})
			return after, sessions, queries
		}
	}
}

// tellVersion runs "ceph tell target version" in the control container as a
// new native client within bound, keeping at most output bytes of stdout, and
// returns its stdout and stderr. A command that could not be confirmed as
// stopped gets one more stop with an allowance of its own, outside ctx; the
// error of that stop is returned apart from the query.
func tellVersion(ctx context.Context, cluster *ceph.Container, target string, bound time.Duration, output int) (Control, Query, []byte, error) {
	control := Control{Target: target, Command: "tell version"}
	var printed []byte
	var stopErr error
	query := bounded(ctx, bound, "tell "+target+" version", func(ctx context.Context) error {
		runner, err := cluster.ControlContainer(ctx)
		if err != nil {
			return err
		}
		result, err := cluster.RunNativeCommand(ctx, runner, ceph.NativeCommand{
			Argv: []string{"ceph", "--connect-timeout", "5", "tell", target, "version"}, Timeout: bound,
			MaxStdout: output, MaxStderr: 16 << 10, Label: "tell " + target + " version",
		})
		var unstopped *ceph.NativeCommandUnstoppedError
		if errors.As(err, &unstopped) {
			stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
			stopErr = unstopped.Stop(stopCtx)
			cancel()
		}
		printed = append(slices.Clone(result.Stdout), result.Stderr...)
		control.ExitCode, control.Complete = result.ExitCode, err == nil && result.Complete()
		if err == nil && !result.Complete() {
			err = fmt.Errorf("%w: timed out %t, truncated %t", ceph.ErrNativeCommandIncomplete, result.TimedOut, result.StdoutTruncated || result.StderrTruncated)
		}
		return err
	})
	return control, query, printed, stopErr
}
