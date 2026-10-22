//go:build all || integration

package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
)

// monReplacementCluster is the part of *ceph.Container that a recorded MON
// replacement uses: the membership operations and read-only observations
// through public APIs. Membership is listed with the context getters, so a
// held topology mutex cannot outlive an observation's bound.
type monReplacementCluster interface {
	AddMonitor(context.Context, string) (*ceph.MonitorContainer, error)
	RemoveMonitor(context.Context, string) error
	QuorumStatus(context.Context) (ceph.QuorumStatus, error)
	AdminSocket(context.Context, string, ...string) ([]byte, error)
	CollectDiagnostics(context.Context, ceph.DiagnosticsConfig) (*ceph.DiagnosticsReport, error)
	MonitorsContext(context.Context) ([]*ceph.MonitorContainer, error)
	ManagersContext(context.Context) ([]*ceph.ManagerContainer, error)
	OSDsContext(context.Context) ([]*ceph.OSDContainer, error)
}

// monReplacementBounds bounds every step. Operation guards AddMonitor and
// RemoveMonitor, which apply the cluster's own startup timeout inside it;
// Quorum is the wait for the expected quorum afterwards, the 90 seconds that
// topologyWait allowed before. Query bounds one admin socket or quorum read,
// Snapshot one set of them, and Bundle the whole failure collection.
type monReplacementBounds struct {
	Operation, Quorum, QuorumInterval, Query, Snapshot, Bundle time.Duration
}

var monReplacementDefaultBounds = monReplacementBounds{
	Operation: 4 * time.Minute, Quorum: 90 * time.Second, QuorumInterval: 500 * time.Millisecond,
	Query: 10 * time.Second, Snapshot: 45 * time.Second, Bundle: 3 * time.Minute,
}

// monReplacementDebugLevel is the debug_monc level of the diagnostic window:
// MonClient hunting, authentication and session changes reach the warm
// daemons' container output while the MON membership changes.
const monReplacementDebugLevel = "10/10"

// monReplacementSettings are the effective settings that decide how a warm
// daemon's MON client reacts to a lost session. They are read once, at the
// baseline, from each warm MGR and OSD.
var monReplacementSettings = []string{"debug_monc", "auth_exit_on_failure", "auth_mon_ticket_ttl", "auth_service_ticket_ttl", "mon_client_hunt_interval"}

// monReplacementExecDaemon is a warm daemon whose admin socket the cluster's
// AdminSocket does not reach, such as an MDS: the socket is in the daemon's
// own container, and the recorder reaches it with that image's ceph CLI. An
// image without the CLI leaves the daemon's observations incomplete.
type monReplacementExecDaemon struct {
	Daemon    string
	Container testcontainers.Container
	Socket    string
}

// errMonReplacementNoCLI marks a daemon whose image cannot run the ceph CLI.
var errMonReplacementNoCLI = errors.New("the daemon's image has no ceph CLI for its admin socket")

// monReplacementExecSocket runs one admin socket command in the daemon's own
// container, supervised and bounded by ctx.
func monReplacementExecSocket(ctx context.Context, d monReplacementExecDaemon, args ...string) ([]byte, error) {
	limit := 30 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		limit = time.Until(deadline)
	}
	if limit <= 0 {
		return nil, fmt.Errorf("admin socket of %s: %w", d.Daemon, context.DeadlineExceeded)
	}
	result, err := supervised.Command(ctx, d.Container, "admin socket of "+d.Daemon, limit, 4<<20, 64<<10, append([]string{"ceph", "--admin-daemon", d.Socket}, args...)...)
	switch {
	case err != nil:
		return nil, fmt.Errorf("admin socket of %s: %w", d.Daemon, err)
	case result.ExitCode == 126 || result.ExitCode == 127:
		return nil, fmt.Errorf("admin socket of %s: %w (exit %d)", d.Daemon, errMonReplacementNoCLI, result.ExitCode)
	case result.ExitCode != 0:
		stderr := bytes.TrimSpace(result.Stderr)
		return result.Stdout, fmt.Errorf("admin socket of %s: exit %d: %s", d.Daemon, result.ExitCode, stderr[:min(len(stderr), 512)])
	}
	return result.Stdout, nil
}

// monLocalStatus is what one MON reports about itself on its admin socket.
type monLocalStatus struct {
	Name          string   `json:"name"`
	State         string   `json:"state,omitempty"`
	Rank          int      `json:"rank"`
	ElectionEpoch int      `json:"election_epoch,omitempty"`
	MonMapEpoch   int      `json:"monmap_epoch,omitempty"`
	Members       []string `json:"members,omitempty"`
	Quorum        []string `json:"quorum,omitempty"`
	OutsideQuorum []string `json:"outside_quorum,omitempty"`
	Error         string   `json:"error,omitempty"`
}

// monReplacementSession is one MON session of a warm daemon, as that MON lists it.
type monReplacementSession struct {
	Mon           string `json:"mon"`
	Name          string `json:"name"`
	GlobalID      uint64 `json:"global_id"`
	ConType       string `json:"con_type,omitempty"`
	Open          bool   `json:"open"`
	Authenticated bool   `json:"authenticated"`
}

// warmDaemonStatus identifies a daemon that stays up through the
// replacement: its container generation and what it reports on its own
// admin socket.
type warmDaemonStatus struct {
	Daemon      string            `json:"daemon"`
	ContainerID string            `json:"container_id,omitempty"`
	StartedAt   string            `json:"started_at,omitempty"`
	Running     bool              `json:"running"`
	Native      map[string]any    `json:"native,omitempty"`
	Config      map[string]string `json:"config,omitempty"`
	Errors      []string          `json:"errors,omitempty"`
}

type monReplacementSnapshot struct {
	At          time.Time               `json:"at"`
	Quorum      []string                `json:"quorum,omitempty"`
	MonMap      []string                `json:"monmap,omitempty"`
	QuorumError string                  `json:"quorum_error,omitempty"`
	Monitors    []monLocalStatus        `json:"monitors"`
	Sessions    []monReplacementSession `json:"sessions"`
	Warm        []warmDaemonStatus      `json:"warm"`
	Incomplete  []string                `json:"incomplete,omitempty"`
}

type monReplacementPhase struct {
	Seq            int                     `json:"seq"`
	Name           string                  `json:"name"`
	Started        time.Time               `json:"started"`
	OperationEnded time.Time               `json:"operation_ended"`
	Ended          time.Time               `json:"ended"`
	QuorumProbes   int                     `json:"quorum_probes,omitempty"`
	Handle         string                  `json:"handle,omitempty"`
	Error          string                  `json:"error,omitempty"`
	After          *monReplacementSnapshot `json:"after,omitempty"`
}

// monReplacementDebug is one daemon's debug_monc window. Restored records
// the outcome: the exact prior value, or why it was not written back.
type monReplacementDebug struct {
	Daemon    string `json:"daemon"`
	StartedAt string `json:"started_at,omitempty"`
	Prior     string `json:"prior,omitempty"`
	Set       bool   `json:"set"`
	Restored  string `json:"restored,omitempty"`
	Error     string `json:"error,omitempty"`
}

// monReplacementTrace is the tail of one container's log lines about its MON
// client: hunting, authentication, beacons and mounts. Matched counts every
// matching line; Lines keeps the newest of them.
type monReplacementTrace struct {
	Daemon  string   `json:"daemon"`
	Matched int      `json:"matched"`
	Lines   []string `json:"lines,omitempty"`
	Error   string   `json:"error,omitempty"`
}

var monReplacementTracePattern = regexp.MustCompile(`(?i)monclient|beacon|laggy|hunt|auth|mount|session`)

const (
	monReplacementTraceLines    = 300
	monReplacementTraceLineSize = 512
)

// monReplacementBundle is what a failure leaves behind: a failed phase, or a
// later step of the test after a replacement that succeeded. It is collected
// after the failed operation returned and before the fixture is terminated.
type monReplacementBundle struct {
	Token            string                  `json:"token"`
	Cluster          string                  `json:"cluster"`
	Failed           monReplacementPhase     `json:"failed"`
	Phases           []monReplacementPhase   `json:"phases"`
	Baseline         *monReplacementSnapshot `json:"baseline,omitempty"`
	Failure          *monReplacementSnapshot `json:"failure,omitempty"`
	Debug            []monReplacementDebug   `json:"debug"`
	Traces           []monReplacementTrace   `json:"traces"`
	Diagnostics      *ceph.DiagnosticsReport `json:"diagnostics,omitempty"`
	DiagnosticsError string                  `json:"diagnostics_error,omitempty"`
}

// monReplacementRecorder runs a full MON replacement as named phases and
// observes, without changing membership itself, what each MON and each warm
// daemon reports between them. Observation failures are recorded as
// incomplete; they never fail or retry the replacement.
type monReplacementRecorder struct {
	Token, Cluster string
	cluster        monReplacementCluster
	bounds         monReplacementBounds
	additional     []ceph.DiagnosticsContainer
	exec           []monReplacementExecDaemon
	execSocket     func(context.Context, monReplacementExecDaemon, ...string) ([]byte, error)
	bundled        bool
	Phases         []monReplacementPhase
	Baseline       *monReplacementSnapshot
	debugMu        sync.Mutex
	Debug          []monReplacementDebug
	helpMu         sync.Mutex
	help           map[string]map[string]bool
}

func newMonReplacementRecorder(label string, cluster monReplacementCluster, bounds monReplacementBounds, additional ...ceph.DiagnosticsContainer) *monReplacementRecorder {
	return &monReplacementRecorder{Token: uuid.NewString()[:8], Cluster: label, cluster: cluster, bounds: bounds, additional: additional,
		execSocket: monReplacementExecSocket, help: make(map[string]map[string]bool)}
}

// withDaemons adds warm daemons that are reached in their own containers.
// Call it before the replacement starts.
func (r *monReplacementRecorder) withDaemons(daemons ...monReplacementExecDaemon) *monReplacementRecorder {
	r.exec = append(r.exec, daemons...)
	return r
}

func (r *monReplacementRecorder) adminSocket(ctx context.Context, daemon string, args ...string) ([]byte, error) {
	for _, d := range r.exec {
		if d.Daemon == daemon {
			return r.execSocket(ctx, d, args...)
		}
	}
	return r.cluster.AdminSocket(ctx, daemon, args...)
}

// monReplacementReporter is the part of *testing.T that reports a run.
type monReplacementReporter interface {
	Helper()
	Logf(format string, args ...any)
	Fatalf(format string, args ...any)
}

// runMonReplacement replaces each pair's old MON with its new one, first
// adding the new MON, then removing the old one, and waits for the expected
// quorum after each. A failed phase is not repeated: its bundle is logged and
// then the test fails with the phase's own error. After a successful
// replacement the debug_monc windows stay open until finishMonReplacement,
// so that the warm daemons' MON clients stay traced through the rest of the
// test.
func runMonReplacement(t monReplacementReporter, ctx context.Context, r *monReplacementRecorder, pairs [][2]string) {
	t.Helper()
	r.begin(ctx)
	if err := r.replace(ctx, pairs); err != nil {
		bundle := r.collect(ctx, r.Phases[len(r.Phases)-1])
		t.Logf("MON_REPLACEMENT_BUNDLE %s", monReplacementJSON(bundle))
		t.Fatalf("MON replacement token=%s cluster=%s phase %d %q: %v", r.Token, r.Cluster, bundle.Failed.Seq, bundle.Failed.Name, err)
		return
	}
	t.Logf("%s", r.receipt())
}

// monReplacementFinisher is the part of *testing.T that a cleanup reads.
type monReplacementFinisher interface {
	Logf(format string, args ...any)
	Failed() bool
}

// finishMonReplacement runs as a cleanup, before the fixture is terminated.
// When the test failed after the replacement succeeded, it logs a bundle for
// the phase "after replacement"; either way it closes the debug_monc windows
// that are still open.
func finishMonReplacement(t monReplacementFinisher, r *monReplacementRecorder) {
	if r.Baseline == nil {
		return
	}
	if t.Failed() && !r.bundled {
		after := monReplacementPhase{Seq: len(r.Phases) + 1, Name: "after replacement", Ended: time.Now(),
			Error: "a later step of the test failed; see the test's own failure"}
		if len(r.Phases) > 0 {
			after.Started = r.Phases[len(r.Phases)-1].Ended
		}
		t.Logf("MON_REPLACEMENT_BUNDLE %s", monReplacementJSON(r.collect(context.Background(), after)))
		return
	}
	debug := r.restoreDebug(context.Background())
	restored, set := 0, 0
	var issues []string
	for _, window := range debug {
		if !window.Set {
			continue
		}
		set++
		if window.Restored == "ok" {
			restored++
		} else {
			issues = append(issues, window.Daemon+":"+window.Restored)
		}
	}
	t.Logf("MON_REPLACEMENT_DEBUG token=%s cluster=%s debug_monc_restored=%d/%d%s", r.Token, r.Cluster, restored, set, monReplacementIssues(issues))
}

func monReplacementJSON(bundle *monReplacementBundle) []byte {
	raw, err := json.Marshal(bundle)
	if err != nil {
		raw = []byte(strconv.Quote(err.Error()))
	}
	return raw
}

// begin records the baseline and opens the debug_monc window on every warm
// MGR, OSD and MDS that offers config get/set on its admin socket.
func (r *monReplacementRecorder) begin(ctx context.Context) {
	r.Baseline = r.snapshot(ctx, true)
	r.debugMu.Lock()
	defer r.debugMu.Unlock()
	for _, warm := range r.Baseline.Warm {
		window := monReplacementDebug{Daemon: warm.Daemon, StartedAt: warm.StartedAt}
		if err := r.require(ctx, warm.Daemon, "config set"); err != nil {
			window.Error = err.Error()
			r.Debug = append(r.Debug, window)
			continue
		}
		prior, err := r.configGet(ctx, warm.Daemon, "debug_monc")
		if err != nil {
			window.Error = "read: " + err.Error()
			r.Debug = append(r.Debug, window)
			continue
		}
		window.Prior = prior
		queryCtx, cancel := context.WithTimeout(ctx, r.bounds.Query)
		_, err = r.adminSocket(queryCtx, warm.Daemon, "config", "set", "debug_monc", monReplacementDebugLevel)
		cancel()
		// A failed or unconfirmed set may still have applied, so the window
		// is restored either way.
		window.Set = true
		if err != nil {
			window.Error = "set: " + err.Error()
		}
		r.Debug = append(r.Debug, window)
	}
}

func (r *monReplacementRecorder) replace(ctx context.Context, pairs [][2]string) error {
	members := slices.Clone(r.Baseline.MonMap)
	if len(members) == 0 {
		listCtx, cancel := context.WithTimeout(ctx, r.bounds.Query)
		monitors, err := r.cluster.MonitorsContext(listCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("list monitors before the replacement: %w", err)
		}
		for _, mon := range monitors {
			members = append(members, mon.DaemonName)
		}
	}
	for _, pair := range pairs {
		old, added := pair[0], pair[1]
		members = append(members, added)
		if err := r.phase(ctx, "add "+added, members, func(opCtx context.Context) (string, error) {
			mon, err := r.cluster.AddMonitor(opCtx, added)
			return monReplacementHandle(mon), err
		}); err != nil {
			return err
		}
		members = slices.DeleteFunc(members, func(name string) bool { return name == old })
		if err := r.phase(ctx, "remove "+old, members, func(opCtx context.Context) (string, error) {
			return "", r.cluster.RemoveMonitor(opCtx, old)
		}); err != nil {
			return err
		}
	}
	return nil
}

func monReplacementHandle(mon *ceph.MonitorContainer) string {
	if mon == nil {
		return ""
	}
	handle := "mon." + mon.DaemonName
	if mon.Container != nil {
		if id := mon.GetContainerID(); id != "" {
			handle += " container=" + id[:min(12, len(id))]
		}
	}
	return handle
}

// phase runs one membership operation under its bound, then waits for the
// quorum to be exactly want. The snapshot after it is taken whether or not it
// succeeded; a failure keeps the operation's partial handle.
func (r *monReplacementRecorder) phase(ctx context.Context, name string, want []string, operation func(context.Context) (string, error)) error {
	p := monReplacementPhase{Seq: len(r.Phases) + 1, Name: name, Started: time.Now()}
	opCtx, cancel := context.WithTimeout(ctx, r.bounds.Operation)
	handle, err := operation(opCtx)
	cancel()
	p.OperationEnded, p.Handle = time.Now(), handle
	if err == nil {
		var seen observation[ceph.QuorumStatus]
		seen, err = observe(ctx, r.bounds.Quorum, r.bounds.Query, r.bounds.QuorumInterval, func(probeCtx context.Context) (ceph.QuorumStatus, bool, error) {
			status, err := r.cluster.QuorumStatus(probeCtx)
			if err != nil {
				// A read that fails while membership changes is not final.
				return status, false, nil
			}
			// So is owned membership that the probe's bound cannot list.
			monitors, err := r.cluster.MonitorsContext(probeCtx)
			if err != nil {
				return status, false, nil
			}
			return status, monReplacementQuorumIs(status, want) && len(monitors) == len(want), nil
		})
		p.QuorumProbes = seen.Attempts
		if err != nil {
			err = fmt.Errorf("quorum %v after %s: %w (last quorum %v)", want, name, err, seen.Value.QuorumNames)
		}
	}
	p.Ended = time.Now()
	if err != nil {
		p.Error = err.Error()
		r.Phases = append(r.Phases, p)
		return err
	}
	p.After = r.snapshot(ctx, false)
	r.Phases = append(r.Phases, p)
	return nil
}

func monReplacementQuorumIs(status ceph.QuorumStatus, want []string) bool {
	quorum, members := slices.Clone(status.QuorumNames), make([]string, 0, len(status.MonMap.Mons))
	for _, mon := range status.MonMap.Mons {
		members = append(members, mon.Name)
	}
	want = slices.Clone(want)
	slices.Sort(quorum)
	slices.Sort(members)
	slices.Sort(want)
	return slices.Equal(quorum, want) && slices.Equal(members, want)
}

// collect builds the bundle of the first failure under a fresh bound: the
// operation's context may already have ended, and the failed operation has
// returned, so the topology lock is free again. It is collected once.
func (r *monReplacementRecorder) collect(parent context.Context, failed monReplacementPhase) *monReplacementBundle {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), r.bounds.Bundle)
	defer cancel()
	r.bundled = true
	bundle := &monReplacementBundle{Token: r.Token, Cluster: r.Cluster, Failed: failed, Phases: r.Phases, Baseline: r.Baseline}
	bundle.Failure = r.snapshot(ctx, true)
	bundle.Traces = r.traces(ctx)
	report, err := r.cluster.CollectDiagnostics(ctx, ceph.DiagnosticsConfig{
		Timeout: r.bounds.Bundle / 2, OperationTimeout: r.bounds.Query, MaxOutputBytes: 32 << 10, LogTail: 400,
		AdditionalContainers: r.additional,
	})
	bundle.Diagnostics = report
	if err != nil {
		bundle.DiagnosticsError = err.Error()
	}
	bundle.Debug = r.restoreDebug(ctx)
	return bundle
}

// restoreDebug writes each window's exact prior debug_monc back, once, and
// only to the same container generation that it was read from.
func (r *monReplacementRecorder) restoreDebug(parent context.Context) []monReplacementDebug {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), r.bounds.Snapshot)
	defer cancel()
	r.debugMu.Lock()
	defer r.debugMu.Unlock()
	warm := make(map[string]warmDaemonStatus)
	identities, unlisted := r.warmIdentities(ctx)
	for _, status := range identities {
		warm[status.Daemon] = status
	}
	for i := range r.Debug {
		window := &r.Debug[i]
		if !window.Set || window.Restored != "" {
			continue
		}
		current, ok := warm[window.Daemon]
		switch {
		case !ok && len(unlisted) > 0:
			// The daemon may still run with the diagnostic level.
			window.Restored = "skipped: identity unavailable: " + strings.Join(unlisted, "; ")
			continue
		case !ok || !current.Running:
			window.Restored = "skipped: daemon is not running"
			continue
		case current.StartedAt != window.StartedAt:
			window.Restored = "skipped: container generation changed"
			continue
		}
		queryCtx, cancelQuery := context.WithTimeout(ctx, r.bounds.Query)
		_, err := r.adminSocket(queryCtx, window.Daemon, "config", "set", "debug_monc", window.Prior)
		cancelQuery()
		if err != nil {
			window.Restored = "failed: " + err.Error()
			continue
		}
		value, err := r.configGet(ctx, window.Daemon, "debug_monc")
		switch {
		case err != nil:
			window.Restored = "unconfirmed: " + err.Error()
		case value != window.Prior:
			window.Restored = fmt.Sprintf("mismatch: %q", value)
		default:
			window.Restored = "ok"
		}
	}
	return slices.Clone(r.Debug)
}

// snapshot reads, within the snapshot bound, the control quorum, each MON's
// own mon_status and sessions, and each warm daemon's identity. full adds the
// warm daemons' effective MON client settings.
func (r *monReplacementRecorder) snapshot(parent context.Context, full bool) *monReplacementSnapshot {
	ctx, cancel := context.WithTimeout(parent, r.bounds.Snapshot)
	defer cancel()
	s := &monReplacementSnapshot{At: time.Now(), Monitors: []monLocalStatus{}, Sessions: []monReplacementSession{}, Warm: []warmDaemonStatus{}}
	var mu sync.Mutex
	incomplete := func(format string, args ...any) {
		mu.Lock()
		s.Incomplete = append(s.Incomplete, fmt.Sprintf(format, args...))
		mu.Unlock()
	}
	queryCtx, cancelQuery := context.WithTimeout(ctx, r.bounds.Query)
	status, err := r.cluster.QuorumStatus(queryCtx)
	cancelQuery()
	if err != nil {
		s.QuorumError = err.Error()
		incomplete("quorum: %v", err)
	} else {
		s.Quorum = slices.Clone(status.QuorumNames)
		for _, mon := range status.MonMap.Mons {
			s.MonMap = append(s.MonMap, mon.Name)
		}
	}
	var wg sync.WaitGroup
	limit := make(chan struct{}, 4)
	run := func(work func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			limit <- struct{}{}
			defer func() { <-limit }()
			work()
		}()
	}
	listCtx, cancelList := context.WithTimeout(ctx, r.bounds.Query)
	monitors, err := r.cluster.MonitorsContext(listCtx)
	cancelList()
	if err != nil {
		incomplete("list monitors: %v", err)
	}
	s.Monitors = make([]monLocalStatus, len(monitors))
	sessions := make([][]monReplacementSession, len(monitors))
	for i, mon := range monitors {
		daemon := "mon." + mon.DaemonName
		run(func() {
			local := monLocalStatus{Name: mon.DaemonName, Rank: -1}
			if raw, err := r.query(ctx, daemon, "mon_status"); err != nil {
				local.Error = err.Error()
				incomplete("%s mon_status: %v", daemon, err)
			} else if err := parseMonLocalStatus(raw, &local); err != nil {
				local.Error = err.Error()
				incomplete("%s mon_status: %v", daemon, err)
			}
			s.Monitors[i] = local
			if raw, err := r.query(ctx, daemon, "sessions"); err != nil {
				incomplete("%s sessions: %v", daemon, err)
			} else if sessions[i], err = parseWarmSessions(mon.DaemonName, raw); err != nil {
				incomplete("%s sessions: %v", daemon, err)
			}
		})
	}
	warm, unlisted := r.warmIdentities(ctx)
	for _, problem := range unlisted {
		incomplete("%s", problem)
	}
	for i := range warm {
		w := &warm[i]
		run(func() {
			if !w.Running {
				w.Errors = append(w.Errors, "container is not running")
				incomplete("%s: container is not running", w.Daemon)
				return
			}
			command := "status" // OSD and MDS
			if strings.HasPrefix(w.Daemon, "mgr.") {
				command = "mgr_status"
			}
			if raw, err := r.query(ctx, w.Daemon, command); err != nil {
				w.Errors = append(w.Errors, command+": "+err.Error())
				incomplete("%s %s: %v", w.Daemon, command, err)
			} else {
				w.Native = monReplacementNative(raw)
			}
			settings := []string{"mon_host"}
			if full {
				settings = append(settings, monReplacementSettings...)
			}
			w.Config = make(map[string]string)
			for _, name := range settings {
				value, err := r.configGet(ctx, w.Daemon, name)
				if err != nil {
					w.Errors = append(w.Errors, name+": "+err.Error())
					incomplete("%s config get %s: %v", w.Daemon, name, err)
					continue
				}
				w.Config[name] = value
			}
		})
	}
	wg.Wait()
	for _, list := range sessions {
		s.Sessions = append(s.Sessions, list...)
	}
	s.Warm = warm
	slices.Sort(s.Incomplete)
	return s
}

// warmIdentities lists the owned MGRs and OSDs and the daemons reached in
// their own containers, such as MDS daemons, with their container generation.
// unlisted says which owned kinds could not be listed within the query bound.
func (r *monReplacementRecorder) warmIdentities(ctx context.Context) (result []warmDaemonStatus, unlisted []string) {
	identify := func(daemon string, ctr testcontainers.Container) {
		status := warmDaemonStatus{Daemon: daemon}
		if ctr == nil {
			status.Errors = append(status.Errors, "no container")
			result = append(result, status)
			return
		}
		id := ctr.GetContainerID()
		status.ContainerID = id[:min(12, len(id))]
		stateCtx, cancel := context.WithTimeout(ctx, r.bounds.Query)
		state, err := ctr.State(stateCtx)
		cancel()
		if err != nil || state == nil {
			status.Errors = append(status.Errors, fmt.Sprintf("state: %v", err))
		} else {
			status.StartedAt, status.Running = state.StartedAt, state.Running
		}
		result = append(result, status)
	}
	managers, osds, unlisted := r.ownedDaemons(ctx)
	for _, mgr := range managers {
		identify("mgr."+mgr.DaemonName, mgr.Container)
	}
	for _, osd := range osds {
		identify("osd."+strconv.Itoa(osd.ID), osd.Container)
	}
	for _, d := range r.exec {
		identify(d.Daemon, d.Container)
	}
	return result, unlisted
}

// ownedDaemons lists the owned MGRs and OSDs, each within the query bound.
// A kind that cannot be listed is reported instead of waited for.
func (r *monReplacementRecorder) ownedDaemons(ctx context.Context) ([]*ceph.ManagerContainer, []*ceph.OSDContainer, []string) {
	var unlisted []string
	listCtx, cancel := context.WithTimeout(ctx, r.bounds.Query)
	managers, err := r.cluster.ManagersContext(listCtx)
	cancel()
	if err != nil {
		unlisted = append(unlisted, "list managers: "+err.Error())
	}
	listCtx, cancel = context.WithTimeout(ctx, r.bounds.Query)
	osds, err := r.cluster.OSDsContext(listCtx)
	cancel()
	if err != nil {
		unlisted = append(unlisted, "list osds: "+err.Error())
	}
	return managers, osds, unlisted
}

// traces reads the MON client lines of every warm daemon and every additional
// container, such as the clients and mirror daemons that were stopped during
// the replacement and started after it.
func (r *monReplacementRecorder) traces(ctx context.Context) []monReplacementTrace {
	type target struct {
		name string
		ctr  testcontainers.Container
	}
	var targets []target
	managers, osds, unlisted := r.ownedDaemons(ctx)
	for _, mgr := range managers {
		targets = append(targets, target{"mgr." + mgr.DaemonName, mgr.Container})
	}
	for _, osd := range osds {
		targets = append(targets, target{"osd." + strconv.Itoa(osd.ID), osd.Container})
	}
	for _, d := range r.exec {
		targets = append(targets, target{d.Daemon, d.Container})
	}
	for _, extra := range r.additional {
		targets = append(targets, target{extra.Name, extra.Container})
	}
	result := make([]monReplacementTrace, 0, len(targets)+len(unlisted))
	for _, problem := range unlisted {
		// The owned daemons of a kind that could not be listed have no trace.
		result = append(result, monReplacementTrace{Daemon: "owned", Error: problem})
	}
	for _, target := range targets {
		trace := monReplacementTrace{Daemon: target.name}
		if target.ctr == nil {
			trace.Error = "no container"
		} else if err := readMonReplacementTrace(ctx, target.ctr, r.bounds.Query, &trace); err != nil {
			trace.Error = err.Error()
		}
		result = append(result, trace)
	}
	return result
}

func readMonReplacementTrace(parent context.Context, ctr testcontainers.Container, bound time.Duration, trace *monReplacementTrace) error {
	ctx, cancel := context.WithTimeout(parent, bound)
	defer cancel()
	logs, err := ctr.Logs(ctx)
	if err != nil {
		return fmt.Errorf("logs: %w", err)
	}
	defer func() { _ = logs.Close() }()
	scanner := bufio.NewScanner(logs)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if !monReplacementTracePattern.MatchString(line) {
			continue
		}
		trace.Matched++
		if len(trace.Lines) == monReplacementTraceLines {
			trace.Lines = trace.Lines[1:]
		}
		trace.Lines = append(trace.Lines, line[:min(len(line), monReplacementTraceLineSize)])
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("logs: %w", err)
	}
	return nil
}

// offers reports whether daemon lists command in its own help, read once per
// daemon name. A help that cannot be read is an error, retried on the next
// query rather than cached.
func (r *monReplacementRecorder) offers(ctx context.Context, daemon, command string) (bool, error) {
	r.helpMu.Lock()
	commands, ok := r.help[daemon]
	r.helpMu.Unlock()
	if !ok {
		queryCtx, cancel := context.WithTimeout(ctx, r.bounds.Query)
		raw, err := r.adminSocket(queryCtx, daemon, "help")
		cancel()
		if err != nil {
			return false, fmt.Errorf("help: %w", err)
		}
		var help map[string]string
		if err := json.Unmarshal(raw, &help); err != nil {
			return false, fmt.Errorf("help: %w", err)
		}
		commands = make(map[string]bool, len(help))
		for name := range help {
			commands[name] = true
		}
		r.helpMu.Lock()
		r.help[daemon] = commands
		r.helpMu.Unlock()
	}
	return commands[command], nil
}

func (r *monReplacementRecorder) require(ctx context.Context, daemon, command string) error {
	ok, err := r.offers(ctx, daemon, command)
	if err == nil && !ok {
		err = fmt.Errorf("%q is not in the daemon's help", command)
	}
	return err
}

func (r *monReplacementRecorder) query(ctx context.Context, daemon string, args ...string) ([]byte, error) {
	if err := r.require(ctx, daemon, strings.Join(args, " ")); err != nil {
		return nil, err
	}
	queryCtx, cancel := context.WithTimeout(ctx, r.bounds.Query)
	defer cancel()
	return r.adminSocket(queryCtx, daemon, args...)
}

func (r *monReplacementRecorder) configGet(ctx context.Context, daemon, name string) (string, error) {
	if err := r.require(ctx, daemon, "config get"); err != nil {
		return "", err
	}
	queryCtx, cancel := context.WithTimeout(ctx, r.bounds.Query)
	defer cancel()
	raw, err := r.adminSocket(queryCtx, daemon, "config", "get", name)
	if err != nil {
		return "", err
	}
	var value map[string]string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("config get %s: %w", name, err)
	}
	result, ok := value[name]
	if !ok {
		return "", fmt.Errorf("config get %s: no value", name)
	}
	return result, nil
}

func parseMonLocalStatus(raw []byte, local *monLocalStatus) error {
	var status struct {
		State         string   `json:"state"`
		Rank          int      `json:"rank"`
		ElectionEpoch int      `json:"election_epoch"`
		Quorum        []int    `json:"quorum"`
		OutsideQuorum []string `json:"outside_quorum"`
		MonMap        struct {
			Epoch int `json:"epoch"`
			Mons  []struct {
				Rank int    `json:"rank"`
				Name string `json:"name"`
			} `json:"mons"`
		} `json:"monmap"`
	}
	if err := json.Unmarshal(raw, &status); err != nil {
		return fmt.Errorf("decode mon_status: %w", err)
	}
	local.State, local.Rank, local.ElectionEpoch, local.MonMapEpoch = status.State, status.Rank, status.ElectionEpoch, status.MonMap.Epoch
	local.OutsideQuorum = status.OutsideQuorum
	names := make(map[int]string)
	for _, mon := range status.MonMap.Mons {
		names[mon.Rank] = mon.Name
		local.Members = append(local.Members, mon.Name)
	}
	for _, rank := range status.Quorum {
		name, ok := names[rank]
		if !ok {
			name = "rank" + strconv.Itoa(rank)
		}
		local.Quorum = append(local.Quorum, name)
	}
	return nil
}

// parseWarmSessions keeps the sessions of MGR, OSD and MDS entities, the
// daemons that stay up. MON peers and clients are dropped, and so are caps
// and addresses.
func parseWarmSessions(mon string, raw []byte) ([]monReplacementSession, error) {
	var list []struct {
		Name          string `json:"name"`
		EntityName    string `json:"entity_name"`
		GlobalID      uint64 `json:"global_id"`
		ConType       string `json:"con_type"`
		Open          bool   `json:"open"`
		Authenticated bool   `json:"authenticated"`
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, fmt.Errorf("decode sessions: %w", err)
	}
	var result []monReplacementSession
	for _, session := range list {
		name := session.EntityName
		if name == "" {
			name = session.Name
		}
		if !strings.HasPrefix(name, "mgr.") && !strings.HasPrefix(name, "osd.") && !strings.HasPrefix(name, "mds.") {
			continue
		}
		result = append(result, monReplacementSession{Mon: mon, Name: name, GlobalID: session.GlobalID, ConType: session.ConType, Open: session.Open, Authenticated: session.Authenticated})
	}
	return result, nil
}

// monReplacementNative keeps the identity and map fields of an OSD status or
// MGR mgr_status.
func monReplacementNative(raw []byte) map[string]any {
	var value map[string]any
	if json.Unmarshal(raw, &value) != nil {
		return map[string]any{"raw_bytes": len(raw)}
	}
	result := make(map[string]any)
	for _, key := range []string{"cluster_fsid", "whoami", "id", "state", "want_state", "newest_map", "mdsmap_epoch", "osdmap_epoch", "mgrmap_epoch", "initialized"} {
		if v, ok := value[key]; ok {
			result[key] = v
		}
	}
	return result
}

// monSessionPath lists, for each warm entity, the MONs that held its daemon
// session (con_type mgr/osd/mds) across the baseline and every phase, as
// "a>d". A "-" marks a snapshot in which no MON listed one.
func (r *monReplacementRecorder) monSessionPath() string {
	snapshots := []*monReplacementSnapshot{r.Baseline}
	for _, p := range r.Phases {
		snapshots = append(snapshots, p.After)
	}
	entities := make(map[string][]string)
	var names []string
	for _, s := range snapshots {
		if s == nil {
			continue
		}
		for _, session := range s.Sessions {
			if _, ok := entities[session.Name]; !ok {
				entities[session.Name] = nil
				names = append(names, session.Name)
			}
		}
	}
	slices.Sort(names)
	var parts []string
	for _, name := range names {
		var path []string
		for _, s := range snapshots {
			if s == nil {
				continue
			}
			var mons []string
			for _, session := range s.Sessions {
				kind, _, _ := strings.Cut(name, ".")
				if session.Name == name && session.ConType == kind && !slices.Contains(mons, session.Mon) {
					mons = append(mons, session.Mon)
				}
			}
			step := "-"
			if len(mons) > 0 {
				step = strings.Join(mons, "+")
			}
			if len(path) == 0 || path[len(path)-1] != step {
				path = append(path, step)
			}
		}
		parts = append(parts, name+":"+strings.Join(path, ">"))
	}
	return strings.Join(parts, ",")
}

// monMapEpoch is the highest monmap epoch any MON reported in s.
func (s *monReplacementSnapshot) monMapEpoch() int {
	epoch := 0
	for _, mon := range s.Monitors {
		epoch = max(epoch, mon.MonMapEpoch)
	}
	return epoch
}

func (r *monReplacementRecorder) receipt() string {
	var phases []string
	incomplete := len(r.Baseline.Incomplete)
	var total time.Duration
	for _, p := range r.Phases {
		total += p.Ended.Sub(p.Started)
		epoch := 0
		if p.After != nil {
			epoch = p.After.monMapEpoch()
			incomplete += len(p.After.Incomplete)
		}
		phases = append(phases, fmt.Sprintf("%s=%s/%s@e%d", strings.ReplaceAll(p.Name, " ", "-"),
			p.OperationEnded.Sub(p.Started).Round(100*time.Millisecond), p.Ended.Sub(p.Started).Round(100*time.Millisecond), epoch))
	}
	r.debugMu.Lock()
	open := 0
	var debugIssues []string
	for _, window := range r.Debug {
		if window.Set && window.Error == "" {
			open++
		} else {
			debugIssues = append(debugIssues, window.Daemon+":"+window.Error)
		}
	}
	windows := len(r.Debug)
	r.debugMu.Unlock()
	return fmt.Sprintf("MON_REPLACEMENT_RECEIPT token=%s cluster=%s phases=%d total=%s baseline_epoch=%d %s sessions=%s debug_monc_open=%d/%d%s incomplete=%d",
		r.Token, r.Cluster, len(r.Phases), total.Round(100*time.Millisecond), r.Baseline.monMapEpoch(), strings.Join(phases, " "),
		r.monSessionPath(), open, windows, monReplacementIssues(debugIssues), incomplete)
}

func monReplacementIssues(issues []string) string {
	if len(issues) == 0 {
		return ""
	}
	return " debug_issues=" + strings.Join(issues, ";")
}
