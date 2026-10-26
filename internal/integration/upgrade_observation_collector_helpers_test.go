//go:build all || integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

// The types below model, without Ceph or Docker, the private read-only
// collector of a mixed-version upgrade harness. It connects three merged
// pieces that were only exercised separately: the phase and executor
// generation ownership (upgradeTargets, upgradePhase), the strict parsers for
// the eight command outputs and their settled-evidence adapter
// (upgradeSettledFromOutputs), and the sequence plan that consumes the
// evidence (upgradeSequence.settle).
//
// What it guarantees, over an injected command runner:
//
//   - Read-only: only eight exact argument vectors can reach the runner. Each
//     one is a "r" permission command in the pinned Ceph source (Tentacle
//     20.2.4, commit 7f793731f1b39eb4f465e960113d2363c311b964): versions,
//     mon dump, quorum_status, osd stat, mgr stat and mgr dump
//     (src/mon/MonCommands.h), pg stat and pg dump_stuck
//     (src/mgr/MgrCommands.h). Anything else is refused before the runner is
//     called, including a command table that was extended by mistake.
//   - Binding: every run is stamped by the runner with the executor, the
//     executor generation and the phase it ran for. A stamp that differs from
//     what the collector asked for, or a target that changed generation while
//     the commands ran, makes the whole collection unusable.
//   - Freshness: each command must start no earlier than the collection and
//     finish no later than its end and before the phase deadline, and the
//     whole collection must fit the allowed span. The eight outputs are
//     separate commands, so the span bounds how far apart they were sampled.
//   - All or nothing: any failed, cancelled, late, stale, mis-bound or
//     unparsable command yields only ObservationErr. Nothing is published
//     from a partial set, and the phase keeps no inventory from it.
//   - Attribution: phase expiry or cancellation of the caller is reported
//     separately from an error of one probe.
//
// "pg dump_stuck" needs explicit stuck types. The handler defaults to
// "unclean" only (src/mgr/DaemonServer.cc), and PGMap::get_stuck_stats
// asserts a non-zero type mask, so the collector names inactive, unclean,
// stale, undersized and degraded itself.
//
// It does not run Ceph, does not prove that a runner really executed the
// command inside the stamped executor, and is not native upgrade evidence.

var (
	errUpgradeCollectDeadline = errors.New("upgrade observation phase deadline")
	errUpgradeCollectProbe    = errors.New("upgrade observation probe failed")
	errUpgradeCollectStale    = errors.New("upgrade observation is not fresh")
	errUpgradeCollectBinding  = errors.New("upgrade observation is bound to another phase or executor")
	errUpgradeCollectDenied   = errors.New("upgrade observation command is not allowed")
)

// upgradeCollectCommand is one allowed read-only command and the output slot
// it fills.
type upgradeCollectCommand struct {
	Name string
	Argv []string
}

// upgradeCollectAllowlist is the complete set of commands the collector may
// run, in collection order.
func upgradeCollectAllowlist() []upgradeCollectCommand {
	format := []string{"--format", "json"}
	build := func(name string, words ...string) upgradeCollectCommand {
		argv := append([]string{"ceph"}, words...)
		return upgradeCollectCommand{Name: name, Argv: append(argv, format...)}
	}
	return []upgradeCollectCommand{
		build("versions", "versions"),
		build("mon dump", "mon", "dump"),
		build("quorum_status", "quorum_status"),
		build("osd stat", "osd", "stat"),
		build("mgr stat", "mgr", "stat"),
		build("mgr dump", "mgr", "dump"),
		build("pg stat", "pg", "stat"),
		build("pg dump_stuck", "pg", "dump_stuck", "inactive", "unclean", "stale", "undersized", "degraded"),
	}
}

// upgradeCollectAllowed reports whether argv is exactly one allowed command.
func upgradeCollectAllowed(argv []string) bool {
	for _, c := range upgradeCollectAllowlist() {
		if strings.Join(c.Argv, "\x00") == strings.Join(argv, "\x00") && len(c.Argv) == len(argv) {
			return true
		}
	}
	return false
}

// upgradeCommandRequest is what the collector asks the runner to execute.
type upgradeCommandRequest struct {
	Executor   string
	Generation uint64
	Phase      uint64
	Argv       []string
}

// upgradeCommandResult is the runner's answer. Stdout is the only parsed
// stream (the CLI prints human notes on stderr). Executor, Generation and
// Phase echo the binding the run actually used; Started and Finished are the
// runner's own timestamps of the command.
type upgradeCommandResult struct {
	Stdout     []byte
	ExitCode   int
	Executor   string
	Generation uint64
	Phase      uint64
	Started    time.Time
	Finished   time.Time
}

// upgradeCommandRunner executes one command. It is the only seam to a real
// cluster and is injected, so the collector is Docker-free.
type upgradeCommandRunner interface {
	Run(ctx context.Context, req upgradeCommandRequest) (upgradeCommandResult, error)
}

// upgradeCollectorConfig configures a collector. Every field except Commands
// is required.
type upgradeCollectorConfig struct {
	Runner   upgradeCommandRunner
	Now      func() time.Time
	Executor string        // CLI executor target registered in upgradeTargets
	Deadline time.Time     // phase deadline: a result after it is not evidence
	MaxSpan  time.Duration // longest allowed collection from first start to last finish
	// Commands overrides the default command table. Every entry is still
	// checked against the allowlist before it is run.
	Commands []upgradeCollectCommand
}

type upgradeCollector struct {
	cfg upgradeCollectorConfig
}

func newUpgradeCollector(cfg upgradeCollectorConfig) (*upgradeCollector, error) {
	switch {
	case isNilUpgradeRunner(cfg.Runner):
		return nil, errors.New("upgrade collector has no runner")
	case cfg.Now == nil:
		return nil, errors.New("upgrade collector has no clock")
	case cfg.Executor == "":
		return nil, errors.New("upgrade collector has no executor")
	case cfg.Deadline.IsZero():
		return nil, errors.New("upgrade collector has no phase deadline")
	case cfg.MaxSpan <= 0:
		return nil, errors.New("upgrade collector has no collection span")
	}
	return &upgradeCollector{cfg: cfg}, nil
}

// isNilUpgradeRunner rejects an interface that holds a typed nil pointer.
func isNilUpgradeRunner(r upgradeCommandRunner) bool {
	if r == nil {
		return true
	}
	v := reflect.ValueOf(r)
	switch v.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface:
		return v.IsNil()
	}
	return false
}

func (c *upgradeCollector) commands() []upgradeCollectCommand {
	if c.cfg.Commands != nil {
		return c.cfg.Commands
	}
	return upgradeCollectAllowlist()
}

// collect runs the allowed commands for daemon's transition inside phase and
// returns settled evidence for the sequence plan. want is the version
// checkpoint implied by the confirmed transitions. On any problem the result
// carries only ObservationErr.
func (c *upgradeCollector) collect(ctx context.Context, phase *upgradePhase, daemon string, want upgradeCheckpoint) upgradeSettled {
	outputs, err := c.run(ctx, phase)
	if err != nil {
		return upgradeSettled{Daemon: daemon, ObservationErr: fmt.Errorf("collect for %q: %w", daemon, err)}
	}
	ev := upgradeSettledFromOutputs(daemon, outputs)
	if ev.ObservationErr != nil {
		return ev
	}
	// The phase takes the inventory only after every output was usable, only
	// when it matches the checkpoint and only while no target moved (the
	// phase re-checks ownership itself, so a target that moved during the
	// last command is caught here too), so a failed collection leaves nothing
	// behind in the phase.
	if err := phase.observeInventory(outputs.Versions, want); err != nil {
		return upgradeSettled{Daemon: daemon, ObservationErr: fmt.Errorf("collect for %q: %w", daemon, err)}
	}
	return ev
}

func (c *upgradeCollector) run(ctx context.Context, phase *upgradePhase) (upgradeObservationOutputs, error) {
	var out upgradeObservationOutputs
	if phase == nil {
		return out, errors.New("no phase")
	}
	if err := phase.current(); err != nil {
		return out, err
	}
	generation, ok := phase.opened[c.cfg.Executor]
	if !ok {
		return out, fmt.Errorf("executor %q is not a target of phase %q", c.cfg.Executor, phase.name)
	}
	cmds := c.commands()
	if len(cmds) != len(upgradeCollectAllowlist()) {
		return out, fmt.Errorf("%w: command table has %d entries", errUpgradeCollectDenied, len(cmds))
	}
	// Refuse the whole table before anything runs, so a bad entry late in the
	// table cannot follow earlier reads.
	slotNames := map[string]bool{}
	for _, cmd := range cmds {
		if !upgradeCollectAllowed(cmd.Argv) {
			return out, fmt.Errorf("%w: %q", errUpgradeCollectDenied, strings.Join(cmd.Argv, " "))
		}
		if slotNames[cmd.Name] {
			return out, fmt.Errorf("%w: command slot %q is used twice", errUpgradeCollectDenied, cmd.Name)
		}
		slotNames[cmd.Name] = true
	}
	begin := c.cfg.Now()
	slots := map[string]*[]byte{
		"versions": &out.Versions, "mon dump": &out.MonDump, "quorum_status": &out.QuorumStatus,
		"osd stat": &out.OSDStat, "mgr stat": &out.MgrStat, "mgr dump": &out.MgrDump,
		"pg stat": &out.PGStat, "pg dump_stuck": &out.PGStuck,
	}
	for name := range slotNames {
		if slots[name] == nil {
			return out, fmt.Errorf("%w: unknown command slot %q", errUpgradeCollectDenied, name)
		}
	}
	var results []upgradeCommandResult
	for _, cmd := range cmds {
		slot := slots[cmd.Name]
		if err := c.expired(ctx); err != nil {
			return upgradeObservationOutputs{}, err
		}
		if err := phase.current(); err != nil {
			return upgradeObservationOutputs{}, err
		}
		req := upgradeCommandRequest{Executor: c.cfg.Executor, Generation: generation, Phase: phase.seq, Argv: append([]string(nil), cmd.Argv...)}
		res, err := c.cfg.Runner.Run(ctx, req)
		// The caller's cancellation or the phase deadline wins over whatever
		// the runner reported, so it is never blamed on the probe.
		if expired := c.expired(ctx); expired != nil {
			return upgradeObservationOutputs{}, expired
		}
		if err != nil {
			return upgradeObservationOutputs{}, fmt.Errorf("%w: %s: %v", errUpgradeCollectProbe, cmd.Name, err)
		}
		if res.ExitCode != 0 {
			return upgradeObservationOutputs{}, fmt.Errorf("%w: %s exited with %d", errUpgradeCollectProbe, cmd.Name, res.ExitCode)
		}
		if res.Executor != req.Executor || res.Generation != req.Generation || res.Phase != req.Phase {
			return upgradeObservationOutputs{}, fmt.Errorf("%w: %s ran as %q generation %d phase %d, want %q generation %d phase %d",
				errUpgradeCollectBinding, cmd.Name, res.Executor, res.Generation, res.Phase, req.Executor, req.Generation, req.Phase)
		}
		if len(res.Stdout) == 0 {
			return upgradeObservationOutputs{}, fmt.Errorf("%w: %s printed nothing", errUpgradeCollectProbe, cmd.Name)
		}
		*slot = append([]byte(nil), res.Stdout...)
		results = append(results, res)
	}
	end := c.cfg.Now()
	if err := c.fresh(begin, end, results); err != nil {
		return upgradeObservationOutputs{}, err
	}
	if end.After(c.cfg.Deadline) {
		return upgradeObservationOutputs{}, fmt.Errorf("%w: collection ended after the deadline", errUpgradeCollectDeadline)
	}
	return out, nil
}

// expired reports cancellation of the caller or the phase deadline.
func (c *upgradeCollector) expired(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: %v", errUpgradeCollectDeadline, err)
	}
	if c.cfg.Now().After(c.cfg.Deadline) {
		return fmt.Errorf("%w: deadline passed", errUpgradeCollectDeadline)
	}
	return nil
}

// fresh compares each run with the collection interval [begin, end]. A run
// that started before the collection may be a cached answer, one that
// finished after it cannot belong to it, and one that finished after the phase
// deadline is not evidence.
func (c *upgradeCollector) fresh(begin, end time.Time, results []upgradeCommandResult) error {
	var problems []string
	for i, r := range results {
		switch {
		case r.Started.IsZero() || r.Finished.IsZero():
			problems = append(problems, fmt.Sprintf("command %d has no timestamps", i))
		case r.Finished.Before(r.Started):
			problems = append(problems, fmt.Sprintf("command %d finished before it started", i))
		case r.Started.Before(begin):
			problems = append(problems, fmt.Sprintf("command %d started before the collection", i))
		case r.Finished.After(c.cfg.Deadline):
			problems = append(problems, fmt.Sprintf("command %d finished after the phase deadline", i))
		case r.Finished.After(end):
			problems = append(problems, fmt.Sprintf("command %d finished after the collection", i))
		}
	}
	if span := end.Sub(begin); span > c.cfg.MaxSpan {
		problems = append(problems, fmt.Sprintf("collection took %s, allowed %s", span, c.cfg.MaxSpan))
	}
	if len(problems) == 0 {
		return nil
	}
	sort.Strings(problems)
	// A result after the deadline is attributed to the phase, not to a probe.
	for _, p := range problems {
		if strings.HasSuffix(p, "after the phase deadline") {
			return fmt.Errorf("%w: %s", errUpgradeCollectDeadline, strings.Join(problems, "; "))
		}
	}
	return fmt.Errorf("%w: %s", errUpgradeCollectStale, strings.Join(problems, "; "))
}
