//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

// These are Docker-free controls of the private read-only upgrade observation
// collector. The runner is a fake that replays synthetic command outputs
// shaped like the pinned Ceph source (see the parser helpers). They executed
// no Ceph or Docker, so they say nothing about a native upgrade, and a fake
// runner cannot show that a real runner executed inside the stamped executor.

type collectClock struct{ now time.Time }

func (c *collectClock) Now() time.Time { return c.now }

// collectRunner replays outputs by argument vector. hook may rewrite the
// request/result of call i (0-based) or fail it.
type collectRunner struct {
	clock   *collectClock
	outputs map[string][]byte
	step    time.Duration
	calls   [][]string
	hook    func(i int, req upgradeCommandRequest, res *upgradeCommandResult) error
}

func collectOutputsByArgv(o upgradeObservationOutputs) map[string][]byte {
	byName := map[string][]byte{
		"versions": o.Versions, "mon dump": o.MonDump, "quorum_status": o.QuorumStatus, "osd stat": o.OSDStat,
		"mgr stat": o.MgrStat, "mgr dump": o.MgrDump, "pg stat": o.PGStat, "pg dump_stuck": o.PGStuck,
	}
	out := map[string][]byte{}
	for _, c := range upgradeCollectAllowlist() {
		out[strings.Join(c.Argv, " ")] = byName[c.Name]
	}
	return out
}

func (r *collectRunner) Run(_ context.Context, req upgradeCommandRequest) (upgradeCommandResult, error) {
	i := len(r.calls)
	r.calls = append(r.calls, req.Argv)
	res := upgradeCommandResult{
		Stdout: r.outputs[strings.Join(req.Argv, " ")], Executor: req.Executor, Generation: req.Generation, Phase: req.Phase,
		Started: r.clock.now,
	}
	r.clock.now = r.clock.now.Add(r.step)
	res.Finished = r.clock.now
	if r.hook != nil {
		if err := r.hook(i, req, &res); err != nil {
			return upgradeCommandResult{}, err
		}
	}
	return res, nil
}

type collectEnv struct {
	t       *testing.T
	clock   *collectClock
	targets *upgradeTargets
	runner  *collectRunner
	phase   *upgradePhase
	col     *upgradeCollector
	want    upgradeCheckpoint
}

var collectDaemons = []string{"mon.a", "mon.b", "mon.c", "mgr.x", "mgr.y", "osd.0", "osd.1", "osd.2"}

// newCollectEnv opens a phase after n transitions with a runner that replays
// the matching outputs. The deadline is 10 minutes after the start.
func newCollectEnv(t *testing.T, n int) *collectEnv {
	t.Helper()
	clock := &collectClock{now: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	targets := newUpgradeTargets(append([]string{"cli"}, collectDaemons...)...)
	want, err := upgradeCheckpointAfter(seqPlan(), upgradePair, n)
	if err != nil {
		t.Fatal(err)
	}
	env := &collectEnv{t: t, clock: clock, targets: targets, want: want}
	env.runner = &collectRunner{clock: clock, step: time.Second, outputs: collectOutputsByArgv(obsOutputsFor(t, n))}
	env.phase = targets.begin("checkpoint")
	env.col = env.newCollector(func(c *upgradeCollectorConfig) {})
	return env
}

func (e *collectEnv) newCollector(edit func(*upgradeCollectorConfig)) *upgradeCollector {
	e.t.Helper()
	cfg := upgradeCollectorConfig{Runner: e.runner, Now: e.clock.Now, Executor: "cli", Deadline: e.clock.now.Add(10 * time.Minute), MaxSpan: time.Minute}
	edit(&cfg)
	col, err := newUpgradeCollector(cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	return col
}

func (e *collectEnv) collect(daemon string) upgradeSettled {
	return e.col.collect(context.Background(), e.phase, daemon, e.want)
}

// requireOnlyError asserts the all-or-nothing contract: an observation error
// and no evidence field, and no inventory kept by the phase.
func requireOnlyError(t *testing.T, label string, ev upgradeSettled, phase *upgradePhase, target error) {
	t.Helper()
	if ev.ObservationErr == nil {
		t.Fatalf("%s: no observation error: %+v", label, ev)
	}
	if target != nil && !errors.Is(ev.ObservationErr, target) {
		t.Fatalf("%s: error %v is not %v", label, ev.ObservationErr, target)
	}
	if ev.Inventory != nil || ev.Quorum != nil || ev.MgrActive != "" || ev.MgrStandbys != nil || ev.OSDsUpIn != nil ||
		ev.PGsSettled != upgradeUnknown || ev.MinMonRelease != nil {
		t.Fatalf("%s: partial evidence leaked: %+v", label, ev)
	}
	if phase != nil && phase.inventory != nil {
		t.Fatalf("%s: phase kept an inventory %v", label, phase.inventory)
	}
}

func TestUpgradeCollectorPublishesWhatTheAdapterBuilds(t *testing.T) {
	env := newCollectEnv(t, 3)
	ev := env.collect("mon.c")
	if ev.ObservationErr != nil {
		t.Fatal(ev.ObservationErr)
	}
	want := upgradeSettledFromOutputs("mon.c", obsOutputsFor(t, 3))
	if !reflect.DeepEqual(ev, want) {
		t.Fatalf("collector evidence %+v differs from the adapter's %+v", ev, want)
	}
	if got, err := env.phase.satisfied(); err != nil || !reflect.DeepEqual(got, ev.Inventory) {
		t.Fatalf("phase inventory = %v, %v", got, err)
	}
	if len(env.runner.calls) != 8 {
		t.Fatalf("runner got %d commands, want 8", len(env.runner.calls))
	}
}

func TestUpgradeCollectorDrivesTheWholePlan(t *testing.T) {
	s := newSeq(t, goodFacts(t))
	clock := &collectClock{now: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	targets := newUpgradeTargets(append([]string{"cli"}, collectDaemons...)...)
	plan := seqPlan()
	for i := 1; i <= len(plan); i++ {
		id, d, err := s.admit()
		if err != nil {
			t.Fatal(err)
		}
		ledgerOK(t, s.ledger.fence(id, d))
		ledgerOK(t, s.ledger.oldAbsent(id, false, nil))
		ledgerOK(t, s.ledger.startAttempted(id, "dst-"+id))
		ledgerOK(t, s.ledger.confirm(id, upgradeNativeEvidence{Instance: "dst-" + id, Version: "20.2.4", FSID: d.FSID, StoreID: d.StoreID}))
		// The transition changes the daemon's generation; after the MON phase
		// the destination CLI is a new executor as well.
		if err := targets.advance(d.key()); err != nil {
			t.Fatal(err)
		}
		if i == 3 {
			if err := targets.advance("cli"); err != nil {
				t.Fatal(err)
			}
		}
		phase := targets.begin("after " + d.key())
		runner := &collectRunner{clock: clock, step: time.Second, outputs: collectOutputsByArgv(obsOutputsFor(t, i))}
		col, err := newUpgradeCollector(upgradeCollectorConfig{Runner: runner, Now: clock.Now, Executor: "cli", Deadline: clock.now.Add(time.Hour), MaxSpan: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		want, _ := upgradeCheckpointAfter(plan, upgradePair, i)
		ev := col.collect(context.Background(), phase, d.key(), want)
		if ev.ObservationErr != nil {
			t.Fatalf("transition %d: %v", i, ev.ObservationErr)
		}
		ledgerOK(t, s.settle(ev))
	}
	ledgerOK(t, s.finalize())
}

func TestUpgradeCollectorRunsOnlyTheEightReadOnlyCommands(t *testing.T) {
	env := newCollectEnv(t, 1)
	if ev := env.collect("mon.a"); ev.ObservationErr != nil {
		t.Fatal(ev.ObservationErr)
	}
	var got []string
	for _, argv := range env.runner.calls {
		got = append(got, strings.Join(argv, " "))
	}
	want := []string{
		"ceph versions --format json",
		"ceph mon dump --format json",
		"ceph quorum_status --format json",
		"ceph osd stat --format json",
		"ceph mgr stat --format json",
		"ceph mgr dump --format json",
		"ceph pg stat --format json",
		// The handler defaults to "unclean" alone, so every type is explicit.
		"ceph pg dump_stuck inactive unclean stale undersized degraded --format json",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("commands = %q\nwant      %q", got, want)
	}
	mutating := []string{"set", "unset", "rm", "remove", "add", "out", "in", "down", "reweight", "require-osd-release", "scrub", "repair", "tell", "daemon", "config", "auth", "pool", "crush", "fail", "restart"}
	for _, argv := range env.runner.calls {
		for _, word := range argv[1:] {
			for _, bad := range mutating {
				if word == bad {
					t.Errorf("command %q contains the mutating word %q", argv, bad)
				}
			}
		}
	}
}

func TestUpgradeCollectorRefusesAnyOtherCommandBeforeRunning(t *testing.T) {
	good := upgradeCollectAllowlist()
	replace := func(i int, argv ...string) []upgradeCollectCommand {
		table := append([]upgradeCollectCommand(nil), good...)
		table[i] = upgradeCollectCommand{Name: good[i].Name, Argv: argv}
		return table
	}
	rename := func(i int, name string) []upgradeCollectCommand {
		table := append([]upgradeCollectCommand(nil), good...)
		table[i] = upgradeCollectCommand{Name: name, Argv: good[i].Argv}
		return table
	}
	for name, table := range map[string][]upgradeCollectCommand{
		"set noout last":        replace(7, "ceph", "osd", "set", "noout"),
		"require osd release":   replace(3, "ceph", "osd", "require-osd-release", "tentacle"),
		"mon remove":            replace(1, "ceph", "mon", "remove", "a"),
		"tell":                  replace(0, "ceph", "tell", "osd.0", "injectargs", "--x"),
		"daemon socket":         replace(0, "ceph", "daemon", "mon.a", "config", "set", "k", "v"),
		"extra argument":        replace(0, "ceph", "versions", "--format", "json", "--force"),
		"missing format":        replace(0, "ceph", "versions"),
		"plain format":          replace(0, "ceph", "versions", "--format", "plain"),
		"dump_stuck default":    replace(7, "ceph", "pg", "dump_stuck", "--format", "json"),
		"dump_stuck one type":   replace(7, "ceph", "pg", "dump_stuck", "unclean", "--format", "json"),
		"other binary":          replace(0, "rados", "versions", "--format", "json"),
		"empty":                 replace(0),
		"empty table":           {},
		"short table":           good[:7],
		"long table":            append(append([]upgradeCollectCommand(nil), good...), good[0]),
		"duplicate slot":        rename(1, "versions"),
		"unknown slot":          rename(1, "osd dump"),
		"allowed argv, no slot": rename(1, ""),
	} {
		t.Run(name, func(t *testing.T) {
			env := newCollectEnv(t, 1)
			col := env.newCollector(func(c *upgradeCollectorConfig) { c.Commands = table })
			ev := col.collect(context.Background(), env.phase, "mon.a", env.want)
			requireOnlyError(t, name, ev, env.phase, errUpgradeCollectDenied)
			if len(env.runner.calls) != 0 {
				t.Fatalf("runner got %q before the table was refused", env.runner.calls)
			}
		})
	}
}

func TestUpgradeCollectorBindsEveryRunToThePhaseAndExecutor(t *testing.T) {
	stamp := map[string]func(*upgradeCommandRequest, *upgradeCommandResult){
		"old executor generation":  func(req *upgradeCommandRequest, res *upgradeCommandResult) { res.Generation = req.Generation - 1 },
		"next executor generation": func(req *upgradeCommandRequest, res *upgradeCommandResult) { res.Generation = req.Generation + 1 },
		"another executor":         func(_ *upgradeCommandRequest, res *upgradeCommandResult) { res.Executor = "cli-source" },
		"no executor":              func(_ *upgradeCommandRequest, res *upgradeCommandResult) { res.Executor = "" },
		"earlier phase":            func(req *upgradeCommandRequest, res *upgradeCommandResult) { res.Phase = req.Phase - 1 },
		"later phase":              func(req *upgradeCommandRequest, res *upgradeCommandResult) { res.Phase = req.Phase + 1 },
		"unset phase":              func(_ *upgradeCommandRequest, res *upgradeCommandResult) { res.Phase = 0 },
	}
	for name, apply := range stamp {
		for _, at := range []int{0, 4, 7} {
			t.Run(name, func(t *testing.T) {
				env := newCollectEnv(t, 1)
				env.runner.hook = func(i int, req upgradeCommandRequest, res *upgradeCommandResult) error {
					if i == at {
						apply(&req, res)
					}
					return nil
				}
				ev := env.collect("mon.a")
				requireOnlyError(t, name, ev, env.phase, errUpgradeCollectBinding)
			})
		}
	}
}

func TestUpgradeCollectorRefusesAStalePhase(t *testing.T) {
	t.Run("superseded phase runs nothing", func(t *testing.T) {
		env := newCollectEnv(t, 1)
		env.targets.begin("next")
		ev := env.collect("mon.a")
		requireOnlyError(t, "superseded", ev, env.phase, errUpgradePhaseClosed)
		if len(env.runner.calls) != 0 {
			t.Fatalf("runner got %q for a closed phase", env.runner.calls)
		}
	})
	t.Run("target changed before the run", func(t *testing.T) {
		env := newCollectEnv(t, 1)
		if err := env.targets.advance("mon.b"); err != nil {
			t.Fatal(err)
		}
		ev := env.collect("mon.a")
		requireOnlyError(t, "moved before", ev, env.phase, errUpgradeStale)
		if len(env.runner.calls) != 0 {
			t.Fatalf("runner got %q", env.runner.calls)
		}
	})
	for _, target := range []string{"mon.a", "osd.2", "cli"} {
		for _, at := range []int{2, 7} {
			t.Run("target "+target+" changes during the run", func(t *testing.T) {
				env := newCollectEnv(t, 1)
				env.runner.hook = func(i int, _ upgradeCommandRequest, _ *upgradeCommandResult) error {
					if i == at {
						return env.targets.advance(target)
					}
					return nil
				}
				ev := env.collect("mon.a")
				requireOnlyError(t, "moved during", ev, env.phase, errUpgradeStale)
				if len(env.runner.calls) != at+1 {
					t.Fatalf("runner got %d commands after the target moved, want %d", len(env.runner.calls), at+1)
				}
			})
		}
	}
	t.Run("phase superseded during the run", func(t *testing.T) {
		env := newCollectEnv(t, 1)
		env.runner.hook = func(i int, _ upgradeCommandRequest, _ *upgradeCommandResult) error {
			if i == 5 {
				env.targets.begin("next")
			}
			return nil
		}
		ev := env.collect("mon.a")
		requireOnlyError(t, "superseded during", ev, env.phase, errUpgradePhaseClosed)
	})
	t.Run("an old success does not satisfy a later generation", func(t *testing.T) {
		env := newCollectEnv(t, 1)
		if ev := env.collect("mon.a"); ev.ObservationErr != nil {
			t.Fatal(ev.ObservationErr)
		}
		if err := env.targets.advance("mon.b"); err != nil {
			t.Fatal(err)
		}
		calls := len(env.runner.calls)
		ev := env.collect("mon.a")
		if ev.ObservationErr == nil || !errors.Is(ev.ObservationErr, errUpgradeStale) {
			t.Fatalf("second collection = %+v", ev)
		}
		if len(env.runner.calls) != calls {
			t.Fatal("a stale phase ran commands")
		}
		if _, err := env.phase.satisfied(); !errors.Is(err, errUpgradeStale) {
			t.Fatalf("stale phase still satisfied: %v", err)
		}
	})
	t.Run("unchanged reuse is allowed", func(t *testing.T) {
		env := newCollectEnv(t, 1)
		for i := 0; i < 2; i++ {
			if ev := env.collect("mon.a"); ev.ObservationErr != nil {
				t.Fatalf("collection %d: %v", i, ev.ObservationErr)
			}
		}
	})
}

func TestUpgradeCollectorChecksFreshnessAgainstTheCollectionInterval(t *testing.T) {
	cases := map[string]struct {
		edit func(i int, begin time.Time, res *upgradeCommandResult)
		want error
	}{
		"started before the collection": {func(i int, begin time.Time, res *upgradeCommandResult) {
			if i == 3 {
				res.Started = begin.Add(-time.Nanosecond)
			}
		}, errUpgradeCollectStale},
		"finished after the collection": {func(i int, _ time.Time, res *upgradeCommandResult) {
			if i == 3 {
				res.Finished = res.Finished.Add(time.Hour)
			}
		}, errUpgradeCollectDeadline},
		"finished after only the interval": {func(i int, _ time.Time, res *upgradeCommandResult) {
			if i == 3 {
				res.Finished = res.Finished.Add(30 * time.Second)
			}
		}, errUpgradeCollectStale},
		"no start": {func(i int, _ time.Time, res *upgradeCommandResult) {
			if i == 0 {
				res.Started = time.Time{}
			}
		}, errUpgradeCollectStale},
		"no finish": {func(i int, _ time.Time, res *upgradeCommandResult) {
			if i == 7 {
				res.Finished = time.Time{}
			}
		}, errUpgradeCollectStale},
		"finished before it started": {func(i int, _ time.Time, res *upgradeCommandResult) {
			if i == 2 {
				res.Finished = res.Started.Add(-time.Second)
			}
		}, errUpgradeCollectStale},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			env := newCollectEnv(t, 1)
			begin := env.clock.now
			env.runner.hook = func(i int, _ upgradeCommandRequest, res *upgradeCommandResult) error {
				tc.edit(i, begin, res)
				return nil
			}
			requireOnlyError(t, name, env.collect("mon.a"), env.phase, tc.want)
		})
	}
	t.Run("collection longer than the allowed span", func(t *testing.T) {
		env := newCollectEnv(t, 1)
		env.runner.step = 10 * time.Second // 80s in total, allowed 60s
		requireOnlyError(t, "span", env.collect("mon.a"), env.phase, errUpgradeCollectStale)
	})
	t.Run("the exact span is allowed", func(t *testing.T) {
		env := newCollectEnv(t, 1)
		env.runner.step = 7500 * time.Millisecond // 60s in total
		if ev := env.collect("mon.a"); ev.ObservationErr != nil {
			t.Fatal(ev.ObservationErr)
		}
	})
}

func TestUpgradeCollectorAnyFailedCommandPublishesNothing(t *testing.T) {
	breakIt := map[string]func(*upgradeCommandResult) error{
		"runner error": func(*upgradeCommandResult) error { return errors.New("exec failed") },
		"nonzero exit": func(res *upgradeCommandResult) error { res.ExitCode = 22; return nil },
		"no output":    func(res *upgradeCommandResult) error { res.Stdout = nil; return nil },
		"not json":     func(res *upgradeCommandResult) error { res.Stdout = []byte("dumped monmap epoch 3"); return nil },
		"truncated":    func(res *upgradeCommandResult) error { res.Stdout = res.Stdout[:len(res.Stdout)/2]; return nil },
		"null":         func(res *upgradeCommandResult) error { res.Stdout = []byte("null"); return nil },
		"trailing output": func(res *upgradeCommandResult) error {
			res.Stdout = append(append([]byte(nil), res.Stdout...), " {}"...)
			return nil
		},
	}
	for name, apply := range breakIt {
		for at := 0; at < 8; at++ {
			env := newCollectEnv(t, 1)
			env.runner.hook = func(i int, _ upgradeCommandRequest, res *upgradeCommandResult) error {
				if i == at {
					return apply(res)
				}
				return nil
			}
			ev := env.collect("mon.a")
			requireOnlyError(t, name, ev, env.phase, nil)
			if errors.Is(ev.ObservationErr, errUpgradeCollectDeadline) {
				t.Errorf("%s at %d: a probe failure was attributed to the phase deadline: %v", name, at, ev.ObservationErr)
			}
			if name == "runner error" || name == "nonzero exit" || name == "no output" {
				if !errors.Is(ev.ObservationErr, errUpgradeCollectProbe) {
					t.Errorf("%s at %d: not attributed to the probe: %v", name, at, ev.ObservationErr)
				}
				if len(env.runner.calls) != at+1 {
					t.Errorf("%s at %d: ran %d commands, want to stop at %d", name, at, len(env.runner.calls), at+1)
				}
			}
		}
	}
}

func TestUpgradeCollectorDeadlineAndCancellationAreNotProbeErrors(t *testing.T) {
	t.Run("cancelled before the run", func(t *testing.T) {
		env := newCollectEnv(t, 1)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		ev := env.col.collect(ctx, env.phase, "mon.a", env.want)
		requireOnlyError(t, "cancelled", ev, env.phase, errUpgradeCollectDeadline)
		if errors.Is(ev.ObservationErr, errUpgradeCollectProbe) || len(env.runner.calls) != 0 {
			t.Fatalf("calls %d, error %v", len(env.runner.calls), ev.ObservationErr)
		}
	})
	t.Run("cancelled while a command ran", func(t *testing.T) {
		env := newCollectEnv(t, 1)
		ctx, cancel := context.WithCancel(context.Background())
		env.runner.hook = func(i int, _ upgradeCommandRequest, _ *upgradeCommandResult) error {
			if i == 3 {
				cancel()
				return ctx.Err()
			}
			return nil
		}
		ev := env.col.collect(ctx, env.phase, "mon.a", env.want)
		requireOnlyError(t, "cancelled during", ev, env.phase, errUpgradeCollectDeadline)
		if errors.Is(ev.ObservationErr, errUpgradeCollectProbe) || len(env.runner.calls) != 4 {
			t.Fatalf("calls %d, error %v", len(env.runner.calls), ev.ObservationErr)
		}
	})
	t.Run("cancelled but the runner still returned output", func(t *testing.T) {
		env := newCollectEnv(t, 1)
		ctx, cancel := context.WithCancel(context.Background())
		env.runner.hook = func(i int, _ upgradeCommandRequest, _ *upgradeCommandResult) error {
			if i == 7 {
				cancel()
			}
			return nil
		}
		requireOnlyError(t, "late cancel", env.col.collect(ctx, env.phase, "mon.a", env.want), env.phase, errUpgradeCollectDeadline)
	})
	t.Run("deadline passes between commands", func(t *testing.T) {
		env := newCollectEnv(t, 1)
		env.runner.hook = func(i int, _ upgradeCommandRequest, _ *upgradeCommandResult) error {
			if i == 2 {
				env.clock.now = env.clock.now.Add(time.Hour)
			}
			return nil
		}
		ev := env.collect("mon.a")
		requireOnlyError(t, "deadline", ev, env.phase, errUpgradeCollectDeadline)
		if len(env.runner.calls) != 3 {
			t.Fatalf("ran %d commands after the deadline", len(env.runner.calls)-3)
		}
	})
	t.Run("last result arrives after the deadline", func(t *testing.T) {
		env := newCollectEnv(t, 1)
		env.col = env.newCollector(func(c *upgradeCollectorConfig) { c.Deadline = env.clock.now.Add(7*time.Second + 500*time.Millisecond) })
		requireOnlyError(t, "late result", env.collect("mon.a"), env.phase, errUpgradeCollectDeadline)
	})
	t.Run("a result exactly at the deadline is evidence", func(t *testing.T) {
		env := newCollectEnv(t, 1)
		env.col = env.newCollector(func(c *upgradeCollectorConfig) { c.Deadline = env.clock.now.Add(8 * time.Second) })
		if ev := env.collect("mon.a"); ev.ObservationErr != nil {
			t.Fatal(ev.ObservationErr)
		}
	})
	t.Run("the runner's own timeout is a probe error", func(t *testing.T) {
		env := newCollectEnv(t, 1)
		env.runner.hook = func(i int, _ upgradeCommandRequest, _ *upgradeCommandResult) error {
			if i == 1 {
				return context.DeadlineExceeded
			}
			return nil
		}
		ev := env.collect("mon.a")
		requireOnlyError(t, "own timeout", ev, env.phase, errUpgradeCollectProbe)
		if errors.Is(ev.ObservationErr, errUpgradeCollectDeadline) {
			t.Fatalf("an independent probe error was attributed to the phase: %v", ev.ObservationErr)
		}
	})
}

func TestUpgradeCollectorChecksTheCheckpointBeforePublishing(t *testing.T) {
	env := newCollectEnv(t, 1)
	// Outputs of the previous checkpoint are well formed but not the current one.
	env.runner.outputs = collectOutputsByArgv(obsOutputsFor(t, 0))
	requireOnlyError(t, "old inventory", env.collect("mon.a"), env.phase, nil)

	// Contradicting MON documents stay unusable through the collector as well.
	env = newCollectEnv(t, 3)
	o := obsOutputsFor(t, 3)
	o.QuorumStatus = []byte(obsEdit(t, string(o.QuorumStatus), func(m map[string]any) { m["monmap"].(map[string]any)["epoch"] = 99 }))
	env.runner.outputs = collectOutputsByArgv(o)
	requireOnlyError(t, "contradiction", env.collect("mon.c"), env.phase, nil)
}

func TestUpgradeCollectorFailureLeavesThePlanAtTheSameStep(t *testing.T) {
	s := newSeq(t, goodFacts(t))
	driveTo(t, s, 1)
	id, d, err := s.admit()
	if err != nil {
		t.Fatal(err)
	}
	ledgerOK(t, s.ledger.fence(id, d))
	ledgerOK(t, s.ledger.oldAbsent(id, false, nil))
	ledgerOK(t, s.ledger.startAttempted(id, "dst-"+id))
	ledgerOK(t, s.ledger.confirm(id, upgradeNativeEvidence{Instance: "dst-" + id, Version: "20.2.4", FSID: d.FSID, StoreID: d.StoreID}))

	env := newCollectEnv(t, 2)
	env.runner.hook = func(i int, _ upgradeCommandRequest, _ *upgradeCommandResult) error {
		if i == 6 {
			return errors.New("pg stat failed")
		}
		return nil
	}
	bad := env.collect(d.key())
	requireOnlyError(t, "failed pg stat", bad, env.phase, errUpgradeCollectProbe)
	if err := s.settle(bad); err == nil {
		t.Fatal("the plan settled on a failed collection")
	}
	if _, _, err := s.admit(); err == nil {
		t.Fatal("the plan admitted the next transition after a failed collection")
	}

	env = newCollectEnv(t, 2)
	good := env.collect(d.key())
	if good.ObservationErr != nil {
		t.Fatal(good.ObservationErr)
	}
	ledgerOK(t, s.settle(good))
	if _, _, err := s.admit(); err != nil {
		t.Fatalf("next transition after good evidence: %v", err)
	}
}

func TestUpgradeCollectorRejectsBadConfiguration(t *testing.T) {
	env := newCollectEnv(t, 1)
	var typedNil *collectRunner
	base := upgradeCollectorConfig{Runner: env.runner, Now: env.clock.Now, Executor: "cli", Deadline: env.clock.now.Add(time.Minute), MaxSpan: time.Minute}
	for name, edit := range map[string]func(*upgradeCollectorConfig){
		"nil runner":       func(c *upgradeCollectorConfig) { c.Runner = nil },
		"typed nil runner": func(c *upgradeCollectorConfig) { c.Runner = typedNil },
		"no clock":         func(c *upgradeCollectorConfig) { c.Now = nil },
		"no executor":      func(c *upgradeCollectorConfig) { c.Executor = "" },
		"no deadline":      func(c *upgradeCollectorConfig) { c.Deadline = time.Time{} },
		"no span":          func(c *upgradeCollectorConfig) { c.MaxSpan = 0 },
		"negative span":    func(c *upgradeCollectorConfig) { c.MaxSpan = -time.Second },
	} {
		cfg := base
		edit(&cfg)
		if col, err := newUpgradeCollector(cfg); err == nil || col != nil {
			t.Errorf("%s: accepted", name)
		}
	}
	col, err := newUpgradeCollector(base)
	if err != nil {
		t.Fatal(err)
	}
	ev := col.collect(context.Background(), nil, "mon.a", env.want)
	requireOnlyError(t, "no phase", ev, nil, nil)
	other := env.newCollector(func(c *upgradeCollectorConfig) { c.Executor = "unregistered" })
	requireOnlyError(t, "unknown executor", other.collect(context.Background(), env.phase, "mon.a", env.want), env.phase, nil)
	if len(env.runner.calls) != 0 {
		t.Fatalf("runner got %q", env.runner.calls)
	}
}
