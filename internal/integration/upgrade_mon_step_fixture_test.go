//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// These are Docker-free controls of the first bounded upgrade step driver.
// Container effects and the workload client are fakes, and the cluster
// observations are the synthetic documents of the parser fixtures. They ran
// no Ceph or Docker, no retained store, no image change and no real client,
// so they establish orchestration and failure-path behavior only, not a
// native upgrade.

type stepEffects struct {
	calls []string
	// per-call overrides
	fenceID      *upgradeDaemonID
	fenceErr     error
	present      bool
	presentErr   error
	hasStore     bool
	reserved     upgradeMonVector
	retainedErr  error
	launchErr    error
	observeErr   error
	evidence     func(instance string) upgradeNativeEvidence
	disposeErr   error
	disposed     []string
	launches     []string
	plans        []upgradeMonPlan
	onDispose    func()
	disposeCtxOK bool
}

func newStepEffects() *stepEffects {
	r := retainedMon()
	return &stepEffects{hasStore: true, reserved: append(upgradeMonVector(nil), r.Addrs...)}
}

func (e *stepEffects) Fence(_ context.Context, d upgradeDaemonID) (upgradeDaemonID, error) {
	e.calls = append(e.calls, "fence")
	if e.fenceErr != nil {
		return upgradeDaemonID{}, e.fenceErr
	}
	if e.fenceID != nil {
		return *e.fenceID, nil
	}
	return d, nil
}

func (e *stepEffects) SourcePresent(context.Context, upgradeDaemonID) (bool, error) {
	e.calls = append(e.calls, "source-present")
	return e.present, e.presentErr
}

func (e *stepEffects) Retained(context.Context, upgradeDaemonID) (bool, upgradeMonVector, error) {
	e.calls = append(e.calls, "retained")
	return e.hasStore, e.reserved, e.retainedErr
}

func (e *stepEffects) Launch(_ context.Context, instance string, plan upgradeMonPlan) error {
	e.calls = append(e.calls, "launch")
	e.launches = append(e.launches, instance)
	e.plans = append(e.plans, plan)
	return e.launchErr
}

func (e *stepEffects) Observe(_ context.Context, instance string) (upgradeNativeEvidence, error) {
	e.calls = append(e.calls, "observe")
	if e.observeErr != nil {
		return upgradeNativeEvidence{}, e.observeErr
	}
	if e.evidence != nil {
		return e.evidence(instance), nil
	}
	return upgradeNativeEvidence{Instance: instance, Version: "20.2.4", FSID: "fsid-1", StoreID: "store-mona"}, nil
}

func (e *stepEffects) Dispose(ctx context.Context, instances []string) error {
	e.calls = append(e.calls, "dispose")
	e.disposeCtxOK = ctx.Err() == nil
	e.disposed = append(e.disposed, instances...)
	if e.onDispose != nil {
		e.onDispose()
	}
	return e.disposeErr
}

type stepWorkload struct {
	objects  map[string][]byte
	writeErr map[int]error // by 1-based write number
	writes   []string
	reads    map[string]int
	readErr  map[string]error
	// mutate rewrites what a read returns
	mutate func(object string, data []byte, present bool) ([]byte, bool)
	// applyOnError stores the object even though Write returned an error.
	applyOnError bool
}

func newStepWorkload() *stepWorkload {
	return &stepWorkload{objects: map[string][]byte{}, writeErr: map[int]error{}, reads: map[string]int{}, readErr: map[string]error{}}
}

func (w *stepWorkload) Write(_ context.Context, object string, data []byte) error {
	w.writes = append(w.writes, object)
	if err := w.writeErr[len(w.writes)]; err != nil {
		if w.applyOnError {
			w.objects[object] = append([]byte(nil), data...)
		}
		return err
	}
	w.objects[object] = append([]byte(nil), data...)
	return nil
}

func (w *stepWorkload) ReadFresh(_ context.Context, object string) ([]byte, bool, error) {
	w.reads[object]++
	if err := w.readErr[object]; err != nil {
		return nil, false, err
	}
	data, ok := w.objects[object]
	if w.mutate != nil {
		data, ok = w.mutate(object, data, ok)
	}
	return data, ok, nil
}

type stepEnv struct {
	t        *testing.T
	seq      *upgradeSequence
	targets  *upgradeTargets
	handoff  *upgradeMonHandoff
	effects  *stepEffects
	workload *stepWorkload
	runner   *collectRunner
	clock    *collectClock
	receipts []upgradeMonReceipt
	outputs  upgradeObservationOutputs
}

func newStepEnv(t *testing.T) *stepEnv {
	t.Helper()
	clock := &collectClock{now: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	env := &stepEnv{t: t, clock: clock, effects: newStepEffects(), workload: newStepWorkload(), outputs: obsOutputsFor(t, 1)}
	env.seq = newSeq(t, goodFacts(t))
	env.targets = newUpgradeTargets(append([]string{"cli"}, collectDaemons...)...)
	r := retainedMon()
	r.StoreID = "store-mona"
	h, err := newUpgradeMonHandoff(r)
	if err != nil {
		t.Fatal(err)
	}
	env.handoff = h
	return env
}

// plan builds the step with a collector that replays env.outputs.
func (e *stepEnv) step(edit func(*upgradeMonStepConfig)) *upgradeMonStep {
	e.t.Helper()
	e.runner = &collectRunner{clock: e.clock, step: time.Second, outputs: collectOutputsByArgv(e.outputs)}
	col, err := newUpgradeCollector(upgradeCollectorConfig{Runner: e.runner, Now: e.clock.Now, Executor: "cli", Deadline: e.clock.now.Add(time.Hour), MaxSpan: time.Minute})
	if err != nil {
		e.t.Fatal(err)
	}
	cfg := upgradeMonStepConfig{
		Sequence: e.seq, Targets: e.targets, Handoff: e.handoff, Collector: col, Effects: e.effects, Workload: e.workload,
		Baseline: 2, Mixed: 2, OnReceipt: func(r upgradeMonReceipt) { e.receipts = append(e.receipts, r) },
	}
	if edit != nil {
		edit(&cfg)
	}
	s, err := newUpgradeMonStep(cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	return s
}

func (e *stepEnv) hasCall(name string) bool {
	for _, c := range e.effects.calls {
		if c == name {
			return true
		}
	}
	return false
}

func TestUpgradeMonStepSettlesOneTransitionAndChecksTheWorkload(t *testing.T) {
	env := newStepEnv(t)
	rc := env.step(nil).run(context.Background())
	if rc.Err != nil || rc.CleanupErr != nil {
		t.Fatalf("err = %v, cleanup = %v, events = %v", rc.Err, rc.CleanupErr, rc.Events)
	}
	if got := strings.Join(env.effects.calls, ","); got != "fence,source-present,retained,launch,observe,dispose" {
		t.Fatalf("effect order = %s", got)
	}
	if rc.Step != upgradeNativeConfirmed || !rc.Settled || rc.Transition != "t1-mon.a" {
		t.Fatalf("receipt = %+v", rc)
	}
	if len(env.effects.plans) != 1 || env.effects.plans[0].Mkfs || env.effects.plans[0].MonmapUpdate || env.effects.plans[0].MembershipChange {
		t.Fatalf("launch plans = %+v", env.effects.plans)
	}
	if len(env.runner.calls) != 8 {
		t.Fatalf("collector ran %d commands, want 8", len(env.runner.calls))
	}
	if !rc.Workload.OK() || len(rc.Workload.Classes) != 4 {
		t.Fatalf("workload = %+v", rc.Workload)
	}
	for obj, class := range rc.Workload.Classes {
		if class != "acknowledged-exact" {
			t.Errorf("%s is %s", obj, class)
		}
	}
	// The baseline objects were written before the fence and read back with
	// the mixed ones after the transition.
	if env.workload.reads["upgrade-baseline-001"] != 2 || env.workload.reads["upgrade-mixed-003"] != 1 {
		t.Fatalf("reads = %v", env.workload.reads)
	}
	if env.seq.settled != 1 || env.seq.done {
		t.Fatalf("sequence settled=%d done=%v: one MON is a checkpoint, not a completed upgrade", env.seq.settled, env.seq.done)
	}
	if len(env.effects.disposed) != 1 || env.effects.disposed[0] != "dst-t1-mon.a" || !env.effects.disposeCtxOK {
		t.Fatalf("disposed = %v", env.effects.disposed)
	}
}

func TestUpgradeMonStepStopsBeforeTheFault(t *testing.T) {
	cases := map[string]func(*stepEnv){
		"baseline write fails": func(e *stepEnv) { e.workload.writeErr[2] = errors.New("timeout") },
		"baseline write rejected": func(e *stepEnv) {
			e.workload.writeErr[1] = fmt.Errorf("%w: bad", errUpgradeWriteRejected)
		},
		"baseline readback differs": func(e *stepEnv) {
			e.workload.mutate = func(_ string, d []byte, ok bool) ([]byte, bool) { return append([]byte("x"), d...), ok }
		},
		"baseline readback unavailable": func(e *stepEnv) { e.workload.readErr["upgrade-baseline-002"] = errors.New("no route") },
		"preflight unknown": func(e *stepEnv) {
			facts := goodFacts(t)
			facts.NooutOwned = upgradeUnknown
			e.seq = newSeq(t, facts)
		},
		"sequence halted earlier": func(e *stepEnv) { e.seq.halt(errors.New("earlier")) },
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			env := newStepEnv(t)
			setup(env)
			rc := env.step(nil).run(context.Background())
			if rc.Err == nil {
				t.Fatal("no error")
			}
			if len(env.effects.calls) != 0 {
				t.Fatalf("effects ran before admission: %v", env.effects.calls)
			}
			if len(env.seq.ledger.transitions) != 0 && name != "sequence halted earlier" {
				t.Fatalf("a transition was recorded: %v", env.seq.ledger.transitions)
			}
			if rc.Settled || len(env.receipts) != 1 {
				t.Fatalf("settled=%v receipts=%d", rc.Settled, len(env.receipts))
			}
		})
	}
}

func TestUpgradeMonStepRefusesToStartAWriterOnAnUnprovenStore(t *testing.T) {
	cases := map[string]struct {
		setup func(*stepEnv)
		step  upgradeStep
	}{
		"fence fails":             {func(e *stepEnv) { e.effects.fenceErr = errors.New("stop failed") }, upgradeNotAttempted},
		"fenced another identity": {func(e *stepEnv) { id := ledgerDaemon("mon", "b"); e.effects.fenceID = &id }, upgradeNotAttempted},
		"source still present":    {func(e *stepEnv) { e.effects.present = true }, upgradeFenced},
		"source observation fail": {func(e *stepEnv) { e.effects.presentErr = errors.New("inspect failed") }, upgradeFenced},
		"retained inspect fails":  {func(e *stepEnv) { e.effects.retainedErr = errors.New("inspect failed") }, upgradeOldAbsent},
		"store.db missing":        {func(e *stepEnv) { e.effects.hasStore = false }, upgradeOldAbsent},
		"endpoint differs": {func(e *stepEnv) {
			e.effects.reserved = upgradeMonVector{monAddr("10.0.0.9", 3300, 6789)}
		}, upgradeOldAbsent},
		"endpoint v1 dropped": {func(e *stepEnv) {
			e.effects.reserved = upgradeMonVector{monAddr("10.0.0.2", 3300, 0)}
		}, upgradeOldAbsent},
		"endpoint missing": {func(e *stepEnv) { e.effects.reserved = nil }, upgradeOldAbsent},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			env := newStepEnv(t)
			tc.setup(env)
			rc := env.step(nil).run(context.Background())
			if rc.Err == nil {
				t.Fatal("no error")
			}
			if env.hasCall("launch") || len(rc.Instances) != 0 || len(env.seq.ledger.instances) != 0 {
				t.Fatalf("a destination was started or reserved: calls %v", env.effects.calls)
			}
			if rc.Step != tc.step {
				t.Fatalf("step = %s, want %s", rc.Step, tc.step)
			}
			if env.hasCall("dispose") {
				t.Fatal("disposal ran with nothing owned")
			}
			if _, _, err := env.seq.admit(); err == nil {
				t.Fatal("the halted sequence admitted another transition")
			}
			if len(env.workload.writes) != 2 {
				t.Fatalf("workload wrote after the fault path stopped: %v", env.workload.writes)
			}
			if rc.Settled || len(env.receipts) != 1 {
				t.Fatalf("settled=%v receipts=%d", rc.Settled, len(env.receipts))
			}
		})
	}
}

func TestUpgradeMonStepLaunchErrorIsObservedNeverReplayed(t *testing.T) {
	// The creation call returned an error, but the instance may exist.
	t.Run("native evidence confirms the attempt", func(t *testing.T) {
		env := newStepEnv(t)
		env.effects.launchErr = errors.New("connection reset")
		rc := env.step(nil).run(context.Background())
		if len(env.effects.launches) != 1 {
			t.Fatalf("launches = %v", env.effects.launches)
		}
		if rc.Err == nil || !strings.Contains(rc.Err.Error(), "connection reset") {
			t.Fatalf("first failure = %v", rc.Err)
		}
		if rc.Step != upgradeNativeConfirmed || rc.Settled || env.seq.settled != 0 {
			t.Fatalf("a launch error must not settle: step %s settled %v", rc.Step, rc.Settled)
		}
		if len(env.workload.writes) != 2 {
			t.Fatalf("workload ran after an uncertain launch: %v", env.workload.writes)
		}
		if len(env.effects.disposed) != 1 {
			t.Fatalf("disposed = %v", env.effects.disposed)
		}
	})
	t.Run("observation fails too", func(t *testing.T) {
		env := newStepEnv(t)
		env.effects.launchErr = errors.New("connection reset")
		env.effects.observeErr = errors.New("inspect failed")
		rc := env.step(nil).run(context.Background())
		if len(env.effects.launches) != 1 {
			t.Fatalf("launches = %v", env.effects.launches)
		}
		if rc.Step != upgradeStartAttempted {
			t.Fatalf("step = %s", rc.Step)
		}
		if got := env.seq.ledger.owned(); len(got) != 1 || got[0] != "dst-t1-mon.a" {
			t.Fatalf("owned = %v", got)
		}
		if act, _ := env.seq.ledger.next("t1-mon.a"); act != upgradeActObserveAttempt {
			t.Fatalf("next action = %v", act)
		}
		if len(env.effects.disposed) != 1 || rc.Err == nil || !strings.Contains(rc.Err.Error(), "connection reset") {
			t.Fatalf("disposed %v err %v", env.effects.disposed, rc.Err)
		}
	})
}

func TestUpgradeMonStepRejectsEvidenceThatIsNotTheDestination(t *testing.T) {
	cases := map[string]func(string) upgradeNativeEvidence{
		"same image replacement": func(i string) upgradeNativeEvidence {
			return upgradeNativeEvidence{Instance: i, Version: "19.2.5", FSID: "fsid-1", StoreID: "store-mona"}
		},
		"other store": func(i string) upgradeNativeEvidence {
			return upgradeNativeEvidence{Instance: i, Version: "20.2.4", FSID: "fsid-1", StoreID: "store-new"}
		},
		"other FSID": func(i string) upgradeNativeEvidence {
			return upgradeNativeEvidence{Instance: i, Version: "20.2.4", FSID: "fsid-2", StoreID: "store-mona"}
		},
		"other instance": func(string) upgradeNativeEvidence {
			return upgradeNativeEvidence{Instance: "other", Version: "20.2.4", FSID: "fsid-1", StoreID: "store-mona"}
		},
	}
	for name, ev := range cases {
		t.Run(name, func(t *testing.T) {
			env := newStepEnv(t)
			env.effects.evidence = ev
			rc := env.step(nil).run(context.Background())
			if rc.Err == nil || rc.Step != upgradeStartAttempted || rc.Settled {
				t.Fatalf("step %s settled %v err %v", rc.Step, rc.Settled, rc.Err)
			}
			if len(env.effects.launches) != 1 || len(env.workload.writes) != 2 || len(env.effects.disposed) != 1 {
				t.Fatalf("launches %v writes %v disposed %v", env.effects.launches, env.workload.writes, env.effects.disposed)
			}
		})
	}
}

func TestUpgradeMonStepSettlesOnlyOnFreshMixedVersionObservations(t *testing.T) {
	t.Run("cluster still reports the source version", func(t *testing.T) {
		env := newStepEnv(t)
		env.outputs = obsOutputsFor(t, 0)
		rc := env.step(nil).run(context.Background())
		if rc.Err == nil || rc.Settled || env.seq.settled != 0 {
			t.Fatalf("settled=%v err=%v", rc.Settled, rc.Err)
		}
		if len(env.workload.writes) != 2 {
			t.Fatalf("mixed workload ran without a settled checkpoint: %v", env.workload.writes)
		}
		if len(env.effects.disposed) != 1 {
			t.Fatal("destination not disposed")
		}
	})
	t.Run("cluster reports more destination daemons than one", func(t *testing.T) {
		env := newStepEnv(t)
		env.outputs = obsOutputsFor(t, 2)
		rc := env.step(nil).run(context.Background())
		if rc.Err == nil || rc.Settled {
			t.Fatalf("settled=%v err=%v", rc.Settled, rc.Err)
		}
	})
	t.Run("a probe fails", func(t *testing.T) {
		env := newStepEnv(t)
		s := env.step(nil)
		env.runner.hook = func(i int, _ upgradeCommandRequest, _ *upgradeCommandResult) error {
			if i == 3 {
				return errors.New("exec failed")
			}
			return nil
		}
		rc := s.run(context.Background())
		if rc.Err == nil || !errors.Is(rc.Err, errUpgradeCollectProbe) || rc.Settled {
			t.Fatalf("settled=%v err=%v", rc.Settled, rc.Err)
		}
	})
	t.Run("the observation belongs to an earlier phase", func(t *testing.T) {
		env := newStepEnv(t)
		s := env.step(nil)
		// A later phase supersedes the one the step opened while it collects.
		env.runner.hook = func(i int, _ upgradeCommandRequest, _ *upgradeCommandResult) error {
			if i == 2 {
				env.targets.begin("another")
			}
			return nil
		}
		rc := s.run(context.Background())
		if rc.Err == nil || !errors.Is(rc.Err, errUpgradePhaseClosed) || rc.Settled {
			t.Fatalf("settled=%v err=%v", rc.Settled, rc.Err)
		}
	})
	t.Run("the daemon generation moved during collection", func(t *testing.T) {
		env := newStepEnv(t)
		s := env.step(nil)
		env.runner.hook = func(i int, _ upgradeCommandRequest, _ *upgradeCommandResult) error {
			if i == 5 {
				return env.targets.advance("mon.a")
			}
			return nil
		}
		rc := s.run(context.Background())
		if rc.Err == nil || !errors.Is(rc.Err, errUpgradeStale) || rc.Settled {
			t.Fatalf("settled=%v err=%v", rc.Settled, rc.Err)
		}
	})
}

func TestUpgradeMonStepKeepsUncertainWritesUncertain(t *testing.T) {
	t.Run("unknown write that did not apply", func(t *testing.T) {
		env := newStepEnv(t)
		env.workload.writeErr[3] = errors.New("timeout")
		rc := env.step(nil).run(context.Background())
		if rc.Err != nil {
			t.Fatal(rc.Err)
		}
		if got := rc.Workload.Classes["upgrade-mixed-003"]; got != "unknown-absent" {
			t.Fatalf("class = %s", got)
		}
		if rc.Writes[2].Outcome != upgradeWriteUnknown || len(rc.Workload.Unknown) != 1 {
			t.Fatalf("write 3 = %+v, unknown %v", rc.Writes[2], rc.Workload.Unknown)
		}
		if len(env.workload.writes) != 4 {
			t.Fatalf("the unknown write was replayed: %v", env.workload.writes)
		}
	})
	t.Run("unknown write that did apply is not promoted", func(t *testing.T) {
		env := newStepEnv(t)
		env.workload.writeErr[3] = errors.New("timeout")
		env.workload.applyOnError = true
		rc := env.step(nil).run(context.Background())
		if rc.Err != nil {
			t.Fatal(rc.Err)
		}
		if got := rc.Workload.Classes["upgrade-mixed-003"]; got != "unknown-applied" {
			t.Fatalf("class = %s", got)
		}
		if rc.Writes[2].Outcome != upgradeWriteUnknown {
			t.Fatalf("a readback promoted the record to %s", rc.Writes[2].Outcome)
		}
	})
	t.Run("unknown write with different content", func(t *testing.T) {
		env := newStepEnv(t)
		env.workload.writeErr[3] = errors.New("timeout")
		env.workload.applyOnError = true
		env.workload.mutate = func(o string, d []byte, ok bool) ([]byte, bool) {
			if o == "upgrade-mixed-003" {
				return d[:10], ok
			}
			return d, ok
		}
		rc := env.step(nil).run(context.Background())
		if rc.Err == nil || rc.Workload.Classes["upgrade-mixed-003"] != "unknown-corrupt" {
			t.Fatalf("err %v class %v", rc.Err, rc.Workload.Classes)
		}
	})
	t.Run("rejected write that is present", func(t *testing.T) {
		env := newStepEnv(t)
		env.workload.writeErr[4] = fmt.Errorf("%w: invalid", errUpgradeWriteRejected)
		env.workload.applyOnError = true
		rc := env.step(nil).run(context.Background())
		if rc.Err == nil || rc.Workload.Classes["upgrade-mixed-004"] != "rejected-present" {
			t.Fatalf("err %v class %v", rc.Err, rc.Workload.Classes)
		}
	})
	t.Run("rejected write that is absent", func(t *testing.T) {
		env := newStepEnv(t)
		env.workload.writeErr[4] = fmt.Errorf("%w: invalid", errUpgradeWriteRejected)
		rc := env.step(nil).run(context.Background())
		if rc.Err != nil || rc.Workload.Classes["upgrade-mixed-004"] != "rejected-absent" {
			t.Fatalf("err %v class %v", rc.Err, rc.Workload.Classes)
		}
	})
}

func TestUpgradeMonStepExactDataChecks(t *testing.T) {
	cases := map[string]struct {
		mutate func(string, []byte, bool) ([]byte, bool)
		readE  string
		class  string
	}{
		"baseline object missing after the transition": {mutate: func(o string, d []byte, ok bool) ([]byte, bool) {
			if o == "upgrade-baseline-001" {
				return nil, false
			}
			return d, ok
		}, class: "acknowledged-missing"},
		"one byte flipped": {mutate: func(o string, d []byte, ok bool) ([]byte, bool) {
			if o == "upgrade-mixed-004" {
				c := bytes.Clone(d)
				c[len(c)-1] ^= 1
				return c, ok
			}
			return d, ok
		}, class: "acknowledged-corrupt"},
		"truncated": {mutate: func(o string, d []byte, ok bool) ([]byte, bool) {
			if o == "upgrade-mixed-004" {
				return d[:len(d)-1], ok
			}
			return d, ok
		}, class: "acknowledged-corrupt"},
		"empty but present": {mutate: func(o string, d []byte, ok bool) ([]byte, bool) {
			if o == "upgrade-mixed-004" {
				return []byte{}, true
			}
			return d, ok
		}, class: "acknowledged-corrupt"},
		"older content of the same size": {mutate: func(o string, d []byte, ok bool) ([]byte, bool) {
			if o == "upgrade-mixed-004" {
				c := bytes.Clone(d)
				copy(c, "upgrade-seq=9;")
				return c, ok
			}
			return d, ok
		}, class: "acknowledged-corrupt"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			env := newStepEnv(t)
			calls := 0
			env.workload.mutate = func(o string, d []byte, ok bool) ([]byte, bool) {
				// Leave the pre-fault verification untouched.
				if len(env.effects.calls) == 0 {
					return d, ok
				}
				calls++
				return tc.mutate(o, d, ok)
			}
			rc := env.step(nil).run(context.Background())
			if rc.Err == nil || rc.Workload.OK() {
				t.Fatalf("err %v verdict %+v", rc.Err, rc.Workload)
			}
			found := false
			for _, c := range rc.Workload.Classes {
				found = found || c == tc.class
			}
			if !found {
				t.Fatalf("no object classified %s: %v", tc.class, rc.Workload.Classes)
			}
			if !rc.Settled || len(env.effects.disposed) != 1 {
				t.Fatalf("settled=%v disposed=%v", rc.Settled, env.effects.disposed)
			}
		})
	}
	t.Run("readback unavailable is inconclusive, not a pass", func(t *testing.T) {
		env := newStepEnv(t)
		s := env.step(nil)
		env.workload.readErr["upgrade-mixed-003"] = errors.New("no route")
		rc := s.run(context.Background())
		if rc.Err == nil || rc.Workload.OK() || len(rc.Workload.Inconclusive) != 1 || len(rc.Workload.Violations) != 0 {
			t.Fatalf("err %v verdict %+v", rc.Err, rc.Workload)
		}
	})
	t.Run("presence and data disagree", func(t *testing.T) {
		env := newStepEnv(t)
		env.workload.mutate = func(o string, d []byte, ok bool) ([]byte, bool) {
			if o == "upgrade-mixed-003" && len(env.effects.calls) > 0 {
				return nil, true // present with null data
			}
			return d, ok
		}
		rc := env.step(nil).run(context.Background())
		if rc.Err == nil || len(rc.Workload.Inconclusive) != 1 || rc.Workload.Classes["upgrade-mixed-003"] != "readback-malformed" {
			t.Fatalf("err %v verdict %+v", rc.Err, rc.Workload)
		}
	})
}

func TestUpgradeMonStepReceiptPrecedesCleanupAndKeepsFailuresApart(t *testing.T) {
	env := newStepEnv(t)
	env.effects.disposeErr = errors.New("remove failed")
	order := []string{}
	env.effects.onDispose = func() { order = append(order, fmt.Sprintf("dispose after %d receipts", len(env.receipts))) }
	env.workload.writeErr[3] = errors.New("timeout") // unknown, not a failure
	env.effects.launchErr = nil
	s := env.step(func(c *upgradeMonStepConfig) {
		prev := c.OnReceipt
		c.OnReceipt = func(r upgradeMonReceipt) {
			prev(r)
			order = append(order, "receipt")
			if r.CleanupErr != nil || len(r.Instances) != 1 {
				t.Errorf("receipt = %+v", r)
			}
		}
	})
	rc := s.run(context.Background())
	if strings.Join(order, ",") != "receipt,dispose after 1 receipts" {
		t.Fatalf("order = %v", order)
	}
	if rc.Err != nil || rc.CleanupErr == nil {
		t.Fatalf("err %v cleanup %v", rc.Err, rc.CleanupErr)
	}
	// A failure and a cleanup failure stay separate values.
	env2 := newStepEnv(t)
	env2.effects.disposeErr = errors.New("remove failed")
	env2.effects.launchErr = errors.New("connection reset")
	rc2 := env2.step(nil).run(context.Background())
	if rc2.Err == nil || rc2.CleanupErr == nil || strings.Contains(rc2.Err.Error(), "remove failed") || strings.Contains(rc2.CleanupErr.Error(), "connection reset") {
		t.Fatalf("err %v cleanup %v", rc2.Err, rc2.CleanupErr)
	}
}

func TestUpgradeMonStepCleanupSurvivesCancellation(t *testing.T) {
	env := newStepEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	env.effects.launchErr = errors.New("canceled")
	env.effects.onDispose = func() {}
	s := env.step(nil)
	// Cancel once the destination was attempted.
	env.effects.evidence = func(i string) upgradeNativeEvidence {
		cancel()
		return upgradeNativeEvidence{Instance: i, Version: "20.2.4", FSID: "fsid-1", StoreID: "store-mona"}
	}
	rc := s.run(ctx)
	if rc.Err == nil || len(env.effects.disposed) != 1 || !env.effects.disposeCtxOK {
		t.Fatalf("err %v disposed %v live ctx %v", rc.Err, env.effects.disposed, env.effects.disposeCtxOK)
	}
}

func TestUpgradeMonStepCancelledCollectionIsNotSettled(t *testing.T) {
	env := newStepEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	s := env.step(nil)
	env.runner.hook = func(i int, _ upgradeCommandRequest, _ *upgradeCommandResult) error {
		if i == 1 {
			cancel()
		}
		return nil
	}
	rc := s.run(ctx)
	if rc.Err == nil || !errors.Is(rc.Err, errUpgradeCollectDeadline) || rc.Settled {
		t.Fatalf("settled=%v err=%v", rc.Settled, rc.Err)
	}
	if len(env.effects.disposed) != 1 {
		t.Fatal("not disposed")
	}
}

func TestUpgradeMonStepDoesNotAdvanceGenerationBeforeAttempt(t *testing.T) {
	env := newStepEnv(t)
	env.effects.present = true // stops before the start attempt
	env.step(nil).run(context.Background())
	if g := env.targets.generation["mon.a"]; g != 1 {
		t.Fatalf("generation = %d before any destination existed", g)
	}
	env2 := newStepEnv(t)
	env2.step(nil).run(context.Background())
	if g := env2.targets.generation["mon.a"]; g != 2 {
		t.Fatalf("generation = %d after the attempt", g)
	}
}

func TestUpgradeMonStepRejectsMisconfiguration(t *testing.T) {
	env := newStepEnv(t)
	var nilEffects *stepEffects
	var nilWorkload *stepWorkload
	cases := map[string]func(*upgradeMonStepConfig){
		"no sequence":        func(c *upgradeMonStepConfig) { c.Sequence = nil },
		"no targets":         func(c *upgradeMonStepConfig) { c.Targets = nil },
		"no handoff":         func(c *upgradeMonStepConfig) { c.Handoff = nil },
		"no collector":       func(c *upgradeMonStepConfig) { c.Collector = nil },
		"nil effects":        func(c *upgradeMonStepConfig) { c.Effects = nil },
		"typed nil effects":  func(c *upgradeMonStepConfig) { c.Effects = nilEffects },
		"nil workload":       func(c *upgradeMonStepConfig) { c.Workload = nil },
		"typed nil workload": func(c *upgradeMonStepConfig) { c.Workload = nilWorkload },
		"no baseline":        func(c *upgradeMonStepConfig) { c.Baseline = 0 },
		"no mixed writes":    func(c *upgradeMonStepConfig) { c.Mixed = 0 },
		"plan not MON-first": func(c *upgradeMonStepConfig) {
			s := *c.Sequence
			s.plan = s.plan[3:]
			c.Sequence = &s
		},
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic instead of an error: %v", r)
				}
			}()
			base := env.step(nil).cfg
			edit(&base)
			if _, err := newUpgradeMonStep(base); !errors.Is(err, errUpgradeStepConfig) {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

// snapshotStepEffects captures every observable side effect of a step.
func (e *stepEnv) effectsSnapshot() string {
	return fmt.Sprintf("writes=%v reads=%v calls=%v launches=%v disposed=%v collector=%d receipts=%d ledger=%d/%d/%d seq=%d/%d gen=%v",
		e.workload.writes, e.workload.reads, e.effects.calls, e.effects.launches, e.effects.disposed, len(e.runner.calls), len(e.receipts),
		len(e.seq.ledger.transitions), len(e.seq.ledger.instances), len(e.seq.ledger.owned()), e.seq.began, e.seq.settled, e.targets.generation)
}

func TestUpgradeMonStepRunsOnlyOnce(t *testing.T) {
	outcomes := map[string]func(*stepEnv) context.Context{
		"success": func(*stepEnv) context.Context { return context.Background() },
		"unknown baseline write": func(e *stepEnv) context.Context {
			e.workload.writeErr[1] = errors.New("timeout")
			return context.Background()
		},
		"unknown baseline write that applied": func(e *stepEnv) context.Context {
			e.workload.writeErr[1] = errors.New("timeout")
			e.workload.applyOnError = true
			return context.Background()
		},
		"launch error": func(e *stepEnv) context.Context {
			e.effects.launchErr = errors.New("connection reset")
			return context.Background()
		},
		"cancelled before the first write": func(*stepEnv) context.Context {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx
		},
		"halted by the source still present": func(e *stepEnv) context.Context {
			e.effects.present = true
			return context.Background()
		},
	}
	for name, setup := range outcomes {
		t.Run(name, func(t *testing.T) {
			env := newStepEnv(t)
			ctx := setup(env)
			s := env.step(nil)
			first := s.run(ctx)
			saved, ok := s.firstReceipt()
			if !ok {
				t.Fatal("no receipt kept for the first run")
			}
			before := env.effectsSnapshot()
			for i := 0; i < 2; i++ {
				// Also with a live context and after the failure was fixed.
				env.workload.writeErr = map[int]error{}
				env.effects.present, env.effects.launchErr = false, nil
				rc := s.run(context.Background())
				if !errors.Is(rc.Err, errUpgradeStepConsumed) || rc.Settled || len(rc.Events) != 0 || len(rc.Instances) != 0 || rc.Transition != "" {
					t.Fatalf("repeated run = %+v, want only a consumed refusal", rc)
				}
				if after := env.effectsSnapshot(); after != before {
					t.Fatalf("repeated run had side effects:\nbefore %s\nafter  %s", before, after)
				}
			}
			kept, _ := s.firstReceipt()
			if fmt.Sprint(kept) != fmt.Sprint(saved) || fmt.Sprint(kept.Events) != fmt.Sprint(first.Events) || fmt.Sprint(kept.Err) != fmt.Sprint(first.Err) {
				t.Fatalf("original receipt was replaced:\n%+v\n%+v", saved, kept)
			}
			if len(env.receipts) != 1 {
				t.Fatalf("receipt callback ran %d times", len(env.receipts))
			}
		})
	}
	t.Run("the original unknown write stays unknown in the kept receipt", func(t *testing.T) {
		env := newStepEnv(t)
		env.workload.writeErr[1] = errors.New("timeout")
		s := env.step(nil)
		s.run(context.Background())
		s.run(context.Background())
		kept, _ := s.firstReceipt()
		if len(kept.Writes) != 1 || kept.Writes[0].Outcome != upgradeWriteUnknown || len(env.workload.writes) != 1 {
			t.Fatalf("writes = %+v submitted %v", kept.Writes, env.workload.writes)
		}
	})
	t.Run("concurrent calls run once", func(t *testing.T) {
		env := newStepEnv(t)
		s := env.step(nil)
		const n = 8
		results := make(chan upgradeMonReceipt, n)
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); results <- s.run(context.Background()) }()
		}
		wg.Wait()
		close(results)
		ran, refused := 0, 0
		for rc := range results {
			if errors.Is(rc.Err, errUpgradeStepConsumed) {
				refused++
			} else {
				ran++
			}
		}
		if ran != 1 || refused != n-1 || len(env.effects.launches) != 1 || len(env.workload.writes) != 4 {
			t.Fatalf("ran %d refused %d launches %v writes %v", ran, refused, env.effects.launches, env.workload.writes)
		}
	})
	t.Run("a new step over the same sequence cannot replay the transition", func(t *testing.T) {
		env := newStepEnv(t)
		env.step(nil).run(context.Background())
		before := len(env.effects.launches)
		rc := env.step(nil).run(context.Background())
		if rc.Err == nil || len(env.effects.launches) != before || env.hasCallAfterFence() {
			t.Fatalf("err %v launches %v", rc.Err, env.effects.launches)
		}
	})
}

// hasCallAfterFence reports more than one fence, i.e. a second writer attempt.
func (e *stepEnv) hasCallAfterFence() bool {
	n := 0
	for _, c := range e.effects.calls {
		if c == "fence" {
			n++
		}
	}
	return n > 1
}

// The ledger and handoff are the other entry points with side effects. Each
// refuses a repeated call and keeps its state.
func TestUpgradeMonStepLedgerAndHandoffRefuseRepeats(t *testing.T) {
	l := newTestLedger(t)
	d := ledgerDaemon("mon", "a")
	ledgerOK(t, l.begin(ledgerBegin("t", d)))
	repeat := func(name string, f func() error, call func() error) {
		t.Helper()
		ledgerOK(t, f())
		before, _ := l.stepOf("t")
		if err := call(); err == nil {
			t.Fatalf("%s: repeated call accepted", name)
		}
		if after, _ := l.stepOf("t"); after != before {
			t.Fatalf("%s: repeated call moved %s to %s", name, before, after)
		}
	}
	ledgerOK(t, func() error {
		if err := l.begin(ledgerBegin("t2", d)); err == nil {
			return errors.New("second transition for the same daemon accepted")
		}
		return nil
	}())
	repeat("fence", func() error { return l.fence("t", d) }, func() error { return l.fence("t", d) })
	repeat("oldAbsent", func() error { return l.oldAbsent("t", false, nil) }, func() error { return l.oldAbsent("t", false, nil) })
	repeat("startAttempted", func() error { return l.startAttempted("t", "dst") }, func() error { return l.startAttempted("t", "dst2") })
	ev := upgradeNativeEvidence{Instance: "dst", Version: "20.2.4", FSID: d.FSID, StoreID: d.StoreID}
	repeat("confirm", func() error { return l.confirm("t", ev) }, func() error { return l.confirm("t", ev) })
	if act, _ := l.next("t"); act != upgradeActNone {
		t.Fatalf("next action after confirmation = %v", act)
	}
	h, err := newUpgradeMonHandoff(retainedMon())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.admit(launchFor(retainedMon())); err != nil {
		t.Fatal(err)
	}
	if _, err := h.admit(launchFor(retainedMon())); err == nil {
		t.Fatal("handoff admitted a second writer for one transition")
	}
	s := newSeq(t, goodFacts(t))
	if _, _, err := s.admit(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.admit(); err == nil {
		t.Fatal("sequence admitted a second transition before the first settled")
	}
}
