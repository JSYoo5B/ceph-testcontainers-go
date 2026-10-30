//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

// outageFakeRados stands in for the rados module with a directory as the
// object store, so that the retained client's bookkeeping and the readback can
// be checked without a cluster, and across processes. It can inject an error
// before or after an object is stored, hang an operation, lose a stored object
// or store other bytes. It checks the ledger and the oracle, not librados or
// Ceph behavior.
const outageFakeRados = `import os, time

STORE = os.environ["FAKE_STORE"]
FAIL = int(os.environ.get("FAKE_SEQ", "-1"))
MODE = os.environ.get("FAKE_MODE", "")

class ObjectNotFound(Exception): pass

class Ioctx:
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def write_full(self, name, data):
        seq = int(name.rsplit("-", 1)[1]) if name.startswith("op-") else -1
        target = os.path.join(STORE, name)
        def store(payload):
            with open(target, "wb") as f: f.write(payload)
        if seq == FAIL and MODE == "before": raise RuntimeError("timed out")
        if seq == FAIL and MODE == "after": store(data); raise RuntimeError("timed out")
        if seq == FAIL and MODE == "hang": time.sleep(120)
        if seq == FAIL and MODE == "lose": return
        if seq == FAIL and MODE == "corrupt": store(data[:-1] + b"?"); return
        store(data)
    def stat(self, name):
        try: return os.path.getsize(os.path.join(STORE, name)), None
        except FileNotFoundError: raise ObjectNotFound(name)
    def read(self, name, length, offset):
        try:
            with open(os.path.join(STORE, name), "rb") as f: return f.read(length)
        except FileNotFoundError: raise ObjectNotFound(name)

class Rados:
    def __init__(self, **kw): pass
    def connect(self): pass
    def get_instance_id(self): return 4123
    def open_ioctx(self, pool): return Ioctx()
    def shutdown(self): pass
`

type outageFake struct {
	t      *testing.T
	dir    string
	ledger string
	env    []string
}

func newOutageFake(t *testing.T, env ...string) *outageFake {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("the ledger fixture needs python3")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "rados.py"), []byte(outageFakeRados), 0o600); err != nil {
		t.Fatal(err)
	}
	store := filepath.Join(dir, "store")
	if err := os.Mkdir(store, 0o700); err != nil {
		t.Fatal(err)
	}
	return &outageFake{t: t, dir: dir, ledger: filepath.Join(dir, "ledger"), env: append([]string{"FAKE_STORE=" + store}, env...)}
}

func (f *outageFake) command(mode string, extra ...string) *exec.Cmd {
	args := append([]string{"-I", "-c", "import sys; sys.path.insert(0, sys.argv.pop(1)); exec(sys.argv.pop(1))", f.dir, outageLedgerScript, mode, "pool", f.ledger, "fake-run"}, extra...)
	cmd := exec.Command("python3", args...)
	cmd.Env = append(os.Environ(), f.env...)
	return cmd
}

func (f *outageFake) output(mode string, extra ...string) []byte {
	f.t.Helper()
	out, err := f.command(mode, extra...).CombinedOutput()
	if err != nil {
		f.t.Fatalf("%s: %v: %s", mode, err, out)
	}
	return out
}

func (f *outageFake) token(value string) { f.t.Helper(); f.output("token", value) }

func (f *outageFake) Events() []outageEvent {
	f.t.Helper()
	var events []outageEvent
	if err := json.Unmarshal(f.output("ledger"), &events); err != nil {
		f.t.Fatal(err)
	}
	return events
}

func (f *outageFake) ops() []outageOp {
	f.t.Helper()
	ops, err := outageOps(f.Events(), "fake-run")
	if err != nil {
		f.t.Fatal(err)
	}
	return ops
}

// start runs the retained client. The returned function ends it with a stop
// token and waits for a clean exit.
func (f *outageFake) start() (*exec.Cmd, chan error) {
	f.t.Helper()
	cmd := f.command("actor")
	if err := cmd.Start(); err != nil {
		f.t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	f.t.Cleanup(func() { _ = cmd.Process.Kill() })
	return cmd, done
}

// waitFor polls the ledger until check accepts its operations.
func (f *outageFake) waitFor(what string, check func([]outageOp) bool) []outageOp {
	f.t.Helper()
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		if _, err := os.Stat(filepath.Join(f.ledger, "ledger.jsonl")); err == nil {
			if ops := f.ops(); check(ops) {
				return ops
			}
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func (f *outageFake) readback(ops []outageOp) map[string]outageObject {
	f.t.Helper()
	var objects map[string]outageObject
	if err := json.Unmarshal(f.output("read", outageNames(ops)), &objects); err != nil {
		f.t.Fatal(err)
	}
	return objects
}

func acknowledged(key string, want int) func([]outageOp) bool {
	return func(ops []outageOp) bool { return outageTally(ops)[key] >= want }
}

// TestCorrelatedLedgerFollowsPhaseTokens drives the real actor script through
// every phase of an outage. Each operation carries the token it began under and
// the token it returned under, and only operations that kept the "outage" token
// are credited to the fully applied outage.
func TestCorrelatedLedgerFollowsPhaseTokens(t *testing.T) {
	fake := newOutageFake(t)
	fake.token(outagePhaseBaseline)
	_, done := fake.start()
	fake.waitFor("baseline", acknowledged("baseline/acked", 2))
	fake.token(outagePhaseApplying)
	fake.waitFor("applying", acknowledged("applying/acked", 2))
	fake.token(outagePhaseOutage)
	fake.waitFor("outage", acknowledged("outage/acked", 2))
	fake.token(outagePhaseRestoring)
	fake.waitFor("restoring", acknowledged("restoring/acked", 1))
	fake.token(outagePhaseRecovered)
	fake.waitFor("recovered", acknowledged("recovered/acked", 1))
	fake.token(outagePhaseStop)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("the actor did not end cleanly:", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the actor ignored the stop token")
	}
	events := fake.Events()
	ops, err := outageOps(events, "fake-run")
	if err != nil {
		t.Fatal(err)
	}
	if events[len(events)-1].Event != "stopped" || events[len(events)-1].Ops != len(ops) {
		t.Fatalf("the actor did not report its end: %+v", events[len(events)-1])
	}
	order := []string{outagePhaseBaseline, outagePhaseApplying, outagePhaseOutage, outagePhaseRestoring, outagePhaseRecovered}
	last := 0
	sessions := 0
	for _, event := range events {
		if event.Event == "session" {
			sessions++
		}
	}
	for _, op := range ops {
		at := slices.Index(order, op.Phase)
		if at < last {
			t.Fatalf("operation %d began under %q after a later token", op.Seq, op.Phase)
		}
		last = at
		if op.Outcome() != "acked" || op.IntentNS >= op.ReturnNS {
			t.Fatalf("unexpected record of %+v", op)
		}
	}
	// One Rados instance and one process wrote every record.
	if sessions != 1 {
		t.Fatalf("%d sessions, want one retained client", sessions)
	}
	unconfirmed, err := outageVerify(ops, fake.readback(ops))
	if err != nil || unconfirmed != 0 {
		t.Fatalf("independent readback: %d %v", unconfirmed, err)
	}
}

// TestCorrelatedLedgerConditionalError reproduces the reporting boundary of a
// whole-command check: an error on one operation after the call applied it and
// an error before it did produce the same failure record but different object
// state. The ledger keeps the operation as failed, later operations continue
// under new IDs, nothing is replayed, and the readback is not promoted into an
// acknowledgement.
func TestCorrelatedLedgerConditionalError(t *testing.T) {
	var records [2][]outageOp
	var unconfirmed [2]int
	for i, mode := range []string{"before", "after"} {
		fake := newOutageFake(t, "FAKE_SEQ=2", "FAKE_MODE="+mode)
		fake.token(outagePhaseOutage)
		_, done := fake.start()
		fake.waitFor("operations after the error", func(ops []outageOp) bool { return len(ops) >= 5 && ops[4].Returned })
		fake.token(outagePhaseStop)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		ops := fake.ops()
		intents := map[string]bool{}
		for _, op := range ops {
			if intents[op.Op] {
				t.Fatalf("%s: operation %s submitted twice", mode, op.Op)
			}
			intents[op.Op] = true
		}
		if ops[2].Outcome() != "failed" || !strings.Contains(ops[2].Error, "timed out") || ops[3].Outcome() != "acked" || ops[4].Outcome() != "acked" {
			t.Fatalf("%s: records %+v", mode, ops[:5])
		}
		var err error
		if unconfirmed[i], err = outageVerify(ops, fake.readback(ops)); err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		records[i] = ops
	}
	// The two runs are indistinguishable in the ledger and different in state.
	a, b := records[0][2], records[1][2]
	if a.Outcome() != b.Outcome() || a.Error != b.Error || a.Phase != b.Phase || a.Object != b.Object {
		t.Fatalf("the failure records differ: %+v %+v", a, b)
	}
	if unconfirmed != [2]int{0, 1} {
		t.Fatalf("unconfirmed operations that took effect: %v, want [0 1]", unconfirmed)
	}
}

// TestCorrelatedLedgerInFlightOperation ends the actor while an operation is
// in flight. The intent stays, with no return, and is neither a success nor a
// failure; it is not replayed and not promoted by a later read.
func TestCorrelatedLedgerInFlightOperation(t *testing.T) {
	fake := newOutageFake(t, "FAKE_SEQ=2", "FAKE_MODE=hang")
	fake.token(outagePhaseOutage)
	cmd, done := fake.start()
	fake.waitFor("the hung operation", func(ops []outageOp) bool { return len(ops) == 3 })
	time.Sleep(300 * time.Millisecond)
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil {
		t.Fatal("a killed actor reported a clean exit")
	}
	ops := fake.ops()
	if len(ops) != 3 || ops[2].Outcome() != "inflight" || ops[0].Outcome() != "acked" || ops[1].Outcome() != "acked" {
		t.Fatalf("records %+v", ops)
	}
	if tally := outageTally(ops); tally["outage/inflight"] != 1 || tally["outage/acked"] != 2 {
		t.Fatalf("tally %v", tally)
	}
	unconfirmed, err := outageVerify(ops, fake.readback(ops))
	if err != nil || unconfirmed != 0 {
		t.Fatalf("in-flight readback: %d %v", unconfirmed, err)
	}
	// No stop record: the actor did not get to report one.
	for _, event := range fake.Events() {
		if event.Event == "stopped" {
			t.Fatal("a killed actor reported a clean stop")
		}
	}
}

// TestCorrelatedLedgerReadbackRefusals proves that the independent oracle
// rejects an acknowledged write that is gone and one that holds other bytes.
func TestCorrelatedLedgerReadbackRefusals(t *testing.T) {
	for _, mode := range []string{"lose", "corrupt"} {
		fake := newOutageFake(t, "FAKE_SEQ=1", "FAKE_MODE="+mode)
		fake.token(outagePhaseOutage)
		_, done := fake.start()
		fake.waitFor("operations", func(ops []outageOp) bool { return len(ops) >= 3 && ops[2].Returned })
		fake.token(outagePhaseStop)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		ops := fake.ops()
		if ops[1].Outcome() != "acked" {
			t.Fatalf("%s: the actor must see a success: %+v", mode, ops[1])
		}
		if _, err := outageVerify(ops, fake.readback(ops)); err == nil {
			t.Fatalf("%s: an acknowledged write that is lost or altered passed the readback", mode)
		}
	}
}

func TestCorrelatedLedgerCredit(t *testing.T) {
	for name, tc := range map[string]struct {
		op   outageOp
		want string
	}{
		"baseline":                {outageOp{Phase: "baseline", PhaseAfter: "baseline", Returned: true}, "baseline/acked"},
		"begun while applying":    {outageOp{Phase: "applying", PhaseAfter: "outage", Returned: true}, "applying/acked"},
		"outage kept the token":   {outageOp{Phase: "outage", PhaseAfter: "outage", Returned: true}, "outage/acked"},
		"outage ended in restore": {outageOp{Phase: "outage", PhaseAfter: "restoring", Returned: true}, "outage-overlapping-acked"},
		"outage failed":           {outageOp{Phase: "outage", PhaseAfter: "outage", Returned: true, Error: "x"}, "outage/failed"},
		"outage inflight":         {outageOp{Phase: "outage"}, "outage/inflight"},
		"overlapping failure":     {outageOp{Phase: "outage", PhaseAfter: "restoring", Returned: true, Error: "x"}, "outage-overlapping-failed"},
	} {
		if got := tc.op.Credit(); got != tc.want {
			t.Errorf("%s: %s, want %s", name, got, tc.want)
		}
	}
}

func TestCorrelatedLedgerRefusals(t *testing.T) {
	op := func(seq int) (string, string, int) {
		id := fmt.Sprintf("r-%05d", seq)
		return id, outageDigest(outagePayload(id, seq)), len(outagePayload(id, seq))
	}
	intent := func(seq int, pid int) outageEvent {
		id, sum, size := op(seq)
		return outageEvent{Event: "intent", Run: "r", PID: pid, Op: id, Seq: seq, Object: "op-" + id, SHA256: sum, Bytes: size, Phase: "outage"}
	}
	session := outageEvent{Event: "session", Run: "r", PID: 7, Client: "client.1"}
	returned := func(seq int) outageEvent {
		id, _, _ := op(seq)
		return outageEvent{Event: "return", Run: "r", PID: 7, Op: id, PhaseAfter: "outage"}
	}
	bad := intent(0, 7)
	bad.SHA256 = "00"
	foreign := intent(0, 7)
	foreign.Run = "other"
	for name, tc := range map[string]struct {
		events []outageEvent
		ok     bool
	}{
		"in flight then stopped": {[]outageEvent{session, intent(0, 7), returned(0), intent(1, 7), {Event: "stopped", Run: "r", PID: 7, Ops: 2}}, true},
		"complete":               {[]outageEvent{session, intent(0, 7), returned(0), intent(1, 7), returned(1), {Event: "stopped", Run: "r", PID: 7, Ops: 2}}, true},
		"in flight":              {[]outageEvent{session, intent(0, 7), returned(0), intent(1, 7)}, true},
		"digest differs":         {[]outageEvent{session, bad}, false},
		"foreign run":            {[]outageEvent{session, foreign}, false},
		"second process":         {[]outageEvent{session, intent(0, 7), returned(0), intent(1, 8)}, false},
		"second session":         {[]outageEvent{session, session}, false},
		"intent before session":  {[]outageEvent{intent(0, 7)}, false},
		"repeated operation":     {[]outageEvent{session, intent(0, 7), returned(0), intent(0, 7)}, false},
		"skipped operation":      {[]outageEvent{session, intent(1, 7)}, false},
		"return without intent":  {[]outageEvent{session, returned(0)}, false},
		"return twice":           {[]outageEvent{session, intent(0, 7), returned(0), returned(0)}, false},
		"unknown record":         {[]outageEvent{session, {Event: "retry", Run: "r", PID: 7}}, false},
		"stop count disagrees":   {[]outageEvent{session, intent(0, 7), returned(0), {Event: "stopped", Run: "r", PID: 7, Ops: 5}}, false},
		"token records excluded": {[]outageEvent{{Event: "token", Run: "r", PID: 99, Value: "outage"}, session, intent(0, 7), returned(0)}, true},
	} {
		if _, err := outageOps(tc.events, "r"); (err == nil) != tc.ok {
			t.Errorf("%s: error=%v, want valid=%t", name, err, tc.ok)
		}
	}
}

// outageFakeDaemon records the calls of a plan.
type outageFakeDaemon struct {
	name              string
	log               *[]string
	stopErr, startErr error
	running           bool
	staysUp, staysOff bool
}

func (d *outageFakeDaemon) Stop(context.Context, *time.Duration) error {
	*d.log = append(*d.log, "stop "+d.name)
	if d.stopErr != nil {
		return d.stopErr
	}
	d.running = d.staysUp
	return nil
}

func (d *outageFakeDaemon) Start(context.Context) error {
	*d.log = append(*d.log, "start "+d.name)
	if d.startErr != nil {
		return d.startErr
	}
	d.running = !d.staysOff
	return nil
}

func (d *outageFakeDaemon) Running(context.Context) (bool, error) { return d.running, nil }

func outageFakePlan(daemons ...*outageFakeDaemon) (*outagePlan, []outageMember, *time.Duration) {
	var members []outageMember
	for i, d := range daemons {
		id := fmt.Sprintf("container-%d", i)
		d.running = true
		members = append(members, outageMember{Role: "mon", Name: d.name, Host: "host-a", Container: id, daemon: d, current: func() string { return id }})
	}
	clock := new(time.Duration)
	return newOutagePlan(func() time.Duration { *clock += 10 * time.Millisecond; return *clock }, members), members, clock
}

func TestOutagePlanAppliesAndRestoresInOrder(t *testing.T) {
	var log []string
	plan, _, _ := outageFakePlan(&outageFakeDaemon{name: "a", log: &log}, &outageFakeDaemon{name: "b", log: &log}, &outageFakeDaemon{name: "c", log: &log})
	if err := plan.Apply(t.Context(), time.Second); err != nil {
		t.Fatal(err)
	}
	interval, ok := plan.AppliedInterval()
	if !ok || interval <= 0 {
		t.Fatalf("interval %v %v", interval, ok)
	}
	for _, r := range plan.Receipts {
		if !r.Applied || r.RequestedMS < 0 || r.AppliedMS < r.RequestedMS || r.ObservedMS != -1 {
			t.Fatalf("receipt %+v", r)
		}
	}
	// The stops are sequential, so no later request starts before the earlier one is applied.
	if plan.Receipts[1].RequestedMS < plan.Receipts[0].AppliedMS || plan.Receipts[2].RequestedMS < plan.Receipts[1].AppliedMS {
		t.Fatalf("stops overlapped: %+v", plan.Receipts)
	}
	plan.Observe("mon", "b")
	if plan.Receipts[1].ObservedMS < plan.Receipts[1].AppliedMS || plan.Receipts[0].ObservedMS != -1 {
		t.Fatalf("observation not attributed to its target: %+v", plan.Receipts)
	}
	if err := plan.Restore(t.Context(), "primary"); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(log, ","); got != "stop a,stop b,stop c,start c,start b,start a" {
		t.Fatalf("calls %s", got)
	}
}

func TestOutagePlanPartialApplicationIsStillRestored(t *testing.T) {
	var log []string
	boom := errors.New("daemon refused to stop")
	plan, _, _ := outageFakePlan(&outageFakeDaemon{name: "a", log: &log}, &outageFakeDaemon{name: "b", log: &log, stopErr: boom}, &outageFakeDaemon{name: "c", log: &log})
	err := plan.Apply(t.Context(), time.Second)
	if !errors.Is(err, boom) {
		t.Fatalf("the original stop error was not preserved: %v", err)
	}
	if r := plan.Receipts; !r[0].Applied || r[1].Applied || !r[1].Attempted || r[1].StopError == "" || r[2].Attempted || r[2].RequestedMS != -1 {
		t.Fatalf("receipts of a partly applied plan: %+v %+v %+v", r[0], r[1], r[2])
	}
	if _, ok := plan.AppliedInterval(); ok {
		t.Fatal("one applied stop is not a correlated interval")
	}
	if err := plan.Restore(t.Context(), "primary"); err != nil {
		t.Fatal(err)
	}
	// The target whose stop failed has an uncertain state and is started too;
	// the one that was never attempted is left alone.
	if got := strings.Join(log, ","); got != "stop a,stop b,start b,start a" {
		t.Fatalf("calls %s", got)
	}
}

func TestOutagePlanFailedRestoreKeepsEveryError(t *testing.T) {
	var log []string
	first, second := errors.New("start a failed"), errors.New("start c failed")
	a := &outageFakeDaemon{name: "a", log: &log, startErr: first}
	c := &outageFakeDaemon{name: "c", log: &log, startErr: second}
	plan, _, _ := outageFakePlan(a, &outageFakeDaemon{name: "b", log: &log}, c)
	if err := plan.Apply(t.Context(), time.Second); err != nil {
		t.Fatal(err)
	}
	err := plan.Restore(t.Context(), "primary")
	if !errors.Is(err, first) || !errors.Is(err, second) {
		t.Fatalf("restore lost an original error: %v", err)
	}
	// A failing restore does not skip the targets after it.
	if got := strings.Join(log, ","); got != "stop a,stop b,stop c,start c,start b,start a" {
		t.Fatalf("calls %s", got)
	}
	if r := plan.Receipts; r[0].Restored || r[0].RestoreError == "" || !r[1].Restored || r[2].Restored {
		t.Fatalf("receipts %+v %+v %+v", r[0], r[1], r[2])
	}
	// A second restore retries only what is still stopped.
	log = nil
	a.startErr, c.startErr = nil, nil
	if err := plan.Restore(t.Context(), "primary"); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(log, ","); got != "start c,start a" {
		t.Fatalf("retry calls %s", got)
	}
}

func TestOutagePlanRefusesReplacedGenerationAndFalseStops(t *testing.T) {
	var log []string
	_, members, _ := outageFakePlan(&outageFakeDaemon{name: "a", log: &log}, &outageFakeDaemon{name: "b", log: &log}, &outageFakeDaemon{name: "c", log: &log})
	members[1].current = func() string { return "container-replacement" }
	plan := newOutagePlan(func() time.Duration { return 0 }, members)
	err := plan.Apply(t.Context(), time.Second)
	if err == nil || !strings.Contains(err.Error(), "generation") || strings.Join(log, ",") != "stop a" || plan.Receipts[1].Attempted {
		t.Fatalf("a replaced container was stopped: %v %v", err, log)
	}
	// A stop that returns while the container still runs is not an applied stop.
	log = nil
	plan, _, _ = outageFakePlan(&outageFakeDaemon{name: "a", log: &log, staysUp: true})
	if err := plan.Apply(t.Context(), time.Second); err == nil || plan.Receipts[0].Applied || !plan.Receipts[0].Attempted {
		t.Fatalf("a running container counted as stopped: %v", err)
	}
	// Likewise a start that leaves the container down.
	log = nil
	plan, _, _ = outageFakePlan(&outageFakeDaemon{name: "a", log: &log, staysOff: true})
	if err := plan.Apply(t.Context(), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := plan.Restore(t.Context(), "primary"); err == nil || plan.Receipts[0].Restored {
		t.Fatalf("a container that is still down counted as restored: %v", err)
	}
}

func TestOutageManifestRefusals(t *testing.T) {
	daemon := &outageFakeDaemon{name: "x", log: new([]string)}
	member := func(role, name, host, container string) outageMember {
		return outageMember{Role: role, Name: name, Host: host, Container: container, daemon: daemon, current: func() string { return container }}
	}
	hosts := []string{"host-a", "host-b"}
	owned := map[string]bool{"c1": true, "c2": true, "c3": true}
	for name, tc := range map[string]struct {
		members []outageMember
		ok      bool
	}{
		"valid":                 {[]outageMember{member("mon", "a", "host-a", "c1"), member("osd", "0", "host-a", "c2"), member("mon", "b", "host-b", "c3")}, true},
		"duplicate identity":    {[]outageMember{member("mon", "a", "host-a", "c1"), member("mon", "a", "host-b", "c2")}, false},
		"duplicate container":   {[]outageMember{member("mon", "a", "host-a", "c1"), member("osd", "0", "host-b", "c1")}, false},
		"foreign container":     {[]outageMember{member("mon", "a", "host-a", "foreign")}, false},
		"host outside manifest": {[]outageMember{member("mon", "a", "host-z", "c1")}, false},
		"unknown role":          {[]outageMember{member("mds", "a", "host-a", "c1")}, false},
		"unnamed":               {[]outageMember{member("mon", "", "host-a", "c1")}, false},
		"no container":          {[]outageMember{member("mon", "a", "host-a", "")}, false},
	} {
		manifest, err := newOutageManifest(hosts, tc.members, owned)
		if (err == nil) != tc.ok {
			t.Errorf("%s: error=%v, want valid=%t", name, err, tc.ok)
		}
		if err == nil && (len(manifest.onHost("host-a")) != 2 || len(manifest.onHost("host-b")) != 1) {
			t.Errorf("%s: membership by host %+v", name, manifest)
		}
	}
}

func TestCorrelatedLedgerVerifyTable(t *testing.T) {
	exact := func(_, seq int) outageObject {
		id := fmt.Sprintf("r-%05d", seq)
		data := outagePayload(id, seq)
		return outageObject{Present: true, Size: len(data), DataLen: len(data), SHA256: outageDigest(data)}
	}
	ops := func(returned bool, errText string) []outageOp {
		id := "r-00000"
		return []outageOp{{Op: id, Object: "op-" + id, Seq: 0, Returned: returned, Error: errText}}
	}
	other := outageObject{Present: true, Size: 3, DataLen: 3, SHA256: "00"}
	for name, tc := range map[string]struct {
		ops         []outageOp
		got         outageObject
		wantErr     bool
		unconfirmed int
	}{
		"acked exact":                  {ops(true, ""), exact(0, 0), false, 0},
		"acked absent":                 {ops(true, ""), outageObject{}, true, 0},
		"acked other bytes":            {ops(true, ""), other, true, 0},
		"failed absent":                {ops(true, "boom"), outageObject{}, false, 0},
		"failed but applied":           {ops(true, "boom"), exact(0, 0), false, 1},
		"failed with other bytes":      {ops(true, "boom"), other, true, 0},
		"in flight absent":             {ops(false, ""), outageObject{}, false, 0},
		"in flight but applied":        {ops(false, ""), exact(0, 0), false, 1},
		"in flight with other bytes":   {ops(false, ""), other, true, 0},
		"truncated read of right hash": {ops(true, ""), outageObject{Present: true, Size: 9, DataLen: 9, SHA256: exact(0, 0).SHA256}, true, 0},
	} {
		got, err := outageVerify(tc.ops, map[string]outageObject{tc.ops[0].Object: tc.got})
		if (err != nil) != tc.wantErr || got != tc.unconfirmed {
			t.Errorf("%s: unconfirmed=%d error=%v", name, got, err)
		}
	}
	if _, err := outageVerify(ops(true, ""), map[string]outageObject{}); err == nil {
		t.Error("a missing answer passed")
	}
}

func TestCorrelatedLedgerKeepsBothTokens(t *testing.T) {
	id := "r-00000"
	events := []outageEvent{{Event: "session", Run: "r", PID: 7, Client: "client.1"},
		{Event: "intent", Run: "r", PID: 7, Op: id, Object: "op-" + id, SHA256: outageDigest(outagePayload(id, 0)), Bytes: len(outagePayload(id, 0)), Phase: "applying", NS: 5},
		{Event: "return", Run: "r", PID: 7, Op: id, PhaseAfter: "outage", NS: 9}}
	ops, err := outageOps(events, "r")
	if err != nil || len(ops) != 1 || ops[0].Phase != "applying" || ops[0].PhaseAfter != "outage" || ops[0].IntentNS != 5 || ops[0].ReturnNS != 9 {
		t.Fatalf("%+v %v", ops, err)
	}
	if got := ops[0].Credit(); got != "applying/acked" {
		t.Fatalf("an operation begun while applying was credited as %s", got)
	}
}

func outageCapture() (func(string, ...any), func(string, ...any), *[]string, *[]string) {
	var logs, errs []string
	return func(f string, a ...any) { logs = append(logs, fmt.Sprintf(f, a...)) },
		func(f string, a ...any) { errs = append(errs, fmt.Sprintf(f, a...)) }, &logs, &errs
}

func outageParseReceipt(t *testing.T, logs []string) (receipts []outageReceipt, attempts []outageRestoreAttempt) {
	t.Helper()
	if len(logs) != 1 || !strings.HasPrefix(logs[0], "CORRELATED_OUTAGE_RECEIPTS ") {
		t.Fatalf("want exactly one receipt line, got %q", logs)
	}
	var parsed struct {
		Receipts        []outageReceipt        `json:"receipts"`
		RestoreAttempts []outageRestoreAttempt `json:"restore_attempts"`
	}
	if err := json.Unmarshal([]byte(strings.TrimPrefix(logs[0], "CORRELATED_OUTAGE_RECEIPTS ")), &parsed); err != nil {
		t.Fatal(err)
	}
	return parsed.Receipts, parsed.RestoreAttempts
}

// A later stop fails and the scenario ends early: the cleanup must still
// restore every attempted target and emit the receipt with the attempted set.
func TestOutageFinishEmitsReceiptOfPartialInjection(t *testing.T) {
	var log []string
	boom := errors.New("daemon refused to stop")
	plan, _, _ := outageFakePlan(&outageFakeDaemon{name: "a", log: &log}, &outageFakeDaemon{name: "b", log: &log, stopErr: boom}, &outageFakeDaemon{name: "c", log: &log})
	if err := plan.Apply(t.Context(), time.Second); !errors.Is(err, boom) {
		t.Fatal(err)
	}
	logf, errorf, logs, errs := outageCapture()
	outageFinish(t.Context(), plan, logf, errorf)
	receipts, attempts := outageParseReceipt(t, *logs)
	if len(*errs) != 0 || len(receipts) != 3 {
		t.Fatalf("errors %v receipts %+v", *errs, receipts)
	}
	if !receipts[0].Attempted || !receipts[0].Applied || !receipts[0].Restored || !receipts[1].Attempted || receipts[1].Applied || receipts[1].StopError == "" || !receipts[1].Restored || receipts[2].Attempted || receipts[2].Restored {
		t.Fatalf("receipts %+v", receipts)
	}
	if len(attempts) != 1 || attempts[0].Kind != "cleanup retry" || attempts[0].Targets != 2 || attempts[0].Error != "" {
		t.Fatalf("attempts %+v", attempts)
	}
}

// The primary restoration fails and so does the cleanup retry: the receipt
// keeps both, tells them apart, and shows the targets as not restored.
func TestOutageFinishKeepsFailedPrimaryRestoreApartFromRetry(t *testing.T) {
	var log []string
	failing := errors.New("start b failed")
	b := &outageFakeDaemon{name: "b", log: &log, startErr: failing}
	plan, _, _ := outageFakePlan(&outageFakeDaemon{name: "a", log: &log}, b)
	if err := plan.Apply(t.Context(), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := plan.Restore(t.Context(), "primary"); !errors.Is(err, failing) {
		t.Fatal(err)
	}
	logf, errorf, logs, errs := outageCapture()
	outageFinish(t.Context(), plan, logf, errorf)
	receipts, attempts := outageParseReceipt(t, *logs)
	if len(*errs) != 1 || !strings.Contains((*errs)[0], "start b failed") {
		t.Fatalf("the retry failure was not reported: %v", *errs)
	}
	if !receipts[0].Restored || receipts[1].Restored || receipts[1].RestoreError == "" || !receipts[1].Attempted {
		t.Fatalf("receipts %+v", receipts)
	}
	if len(attempts) != 2 || attempts[0].Kind != "primary" || attempts[1].Kind != "cleanup retry" ||
		!strings.Contains(attempts[0].Error, "start b failed") || !strings.Contains(attempts[1].Error, "start b failed") || attempts[0].Targets != 2 || attempts[1].Targets != 1 {
		t.Fatalf("attempts %+v", attempts)
	}
}

// With nothing attempted the receipt is still emitted and no restore is recorded.
func TestOutageFinishWithoutInjection(t *testing.T) {
	var log []string
	plan, _, _ := outageFakePlan(&outageFakeDaemon{name: "a", log: &log})
	logf, errorf, logs, errs := outageCapture()
	outageFinish(t.Context(), plan, logf, errorf)
	receipts, attempts := outageParseReceipt(t, *logs)
	if len(*errs) != 0 || len(attempts) != 0 || receipts[0].Attempted || len(log) != 0 {
		t.Fatalf("%v %+v %+v", *errs, receipts, attempts)
	}
}
