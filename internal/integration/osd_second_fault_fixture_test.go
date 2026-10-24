//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// secondFaultFakeRados stands in for the rados module with an in-memory store
// kept in a file, so the live writer's ledger and its oracle can be checked
// without a cluster. FAKE_MODE injects one failure at operation FAKE_AT:
// "before" raises a timeout without storing, "after" stores the object and then
// raises (a lost reply), "refuse" raises ENOSPC without storing. It tests the
// writer's bookkeeping, not librados or Ceph behavior.
const secondFaultFakeRados = `import json, os

STORE = os.environ["FAKE_STORE"]
MODE = os.environ.get("FAKE_MODE", "")
AT = int(os.environ.get("FAKE_AT", "-1"))

class Error(Exception):
    def __init__(self, errno=0): self.errno = errno
class ObjectNotFound(Error): pass

def load():
    return json.load(open(STORE)) if os.path.exists(STORE) else {}

class Ioctx:
    writes = 0
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def write_full(self, name, data):
        number = Ioctx.writes; Ioctx.writes += 1
        if number == AT and MODE == "before": raise Error(110)
        if number == AT and MODE == "refuse": raise Error(28)
        store = load(); store[name] = data.hex(); json.dump(store, open(STORE, "w"))
        if number == AT and MODE == "after": raise Error(110)
    def get_last_version(self): return 7
    def stat(self, name):
        store = load()
        if name not in store: raise ObjectNotFound(2)
        return len(store[name]) // 2, 0
    def read(self, name, length, offset): return bytes.fromhex(load()[name])

class Rados:
    def __init__(self, **kw): pass
    def connect(self): pass
    def get_instance_id(self): return 4177
    def open_ioctx(self, pool): return Ioctx()
    def shutdown(self): pass
`

func runSecondFaultActor(t *testing.T, dir string, env []string, stopAfter time.Duration, args ...string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "rados.py"), []byte(secondFaultFakeRados), 0o600); err != nil {
		t.Fatal(err)
	}
	argv := append([]string{"-I", "-c", "import sys; sys.path.insert(0, sys.argv.pop(1)); exec(sys.argv.pop(1))", dir, secondFaultActor}, args...)
	cmd := exec.Command("python3", argv...)
	cmd.Env = append(os.Environ(), append([]string{"FAKE_STORE=" + filepath.Join(dir, "store.json")}, env...)...)
	if stopAfter > 0 {
		time.AfterFunc(stopAfter, func() { _ = os.WriteFile(filepath.Join(dir, "stop"), nil, 0o600) })
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
}

func readSecondFaultLedger(t *testing.T, path string) []secondFaultEvent {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var events []secondFaultEvent
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var event secondFaultEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	return events
}

func TestSecondFaultWriterLedgerAndOracle(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("the writer's ledger check needs python3")
	}
	for name, tc := range map[string]struct {
		env     []string
		outcome string // outcome of operation 2
		present bool   // whether the fake store holds operation 2
	}{
		"all acknowledged":           {nil, opAcked, true},
		"reply lost after commit":    {[]string{"FAKE_MODE=after", "FAKE_AT=2"}, opUnknown, true},
		"timeout before commit":      {[]string{"FAKE_MODE=before", "FAKE_AT=2"}, opUnknown, false},
		"refused before application": {[]string{"FAKE_MODE=refuse", "FAKE_AT=2"}, opRefused, false},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			ledger := filepath.Join(dir, "ledger.jsonl")
			runSecondFaultActor(t, dir, tc.env, 0, "actor", "pool", ledger, filepath.Join(dir, "stop"), "0.01", "5")
			events := readSecondFaultLedger(t, ledger)
			ops, err := secondFaultOps(events, true)
			if err != nil || len(ops) != 5 {
				t.Fatalf("%d operations: %v", len(ops), err)
			}
			intents := 0
			for _, event := range events {
				if event.Event == "intent" {
					intents++
				}
			}
			if intents != 5 {
				t.Fatalf("%d submissions, want 5 and no replay", intents)
			}
			for i, op := range ops {
				want := opAcked
				if i == 2 {
					want = tc.outcome
				}
				if op.Outcome != want || op.Seq != i {
					t.Fatalf("operation %d: %+v, want %s", i, op, want)
				}
			}
			names := make([]string, len(ops))
			for i, op := range ops {
				names[i] = op.Name
			}
			encoded, _ := json.Marshal(names)
			read := runSecondFaultReader(t, dir, string(encoded))
			if err := secondFaultVerify(ops, read); err != nil {
				t.Fatal(err)
			}
			if _, stored := read[ops[2].Name]; !stored || read[ops[2].Name].Present != tc.present {
				t.Fatalf("operation 2 stored %t, want %t", read[ops[2].Name].Present, tc.present)
			}
			mark := secondFaultCount(ops)
			wantAcked := 5
			if tc.outcome != opAcked {
				wantAcked = 4
			}
			if mark.Submitted != 5 || mark.Acked != wantAcked {
				t.Fatalf("watermark %+v", mark)
			}

			// Each corruption of the readback must fail the oracle.
			for what, mutate := range map[string]func(map[string]secondFaultObject){
				"acknowledged object missing": func(m map[string]secondFaultObject) { m[ops[0].Name] = secondFaultObject{} },
				"acknowledged object torn": func(m map[string]secondFaultObject) {
					o := m[ops[1].Name]
					o.SHA256, o.DataLen = "0000", o.DataLen-1
					m[ops[1].Name] = o
				},
				"acknowledged object replaced": func(m map[string]secondFaultObject) {
					o := m[ops[3].Name]
					o.SHA256 = "0000"
					m[ops[3].Name] = o
				},
				"object not read": func(m map[string]secondFaultObject) { delete(m, ops[4].Name) },
			} {
				broken := map[string]secondFaultObject{}
				for k, v := range read {
					broken[k] = v
				}
				mutate(broken)
				if secondFaultVerify(ops, broken) == nil {
					t.Errorf("%s was accepted", what)
				}
			}
			if tc.outcome == opUnknown {
				torn := map[string]secondFaultObject{}
				for k, v := range read {
					torn[k] = v
				}
				torn[ops[2].Name] = secondFaultObject{Present: true, Size: ops[2].Bytes, DataLen: ops[2].Bytes, SHA256: "0000"}
				if secondFaultVerify(ops, torn) == nil {
					t.Error("an unknown outcome that reads back torn was accepted")
				}
			}
			if tc.outcome == opRefused {
				present := map[string]secondFaultObject{}
				for k, v := range read {
					present[k] = v
				}
				present[ops[2].Name] = secondFaultObject{Present: true, Size: ops[2].Bytes, DataLen: ops[2].Bytes, SHA256: ops[2].SHA256}
				if secondFaultVerify(ops, present) == nil {
					t.Error("a refused write that is present was accepted")
				}
			}
		})
	}
}

func runSecondFaultReader(t *testing.T, dir, names string) map[string]secondFaultObject {
	t.Helper()
	argv := []string{"-I", "-c", "import sys; sys.path.insert(0, sys.argv.pop(1)); exec(sys.argv.pop(1))", dir, secondFaultActor, "read", "pool", names}
	cmd := exec.Command("python3", argv...)
	cmd.Env = append(os.Environ(), "FAKE_STORE="+filepath.Join(dir, "store.json"))
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	var read map[string]secondFaultObject
	if err := json.Unmarshal(out, &read); err != nil {
		t.Fatalf("%s: %v", out, err)
	}
	return read
}

func TestSecondFaultWriterStopsOnItsStopFile(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("the writer's ledger check needs python3")
	}
	dir := t.TempDir()
	ledger := filepath.Join(dir, "ledger.jsonl")
	runSecondFaultActor(t, dir, nil, 300*time.Millisecond, "actor", "pool", ledger, filepath.Join(dir, "stop"), "0.05", "100000")
	events := readSecondFaultLedger(t, ledger)
	ops, err := secondFaultOps(events, true)
	if err != nil || len(ops) == 0 || len(ops) > 1000 {
		t.Fatalf("%d operations: %v", len(ops), err)
	}
	last := events[len(events)-1]
	if last.Event != "stopped" || last.Seq != len(ops) {
		t.Fatalf("last record %+v for %d operations", last, len(ops))
	}
	if events[0].Event != "start" || events[0].Client != "client.4177" {
		t.Fatalf("first record %+v", events[0])
	}
	if mark := secondFaultCount(ops); mark.Acked != len(ops) {
		t.Fatalf("watermark %+v of %d operations", mark, len(ops))
	}
}

func TestSecondFaultLedgerRules(t *testing.T) {
	intent := func(seq int) secondFaultEvent {
		return secondFaultEvent{Event: "intent", Object: "live-" + strconv.Itoa(seq), Seq: seq, Bytes: 10, SHA256: "ab"}
	}
	ack := func(seq int) secondFaultEvent {
		return secondFaultEvent{Event: "ack", Object: "live-" + strconv.Itoa(seq)}
	}
	fail := func(seq, errno int) secondFaultEvent {
		return secondFaultEvent{Event: "error", Object: "live-" + strconv.Itoa(seq), Errno: errno}
	}
	start, stop := secondFaultEvent{Event: "start"}, secondFaultEvent{Event: "stopped"}
	for name, tc := range map[string]struct {
		events  []secondFaultEvent
		final   bool
		wantErr bool
		want    secondFaultMark
	}{
		"empty ledger":                  {nil, false, false, secondFaultMark{}},
		"open call, not final":          {[]secondFaultEvent{start, intent(0)}, false, false, secondFaultMark{Submitted: 1, Outstanding: 1}},
		"open call at the end is lost":  {[]secondFaultEvent{start, intent(0), stop}, true, false, secondFaultMark{Submitted: 1, Unknown: 1}},
		"acknowledged then refused":     {[]secondFaultEvent{start, intent(0), ack(0), intent(1), fail(1, 28), stop}, true, false, secondFaultMark{Submitted: 2, Acked: 1, Refused: 1}},
		"timeout stays unknown":         {[]secondFaultEvent{start, intent(0), fail(0, 110), stop}, true, false, secondFaultMark{Submitted: 1, Unknown: 1}},
		"error without an errno":        {[]secondFaultEvent{start, intent(0), fail(0, 0), stop}, true, false, secondFaultMark{Submitted: 1, Unknown: 1}},
		"replayed submission":           {[]secondFaultEvent{start, intent(0), fail(0, 110), intent(0)}, false, true, secondFaultMark{}},
		"terminal without a submission": {[]secondFaultEvent{start, ack(0)}, false, true, secondFaultMark{}},
		"two terminals":                 {[]secondFaultEvent{start, intent(0), ack(0), fail(0, 110)}, false, true, secondFaultMark{}},
		"overlapping submissions":       {[]secondFaultEvent{start, intent(0), intent(1)}, false, true, secondFaultMark{}},
		"sequence gap":                  {[]secondFaultEvent{start, intent(1)}, false, true, secondFaultMark{}},
		"final without a stop record":   {[]secondFaultEvent{start, intent(0), ack(0)}, true, true, secondFaultMark{}},
		"record after the stop":         {[]secondFaultEvent{start, stop, intent(0)}, true, true, secondFaultMark{}},
		"acknowledgment with an error":  {[]secondFaultEvent{start, intent(0), {Event: "ack", Object: "live-0", Ret: -5}}, false, true, secondFaultMark{}},
		"submission without a digest":   {[]secondFaultEvent{start, {Event: "intent", Object: "live-0", Bytes: 10}}, false, true, secondFaultMark{}},
		"second start":                  {[]secondFaultEvent{start, intent(0), start}, false, true, secondFaultMark{}},
		"unknown record kind":           {[]secondFaultEvent{start, {Event: "retry", Object: "live-0"}}, false, true, secondFaultMark{}},
	} {
		ops, err := secondFaultOps(tc.events, tc.final)
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: error %v", name, err)
			continue
		}
		if err == nil && secondFaultCount(ops) != tc.want {
			t.Errorf("%s: watermark %+v, want %+v", name, secondFaultCount(ops), tc.want)
		}
	}
	// An operation still open at readback has no outcome to check against.
	open, _ := secondFaultOps([]secondFaultEvent{start, intent(0)}, false)
	if secondFaultVerify(open, map[string]secondFaultObject{"live-0": {}}) == nil {
		t.Error("an outstanding operation passed the oracle")
	}
}

func TestSecondFaultWriterAdvanceNeedsTheConfirmedBaseline(t *testing.T) {
	// Two acknowledgments arrived between the pre-stop read and the confirmed
	// outage, and nothing after it.
	pre, confirmed, now := secondFaultMark{Acked: 3}, secondFaultMark{Acked: 5}, secondFaultMark{Acked: 5}
	if !secondFaultAdvanced(pre, now, 2) {
		t.Fatal("the control must show that the pre-stop baseline is satisfied by pre-outage acknowledgments")
	}
	if secondFaultAdvanced(confirmed, now, 2) {
		t.Fatal("no acknowledgment after the confirmed outage was accepted")
	}
	if !secondFaultAdvanced(confirmed, secondFaultMark{Acked: 7}, 2) || secondFaultAdvanced(confirmed, secondFaultMark{Acked: 6}, 2) {
		t.Fatal("exactly two acknowledgments after the baseline are required")
	}
}

func TestSecondFaultStopAdmission(t *testing.T) {
	running := secondFaultInstance{Running: true, Pid: 40, StartedAt: "t0"}
	if err := secondFaultAdmitStop(running, running); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct{ captured, now secondFaultInstance }{
		"already exited":         {running, secondFaultInstance{Pid: 0, StartedAt: "t0"}},
		"exited and OOM killed":  {running, secondFaultInstance{OOMKilled: true, StartedAt: "t0"}},
		"restarted":              {running, secondFaultInstance{Running: true, Pid: 41, StartedAt: "t1"}},
		"same pid, new start":    {running, secondFaultInstance{Running: true, Pid: 40, StartedAt: "t1"}},
		"captured while stopped": {secondFaultInstance{StartedAt: "t0"}, running},
		"captured without a pid": {secondFaultInstance{Running: true}, running},
	} {
		if secondFaultAdmitStop(tc.captured, tc.now) == nil {
			t.Errorf("%s was admitted", name)
		}
	}
}
