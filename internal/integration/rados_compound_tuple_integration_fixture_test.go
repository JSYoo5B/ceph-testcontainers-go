//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

// These controls test the tuple ledger, the observation contract and the
// oracle with synthetic input. They do not test librados, Ceph or any cluster.

type tupleFixtureValue struct{ data, xattr, omap []byte }

var tupleFixtureTuples = map[string]tupleFixtureValue{
	"A": {bytes.Repeat([]byte("a"), 1000), []byte("xattr-a"), []byte("omap-a")},
	"B": {bytes.Repeat([]byte("b"), 2000), []byte("xattr-b"), []byte("omap-b")},
}

var tupleFixtureID = tupleIdentity{Object: tupleObject, Pool: "pool", PoolID: 5, FSID: "11111111-2222-3333-4444-555555555555"}

func tupleFixtureEvent(event, tag string, ns int64) tupleEvent {
	v := tupleFixtureTuples[tag]
	sum := sha256.Sum256(v.data)
	return tupleEvent{Event: event, NS: ns, Object: tupleObject, Bytes: len(v.data), SHA256: hex.EncodeToString(sum[:]),
		Xattr: hex.EncodeToString(v.xattr), OMAP: hex.EncodeToString(v.omap), Actions: []string{"write_full", "set_xattr", "omap_set"}}
}

// tupleFixtureLedger returns the ledger of an outcome: "pre_submission",
// "known_success" (completion), "callback_success" (callback only), "unknown"
// (no completion).
func tupleFixtureLedger(outcome string) []tupleEvent {
	events := []tupleEvent{tupleFixtureEvent("seed", "A", 10)}
	if outcome == "pre_submission" {
		return events
	}
	events = append(events, tupleFixtureEvent("intent", "B", 20), tupleEvent{Event: "api_return", NS: 21, Object: tupleObject})
	switch outcome {
	case "known_success":
		events = append(events, tupleEvent{Event: "completed", NS: 30, Object: tupleObject, Via: "poll"})
	case "callback_success":
		events = append(events, tupleEvent{Event: "callback", NS: 30, CallbackNS: 30, Object: tupleObject})
	}
	return events
}

// tupleFixtureObservation renders a compound-read observation whose components
// come from tuple d (data), x (xattr) and o (omap).
func tupleFixtureObservation(d, x, o string, mutate ...func(map[string]any)) []byte {
	sum := sha256.Sum256(tupleFixtureTuples[d].data)
	obs := map[string]any{
		"object": tupleObject, "pool": tupleFixtureID.Pool, "pool_id": tupleFixtureID.PoolID, "fsid": tupleFixtureID.FSID,
		"client": "client.4123", "compound": true, "actions": []string{"stat", "read", "getxattrs", "omap_get_vals_by_keys"},
		"operates": 1, "operate_ret": 0, "subresults": map[string]int{"stat": 0, "read": 0, "getxattrs": 0, "omap_get_vals_by_keys": 0},
		"size": len(tupleFixtureTuples[d].data), "bytes_read": len(tupleFixtureTuples[d].data), "eof": true, "sha256": hex.EncodeToString(sum[:]),
		"xattrs": map[string]string{tupleXattrName: hex.EncodeToString(tupleFixtureTuples[x].xattr)},
		"omap":   map[string]string{tupleOMAPKey: hex.EncodeToString(tupleFixtureTuples[o].omap)},
	}
	for _, m := range mutate {
		m(obs)
	}
	data, _ := json.Marshal(obs)
	return data
}

func tupleUnfenced(obs map[string]any) {
	obs["compound"], obs["operates"] = false, 3
}

// The model: 3 outcomes x 8 coherent (data, xattr, omap) observations, plus two
// observations assembled from separate reads across the A to B transition.
func TestRADOSCompoundTupleModel(t *testing.T) {
	accepts := map[string][]string{"pre_submission": {"AAA"}, "known_success": {"BBB"}, "unknown": {"AAA", "BBB"}}
	combinations := 0
	for _, outcome := range []string{"pre_submission", "known_success", "unknown"} {
		for _, shape := range []string{"AAA", "AAB", "ABA", "ABB", "BAA", "BAB", "BBA", "BBB"} {
			combinations++
			got := tupleJudge(tupleFixtureLedger(outcome), tupleFixtureObservation(shape[0:1], shape[1:2], shape[2:3]), tupleFixtureID, true)
			want := "refused"
			if slices.Contains(accepts[outcome], shape) {
				want = "accepted"
			}
			if got.Status != want || got.Outcome != outcome {
				t.Errorf("%s observing %s: %+v, want %s", outcome, shape, got, want)
			}
		}
	}
	// Separate data, xattr and omap reads across one atomic transition can show
	// a mixture that never existed. Without the single-operation fence it is not
	// evidence of a partial commit, whatever it looks like.
	for _, shape := range []string{"AAB", "ABB"} {
		combinations++
		got := tupleJudge(tupleFixtureLedger("unknown"), tupleFixtureObservation(shape[0:1], shape[1:2], shape[2:3], tupleUnfenced), tupleFixtureID, true)
		if got.Status != "inconclusive" {
			t.Errorf("straddled read %s: %+v, want inconclusive", shape, got)
		}
	}
	if combinations != 26 {
		t.Fatalf("%d combinations, want 26", combinations)
	}
}

// Independent field membership would accept all six mixtures of an unknown
// outcome. The whole-tuple oracle refuses every one of them.
func TestRADOSCompoundTupleRefusesMixtures(t *testing.T) {
	independent := func(d, x, o string) bool { return true } // each field is A or B
	mixtures := 0
	for _, shape := range []string{"AAB", "ABA", "ABB", "BAA", "BAB", "BBA"} {
		mixtures++
		if !independent(shape[0:1], shape[1:2], shape[2:3]) {
			t.Fatal("independent membership model changed")
		}
		for _, outcome := range []string{"pre_submission", "known_success", "unknown"} {
			if got := tupleJudge(tupleFixtureLedger(outcome), tupleFixtureObservation(shape[0:1], shape[1:2], shape[2:3]), tupleFixtureID, true); got.Status != "refused" {
				t.Errorf("%s accepted mixture %s: %+v", outcome, shape, got)
			}
		}
	}
	if mixtures != 6 {
		t.Fatal(mixtures)
	}
}

func TestRADOSCompoundTupleOutcomes(t *testing.T) {
	for name, tc := range map[string]struct {
		events []tupleEvent
		want   string
	}{
		"nothing submitted":         {tupleFixtureLedger("pre_submission"), "pre_submission"},
		"completion succeeded":      {tupleFixtureLedger("known_success"), "known_success"},
		"callback-only success":     {tupleFixtureLedger("callback_success"), "known_success"},
		"no completion":             {tupleFixtureLedger("unknown"), "unknown"},
		"failed completion":         {append(tupleFixtureLedger("unknown"), tupleEvent{Event: "completed", NS: 30, Object: tupleObject, Ret: -110}), "unknown"},
		"api error":                 {append(tupleFixtureLedger("pre_submission"), tupleFixtureEvent("intent", "B", 20), tupleEvent{Event: "api_return", NS: 21, Object: tupleObject, Error: "boom"}), "unknown"},
		"intent without api return": {append(tupleFixtureLedger("pre_submission"), tupleFixtureEvent("intent", "B", 20)), "unknown"},
	} {
		if _, _, outcome, err := tupleLedger(tc.events); err != nil || outcome != tc.want {
			t.Errorf("%s: outcome %q error %v, want %q", name, outcome, err, tc.want)
		}
	}
	replay := append(tupleFixtureLedger("known_success"), tupleFixtureEvent("intent", "B", 40))
	sameComponent := tupleFixtureLedger("known_success")
	sameComponent[1].OMAP = sameComponent[0].OMAP
	otherObject := tupleFixtureLedger("known_success")
	otherObject[3].Object = "other"
	noSeed := tupleFixtureLedger("known_success")[1:]
	twoSeeds := append(tupleFixtureLedger("pre_submission"), tupleFixtureEvent("seed", "A", 11))
	completionWithoutReturn := []tupleEvent{tupleFixtureEvent("seed", "A", 10), tupleFixtureEvent("intent", "B", 20), {Event: "completed", NS: 30, Object: tupleObject}}
	completionAfterError := append(tupleFixtureLedger("pre_submission"), tupleFixtureEvent("intent", "B", 20), tupleEvent{Event: "api_return", NS: 21, Object: tupleObject, Error: "boom"}, tupleEvent{Event: "completed", NS: 30, Object: tupleObject})
	disagree := append(tupleFixtureLedger("known_success"), tupleEvent{Event: "callback", NS: 31, CallbackNS: 31, Object: tupleObject, Ret: -5})
	orphan := append(tupleFixtureLedger("pre_submission"), tupleEvent{Event: "completed", NS: 30, Object: tupleObject})
	badSeed := tupleFixtureLedger("pre_submission")
	badSeed[0].Actions = []string{"write_full"}
	for name, events := range map[string][]tupleEvent{
		"replay": replay, "a component shared by A and B": sameComponent, "completion of another object": otherObject,
		"no seed": noSeed, "two seeds": twoSeeds, "completion without api return": completionWithoutReturn,
		"completion after an api error": completionAfterError, "completion and callback disagree": disagree,
		"completion without intent": orphan, "seed by independent setters": badSeed,
	} {
		if result := tupleJudge(events, tupleFixtureObservation("B", "B", "B"), tupleFixtureID, true); result.Status != "refused" {
			t.Errorf("%s: %+v", name, result)
		}
	}
	// An unknown outcome is never promoted by the readback: B observed after an
	// unknown submission is still an unknown outcome.
	if r := tupleJudge(tupleFixtureLedger("unknown"), tupleFixtureObservation("B", "B", "B"), tupleFixtureID, true); r.Status != "accepted" || r.Outcome != "unknown" || r.Matched != "B" {
		t.Errorf("%+v", r)
	}
	// A known success must not be satisfied by the old tuple, a pre-submission
	// control must not be satisfied by the new one.
	if r := tupleJudge(tupleFixtureLedger("known_success"), tupleFixtureObservation("A", "A", "A"), tupleFixtureID, true); r.Status != "refused" {
		t.Errorf("%+v", r)
	}
	if r := tupleJudge(tupleFixtureLedger("callback_success"), tupleFixtureObservation("A", "A", "A"), tupleFixtureID, true); r.Status != "refused" {
		t.Errorf("%+v", r)
	}
	if r := tupleJudge(tupleFixtureLedger("pre_submission"), tupleFixtureObservation("B", "B", "B"), tupleFixtureID, true); r.Status != "refused" {
		t.Errorf("%+v", r)
	}
	// The actor's stop is recorded apart from the outcome.
	if r := tupleJudge(tupleFixtureLedger("unknown"), tupleFixtureObservation("A", "A", "A"), tupleFixtureID, false); r.Status != "inconclusive" {
		t.Errorf("unknown outcome without a recorded stop: %+v", r)
	}
	stop := append(tupleFixtureLedger("unknown"), tupleEvent{Event: "stop"})
	for name, tc := range map[string]struct {
		events   []tupleEvent
		exit     int
		complete bool
		want     bool
	}{
		"stopped and joined": {stop, 0, true, true}, "no stop record": {tupleFixtureLedger("unknown"), 0, true, false},
		"incomplete process": {stop, 0, false, false}, "failed exit": {stop, 1, true, false},
	} {
		if got := tupleStopped(tc.events, tc.exit, tc.complete); got != tc.want {
			t.Errorf("%s: %t", name, got)
		}
	}
}

func TestRADOSCompoundTupleChannel(t *testing.T) {
	for name, tc := range map[string]struct {
		events []tupleEvent
		want   string
	}{
		"complete":    {[]tupleEvent{{Event: "channel", Callbacks: []string{tupleObject}}}, "complete"},
		"outstanding": {[]tupleEvent{{Event: "channel", Pending: []string{tupleObject}, Stopped: true}}, "incomplete"},
		"no callback": {[]tupleEvent{{Event: "channel"}}, "incomplete"},
		"no report":   {[]tupleEvent{{Event: "intent"}}, "missing"},
	} {
		if got := tupleChannel(tc.events); got != tc.want {
			t.Errorf("%s: %s, want %s", name, got, tc.want)
		}
	}
}

func TestRADOSCompoundTupleObservationContract(t *testing.T) {
	good := tupleFixtureObservation("A", "A", "A")
	if _, err := tupleObserve(good, tupleFixtureID); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(map[string]any){
		"error reported":    func(o map[string]any) { o["error"] = "OSError('boom')" },
		"not compound":      func(o map[string]any) { o["compound"] = false },
		"two operations":    func(o map[string]any) { o["operates"] = 2 },
		"no operation":      func(o map[string]any) { o["operates"] = 0 },
		"missing action":    func(o map[string]any) { o["actions"] = []string{"stat", "read", "getxattrs"} },
		"reordered actions": func(o map[string]any) { o["actions"] = []string{"read", "stat", "getxattrs", "omap_get_vals_by_keys"} },
		"null actions":      func(o map[string]any) { o["actions"] = nil },
		"operate failed":    func(o map[string]any) { o["operate_ret"] = -2 },
		"subresult failed": func(o map[string]any) {
			o["subresults"] = map[string]int{"stat": 0, "read": 0, "getxattrs": 0, "omap_get_vals_by_keys": -5}
		},
		"subresult not run": func(o map[string]any) {
			o["subresults"] = map[string]int{"stat": 0, "read": 0, "getxattrs": 0, "omap_get_vals_by_keys": -99999}
		},
		"subresult missing": func(o map[string]any) { o["subresults"] = map[string]int{"stat": 0, "read": 0, "getxattrs": 0} },
		"subresult null": func(o map[string]any) {
			o["subresults"] = map[string]any{"stat": 0, "read": 0, "getxattrs": nil, "omap_get_vals_by_keys": 0}
		},
		"extra subresult": func(o map[string]any) {
			o["subresults"] = map[string]int{"stat": 0, "read": 0, "getxattrs": 0, "omap_get_vals_by_keys": 0, "x": 0}
		},
		"other object":             func(o map[string]any) { o["object"] = "other" },
		"other pool":               func(o map[string]any) { o["pool"] = "other" },
		"other pool id":            func(o map[string]any) { o["pool_id"] = 6 },
		"other cluster":            func(o map[string]any) { o["fsid"] = "other" },
		"anonymous client":         func(o map[string]any) { o["client"] = "" },
		"short read":               func(o map[string]any) { o["eof"] = false },
		"bytes disagree with size": func(o map[string]any) { o["bytes_read"] = 999 },
		"size wrong type":          func(o map[string]any) { o["size"] = "1000" },
		"size fractional":          func(o map[string]any) { o["size"] = 1000.5 },
		"hash not hex":             func(o map[string]any) { o["sha256"] = strings.Repeat("z", 64) },
		"hash short":               func(o map[string]any) { o["sha256"] = "ab" },
		"xattr value not hex":      func(o map[string]any) { o["xattrs"] = map[string]string{tupleXattrName: "zz"} },
		"xattr value null":         func(o map[string]any) { o["xattrs"] = map[string]any{tupleXattrName: nil} },
		"omap value null":          func(o map[string]any) { o["omap"] = map[string]any{tupleOMAPKey: nil} },
		"xattrs wrong type":        func(o map[string]any) { o["xattrs"] = []string{} },
		"unknown field":            func(o map[string]any) { o["extra"] = 1 },
		"operate_ret as string":    func(o map[string]any) { o["operate_ret"] = "0" },
		"compound as string":       func(o map[string]any) { o["compound"] = "true" },
	} {
		if _, err := tupleObserve(tupleFixtureObservation("A", "A", "A", mutate), tupleFixtureID); err == nil {
			t.Errorf("%s: accepted", name)
		}
		if r := tupleJudge(tupleFixtureLedger("unknown"), tupleFixtureObservation("A", "A", "A", mutate), tupleFixtureID, true); r.Status != "inconclusive" {
			t.Errorf("%s: %+v", name, r)
		}
	}
	// Each required field, absent and null, is unproven, never an empty value.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(good, &fields); err != nil {
		t.Fatal(err)
	}
	for field := range fields {
		for _, null := range []bool{false, true} {
			broken := tupleCopyFields(fields)
			if null {
				broken[field] = json.RawMessage("null")
			} else {
				delete(broken, field)
			}
			data, _ := json.Marshal(broken)
			if _, err := tupleObserve(data, tupleFixtureID); err == nil {
				t.Errorf("field %s (null=%t) was accepted", field, null)
			}
		}
	}
	// Empty is a read result, not a missing field: it is a coherent observation
	// of a state that is neither A nor B, so it is refused, not inconclusive.
	for name, mutate := range map[string]func(map[string]any){
		"no xattrs": func(o map[string]any) { o["xattrs"] = map[string]string{} },
		"no omap":   func(o map[string]any) { o["omap"] = map[string]string{} },
		"empty object": func(o map[string]any) {
			o["size"], o["bytes_read"] = 0, 0
			o["sha256"] = hex.EncodeToString(sha256.New().Sum(nil))
		},
		"additional xattr": func(o map[string]any) {
			o["xattrs"] = map[string]string{tupleXattrName: hex.EncodeToString([]byte("xattr-a")), "user.extra": "00"}
		},
	} {
		if r := tupleJudge(tupleFixtureLedger("unknown"), tupleFixtureObservation("A", "A", "A", mutate), tupleFixtureID, true); r.Status != "refused" {
			t.Errorf("%s: %+v", name, r)
		}
	}
	for _, data := range []string{"", "null", "[]", "{", `"x"`} {
		if _, err := tupleObserve([]byte(data), tupleFixtureID); err == nil {
			t.Errorf("%q accepted", data)
		}
	}
}

func tupleCopyFields(in map[string]json.RawMessage) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// fakeTupleRados stands in for the rados module, so that the writer's ledger
// can be checked without a cluster. Its Ioctx deliberately has no write_full,
// set_xattr or aio_write_full: an independent setter fails the actor.
const fakeTupleRados = `import os, threading, time

SCHEDULE = os.environ["FAKE_SCHEDULE"]
ACTIONS = ["write_full", "set_xattr", "omap_set"]

class Error(Exception): pass

class WriteOpCtx:
    def __enter__(self): self.actions = []; return self
    def __exit__(self, *a): return False
    def write_full(self, data): self.actions.append("write_full")
    def set_xattr(self, name, value):
        if SCHEDULE == "build_fails": raise Error("rejected before submission")
        self.actions.append("set_xattr")

class Completion:
    def __init__(self, oncomplete):
        self.done = False
        if SCHEDULE == "immediate":
            self.done = True
            if oncomplete: oncomplete(self)
        elif SCHEDULE in ("delayed", "failed"):
            def finish():
                self.done = True
                time.sleep(0.3)
                if oncomplete: oncomplete(self)
            timer = threading.Timer(0.2, finish); timer.daemon = True; timer.start()
    def is_complete(self): return self.done
    def get_return_value(self): return -5 if SCHEDULE == "failed" else 0

class Ioctx:
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def set_omap(self, op, keys, values): op.actions.append("omap_set")
    def operate_write_op(self, op, oid):
        assert op.actions == ACTIONS, op.actions
    def operate_aio_write_op(self, op, oid, oncomplete=None):
        assert op.actions == ACTIONS, op.actions
        if SCHEDULE == "raises": raise Error("rejected")
        return Completion(oncomplete)

class Rados:
    def __init__(self, **kw): pass
    def connect(self): pass
    def get_instance_id(self): return 4123
    def open_ioctx(self, pool): return Ioctx()
    def shutdown(self): pass
`

func tupleFixtureRun(t *testing.T, dir string, env []string, phase, ledger string, terminateAfter time.Duration) string {
	t.Helper()
	cmd := exec.Command("python3", "-I", "-c", "import sys; sys.path.insert(0, sys.argv.pop(1)); exec(sys.argv.pop(1))", dir, tupleScript, phase, "pool", ledger)
	cmd.Env = append(os.Environ(), env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var terminate <-chan time.Time
	if terminateAfter > 0 {
		terminate = time.After(terminateAfter)
	}
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s: %v %s", phase, err, stderr.String())
			}
			return stdout.String()
		case <-terminate:
			_ = cmd.Process.Signal(syscall.SIGTERM)
			terminate = nil
		case <-time.After(15 * time.Second):
			_ = cmd.Process.Kill()
			t.Fatalf("%s did not end: %s", phase, stderr.String())
		}
	}
}

func tupleFixtureReadLedger(t *testing.T, path string) []tupleEvent {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var events []tupleEvent
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var e tupleEvent
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatal(err)
		}
		events = append(events, e)
	}
	return events
}

func TestRADOSCompoundTupleWriterSchedules(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("the writer's ledger check needs python3")
	}
	for name, tc := range map[string]struct {
		schedule  string
		terminate time.Duration
		kinds     []string
		outcome   string
	}{
		"callback before api return": {"immediate", 0, []string{"seed", "intent", "api_return", "completed", "callback", "channel", "stop"}, "known_success"},
		"delayed callback":           {"delayed", 0, []string{"seed", "intent", "api_return", "completed", "callback", "channel", "stop"}, "known_success"},
		"failed completion":          {"failed", 0, []string{"seed", "intent", "api_return", "completed", "callback", "channel", "stop"}, "unknown"},
		"never completes":            {"never", 300 * time.Millisecond, []string{"seed", "intent", "api_return", "channel", "stop"}, "unknown"},
		"api error":                  {"raises", 0, []string{"seed", "intent", "api_return", "stop"}, "unknown"},
		"rejected while building":    {"build_fails", 0, []string{"seed", "rejected"}, "pre_submission"},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "rados.py"), []byte(fakeTupleRados), 0o600); err != nil {
				t.Fatal(err)
			}
			ledger := filepath.Join(dir, "ledger.jsonl")
			// The seed phase builds the same operation; only submit may fail to build.
			tupleFixtureRun(t, dir, []string{"FAKE_SCHEDULE=seed"}, "seed", ledger, 0)
			tupleFixtureRun(t, dir, []string{"FAKE_SCHEDULE=" + tc.schedule}, "submit", ledger, tc.terminate)
			events := tupleFixtureReadLedger(t, ledger)
			var kinds []string
			for _, e := range events {
				kinds = append(kinds, e.Event)
			}
			if !slices.Equal(kinds, tc.kinds) {
				t.Fatalf("events %v, want %v", kinds, tc.kinds)
			}
			if _, _, outcome, err := tupleLedger(events); err != nil || outcome != tc.outcome {
				t.Fatalf("outcome %q error %v, want %q (%+v)", outcome, err, tc.outcome, events)
			}
			if tc.schedule == "immediate" && (events[3].Via != "poll" && events[3].Via != "callback") {
				t.Fatalf("completion %+v", events[3])
			}
			if tc.schedule == "never" && !events[len(events)-1].Stopped {
				t.Fatalf("the stop was not acknowledged: %+v", events[len(events)-1])
			}
		})
	}
}

// fakeTupleLibrados stands in for librados as seen through ctypes. It keeps a
// call log, applies the registered read actions only when the one operate call
// runs, and returns one fixed object state.
const fakeTupleLibrados = `import ctypes, json, os, types

OBJ = json.loads(os.environ["FAKE_OBJ"])
FAULT = os.environ.get("FAKE_FAULT", "")
LOG = open(os.environ["FAKE_CALLS"], "a")
pending, keep = [], []

def log(name): LOG.write(name + "\n"); LOG.flush()

def rados_create2(pc, cluster, name, flags): log("create2"); pc.contents.value = 1; return 0
def rados_conf_read_file(c, path): return 0
def rados_conf_set(c, k, v): return 0
def rados_connect(c):
    return -110 if FAULT == "connect" else 0
def rados_shutdown(c): pass
def rados_get_instance_id(c): return 4123
def rados_cluster_fsid(c, buf, n): ctypes.memmove(buf, b"11111111-2222-3333-4444-555555555555\0", 37); return 0
def rados_ioctx_create(c, pool, pio): pio.contents.value = 2; return 0
def rados_ioctx_get_id(io): return 5
def rados_ioctx_destroy(io): pass
def rados_create_read_op(): log("create_read_op"); return 7
def rados_release_read_op(op): log("release_read_op")
def rados_read_op_stat(op, psize, pmtime, prval):
    log("stat")
    def run(): psize.contents.value = len(bytes.fromhex(OBJ["data"])); prval.contents.value = 0
    pending.append(run)
def rados_read_op_read(op, off, n, buf, pread, prval):
    log("read")
    def run():
        data = bytes.fromhex(OBJ["data"]); assert len(data) < n
        ctypes.memmove(buf, data, len(data)); pread.contents.value = len(data); prval.contents.value = 0
    pending.append(run)
def rados_read_op_getxattrs(op, pit, prval):
    log("getxattrs")
    def run(): pit.contents.value = 100; prval.contents.value = 0
    pending.append(run)
def rados_read_op_omap_get_vals_by_keys2(op, keys, n, lens, pit, prval):
    log("omap_get_vals_by_keys2")
    assert n == 1 and keys[0] == b"tuple.key" and lens[0] == 9
    def run():
        pit.contents.value = 200; prval.contents.value = -5 if FAULT == "omap" else 0
    pending.append(run)
def rados_read_op_operate(op, io, oid, flags):
    log("operate")
    if OBJ.get("missing"): return -2
    for run in pending: run()
    return -5 if FAULT == "omap" else 0
cursor = {}
def _next(it, items, pk, pv, pn, pvn, named):
    it = getattr(it, "value", it); i = cursor.get(it, 0)
    if i >= len(items):
        pk.contents.value = None; pv.contents.value = None; pn.contents.value = 0
        if pvn: pvn.contents.value = 0
        return 0
    k, v = items[i]; cursor[it] = i + 1
    kb, vb = ctypes.create_string_buffer(k.encode()), ctypes.create_string_buffer(bytes.fromhex(v) or b"x")
    keep.extend([kb, vb])
    pk.contents.value = ctypes.addressof(kb); pv.contents.value = ctypes.addressof(vb)
    if named: pn.contents.value = len(bytes.fromhex(v))
    else: pn.contents.value = len(k); pvn.contents.value = len(bytes.fromhex(v))
    return 0
def rados_getxattrs_next(it, pname, pval, plen): return _next(it, list(OBJ["xattrs"].items()), pname, pval, plen, None, True)
def rados_getxattrs_end(it): log("getxattrs_end")
def rados_omap_get_next2(it, pk, pv, pkl, pvl): return _next(it, list(OBJ["omap"].items()), pk, pv, pkl, pvl, False)
def rados_omap_get_end(it): log("omap_end")

LIB = types.SimpleNamespace(**{k: v for k, v in dict(globals()).items() if k.startswith("rados_")})
`

func TestRADOSCompoundTupleReader(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("the reader check needs python3")
	}
	hexOf := func(b []byte) string { return hex.EncodeToString(b) }
	object := func(d, x, o string) map[string]any {
		return map[string]any{"data": hexOf(tupleFixtureTuples[d].data),
			"xattrs": map[string]string{tupleXattrName: hexOf(tupleFixtureTuples[x].xattr)},
			"omap":   map[string]string{tupleOMAPKey: hexOf(tupleFixtureTuples[o].omap)}}
	}
	for name, tc := range map[string]struct {
		object map[string]any
		fault  string
		// outcome and want are the verdict on this observation.
		outcome string
		want    string
		calls   []string
	}{
		"tuple B after a known success": {object("B", "B", "B"), "", "known_success", "accepted", nil},
		"tuple A before submission":     {object("A", "A", "A"), "", "pre_submission", "accepted", nil},
		"mixture is refused":            {object("A", "B", "A"), "", "unknown", "refused", nil},
		"absent object":                 {map[string]any{"missing": true}, "", "unknown", "inconclusive", []string{"create_read_op", "stat", "read", "getxattrs", "omap_get_vals_by_keys2", "operate", "release_read_op"}},
		"failed subresult":              {object("A", "A", "A"), "omap", "unknown", "inconclusive", nil},
		"connection failure":            {object("A", "A", "A"), "connect", "unknown", "inconclusive", []string{}},
		"empty omap":                    {map[string]any{"data": hexOf(tupleFixtureTuples["A"].data), "xattrs": map[string]string{tupleXattrName: hexOf(tupleFixtureTuples["A"].xattr)}, "omap": map[string]string{}}, "", "unknown", "refused", nil},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "fakelib.py"), []byte(fakeTupleLibrados), 0o600); err != nil {
				t.Fatal(err)
			}
			encoded, _ := json.Marshal(tc.object)
			calls := filepath.Join(dir, "calls")
			cmd := exec.Command("python3", "-I", "-c", "import ctypes, sys; sys.path.insert(0, sys.argv.pop(1)); import fakelib; ctypes.CDLL = lambda name: fakelib.LIB; exec(sys.argv.pop(1))", dir, tupleScript, "read", "pool", filepath.Join(dir, "unused"))
			cmd.Env = append(os.Environ(), "FAKE_OBJ="+string(encoded), "FAKE_FAULT="+tc.fault, "FAKE_CALLS="+calls)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			out, err := cmd.Output()
			if err != nil {
				t.Fatalf("%v %s", err, stderr.String())
			}
			id := tupleIdentity{Object: tupleObject, Pool: "pool", PoolID: 5, FSID: tupleFixtureID.FSID}
			result := tupleJudge(tupleFixtureLedger(tc.outcome), out, id, true)
			if result.Status != tc.want {
				t.Fatalf("%+v, want %s; observation %s", result, tc.want, out)
			}
			log, _ := os.ReadFile(calls)
			got := strings.Fields(string(log))
			if tc.calls != nil {
				// Connection failures stop before any read operation exists.
				if tc.fault == "connect" {
					if slices.Contains(got, "create_read_op") {
						t.Fatalf("a read operation was built after a failed connect: %v", got)
					}
					return
				}
				if !slices.Equal(got[1:], tc.calls) {
					t.Fatalf("calls %v, want %v", got, tc.calls)
				}
				return
			}
			// One read operation holds every action and is operated once.
			want := []string{"create_read_op", "stat", "read", "getxattrs", "omap_get_vals_by_keys2", "operate"}
			if len(got) < 1+len(want) || !slices.Equal(got[1:1+len(want)], want) || got[len(got)-1] != "release_read_op" || strings.Count(strings.Join(got, " "), "operate") != 1 {
				t.Fatalf("calls %v", got)
			}
		})
	}
}
