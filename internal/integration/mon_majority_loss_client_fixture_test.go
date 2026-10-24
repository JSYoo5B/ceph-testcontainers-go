//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

// monLossFakeRados stands in for the rados module with a directory as the
// object store. It checks the client script's bookkeeping, classification and
// oracle, not librados or Ceph behavior. Environment variables script what
// each stage does: FAKE_CONNECT and FAKE_IOCTX are "ok", "timeout", "refuse"
// or "hang"; FAKE_READ_NTH and FAKE_READ_MODE make the nth seed read "timeout",
// "hang" or "corrupt"; FAKE_WRITE_SEQ and FAKE_WRITE_MODE make the write of
// that sequence "before" or "after" (an error raised before or after the object
// is stored), "hang", "lose" (a success that stored nothing) or "corrupt". The
// configuration the client passed to rados.Rados is saved for inspection.
const monLossFakeRados = `import json, os, time

STORE = os.environ["FAKE_STORE"]

def env(name, default=""): return os.environ.get(name, default)

class Error(Exception):
    def __init__(self, message, errno=None):
        super().__init__(message)
        self.errno = errno

class TimedOut(Error): pass
class PermissionDeniedError(Error): pass
class ObjectNotFound(Error): pass

def stage(mode):
    if mode == "timeout": raise TimedOut("timed out", errno=110)
    if mode == "refuse": raise PermissionDeniedError("denied", errno=13)
    if mode == "hang": time.sleep(300)

reads = [0]

class Entry:
    def __init__(self, key): self.key = key

class Ioctx:
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def close(self): pass
    def write_full(self, name, data):
        seq = int(name.rsplit("-w", 1)[1]) if name.startswith("mlc-") else -1
        target = os.path.join(STORE, name)
        def store(payload):
            with open(target, "wb") as f: f.write(payload)
        hit = str(seq) == env("FAKE_WRITE_SEQ")
        mode = env("FAKE_WRITE_MODE") if hit else ""
        if mode == "before": raise TimedOut("timed out", errno=110)
        if mode == "after": store(data); raise TimedOut("timed out", errno=110)
        if mode == "hang": time.sleep(300)
        if mode == "lose": return
        if mode == "corrupt": store(data[:-1] + b"?"); return
        store(data)
    def stat(self, name):
        try: return os.path.getsize(os.path.join(STORE, name)), None
        except FileNotFoundError: raise ObjectNotFound(name, errno=2)
    def read(self, name, length, offset):
        if name == "seed":
            reads[0] += 1
            if str(reads[0]) == env("FAKE_READ_NTH"):
                stage(env("FAKE_READ_MODE"))
                if env("FAKE_READ_MODE") == "corrupt":
                    with open(os.path.join(STORE, name), "rb") as f: return f.read(length)[:-1] + b"?"
        try:
            with open(os.path.join(STORE, name), "rb") as f: return f.read(length)
        except FileNotFoundError: raise ObjectNotFound(name, errno=2)
    def list_objects(self): return [Entry(n) for n in sorted(os.listdir(STORE))]

class Rados:
    def __init__(self, conffile=None, conf=None, **kw):
        with open(os.path.join(STORE, "..", "conf-%d.json" % os.getpid()), "w") as f: json.dump(conf, f)
    def connect(self): stage(env("FAKE_CONNECT"))
    def get_instance_id(self): return 4123
    def open_ioctx(self, pool):
        stage(env("FAKE_IOCTX"))
        return Ioctx()
    def shutdown(self): pass
`

type monLossFake struct {
	t      *testing.T
	dir    string
	ledger string
	env    []string
}

func newMonLossFake(t *testing.T, env ...string) *monLossFake {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Fatal("the client fixture needs python3:", err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "rados.py"), []byte(monLossFakeRados), 0o600); err != nil {
		t.Fatal(err)
	}
	store := filepath.Join(dir, "store")
	if err := os.Mkdir(store, 0o700); err != nil {
		t.Fatal(err)
	}
	f := &monLossFake{t: t, dir: dir, ledger: filepath.Join(dir, "ledger"), env: []string{"FAKE_STORE=" + store}}
	f.output("seed") // the seed is written before any scripted failure applies
	f.env = append(f.env, env...)
	return f
}

func (f *monLossFake) command(mode string, extra ...string) *exec.Cmd {
	args := append([]string{"-I", "-c", "import sys; sys.path.insert(0, sys.argv.pop(1)); exec(sys.argv.pop(1))", f.dir, monLossClientScript, mode, "pool", f.ledger, "fake-run"}, extra...)
	cmd := exec.Command("python3", args...)
	cmd.Env = append(os.Environ(), f.env...)
	return cmd
}

func (f *monLossFake) output(mode string, extra ...string) []byte {
	f.t.Helper()
	out, err := f.command(mode, extra...).Output()
	if err != nil {
		var exit *exec.ExitError
		stderr := ""
		if errors.As(err, &exit) {
			stderr = string(exit.Stderr)
		}
		f.t.Fatalf("%s: %v: %s %s", mode, err, out, stderr)
	}
	return out
}

func (f *monLossFake) token(value string) { f.t.Helper(); f.output("token", value) }

func (f *monLossFake) raw() []byte {
	f.t.Helper()
	return f.output("ledger")
}

func (f *monLossFake) parse() *monLossLedger {
	f.t.Helper()
	ledger, err := parseMonLossLedger(f.raw(), "fake-run")
	if err != nil {
		f.t.Fatal(err)
	}
	return ledger
}

func (f *monLossFake) start() (*exec.Cmd, chan error) {
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

func (f *monLossFake) waitFor(what string, check func(*monLossLedger) bool) *monLossLedger {
	f.t.Helper()
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		if _, err := os.Stat(filepath.Join(f.ledger, "ledger.jsonl")); err == nil {
			if ledger := f.parse(); check(ledger) {
				return ledger
			}
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func (f *monLossFake) readback(ledger *monLossLedger) monLossReadback {
	f.t.Helper()
	var names []string
	for _, op := range ledger.Writes() {
		names = append(names, op.Object)
	}
	list, _ := json.Marshal(append([]string{monLossSeed}, names...))
	var rb monLossReadback
	if err := json.Unmarshal(f.output("read", string(list)), &rb); err != nil {
		f.t.Fatal(err)
	}
	return rb
}

func (f *monLossFake) stop(done chan error) {
	f.t.Helper()
	f.token(monLossPhaseStop)
	select {
	case err := <-done:
		if err != nil {
			f.t.Fatal("the retained client did not end cleanly:", err)
		}
	case <-time.After(30 * time.Second):
		f.t.Fatal("the retained client ignored the stop token")
	}
}

func acknowledgedUnder(kind, phase string, want int) func(*monLossLedger) bool {
	return func(l *monLossLedger) bool { return l.StayedAcked(kind, phase) >= want }
}

// TestMonLossClientRetainedLedger drives the retained client through every
// phase token. Reads and writes are separate calls with their own records,
// one process holds one session for all of them, each write goes out once under
// its own name, the client passes bounded librados timeouts, and the readback
// of the independently derived bytes matches every acknowledgement.
func TestMonLossClientRetainedLedger(t *testing.T) {
	fake := newMonLossFake(t)
	fake.token(monLossPhaseBaseline)
	_, done := fake.start()
	for _, step := range []string{monLossPhaseBaseline, monLossPhaseMinority, monLossPhaseOutage, monLossPhaseRestoring, monLossPhaseRecovered} {
		fake.token(step)
		fake.waitFor(step, func(l *monLossLedger) bool {
			return acknowledgedUnder("read", step, 2)(l) && acknowledgedUnder("write", step, 2)(l)
		})
	}
	fake.stop(done)
	ledger := fake.parse()
	if !ledger.Stopped || !ledger.Session || ledger.Instance != 4123 {
		t.Fatalf("session or stop record missing: %+v", ledger)
	}
	order := []string{monLossPhaseBaseline, monLossPhaseMinority, monLossPhaseOutage, monLossPhaseRestoring, monLossPhaseRecovered}
	last, reads, writes := 0, 0, 0
	for _, call := range ledger.Calls {
		at := slices.Index(order, call.Phase)
		if at < last {
			t.Fatalf("call %s began under %q after a later token", call.Op, call.Phase)
		}
		last = at
		if call.Outcome() != "acked" || call.IntentNS >= call.ReturnNS {
			t.Fatalf("unexpected record %+v", call)
		}
		switch call.Kind {
		case "read":
			reads++
		case "write":
			writes++
		}
	}
	if reads == 0 || writes == 0 || reads < writes {
		t.Fatalf("reads %d writes %d: each cycle reads once and then writes once", reads, writes)
	}
	unconfirmed, err := monLossVerifyReadback(ledger, fake.readback(ledger))
	if err != nil || unconfirmed != 0 {
		t.Fatalf("independent readback: %d %v", unconfirmed, err)
	}
	data, err := os.ReadFile(filepath.Join(fake.dir, fmt.Sprintf("conf-%d.json", ledger.PID)))
	if err != nil {
		t.Fatal(err)
	}
	// The three native timeouts that bound the calls must all be set and nonzero.
	var conf map[string]string
	if err := json.Unmarshal(data, &conf); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"client_mount_timeout", "rados_mon_op_timeout", "rados_osd_op_timeout"} {
		if conf[key] == "" || conf[key] == "0" {
			t.Fatalf("%s is unbounded in the client configuration %v", key, conf)
		}
	}
}

// TestMonLossClientOutcomesStaySeparate checks that a timed-out read, a write
// that failed before it applied and a write that failed after it applied are
// three distinct, classified records: none is replayed, none becomes an
// acknowledgement, a later call continues under a new name, and the readback
// only reports (never promotes) the unconfirmed write that took effect.
func TestMonLossClientOutcomesStaySeparate(t *testing.T) {
	var unconfirmed [2]int
	for i, mode := range []string{"before", "after"} {
		fake := newMonLossFake(t, "FAKE_WRITE_SEQ=1", "FAKE_WRITE_MODE="+mode, "FAKE_READ_NTH=2", "FAKE_READ_MODE=timeout")
		fake.token(monLossPhaseOutage)
		_, done := fake.start()
		fake.waitFor("calls after the errors", func(l *monLossLedger) bool { return len(l.Calls) >= 7 && l.Calls[6].Returned })
		fake.stop(done)
		ledger := fake.parse()
		seen := map[string]bool{}
		for _, call := range ledger.Calls {
			if seen[call.Op] {
				t.Fatalf("%s: call %s was submitted twice", mode, call.Op)
			}
			seen[call.Op] = true
		}
		// Cycle 0: read, write; cycle 1: read (times out), write (fails); cycle 2 continues.
		if ledger.Calls[2].Detail() != "timeout" || ledger.Calls[2].Kind != "read" {
			t.Fatalf("%s: second read %+v", mode, ledger.Calls[2])
		}
		if ledger.Calls[3].Detail() != "timeout" || ledger.Calls[3].Kind != "write" {
			t.Fatalf("%s: second write %+v", mode, ledger.Calls[3])
		}
		for _, call := range append(slices.Clone(ledger.Calls[:2]), ledger.Calls[4:7]...) {
			if call.Outcome() != "acked" {
				t.Fatalf("%s: %+v should be acknowledged", mode, call)
			}
		}
		var err error
		if unconfirmed[i], err = monLossVerifyReadback(ledger, fake.readback(ledger)); err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		if got := ledger.Tally()["write:outage/timeout"]; got != 1 {
			t.Fatalf("%s: tally %v", mode, ledger.Tally())
		}
	}
	// The ledger cannot tell the two writes apart; the object store can.
	if unconfirmed != [2]int{0, 1} {
		t.Fatalf("unconfirmed writes that took effect: %v, want [0 1]", unconfirmed)
	}
}

// TestMonLossClientInFlightCall kills the client while a write is in flight.
// The intent stays with no return, and the call is neither a success nor a
// failure; a later readback neither replays nor promotes it.
func TestMonLossClientInFlightCall(t *testing.T) {
	fake := newMonLossFake(t, "FAKE_WRITE_SEQ=1", "FAKE_WRITE_MODE=hang")
	fake.token(monLossPhaseOutage)
	cmd, done := fake.start()
	fake.waitFor("the hung write", func(l *monLossLedger) bool { return len(l.Calls) == 4 })
	time.Sleep(300 * time.Millisecond)
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil {
		t.Fatal("a killed client reported a clean exit")
	}
	ledger := fake.parse()
	if ledger.Stopped || len(ledger.Calls) != 4 || ledger.Calls[3].Outcome() != "inflight" || ledger.Calls[3].Kind != "write" {
		t.Fatalf("records %+v", ledger.Calls)
	}
	if tally := ledger.Tally(); tally["write:outage/inflight"] != 1 || tally["write:outage/acked"] != 1 || tally["read:outage/acked"] != 2 {
		t.Fatalf("tally %v", tally)
	}
	unconfirmed, err := monLossVerifyReadback(ledger, fake.readback(ledger))
	if err != nil || unconfirmed != 0 {
		t.Fatalf("in-flight readback: %d %v", unconfirmed, err)
	}
	if _, err := os.Stat(filepath.Join(fake.dir, "store", "mlc-"+ledger.Calls[3].Op)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("the hung write left an object")
	}
}

// TestMonLossClientReadbackRefusals proves the oracle rejects an acknowledged
// write that is gone or holds other bytes, a seed that changed, an object no
// intent names (a replay or stray write), and a read that returned a success
// with other bytes.
func TestMonLossClientReadbackRefusals(t *testing.T) {
	for _, mode := range []string{"lose", "corrupt"} {
		fake := newMonLossFake(t, "FAKE_WRITE_SEQ=1", "FAKE_WRITE_MODE="+mode)
		fake.token(monLossPhaseOutage)
		_, done := fake.start()
		fake.waitFor("writes", func(l *monLossLedger) bool { return len(l.Calls) >= 5 && l.Calls[4].Returned })
		fake.stop(done)
		ledger := fake.parse()
		if ledger.Calls[3].Outcome() != "acked" {
			t.Fatalf("%s: the client must see a success: %+v", mode, ledger.Calls[3])
		}
		if _, err := monLossVerifyReadback(ledger, fake.readback(ledger)); err == nil {
			t.Fatalf("%s: an acknowledged write that is lost or altered passed the readback", mode)
		}
	}
	fake := newMonLossFake(t)
	fake.token(monLossPhaseOutage)
	_, done := fake.start()
	fake.waitFor("writes", acknowledgedUnder("write", monLossPhaseOutage, 2))
	fake.stop(done)
	ledger := fake.parse()
	if _, err := monLossVerifyReadback(ledger, fake.readback(ledger)); err != nil {
		t.Fatal("control readback:", err)
	}
	store := filepath.Join(fake.dir, "store")
	if err := os.WriteFile(filepath.Join(store, "mlc-fake-run-w99999"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := monLossVerifyReadback(ledger, fake.readback(ledger)); err == nil {
		t.Fatal("an object no intent names passed the readback")
	}
	_ = os.Remove(filepath.Join(store, "mlc-fake-run-w99999"))
	if err := os.WriteFile(filepath.Join(store, monLossSeed), []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := monLossVerifyReadback(ledger, fake.readback(ledger)); err == nil {
		t.Fatal("a changed seed passed the readback")
	}
	// A seed read that returns a success with other bytes is refused at parse time.
	bad := newMonLossFake(t, "FAKE_READ_NTH=1", "FAKE_READ_MODE=corrupt")
	bad.token(monLossPhaseOutage)
	cmd, doneBad := bad.start()
	var raw []byte
	for deadline := time.Now().Add(30 * time.Second); strings.Count(string(raw), "\n") < 4; time.Sleep(50 * time.Millisecond) {
		raw, _ = os.ReadFile(filepath.Join(bad.ledger, "ledger.jsonl"))
		if time.Now().After(deadline) {
			t.Fatal("the corrupted read left no return record")
		}
	}
	_ = cmd.Process.Kill()
	if err := <-doneBad; err == nil {
		t.Fatal("a killed client reported a clean exit")
	}
	if _, err := parseMonLossLedger(raw, "fake-run"); err == nil {
		t.Fatal("a successful read with other bytes was accepted")
	}
}

// probe runs one fresh probe against the fake and returns its parsed record.
func (f *monLossFake) probe(name string, watchdog int) *monLossProbe {
	f.t.Helper()
	f.output("fresh", name, fmt.Sprint(watchdog))
	ledger := f.parse()
	probe := ledger.Probes[name]
	if probe == nil || !probe.Ended {
		f.t.Fatalf("probe %s left no end record: %+v", name, probe)
	}
	return probe
}

// TestMonLossFreshProbeStages characterizes the fresh-connection probe with
// deterministic controls: a connect that times out, an explicit refusal, a
// connect that never returns, a connect that succeeds and is followed by an
// ioctx or read that stalls, and a read that times out. The connect result is
// published before any later stage, a stall is recorded as pending in its own
// stage, and the watchdog kills and reaps the process while preserving the
// records before it.
func TestMonLossFreshProbeStages(t *testing.T) {
	for _, tc := range []struct {
		name    string
		env     []string
		verdict string
		ok      bool
		killed  bool
		check   func(*testing.T, *monLossProbe)
	}{
		{name: "all-ok", verdict: "connect=ok ioctx=ok read=ok", ok: true, check: func(t *testing.T, p *monLossProbe) {
			if p.Stage("connect").Instance != 4123 {
				t.Fatal("connect did not publish the instance identity")
			}
		}},
		{name: "connect-timeout", env: []string{"FAKE_CONNECT=timeout"}, verdict: "connect=timeout ioctx=- read=-"},
		{name: "connect-refused", env: []string{"FAKE_CONNECT=refuse"}, verdict: "connect=error(PermissionDeniedError/13) ioctx=- read=-", check: func(t *testing.T, p *monLossProbe) {
			if p.Stage("connect").State() == "timeout" {
				t.Fatal("an explicit error was classified as a timeout")
			}
		}},
		{name: "connect-hangs", env: []string{"FAKE_CONNECT=hang"}, verdict: "connect=pending ioctx=- read=- watchdog_killed(reaped=true)", killed: true},
		{name: "ioctx-hangs", env: []string{"FAKE_IOCTX=hang"}, verdict: "connect=ok ioctx=pending read=- watchdog_killed(reaped=true)", killed: true, check: func(t *testing.T, p *monLossProbe) {
			// The ambiguity of the older probe: a stall after a successful
			// connect must not look like a connect that never returned.
			if p.Stage("connect").State() != "ok" || p.Stage("ioctx").State() != "pending" {
				t.Fatal("a stall after connect was attributed to the connection")
			}
		}},
		{name: "ioctx-timeout", env: []string{"FAKE_IOCTX=timeout"}, verdict: "connect=ok ioctx=timeout read=-"},
		{name: "read-timeout", env: []string{"FAKE_READ_NTH=1", "FAKE_READ_MODE=timeout"}, verdict: "connect=ok ioctx=ok read=timeout"},
		{name: "read-hangs", env: []string{"FAKE_READ_NTH=1", "FAKE_READ_MODE=hang"}, verdict: "connect=ok ioctx=ok read=pending watchdog_killed(reaped=true)", killed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newMonLossFake(t, tc.env...)
			// The seed is written by newMonLossFake with a clean environment.
			probe := fake.probe(tc.name, 3)
			if got := probe.Verdict(); got != tc.verdict {
				t.Fatalf("verdict %q, want %q", got, tc.verdict)
			}
			if probe.AllOK() != tc.ok || probe.Killed != tc.killed || !probe.Reaped || probe.Crashed {
				t.Fatalf("probe end %+v", probe)
			}
			if tc.killed {
				if probe.Exit == nil || *probe.Exit != -int(syscall.SIGKILL) {
					t.Fatalf("a watchdog-killed probe exited %v, want SIGKILL", probe.Exit)
				}
			}
			if tc.check != nil {
				tc.check(t, probe)
			}
		})
	}
}

// fakeLedger builds synthetic records for the refusal controls.
type fakeLedgerEvent = map[string]any

func monLossRecord(event string, kv ...any) fakeLedgerEvent {
	e := fakeLedgerEvent{"event": event, "ns": 1, "pid": 11, "run": "r"}
	for i := 0; i < len(kv); i += 2 {
		e[kv[i].(string)] = kv[i+1]
	}
	return e
}

func monLossLines(evs ...fakeLedgerEvent) []byte {
	var b strings.Builder
	for _, e := range evs {
		data, _ := json.Marshal(e)
		b.Write(data)
		b.WriteByte('\n')
	}
	return []byte(b.String())
}

// monLossValid is a retained session with a read and a write, then a probe
// whose three stages succeeded.
func monLossValid() []fakeLedgerEvent {
	write := outagePayload("r-w00000", 0)
	return []fakeLedgerEvent{
		monLossRecord("token", "value", "baseline"),
		monLossRecord("session", "who", "retained", "instance", 4123, "phase", "baseline"),
		monLossRecord("intent", "who", "retained", "kind", "read", "op", "r-r00000", "phase", "baseline"),
		monLossRecord("return", "who", "retained", "kind", "read", "op", "r-r00000", "ok", true, "phase_after", "baseline", "ms", 3,
			"sha256", monLossSeedDigest, "len", monLossSeedLen()),
		monLossRecord("intent", "who", "retained", "kind", "write", "op", "r-w00000", "phase", "baseline",
			"object", "mlc-r-w00000", "seq", 0, "bytes", len(write), "sha256", outageDigest(write)),
		monLossRecord("return", "who", "retained", "kind", "write", "op", "r-w00000", "ok", false, "phase_after", "outage", "ms", 15000,
			"exc", "TimedOut", "errno", 110, "message", "timed out"),
		monLossRecord("stopped", "who", "retained", "reads", 1, "writes", 1),
		monLossRecord("probe_start", "who", "p", "watchdog_s", 10.0, "pid", 20),
		monLossRecord("intent", "who", "p", "kind", "connect", "op", "p-connect", "phase", "outage", "pid", 21),
		monLossRecord("return", "who", "p", "kind", "connect", "op", "p-connect", "ok", true, "phase_after", "outage", "ms", 5, "instance", 4124, "pid", 21),
		monLossRecord("intent", "who", "p", "kind", "ioctx", "op", "p-ioctx", "phase", "outage", "pid", 21),
		monLossRecord("return", "who", "p", "kind", "ioctx", "op", "p-ioctx", "ok", false, "phase_after", "outage", "ms", 15000, "exc", "TimedOut", "errno", 110, "pid", 21),
		monLossRecord("probe_end", "who", "p", "killed", false, "reaped", true, "exit", 0, "child", 21, "pid", 20),
	}
}

func cloneEvents(evs []fakeLedgerEvent) []fakeLedgerEvent {
	result := make([]fakeLedgerEvent, len(evs))
	for i, e := range evs {
		result[i] = maps.Clone(e)
	}
	return result
}

// TestMonLossLedgerAcceptsValid is the control of the refusal tests: the
// synthetic ledger parses into the separate records they mutate.
func TestMonLossLedgerAcceptsValid(t *testing.T) {
	ledger, err := parseMonLossLedger(monLossLines(monLossValid()...), "r")
	if err != nil {
		t.Fatal(err)
	}
	p := ledger.Probes["p"]
	if !ledger.Stopped || len(ledger.Calls) != 2 || ledger.Calls[1].Key() != "write:baseline>outage/timeout" || p.Verdict() != "connect=ok ioctx=timeout read=-" {
		t.Fatalf("%+v %v %s", ledger.Calls, ledger.Tally(), p.Verdict())
	}
	if ledger.Instance == p.Stage("connect").Instance {
		t.Fatal("the fresh probe reused the retained client's identity")
	}
	empty, err := parseMonLossLedger(nil, "r")
	if err != nil || empty.Session || len(empty.Calls) != 0 {
		t.Fatalf("an empty ledger is no observation of any call: %+v %v", empty, err)
	}
	// An error without an errno is a legitimate record (a non-librados error);
	// it is not a timeout.
	events := cloneEvents(monLossValid())
	delete(events[5], "errno")
	if l, err := parseMonLossLedger(monLossLines(events...), "r"); err != nil || l.Calls[1].Detail() != "error(TimedOut)" {
		t.Fatalf("errno is optional but never implied: %v %+v", err, l)
	}
	events[5]["errno"] = nil
	if l, err := parseMonLossLedger(monLossLines(events...), "r"); err != nil || l.Calls[1].Detail() == "timeout" {
		t.Fatalf("a null errno was read as a timeout: %v", err)
	}
}

// TestMonLossLedgerRefusals pairs missing, null and wrong-typed required
// fields, each of which must make the observation inconclusive rather than
// decode into an empty value, with the structural refusals.
func TestMonLossLedgerRefusals(t *testing.T) {
	required := map[int][]string{
		1:  {"instance", "who", "phase"},
		2:  {"phase"},
		3:  {"ok", "phase_after", "ms", "sha256", "len"},
		4:  {"seq", "bytes", "sha256", "phase", "object"},
		5:  {"ok", "exc", "phase_after", "ms"},
		6:  {"reads", "writes"},
		7:  {"watchdog_s"},
		9:  {"ok", "ms", "instance"},
		11: {"ok", "ms", "exc"},
		12: {"killed", "reaped", "child"},
	}
	for index, fields := range required {
		for _, field := range fields {
			for _, how := range []string{"missing", "null", "wrong-type"} {
				events := cloneEvents(monLossValid())
				switch how {
				case "missing":
					delete(events[index], field)
				case "null":
					events[index][field] = nil
				default:
					if _, isString := events[index][field].(string); isString {
						events[index][field] = 7
					} else {
						events[index][field] = "x"
					}
				}
				if _, err := parseMonLossLedger(monLossLines(events...), "r"); err == nil {
					t.Errorf("record %d accepted with %s %s", index, how, field)
				}
			}
		}
	}
	for index := range monLossValid() {
		for _, field := range []string{"ns", "pid", "run"} {
			for _, how := range []string{"missing", "null"} {
				events := cloneEvents(monLossValid())
				if how == "missing" {
					delete(events[index], field)
				} else {
					events[index][field] = nil
				}
				if _, err := parseMonLossLedger(monLossLines(events...), "r"); err == nil {
					t.Errorf("record %d accepted with %s %s", index, how, field)
				}
			}
		}
	}
	for name, mutate := range map[string]func([]fakeLedgerEvent) []fakeLedgerEvent{
		"another run":   func(e []fakeLedgerEvent) []fakeLedgerEvent { e[3]["run"] = "other"; return e },
		"unknown field": func(e []fakeLedgerEvent) []fakeLedgerEvent { e[3]["surprise"] = 1; return e },
		"second session": func(e []fakeLedgerEvent) []fakeLedgerEvent {
			return append(e[:2], append([]fakeLedgerEvent{e[1]}, e[2:]...)...)
		},
		"call before the session":     func(e []fakeLedgerEvent) []fakeLedgerEvent { e[1], e[2] = e[2], e[1]; return e },
		"return without intent":       func(e []fakeLedgerEvent) []fakeLedgerEvent { return append(e[:2], e[3:]...) },
		"repeated write":              func(e []fakeLedgerEvent) []fakeLedgerEvent { e[4]["seq"] = 3; return e },
		"misnamed write":              func(e []fakeLedgerEvent) []fakeLedgerEvent { e[4]["op"] = "r-w00009"; return e },
		"write digest off the oracle": func(e []fakeLedgerEvent) []fakeLedgerEvent { e[4]["sha256"] = strings.Repeat("0", 64); return e },
		"seed read with other bytes":  func(e []fakeLedgerEvent) []fakeLedgerEvent { e[3]["sha256"] = strings.Repeat("0", 64); return e },
		"stopped miscount":            func(e []fakeLedgerEvent) []fakeLedgerEvent { e[6]["writes"] = 2; return e },
		"second retained process":     func(e []fakeLedgerEvent) []fakeLedgerEvent { e[4]["pid"] = 12; return e },
		"probe stage after failure":   func(e []fakeLedgerEvent) []fakeLedgerEvent { e[9]["ok"] = false; e[9]["exc"] = "TimedOut"; return e },
		"probe stage out of order":    func(e []fakeLedgerEvent) []fakeLedgerEvent { e[8]["kind"] = "ioctx"; return e },
		"probe return unmatched":      func(e []fakeLedgerEvent) []fakeLedgerEvent { e[9]["kind"] = "ioctx"; return e },
		"probe end without start":     func(e []fakeLedgerEvent) []fakeLedgerEvent { e[12]["who"] = "q"; return e },
		"probe from two processes":    func(e []fakeLedgerEvent) []fakeLedgerEvent { e[10]["pid"] = 22; return e },
		"reaped without an exit":      func(e []fakeLedgerEvent) []fakeLedgerEvent { delete(e[12], "exit"); return e },
	} {
		if _, err := parseMonLossLedger(monLossLines(mutate(cloneEvents(monLossValid()))...), "r"); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	valid := monLossLines(monLossValid()...)
	for name, raw := range map[string][]byte{
		"truncated final record": valid[:len(valid)-9],
		"missing final newline":  valid[:len(valid)-1],
		"trailing data":          append(slices.Clone(valid[:len(valid)-1]), []byte(` {"event":"token"}`+"\n")...),
		"not JSON":               []byte("garbage\n"),
	} {
		if _, err := parseMonLossLedger(raw, "r"); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	// A read that has an intent and no return is pending, not refused.
	pending := monLossValid()[:3]
	if l, err := parseMonLossLedger(monLossLines(pending...), "r"); err != nil || len(l.Calls) != 1 || l.Calls[0].Outcome() != "inflight" {
		t.Fatalf("an open call: %v %+v", err, l)
	}
}

// TestMonLossLedgerTallyKeepsTransitions pins how a call is credited: a call
// that returned under another token than it began under is tallied apart.
func TestMonLossLedgerTallyKeepsTransitions(t *testing.T) {
	l := &monLossLedger{Calls: []monLossCall{
		{Kind: "read", Phase: "outage", PhaseAfter: "outage", Returned: true, OK: true},
		{Kind: "read", Phase: "outage", PhaseAfter: "restoring", Returned: true, OK: true},
		{Kind: "write", Phase: "outage", PhaseAfter: "outage", Returned: true},
		{Kind: "write", Phase: "outage"},
	}}
	if got, want := monLossTallyString(l.Tally()), "read:outage/acked=1 read:outage>restoring/acked=1 write:outage/error()=1 write:outage/inflight=1"; got != want {
		t.Fatalf("tally %q, want %q", got, want)
	}
	if l.StayedAcked("read", "outage") != 1 {
		t.Fatal("a call that crossed a token was credited to the phase it began in")
	}
}

// TestMonLossFreshProbeCorruptRead proves that a fresh probe whose read
// "succeeds" with other bytes leaves a ledger that is refused, not a probe
// that counts as a working read.
func TestMonLossFreshProbeCorruptRead(t *testing.T) {
	fake := newMonLossFake(t, "FAKE_READ_NTH=1", "FAKE_READ_MODE=corrupt")
	fake.output("fresh", "corrupt", "3")
	if _, err := parseMonLossLedger(fake.raw(), "fake-run"); err == nil {
		t.Fatal("a fresh read that returned other bytes was accepted")
	}
}

// TestMonLossPollHonorsPhaseBound pins the bounded observation loop: a ledger
// that is ready in time is accepted, one that becomes ready only after the
// phase bound is not (it is returned as the last view), a slow read is cut by
// the derived context, and the delay between polls honors the same bound.
func TestMonLossPollHonorsPhaseBound(t *testing.T) {
	ready := &monLossLedger{Session: true}
	accept := func(l *monLossLedger) bool { return l.Session }
	read := func(delay time.Duration, l *monLossLedger) func(context.Context) (*monLossLedger, error) {
		return func(ctx context.Context) (*monLossLedger, error) {
			select {
			case <-time.After(delay):
				return l, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	if _, ok, err := monLossPoll(t.Context(), time.Second, 5*time.Millisecond, read(0, ready), accept); !ok || err != nil {
		t.Fatalf("a timely ready ledger was not accepted: %v %v", ok, err)
	}
	// Ready, but only after the bound: the read ignores the context and
	// returns a ready snapshot late, as a slow exec that finished anyway.
	late := func(context.Context) (*monLossLedger, error) { time.Sleep(60 * time.Millisecond); return ready, nil }
	if last, ok, _ := monLossPoll(t.Context(), 20*time.Millisecond, 5*time.Millisecond, late, accept); ok || last != ready {
		t.Fatalf("a snapshot ready after the bound was accepted (ok=%t) or lost (last=%v)", ok, last)
	}
	// Never ready: bounded, and the polling delay does not outlast the bound.
	started := time.Now()
	if _, ok, _ := monLossPoll(t.Context(), 30*time.Millisecond, time.Hour, read(0, &monLossLedger{}), accept); ok || time.Since(started) > 2*time.Second {
		t.Fatalf("an unready ledger was accepted or the delay ignored the bound (%v)", time.Since(started))
	}
	// A read that blocks is cut by the derived context and reports its error.
	started = time.Now()
	if _, ok, err := monLossPoll(t.Context(), 30*time.Millisecond, time.Millisecond, read(time.Hour, ready), accept); ok || err == nil || time.Since(started) > 2*time.Second {
		t.Fatalf("a blocked read outlived the bound: ok=%t err=%v after %v", ok, err, time.Since(started))
	}
}

// TestMonLossFreshProbeCancelledLauncher cancels the launcher the way the
// native supervisor does, by killing its whole process group, while the probe
// is stalled in connect(). The probe must be inside that group and die with
// it, leaving the connect intent as a pending stage and no end record.
func TestMonLossFreshProbeCancelledLauncher(t *testing.T) {
	fake := newMonLossFake(t, "FAKE_CONNECT=hang")
	cmd := fake.command("fresh", "cancelled", "60")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	t.Cleanup(func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) })
	var child int
	for deadline := time.Now().Add(30 * time.Second); child == 0; time.Sleep(50 * time.Millisecond) {
		raw, _ := os.ReadFile(filepath.Join(fake.ledger, "ledger.jsonl"))
		for _, line := range strings.Split(string(raw), "\n") {
			var event monLossEvent
			if json.Unmarshal([]byte(line), &event) == nil && event.Event == "intent" && event.Kind == "connect" && event.PID != nil {
				child = *event.PID
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("the probe never recorded its connect intent")
		}
	}
	if child == cmd.Process.Pid {
		t.Fatal("the connect intent came from the launcher, not a child")
	}
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if err := <-waited; err == nil {
		t.Fatal("a killed launcher reported a clean exit")
	}
	gone := false
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if err := syscall.Kill(child, 0); errors.Is(err, syscall.ESRCH) {
			gone = true
			break
		}
	}
	if !gone {
		_ = syscall.Kill(child, syscall.SIGKILL)
		t.Fatal("the probe survived the cancellation of its launcher's process group")
	}
	probe := fake.parse().Probes["cancelled"]
	if probe == nil || probe.Ended || probe.Verdict() != "connect=pending ioctx=- read=-" {
		t.Fatalf("partial records were not preserved: %+v", probe)
	}
}
