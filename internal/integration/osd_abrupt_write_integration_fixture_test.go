//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"bytes"
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

func TestOSDAbruptOverlapClassification(t *testing.T) {
	valid := []abruptWriteEvent{
		{Event: "intent", Object: "fresh", NS: 10}, {Event: "api_return", Object: "fresh", NS: 11},
		{Event: "intent", Object: "replacement", NS: 12}, {Event: "api_return", Object: "replacement", NS: 13},
	}
	for _, tc := range []struct {
		name  string
		extra abruptWriteEvent
		want  bool
	}{
		{"missing_completion_is_unknown", abruptWriteEvent{}, true},
		{"success_after_exit", abruptWriteEvent{Event: "completed", Object: "fresh", NS: 31, Ret: 0}, true},
		{"failure_after_exit", abruptWriteEvent{Event: "completed", Object: "fresh", NS: 31, Ret: -110}, true},
		{"callback_after_exit", abruptWriteEvent{Event: "callback", Object: "fresh", CallbackNS: 32}, true},
		{"completed_before_kill", abruptWriteEvent{Event: "completed", Object: "fresh", NS: 19}, false},
		{"completion_during_kill_rpc", abruptWriteEvent{Event: "completed", Object: "fresh", NS: 25}, false},
		{"completion_at_exit", abruptWriteEvent{Event: "completed", Object: "fresh", NS: 30}, false},
		{"callback_before_exit", abruptWriteEvent{Event: "callback", Object: "fresh", CallbackNS: 22}, false},
		{"callback_without_clock", abruptWriteEvent{Event: "callback", Object: "fresh"}, false},
		{"unknown_completion", abruptWriteEvent{Event: "completed", Object: "other", NS: 31}, false},
		{"replayed_submission", abruptWriteEvent{Event: "intent", Object: "fresh", NS: 14}, false},
		{"api_error", abruptWriteEvent{Event: "api_return", Object: "other", NS: 14, Error: "boom"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events := append(append([]abruptWriteEvent{}, valid...), tc.extra)
			if tc.extra.Event == "" {
				events = valid
			}
			if err := abruptOverlap(events, 20, 30); (err == nil) != tc.want {
				t.Fatalf("error=%v want valid=%t", err, tc.want)
			}
		})
	}
	if abruptOverlap(valid[:2], 20, 30) == nil || abruptOverlap(valid, 30, 20) == nil {
		t.Fatal("missing submission or clock reversal passed")
	}
	// An API return after the kill request means the call was still running.
	late := append([]abruptWriteEvent{}, valid...)
	late[3].NS = 21
	if abruptOverlap(late, 20, 30) == nil {
		t.Fatal("a submission that returned after the kill request passed")
	}
	// An intent without an API return is not a submission.
	if abruptOverlap([]abruptWriteEvent{valid[0], valid[2], valid[3]}, 20, 30) == nil {
		t.Fatal("an intent without an API return passed")
	}
}

func TestOSDAbruptChannelIsSeparateFromOutcomes(t *testing.T) {
	one := abruptWriteEvent{Event: "channel", Callbacks: []string{"fresh"}}
	for name, tc := range map[string]struct {
		events []abruptWriteEvent
		want   string
	}{
		"complete":         {[]abruptWriteEvent{one, one}, "complete"},
		"one report":       {[]abruptWriteEvent{one}, "incomplete"},
		"no report":        {[]abruptWriteEvent{{Event: "intent"}}, "missing"},
		"outstanding":      {[]abruptWriteEvent{{Event: "channel", Pending: []string{"fresh"}, Stopped: true}}, "incomplete"},
		"missing callback": {[]abruptWriteEvent{one, {Event: "channel"}}, "incomplete"},
	} {
		if got := abruptChannel(tc.events); got != tc.want {
			t.Errorf("%s: %s, want %s", name, got, tc.want)
		}
	}
}

func TestOSDAbruptSingleCopyWitness(t *testing.T) {
	// While a request is held in dispatch it is undecoded: the OSD prints its
	// client request ID, PG and flags, but neither object nor operations.
	undecoded := func(client, tid, pg, flag string) map[string]any {
		return map[string]any{
			"description": "osd_op(" + client + ".0:" + tid + " " + pg + " " + pg + " (undecoded) ondisk+write+known_if_redirected e12)",
			"type_data":   map[string]any{"flag_point": flag, "events": []map[string]string{{"event": "initiated"}, {"event": "throttled"}}},
		}
	}
	decoded := func(client, tid, object, pg, flag string) map[string]any {
		return map[string]any{
			"description": "osd_op(" + client + ".0:" + tid + " " + pg + " 1:abcdef:::" + object + ":head [writefull 0~65539] snapc 0=[] ondisk+write+known_if_redirected e12)",
			"type_data":   map[string]any{"flag_point": flag, "events": []map[string]string{{"event": "initiated"}}},
		}
	}
	dump := func(ops ...map[string]any) []byte {
		data, _ := json.Marshal(map[string]any{"ops": ops})
		return data
	}
	// A single-copy write is never "waiting for sub ops"; any flag point is kept.
	witness, ok, err := abruptFindWitness(dump(undecoded("client.4123", "5", "1.0", "delayed"), undecoded("client.4124", "1", "1.0", "queued")), "fresh", "client.4123", "1.0")
	if err != nil || !ok || witness.Request != "client.4123.0:5" || witness.Flag != "delayed" || len(witness.Events) != 2 || witness.Decoded || witness.Object != "fresh" {
		t.Fatalf("undecoded witness %+v %v %v", witness, ok, err)
	}
	witness, ok, err = abruptFindWitness(dump(decoded("client.4123", "5", "fresh", "1.0", "")), "fresh", "client.4123", "1.0")
	if err != nil || !ok || !witness.Decoded {
		t.Fatalf("decoded witness %+v %v %v", witness, ok, err)
	}
	if _, ok, err := abruptFindWitness(dump(undecoded("client.4124", "1", "1.0", "")), "fresh", "client.4123", "1.0"); ok || err != nil {
		t.Fatalf("a request of another client is no witness: %v %v", ok, err)
	}
	if _, ok, err := abruptFindWitness(dump(undecoded("client.41230", "1", "1.0", "")), "fresh", "client.4123", "1.0"); ok || err != nil {
		t.Fatalf("a client ID that only shares a prefix is no witness: %v %v", ok, err)
	}
	for name, tc := range map[string]struct {
		data []byte
		want string
	}{
		"ambiguous":    {dump(undecoded("client.4123", "5", "1.0", ""), undecoded("client.4123", "7", "1.0", "")), "2 requests"},
		"other PG":     {dump(undecoded("client.4123", "5", "2.0", "")), "not a write to PG"},
		"not a write":  {[]byte(`{"ops":[{"description":"osd_op(client.4123.0:5 1.0 1.0 (undecoded) read e12)"}]}`), "not a write"},
		"other object": {dump(decoded("client.4123", "5", "replacement", "1.0", "")), "another object"},
		"unreadable":   {[]byte(`not json`), "dump_ops_in_flight"},
	} {
		if _, ok, err := abruptFindWitness(tc.data, "fresh", "client.4123", "1.0"); ok || err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: ok=%t err=%v, want an error with %q", name, ok, err, tc.want)
		}
	}
}

// fakeRados stands in for the rados module with scheduled completions, so that
// the writer's ledger can be checked without a cluster. It tests the writer's
// bookkeeping only, not librados or Ceph behavior.
const fakeRados = `import os, threading, time

SCHEDULE = os.environ["FAKE_SCHEDULE"]

class ObjectNotFound(Exception): pass

class Completion:
    def __init__(self, oncomplete):
        self.done = False; self.born = time.monotonic()
        if SCHEDULE == "immediate":
            self.done = True
            if oncomplete: oncomplete(self)
        elif SCHEDULE == "between":
            # The request completes after the writer's last poll: the callback
            # delivers the outcome, and the writer is stopped before it polls again.
            self.done = True
            if oncomplete: oncomplete(self)
        elif SCHEDULE in ("delayed", "late"):
            def finish():
                self.done = True
                time.sleep(0.5 if SCHEDULE == "delayed" else 5)
                if oncomplete: oncomplete(self)
            timer = threading.Timer(0.2, finish); timer.daemon = True; timer.start()
    def is_complete(self): return self.done and SCHEDULE != "between" 
    def get_return_value(self): return 0

class Ioctx:
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def aio_write_full(self, name, data, oncomplete=None):
        if SCHEDULE == "raises": raise RuntimeError("rejected")
        return Completion(oncomplete)

class Rados:
    def __init__(self, **kw): pass
    def connect(self): pass
    def get_instance_id(self): return 4123
    def open_ioctx(self, pool): return Ioctx()
    def shutdown(self): pass
`

func TestOSDAbruptWriterLedgerSchedules(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("the writer's ledger check needs python3")
	}
	for name, tc := range map[string]struct {
		schedule string
		// terminate is how long the writer runs before SIGTERM.
		terminate time.Duration
		want      []string
		channel   string
	}{
		// The callback runs before the post-return record, yet the ledger keeps
		// intent, return, completion and callback as separate facts.
		"immediate callback": {"immediate", 700 * time.Millisecond, []string{"intent", "api_return", "intent", "api_return", "completed", "completed", "callback", "callback", "channel", "channel"}, "complete"},
		// The requests complete after the last poll and the writer stops before
		// it polls again: their outcome is kept, not only their callback.
		"stopped between polls": {"between", 300 * time.Millisecond, []string{"intent", "api_return", "intent", "api_return", "completed", "completed", "callback", "callback", "channel", "channel"}, "complete"},
		// The callback arrives within the writer's bounded wait after the stop.
		"delayed callback": {"delayed", 450 * time.Millisecond, []string{"intent", "api_return", "intent", "api_return", "completed", "completed", "callback", "callback", "channel", "channel"}, "complete"},
		// The completion is observable, but its callback never arrives in time:
		// the outcome is recorded and the channel is reported incomplete.
		"late callback": {"late", 450 * time.Millisecond, []string{"intent", "api_return", "intent", "api_return", "completed", "completed", "channel", "channel"}, "incomplete"},
		// Nothing completes: the stop is acknowledged with both requests pending.
		"never completes": {"never", 300 * time.Millisecond, []string{"intent", "api_return", "intent", "api_return", "channel", "channel"}, "incomplete"},
		// A rejected API call is its own record and is not retried.
		"api error": {"raises", 300 * time.Millisecond, []string{"intent", "api_return", "intent", "api_return", "channel", "channel"}, "incomplete"},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "rados.py"), []byte(fakeRados), 0o600); err != nil {
				t.Fatal(err)
			}
			ledger := filepath.Join(dir, "ledger.jsonl")
			// Both writers share one ledger, as in the scenario.
			var writers []*exec.Cmd
			var stderr bytes.Buffer
			for _, phase := range []string{"actor-fresh", "actor-replacement"} {
				cmd := exec.Command("python3", "-I", "-c", "import sys; sys.path.insert(0, sys.argv.pop(1)); exec(sys.argv.pop(1))", dir, abruptWriteScript, phase, "pool", ledger)
				cmd.Env = append(os.Environ(), "FAKE_SCHEDULE="+tc.schedule)
				cmd.Stderr = &stderr
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				writers = append(writers, cmd)
			}
			time.Sleep(tc.terminate)
			for _, cmd := range writers {
				_ = cmd.Process.Signal(syscall.SIGTERM)
			}
			for _, cmd := range writers {
				done := make(chan error, 1)
				go func() { done <- cmd.Wait() }()
				select {
				case err := <-done:
					if err != nil {
						t.Fatalf("writer: %v %s", err, stderr.String())
					}
				case <-time.After(10 * time.Second):
					_ = cmd.Process.Kill()
					t.Fatal("writer did not acknowledge SIGTERM")
				}
			}
			data, err := os.ReadFile(ledger)
			if err != nil {
				t.Fatal(err)
			}
			var events []abruptWriteEvent
			var kinds []string
			for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
				var event abruptWriteEvent
				if err := json.Unmarshal([]byte(line), &event); err != nil {
					t.Fatal(err)
				}
				events = append(events, event)
				kinds = append(kinds, event.Event)
			}
			if tc.schedule == "raises" {
				// The error is recorded for each submission, which is never retried.
				for _, event := range events {
					if event.Event == "api_return" && event.Error == "" {
						t.Fatalf("a rejected call was recorded as returned: %+v", event)
					}
				}
			}
			slices.Sort(kinds)
			want := slices.Clone(tc.want)
			slices.Sort(want)
			if !slices.Equal(kinds, want) {
				t.Fatalf("events %v, want %v", kinds, want)
			}
			if tc.schedule == "between" {
				for _, event := range events {
					if event.Event == "completed" && (event.Via != "callback" || event.Ret != 0) {
						t.Fatalf("a callback-only outcome was not normalized: %+v", event)
					}
				}
				if len(abruptSuccesses(events)) != 2 {
					t.Fatalf("successes %v", abruptSuccesses(events))
				}
			}
			if got := abruptChannel(events); got != tc.channel {
				t.Fatalf("channel %s, want %s (%+v)", got, tc.channel, events)
			}
			intents := 0
			for _, event := range events {
				if event.Event == "intent" {
					intents++
				}
			}
			if intents != 2 {
				t.Fatalf("%d submission intents, want 2 and no retries", intents)
			}
		})
	}
}

func TestOSDAbruptSameNumber(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		want bool
	}{{"30.000000", "30", true}, {"1", "1.0", true}, {"0.1", "1", false}, {"", "1", false}, {"x", "x", false}} {
		if got := abruptSameNumber(tc.a, tc.b); got != tc.want {
			t.Errorf("%q %q: %t", tc.a, tc.b, got)
		}
	}
}

func TestOSDAbruptVerifiesEverySuccessfulOriginalOutcome(t *testing.T) {
	intents := []abruptWriteEvent{
		{Event: "intent", Object: "fresh", Bytes: 100, SHA256: "new-fresh"},
		{Event: "intent", Object: "replacement", Bytes: 200, SHA256: "new-replacement"},
	}
	events := func(extra ...abruptWriteEvent) []abruptWriteEvent {
		return append(append([]abruptWriteEvent{}, intents...), extra...)
	}
	exact := map[string]abruptObjectState{
		"fresh":       {Present: true, Size: 100, SHA256: "new-fresh"},
		"replacement": {Present: true, Size: 200, SHA256: "new-replacement"},
	}
	stale := map[string]abruptObjectState{"replacement": {Present: true, Size: 150, SHA256: "old-replacement"}}
	for name, tc := range map[string]struct {
		events    []abruptWriteEvent
		recovered map[string]abruptObjectState
		wantErr   bool
	}{
		"unknown outcomes are not checked here": {events(), stale, false},
		"completed success, exact bytes":        {events(abruptWriteEvent{Event: "completed", Object: "fresh"}), exact, false},
		"completed success, absent":             {events(abruptWriteEvent{Event: "completed", Object: "fresh"}), stale, true},
		// The callback is the only record of the success: the writer stopped first.
		"callback-only success, absent fresh": {events(abruptWriteEvent{Event: "callback", Object: "fresh"}), stale, true},
		"callback-only success, stale value":  {events(abruptWriteEvent{Event: "callback", Object: "replacement"}), stale, true},
		"callback-only success, exact bytes":  {events(abruptWriteEvent{Event: "callback", Object: "replacement"}), exact, false},
		"failed completion is not a success":  {events(abruptWriteEvent{Event: "completed", Object: "fresh", Ret: -110}), stale, false},
		"failed callback is not a success":    {events(abruptWriteEvent{Event: "callback", Object: "fresh", Ret: -110}), stale, false},
	} {
		if err := abruptVerifyOutcomes(tc.events, tc.recovered); (err != nil) != tc.wantErr {
			t.Errorf("%s: %v", name, err)
		}
	}
}
