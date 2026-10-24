//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// These checks run the live-client fullness scenario's judges on synthetic PG
// documents and ledgers. They test the evidence rules, not Ceph.

// liveEvents builds a writer ledger: acked successful operations, then
// optionally one that raised errno (a refusal for 28, an unknown outcome for
// others), then optionally one that has not returned.
func liveEvents(acked, errno int, outstanding bool) []secondFaultEvent {
	events := []secondFaultEvent{{Event: "start", Client: "client.1"}}
	seq := 0
	add := func(terminal secondFaultEvent) {
		name := fmt.Sprintf("live-%04d", seq)
		events = append(events, secondFaultEvent{Event: "intent", Object: name, Seq: seq, Bytes: 10, SHA256: "x"})
		if terminal.Event != "" {
			terminal.Object = name
			events = append(events, terminal)
		}
		seq++
	}
	for range acked {
		add(secondFaultEvent{Event: "ack"})
	}
	if errno != 0 {
		add(secondFaultEvent{Event: "error", Errno: errno})
	}
	if outstanding {
		add(secondFaultEvent{})
	}
	return events
}

const liveToofull = "active+undersized+degraded+remapped+backfill_toofull"

func liveSample(t *testing.T, at time.Time, edit func(*progressSample), events []secondFaultEvent) fullnessLiveSample {
	t.Helper()
	s := newProgressSample()
	s.state = liveToofull
	if edit != nil {
		edit(&s)
	}
	return fullnessLiveSampleOf(barrierBaseline(t), at, s.json(t), nil, events, nil)
}

func TestFullnessLiveSampleDecoding(t *testing.T) {
	t0 := time.Unix(1000, 0)
	good := liveSample(t, t0, nil, liveEvents(7, 0, true))
	if good.Err != nil || good.Verdict != barrierHolds || good.Mark.Acked != 7 || good.Mark.Outstanding != 1 || good.Outstanding != "live-0007" || good.Interval != 38 {
		t.Fatalf("a conclusive sample was read as %+v", good)
	}
	base := barrierBaseline(t)
	ok := newProgressSample()
	ok.state = liveToofull
	for name, got := range map[string]fullnessLiveSample{
		"failed pg read":            fullnessLiveSampleOf(base, t0, nil, fmt.Errorf("timeout"), liveEvents(7, 0, true), nil),
		"empty pg document":         fullnessLiveSampleOf(base, t0, nil, nil, liveEvents(7, 0, true), nil),
		"null pg document":          fullnessLiveSampleOf(base, t0, []byte("null"), nil, liveEvents(7, 0, true), nil),
		"failed ledger read":        fullnessLiveSampleOf(base, t0, ok.json(t), nil, nil, fmt.Errorf("exit 1")),
		"missing epoch":             liveSample(t, t0, func(s *progressSample) { s.omit = []string{"epoch"} }, liveEvents(7, 0, true)),
		"missing interval":          liveSample(t, t0, func(s *progressSample) { s.omit = []string{"same_interval_since"} }, liveEvents(7, 0, true)),
		"missing recovery progress": liveSample(t, t0, func(s *progressSample) { s.omit = []string{"recovery_progress"} }, liveEvents(7, 0, true)),
		"missing target cursor":     liveSample(t, t0, func(s *progressSample) { s.omit = []string{"peer_cursor"} }, liveEvents(7, 0, true)),
		"replayed submission":       fullnessLiveSampleOf(base, t0, ok.json(t), nil, append(liveEvents(2, 0, false), secondFaultEvent{Event: "intent", Object: "live-0000", Seq: 2, Bytes: 10, SHA256: "x"}), nil),
	} {
		if got.Err == nil || got.Verdict != barrierInconclusive {
			t.Errorf("%s: read as %+v, want an inconclusive sample with an error", name, got)
		}
	}
	// Other readings keep the barrier verdict of the shared judge.
	running := liveSample(t, t0, func(s *progressSample) { s.state = "active+degraded+remapped+backfilling" }, liveEvents(7, 0, false))
	advanced := liveSample(t, t0, func(s *progressSample) { s.cursor = "3:0a000000::::live-x:head" }, liveEvents(7, 0, false))
	if running.Verdict != barrierRunning || advanced.Verdict != barrierAdvanced {
		t.Errorf("verdicts %s and %s, want running and advanced", running.Verdict, advanced.Verdict)
	}
}

// liveWindow builds conclusive samples, one every step seconds, with the
// writer's ledger given for each.
func liveWindow(t *testing.T, step int, ledgers ...[]secondFaultEvent) []fullnessLiveSample {
	t.Helper()
	t0 := time.Unix(1000, 0)
	var out []fullnessLiveSample
	for i, events := range ledgers {
		out = append(out, liveSample(t, t0.Add(time.Duration(i*step)*time.Second), nil, events))
	}
	if len(out) > 0 && out[0].Err != nil {
		t.Fatal(out[0].Err)
	}
	return out
}

func TestJudgeFullnessLiveClasses(t *testing.T) {
	const gap = 10 * time.Second
	for _, test := range []struct {
		name    string
		ledgers [][]secondFaultEvent
		want    fullnessLiveClass
		promise bool
	}{
		{"served", [][]secondFaultEvent{liveEvents(5, 0, true), liveEvents(8, 0, false), liveEvents(12, 0, true), liveEvents(15, 0, false)}, liveServed, true},
		{"served although one waited at the start", [][]secondFaultEvent{liveEvents(5, 0, true), liveEvents(11, 0, false)}, liveServed, true},
		{"blocked", [][]secondFaultEvent{liveEvents(5, 0, true), liveEvents(5, 0, true), liveEvents(5, 0, true)}, liveBlocked, false},
		{"failed with an error", [][]secondFaultEvent{liveEvents(5, 0, false), liveEvents(9, 110, false), liveEvents(14, 110, false)}, liveFailed, false},
		{"failed with a refusal", [][]secondFaultEvent{liveEvents(5, 0, false), liveEvents(12, 28, false)}, liveFailed, false},
		{"too few acknowledgments", [][]secondFaultEvent{liveEvents(5, 0, false), liveEvents(7, 0, false), liveEvents(7, 0, false)}, liveUnknown, false},
		{"idle writer", [][]secondFaultEvent{liveEvents(5, 0, false), liveEvents(5, 0, false)}, liveUnknown, false},
		{"a different submission waits each time", [][]secondFaultEvent{liveEvents(5, 0, true), liveEvents(6, 0, true), liveEvents(6, 0, true)}, liveUnknown, false},
	} {
		j, err := judgeFullnessLive(liveWindow(t, 5, test.ledgers...), fullnessLiveMinAcks, gap)
		if err != nil || j.Class != test.want {
			t.Errorf("%s: class %q (%s), error %v, want %q", test.name, j.Class, j.Why, err, test.want)
			continue
		}
		if met := fullnessLiveExpect(j) == nil; met != test.promise {
			t.Errorf("%s: promise met %t, want %t", test.name, met, test.promise)
		}
	}

	// Outcomes recorded before the window belong to it only through the
	// counters at its start: an earlier failure is not a failure inside it.
	early := [][]secondFaultEvent{liveEvents(5, 110, false), liveEvents(12, 110, false)}
	if j, err := judgeFullnessLive(liveWindow(t, 5, early...), fullnessLiveMinAcks, gap); err != nil || j.Class != liveServed {
		t.Errorf("a failure before the window was counted inside it: %+v %v", j, err)
	}
}

func TestJudgeFullnessLiveRefusesBrokenWindows(t *testing.T) {
	const gap = 10 * time.Second
	ledgers := [][]secondFaultEvent{liveEvents(5, 0, false), liveEvents(8, 0, false), liveEvents(12, 0, false), liveEvents(15, 0, false)}
	if _, err := judgeFullnessLive(liveWindow(t, 5, ledgers...), fullnessLiveMinAcks, gap); err != nil {
		t.Fatalf("the control window is refused: %v", err)
	}
	refuse := func(name, want string, edit func([]fullnessLiveSample) []fullnessLiveSample) {
		t.Helper()
		j, err := judgeFullnessLive(edit(liveWindow(t, 5, ledgers...)), fullnessLiveMinAcks, gap)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: judged %+v with error %v, want an error containing %q", name, j, err, want)
		}
	}
	// A failed or unreadable observation inside the window is kept and fails
	// it: the served acknowledgments on either side are not bridged.
	refuse("failed read inside", "inconclusive", func(s []fullnessLiveSample) []fullnessLiveSample {
		s[2] = fullnessLiveSample{At: s[2].At, Verdict: barrierInconclusive, Err: fmt.Errorf("pg query timed out")}
		return s
	})
	refuse("dropped sample leaves a gap", "after the previous one", func(s []fullnessLiveSample) []fullnessLiveSample {
		s[3].At = s[3].At.Add(30 * time.Second)
		return s
	})
	refuse("barrier gone inside", "not backfill_toofull", func(s []fullnessLiveSample) []fullnessLiveSample {
		s[2].Verdict = barrierWaiting
		return s
	})
	refuse("other interval", "interval", func(s []fullnessLiveSample) []fullnessLiveSample {
		s[2].Interval++
		return s
	})
	refuse("counters go back", "counters went", func(s []fullnessLiveSample) []fullnessLiveSample {
		s[2].Mark.Acked = 2
		return s
	})
	refuse("time goes back", "after the previous one", func(s []fullnessLiveSample) []fullnessLiveSample {
		s[2].At = s[1].At.Add(-time.Second)
		return s
	})
	refuse("no barrier at all", "no conclusive backfill_toofull", func(s []fullnessLiveSample) []fullnessLiveSample {
		for i := range s {
			s[i].Verdict = barrierWaiting
		}
		return s
	})
	refuse("only inconclusive samples", "no conclusive backfill_toofull", func(s []fullnessLiveSample) []fullnessLiveSample {
		for i := range s {
			s[i].Err = fmt.Errorf("no ledger")
		}
		return s
	})
	if _, err := judgeFullnessLive(nil, fullnessLiveMinAcks, gap); err == nil {
		t.Error("an empty window was judged")
	}
	// Samples before the first conclusive barrier sample credit nothing, so a
	// failure there does not break the window that begins later.
	later := liveWindow(t, 5, ledgers...)
	later = append([]fullnessLiveSample{{At: later[0].At.Add(-time.Hour), Verdict: barrierInconclusive, Err: fmt.Errorf("early")}}, later...)
	if j, err := judgeFullnessLive(later, fullnessLiveMinAcks, gap); err != nil || j.Start.Acked != 5 || j.Samples != 4 || j.Class != liveServed {
		t.Errorf("an early failed read broke or shifted a later window: %+v %v", j, err)
	}
}

func TestFullnessLiveSettleKeepsOriginalOutcomes(t *testing.T) {
	ops := func(events []secondFaultEvent, final bool) []secondFaultOp {
		t.Helper()
		out, err := secondFaultOps(events, final)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	stop := secondFaultEvent{Event: "stopped"}
	atEnd := ops(liveEvents(5, 110, true), false) // 5 acked, 1 unknown, 1 outstanding

	// The outstanding one later acknowledges: reported, not rewritten.
	late, err := fullnessLiveSettle(atEnd, ops(append(liveEvents(5, 110, true), secondFaultEvent{Event: "ack", Object: "live-0006"}, stop), true))
	if err != nil || len(late) != 1 || late[0].Name != "live-0006" || late[0].Final != opAcked {
		t.Fatalf("late success: %+v %v", late, err)
	}
	// Still outstanding at the stop: unknown, never acknowledged by readback.
	late, err = fullnessLiveSettle(atEnd, ops(append(liveEvents(5, 110, true), stop), true))
	if err != nil || len(late) != 1 || late[0].Final != opUnknown {
		t.Fatalf("outstanding at the stop: %+v %v", late, err)
	}
	// A terminal outcome cannot change, the ledger cannot shrink or reorder.
	flipped := ops(append(liveEvents(5, 110, true), stop), true)
	flipped[5].Outcome = opAcked // the unknown one is now acked
	if _, err := fullnessLiveSettle(atEnd, flipped); err == nil || !strings.Contains(err.Error(), "returned") {
		t.Errorf("an outcome that changed after it returned was accepted: %v", err)
	}
	if _, err := fullnessLiveSettle(atEnd, atEnd[:3]); err == nil {
		t.Error("a shortened final ledger was accepted")
	}
	renamed := ops(append(liveEvents(5, 110, true), stop), true)
	renamed[2].Name = "other"
	if _, err := fullnessLiveSettle(atEnd, renamed); err == nil {
		t.Error("a renamed operation was accepted")
	}
}

func TestFullnessLiveVerifyDoesNotPromoteUnknowns(t *testing.T) {
	ops := []secondFaultOp{
		{Name: "a", Bytes: 4, SHA256: "ha", Outcome: opAcked},
		{Name: "b", Bytes: 4, SHA256: "hb", Outcome: opUnknown},
		{Name: "c", Bytes: 4, SHA256: "hc", Outcome: opUnknown},
	}
	whole := func(op secondFaultOp) secondFaultObject {
		return secondFaultObject{Present: true, Size: op.Bytes, SHA256: op.SHA256, DataLen: op.Bytes}
	}
	read := map[string]secondFaultObject{"a": whole(ops[0]), "b": whole(ops[1]), "c": {}}
	got, err := fullnessLiveVerify(ops, read)
	if err != nil || got.AckedVerified != 1 || got.UnknownPresent != 1 || got.UnknownAbsent != 1 {
		t.Fatalf("readback %+v: %v", got, err)
	}
	for name, edit := range map[string]func(map[string]secondFaultObject){
		"acknowledged object missing": func(m map[string]secondFaultObject) { m["a"] = secondFaultObject{} },
		"acknowledged object altered": func(m map[string]secondFaultObject) { o := whole(ops[0]); o.SHA256 = "other"; m["a"] = o },
		"acknowledged object short":   func(m map[string]secondFaultObject) { o := whole(ops[0]); o.DataLen--; m["a"] = o },
		"unknown object torn":         func(m map[string]secondFaultObject) { o := whole(ops[1]); o.Size--; m["b"] = o },
		"object not read back at all": func(m map[string]secondFaultObject) { delete(m, "c") },
	} {
		copied := map[string]secondFaultObject{}
		for k, v := range read {
			copied[k] = v
		}
		edit(copied)
		if _, err := fullnessLiveVerify(ops, copied); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// A ledger with nothing acknowledged is no evidence that data was read.
	if _, err := fullnessLiveVerify(ops[1:], map[string]secondFaultObject{"b": whole(ops[1]), "c": whole(ops[2])}); err == nil {
		t.Error("a ledger without an acknowledged write passed the readback")
	}
	if _, err := fullnessLiveVerify(nil, nil); err == nil {
		t.Error("an empty ledger passed the readback")
	}
}
