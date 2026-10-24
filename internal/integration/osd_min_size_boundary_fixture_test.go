//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// These checks run the min_size scenario's judges on synthetic samples. They
// test the evidence rules, not Ceph.

func minSizeJSON(state, acting string, epoch bool) []byte {
	epochs := ""
	if epoch {
		epochs = `"epoch": 40, "info": {"history": {"same_interval_since": 38, "same_primary_since": 30}},`
	}
	return []byte(fmt.Sprintf(`{"state": %q, %s "up": [1, 3], %s "recovery_state": []}`, state, `"acting": `+acting+`,`, epochs))
}

func minSizeLedgerEvents(acked int, outstanding bool) []secondFaultEvent {
	events := []secondFaultEvent{{Event: "start", Client: "client.1"}}
	for i := range acked {
		name := fmt.Sprintf("live-%04d", i)
		events = append(events, secondFaultEvent{Event: "intent", Object: name, Seq: i, Bytes: 10, SHA256: "x"}, secondFaultEvent{Event: "ack", Object: name})
	}
	if outstanding {
		events = append(events, secondFaultEvent{Event: "intent", Object: fmt.Sprintf("live-%04d", acked), Seq: acked, Bytes: 10, SHA256: "x"})
	}
	return events
}

// minSizeGood is a sample that shows the barrier at offset seconds after t0.
func minSizeGood(t0 time.Time, offset int) minSizeSample {
	at := t0.Add(time.Duration(offset) * time.Second)
	return minSizeSampleOf(at, minSizeJSON("undersized+degraded+peered", "[1]", true), nil, minSizeLedgerEvents(7, true), nil)
}

func minSizeWindow(t0 time.Time, offsets ...int) (minSizeBaseline, []minSizeSample, error) {
	base, err := minSizeBaselineOf(minSizeGood(t0, 0), 2)
	var samples []minSizeSample
	for _, offset := range offsets {
		samples = append(samples, minSizeGood(t0, offset))
	}
	return base, samples, err
}

func TestMinSizeBlockJudge(t *testing.T) {
	t0 := time.Unix(1000, 0)
	good := []int{4, 8, 12, 16, 20, 24}
	base, samples, err := minSizeWindow(t0, good...)
	if err != nil {
		t.Fatal(err)
	}
	if base.Blocked != "live-0007" || base.Acked != 7 || base.Interval != 38 {
		t.Fatalf("baseline %+v", base)
	}
	if j, err := judgeMinSizeBlock(base, samples, 2, 20*time.Second, 10*time.Second, 5); err != nil || j.Span != 24*time.Second || j.States["undersized+degraded+peered"] != len(good) {
		t.Fatalf("a complete window was refused: %+v: %v", j, err)
	}
	edit := func(i int, change func(*minSizeSample)) []minSizeSample {
		out := append([]minSizeSample(nil), samples...)
		change(&out[i])
		return out
	}
	for name, tc := range map[string]struct {
		samples []minSizeSample
		want    string
	}{
		"failed read in the middle":             {edit(2, func(s *minSizeSample) { s.Err = errors.New("pg query timed out") }), "inconclusive"},
		"failed read at the end":                {edit(5, func(s *minSizeSample) { s.Err = errors.New("ledger") }), "inconclusive"},
		"empty state":                           {edit(1, func(s *minSizeSample) { s.State = "" }), "no state"},
		"active again":                          {edit(3, func(s *minSizeSample) { s.State = "active+undersized+degraded" }), "not peered"},
		"incomplete instead":                    {edit(3, func(s *minSizeSample) { s.State = "incomplete" }), "not peered"},
		"two acting":                            {edit(2, func(s *minSizeSample) { s.Acting = []int{1, 2} }), "acting"},
		"no acting member":                      {edit(2, func(s *minSizeSample) { s.Acting = []int{noOSD} }), "acting"},
		"another interval":                      {edit(4, func(s *minSizeSample) { s.Interval = 39 }), "interval"},
		"an acknowledgment":                     {edit(4, func(s *minSizeSample) { s.Mark.Acked = 8 }), "acknowledged"},
		"the submission returned with an error": {edit(4, func(s *minSizeSample) { s.Mark.Unknown, s.Mark.Outstanding, s.Outstanding = 1, 0, "" }), "outcomes"},
		"a refusal":                             {edit(4, func(s *minSizeSample) { s.Mark.Refused = 1 }), "outcomes"},
		"another submission waits":              {edit(4, func(s *minSizeSample) { s.Outstanding = "live-0008" }), "waiting submission"},
		"a gap too long":                        {[]minSizeSample{samples[0], samples[1], samples[2], samples[3], minSizeGood(t0, 30)}, "after the previous"},
		"too few samples":                       {samples[:3], "samples, want at least"},
		"too short":                             {[]minSizeSample{samples[0], samples[1], samples[2], samples[3], samples[3], samples[3]}, "observed for"},
		"out of order":                          {edit(2, func(s *minSizeSample) { s.At = t0 }), "after the previous"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := judgeMinSizeBlock(base, tc.samples, 2, 20*time.Second, 10*time.Second, 5); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want an error containing %q", err, tc.want)
			}
		})
	}
	// A baseline needs the window to start from a confirmed outage with exactly
	// one waiting submission.
	for name, s := range map[string]minSizeSample{
		"active":                minSizeSampleOf(t0, minSizeJSON("active+undersized+degraded", "[1]", true), nil, minSizeLedgerEvents(7, true), nil),
		"no waiting submission": minSizeSampleOf(t0, minSizeJSON("undersized+degraded+peered", "[1]", true), nil, minSizeLedgerEvents(7, false), nil),
		"failed read":           minSizeSampleOf(t0, nil, errors.New("timed out"), minSizeLedgerEvents(7, true), nil),
	} {
		if _, err := minSizeBaselineOf(s, 2); err == nil {
			t.Errorf("%s was accepted as a baseline", name)
		}
	}
}

func TestMinSizeSampleDecoding(t *testing.T) {
	t0 := time.Unix(1000, 0)
	ledger := minSizeLedgerEvents(3, true)
	for name, s := range map[string]minSizeSample{
		"missing acting":     minSizeSampleOf(t0, []byte(`{"state": "peered", "epoch": 4, "info": {"history": {"same_interval_since": 3}}}`), nil, ledger, nil),
		"null acting":        minSizeSampleOf(t0, minSizeJSON("peered", "null", true), nil, ledger, nil),
		"missing epoch":      minSizeSampleOf(t0, minSizeJSON("peered", "[1]", false), nil, ledger, nil),
		"empty state":        minSizeSampleOf(t0, minSizeJSON("", "[1]", true), nil, ledger, nil),
		"wrong-typed acting": minSizeSampleOf(t0, minSizeJSON("peered", `"1"`, true), nil, ledger, nil),
		"query failed":       minSizeSampleOf(t0, nil, errors.New("exit 1"), ledger, nil),
		"ledger failed":      minSizeSampleOf(t0, minSizeJSON("peered", "[1]", true), nil, nil, errors.New("exit 1")),
		"ledger malformed":   minSizeSampleOf(t0, minSizeJSON("peered", "[1]", true), nil, append(ledger, secondFaultEvent{Event: "ack", Object: "never-submitted"}), nil),
		"ledger repeats":     minSizeSampleOf(t0, minSizeJSON("peered", "[1]", true), nil, append(ledger, ledger[len(ledger)-1]), nil),
	} {
		if s.Err == nil {
			t.Errorf("%s decoded into a usable sample: %+v", name, s)
		}
		if _, err := minSizeBaselineOf(s, 2); err == nil {
			t.Errorf("%s became a baseline", name)
		}
	}
	good := minSizeSampleOf(t0, minSizeJSON("undersized+degraded+peered", "[1]", true), nil, ledger, nil)
	if good.Err != nil || good.Outstanding != "live-0003" || good.Mark.Acked != 3 || good.Interval != 38 || good.Epoch != 40 {
		t.Fatalf("a well-formed sample decoded as %+v", good)
	}
	// An empty ledger has no waiting submission and is not an error.
	if empty := minSizeSampleOf(t0, minSizeJSON("peered", "[1]", true), nil, nil, nil); empty.Err != nil || empty.Outstanding != "" {
		t.Fatalf("an empty ledger decoded as %+v", empty)
	}
}

// A collector that skipped failed reads would let a window pass although the
// barrier was not observed for part of it. The real collector keeps them.
func TestMinSizeCollectorKeepsFailedReads(t *testing.T) {
	t0 := time.Unix(1000, 0)
	base, _, err := minSizeWindow(t0)
	if err != nil {
		t.Fatal(err)
	}
	next := 0
	sample := func() minSizeSample {
		next += 4
		s := minSizeGood(t0, next)
		if next == 12 || next == 16 {
			s = minSizeSampleOf(t0.Add(time.Duration(next)*time.Second), nil, errors.New("pg query timed out"), minSizeLedgerEvents(7, true), nil)
		}
		return s
	}
	samples, err := collectMinSizeWindow(context.Background(), t0, 24*time.Second, sample, func(context.Context) error { return nil })
	if err != nil || len(samples) != 6 {
		t.Fatalf("%d samples: %v", len(samples), err)
	}
	if _, err := judgeMinSizeBlock(base, samples, 2, 20*time.Second, 10*time.Second, 5); err == nil || !strings.Contains(err.Error(), "inconclusive") {
		t.Fatalf("a window with failed reads was accepted: %v", err)
	}
	var kept []minSizeSample
	for _, s := range samples {
		if s.Err == nil {
			kept = append(kept, s)
		}
	}
	// Negative control: dropping the failures leaves samples at 4, 8, 20, 24
	// (four, below the sample count) and a 12 s gap; with a smaller count and a
	// larger gap bound the dropped window would be accepted.
	if _, err := judgeMinSizeBlock(base, kept, 2, 20*time.Second, 20*time.Second, 4); err != nil {
		t.Fatalf("the negative control no longer shows what dropping failed reads would credit: %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := collectMinSizeWindow(cancelled, t0, time.Hour, func() minSizeSample { return minSizeGood(t0, 0) }, func(c context.Context) error { return c.Err() }); err == nil {
		t.Fatal("a cancelled collection reported a window")
	}
}

// The waiting submission's late outcome is whatever the writer recorded; a
// later readback never changes it, and it is never submitted twice.
func TestMinSizeLateOutcomes(t *testing.T) {
	waiting := minSizeLedgerEvents(2, true)
	name := "live-0002"
	for outcome, tc := range map[string]struct {
		event secondFaultEvent
		read  secondFaultObject
	}{
		opAcked:   {secondFaultEvent{Event: "ack", Object: name}, secondFaultObject{Present: true, Size: 10, DataLen: 10, SHA256: "x"}},
		opUnknown: {secondFaultEvent{Event: "error", Object: name, Errno: 110}, secondFaultObject{Present: true, Size: 10, DataLen: 10, SHA256: "x"}},
	} {
		events := append(append([]secondFaultEvent(nil), waiting...), tc.event, secondFaultEvent{Event: "stopped"})
		ops, err := secondFaultOps(events, true)
		if err != nil || len(ops) != 3 || ops[2].Outcome != outcome {
			t.Fatalf("%s: %+v: %v", outcome, ops, err)
		}
		read := map[string]secondFaultObject{"live-0000": tc.read, "live-0001": tc.read, name: tc.read}
		if err := secondFaultVerify(ops, read); err != nil {
			t.Fatalf("%s: %v", outcome, err)
		}
		if ops[2].Outcome != outcome {
			t.Fatalf("a whole readback promoted %s to %s", outcome, ops[2].Outcome)
		}
	}
	// An acknowledged submission that reads back absent fails the oracle.
	ops, err := secondFaultOps(append(append([]secondFaultEvent(nil), waiting...), secondFaultEvent{Event: "ack", Object: name}, secondFaultEvent{Event: "stopped"}), true)
	if err != nil {
		t.Fatal(err)
	}
	whole := secondFaultObject{Present: true, Size: 10, DataLen: 10, SHA256: "x"}
	if err := secondFaultVerify(ops, map[string]secondFaultObject{"live-0000": whole, "live-0001": whole, name: {}}); err == nil {
		t.Fatal("an acknowledged submission read back absent")
	}
	// A replayed submission is refused, and a writer stopped with the submission
	// waiting reports it unknown rather than outstanding.
	if _, err := secondFaultOps(append(append([]secondFaultEvent(nil), waiting...), waiting[len(waiting)-1]), false); err == nil {
		t.Fatal("a replayed submission was accepted")
	}
	final, err := secondFaultOps(append(append([]secondFaultEvent(nil), waiting...), secondFaultEvent{Event: "stopped"}), true)
	if err != nil || final[2].Outcome != opUnknown {
		t.Fatalf("%+v: %v", final, err)
	}
}

// Acknowledgments that accumulated before the phase was confirmed must not
// satisfy the gate. A stale watermark (5, taken before setup) passes against a
// writer that stopped at 9 after setup; the fresh watermark does not.
func TestMinSizeProgressNeedsAcksAfterConfirmation(t *testing.T) {
	stale, setupDone := secondFaultMark{Acked: 5}, secondFaultMark{Acked: 9}
	if !secondFaultAdvanced(stale, setupDone, 2) {
		t.Fatal("the negative control no longer shows a stale watermark passing")
	}
	reads := 0
	stalled := func() (secondFaultMark, error) { reads++; return setupDone, nil }
	waits := 0
	if confirmed, after, err := minSizeProgressAfter(stalled, 2, 5, func() { waits++ }); err == nil || confirmed != setupDone || after != setupDone || waits != 5 {
		t.Fatalf("a writer without new acknowledgments passed: %+v %+v %v after %d waits", confirmed, after, err, waits)
	}
	// A writer that is acknowledged after the confirmation passes, from the
	// watermark taken at the confirmation.
	acked := 8
	moving := func() (secondFaultMark, error) { acked++; return secondFaultMark{Acked: acked}, nil }
	if confirmed, after, err := minSizeProgressAfter(moving, 2, 5, func() {}); err != nil || confirmed.Acked != 9 || after.Acked != 11 {
		t.Fatalf("%+v %+v %v", confirmed, after, err)
	}
	// A failed read is returned, not skipped.
	failing := func() (secondFaultMark, error) { return secondFaultMark{}, errors.New("ledger unreadable") }
	if _, _, err := minSizeProgressAfter(failing, 2, 5, func() {}); err == nil {
		t.Fatal("a failed ledger read passed")
	}
}
