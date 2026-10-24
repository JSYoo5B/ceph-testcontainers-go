//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// The samples are synthetic documents in the layout of the pinned Tentacle
// source (see progressSample). These controls test the decoders and the
// judges, not what a cluster reports.

func barrierBaseline(t *testing.T) backfillBaseline {
	t.Helper()
	held := newProgressSample()
	held.state = "active+undersized+degraded+remapped+backfilling"
	base, err := backfillHeld(held.parse(t), 3)
	if err != nil {
		t.Fatal(err)
	}
	return base
}

func TestPGStateHasNamesWhole(t *testing.T) {
	state := "active+undersized+degraded+remapped+backfill_wait+backfill_toofull"
	for name, want := range map[string]bool{
		"backfill_toofull": true, "backfill_wait": true, "backfilling": false, "backfill": false,
		"toofull": false, "recovery_toofull": false, "active": true, "": false,
	} {
		if got := pgStateHas(state, name); got != want {
			t.Errorf("pgStateHas(%q, %q) = %t", state, name, got)
		}
	}
	if pgStateHas("active+backfill_toofull_x", "backfill_toofull") || pgStateHas("", "backfill_toofull") {
		t.Error("a longer name or an empty state matched")
	}
}

func TestFullnessBarrierSample(t *testing.T) {
	base := barrierBaseline(t)
	sample := func(edit func(*progressSample)) pgProgress {
		s := newProgressSample()
		s.state = "active+undersized+degraded+remapped+backfill_toofull"
		if edit != nil {
			edit(&s)
		}
		return s.parse(t)
	}
	for _, test := range []struct {
		name string
		p    pgProgress
		want barrierVerdict
	}{
		{"toofull at MIN", sample(nil), barrierHolds},
		{"toofull with the retry's wait", sample(func(s *progressSample) { s.state = "active+degraded+remapped+backfill_wait+backfill_toofull" }), barrierHolds},
		{"toofull after the map advanced", sample(func(s *progressSample) { s.epoch = 55 }), barrierHolds},
		{"running backfill", sample(func(s *progressSample) { s.state = "active+degraded+remapped+backfilling" }), barrierRunning},
		{"running beats a stale toofull name", sample(func(s *progressSample) { s.state = "active+backfilling+backfill_toofull" }), barrierRunning},
		{"waiting without toofull", sample(func(s *progressSample) { s.state = "active+degraded+remapped+backfill_wait" }), barrierWaiting},
		{"recovery_toofull is not backfill_toofull", sample(func(s *progressSample) { s.state = "active+degraded+recovery_toofull" }), barrierWaiting},
		{"a longer name is not backfill_toofull", sample(func(s *progressSample) { s.state = "active+backfill_toofull_x" }), barrierWaiting},
		{"cursor advanced", sample(func(s *progressSample) { s.cursor = "3:0a000000::::live-x:head" }), barrierAdvanced},
		{"transfer complete", sample(func(s *progressSample) { s.cursor = "MAX" }), barrierAdvanced},
		{"other interval", sample(func(s *progressSample) { s.interval = 50 }), barrierInvalid},
		{"other primary interval", sample(func(s *progressSample) { s.primarySince = 31 }), barrierInvalid},
		{"other acting set", sample(func(s *progressSample) { s.acting = []int{1} }), barrierInvalid},
		{"other up set", sample(func(s *progressSample) { s.up = []int{1, 2, 4} }), barrierInvalid},
		{"map before the baseline", sample(func(s *progressSample) { s.epoch = 3 }), barrierInvalid},
		{"no epoch", sample(func(s *progressSample) { s.omit = []string{"epoch"} }), barrierInconclusive},
		{"no interval", sample(func(s *progressSample) { s.omit = []string{"same_interval_since"} }), barrierInconclusive},
		{"no primary interval", sample(func(s *progressSample) { s.omit = []string{"same_primary_since"} }), barrierInconclusive},
		{"no cursor", sample(func(s *progressSample) { s.omit = []string{"peer_cursor"} }), barrierInconclusive},
		{"empty cursor", sample(func(s *progressSample) { s.cursor = "" }), barrierInconclusive},
		{"no recovery progress", sample(func(s *progressSample) { s.omit = []string{"recovery_progress"} }), barrierInconclusive},
		{"no backfill target", sample(func(s *progressSample) { s.targets = nil }), barrierInconclusive},
		{"other backfill target", sample(func(s *progressSample) { s.targets = []string{"4"} }), barrierInconclusive},
		{"two backfill targets", sample(func(s *progressSample) { s.targets = []string{"3", "4"} }), barrierInconclusive},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got, why := fullnessBarrierSample(base, test.p); got != test.want || why == "" {
				t.Fatalf("verdict %s (%s), want %s", got, why, test.want)
			}
		})
	}
	// A null cursor is the same as an empty one: no evidence.
	s := newProgressSample()
	s.state = "active+backfill_toofull"
	data := bytes.Replace(s.json(t), []byte(`"last_backfill":"MIN"`), []byte(`"last_backfill":null`), 1)
	p, err := parsePGProgress(data)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := fullnessBarrierSample(base, p); got != barrierInconclusive {
		t.Fatalf("null cursor: %s", got)
	}
}

func TestJudgeBarrier(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	series := func(verdicts ...barrierVerdict) []barrierSample {
		out := make([]barrierSample, len(verdicts))
		for i, v := range verdicts {
			out[i] = barrierSample{Verdict: v, At: at.Add(time.Duration(i) * 5 * time.Second)}
		}
		return out
	}
	hold := func(n int) []barrierVerdict {
		out := make([]barrierVerdict, n)
		for i := range out {
			out[i] = barrierHolds
		}
		return out
	}
	join := func(parts ...[]barrierVerdict) []barrierVerdict {
		var out []barrierVerdict
		for _, p := range parts {
			out = append(out, p...)
		}
		return out
	}
	one := func(v ...barrierVerdict) []barrierVerdict { return v }
	const span = 20 * time.Second
	for _, test := range []struct {
		name    string
		samples []barrierVerdict
		revoke  bool
		wantErr string
	}{
		{"refused: held for the span", hold(5), false, ""},
		{"refused: waiting first", join(one(barrierWaiting), hold(5)), false, ""},
		{"revoked: backfilling first", join(one(barrierRunning, barrierRunning), hold(5)), true, ""},
		{"revoked: waiting between", join(one(barrierRunning, barrierWaiting), hold(5)), true, ""},
		{"never toofull", one(barrierWaiting, barrierWaiting), false, "never reported"},
		{"no samples", nil, true, "never reported"},
		{"held shorter than the span", hold(4), true, "at least"},
		{"a single report", hold(1), true, "at least"},
		{"refused: a backfill ran first", join(one(barrierRunning), hold(5)), false, "not refused at admission"},
		{"running after toofull", join(hold(3), one(barrierRunning), hold(3)), true, "after the first"},
		{"advanced after toofull", join(hold(3), one(barrierAdvanced)), true, "after the first"},
		{"inconclusive inside the window", join(hold(3), one(barrierInconclusive), hold(3)), false, "after the first"},
		{"waiting inside the window", join(hold(3), one(barrierWaiting), hold(3)), false, "after the first"},
		{"invalid inside the window", join(hold(3), one(barrierInvalid), hold(3)), false, "after the first"},
		{"inconclusive before the first report", join(one(barrierInconclusive), hold(5)), true, "before the first"},
		{"advanced before the first report", join(one(barrierAdvanced), hold(5)), true, "before the first"},
		{"invalid before the first report", join(one(barrierInvalid), hold(5)), false, "before the first"},
	} {
		t.Run(test.name, func(t *testing.T) {
			judged, err := judgeBarrier(series(test.samples...), span, test.revoke)
			switch {
			case test.wantErr == "" && err != nil:
				t.Fatalf("unexpected refusal: %v", err)
			case test.wantErr != "" && (err == nil || !strings.Contains(err.Error(), test.wantErr)):
				t.Fatalf("error %v, want one containing %q", err, test.wantErr)
			case test.wantErr == "" && (judged.HeldFor < span || judged.Holds == 0):
				t.Fatalf("accepted a window of %s with %d reports", judged.HeldFor, judged.Holds)
			}
		})
	}
}

func TestOSDStatesAndBackfillFull(t *testing.T) {
	good := []byte(`{"osds":[{"osd":0,"state":["exists","up","backfillfull"]},{"osd":1,"state":["exists","up"]},{"osd":2,"state":["exists","backfillfull"]},{"osd":3,"state":["exists","up","backfillfull","full"]}]}`)
	states, err := osdStates(good)
	if err != nil || len(states) != 4 {
		t.Fatalf("states %v: %v", states, err)
	}
	for _, test := range []struct {
		name string
		want bool
		osds []int
		ok   bool
	}{
		{"backfillfull and up", true, []int{0}, true},
		{"not backfillfull", false, []int{1}, true},
		{"one of two is not backfillfull", true, []int{0, 1}, false},
		{"want clear, but set", false, []int{0}, false},
		{"down OSD", true, []int{2}, false},
		{"full OSD is not this scenario", true, []int{3}, false},
		{"full OSD is not clear", false, []int{3}, false},
		{"absent OSD", true, []int{9}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if ok, why := backfillFullIs(states, test.want, test.osds...); ok != test.ok || (!ok && why == "") {
				t.Fatalf("got %t (%s), want %t", ok, why, test.ok)
			}
		})
	}
	for name, doc := range map[string]string{
		"not json":          `{`,
		"no osds":           `{"epoch":3}`,
		"null osds":         `{"osds":null}`,
		"empty osds":        `{"osds":[]}`,
		"missing state":     `{"osds":[{"osd":0}]}`,
		"null state":        `{"osds":[{"osd":0,"state":null}]}`,
		"wrong-typed state": `{"osds":[{"osd":0,"state":"up"}]}`,
		"missing id":        `{"osds":[{"state":["up"]}]}`,
		"null id":           `{"osds":[{"osd":null,"state":["up"]}]}`,
		"duplicate id":      `{"osds":[{"osd":0,"state":["up"]},{"osd":0,"state":["up"]}]}`,
		"wrong-typed id":    `{"osds":[{"osd":"0","state":["up"]}]}`,
	} {
		if states, err := osdStates([]byte(doc)); err == nil {
			t.Errorf("%s: accepted as %v", name, states)
		}
	}
	// An empty state array is a real, valid state of an OSD, unlike a missing one.
	if states, err := osdStates([]byte(`{"osds":[{"osd":0,"state":[]}]}`)); err != nil || len(states[0]) != 0 {
		t.Fatalf("empty state array: %v %v", states, err)
	}
}

// A read that fails inside the credited window must not be bridged: the same
// holds that pass without it are refused with the failed reads kept, and a
// failure before the first report credits nothing.
func TestFailedReadsBreakTheBarrierWindow(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	sec := func(n int) time.Time { return at.Add(time.Duration(n) * time.Second) }
	const span = 20 * time.Second
	hold := func(n int) barrierSample { return barrierSample{Verdict: barrierHolds, At: sec(n)} }

	// Silent skipping, the former behavior, bridges the gap.
	skipped := []barrierSample{hold(0), hold(5), hold(20)}
	if _, err := judgeBarrier(skipped, span, false); err != nil {
		t.Fatalf("the holds alone should pass this judge: %v", err)
	}
	var kept []barrierSample
	for _, n := range []int{0, 5} {
		kept = append(kept, hold(n))
	}
	for _, n := range []int{10, 15} {
		kept = recordFailedRead(kept, sec(n), true)
	}
	kept = append(kept, hold(20))
	if len(kept) != 5 {
		t.Fatalf("failed reads inside the window were dropped: %d samples", len(kept))
	}
	if _, err := judgeBarrier(kept, span, false); err == nil || !strings.Contains(err.Error(), "after the first") {
		t.Fatalf("a window with failed reads inside it was credited: %v", err)
	}
	// Before the first report nothing is credited, so the list is unchanged.
	before := recordFailedRead(nil, sec(0), false)
	before = append(before, hold(5), hold(10), hold(15), hold(30))
	if len(before) != 4 {
		t.Fatalf("a failed read before the first report entered the window: %d samples", len(before))
	}
	if _, err := judgeBarrier(before, span, false); err != nil {
		t.Fatalf("a failure before the first report should not count: %v", err)
	}
}
