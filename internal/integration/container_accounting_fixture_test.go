//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"errors"
	"fmt"
	"testing"
)

// Docker-free fixtures for the container accounting adapter. The JSON shapes
// follow the Moby StatsResponse encoding at
// 14ebc602124e280ef4ae3e1a8e688dac1255baa6; the values are synthetic and no
// engine or cgroup was read.
//
// Not covered: no native CPU-pressure scenario, no installed-engine
// qualification, no resource-update round trip, and no cgroup-v2 read of
// memory.events.local or victim attribution.

var accountingGen = containerGeneration{ID: "c1", StartedAt: "2026-10-25T00:00:00.000000001Z"}

// mobyStats builds a stats body the way the pinned engine encodes it: the
// throttled time is the cpu.stat microsecond value times 1000.
func mobyStats(id, read string, periods, throttled, throttledUsec uint64, failcnt string) []byte {
	mem := ""
	if failcnt != "" {
		mem = `,"memory_stats":{"usage":1,"failcnt":` + failcnt + `}`
	}
	return fmt.Appendf(nil, `{"id":%q,"read":%q,"cpu_stats":{"throttling_data":{"periods":%d,"throttled_periods":%d,"throttled_time":%d}}%s}`,
		id, read, periods, throttled, throttledUsec*1000, mem)
}

func mustSample(t *testing.T, gen containerGeneration, body []byte) accountingSample {
	t.Helper()
	s, err := parseAccountingSample(gen, body)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

const (
	accT0 = "2026-10-25T00:00:10Z"
	accT1 = "2026-10-25T00:00:15Z"
)

func TestContainerAccountingUnitConversion(t *testing.T) {
	prev := mustSample(t, accountingGen, mobyStats("c1", accT0, 100, 0, 0, ""))
	cur := mustSample(t, accountingGen, mobyStats("c1", accT1, 150, 20, 1500, ""))
	if cur.ThrottledNanos.Unit != unitNanoseconds || cur.Periods.Unit != unitCount {
		t.Fatalf("units not retained: %+v", cur)
	}
	if cur.ThrottledNanos.Value != 1_500_000 {
		t.Fatalf("1500 usec must appear as 1,500,000 ns, got %d", cur.ThrottledNanos.Value)
	}
	d, err := computeCPUThrottleDelta(prev, cur, accountingChecks{})
	if err != nil {
		t.Fatal(err)
	}
	if d.Periods != 50 || d.ThrottledPeriods != 20 || d.ThrottledNanos != 1_500_000 || d.Elapsed.Seconds() != 5 || !d.Throttled() {
		t.Fatalf("unexpected delta %+v", d)
	}
	if cur.Source != accountingSourceAPI || cur.At.IsZero() || cur.Generation != accountingGen {
		t.Fatalf("source/time/generation not retained: %+v", cur)
	}
	// Negative control: a reader that treats the value as microseconds again
	// reports a 1000-fold different delta.
	wrong, err := computeCPUThrottleDelta(prev, cur, accountingChecks{nanosScale: 1000})
	if err != nil || wrong.ThrottledNanos == d.ThrottledNanos {
		t.Fatalf("unit control did not diverge: %+v %v", wrong, err)
	}
}

func TestContainerAccountingObservedZeroVersusAbsent(t *testing.T) {
	zero := mustSample(t, accountingGen, mobyStats("c1", accT0, 0, 0, 0, ""))
	if !zero.Periods.ok() || zero.Periods.Value != 0 || !zero.ThrottledNanos.ok() {
		t.Fatalf("explicit zero must be an observed value: %+v", zero)
	}
	if zero.MemoryFailcnt.State != fieldAbsent {
		t.Fatalf("missing failcnt must stay absent, got %s", zero.MemoryFailcnt.State)
	}
	withZeroFailcnt := mustSample(t, accountingGen, mobyStats("c1", accT0, 0, 0, 0, "0"))
	if withZeroFailcnt.MemoryFailcnt.State != fieldValue || withZeroFailcnt.MemoryFailcnt.Value != 0 {
		t.Fatalf("present zero failcnt must be distinguishable: %+v", withZeroFailcnt.MemoryFailcnt)
	}
	// Unthrottled period advance is a valid delta that is not throttling.
	next := mustSample(t, accountingGen, mobyStats("c1", accT1, 10, 0, 0, ""))
	d, err := computeCPUThrottleDelta(zero, next, accountingChecks{})
	if err != nil || d.Throttled() || d.Periods != 10 {
		t.Fatalf("zero throttle delta misread: %+v %v", d, err)
	}
}

func TestContainerAccountingInconclusiveFields(t *testing.T) {
	good := mustSample(t, accountingGen, mobyStats("c1", accT0, 1, 1, 1, ""))
	cases := map[string]string{
		"missing counter": `{"id":"c1","read":"` + accT1 + `","cpu_stats":{"throttling_data":{"periods":2,"throttled_periods":2}}}`,
		"missing section": `{"id":"c1","read":"` + accT1 + `","cpu_stats":{}}`,
		"null counter":    `{"id":"c1","read":"` + accT1 + `","cpu_stats":{"throttling_data":{"periods":2,"throttled_periods":2,"throttled_time":null}}}`,
		"null section":    `{"id":"c1","read":"` + accT1 + `","cpu_stats":{"throttling_data":null}}`,
		"string counter":  `{"id":"c1","read":"` + accT1 + `","cpu_stats":{"throttling_data":{"periods":"2","throttled_periods":2,"throttled_time":2}}}`,
		"float counter":   `{"id":"c1","read":"` + accT1 + `","cpu_stats":{"throttling_data":{"periods":2.5,"throttled_periods":2,"throttled_time":2}}}`,
		"negative":        `{"id":"c1","read":"` + accT1 + `","cpu_stats":{"throttling_data":{"periods":-2,"throttled_periods":2,"throttled_time":2}}}`,
		"object section":  `{"id":"c1","read":"` + accT1 + `","cpu_stats":{"throttling_data":[]}}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			cur := mustSample(t, accountingGen, []byte(body))
			_, err := computeCPUThrottleDelta(good, cur, accountingChecks{})
			if !errors.Is(err, errAccountingInconclusive) {
				t.Fatalf("want inconclusive, got %v", err)
			}
			// Negative control: without the presence rule a defective
			// field is silently read as zero and a delta is fabricated.
			if _, err := computeCPUThrottleDelta(good, cur, accountingChecks{skipPresence: true}); errors.Is(err, errAccountingInconclusive) {
				t.Fatal("control still refused")
			}
		})
	}
	for name, body := range map[string]string{
		"not an object": `[]`,
		"null body":     `null`,
		"no read":       `{"id":"c1"}`,
		"bad read":      `{"id":"c1","read":"yesterday"}`,
		"zero read":     `{"id":"c1","read":"0001-01-01T00:00:00Z"}`,
	} {
		if _, err := parseAccountingSample(accountingGen, []byte(body)); !errors.Is(err, errAccountingInconclusive) {
			t.Fatalf("%s: want inconclusive, got %v", name, err)
		}
	}
}

func TestContainerAccountingCounterDecreaseRefused(t *testing.T) {
	prev := mustSample(t, accountingGen, mobyStats("c1", accT0, 100, 50, 900, ""))
	for name, body := range map[string][]byte{
		"periods reset":   mobyStats("c1", accT1, 3, 50, 900, ""),
		"throttled reset": mobyStats("c1", accT1, 120, 0, 900, ""),
		"time reset":      mobyStats("c1", accT1, 120, 60, 10, ""),
	} {
		cur := mustSample(t, accountingGen, body)
		if _, err := computeCPUThrottleDelta(prev, cur, accountingChecks{}); !errors.Is(err, errAccountingDecrease) {
			t.Fatalf("%s: want decrease refusal, got %v", name, err)
		}
		// Negative control: without the rule, unsigned arithmetic wraps.
		d, err := computeCPUThrottleDelta(prev, cur, accountingChecks{skipDecrease: true})
		if err != nil || (d.Periods < 1<<62 && d.ThrottledPeriods < 1<<62 && d.ThrottledNanos < 1<<62) {
			t.Fatalf("%s: control did not wrap: %+v %v", name, d, err)
		}
	}
	backwards := mustSample(t, accountingGen, mobyStats("c1", "2026-10-25T00:00:01Z", 120, 60, 1000, ""))
	if _, err := computeCPUThrottleDelta(prev, backwards, accountingChecks{}); !errors.Is(err, errAccountingDecrease) {
		t.Fatalf("time going backwards must be refused, got %v", err)
	}
	same := mustSample(t, accountingGen, mobyStats("c1", accT0, 100, 50, 900, ""))
	if d, err := computeCPUThrottleDelta(prev, same, accountingChecks{}); err != nil || d.Throttled() || d.Elapsed != 0 {
		t.Fatalf("an unchanged sample is a valid zero delta, got %+v %v", d, err)
	}
}

func TestContainerAccountingGenerationMismatchRefused(t *testing.T) {
	restarted := containerGeneration{ID: "c1", StartedAt: "2026-10-25T00:00:12Z"}
	replaced := containerGeneration{ID: "c2", StartedAt: accountingGen.StartedAt}
	prev := mustSample(t, accountingGen, mobyStats("c1", accT0, 100, 50, 900, ""))
	for name, gen := range map[string]containerGeneration{"restarted": restarted, "replaced": replaced} {
		cur := mustSample(t, gen, mobyStats(gen.ID, accT1, 200, 90, 1900, ""))
		if _, err := computeCPUThrottleDelta(prev, cur, accountingChecks{}); !errors.Is(err, errAccountingGeneration) {
			t.Fatalf("%s: want generation refusal, got %v", name, err)
		}
		// Negative control: the same data would otherwise be accepted.
		if _, err := computeCPUThrottleDelta(prev, cur, accountingChecks{skipGeneration: true}); err != nil {
			t.Fatalf("%s: control should accept, got %v", name, err)
		}
	}
	// A body for another container than the frozen one is refused at parse.
	if _, err := parseAccountingSample(accountingGen, mobyStats("c2", accT0, 1, 1, 1, "")); !errors.Is(err, errAccountingGeneration) {
		t.Fatalf("foreign id must be refused, got %v", err)
	}
	for _, gen := range []containerGeneration{{}, {ID: "c1"}, {StartedAt: "x"}} {
		if _, err := parseAccountingSample(gen, mobyStats("c1", accT0, 1, 1, 1, "")); !errors.Is(err, errAccountingGeneration) {
			t.Fatalf("incomplete generation %+v must be refused, got %v", gen, err)
		}
	}
	other := mustSample(t, accountingGen, mobyStats("c1", accT1, 200, 90, 1900, ""))
	other.Source = "elsewhere"
	if _, err := computeCPUThrottleDelta(prev, other, accountingChecks{}); !errors.Is(err, errAccountingGeneration) {
		t.Fatalf("different sources are not comparable, got %v", err)
	}
}

func TestContainerAccountingOOMUnsupportedWithoutLocalEvidence(t *testing.T) {
	zero := uint64(0)
	one := uint64(1)
	for _, failcnt := range []string{"", "0", "7"} {
		s := mustSample(t, accountingGen, mobyStats("c1", accT0, 1, 1, 1, failcnt))
		if got := classifyOOM(s, nil); got != oomUnsupported {
			t.Fatalf("failcnt %q: want unsupported, got %s", failcnt, got)
		}
	}
	s := mustSample(t, accountingGen, mobyStats("c1", accT0, 1, 1, 1, "7"))
	full := localMemoryEvidence{Generation: accountingGen, Source: "memory.events.local", Local: true, OomKill: &one, VictimIsTarget: true}
	if got := classifyOOM(s, &full); got != oomEvidenced {
		t.Fatalf("complete local evidence: got %s", got)
	}
	// Each missing ingredient downgrades to incomplete, never to zero.
	broken := map[string]func(*localMemoryEvidence){
		"hierarchical":   func(e *localMemoryEvidence) { e.Local = false },
		"no oom_kill":    func(e *localMemoryEvidence) { e.OomKill = nil },
		"unknown victim": func(e *localMemoryEvidence) { e.VictimIsTarget = false },
		"other gen":      func(e *localMemoryEvidence) { e.Generation.StartedAt = "later" },
		"no source":      func(e *localMemoryEvidence) { e.Source = "" },
	}
	for name, mutate := range broken {
		e := full
		mutate(&e)
		if got := classifyOOM(s, &e); got != oomIncomplete {
			t.Fatalf("%s: want incomplete, got %s", name, got)
		}
	}
	// An observed local zero is still evidence, not unsupported.
	zeroKill := full
	zeroKill.OomKill = &zero
	if got := classifyOOM(s, &zeroKill); got != oomEvidenced {
		t.Fatalf("observed local zero: got %s", got)
	}
	// Failcnt alone must not change the verdict (negative control for any
	// implementation that substitutes it).
	a := mustSample(t, accountingGen, mobyStats("c1", accT0, 1, 1, 1, ""))
	b := mustSample(t, accountingGen, mobyStats("c1", accT0, 1, 1, 1, "9"))
	if classifyOOM(a, nil) != classifyOOM(b, nil) {
		t.Fatal("failcnt influenced the OOM classification")
	}
}

// The live reader needs Docker and is intentionally not run here.
var _ = sampleContainerAccounting
