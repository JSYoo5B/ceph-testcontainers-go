//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Docker-free controls for the slow-live OSD CPU model. They drive the load
// report parser, the OSD-map entry parser, the window classifier and the run
// classifier with synthetic records, and run the embedded workload script
// against a fake rados module. A fixture pass shows the classifiers' rules and
// the script's bookkeeping, not a native effect: no cgroup, engine, Ceph
// daemon or librados was involved.

const osdLoadGood = `{"completed": true, "wall_ns": 10000000000, "submitted": 120, "acked": 118, "errors": 2, "p50_ns": 9000000, "max_ns": 2500000000}` + "\n"

func TestOSDCPULoadReportRequiresEveryTypedField(t *testing.T) {
	r, err := parseOSDCPULoadReport([]byte(osdLoadGood))
	if err != nil || r.Submitted != 120 || r.Acked != 118 || r.Errors != 2 || r.MaxNanos != 2500000000 {
		t.Fatalf("good report: %+v %v", r, err)
	}
	bad := map[string]string{
		"empty":              "",
		"not an object":      "[]\n",
		"missing completed":  `{"wall_ns":1,"submitted":1,"acked":1,"errors":0,"p50_ns":1,"max_ns":1}`,
		"completed false":    `{"completed":false,"wall_ns":1,"submitted":1,"acked":1,"errors":0,"p50_ns":1,"max_ns":1}`,
		"completed string":   `{"completed":"true","wall_ns":1,"submitted":1,"acked":1,"errors":0,"p50_ns":1,"max_ns":1}`,
		"completed null":     `{"completed":null,"wall_ns":1,"submitted":1,"acked":1,"errors":0,"p50_ns":1,"max_ns":1}`,
		"missing errors":     `{"completed":true,"wall_ns":1,"submitted":1,"acked":1,"p50_ns":1,"max_ns":1}`,
		"null acked":         `{"completed":true,"wall_ns":1,"submitted":1,"acked":null,"errors":0,"p50_ns":1,"max_ns":1}`,
		"string submitted":   `{"completed":true,"wall_ns":1,"submitted":"1","acked":1,"errors":0,"p50_ns":1,"max_ns":1}`,
		"negative wall":      `{"completed":true,"wall_ns":-1,"submitted":1,"acked":1,"errors":0,"p50_ns":1,"max_ns":1}`,
		"float p50":          `{"completed":true,"wall_ns":1,"submitted":1,"acked":1,"errors":0,"p50_ns":1.5,"max_ns":1}`,
		"exponent":           `{"completed":true,"wall_ns":1e3,"submitted":1,"acked":1,"errors":0,"p50_ns":1,"max_ns":1}`,
		"oversized wall":     `{"completed":true,"wall_ns":9223372036854775808,"submitted":1,"acked":1,"errors":0,"p50_ns":1,"max_ns":1}`,
		"unaccounted":        `{"completed":true,"wall_ns":1,"submitted":5,"acked":1,"errors":0,"p50_ns":1,"max_ns":1}`,
		"overaccounted":      `{"completed":true,"wall_ns":1,"submitted":1,"acked":1,"errors":1,"p50_ns":1,"max_ns":1}`,
		"trailing object":    osdLoadGood + osdLoadGood,
		"trailing text":      osdLoadGood + "done",
		"leading plus sign":  `{"completed":true,"wall_ns":+1,"submitted":1,"acked":1,"errors":0,"p50_ns":1,"max_ns":1}`,
		"leading zero digit": `{"completed":true,"wall_ns":01,"submitted":1,"acked":1,"errors":0,"p50_ns":1,"max_ns":1}`,
	}
	for name, data := range bad {
		if r, err := parseOSDCPULoadReport([]byte(data)); err == nil {
			t.Errorf("%s: accepted as %+v", name, r)
		}
	}
}

func TestOSDMapEntryRequiresEveryField(t *testing.T) {
	entry := func(fields string) string {
		return `{"epoch": 41, "osds": [{"osd": 0, "up": 1, "in": 1, "up_from": 5, "down_at": 0}, {"osd": 2` + fields + `}]}`
	}
	good := entry(`, "up": 1, "in": 1, "up_from": 9, "down_at": 7`)
	m, err := parseOSDMapEntry([]byte(good), 2)
	if err != nil || m != (osdMapEntry{Epoch: 41, Up: true, In: true, UpFrom: 9, DownAt: 7}) {
		t.Fatalf("good: %+v %v", m, err)
	}
	if m, err := parseOSDMapEntry([]byte(entry(`, "up": 0, "in": 1, "up_from": 9, "down_at": 7`)), 2); err != nil || m.Up {
		t.Fatalf("down entry: %+v %v", m, err)
	}
	for name, data := range map[string]string{
		"not json":        "nope",
		"no epoch":        `{"osds": []}`,
		"null epoch":      `{"epoch": null, "osds": []}`,
		"no osds":         `{"epoch": 1}`,
		"null osds":       `{"epoch": 1, "osds": null}`,
		"osd absent":      `{"epoch": 1, "osds": [{"osd": 0, "up": 1, "in": 1, "up_from": 1, "down_at": 0}]}`,
		"missing up":      entry(`, "in": 1, "up_from": 9, "down_at": 7`),
		"null up":         entry(`, "up": null, "in": 1, "up_from": 9, "down_at": 7`),
		"missing in":      entry(`, "up": 1, "up_from": 9, "down_at": 7`),
		"missing up_from": entry(`, "up": 1, "in": 1, "down_at": 7`),
		"missing down_at": entry(`, "up": 1, "in": 1, "up_from": 9`),
	} {
		if m, err := parseOSDMapEntry([]byte(data), 2); err == nil {
			t.Errorf("%s: accepted as %+v", name, m)
		}
	}
}

var (
	osdT0    = time.Date(2026, 10, 27, 0, 0, 10, 0, time.UTC)
	osdQuota = cpuMax{Quota: 25000, Period: 100000}
)

func osdWitness(at time.Time, max cpuMax, periods, throttled, usec uint64) *cpuWitness {
	return &cpuWitness{Generation: cpuGen, At: at, Source: cpuWitnessSource, ControllerListed: true, Max: max, Stat: cpuStatCounters{Periods: periods, Throttled: throttled, ThrottledUsec: usec}}
}

func osdView(at time.Time) osdSlowLiveView {
	return osdSlowLiveView{
		At:       at,
		Instance: &osdInstanceView{Running: true, PID: 4242, StartedAt: cpuGen.StartedAt},
		Map:      &osdMapEntry{Epoch: 41, Up: true, In: true, UpFrom: 9, DownAt: 7},
		PGState:  "active+clean", PGRead: true, ActingPrimary: 2,
		HealthCodes: []string{}, HealthRead: true,
	}
}

// osdWindow is a throttled, live, well-bracketed window with a progressing
// workload; each case below breaks exactly one rule.
func osdWindow(t *testing.T) osdCPUWindow {
	t.Helper()
	load, err := parseOSDCPULoadReport([]byte(osdLoadGood))
	if err != nil {
		t.Fatal(err)
	}
	baseline := osdView(osdT0)
	return osdCPUWindow{
		Label: "pressure", Generation: cpuGen, PID: 4242, Expected: osdQuota,
		ExecStart: osdT0.Add(time.Second), ExecEnd: osdT0.Add(12 * time.Second),
		Load:   &load,
		Before: osdWitness(osdT0, osdQuota, 10, 0, 0), After: osdWitness(osdT0.Add(13*time.Second), osdQuota, 120, 100, 7_000_000),
		Baseline: &baseline,
		Samples:  []osdSlowLiveView{osdView(osdT0.Add(3 * time.Second)), osdView(osdT0.Add(7 * time.Second)), osdView(osdT0.Add(13 * time.Second))},
	}
}

func TestOSDCPUWindowClassification(t *testing.T) {
	type tc struct {
		name   string
		mutate func(*osdCPUWindow)
		want   osdCPUWindowVerdict
		// disable turns off the rule that catches the case; the negative
		// control then has to see the wrong verdict.
		disable func(*osdCPUWindowChecks)
		wrong   osdCPUWindowVerdict
	}
	instance := func(w *osdCPUWindow, i int, mutate func(*osdInstanceView)) {
		v := *w.Samples[i].Instance
		mutate(&v)
		w.Samples[i].Instance = &v
	}
	entry := func(w *osdCPUWindow, i int, mutate func(*osdMapEntry)) {
		m := *w.Samples[i].Map
		mutate(&m)
		w.Samples[i].Map = &m
	}
	cases := []tc{
		{name: "slow but live", mutate: func(w *osdCPUWindow) {}, want: osdWindowThrottledLive},
		{name: "daemon stopped mid-window", want: osdWindowExited, wrong: osdWindowThrottledLive,
			mutate:  func(w *osdCPUWindow) { instance(w, 1, func(v *osdInstanceView) { v.Running = false }) },
			disable: func(c *osdCPUWindowChecks) { c.skipExit = true }},
		{name: "daemon OOM-killed", want: osdWindowExited, wrong: osdWindowThrottledLive,
			mutate:  func(w *osdCPUWindow) { instance(w, 2, func(v *osdInstanceView) { v.OOMKilled = true }) },
			disable: func(c *osdCPUWindowChecks) { c.skipExit = true }},
		{name: "replaced process", want: osdWindowExited, wrong: osdWindowThrottledLive,
			mutate:  func(w *osdCPUWindow) { instance(w, 2, func(v *osdInstanceView) { v.PID = 9001 }) },
			disable: func(c *osdCPUWindowChecks) { c.skipExit = true }},
		{name: "restarted generation", want: osdWindowExited, wrong: osdWindowThrottledLive,
			mutate: func(w *osdCPUWindow) {
				instance(w, 2, func(v *osdInstanceView) { v.StartedAt = "2026-10-27T00:00:00Z" })
			},
			disable: func(c *osdCPUWindowChecks) { c.skipExit = true }},
		{name: "exit seen only by the post-window sample with no kernel reading", want: osdWindowExited, wrong: osdWindowUnsupported,
			mutate: func(w *osdCPUWindow) {
				instance(w, 2, func(v *osdInstanceView) { v.Running = false })
				w.After = nil
			},
			disable: func(c *osdCPUWindowChecks) { c.skipExit = true }},
		{name: "marked down", want: osdWindowMarkedDown, wrong: osdWindowThrottledLive,
			mutate:  func(w *osdCPUWindow) { entry(w, 1, func(m *osdMapEntry) { m.Up = false }) },
			disable: func(c *osdCPUWindowChecks) { c.skipMapDown = true }},
		{name: "flapped between samples", want: osdWindowMarkedDown, wrong: osdWindowThrottledLive,
			mutate: func(w *osdCPUWindow) {
				entry(w, 2, func(m *osdMapEntry) { m.DownAt = 44; m.UpFrom = 46 })
			},
			disable: func(c *osdCPUWindowChecks) { c.skipMapDown = true }},
		{name: "no sample inside the window", want: osdWindowIncomplete, wrong: osdWindowThrottledLive,
			mutate: func(w *osdCPUWindow) {
				w.Samples = []osdSlowLiveView{osdView(osdT0.Add(13 * time.Second))}
			},
			disable: func(c *osdCPUWindowChecks) { c.skipSamples = true }},
		{name: "samples lost the map", want: osdWindowIncomplete, wrong: osdWindowThrottledLive,
			mutate: func(w *osdCPUWindow) {
				for i := range w.Samples {
					w.Samples[i].Map = nil
				}
			},
			disable: func(c *osdCPUWindowChecks) { c.skipSamples = true }},
		{name: "baseline unreadable", want: osdWindowIncomplete, mutate: func(w *osdCPUWindow) { w.Baseline.Map = nil }},
		{name: "target not up before the workload", want: osdWindowIncomplete,
			mutate: func(w *osdCPUWindow) { m := *w.Baseline.Map; m.Up = false; w.Baseline.Map = &m }},
		{name: "workload outcome unknown", want: osdWindowIncomplete, wrong: osdWindowThrottledLive,
			mutate: func(w *osdCPUWindow) { w.Load = nil }, disable: func(c *osdCPUWindowChecks) { c.skipLoad = true }},
		{name: "workload submitted nothing", want: osdWindowIncomplete, wrong: osdWindowThrottledLive,
			mutate:  func(w *osdCPUWindow) { w.Load = &osdCPULoadReport{WallNanos: 1_000_000_000} },
			disable: func(c *osdCPUWindowChecks) { c.skipLoad = true }},
		{name: "throttled without client progress", want: osdWindowNoProgress, wrong: osdWindowThrottledLive,
			mutate:  func(w *osdCPUWindow) { w.Load = &osdCPULoadReport{WallNanos: 10_000_000_000, Submitted: 5, Errors: 5} },
			disable: func(c *osdCPUWindowChecks) { c.skipLoad = true }},
		{name: "quota never throttled", want: osdWindowNotThrottled,
			mutate: func(w *osdCPUWindow) { w.After = osdWitness(osdT0.Add(13*time.Second), osdQuota, 120, 0, 0) }},
		{name: "unthrottled with no progress", want: osdWindowIncomplete,
			mutate: func(w *osdCPUWindow) {
				w.After = osdWitness(osdT0.Add(13*time.Second), osdQuota, 120, 0, 0)
				w.Load = &osdCPULoadReport{WallNanos: 10_000_000_000, Submitted: 3, Errors: 3}
			}},
		{name: "no controller witness", want: osdWindowUnsupported,
			mutate: func(w *osdCPUWindow) { w.Before = nil; w.After = nil }},
		{name: "reading from another generation", want: osdWindowIncomplete,
			mutate: func(w *osdCPUWindow) {
				w.After.Generation = containerGeneration{ID: "c2", StartedAt: cpuGen.StartedAt}
			}},
		{name: "first reading after the start", want: osdWindowIncomplete, wrong: osdWindowThrottledLive,
			mutate:  func(w *osdCPUWindow) { w.Before.At = w.ExecStart.Add(time.Millisecond) },
			disable: func(c *osdCPUWindowChecks) { c.skipBracket = true }},
		{name: "second reading before the end", want: osdWindowIncomplete, wrong: osdWindowThrottledLive,
			mutate:  func(w *osdCPUWindow) { w.After.At = w.ExecEnd.Add(-time.Millisecond) },
			disable: func(c *osdCPUWindowChecks) { c.skipBracket = true }},
		{name: "workload longer than its window", want: osdWindowIncomplete, wrong: osdWindowThrottledLive,
			mutate:  func(w *osdCPUWindow) { l := *w.Load; l.WallNanos = 12_000_000_000; w.Load = &l },
			disable: func(c *osdCPUWindowChecks) { c.skipBracket = true }},
		{name: "oversized wall duration", want: osdWindowIncomplete, wrong: osdWindowThrottledLive,
			mutate:  func(w *osdCPUWindow) { l := *w.Load; l.WallNanos = 1 << 63; w.Load = &l },
			disable: func(c *osdCPUWindowChecks) { c.skipBracket = true }},
		{name: "quota changed during the window", want: osdWindowIncomplete, wrong: osdWindowThrottledLive,
			mutate: func(w *osdCPUWindow) {
				w.After = osdWitness(osdT0.Add(13*time.Second), cpuMax{Quota: 100000, Period: 100000}, 120, 100, 7_000_000)
			},
			disable: func(c *osdCPUWindowChecks) { c.skipQuota = true }},
		{name: "expected quota not finite", want: osdWindowIncomplete,
			mutate: func(w *osdCPUWindow) {
				w.Expected = cpuMax{Unlimited: true}
				w.Before.Max, w.After.Max = w.Expected, w.Expected
			}},
		{name: "throttled periods exceed elapsed periods", want: osdWindowIncomplete, wrong: osdWindowThrottledLive,
			mutate:  func(w *osdCPUWindow) { w.After.Stat.Throttled = 500 },
			disable: func(c *osdCPUWindowChecks) { c.skipPeriods = true }},
		{name: "frozen process missing", want: osdWindowIncomplete, mutate: func(w *osdCPUWindow) { w.PID = 0 }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := osdWindow(t)
			c.mutate(&w)
			if got, err := classifyOSDCPUWindow(w, osdCPUWindowChecks{}); got != c.want {
				t.Fatalf("verdict %s (%v), want %s", got, err, c.want)
			}
			if c.disable == nil {
				return
			}
			var checks osdCPUWindowChecks
			c.disable(&checks)
			if got, _ := classifyOSDCPUWindow(w, checks); got != c.wrong {
				t.Fatalf("negative control: with the rule disabled the case is %s, want the wrong verdict %s", got, c.wrong)
			}
		})
	}
}

func osdRunGood() osdCPURun {
	const base, tight = int64(2_000_000_000), int64(250_000_000)
	applied := func(requested int64) cpuTransitionRecord {
		return cpuTransitionRecord{
			Generation: cpuGen, RequestedNano: requested, ReplyReceived: true,
			AfterState:  cpuEngineState{ID: cpuGen.ID, StartedAt: cpuGen.StartedAt, PID: 4242, Running: true},
			AfterLimits: cpuEngineLimits{NanoCPUs: requested},
			Kernel: &cpuWitness{Generation: cpuGen, At: osdT0, Source: cpuWitnessSource, ControllerListed: true,
				Max: cpuMax{Quota: uint64(requested / 10_000), Period: 100000}},
		}
	}
	return osdCPURun{
		Baseline: base, Tight: tight, Admit: applied(base), Tighten: applied(tight), Restore: applied(base),
		Control: osdWindowNotThrottled, Pressure: osdWindowThrottledLive, Back: osdWindowNotThrottled,
		LedgerAcksAfterRestore: true, LedgerVerified: true, ClusterClean: true, SameInstanceAtEnd: true,
	}
}

func TestOSDCPURunClassification(t *testing.T) {
	if v, why := classifyOSDCPURun(osdRunGood()); v != osdRunSlowLive || len(why) != 0 {
		t.Fatalf("good run: %s %v", v, why)
	}
	cases := []struct {
		name   string
		mutate func(*osdCPURun)
		want   osdCPURunVerdict
	}{
		{"exit in the pressure window", func(r *osdCPURun) { r.Pressure = osdWindowExited }, osdRunExited},
		{"exit while restored", func(r *osdCPURun) { r.Back = osdWindowExited; r.SameInstanceAtEnd = false }, osdRunExited},
		{"marked down under pressure", func(r *osdCPURun) { r.Pressure = osdWindowMarkedDown }, osdRunMarkedDown},
		{"exit outranks marked down", func(r *osdCPURun) { r.Control = osdWindowMarkedDown; r.Pressure = osdWindowExited }, osdRunExited},
		{"pressure not reproduced", func(r *osdCPURun) { r.Pressure = osdWindowNotThrottled }, osdRunNotReached},
		{"throttled without progress", func(r *osdCPURun) { r.Pressure = osdWindowNoProgress }, osdRunNotReached},
		{"pressure window incomplete", func(r *osdCPURun) { r.Pressure = osdWindowIncomplete }, osdRunInconclusive},
		{"control window throttled", func(r *osdCPURun) { r.Control = osdWindowThrottledLive }, osdRunInconclusive},
		{"restored window throttled", func(r *osdCPURun) { r.Back = osdWindowThrottledLive }, osdRunInconclusive},
		{"ledger not verified", func(r *osdCPURun) { r.LedgerVerified = false }, osdRunInconclusive},
		{"no acknowledgment after restoration", func(r *osdCPURun) { r.LedgerAcksAfterRestore = false }, osdRunInconclusive},
		{"cluster not clean", func(r *osdCPURun) { r.ClusterClean = false }, osdRunInconclusive},
		{"target replaced at the end", func(r *osdCPURun) { r.SameInstanceAtEnd = false }, osdRunInconclusive},
		{"tighten not applied", func(r *osdCPURun) { r.Tighten.AfterLimits.NanoCPUs = 0 }, osdRunInconclusive},
		{"restore request differs from the baseline", func(r *osdCPURun) { r.Restore.RequestedNano = 1_000_000_000 }, osdRunInconclusive},
		{"restore with a lost reply", func(r *osdCPURun) { r.Restore.ReplyReceived = false }, osdRunInconclusive},
		{"admission from another generation", func(r *osdCPURun) { r.Admit.AfterState.StartedAt = "other" }, osdRunInconclusive},
		{"tighten differs from the predeclared limit", func(r *osdCPURun) { r.Tight = 123 }, osdRunInconclusive},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := osdRunGood()
			c.mutate(&r)
			if v, why := classifyOSDCPURun(r); v != c.want || len(why) == 0 {
				t.Fatalf("verdict %s %v, want %s with reasons", v, why, c.want)
			}
		})
	}
}

// osdFakeRados stands in for the rados module. FAKE_FAIL_EVERY makes every
// n-th write raise; each write takes a millisecond so the bounded loop does not
// spin.
const osdFakeRados = `import os, threading, time

EVERY = int(os.environ.get("FAKE_FAIL_EVERY", "0"))
LOCK = threading.Lock()
COUNT = [0]

class Ioctx:
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def write_full(self, name, data):
        assert name.startswith("cpuload-") and len(data) == int(os.environ["FAKE_SIZE"]) and int(name.rsplit("-", 1)[1]) < 32
        time.sleep(0.001)
        with LOCK:
            COUNT[0] += 1
            n = COUNT[0]
        if EVERY and n % EVERY == 0: raise OSError(110, "timed out")

class Rados:
    def __init__(self, **kw): pass
    def connect(self): pass
    def open_ioctx(self, pool): return Ioctx()
    def shutdown(self): pass
`

func runOSDLoad(t *testing.T, env ...string) []byte {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "rados.py"), []byte(osdFakeRados), 0o600); err != nil {
		t.Fatal(err)
	}
	argv := []string{"-I", "-c", "import sys; sys.path.insert(0, sys.argv.pop(1)); exec(sys.argv.pop(1))", dir, osdCPULoadScript, "tc-cpu", "1", "3", "4096", "unit"}
	cmd := exec.Command("python3", argv...)
	cmd.Env = append(os.Environ(), append([]string{"FAKE_SIZE=4096"}, env...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	return out
}

func TestOSDCPULoadScriptBookkeeping(t *testing.T) {
	clean, err := parseOSDCPULoadReport(runOSDLoad(t))
	if err != nil || clean.Acked == 0 || clean.Errors != 0 || clean.Submitted != clean.Acked || clean.P50Nanos == 0 || clean.MaxNanos < clean.P50Nanos {
		t.Fatalf("clean run: %+v %v", clean, err)
	}
	if clean.WallNanos < 1_000_000_000 || clean.WallNanos > 5_000_000_000 {
		t.Fatalf("wall time %d ns is not the declared bounded second", clean.WallNanos)
	}
	failing, err := parseOSDCPULoadReport(runOSDLoad(t, "FAKE_FAIL_EVERY=4"))
	if err != nil || failing.Errors == 0 || failing.Acked == 0 || failing.Acked+failing.Errors != failing.Submitted {
		t.Fatalf("failing run: %+v %v", failing, err)
	}
	// A worker that crashes prints no report, and a partial line is refused.
	if _, err := parseOSDCPULoadReport([]byte(strings.TrimSuffix(osdLoadGood, "}\n"))); err == nil {
		t.Fatal("truncated report accepted")
	}
}
