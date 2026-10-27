//go:build all || integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"time"
)

// This file is the pure model of the first native Ceph-daemon CPU-pressure
// step: one selected owned OSD, one finite CPU ceiling tightened and restored
// with the merged update-transition judge, and a bounded client workload whose
// window is bracketed by the merged target-local cpu.stat witness. It
// dispatches nothing and reads no cgroup, so the Docker-free fixtures drive it
// with synthetic records.
//
// "Slow but live" needs more than a throttled cgroup. The OSD must be the same
// running process generation on every observation, must stay up in the OSD
// map, and the client workload must make progress. A process that exited, was
// OOM-killed or was replaced is "exited", a daemon the monitors marked down is
// "marked-down", and a throttled window without client progress is its own
// verdict. None of these is a successful slow-live window, and none is
// promoted by the counters.
//
// Not covered: OOM, MON memory, other roles, a supported CPU default or budget,
// recovery under pressure, stable behavior across runs, or any engine other
// than the one that ran.

var errOSDCPUWindow = errors.New("osd cpu window is not usable")

// osdCPULoadScript is the bounded client workload, run with python3 -c in the
// client container (outside the target cgroup). argv: pool, seconds, threads,
// object bytes, tag. Each thread overwrites a fixed ring of 32 names so the
// space it can occupy is bounded; no operation is retried. It prints one JSON
// object after every thread returned; a killed worker prints nothing.
const osdCPULoadScript = `import json,sys,threading,time,rados
pool,secs,nthreads,size,tag=sys.argv[1],int(sys.argv[2]),int(sys.argv[3]),int(sys.argv[4]),sys.argv[5]
c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'rados_osd_op_timeout':'30','rados_mon_op_timeout':'30'})
c.connect()
lock=threading.Lock()
lat=[]
errs=[0]
sub=[0]
payload=bytes((i*31+7)%251 for i in range(size))
t0=time.monotonic_ns()
end=t0+secs*1000000000
def work(k):
    with c.open_ioctx(pool) as io:
        n=0
        while time.monotonic_ns()<end:
            name='cpuload-%s-%d-%d'%(tag,k,n%32)
            n+=1
            with lock: sub[0]+=1
            s=time.monotonic_ns()
            try:
                io.write_full(name,payload)
                d=time.monotonic_ns()-s
                with lock: lat.append(d)
            except Exception:
                with lock: errs[0]+=1
ths=[threading.Thread(target=work,args=(k,)) for k in range(nthreads)]
for x in ths: x.start()
for x in ths: x.join()
c.shutdown()
lat.sort()
print(json.dumps({"completed":True,"wall_ns":time.monotonic_ns()-t0,"submitted":sub[0],"acked":len(lat),"errors":errs[0],"p50_ns":lat[len(lat)//2] if lat else 0,"max_ns":lat[-1] if lat else 0}))
`

// osdCPULoadReport is the decoded completion report. Every field is required;
// a missing, null or wrong-typed field refuses the report instead of decoding
// to a zero that would count as progress or as "no errors".
type osdCPULoadReport struct {
	WallNanos, Submitted, Acked, Errors, P50Nanos, MaxNanos uint64
}

// parseOSDCPULoadReport decodes the worker's single JSON object. completed
// must be the JSON value true; numbers must be plain non-negative integers; the
// outcomes must account for every submission.
func parseOSDCPULoadReport(data []byte) (osdCPULoadReport, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var raw map[string]json.RawMessage
	if err := dec.Decode(&raw); err != nil {
		return osdCPULoadReport{}, fmt.Errorf("%w: load report is not a JSON object: %v", errOSDCPUWindow, err)
	}
	if rest := bytes.TrimSpace(data[dec.InputOffset():]); len(rest) != 0 {
		return osdCPULoadReport{}, fmt.Errorf("%w: trailing bytes after the load report", errOSDCPUWindow)
	}
	if v, ok := raw["completed"]; !ok || string(bytes.TrimSpace(v)) != "true" {
		return osdCPULoadReport{}, fmt.Errorf("%w: load report is not marked completed", errOSDCPUWindow)
	}
	var r osdCPULoadReport
	for key, dst := range map[string]*uint64{
		"wall_ns": &r.WallNanos, "submitted": &r.Submitted, "acked": &r.Acked,
		"errors": &r.Errors, "p50_ns": &r.P50Nanos, "max_ns": &r.MaxNanos,
	} {
		v, ok := raw[key]
		if !ok {
			return osdCPULoadReport{}, fmt.Errorf("%w: load report field %q is missing", errOSDCPUWindow, key)
		}
		s := string(bytes.TrimSpace(v))
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil || strconv.FormatUint(n, 10) != s || n > math.MaxInt64 {
			return osdCPULoadReport{}, fmt.Errorf("%w: load report field %q is not a non-negative integer", errOSDCPUWindow, key)
		}
		*dst = n
	}
	if r.Acked+r.Errors != r.Submitted {
		return osdCPULoadReport{}, fmt.Errorf("%w: %d acknowledged and %d failed do not account for %d submissions", errOSDCPUWindow, r.Acked, r.Errors, r.Submitted)
	}
	return r, nil
}

// osdInstanceView is the engine's account of the target container at one
// moment: the running identity compared with the frozen one.
type osdInstanceView struct {
	Running, OOMKilled bool
	PID                int
	StartedAt          string
}

// osdMapEntry is the target's entry in one "osd dump".
type osdMapEntry struct {
	Epoch          int
	Up, In         bool
	UpFrom, DownAt int
}

// parseOSDMapEntry extracts one OSD from "osd dump --format json". Epoch, the
// osds list, and the entry's osd, up, in, up_from and down_at fields are all
// required and typed: a missing or null field is an error, never a down or
// zero entry.
func parseOSDMapEntry(data []byte, osd int) (osdMapEntry, error) {
	var dump struct {
		Epoch *int `json:"epoch"`
		OSDs  *[]struct {
			OSD    *int `json:"osd"`
			Up     *int `json:"up"`
			In     *int `json:"in"`
			UpFrom *int `json:"up_from"`
			DownAt *int `json:"down_at"`
		} `json:"osds"`
	}
	if err := json.Unmarshal(data, &dump); err != nil {
		return osdMapEntry{}, fmt.Errorf("osd dump: %w", err)
	}
	if dump.Epoch == nil || dump.OSDs == nil {
		return osdMapEntry{}, errors.New("osd dump has no epoch or osds")
	}
	for _, e := range *dump.OSDs {
		if e.OSD == nil || *e.OSD != osd {
			continue
		}
		if e.Up == nil || e.In == nil || e.UpFrom == nil || e.DownAt == nil {
			return osdMapEntry{}, fmt.Errorf("osd.%d entry lacks up, in, up_from or down_at", osd)
		}
		return osdMapEntry{Epoch: *dump.Epoch, Up: *e.Up == 1, In: *e.In == 1, UpFrom: *e.UpFrom, DownAt: *e.DownAt}, nil
	}
	return osdMapEntry{}, fmt.Errorf("osd.%d is not in the osd dump", osd)
}

// osdSlowLiveView is one observation of the target. A part that could not be
// read stays nil or false-with-an-error; it is never decoded as healthy.
type osdSlowLiveView struct {
	At       time.Time
	Errs     []string
	Instance *osdInstanceView
	Map      *osdMapEntry
	// PGState is the selected PG's state string, PGRead whether it was read.
	PGState       string
	PGRead        bool
	ActingPrimary int
	HealthCodes   []string
	HealthRead    bool
}

// osdCPUWindow is one bounded workload window on the frozen target.
type osdCPUWindow struct {
	Label      string
	Generation containerGeneration
	// PID and Expected are the frozen daemon process and the finite cpu.max
	// that must be installed on both kernel readings.
	PID      int
	Expected cpuMax

	ExecStart, ExecEnd time.Time
	// Load is nil when the worker outcome is unknown (exec error, nonzero
	// exit, unparsable output, deadline). LoadErr keeps the reason.
	Load    *osdCPULoadReport
	LoadErr string

	Before, After *cpuWitness
	// BeforeErr and AfterErr keep why a kernel reading is absent.
	BeforeErr, AfterErr string

	// Baseline is the observation taken before the workload started. Samples
	// are the observations taken while it ran.
	Baseline *osdSlowLiveView
	Samples  []osdSlowLiveView
}

type osdCPUWindowVerdict string

const (
	// osdWindowThrottledLive: all rules held, the kernel throttling counters
	// advanced, the client made progress and the same daemon stayed up.
	osdWindowThrottledLive osdCPUWindowVerdict = "throttled-live"
	// osdWindowNotThrottled: the same, but no throttling counter advanced. As
	// the baseline control this is the expected clean outcome; as the tight
	// window it means the pressure was not reproduced.
	osdWindowNotThrottled osdCPUWindowVerdict = "not-throttled"
	// osdWindowNoProgress: throttled, daemon up, but no client write was
	// acknowledged.
	osdWindowNoProgress osdCPUWindowVerdict = "throttled-no-progress"
	// osdWindowExited: an observation shows the target stopped, OOM-killed or
	// replaced. This is process death, not slow-live.
	osdWindowExited osdCPUWindowVerdict = "exited"
	// osdWindowMarkedDown: the process kept its identity but the OSD map shows
	// it down or flapped.
	osdWindowMarkedDown osdCPUWindowVerdict = "marked-down"
	// osdWindowUnsupported: no admitted controller witness.
	osdWindowUnsupported osdCPUWindowVerdict = "unsupported"
	// osdWindowIncomplete: anything else, including evidence that is missing,
	// from another generation, outside the window or contradictory.
	osdWindowIncomplete osdCPUWindowVerdict = "incomplete"
)

// osdCPUWindowChecks disables one rule per negative control.
type osdCPUWindowChecks struct {
	skipExit    bool
	skipMapDown bool
	skipSamples bool
	skipTimes   bool
	skipLoad    bool
	skipBracket bool
	skipQuota   bool
	skipPeriods bool
}

// classifyOSDCPUWindow judges one window. Process death and a map-down daemon
// outrank the kernel counters; missing evidence is incomplete or unsupported.
func classifyOSDCPUWindow(w osdCPUWindow, checks osdCPUWindowChecks) (osdCPUWindowVerdict, error) {
	if !w.Generation.valid() || w.PID <= 0 {
		return osdWindowIncomplete, fmt.Errorf("%w: frozen generation or process is incomplete", errOSDCPUWindow)
	}
	views := append([]osdSlowLiveView(nil), w.Samples...)
	if w.Baseline != nil {
		views = append(views, *w.Baseline)
	}
	if !checks.skipExit {
		for _, v := range views {
			if i := v.Instance; i != nil && (!i.Running || i.OOMKilled || i.PID != w.PID || i.StartedAt != w.Generation.StartedAt) {
				return osdWindowExited, fmt.Errorf("%w: target observed as %+v, frozen pid %d started %s", errOSDCPUWindow, *i, w.PID, w.Generation.StartedAt)
			}
		}
	}
	// Every recorded time must exist before any ordering is judged: the zero
	// time compares before every real one and would pass as "earlier".
	if !checks.skipTimes {
		switch {
		case w.ExecStart.IsZero() || w.ExecEnd.IsZero():
			return osdWindowIncomplete, fmt.Errorf("%w: execution window time is missing", errOSDCPUWindow)
		case w.Baseline != nil && (w.Baseline.At.IsZero() || w.Baseline.At.After(w.ExecStart)):
			return osdWindowIncomplete, fmt.Errorf("%w: pre-workload observation has no time or was taken after the start", errOSDCPUWindow)
		case (w.Before != nil && w.Before.At.IsZero()) || (w.After != nil && w.After.At.IsZero()):
			return osdWindowIncomplete, fmt.Errorf("%w: kernel reading has no timestamp", errOSDCPUWindow)
		}
		for _, v := range w.Samples {
			if v.At.IsZero() {
				return osdWindowIncomplete, fmt.Errorf("%w: observation has no timestamp", errOSDCPUWindow)
			}
		}
	}
	if w.Baseline == nil || w.Baseline.Instance == nil || w.Baseline.Map == nil {
		return osdWindowIncomplete, fmt.Errorf("%w: no usable observation before the workload", errOSDCPUWindow)
	}
	if !checks.skipMapDown {
		base := *w.Baseline.Map
		if !base.Up {
			return osdWindowIncomplete, fmt.Errorf("%w: target was not up before the workload", errOSDCPUWindow)
		}
		for _, v := range w.Samples {
			if m := v.Map; m != nil && (!m.Up || m.UpFrom != base.UpFrom || m.DownAt != base.DownAt) {
				return osdWindowMarkedDown, fmt.Errorf("%w: osd map entry %+v differs from the baseline %+v", errOSDCPUWindow, *m, base)
			}
		}
	}
	// A sample counts only when it was taken inside the execution window, and
	// only a sample that read both the engine and the map is a heartbeat/OSD
	// observation of the target.
	if !checks.skipSamples {
		usable := 0
		for _, v := range w.Samples {
			if v.Instance != nil && v.Map != nil && !v.At.Before(w.ExecStart) && !v.At.After(w.ExecEnd) {
				usable++
			}
		}
		if usable == 0 {
			return osdWindowIncomplete, fmt.Errorf("%w: no complete observation inside the execution window", errOSDCPUWindow)
		}
	}
	if w.Before == nil || w.After == nil {
		return osdWindowUnsupported, nil
	}
	if w.Before.Generation != w.Generation || w.After.Generation != w.Generation {
		return osdWindowIncomplete, fmt.Errorf("%w: readings do not belong to the frozen generation", errOSDCPUWindow)
	}
	if !checks.skipLoad {
		if w.Load == nil {
			return osdWindowIncomplete, fmt.Errorf("%w: workload outcome is unknown", errOSDCPUWindow)
		}
		if w.Load.Submitted == 0 || w.Load.WallNanos == 0 {
			return osdWindowIncomplete, fmt.Errorf("%w: workload submitted nothing", errOSDCPUWindow)
		}
	}
	if !checks.skipBracket {
		switch {
		case w.ExecStart.IsZero() || !w.ExecEnd.After(w.ExecStart):
			return osdWindowIncomplete, fmt.Errorf("%w: execution window is not ordered", errOSDCPUWindow)
		case w.Before.At.After(w.ExecStart):
			return osdWindowIncomplete, fmt.Errorf("%w: first reading was taken after the workload started", errOSDCPUWindow)
		case w.After.At.Before(w.ExecEnd):
			return osdWindowIncomplete, fmt.Errorf("%w: second reading was taken before the workload ended", errOSDCPUWindow)
		}
		// Unsigned comparison: converting an oversized report to a signed
		// duration would wrap it negative.
		if span := w.ExecEnd.Sub(w.ExecStart); w.Load != nil && (span <= 0 || w.Load.WallNanos > uint64(span)) {
			return osdWindowIncomplete, fmt.Errorf("%w: workload duration exceeds the observed execution window", errOSDCPUWindow)
		}
	}
	if !checks.skipQuota {
		if w.Expected.Unlimited || w.Expected.Quota == 0 || w.Expected.Period == 0 {
			return osdWindowIncomplete, fmt.Errorf("%w: expected quota is not finite", errOSDCPUWindow)
		}
		if w.Before.Max != w.Expected || w.After.Max != w.Expected {
			return osdWindowIncomplete, fmt.Errorf("%w: installed quota differs from the expected one", errOSDCPUWindow)
		}
	}
	verdict, err := classifyCPUThrottle(w.Before, w.After, false, cpuClassifyChecks{})
	acked := w.Load != nil && w.Load.Acked > 0
	switch verdict {
	case cpuVerdictUnsupported:
		return osdWindowUnsupported, err
	case cpuVerdictObservedZero:
		if !acked && !checks.skipLoad {
			return osdWindowIncomplete, fmt.Errorf("%w: no client write was acknowledged in an unthrottled window", errOSDCPUWindow)
		}
		return osdWindowNotThrottled, nil
	case cpuVerdictThrottled:
		b, a := w.Before.Stat, w.After.Stat
		if !checks.skipPeriods && (a.Periods <= b.Periods || a.Throttled-b.Throttled > a.Periods-b.Periods) {
			return osdWindowIncomplete, fmt.Errorf("%w: throttled periods are not within the elapsed periods", errOSDCPUWindow)
		}
		if !acked && !checks.skipLoad {
			return osdWindowNoProgress, nil
		}
		return osdWindowThrottledLive, nil
	}
	return osdWindowIncomplete, err
}

// osdCPURun is everything the scenario retains.
type osdCPURun struct {
	// Identity is the frozen target. Every transition, window and kernel
	// reading must name it.
	Identity osdCPUIdentity
	// Refs join each window to the transition that installed its limit.
	ControlRef, PressureRef, BackRef osdCPUWindowRef
	Baseline, Tight                  int64
	// Admit moves the OSD from the unlimited creation state to the finite
	// baseline. That step is not reversed; Tighten and Restore are the round
	// trip.
	Admit, Tighten, Restore cpuTransitionRecord
	Control, Pressure, Back osdCPUWindowVerdict
	// Post-restore checks, each recorded by the native scenario.
	LedgerAcksAfterRestore bool // the live writer was acknowledged again after restoration was confirmed
	LedgerVerified         bool // every acknowledged object read back byte for byte, none torn or replayed
	ClusterClean           bool // every OSD up and in, the PG active+clean
	SameInstanceAtEnd      bool // the target is still the frozen process generation
}

// osdCPUIdentity is the frozen container generation and daemon process.
type osdCPUIdentity struct {
	Generation containerGeneration
	PID        int
}

// osdCPUWindowRef is what a window says about itself: its target, the limit it
// expected and its execution times.
type osdCPUWindowRef struct {
	Identity           osdCPUIdentity
	Expected           cpuMax
	ExecStart, ExecEnd time.Time
}

// osdCPUJoin refuses a run whose records do not all describe one target in one
// order: each transition and its kernel reading must name the frozen
// generation and process, each window must expect the limit its transition
// installed, and the sequence admit, control, tighten, pressure, restore, back
// must be strictly ordered in time with every time present.
func osdCPUJoin(r osdCPURun) []string {
	var out []string
	id := r.Identity
	if !id.Generation.valid() || id.PID <= 0 {
		return []string{"frozen identity is incomplete"}
	}
	for name, tr := range map[string]cpuTransitionRecord{"admit": r.Admit, "tighten": r.Tighten, "restore": r.Restore} {
		if tr.Generation != id.Generation || tr.AfterState.ID != id.Generation.ID || tr.AfterState.StartedAt != id.Generation.StartedAt || tr.AfterState.PID != id.PID {
			out = append(out, name+" transition does not name the frozen target")
		}
		if tr.Kernel == nil || tr.Kernel.Generation != id.Generation || tr.Kernel.At.IsZero() {
			out = append(out, name+" kernel reading is missing, has no time or names another target")
		}
	}
	for name, ref := range map[string]osdCPUWindowRef{"control": r.ControlRef, "pressure": r.PressureRef, "restored": r.BackRef} {
		if ref.Identity != id {
			out = append(out, name+" window does not name the frozen target")
		}
		if ref.ExecStart.IsZero() || !ref.ExecEnd.After(ref.ExecStart) {
			out = append(out, name+" window times are missing or unordered")
		}
	}
	for name, p := range map[string]struct {
		ref osdCPUWindowRef
		tr  cpuTransitionRecord
	}{"control": {r.ControlRef, r.Admit}, "pressure": {r.PressureRef, r.Tighten}, "restored": {r.BackRef, r.Restore}} {
		if p.tr.Kernel != nil && p.ref.Expected != p.tr.Kernel.Max {
			out = append(out, name+" window expected a limit its transition did not install")
		}
	}
	if r.Admit.Kernel != nil && r.Tighten.Kernel != nil && r.Restore.Kernel != nil {
		ordered := r.Admit.Kernel.At.Before(r.ControlRef.ExecStart) && r.ControlRef.ExecEnd.Before(r.Tighten.Kernel.At) &&
			r.Tighten.Kernel.At.Before(r.PressureRef.ExecStart) && r.PressureRef.ExecEnd.Before(r.Restore.Kernel.At) &&
			r.Restore.Kernel.At.Before(r.BackRef.ExecStart)
		if !ordered {
			out = append(out, "transitions and windows are not in the declared order")
		}
	}
	return out
}

type osdCPURunVerdict string

const (
	osdRunSlowLive     osdCPURunVerdict = "slow-live-established"
	osdRunExited       osdCPURunVerdict = "exited"
	osdRunMarkedDown   osdCPURunVerdict = "marked-down"
	osdRunNotReached   osdCPURunVerdict = "pressure-not-established"
	osdRunInconclusive osdCPURunVerdict = "inconclusive"
)

// classifyOSDCPURun combines the windows and the post-restore checks. Only a
// throttled-live pressure window between two clean unthrottled windows, a
// verified finite round trip and the restoration checks is a successful
// slow-live run; an exit or a map-down daemon is reported as such wherever it
// appeared, and every other shortfall is listed.
func classifyOSDCPURun(r osdCPURun) (osdCPURunVerdict, []string) {
	var reasons []string
	windows := map[string]osdCPUWindowVerdict{"control": r.Control, "pressure": r.Pressure, "restored": r.Back}
	for _, name := range []string{"control", "pressure", "restored"} {
		if windows[name] == osdWindowExited {
			return osdRunExited, []string{name + " window: target process exited, was OOM-killed or was replaced"}
		}
	}
	for _, name := range []string{"control", "pressure", "restored"} {
		if windows[name] == osdWindowMarkedDown {
			return osdRunMarkedDown, []string{name + " window: target marked down in the OSD map"}
		}
	}
	reasons = append(reasons, osdCPUJoin(r)...)
	for name, tr := range map[string]cpuTransitionRecord{"admit": r.Admit, "tighten": r.Tighten, "restore": r.Restore} {
		if v, err := classifyCPUTransition(tr, cpuTransitionChecks{}); v != cpuTransitionApplied {
			reasons = append(reasons, fmt.Sprintf("%s transition is %s: %v", name, v, err))
		}
	}
	if err := verifyCPURoundTrip(r.Baseline, r.Tighten, r.Restore, cpuRoundTripChecks{}); err != nil {
		reasons = append(reasons, "finite round trip: "+err.Error())
	}
	if r.Admit.RequestedNano != r.Baseline {
		reasons = append(reasons, "admission did not request the baseline")
	}
	if r.Tighten.RequestedNano != r.Tight {
		reasons = append(reasons, "tighten did not request the predeclared limit")
	}
	notReached := false
	if r.Pressure != osdWindowThrottledLive {
		reasons = append(reasons, "pressure window is "+string(r.Pressure))
		notReached = r.Pressure == osdWindowNotThrottled || r.Pressure == osdWindowNoProgress
	}
	if r.Control != osdWindowNotThrottled {
		reasons = append(reasons, "baseline control window is "+string(r.Control))
	}
	if r.Back != osdWindowNotThrottled {
		reasons = append(reasons, "restored window is "+string(r.Back))
	}
	for name, ok := range map[string]bool{
		"live writer acknowledged after restoration": r.LedgerAcksAfterRestore,
		"ledger read back byte for byte":             r.LedgerVerified,
		"cluster clean on every OSD":                 r.ClusterClean,
		"target is still the frozen generation":      r.SameInstanceAtEnd,
	} {
		if !ok {
			reasons = append(reasons, "missing: "+name)
		}
	}
	slices.Sort(reasons)
	if len(reasons) == 0 {
		return osdRunSlowLive, nil
	}
	if notReached && len(reasons) == 1 {
		return osdRunNotReached, reasons
	}
	return osdRunInconclusive, reasons
}
