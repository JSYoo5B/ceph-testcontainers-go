//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// The client-side receipts of a majority MON loss are kept apart from the
// local-MON oracle: a client call that times out, errors or succeeds does not
// decide whether a majority exists, and a readback never turns a call whose
// outcome is unknown into an acknowledgement. Two kinds of clients are used.
//
//   - One retained librados client opens its session and ioctx before the
//     first interruption and keeps them. Its reads of a seeded object and its
//     writes of unique objects are separate calls. Each call gets an "intent"
//     record before it is submitted and a "return" record after it came back,
//     both flushed, and neither is ever replayed.
//   - A fresh probe is a new process with a new librados instance. Its MON
//     connection, ioctx open (which waits for the first OSD map from a MON)
//     and OSD read are three separate calls. The connection result is
//     recorded the moment connect() returns, before any ioctx or I/O.
//
// Every call is bounded by the client's own librados timeouts, and a fresh
// probe is additionally bounded by a watchdog that kills and reaps its
// process, so that a stall in one stage is recorded as pending and does not
// hide the stages before it. The timeouts are source-backed (Tentacle 20.2.4,
// commit 7f793731f1b39eb4f465e960113d2363c311b964):
//
//   - client_mount_timeout (default 300 s) bounds MonClient::authenticate in
//     RadosClient::connect and surfaces as ETIMEDOUT;
//   - rados_mon_op_timeout (default 0, unlimited) bounds
//     RadosClient::wait_for_osdmap, which every pool lookup in open_ioctx
//     calls until the first OSD map arrived, and surfaces as ETIMEDOUT;
//   - rados_osd_op_timeout (default 0, unlimited) arms a timer per Objecter
//     op that cancels it with ETIMEDOUT. A cancelled write may still be
//     applied by the OSD, so a timed-out write is an unknown outcome.
//
// An established session needs no MON for an OSD op, so a retained read or
// write may well complete during the loss; these helpers record what each
// call did instead of assuming it.

const (
	monLossPhaseBaseline  = "baseline"  // before any interruption
	monLossPhaseApplying  = "applying"  // an interruption is about to be, or is being, applied
	monLossPhaseMinority  = "minority"  // two MONs isolated and 3/5 verified
	monLossPhaseOutage    = "outage"    // majority loss verified by the local oracle
	monLossPhaseRestoring = "restoring" // restoration is about to start
	monLossPhaseRecovered = "recovered" // 5/5 verified again
	monLossPhaseStop      = "stop"      // the retained client ends after its current call
)

const (
	monLossLedgerDir = "/tmp/tc-mon-loss-client"
	monLossSeed      = "seed"
	monLossErrnoTime = 110 // ETIMEDOUT on Linux
)

// monLossClientScript is the retained client, the fresh probe, and the
// controller's helpers. Modes: "token" atomically replaces the phase token and
// records it; "ledger" prints the raw ledger; "seed" writes the seeded object;
// "read" reads named objects and lists the pool with a fresh connection;
// "actor" runs the retained client; "fresh" runs one fresh probe under a
// watchdog (extra arguments: probe name, watchdog seconds).
const monLossClientScript = `import hashlib,json,os,signal,sys,time
mode,pool,path,run=sys.argv[1:5]
ledger=os.path.join(path,'ledger.jsonl'); token=os.path.join(path,'phase')
SEED='seed'
CONF={'keyring':'/etc/ceph/ceph.client.admin.keyring','client_mount_timeout':'30','rados_mon_op_timeout':'15','rados_osd_op_timeout':'15'}
def emit(event,**kw):
    os.makedirs(path,exist_ok=True)
    fd=os.open(ledger,os.O_WRONLY|os.O_APPEND|os.O_CREAT,0o600)
    try:
        os.write(fd,(json.dumps(dict(event=event,ns=time.monotonic_ns(),pid=os.getpid(),run=run,**kw),sort_keys=True)+'\n').encode()); os.fsync(fd)
    finally: os.close(fd)
def phase():
    try: return open(token).read().strip()
    except FileNotFoundError: return ''
def payload(op,seq): return ('op=%s;seq=%d;'%(op,seq)).encode()+bytes((i*7+seq)%251 for i in range(4096+(seq%5)*1024))
def digest(data): return hashlib.sha256(data).hexdigest()
def call(who,kind,op,fn,**intent):
    before=phase()
    emit('intent',who=who,kind=kind,op=op,phase=before,**intent)
    began=time.monotonic()
    try:
        result=dict(ok=True,**(fn() or {}))
    except Exception as e:
        result=dict(ok=False,exc=type(e).__name__,errno=getattr(e,'errno',None),message=str(e)[:160])
    emit('return',who=who,kind=kind,op=op,ms=int((time.monotonic()-began)*1000),phase_after=phase(),**result)
    return result['ok']
def read_seed(io):
    data=io.read(SEED,len(payload(SEED,0))+1,0)
    return dict(sha256=digest(data),len=len(data))
def connect():
    import rados
    c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf=CONF)
    return c
if mode=='token':
    os.makedirs(path,exist_ok=True)
    tmp=token+'.tmp'
    with open(tmp,'w') as f: f.write(sys.argv[5]); f.flush(); os.fsync(f.fileno())
    os.replace(tmp,token); emit('token',value=sys.argv[5]); sys.exit(0)
if mode=='ledger':
    sys.stdout.write(open(ledger).read() if os.path.exists(ledger) else ''); sys.exit(0)
if mode=='seed':
    c=connect(); c.connect()
    try:
        with c.open_ioctx(pool) as io:
            io.write_full(SEED,payload(SEED,0)); print(json.dumps(read_seed(io)))
    finally: c.shutdown()
elif mode=='read':
    c=connect(); c.connect()
    try:
        with c.open_ioctx(pool) as io:
            out={}
            for name in json.loads(sys.argv[5]):
                try:
                    size,_=io.stat(name); data=io.read(name,size+1,0)
                    out[name]=dict(present=True,size=size,data_len=len(data),sha256=digest(data))
                except Exception as e:
                    if type(e).__name__!='ObjectNotFound': raise
                    out[name]=dict(present=False)
            print(json.dumps(dict(objects=out,listing=sorted(o.key for o in io.list_objects()))))
    finally: c.shutdown()
elif mode=='actor':
    c=connect(); c.connect()
    try:
        with c.open_ioctx(pool) as io:
            emit('session',who='retained',instance=c.get_instance_id(),phase=phase())
            reads=writes=0; deadline=time.monotonic()+900
            while reads<2000 and time.monotonic()<deadline and phase()!='stop':
                call('retained','read','%s-r%05d'%(run,reads),lambda: read_seed(io)); reads+=1
                if phase()=='stop': break
                op='%s-w%05d'%(run,writes); data=payload(op,writes)
                call('retained','write',op,lambda: io.write_full('mlc-'+op,data),object='mlc-'+op,seq=writes,bytes=len(data),sha256=digest(data)); writes+=1
                time.sleep(.5)
            emit('stopped',who='retained',reads=reads,writes=writes)
    finally: c.shutdown()
elif mode=='fresh':
    name,watchdog=sys.argv[5],float(sys.argv[6])
    emit('probe_start',who=name,watchdog_s=watchdog)
    child=os.fork()
    if child==0:
        code=0
        try:
            c=connect(); box={}
            def open_connection(): c.connect(); return dict(instance=c.get_instance_id())
            def open_ioctx(): box['io']=c.open_ioctx(pool)
            if call(name,'connect',name+'-connect',open_connection) and call(name,'ioctx',name+'-ioctx',open_ioctx):
                call(name,'read',name+'-read',lambda: read_seed(box['io']))
            try: c.shutdown()
            except Exception: pass
        except BaseException as e:
            emit('probe_crash',who=name,exc=type(e).__name__); code=3
        finally: os._exit(code)
    deadline=time.monotonic()+watchdog; status=None; killed=False
    while status is None and time.monotonic()<deadline:
        done,st=os.waitpid(child,os.WNOHANG)
        if done: status=st
        else: time.sleep(.2)
    if status is None:
        killed=True
        try: os.kill(child,signal.SIGKILL)
        except ProcessLookupError: pass
        reap=time.monotonic()+10
        while status is None and time.monotonic()<reap:
            done,st=os.waitpid(child,os.WNOHANG)
            if done: status=st
            else: time.sleep(.1)
    code=None if status is None else (os.WEXITSTATUS(status) if os.WIFEXITED(status) else -os.WTERMSIG(status))
    emit('probe_end',who=name,killed=killed,reaped=status is not None,exit=code,child=child)
else: raise ValueError('unknown mode')
`

// monLossEvent is one record of the ledger. Required fields are pointers so
// that a missing or null field stays distinguishable from a zero value; the
// parser refuses a record that lacks what its event requires.
type monLossEvent struct {
	Event      string   `json:"event"`
	NS         *int64   `json:"ns"`
	PID        *int     `json:"pid"`
	Run        string   `json:"run"`
	Who        string   `json:"who"`
	Kind       string   `json:"kind"`
	Op         string   `json:"op"`
	Object     string   `json:"object"`
	Seq        *int     `json:"seq"`
	Bytes      *int     `json:"bytes"`
	SHA256     *string  `json:"sha256"`
	Len        *int     `json:"len"`
	Phase      *string  `json:"phase"`
	PhaseAfter *string  `json:"phase_after"`
	OK         *bool    `json:"ok"`
	Exc        *string  `json:"exc"`
	Errno      *int     `json:"errno"`
	Message    string   `json:"message"`
	MS         *int64   `json:"ms"`
	Instance   *uint64  `json:"instance"`
	Value      *string  `json:"value"`
	Reads      *int     `json:"reads"`
	Writes     *int     `json:"writes"`
	WatchdogS  *float64 `json:"watchdog_s"`
	Killed     *bool    `json:"killed"`
	Reaped     *bool    `json:"reaped"`
	Exit       *int     `json:"exit"`
	Child      *int     `json:"child"`
}

// monLossCall is one call of the retained client, assembled from its records.
type monLossCall struct {
	Kind, Op, Object, SHA256 string // SHA256 is the intent's digest for a write
	Seq                      int
	Phase, PhaseAfter        string
	IntentNS, ReturnNS       int64
	Returned, OK             bool
	Exc                      string
	Errno                    *int
	MS                       int64
}

// Outcome is the client's own view: "acked" (the call returned without an
// error), "failed" (it raised) or "inflight" (it has no return record).
func (c monLossCall) Outcome() string {
	switch {
	case !c.Returned:
		return "inflight"
	case !c.OK:
		return "failed"
	}
	return "acked"
}

// Detail names why a failed call failed: a native timeout, or any other error.
func (c monLossCall) Detail() string {
	if c.Outcome() != "failed" {
		return c.Outcome()
	}
	if c.Errno != nil && *c.Errno == monLossErrnoTime {
		return "timeout"
	}
	if c.Errno != nil {
		return fmt.Sprintf("error(%s/%d)", c.Exc, *c.Errno)
	}
	return fmt.Sprintf("error(%s)", c.Exc)
}

// Key is the call's tally key: kind, the token it began under and its
// outcome. A call that began under one token and returned under another
// overlaps a transition and is tallied apart, so it is never credited to a
// phase it did not stay within.
func (c monLossCall) Key() string {
	if c.Returned && c.PhaseAfter != c.Phase {
		return fmt.Sprintf("%s:%s>%s/%s", c.Kind, c.Phase, c.PhaseAfter, c.Detail())
	}
	return fmt.Sprintf("%s:%s/%s", c.Kind, c.Phase, c.Detail())
}

// monLossStage is one call of a fresh probe: "connect", "ioctx" or "read".
type monLossStage struct {
	Kind     string
	Returned bool
	OK       bool
	Exc      string
	Errno    *int
	MS       int64
	Instance uint64
	SHA256   string
	Len      int
}

// State is "ok", "timeout" (ETIMEDOUT from librados), "error" (any other
// raised error, not interpreted as a cause) or "pending" (no return record).
func (s *monLossStage) State() string {
	switch {
	case !s.Returned:
		return "pending"
	case s.OK:
		return "ok"
	case s.Errno != nil && *s.Errno == monLossErrnoTime:
		return "timeout"
	}
	return "error"
}

// monLossProbe is one fresh probe: its stages in order and how it ended.
type monLossProbe struct {
	Name      string
	WatchdogS float64
	Stages    []*monLossStage
	Ended     bool
	Killed    bool
	Reaped    bool
	Exit      *int
	Crashed   bool
}

// Stage returns the named stage, or nil when the probe never began it.
func (p *monLossProbe) Stage(kind string) *monLossStage {
	for _, s := range p.Stages {
		if s.Kind == kind {
			return s
		}
	}
	return nil
}

// Verdict is the probe as one line: the state of each stage, "-" for a stage
// that was never begun, and the watchdog's part if it had to end the process.
func (p *monLossProbe) Verdict() string {
	parts := make([]string, 0, 4)
	for _, kind := range []string{"connect", "ioctx", "read"} {
		state := "-"
		if s := p.Stage(kind); s != nil {
			state = s.State()
			if s.State() == "error" {
				state += fmt.Sprintf("(%s", s.Exc)
				if s.Errno != nil {
					state += fmt.Sprintf("/%d", *s.Errno)
				}
				state += ")"
			}
		}
		parts = append(parts, kind+"="+state)
	}
	if p.Killed {
		parts = append(parts, fmt.Sprintf("watchdog_killed(reaped=%t)", p.Reaped))
	}
	return strings.Join(parts, " ")
}

// AllOK reports whether every stage returned successfully.
func (p *monLossProbe) AllOK() bool {
	for _, kind := range []string{"connect", "ioctx", "read"} {
		if s := p.Stage(kind); s == nil || s.State() != "ok" {
			return false
		}
	}
	return !p.Crashed
}

// monLossLedger is a validated ledger: the retained client's session and
// calls, the fresh probes, and the tokens.
type monLossLedger struct {
	Tokens   []string
	Instance uint64
	PID      int
	Session  bool
	Calls    []monLossCall
	Probes   map[string]*monLossProbe
	Order    []string
	Stopped  bool
}

// monLossSeedDigest and monLossSeedLen are the independently derived bytes of
// the seeded object; a successful read must return exactly these.
var monLossSeedDigest = outageDigest(outagePayload(monLossSeed, 0))

func monLossSeedLen() int { return len(outagePayload(monLossSeed, 0)) }

func monLossRequire(event monLossEvent, problems ...string) error {
	if len(problems) > 0 {
		return fmt.Errorf("%s record lacks or mistypes %s: %+v", event.Event, strings.Join(problems, ", "), event)
	}
	return nil
}

// parseMonLossLedger validates the raw ledger of one run. A truncated or
// malformed record, an unknown field, a missing or null required field, a
// record of another run, a repeated or reordered call, a return without its
// own open intent, a write whose digest differs from the independently derived
// bytes, a successful seed read with other bytes, and a probe stage that began
// without the previous stage having succeeded are all refused; the caller
// treats a refusal as inconclusive, never as an empty observation.
func parseMonLossLedger(raw []byte, run string) (*monLossLedger, error) {
	ledger := &monLossLedger{Probes: map[string]*monLossProbe{}}
	if len(raw) > 0 && raw[len(raw)-1] != '\n' {
		return nil, errors.New("the ledger ends in a truncated record")
	}
	open := -1 // index of the retained client's open call
	openStage := map[string]*monLossStage{}
	probePID := map[string]int{}
	reads, writes := 0, 0
	if len(raw) == 0 {
		return ledger, nil
	}
	for number, line := range bytes.Split(bytes.TrimSuffix(raw, []byte("\n")), []byte("\n")) {
		var event monLossEvent
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&event); err != nil {
			return nil, fmt.Errorf("record %d is not a valid record: %v: %q", number+1, err, line)
		}
		if _, err := decoder.Token(); err != io.EOF {
			return nil, fmt.Errorf("record %d has trailing data: %q", number+1, line)
		}
		var missing []string
		if event.NS == nil {
			missing = append(missing, "ns")
		}
		if event.PID == nil || *event.PID <= 0 {
			missing = append(missing, "pid")
		}
		if err := monLossRequire(event, missing...); err != nil {
			return nil, err
		}
		if event.Run != run {
			return nil, fmt.Errorf("record of another run %q: %+v", event.Run, event)
		}
		switch event.Event {
		case "token":
			if event.Value == nil || *event.Value == "" {
				return nil, monLossRequire(event, "value")
			}
			ledger.Tokens = append(ledger.Tokens, *event.Value)
		case "session":
			if event.Who != "retained" || event.Instance == nil || *event.Instance == 0 || event.Phase == nil {
				return nil, monLossRequire(event, "who=retained", "instance", "phase")
			}
			if ledger.Session {
				return nil, fmt.Errorf("a second retained session %+v", event)
			}
			ledger.Session, ledger.Instance, ledger.PID = true, *event.Instance, *event.PID
		case "stopped":
			if event.Who != "retained" || event.Reads == nil || event.Writes == nil {
				return nil, monLossRequire(event, "who=retained", "reads", "writes")
			}
			if *event.Reads != reads || *event.Writes != writes || open != -1 || *event.PID != ledger.PID {
				return nil, fmt.Errorf("stopped record disagrees with the ledger (%d reads, %d writes, open call %d): %+v", reads, writes, open, event)
			}
			ledger.Stopped = true
		case "intent", "return":
			if event.Who == "retained" {
				if err := ledger.retainedRecord(event, run, &open, &reads, &writes); err != nil {
					return nil, err
				}
				continue
			}
			if err := ledger.probeRecord(event, openStage, probePID); err != nil {
				return nil, err
			}
		case "probe_start":
			if event.Who == "" || event.Who == "retained" || event.WatchdogS == nil || *event.WatchdogS <= 0 {
				return nil, monLossRequire(event, "who", "watchdog_s")
			}
			if _, repeated := ledger.Probes[event.Who]; repeated {
				return nil, fmt.Errorf("probe %q started twice", event.Who)
			}
			ledger.Probes[event.Who] = &monLossProbe{Name: event.Who, WatchdogS: *event.WatchdogS}
			ledger.Order = append(ledger.Order, event.Who)
		case "probe_crash", "probe_end":
			probe := ledger.Probes[event.Who]
			if probe == nil {
				return nil, fmt.Errorf("%s of a probe that did not start: %+v", event.Event, event)
			}
			if event.Event == "probe_crash" {
				probe.Crashed = true
				continue
			}
			if probe.Ended || event.Killed == nil || event.Reaped == nil || event.Child == nil || (*event.Reaped && event.Exit == nil) {
				return nil, fmt.Errorf("repeated or incomplete probe_end: %+v", event)
			}
			probe.Ended, probe.Killed, probe.Reaped, probe.Exit = true, *event.Killed, *event.Reaped, event.Exit
		default:
			return nil, fmt.Errorf("unknown record %+v", event)
		}
	}
	return ledger, nil
}

func (l *monLossLedger) retainedRecord(event monLossEvent, run string, open, reads, writes *int) error {
	if !l.Session {
		return fmt.Errorf("%s before the retained session: %+v", event.Event, event)
	}
	if *event.PID != l.PID {
		return fmt.Errorf("record from a second retained process %d, want %d: %+v", *event.PID, l.PID, event)
	}
	if event.Event == "intent" {
		if *open != -1 {
			return fmt.Errorf("intent while call %d is open: %+v", *open, event)
		}
		if event.Phase == nil {
			return monLossRequire(event, "phase")
		}
		call := monLossCall{Kind: event.Kind, Op: event.Op, Phase: *event.Phase, IntentNS: *event.NS}
		switch event.Kind {
		case "read":
			if event.Op != fmt.Sprintf("%s-r%05d", run, *reads) {
				return fmt.Errorf("repeated, reordered or misnamed read %+v", event)
			}
			*reads++
		case "write":
			if event.Seq == nil || event.Bytes == nil || event.SHA256 == nil {
				return monLossRequire(event, "seq", "bytes", "sha256")
			}
			if *event.Seq != *writes || event.Op != fmt.Sprintf("%s-w%05d", run, *writes) || event.Object != "mlc-"+event.Op {
				return fmt.Errorf("repeated, reordered or misnamed write %+v", event)
			}
			want := outagePayload(event.Op, *event.Seq)
			if *event.SHA256 != outageDigest(want) || *event.Bytes != len(want) {
				return fmt.Errorf("actor bytes differ from the independent oracle %+v", event)
			}
			call.Object, call.Seq, call.SHA256 = event.Object, *event.Seq, *event.SHA256
			*writes++
		default:
			return fmt.Errorf("unknown call kind %+v", event)
		}
		l.Calls = append(l.Calls, call)
		*open = len(l.Calls) - 1
		return nil
	}
	if *open == -1 || l.Calls[*open].Op != event.Op || l.Calls[*open].Kind != event.Kind || l.Calls[*open].Returned {
		return fmt.Errorf("return without a single matching open intent %+v", event)
	}
	call := &l.Calls[*open]
	if event.OK == nil || event.PhaseAfter == nil || event.MS == nil {
		return monLossRequire(event, "ok", "phase_after", "ms")
	}
	if *event.OK {
		if call.Kind == "read" {
			if event.SHA256 == nil || event.Len == nil {
				return monLossRequire(event, "sha256", "len")
			}
			if *event.SHA256 != monLossSeedDigest || *event.Len != monLossSeedLen() {
				return fmt.Errorf("a successful read returned bytes that are not the seed: %+v", event)
			}
		}
	} else if event.Exc == nil || *event.Exc == "" {
		return monLossRequire(event, "exc")
	}
	call.Returned, call.OK, call.PhaseAfter, call.ReturnNS, call.MS = true, *event.OK, *event.PhaseAfter, *event.NS, *event.MS
	if event.Exc != nil {
		call.Exc = *event.Exc
	}
	call.Errno = event.Errno
	*open = -1
	return nil
}

func (l *monLossLedger) probeRecord(event monLossEvent, openStage map[string]*monLossStage, pids map[string]int) error {
	probe := l.Probes[event.Who]
	if probe == nil || probe.Ended {
		return fmt.Errorf("%s of a probe that is not running: %+v", event.Event, event)
	}
	if pid, seen := pids[event.Who]; seen && pid != *event.PID {
		return fmt.Errorf("records of probe %q come from two processes: %+v", event.Who, event)
	}
	pids[event.Who] = *event.PID
	stages := []string{"connect", "ioctx", "read"}
	if event.Event == "intent" {
		if openStage[event.Who] != nil {
			return fmt.Errorf("probe %q began a stage while another is open: %+v", event.Who, event)
		}
		at := slices.Index(stages, event.Kind)
		if at < 0 || at != len(probe.Stages) || event.Op != event.Who+"-"+event.Kind {
			return fmt.Errorf("probe %q stage out of order or misnamed: %+v", event.Who, event)
		}
		if at > 0 && !probe.Stages[at-1].OK {
			return fmt.Errorf("probe %q began %s although %s did not succeed: %+v", event.Who, event.Kind, stages[at-1], event)
		}
		stage := &monLossStage{Kind: event.Kind}
		probe.Stages = append(probe.Stages, stage)
		openStage[event.Who] = stage
		return nil
	}
	stage := openStage[event.Who]
	if stage == nil || stage.Kind != event.Kind || event.Op != event.Who+"-"+event.Kind {
		return fmt.Errorf("return without a matching open intent %+v", event)
	}
	if event.OK == nil || event.MS == nil {
		return monLossRequire(event, "ok", "ms")
	}
	if *event.OK {
		switch stage.Kind {
		case "connect":
			if event.Instance == nil || *event.Instance == 0 {
				return monLossRequire(event, "instance")
			}
			stage.Instance = *event.Instance
		case "read":
			if event.SHA256 == nil || event.Len == nil {
				return monLossRequire(event, "sha256", "len")
			}
			if *event.SHA256 != monLossSeedDigest || *event.Len != monLossSeedLen() {
				return fmt.Errorf("a successful read returned bytes that are not the seed: %+v", event)
			}
			stage.SHA256, stage.Len = *event.SHA256, *event.Len
		}
	} else if event.Exc == nil || *event.Exc == "" {
		return monLossRequire(event, "exc")
	}
	stage.Returned, stage.OK, stage.MS, stage.Errno = true, *event.OK, *event.MS, event.Errno
	if event.Exc != nil {
		stage.Exc = *event.Exc
	}
	openStage[event.Who] = nil
	return nil
}

// Tally counts the retained client's calls by Key.
func (l *monLossLedger) Tally() map[string]int {
	counts := map[string]int{}
	for _, call := range l.Calls {
		counts[call.Key()]++
	}
	return counts
}

func monLossTallyString(counts map[string]int) string {
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, counts[key]))
	}
	return strings.Join(parts, " ")
}

// CallsUnder returns the calls of a kind that began under a token.
func (l *monLossLedger) CallsUnder(kind, phase string) []monLossCall {
	var result []monLossCall
	for _, call := range l.Calls {
		if call.Kind == kind && call.Phase == phase {
			result = append(result, call)
		}
	}
	return result
}

// StayedAcked counts the acknowledged calls of a kind that began and returned
// under one token.
func (l *monLossLedger) StayedAcked(kind, phase string) int {
	n := 0
	for _, call := range l.CallsUnder(kind, phase) {
		if call.Outcome() == "acked" && call.PhaseAfter == phase {
			n++
		}
	}
	return n
}

// Writes converts the retained client's writes to the outage ledger's
// operations, so that the shared readback oracle judges them: an acknowledged
// write must be present and exact, a failed or in-flight one may be absent or
// exact and is never counted as a success, and other bytes are corruption.
func (l *monLossLedger) Writes() []outageOp {
	var ops []outageOp
	for _, call := range l.Calls {
		if call.Kind != "write" {
			continue
		}
		op := outageOp{Op: call.Op, Object: call.Object, SHA256: call.SHA256, Seq: call.Seq, Phase: call.Phase,
			PhaseAfter: call.PhaseAfter, Returned: call.Returned, IntentNS: call.IntentNS, ReturnNS: call.ReturnNS}
		if call.Returned && !call.OK {
			op.Error = call.Detail()
		}
		ops = append(ops, op)
	}
	return ops
}

// monLossReadback is the fresh reader's answer: each named object and the
// pool's whole object listing.
type monLossReadback struct {
	Objects map[string]outageObject `json:"objects"`
	Listing []string                `json:"listing"`
}

// monLossVerifyReadback applies the shared byte-for-byte oracle to the writes
// and the seed, and refuses any object in the pool that no intent names, which
// would be a write the ledger does not know (a replay or a stray submission).
func monLossVerifyReadback(l *monLossLedger, rb monLossReadback) (unconfirmedPresent int, err error) {
	seed, ok := rb.Objects[monLossSeed]
	if !ok || !seed.Present || seed.Size != monLossSeedLen() || seed.DataLen != monLossSeedLen() || seed.SHA256 != monLossSeedDigest {
		return 0, fmt.Errorf("the seeded object is not exact after recovery: %+v", seed)
	}
	ops := l.Writes()
	if unconfirmedPresent, err = outageVerify(ops, rb.Objects); err != nil {
		return 0, err
	}
	known := map[string]bool{monLossSeed: true}
	for _, op := range ops {
		known[op.Object] = true
	}
	for _, name := range rb.Listing {
		if !known[name] {
			return 0, fmt.Errorf("the pool holds %q, which no intent of the ledger names", name)
		}
	}
	return unconfirmedPresent, nil
}

func monLossCommand(pool, run, mode string, timeout time.Duration, extra ...string) ceph.NativeCommand {
	return ceph.NativeCommand{Argv: append([]string{"python3", "-c", monLossClientScript, mode, pool, monLossLedgerDir, run}, extra...),
		Timeout: timeout, MaxStdout: 8 << 20, MaxStderr: 1 << 20, Label: "mon loss client " + mode}
}

// monLossRunner runs the script in the client container. It returns errors and
// never fails a test, so that cleanup can use it.
type monLossRunner struct {
	cluster *ceph.Container
	client  testcontainers.Container
	pool    string
	run     string
}

func (r monLossRunner) do(ctx context.Context, mode string, timeout time.Duration, extra ...string) ([]byte, error) {
	result, err := r.cluster.RunNativeCommand(ctx, r.client, monLossCommand(r.pool, r.run, mode, timeout, extra...))
	if err != nil || !result.Complete() || result.ExitCode != 0 {
		return result.Stdout, fmt.Errorf("mon loss client %s error=%v exit=%d complete=%t stdout=%q stderr=%q", mode, err, result.ExitCode, result.Complete(), result.Stdout, result.Stderr)
	}
	return result.Stdout, nil
}

func (r monLossRunner) token(ctx context.Context, value string) error {
	_, err := r.do(ctx, "token", time.Minute, value)
	return err
}

func (r monLossRunner) ledger(ctx context.Context) (*monLossLedger, []byte, error) {
	// A record is appended by one write, so a reader is not expected to see
	// half of one; a read that does is taken again before it is refused.
	var raw []byte
	for attempt := 0; ; attempt++ {
		var err error
		if raw, err = r.do(ctx, "ledger", time.Minute); err != nil {
			return nil, raw, err
		}
		if len(raw) == 0 || raw[len(raw)-1] == '\n' || attempt == 4 {
			break
		}
		select {
		case <-ctx.Done():
			return nil, raw, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	ledger, err := parseMonLossLedger(raw, r.run)
	return ledger, raw, err
}

// fresh runs one fresh probe. The launcher kills and reaps the probe process
// at the watchdog, and the command's own timeout lies beyond it, so a probe
// that does not return in time still leaves its partial records in the ledger.
func (r monLossRunner) fresh(ctx context.Context, name string, watchdog time.Duration) error {
	_, err := r.do(ctx, "fresh", watchdog+45*time.Second, name, fmt.Sprintf("%d", int(watchdog.Seconds())))
	return err
}

func (r monLossRunner) readback(ctx context.Context, names []string) (monLossReadback, error) {
	var rb monLossReadback
	list, _ := json.Marshal(append([]string{monLossSeed}, names...))
	out, err := r.do(ctx, "read", 3*time.Minute, string(list))
	if err != nil {
		return rb, err
	}
	if err := json.Unmarshal(out, &rb); err != nil || rb.Objects == nil || rb.Listing == nil {
		return rb, fmt.Errorf("readback JSON is incomplete: %v: %s", err, out)
	}
	return rb, nil
}

// monLossPoll reads the ledger until check accepts it, all within bound. The
// read, its retries and the delay between polls share one context derived from
// the bound, and a snapshot that becomes ready only after that context expired
// is not accepted: it is returned, with ok false, as the last diagnostic view.
// A read error is kept as the last error and retried until the bound ends.
func monLossPoll(parent context.Context, bound, interval time.Duration, read func(context.Context) (*monLossLedger, error), check func(*monLossLedger) bool) (last *monLossLedger, ok bool, err error) {
	ctx, cancel := context.WithTimeout(parent, bound)
	defer cancel()
	for {
		ledger, readErr := read(ctx)
		if readErr == nil {
			last, err = ledger, nil
			if ctx.Err() == nil && check(ledger) {
				return last, true, nil
			}
		} else {
			err = readErr
		}
		select {
		case <-ctx.Done():
			return last, false, err
		case <-time.After(interval):
		}
	}
}
