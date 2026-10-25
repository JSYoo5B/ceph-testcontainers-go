//go:build all || integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// Phase tokens are issued by the controller. The actor reads the current token
// before it submits an operation and again after the operation returned, so
// the ledger joins the client's operations to the controller's observations
// without comparing clocks of different processes.
const (
	outagePhaseBaseline  = "baseline"  // before any stop was requested
	outagePhaseApplying  = "applying"  // issued before the first stop request
	outagePhaseOutage    = "outage"    // issued after every selected stop was observed
	outagePhaseRestoring = "restoring" // issued before the first restore request
	outagePhaseRecovered = "recovered" // issued after the cluster is clean again
	outagePhaseStop      = "stop"      // the actor ends after its current operation
)

const outageLedgerDir = "/tmp/tc-correlated-outage"

// outageLedgerScript is the retained client. One Rados instance and one ioctx
// live for the whole run. Each operation is a synchronous write_full of a
// unique object, submitted exactly once and never replayed. The ledger gets an
// "intent" record before the call and a "return" record after it, both
// flushed to disk. An operation that has an intent and no return was in flight
// when the actor ended; it is not promoted to a success or a failure.
//
// Modes: "actor" runs the retained client, "token" atomically replaces the
// phase token and records it, "ledger" prints the records, and "read" reads
// the named objects with a fresh connection.
const outageLedgerScript = `import hashlib,json,os,sys,time
mode,pool,path,run=sys.argv[1:5]
ledger=os.path.join(path,'ledger.jsonl'); token=os.path.join(path,'phase')
def emit(event,**kw):
    fd=os.open(ledger,os.O_WRONLY|os.O_APPEND|os.O_CREAT,0o600)
    try:
        os.write(fd,(json.dumps(dict(event=event,ns=time.monotonic_ns(),pid=os.getpid(),run=run,**kw),sort_keys=True)+'\n').encode()); os.fsync(fd)
    finally: os.close(fd)
def phase():
    try: return open(token).read().strip()
    except FileNotFoundError: return ''
def payload(op,seq): return ('op=%s;seq=%d;'%(op,seq)).encode()+bytes((i*7+seq)%251 for i in range(4096+(seq%5)*1024))
if mode=='token':
    os.makedirs(path,exist_ok=True)
    tmp=token+'.tmp'
    with open(tmp,'w') as f: f.write(sys.argv[5]); f.flush(); os.fsync(f.fileno())
    os.replace(tmp,token); emit('token',value=sys.argv[5]); sys.exit(0)
if mode=='ledger':
    print(json.dumps([json.loads(line) for line in open(ledger)] if os.path.exists(ledger) else [])); sys.exit(0)
import rados
def connect():
    c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_osd_op_timeout':'60','rados_mon_op_timeout':'30'})
    c.connect(); return c
c=connect()
try:
    with c.open_ioctx(pool) as io:
        if mode=='read':
            out={}
            for name in json.loads(sys.argv[5]):
                try:
                    size,_=io.stat(name); data=io.read(name,size+1,0)
                    out[name]=dict(present=True,size=size,data_len=len(data),sha256=hashlib.sha256(data).hexdigest())
                except rados.ObjectNotFound: out[name]=dict(present=False)
            print(json.dumps(out))
        elif mode=='actor':
            emit('session',client='client.%d'%c.get_instance_id(),phase=phase())
            deadline=time.monotonic()+900; seq=0
            while seq<1500 and time.monotonic()<deadline:
                before=phase()
                if before=='stop': break
                op='%s-%05d'%(run,seq); data=payload(op,seq); name='op-'+op
                emit('intent',op=op,seq=seq,object=name,bytes=len(data),sha256=hashlib.sha256(data).hexdigest(),phase=before)
                try: io.write_full(name,data); error=''
                except Exception as e: error=repr(e)
                emit('return',op=op,error=error,phase_after=phase())
                seq+=1; time.sleep(.25)
            emit('stopped',ops=seq)
        else: raise ValueError('unknown mode')
finally: c.shutdown()
`

// outageEvent is one ledger record: "token" (the controller issued Value),
// "session" (the actor connected as Client), "intent", "return" and "stopped".
type outageEvent struct {
	Event      string `json:"event"`
	NS         int64  `json:"ns"`
	PID        int    `json:"pid"`
	Run        string `json:"run"`
	Client     string `json:"client"`
	Op         string `json:"op"`
	Object     string `json:"object"`
	SHA256     string `json:"sha256"`
	Phase      string `json:"phase"`
	PhaseAfter string `json:"phase_after"`
	Error      string `json:"error"`
	Value      string `json:"value"`
	Seq        int    `json:"seq"`
	Bytes      int    `json:"bytes"`
	Ops        int    `json:"ops"`
}

// outagePayload is the Go-side oracle of the bytes written by operation seq.
// It is derived here and compared with the digest the actor recorded; it is
// never taken from observed output.
func outagePayload(op string, seq int) []byte {
	data := []byte(fmt.Sprintf("op=%s;seq=%d;", op, seq))
	for i := range 4096 + (seq%5)*1024 {
		data = append(data, byte((i*7+seq)%251))
	}
	return data
}

func outageDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// outageOp is one operation of the retained client, assembled from its records.
type outageOp struct {
	Op, Object, SHA256 string
	Seq                int
	Phase, PhaseAfter  string
	Returned           bool
	Error              string
	IntentNS, ReturnNS int64
}

// Outcome is the client's own view: "acked" (the call returned without error),
// "failed" (it raised) or "inflight" (no return record). A later read never
// turns a failed or in-flight operation into an acknowledgement.
func (op outageOp) Outcome() string {
	switch {
	case !op.Returned:
		return "inflight"
	case op.Error != "":
		return "failed"
	}
	return "acked"
}

// Credit says what an operation may be counted for. Only an acknowledged
// operation that began and returned under the same "outage" token, which the
// controller issued after every selected stop was observed, is credited as a
// fully applied outage write. An operation that began under "applying" or
// returned under another token overlaps a partial or changing injection.
func (op outageOp) Credit() string {
	outcome := op.Outcome()
	if op.Phase == outagePhaseOutage && op.Returned && op.PhaseAfter != outagePhaseOutage {
		return outagePhaseOutage + "-overlapping-" + outcome
	}
	return op.Phase + "/" + outcome
}

// outageOps validates the ledger of one run and returns its operations in
// submission order. It refuses a record of another run, a second actor process
// or session, a repeated or reordered operation, a return without an intent,
// and an intent whose digest differs from the independently derived bytes.
func outageOps(events []outageEvent, run string) ([]outageOp, error) {
	var ops []outageOp
	index := map[string]int{}
	actorPID, sessions := 0, 0
	for _, event := range events {
		if event.Run != run {
			return nil, fmt.Errorf("record of another run %q: %+v", event.Run, event)
		}
		if event.Event == "token" {
			continue
		}
		if actorPID == 0 {
			actorPID = event.PID
		} else if event.PID != actorPID {
			return nil, fmt.Errorf("record from a second actor process %d, want %d: %+v", event.PID, actorPID, event)
		}
		switch event.Event {
		case "session":
			if sessions++; sessions > 1 || event.Client == "" {
				return nil, fmt.Errorf("invalid or repeated session %+v", event)
			}
		case "intent":
			if sessions == 0 {
				return nil, fmt.Errorf("intent before the session %+v", event)
			}
			if _, repeated := index[event.Op]; repeated || event.Seq != len(ops) || event.Op != fmt.Sprintf("%s-%05d", run, event.Seq) || event.Object != "op-"+event.Op {
				return nil, fmt.Errorf("repeated, reordered or misnamed operation %+v", event)
			}
			if want := outageDigest(outagePayload(event.Op, event.Seq)); event.SHA256 != want || event.Bytes != len(outagePayload(event.Op, event.Seq)) {
				return nil, fmt.Errorf("actor bytes differ from the independent oracle %+v", event)
			}
			index[event.Op] = len(ops)
			ops = append(ops, outageOp{Op: event.Op, Object: event.Object, Seq: event.Seq, SHA256: event.SHA256, Phase: event.Phase, IntentNS: event.NS})
		case "return":
			i, ok := index[event.Op]
			if !ok || ops[i].Returned || i != len(ops)-1 {
				return nil, fmt.Errorf("return without a single matching open intent %+v", event)
			}
			ops[i].Returned, ops[i].Error, ops[i].PhaseAfter, ops[i].ReturnNS = true, event.Error, event.PhaseAfter, event.NS
		case "stopped":
			if event.Ops != len(ops) {
				return nil, fmt.Errorf("actor reports %d operations, ledger has %d", event.Ops, len(ops))
			}
		default:
			return nil, fmt.Errorf("unknown record %+v", event)
		}
	}
	return ops, nil
}

// outageTally counts operations by Credit.
func outageTally(ops []outageOp) map[string]int {
	counts := map[string]int{}
	for _, op := range ops {
		counts[op.Credit()]++
	}
	return counts
}

// outageObject is one object as a fresh reader saw it.
type outageObject struct {
	Present bool   `json:"present"`
	Size    int    `json:"size"`
	DataLen int    `json:"data_len"`
	SHA256  string `json:"sha256"`
}

// outageVerify compares an independent readback with the independently derived
// bytes. An acknowledged operation must be present and exact. A failed or
// in-flight operation may be absent or exact, and is never counted as a
// success; any other bytes are corruption either way. The number of
// unconfirmed operations that nevertheless took effect is returned.
func outageVerify(ops []outageOp, objects map[string]outageObject) (unconfirmedPresent int, err error) {
	for _, op := range ops {
		got, seen := objects[op.Object]
		if !seen {
			return 0, fmt.Errorf("readback has no answer for %s", op.Object)
		}
		want := outagePayload(op.Op, op.Seq)
		exact := got.Present && got.Size == len(want) && got.DataLen == len(want) && got.SHA256 == outageDigest(want)
		switch {
		case got.Present && !exact:
			return 0, fmt.Errorf("%s (%s) has bytes that are not its own: %+v", op.Object, op.Outcome(), got)
		case op.Outcome() == "acked" && !exact:
			return 0, fmt.Errorf("acknowledged %s is absent after recovery", op.Object)
		case op.Outcome() != "acked" && exact:
			unconfirmedPresent++
		}
	}
	return unconfirmedPresent, nil
}

// outageDaemon is the part of a container the plan needs.
type outageDaemon interface {
	Stop(ctx context.Context, timeout *time.Duration) error
	Start(ctx context.Context) error
	Running(ctx context.Context) (bool, error)
}

type outageContainer struct{ testcontainers.Container }

func (c outageContainer) Running(ctx context.Context) (bool, error) {
	state, err := c.State(ctx)
	if err != nil || state == nil {
		return false, fmt.Errorf("container state unavailable: %v", err)
	}
	return state.Running, nil
}

// outageMember is one frozen member of the outage manifest: a daemon with its
// native identity, its logical host and the container generation recorded
// before anything was applied. The host is a label of this simulation and
// does not assert a physical placement.
type outageMember struct {
	Role      string `json:"role"`
	Name      string `json:"name"`
	Host      string `json:"host"`
	Container string `json:"container"`
	daemon    outageDaemon
	// current reports the container that now holds this identity.
	current func() string
}

// outageManifest freezes the membership of every logical host. It is a map of
// service roles, apart from the CRUSH placement of the OSDs.
type outageManifest struct {
	Hosts   []string       `json:"hosts"`
	Members []outageMember `json:"members"`
}

// newOutageManifest refuses a member that is unnamed, outside the hosts, from
// a container the cluster does not own, or repeated by identity or container.
func newOutageManifest(hosts []string, members []outageMember, owned map[string]bool) (outageManifest, error) {
	seenName, seenContainer := map[string]bool{}, map[string]bool{}
	for _, m := range members {
		identity := m.Role + "." + m.Name
		switch {
		case m.Role != "mon" && m.Role != "mgr" && m.Role != "osd", m.Name == "", m.Container == "", m.daemon == nil || m.current == nil:
			return outageManifest{}, fmt.Errorf("incomplete member %q", identity)
		case !slices.Contains(hosts, m.Host):
			return outageManifest{}, fmt.Errorf("member %s names host %q outside the manifest", identity, m.Host)
		case !owned[m.Container]:
			return outageManifest{}, fmt.Errorf("member %s is a foreign container", identity)
		case seenName[identity]:
			return outageManifest{}, fmt.Errorf("duplicate member %s", identity)
		case seenContainer[m.Container]:
			return outageManifest{}, fmt.Errorf("member %s repeats a container of another member", identity)
		}
		seenName[identity], seenContainer[m.Container] = true, true
	}
	return outageManifest{Hosts: slices.Clone(hosts), Members: slices.Clone(members)}, nil
}

// onHost returns the members of one logical host in manifest order.
func (m outageManifest) onHost(host string) []outageMember {
	var result []outageMember
	for _, member := range m.Members {
		if member.Host == host {
			result = append(result, member)
		}
	}
	return result
}

// outageReceipt is the per-target record of an injection. Times are offsets on
// the controller's one clock; -1 means the step did not happen. Observed is
// the time a native observation first showed the effect.
type outageReceipt struct {
	Role, Name, Host, Container string
	RequestedMS, AppliedMS      int64
	ObservedMS                  int64
	RestoreRequestedMS          int64
	RestoredMS                  int64
	Attempted, Applied          bool
	Restored                    bool
	StopError, RestoreError     string
}

// outagePlan is a finite, explicit, sequential list of stops. It is correlated
// and not atomic: a stop that fails leaves the earlier ones applied, and
// Restore still returns every attempted target, including the one whose stop
// result is uncertain.
type outagePlan struct {
	now      func() time.Duration
	members  []outageMember
	Receipts []*outageReceipt
	// Attempts lists every Restore call that had something to start.
	Attempts []outageRestoreAttempt
}

// outageRestoreAttempt records one Restore call. Kind tells the primary
// restoration of the scenario apart from a retry made by the cleanup.
type outageRestoreAttempt struct {
	Kind    string `json:"kind"`
	Targets int    `json:"targets"`
	Error   string `json:"error"`
}

func newOutagePlan(now func() time.Duration, members []outageMember) *outagePlan {
	plan := &outagePlan{now: now, members: members}
	for _, m := range members {
		plan.Receipts = append(plan.Receipts, &outageReceipt{Role: m.Role, Name: m.Name, Host: m.Host, Container: m.Container,
			RequestedMS: -1, AppliedMS: -1, ObservedMS: -1, RestoreRequestedMS: -1, RestoredMS: -1})
	}
	return plan
}

func (p *outagePlan) ms() int64 { return p.now().Milliseconds() }

// Apply stops the members in order and returns at the first failure.
func (p *outagePlan) Apply(ctx context.Context, grace time.Duration) error {
	for i, m := range p.members {
		r := p.Receipts[i]
		if got := m.current(); got != m.Container {
			r.StopError = fmt.Sprintf("container generation changed from %s to %s", m.Container, got)
			return fmt.Errorf("%s.%s not stopped (%d/%d applied): %s", m.Role, m.Name, i, len(p.members), r.StopError)
		}
		r.Attempted, r.RequestedMS = true, p.ms()
		err := m.daemon.Stop(ctx, &grace)
		if err == nil {
			var running bool
			if running, err = m.daemon.Running(ctx); err == nil && running {
				err = errors.New("stop returned but the container is still running")
			}
		}
		if err != nil {
			r.StopError = err.Error()
			return fmt.Errorf("%s.%s stop failed (%d/%d applied): %w", m.Role, m.Name, i, len(p.members), err)
		}
		r.Applied, r.AppliedMS = true, p.ms()
	}
	return nil
}

// Restore starts every attempted, unrestored target in reverse order. A failure
// is recorded and the remaining targets are still attempted. The returned error
// joins every failure and keeps each original error.
func (p *outagePlan) Restore(ctx context.Context, kind string) error {
	var failures []error
	pending := 0
	for _, r := range p.Receipts {
		if r.Attempted && !r.Restored {
			pending++
		}
	}
	defer func() {
		if pending > 0 {
			attempt := outageRestoreAttempt{Kind: kind, Targets: pending}
			if len(failures) > 0 {
				attempt.Error = errors.Join(failures...).Error()
			}
			p.Attempts = append(p.Attempts, attempt)
		}
	}()
	for i := len(p.members) - 1; i >= 0; i-- {
		m, r := p.members[i], p.Receipts[i]
		if !r.Attempted || r.Restored {
			continue
		}
		r.RestoreRequestedMS = p.ms()
		err := m.daemon.Start(ctx)
		if err == nil {
			var running bool
			if running, err = m.daemon.Running(ctx); err == nil && !running {
				err = errors.New("start returned but the container is not running")
			}
		}
		if err != nil {
			r.RestoreError = err.Error()
			failures = append(failures, fmt.Errorf("restore %s.%s: %w", m.Role, m.Name, err))
			continue
		}
		r.Restored, r.RestoredMS, r.RestoreError = true, p.ms(), ""
	}
	return errors.Join(failures...)
}

// AppliedInterval is the span from the first stop request to the last
// completed stop. It is false until at least two stops were applied, and it
// shows that the outage was correlated rather than simultaneous.
func (p *outagePlan) AppliedInterval() (time.Duration, bool) {
	first, last, applied := int64(-1), int64(-1), 0
	for _, r := range p.Receipts {
		if r.RequestedMS >= 0 && (first < 0 || r.RequestedMS < first) {
			first = r.RequestedMS
		}
		if r.Applied {
			applied++
			last = max(last, r.AppliedMS)
		}
	}
	if applied < 2 {
		return 0, false
	}
	return time.Duration(last-first) * time.Millisecond, true
}

// ReceiptJSON is the bounded record of an injection: every target with its
// attempted, applied, observed and restored state and times, and every restore
// attempt with its own error, so a failed primary restore stays visible beside
// any later cleanup retry.
func (p *outagePlan) ReceiptJSON() string {
	interval := int64(-1)
	if d, ok := p.AppliedInterval(); ok {
		interval = d.Milliseconds()
	}
	data, _ := json.Marshal(struct {
		AppliedIntervalMS int64                  `json:"applied_interval_ms"`
		Receipts          []*outageReceipt       `json:"receipts"`
		RestoreAttempts   []outageRestoreAttempt `json:"restore_attempts"`
	}{interval, p.Receipts, p.Attempts})
	return string(data)
}

// outageFinish runs at cleanup, before the cluster is destroyed. It retries
// the restoration of every attempted target that is still stopped, reports a
// failure of that retry, and always emits the receipt, whatever the outcome of
// the scenario was.
func outageFinish(ctx context.Context, plan *outagePlan, logf func(string, ...any), errorf func(string, ...any)) {
	if err := plan.Restore(ctx, "cleanup retry"); err != nil {
		errorf("cleanup retry of the restoration failed: %v", err)
	}
	logf("CORRELATED_OUTAGE_RECEIPTS %s", plan.ReceiptJSON())
}

// Observe records the first native observation of a target's stop.
func (p *outagePlan) Observe(role, name string) {
	for _, r := range p.Receipts {
		if r.Role == role && r.Name == name && r.ObservedMS < 0 {
			r.ObservedMS = p.ms()
		}
	}
}

// outageToken issues a phase token through the same client container as the
// actor. The command returns after the token file was replaced and recorded.
func outageToken(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, pool, run, value string) {
	t.Helper()
	outageRun(t, ctx, cluster, client, pool, run, "token", value)
}

func outageRun(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, pool, run, mode string, extra ...string) []byte {
	t.Helper()
	result, err := cluster.RunNativeCommand(ctx, client, outageCommand(pool, run, mode, extra...))
	if err != nil || !result.Complete() || result.ExitCode != 0 {
		t.Fatalf("outage %s error=%v exit=%d complete=%t stdout=%s stderr=%s", mode, err, result.ExitCode, result.Complete(), result.Stdout, result.Stderr)
	}
	return result.Stdout
}

func outageCommand(pool, run, mode string, extra ...string) ceph.NativeCommand {
	timeout := 3 * time.Minute
	if mode == "actor" {
		timeout = 20 * time.Minute
	}
	return ceph.NativeCommand{Argv: append([]string{"python3", "-c", outageLedgerScript, mode, pool, outageLedgerDir, run}, extra...),
		Timeout: timeout, MaxStdout: 8 << 20, MaxStderr: 1 << 20, Label: "correlated outage " + mode}
}

func outageLedger(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, pool, run string) []outageEvent {
	t.Helper()
	var events []outageEvent
	data := outageRun(t, ctx, cluster, client, pool, run, "ledger")
	if err := json.Unmarshal(data, &events); err != nil {
		t.Fatalf("outage ledger JSON: %v", err)
	}
	return events
}

// outageNames lists the object names of ops, for the readback.
func outageNames(ops []outageOp) string {
	names := make([]string, 0, len(ops))
	for _, op := range ops {
		names = append(names, op.Object)
	}
	data, _ := json.Marshal(names)
	return string(data)
}

func outageShort(ops []outageOp) string {
	var b strings.Builder
	for _, op := range ops {
		fmt.Fprintf(&b, "%d:%s>%s:%s ", op.Seq, op.Phase, op.PhaseAfter, op.Outcome())
	}
	return strings.TrimSpace(b.String())
}
