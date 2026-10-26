//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/testcontainers/testcontainers-go"
)

// secondFaultActor is one live client for the whole scenario. It writes uniquely
// named objects one at a time through one librados connection and records every
// operation in an append-only ledger in the client container, outside every OSD:
// an "intent" before the call, then exactly one "ack" or "error" when the call
// returns. An operation that never returns keeps its intent and nothing else.
// The actor never retries or replays an operation, and an "error" says only that
// the call raised: the write may or may not have been applied.
//
// Phases: "actor" (pool, ledger, stop file, seconds between writes, bound),
// "ledger" (print the ledger as JSON) and "read" (pool, JSON list of object
// names: report each object as a fresh connection sees it).
const secondFaultActor = `import hashlib,json,os,sys,time,rados
phase=sys.argv[1]
def payload(seq): return ('live=%d;'%seq).encode()+bytes((i*7+seq)%251 for i in range(4096+seq*13))
def emit(path,event,**kw):
    with open(path,'a') as f:
        f.write(json.dumps(dict(event=event,ns=time.monotonic_ns(),**kw))+'\n'); f.flush(); os.fsync(f.fileno())
if phase=='ledger':
    path=sys.argv[2]
    print(json.dumps([json.loads(line) for line in open(path)] if os.path.exists(path) else [])); sys.exit(0)
pool=sys.argv[2]
c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'rados_osd_op_timeout':'120','rados_mon_op_timeout':'30'})
c.connect()
try:
    with c.open_ioctx(pool) as io:
        if phase=='actor':
            path,stop,interval,bound=sys.argv[3],sys.argv[4],float(sys.argv[5]),int(sys.argv[6])
            client='client.%d'%c.get_instance_id()
            emit(path,'start',client=client)
            seq=0
            while seq<bound and not os.path.exists(stop):
                name='live-%04d'%seq; data=payload(seq)
                emit(path,'intent',object=name,seq=seq,bytes=len(data),sha256=hashlib.sha256(data).hexdigest(),client=client)
                try:
                    io.write_full(name,data)
                    emit(path,'ack',object=name,ret=0,version=io.get_last_version())
                except Exception as e:
                    emit(path,'error',object=name,errno=getattr(e,'errno',None) or 0,error=repr(e))
                seq+=1
                time.sleep(interval)
            emit(path,'stopped',seq=seq,stop_file=os.path.exists(stop))
        elif phase=='read':
            out={}
            for name in json.loads(sys.argv[3]):
                try:
                    size,mtime=io.stat(name); data=io.read(name,size+1,0)
                    out[name]=dict(present=True,size=size,sha256=hashlib.sha256(data).hexdigest(),data_len=len(data))
                except rados.ObjectNotFound: out[name]=dict(present=False)
            print(json.dumps(out))
        else: raise ValueError('unknown phase')
finally: c.shutdown()
`

// secondFaultEvent is one ledger record: "start", "intent", "ack", "error" or
// "stopped".
type secondFaultEvent struct {
	Event   string `json:"event"`
	NS      int64  `json:"ns"`
	Object  string `json:"object"`
	Client  string `json:"client"`
	Seq     int    `json:"seq"`
	Bytes   int    `json:"bytes"`
	SHA256  string `json:"sha256"`
	Ret     int    `json:"ret"`
	Version uint64 `json:"version"`
	Errno   int    `json:"errno"`
	Error   string `json:"error"`
}

// Outcomes of one submitted operation.
const (
	opAcked       = "acked"       // the call returned success
	opRefused     = "refused"     // the call raised an error that says the OSD rejected it before applying it
	opUnknown     = "unknown"     // the call raised any other error: the write may have been applied
	opOutstanding = "outstanding" // the call has not returned
)

// secondFaultOp is the ledger's account of one operation.
type secondFaultOp struct {
	Name    string `json:"name"`
	Seq     int    `json:"seq"`
	Bytes   int    `json:"bytes"`
	SHA256  string `json:"sha256"`
	Outcome string `json:"outcome"`
	Errno   int    `json:"errno,omitempty"`
}

// refusedErrnos are the errors an OSD answers with before it applies a write:
// permission, a read-only or full condition. Any other error, including a
// timeout, leaves the write's effect unknown.
var refusedErrnos = []int{1, 13, 28, 30, 122}

// secondFaultOps reads a ledger in order. Every operation needs one intent
// before at most one terminal record, and no object is submitted twice: a
// repeated intent would be a replay, and it is refused. With final set the
// writer must have recorded "stopped", and an operation without a terminal
// record is reported unknown instead of outstanding.
func secondFaultOps(events []secondFaultEvent, final bool) ([]secondFaultOp, error) {
	var ops []secondFaultOp
	index := map[string]int{}
	stopped := false
	for _, event := range events {
		if stopped {
			return nil, fmt.Errorf("record %+v after the writer stopped", event)
		}
		switch event.Event {
		case "start":
			if len(ops) != 0 {
				return nil, fmt.Errorf("the writer started again: %+v", event)
			}
		case "stopped":
			stopped = true
		case "intent":
			if _, seen := index[event.Object]; seen || event.Object == "" || event.SHA256 == "" || event.Bytes <= 0 {
				return nil, fmt.Errorf("invalid or repeated submission %+v", event)
			}
			if event.Seq != len(ops) {
				return nil, fmt.Errorf("submission %q has sequence %d, want %d", event.Object, event.Seq, len(ops))
			}
			if len(ops) > 0 && ops[len(ops)-1].Outcome == opOutstanding {
				return nil, fmt.Errorf("submission %q began while %q had not returned", event.Object, ops[len(ops)-1].Name)
			}
			index[event.Object] = len(ops)
			ops = append(ops, secondFaultOp{Name: event.Object, Seq: event.Seq, Bytes: event.Bytes, SHA256: event.SHA256, Outcome: opOutstanding})
		case "ack", "error":
			i, ok := index[event.Object]
			if !ok || ops[i].Outcome != opOutstanding {
				return nil, fmt.Errorf("terminal record without an open submission %+v", event)
			}
			switch {
			case event.Event == "ack" && event.Ret == 0:
				ops[i].Outcome = opAcked
			case event.Event == "ack":
				return nil, fmt.Errorf("acknowledgment with return code %d %+v", event.Ret, event)
			case slices.Contains(refusedErrnos, event.Errno):
				ops[i].Outcome, ops[i].Errno = opRefused, event.Errno
			default:
				ops[i].Outcome, ops[i].Errno = opUnknown, event.Errno
			}
		default:
			return nil, fmt.Errorf("unknown record %+v", event)
		}
	}
	if final {
		if !stopped {
			return nil, errors.New("the writer did not record that it stopped")
		}
		for i := range ops {
			if ops[i].Outcome == opOutstanding {
				ops[i].Outcome = opUnknown
			}
		}
	}
	return ops, nil
}

// secondFaultMark counts operations by outcome at one moment: the watermark that
// joins the writer to the recovery phases.
type secondFaultMark struct {
	Submitted   int `json:"submitted"`
	Acked       int `json:"acked"`
	Refused     int `json:"refused"`
	Unknown     int `json:"unknown"`
	Outstanding int `json:"outstanding"`
}

func secondFaultCount(ops []secondFaultOp) secondFaultMark {
	mark := secondFaultMark{Submitted: len(ops)}
	for _, op := range ops {
		switch op.Outcome {
		case opAcked:
			mark.Acked++
		case opRefused:
			mark.Refused++
		case opUnknown:
			mark.Unknown++
		case opOutstanding:
			mark.Outstanding++
		}
	}
	return mark
}

// secondFaultObject is an object as a fresh connection reads it back.
type secondFaultObject struct {
	Present bool   `json:"present"`
	Size    int    `json:"size"`
	SHA256  string `json:"sha256"`
	DataLen int    `json:"data_len"`
}

// secondFaultVerify closes the data oracle for the writer's operations. An
// acknowledged write must read back with exactly its submitted bytes. An
// operation whose effect is unknown may be absent or whole, never torn: it is
// inspected, not replayed. A refused write, and a submission that was never
// made, must not be present at all.
func secondFaultVerify(ops []secondFaultOp, read map[string]secondFaultObject) error {
	for _, op := range ops {
		got, listed := read[op.Name]
		whole := got.Present && got.Size == op.Bytes && got.DataLen == op.Bytes && got.SHA256 == op.SHA256
		switch {
		case !listed:
			return fmt.Errorf("%s was not read back", op.Name)
		case op.Outcome == opAcked && !whole:
			return fmt.Errorf("acknowledged %s reads back as %+v, submitted %d bytes %s", op.Name, got, op.Bytes, op.SHA256)
		case op.Outcome == opUnknown && got.Present && !whole:
			return fmt.Errorf("%s with an unknown outcome reads back torn: %+v", op.Name, got)
		case op.Outcome == opRefused && got.Present:
			return fmt.Errorf("refused %s is present: %+v", op.Name, got)
		case op.Outcome == opOutstanding:
			return fmt.Errorf("%s is still outstanding at readback", op.Name)
		}
	}
	return nil
}

func secondFaultCommand(label string, timeout time.Duration, args ...string) ceph.NativeCommand {
	return ceph.NativeCommand{
		Argv: append([]string{"python3", "-c", secondFaultActor}, args...), Timeout: timeout,
		MaxStdout: 1 << 20, MaxStderr: 1 << 20, Label: "second fault " + label,
	}
}

func secondFaultLedger(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, path string) []secondFaultEvent {
	t.Helper()
	result, err := cluster.RunNativeCommand(ctx, client, secondFaultCommand("ledger", 3*time.Minute, "ledger", path))
	if err != nil || result.ExitCode != 0 || !result.Complete() {
		t.Fatalf("read the writer ledger: exit %d complete %t: %v: %.1024s", result.ExitCode, result.Complete(), err, result.Stderr)
	}
	var events []secondFaultEvent
	if err := json.Unmarshal(result.Stdout, &events); err != nil {
		t.Fatalf("writer ledger %.512s: %v", result.Stdout, err)
	}
	return events
}

// secondFaultMarkNow reads the ledger and returns the watermark and operations.
func secondFaultMarkNow(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, path string) (secondFaultMark, []secondFaultOp) {
	t.Helper()
	ops, err := secondFaultOps(secondFaultLedger(t, ctx, cluster, client, path), false)
	if err != nil {
		t.Fatal("writer ledger refused:", err)
	}
	return secondFaultCount(ops), ops
}

// secondFaultAcks waits until the writer has acknowledged n more operations than
// it had at since, and returns the new watermark. It fails if an operation
// is refused or the writer is not progressing in time.
func secondFaultAcks(t *testing.T, parent context.Context, cluster *ceph.Container, client testcontainers.Container, path string, since secondFaultMark, n int, what string) secondFaultMark {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	for {
		mark, _ := secondFaultMarkNow(t, ctx, cluster, client, path)
		if secondFaultAdvanced(since, mark, n) {
			return mark
		}
		select {
		case <-ctx.Done():
			t.Fatalf("the live writer was not acknowledged %d more times %s: from %+v to %+v", n, what, since, mark)
		case <-time.After(time.Second):
		}
	}
}

// secondFaultAdvanced reports whether the writer has acknowledged at least n
// operations beyond the baseline since.
func secondFaultAdvanced(since, now secondFaultMark, n int) bool { return now.Acked >= since.Acked+n }

// secondFaultInstance is the running identity of one OSD container.
type secondFaultInstance struct {
	Running, OOMKilled bool
	Pid                int
	StartedAt          string
}

func secondFaultInstanceOf(t *testing.T, ctx context.Context, osd *ceph.OSDContainer) secondFaultInstance {
	t.Helper()
	state, err := osd.State(ctx)
	if err != nil || state == nil {
		t.Fatalf("state of osd.%d: %+v: %v", osd.ID, state, err)
	}
	return secondFaultInstance{Running: state.Running, OOMKilled: state.OOMKilled, Pid: state.Pid, StartedAt: state.StartedAt}
}

// secondFaultAdmitStop refuses to label a stop as the deliberate fault unless
// the target is still the running instance that was captured earlier. An OSD
// that already exited, was OOM-killed or restarted is an unexpected exit.
func secondFaultAdmitStop(captured, now secondFaultInstance) error {
	switch {
	case !captured.Running || captured.Pid <= 0:
		return fmt.Errorf("the captured instance was not running: %+v", captured)
	case !now.Running || now.OOMKilled:
		return fmt.Errorf("the target exited before the stop was requested: %+v", now)
	case now.Pid != captured.Pid || now.StartedAt != captured.StartedAt:
		return fmt.Errorf("the target is not the captured instance: %+v, captured %+v", now, captured)
	}
	return nil
}

// pgQueryRaw is queryPG without a fatal exit, for polling a PG that is
// peering.
func pgQueryRaw(ctx context.Context, cluster *ceph.Container, pgid string) ([]byte, error) {
	return cluster.Ceph(ctx, "pg", pgid, "query", "--format", "json")
}

// secondFaultReceipt records the selected PG's placement, the pool's size and
// min_size, and that exactly the expected OSD containers are running.
func secondFaultReceipt(t *testing.T, ctx context.Context, cluster *ceph.Container, phase string, stopped map[int]bool) ceph.PGState {
	t.Helper()
	snapshot, err := poolHandle(t, ctx, cluster, "tc-paths").PGs(ctx)
	if err != nil || len(snapshot.PGs) != 1 || snapshot.PoolBefore.Size != 3 || snapshot.PoolAfter.Size != 3 || snapshot.PoolBefore.MinSize != 2 || snapshot.PoolAfter.MinSize != 2 {
		t.Fatalf("%s placement and policy receipt: %+v: %v", phase, snapshot, err)
	}
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		state, err := osd.State(ctx)
		if err != nil || state == nil || state.Running == stopped[osd.ID] || state.OOMKilled {
			t.Fatalf("%s: unexpected state of osd.%d: %+v: %v (expected stopped=%t)", phase, osd.ID, state, err, stopped[osd.ID])
		}
	}
	pg := snapshot.PGs[0]
	data, err := json.Marshal(map[string]any{
		"phase": phase, "pg": pg.PGID, "state": pg.State, "up": pg.Up, "acting": pg.Acting, "acting_primary": pg.ActingPrimary,
		"reported_epoch": pg.ReportedEpoch, "mapping_epoch": pg.MappingEpoch, "osdmap_epochs": []uint32{snapshot.OSDMapEpochBefore, snapshot.OSDMapEpochAfter},
		"size": snapshot.PoolAfter.Size, "min_size": snapshot.PoolAfter.MinSize, "stopped": stopped,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("OSD_SECOND_FAULT_PHASE %s", data)
	return pg
}

// testOSDSecondFault interrupts a backfill to a spare OSD with a second OSD
// outage. Four OSDs hold one PG of a size 3, min_size 2 pool. An acting replica
// A stops and is marked out while backfill is held, so that the spare D is the
// new backfill destination; the primary's own report must show the held
// backfill, and after release, a cursor for D that has advanced and a transfer
// that is not complete. Only then is D stopped. The two remaining replicas stay
// at min_size, so client writes continue through the second outage. Afterwards
// both OSDs return and the PG must be clean on three copies. A live writer
// keeps one ledger throughout, and an independent client reads back every
// acknowledged object.
//
// The scheduler is set to WPQ with a recovery sleep before any OSD starts, so
// that backfill has an observable duration: Ceph ignores the sleep under
// mClock.
func testOSDSecondFault(t *testing.T) { testOSDSecondFaultPool(t, false) }

// testOSDSecondFaultPool shares the fault, progress and writer ledger protocol.
// erasure changes only pool construction and shard-shaped placement admission.
func testOSDSecondFaultPool(t *testing.T, erasure bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 22*time.Minute)
	defer cancel()
	const ledger, stopFile = "/tmp/tc-second-fault.jsonl", "/tmp/tc-second-fault.stop"
	image, opts := integrationImages(t)
	cluster, client := newServiceClusterWithOptions(t, image, append(opts, ceph.WithNoInitialOSDs())...)
	restore := func(name string, fn func(context.Context) error) {
		t.Helper()
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := fn(cleanup); err != nil {
				t.Errorf("restore %s: %v", name, err)
			}
		})
	}
	var sleeps []*ceph.ConfigOverrideHandle
	for _, setting := range []ceph.ConfigSetting{
		{Section: "osd", Name: "osd_op_queue", Value: "wpq"},
		{Section: "osd", Name: "osd_recovery_sleep", Value: "0.5"},
		{Section: "osd", Name: "osd_recovery_sleep_degraded", Value: "0.5"},
	} {
		handle, err := cluster.OverrideConfig(ctx, setting)
		if handle != nil {
			restore(setting.Name, handle.Restore)
			if setting.Name != "osd_op_queue" {
				sleeps = append(sleeps, handle)
			}
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	// A short, always-trimmed PG log keeps a spare that has no copy of the PG
	// from being brought up to date by log recovery: its empty copy then lies
	// behind the log tail, which needs a backfill.
	for name, value := range map[string]string{"osd_min_pg_log_entries": "10", "osd_max_pg_log_entries": "10", "osd_pg_log_trim_min": "1"} {
		handle, err := cluster.OverrideConfig(ctx, ceph.ConfigSetting{Section: "osd", Name: name, Value: value})
		if handle != nil {
			restore(name, handle.Restore)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	for range 4 {
		if _, err := cluster.AddOSD(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		for name, want := range map[string]string{"osd_op_queue": "wpq", "osd_recovery_sleep": "0.5"} {
			out, err := cluster.Ceph(ctx, "tell", "osd."+strconv.Itoa(osd.ID), "config", "get", name)
			var effective map[string]string
			if err != nil || json.Unmarshal(out, &effective) != nil || (name == "osd_op_queue" && effective[name] != want) || (name != "osd_op_queue" && !abruptSameNumber(effective[name], want)) {
				t.Fatalf("osd.%d effective %s: %s: %v, want %s", osd.ID, name, out, err, want)
			}
		}
	}
	pool, err := cluster.CreatePool(ctx, secondFaultPoolConfig(erasure))
	if err != nil {
		t.Fatal(err)
	}
	if erasure {
		ecAdmissionPool(t, ctx, cluster, pool)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	recoveryRun(t, ctx, cluster, client, "write", "seed:20")
	clean := secondFaultWaitPG(t, ctx, cluster, "clean on three OSDs", func(pg ceph.PGState) bool {
		return pg.State == "active+clean" && len(pg.Acting) == 3 && !slices.Contains(pg.Acting, 2147483647)
	})
	pgid, primary := clean.PGID, clean.ActingPrimary
	var a, d *ceph.OSDContainer
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		switch {
		case !slices.Contains(clean.Acting, osd.ID):
			d = osd
		case osd.ID != primary:
			a = osd
		}
	}
	if a == nil || d == nil {
		t.Fatalf("no non-primary acting OSD and spare among four OSDs: %+v", clean)
	}
	out, err := cluster.Ceph(ctx, "version")
	if err != nil {
		t.Fatal(err)
	}
	uname, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{Argv: []string{"uname", "-sm"}, Timeout: time.Minute, MaxStdout: 256, MaxStderr: 256, Label: "second fault platform"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("OSD_SECOND_FAULT_RUNTIME ceph=%q platform=%q pg=%s primary=osd.%d replica=osd.%d spare=osd.%d scheduler=wpq recovery_sleep=0.5", strings.TrimSpace(string(out)), strings.TrimSpace(string(uname.Stdout)), pgid, primary, a.ID, d.ID)

	stopped := map[int]bool{}
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		restore(fmt.Sprintf("osd.%d", osd.ID), func(cleanup context.Context) error {
			if stopped[osd.ID] {
				stopped[osd.ID] = false
				return osd.Start(cleanup)
			}
			return nil
		})
	}
	// Every OSD is captured as a running instance before any fault; a stop is
	// admitted only for that same instance, and its result is read back apart
	// from the request.
	ecInstances := map[int]ecAdmissionInstance{}
	instances := map[int]secondFaultInstance{}
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		instances[osd.ID] = secondFaultInstanceOf(t, ctx, osd)
		if erasure {
			ecInstances[osd.ID] = ecAdmissionInstanceOf(t, ctx, osd)
		}
		if !instances[osd.ID].Running {
			t.Fatalf("osd.%d is not running before any fault: %+v", osd.ID, instances[osd.ID])
		}
	}
	var reconstructionBase *backfillBaseline
	stopOSD := func(osd *ceph.OSDContainer) {
		t.Helper()
		now := secondFaultInstanceOf(t, ctx, osd)
		if erasure {
			if err := checkECAdmissionStop(ecInstances[osd.ID], ecAdmissionInstanceOf(t, ctx, osd)); err != nil {
				t.Fatalf("EC osd.%d stop refused: %v", osd.ID, err)
			}
		}
		if err := secondFaultAdmitStop(instances[osd.ID], now); err != nil {
			t.Fatalf("osd.%d stop refused: %v", osd.ID, err)
		}
		if erasure && osd == d && reconstructionBase != nil {
			err := ecReconstructionDispatch(ecInstances[osd.ID], func() error {
				raw, err := pgQueryRaw(ctx, cluster, pgid)
				if err != nil {
					return err
				}
				sample, err := parsePGProgress(raw)
				if err != nil {
					return err
				}
				if verdict, why := backfillProgress(*reconstructionBase, sample); verdict != progressReached {
					return fmt.Errorf("EC destination no longer has observable unfinished progress before Stop: %s: %s", verdict, why)
				}
				t.Logf("EC_RECONSTRUCTION_PRE_STOP %.16384s", raw)
				return nil
			}, func() ecAdmissionInstance {
				return ecAdmissionInstanceOf(t, ctx, osd)
			}, func(admitted ecAdmissionInstance) error {
				// Only this freshly admitted, post-query observation supplies
				// the request receipt and ownership of a planned restart.
				now = secondFaultInstance{Running: admitted.Running, OOMKilled: admitted.OOMKilled, Pid: admitted.Pid, StartedAt: admitted.StartedAt}
				t.Logf("EC_RECONSTRUCTION_STOP_REQUEST %s", mustAdmissionJSON(t, map[string]any{"osd": osd.ID, "captured": ecInstances[osd.ID], "admitted": admitted}))
				stopped[osd.ID] = true
				timeout := 10 * time.Second
				return osd.Stop(ctx, &timeout)
			})
			if err != nil {
				t.Fatal(err)
			}
		} else {
			stopped[osd.ID] = true
			timeout := 10 * time.Second
			if err := osd.Stop(ctx, &timeout); err != nil {
				t.Fatal(err)
			}
		}
		after := secondFaultInstanceOf(t, ctx, osd)
		if erasure {
			if err := checkECAdmissionStopped(ecInstances[osd.ID], ecAdmissionInstanceOf(t, ctx, osd)); err != nil {
				t.Fatal(err)
			}
		}
		if after.Running || after.OOMKilled {
			t.Fatalf("osd.%d after the stop request: %+v", osd.ID, after)
		}
		t.Logf("OSD_SECOND_FAULT_STOP osd=%d requested_from=%+v resulting=%+v", osd.ID, now, after)
	}
	noout, err := cluster.OverrideOSDFlag(ctx, "noout", true)
	if noout != nil {
		restore("noout", noout.Restore)
	}
	if err != nil {
		t.Fatal(err)
	}
	hold, err := cluster.OverrideOSDFlag(ctx, "nobackfill", true)
	if hold != nil {
		restore("nobackfill", hold.Restore)
	}
	if err != nil {
		t.Fatal(err)
	}
	restore("osd in", func(cleanup context.Context) error { return a.SetIn(cleanup, true) })

	// The live writer keeps one connection and one ledger for the whole run.
	actor := startNative(ctx, cluster, client, secondFaultCommand("actor", 20*time.Minute, "actor", "tc-paths", ledger, stopFile, "2", "600"))
	actorDone := false
	t.Cleanup(func() {
		if actorDone {
			return
		}
		actor.cancel()
		select {
		case outcome := <-actor.done:
			var unstopped *ceph.NativeCommandUnstoppedError
			if errors.As(outcome.err, &unstopped) {
				stopCtx, stop := context.WithTimeout(context.Background(), time.Minute)
				defer stop()
				if err := unstopped.Stop(stopCtx); err != nil {
					t.Errorf("stop the live writer: %v", err)
				}
			}
		case <-time.After(time.Minute):
			t.Error("the live writer did not stop")
		}
	})
	mark := secondFaultAcks(t, ctx, cluster, client, ledger, secondFaultMark{}, 3, "while healthy")
	secondFaultReceipt(t, ctx, cluster, "healthy", stopped)

	// First fault: one acting replica stops and keeps its store.
	stopOSD(a)
	secondFaultWaitPG(t, ctx, cluster, "active on the two remaining replicas", func(pg ceph.PGState) bool {
		return strings.HasPrefix(pg.State, "active") && strings.Contains(pg.State, "degraded") && !slices.Contains(pg.Acting, a.ID) && secondFaultTwoSurvivors(pg, clean, a.ID, erasure)
	})
	secondFaultReceipt(t, ctx, cluster, "first-fault", stopped)
	secondFaultAcks(t, ctx, cluster, client, ledger, mark, 2, "with one replica down")
	recoveryRun(t, ctx, cluster, client, "write", "delta:100")

	// Marking the stopped replica out admits the spare as a new member. The
	// held backfill is the baseline: its interval, up and acting sets, and the
	// destination's cursor at MIN.
	if err := a.SetIn(ctx, false); err != nil {
		t.Fatal(err)
	}
	secondFaultWaitPG(t, ctx, cluster, "held backfill to the spare", func(pg ceph.PGState) bool {
		return slices.Contains(pg.Up, d.ID) && !slices.Contains(pg.Acting, d.ID) && strings.Contains(pg.State, "backfill")
	})
	var base backfillBaseline
	var heldRaw []byte
	for deadline := time.Now().Add(2 * time.Minute); ; time.Sleep(500 * time.Millisecond) {
		heldRaw, err = pgQueryRaw(ctx, cluster, pgid)
		if err == nil {
			var held pgProgress
			if held, err = parsePGProgress(heldRaw); err == nil {
				if base, err = backfillHeld(held, d.ID); err == nil {
					break
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("backfill to osd.%d was never seen held: %v\n%.16384s", d.ID, err, heldRaw)
		}
	}
	if erasure {
		if !ecReconstructionTarget(base, clean, a.ID, d.ID) {
			t.Fatalf("EC replacement shifted shard positions: %+v original %+v", base, clean)
		}
		reconstructionBase = &base
	}
	t.Logf("OSD_SECOND_FAULT_HELD baseline=%+v pg query: %.16384s", base, heldRaw)
	time.Sleep(5 * time.Second)
	if again, err := pgQueryRaw(ctx, cluster, pgid); err != nil {
		t.Fatal(err)
	} else if later, err := parsePGProgress(again); err != nil {
		t.Fatal(err)
	} else if verdict, why := backfillProgress(base, later); verdict != progressWait {
		t.Fatalf("nobackfill is set, but the progress gate says %s: %s", verdict, why)
	}
	secondFaultReceipt(t, ctx, cluster, "held-backfill", stopped)

	// Release the hold and wait for a native sample in the same interval that
	// shows progress to the spare while work remains. Anything else is an
	// unreached phase, not a passed one.
	if err := hold.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	var reached string
	var reachedRaw []byte
	gateCtx, stopGate := context.WithTimeout(ctx, 4*time.Minute)
	var last string
	for reached == "" {
		raw, err := pgQueryRaw(gateCtx, cluster, pgid)
		if err == nil {
			var sample pgProgress
			if sample, err = parsePGProgress(raw); err == nil {
				verdict, why := backfillProgress(base, sample)
				last = fmt.Sprintf("%s: %s", verdict, why)
				switch verdict {
				case progressReached:
					reached, reachedRaw = why, raw
				case progressTerminal:
					stopGate()
					t.Fatalf("the transfer to osd.%d completed before the second stop; phase not reached: %s\n%.16384s", d.ID, why, raw)
				case progressInvalid:
					stopGate()
					t.Fatalf("the baseline interval ended before the second stop: %s\n%.16384s", why, raw)
				}
			}
		}
		if reached == "" {
			select {
			case <-gateCtx.Done():
				stopGate()
				t.Fatalf("no progress to osd.%d while work remained within the time allowed; last sample %s; last error %v", d.ID, last, err)
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	stopGate()
	before, _ := secondFaultMarkNow(t, ctx, cluster, client, ledger)
	t.Logf("OSD_SECOND_FAULT_PROGRESS %s; writer watermark before the second stop %+v; pg query: %.16384s", reached, before, reachedRaw)

	// Second fault: the destination stops with its transfer unfinished.
	stopOSD(d)
	secondFaultWaitPG(t, ctx, cluster, "active on the two surviving replicas without the spare", func(pg ceph.PGState) bool {
		return strings.HasPrefix(pg.State, "active") && strings.Contains(pg.State, "degraded") && !slices.Contains(pg.Up, d.ID) && !slices.Contains(pg.Acting, d.ID) && !slices.Contains(pg.Acting, a.ID) && secondFaultTwoSurvivors(pg, clean, a.ID, erasure)
	})
	secondFaultReceipt(t, ctx, cluster, "second-fault", stopped)
	// The pre-stop watermark joined the progress sample. Acknowledgments seen
	// before the outage was confirmed do not count: take a baseline now.
	confirmed, _ := secondFaultMarkNow(t, ctx, cluster, client, ledger)
	afterStop := secondFaultAcks(t, ctx, cluster, client, ledger, confirmed, 2, "after the second outage was confirmed")
	readRecoveryObjects(t, ctx, cluster, "seed:20", "delta:100")
	recoveryRun(t, ctx, cluster, client, "write", "after:10")
	t.Logf("OSD_SECOND_FAULT_BOUNDARY min_size=2 surviving=2 writer_before=%+v writer_at_confirmation=%+v writer_after=%+v independent_write=after:10 independent_read=seed:20,delta:100", before, confirmed, afterStop)

	// Restoration: the sleeps end first, so that three copies can converge.
	for _, handle := range sleeps {
		if err := handle.Restore(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.SetIn(ctx, true); err != nil {
		t.Fatal(err)
	}
	for _, osd := range []*ceph.OSDContainer{a, d} {
		if err := osd.Start(ctx); err != nil {
			t.Fatal(err)
		}
		stopped[osd.ID] = false
	}
	secondFaultWaitPG(t, ctx, cluster, "required redundancy restored", func(pg ceph.PGState) bool {
		return pg.State == "active+clean" && len(pg.Acting) == 3 && !slices.Contains(pg.Acting, 2147483647) &&
			pg.Stats.ObjectsDegraded == 0 && pg.Stats.ObjectsMisplaced == 0 && pg.Stats.ObjectsUnfound == 0 && pg.Stats.ObjectCopies == 3*pg.Stats.Objects
	})
	secondFaultReceipt(t, ctx, cluster, "restored", stopped)

	// Close the writer and the data oracle.
	execCommand(t, ctx, client, "touch", stopFile)
	select {
	case outcome := <-actor.done:
		actorDone = true
		if outcome.err != nil || outcome.result.ExitCode != 0 || !outcome.result.Complete() {
			t.Fatalf("live writer shutdown: %v exit %d: %.1024s", outcome.err, outcome.result.ExitCode, outcome.result.Stderr)
		}
	case <-time.After(2 * time.Minute):
		t.Fatal("the live writer did not stop after its stop file appeared")
	}
	ops, err := secondFaultOps(secondFaultLedger(t, ctx, cluster, client, ledger), true)
	if err != nil {
		t.Fatal("writer ledger refused:", err)
	}
	final := secondFaultCount(ops)
	names := make([]string, len(ops))
	for i, op := range ops {
		names[i] = op.Name
	}
	encodedNames, err := json.Marshal(names)
	if err != nil {
		t.Fatal(err)
	}
	read, err := cluster.RunNativeCommand(ctx, toolkitContainer(t, cluster), secondFaultCommand("independent read", 3*time.Minute, "read", "tc-paths", string(encodedNames)))
	if err != nil || read.ExitCode != 0 || !read.Complete() {
		t.Fatalf("independent read of the writer's objects: exit %d: %v: %.2048s", read.ExitCode, err, read.Stderr)
	}
	var objects map[string]secondFaultObject
	if err := json.Unmarshal(read.Stdout, &objects); err != nil {
		t.Fatalf("independent read %.512s: %v", read.Stdout, err)
	}
	if err := secondFaultVerify(ops, objects); err != nil {
		t.Fatal(err)
	}
	if final.Acked < afterStop.Acked || final.Acked == 0 || final.Refused != 0 {
		t.Fatalf("writer outcomes %+v do not extend the watermark %+v", final, afterStop)
	}
	readRecoveryObjects(t, ctx, cluster, "seed:20", "delta:100", "after:10")
	receipt, err := json.Marshal(map[string]any{
		"pg": pgid, "primary": primary, "replica": a.ID, "spare": d.ID, "baseline": base,
		"progress": reached, "writer_before_second_stop": before, "writer_after_second_stop": afterStop, "writer_final": final,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("OSD_SECOND_FAULT_RECEIPT %s", receipt)
	if erasure {
		t.Logf("EC_RECONSTRUCTION_RECEIPT %s", receipt)
	}
}

// secondFaultWaitPG is waitPG that keeps the last PG it saw for its failure
// message, so a late query error does not hide the state that never matched.
func secondFaultWaitPG(t *testing.T, parent context.Context, cluster *ceph.Container, what string, ok func(ceph.PGState) bool) ceph.PGState {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	var seen, failure string
	for {
		snapshot, err := poolHandle(t, ctx, cluster, "tc-paths").PGs(ctx)
		if err == nil && len(snapshot.PGs) == 1 {
			if ok(snapshot.PGs[0]) {
				return snapshot.PGs[0]
			}
			pg := snapshot.PGs[0]
			seen = fmt.Sprintf("%s up %v acting %v", pg.State, pg.Up, pg.Acting)
		} else if ctx.Err() == nil {
			failure = fmt.Sprintf("%v, %d PGs", err, len(snapshot.PGs))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("PG never %s: last seen %q, last query failure %q", what, seen, failure)
		case <-time.After(500 * time.Millisecond):
		}
	}
}
