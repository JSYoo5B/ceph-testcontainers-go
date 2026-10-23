//go:build all || (integration && topology)

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// The ledger lives in the client, outside the killed OSD. It is an observation
// channel, not part of the store whose durability the scenario inspects. Each
// mutation is submitted once; the actor never reconnects or retries it.
const abruptWriteScript = `import hashlib,json,os,queue,signal,sys,time,rados
phase,pool,path=sys.argv[1:4]
def payload(seq): return ('sequence=%d;'%seq).encode()+bytes((i*13+seq)%251 for i in range(65539+seq*4096))
def emit(event,**kw):
    with open(path,'a') as f:
        f.write(json.dumps(dict(event=event,ns=time.monotonic_ns(),**kw))+'\n'); f.flush(); os.fsync(f.fileno())
def connect():
    c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_osd_op_timeout':'0','rados_mon_op_timeout':'10'})
    c.connect(); return c
def view(io,name):
    try:
        size,mtime=io.stat(name); data=io.read(name,size+1,0); version=io.get_last_version()
        return dict(present=True,size=size,sha256=hashlib.sha256(data).hexdigest(),version=version,mtime=str(mtime),data_len=len(data))
    except rados.ObjectNotFound: return dict(present=False)
if phase=='clock': print(time.monotonic_ns()); sys.exit(0)
if phase=='ledger':
    print(json.dumps([json.loads(line) for line in open(path)] if os.path.exists(path) else [])); sys.exit(0)
if phase=='presubmit':
    # Deliberate local validation failure before constructing an AIO request.
    try: raise ValueError('rejected before submission')
    except ValueError: emit('rejected',object='never-submitted',submissions=0)
    sys.exit(0)
c=connect()
try:
    with c.open_ioctx(pool) as io:
        if phase=='seed':
            for seq,name in enumerate(('control','acknowledged','replacement')):
                data=payload(seq); io.write_full(name,data)
                emit('acknowledged',object=name,seq=seq,ret=0,client='client.%d'%c.get_instance_id(),**view(io,name))
        elif phase=='actor':
            # The callback only enqueues. This one writer serializes every record,
            # so a delayed or missing callback never changes what the ledger says
            # about the request itself.
            callbacks=queue.SimpleQueue(); stopping=[False]
            signal.signal(signal.SIGTERM,lambda *_: stopping.__setitem__(0,True))
            def delivered(name): return lambda completion: callbacks.put((name,time.monotonic_ns()))
            pending={}; called=set()
            def drain():
                while True:
                    try: name,ns=callbacks.get_nowait()
                    except queue.Empty: return
                    called.add(name)
                    emit('callback',object=name,callback_ns=ns,ret=pending[name].get_return_value() if name in pending else None)
            client='client.%d'%c.get_instance_id()
            for seq,name in ((3,'fresh'),(4,'replacement')):
                data=payload(seq)
                # Intent is recorded before the API call: its monotonic time bounds
                # the submission from below, whatever the call does afterwards.
                emit('intent',object=name,seq=seq,bytes=len(data),sha256=hashlib.sha256(data).hexdigest(),client=client)
                try: pending[name]=io.aio_write_full(name,data,oncomplete=delivered(name))
                except Exception as e:
                    emit('api_return',object=name,error=repr(e)); continue
                emit('api_return',object=name,error='')
            outstanding=dict(pending)
            while outstanding and not stopping[0]:
                drain()
                for name,completion in list(outstanding.items()):
                    if completion.is_complete():
                        emit('completed',object=name,ret=completion.get_return_value())
                        del outstanding[name]
                time.sleep(.01)
            # Give the callbacks of completed requests a bounded time to arrive.
            for _ in range(200):
                drain()
                if all(name in called for name in pending if name not in outstanding): break
                time.sleep(.01)
            drain()
            emit('channel',pending=sorted(outstanding),callbacks=sorted(called),stopped=stopping[0])
        elif phase=='read':
            # A separate process and fresh connection, never the actor's ioctx.
            out={name:view(io,name) for name in ('control','acknowledged','replacement','fresh','never-submitted')}
            for seq,name in ((0,'control'),(1,'acknowledged')):
                data=payload(seq); assert out[name]['size']==len(data) and out[name]['sha256']==hashlib.sha256(data).hexdigest(),out
            for name,seqs in (('replacement',(2,4)),('fresh',(3,))):
                if not out[name]['present']:
                    assert name=='fresh',out
                else:
                    assert any(out[name]['size']==len(payload(seq)) and out[name]['sha256']==hashlib.sha256(payload(seq)).hexdigest() for seq in seqs),out
                    assert out[name]['data_len']==out[name]['size'] and out[name]['version']>0,out
            assert not out['never-submitted']['present'],out
            print(json.dumps(out))
        else: raise ValueError('unknown phase')
finally: c.shutdown()
`

// abruptWriteEvent is one ledger record. Events are "intent" (before the API
// call), "api_return" (the call returned, or raised Error), "completed" (the
// completion became observable, with its return code), "callback" (the
// completion callback was delivered) and "channel" (the writer ended and
// reports what it did not see). The writer never retries or resubmits.
type abruptWriteEvent struct {
	Event      string   `json:"event"`
	NS         int64    `json:"ns"`
	CallbackNS int64    `json:"callback_ns"`
	Object     string   `json:"object"`
	Client     string   `json:"client"`
	Ret        int      `json:"ret"`
	Seq        int      `json:"seq"`
	Bytes      int      `json:"bytes"`
	SHA256     string   `json:"sha256"`
	Error      string   `json:"error"`
	Pending    []string `json:"pending"`
	Callbacks  []string `json:"callbacks"`
	Stopped    bool     `json:"stopped"`
}

type abruptObjectState struct {
	Present bool   `json:"present"`
	Size    int    `json:"size"`
	SHA256  string `json:"sha256"`
	Version uint64 `json:"version"`
	Mtime   string `json:"mtime"`
	DataLen int    `json:"data_len"`
}

func abruptPhase(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, phase, pool, path string, out any) []byte {
	t.Helper()
	result, err := cluster.RunNativeCommand(ctx, client, abruptCommand(phase, pool, path))
	if err != nil || !result.Complete() || result.ExitCode != 0 {
		t.Fatalf("abrupt phase %s error=%v exit=%d complete=%t stdout=%s stderr=%s", phase, err, result.ExitCode, result.Complete(), result.Stdout, result.Stderr)
	}
	if out != nil {
		if err := json.Unmarshal(result.Stdout, out); err != nil {
			t.Fatalf("abrupt phase %s JSON %s: %v", phase, result.Stdout, err)
		}
	}
	return result.Stdout
}

func abruptCommand(phase, pool, path string) ceph.NativeCommand {
	return ceph.NativeCommand{Argv: []string{"python3", "-c", abruptWriteScript, phase, pool, path}, Timeout: 3 * time.Minute, MaxStdout: 1 << 20, MaxStderr: 1 << 20, Label: "abrupt write " + phase}
}

func abruptLedger(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, pool, path string) []abruptWriteEvent {
	t.Helper()
	var events []abruptWriteEvent
	abruptPhase(t, ctx, cluster, client, "ledger", pool, path, &events)
	return events
}

// abruptOverlap requires two distinct submissions whose intent and API return
// precede the kill request, and that no completion or callback is recorded
// before the observed exit. A completion inside the kill RPC is a race, not a
// passed window. A completion after the exit is the original outcome and a
// missing one stays unknown.
func abruptOverlap(events []abruptWriteEvent, beforeKill, afterExit int64) error {
	if beforeKill <= 0 || afterExit <= beforeKill {
		return fmt.Errorf("invalid kill clock interval")
	}
	intent, returned := map[string]bool{}, map[string]bool{}
	for _, event := range events {
		switch event.Event {
		case "intent":
			if event.NS <= 0 || event.NS >= beforeKill || intent[event.Object] || (event.Object != "fresh" && event.Object != "replacement") {
				return fmt.Errorf("invalid or repeated submission intent %+v", event)
			}
			intent[event.Object] = true
		case "api_return":
			if !intent[event.Object] || returned[event.Object] || event.NS >= beforeKill || event.Error != "" {
				return fmt.Errorf("submission did not return cleanly before the kill request %+v", event)
			}
			returned[event.Object] = true
		case "completed":
			if !returned[event.Object] || event.NS <= afterExit {
				return fmt.Errorf("completion preceded observed exit or raced with kill %+v", event)
			}
		case "callback":
			if !returned[event.Object] || event.CallbackNS <= afterExit {
				return fmt.Errorf("callback preceded observed exit or raced with kill %+v", event)
			}
		}
	}
	if len(returned) != 2 {
		return fmt.Errorf("need both submitted operations, got %d", len(returned))
	}
	return nil
}

// abruptChannel names the state of the writer's callback channel, apart from
// the outcome of any request: "complete" when the writer ended after seeing
// every completion and delivered callback, "incomplete" when it ended with
// requests outstanding or callbacks missing, and "missing" when it never
// reported (for example because it was killed).
func abruptChannel(events []abruptWriteEvent) string {
	for _, event := range events {
		if event.Event != "channel" {
			continue
		}
		if len(event.Pending) == 0 && len(event.Callbacks) == 2 {
			return "complete"
		}
		return "incomplete"
	}
	return "missing"
}

// abruptWitness is the native request that a caller-outstanding write maps to
// on a single-copy PG. The OSD does not mark such a write "waiting for sub
// ops", so the witness takes the unique matching request and keeps its full ID
// and events. It does not show that the write is uncommitted or that it stays
// outstanding while the daemon is killed.
type abruptWitness struct {
	Object  string   `json:"object"`
	Request string   `json:"request"`
	Flag    string   `json:"flag_point"`
	Events  []string `json:"events"`
}

// abruptFindWitness returns the one write request of client for object in a
// dump_ops_in_flight output. No match is not yet a witness. Several matches,
// an unreadable dump, a request of another client or PG, or a request without
// an ID refuse admission, because the phase would not be proven.
func abruptFindWitness(data []byte, object, client, pg string) (abruptWitness, bool, error) {
	var dump struct {
		Ops []heldOp `json:"ops"`
	}
	if err := json.Unmarshal(data, &dump); err != nil {
		return abruptWitness{}, false, fmt.Errorf("dump_ops_in_flight: %w", err)
	}
	var found []heldOp
	for _, op := range dump.Ops {
		if strings.HasPrefix(op.Description, "osd_op(") && strings.Contains(op.Description, ":::"+object+":head") {
			found = append(found, op)
		}
	}
	switch len(found) {
	case 0:
		return abruptWitness{}, false, nil
	case 1:
	default:
		return abruptWitness{}, false, fmt.Errorf("%d requests name object %s", len(found), object)
	}
	op := found[0]
	request := op.request()
	if request == "" || !strings.HasPrefix(request, client+".") {
		return abruptWitness{}, false, fmt.Errorf("request %q of object %s is not from %s", request, object, client)
	}
	if !strings.Contains(op.Description, " "+pg+" ") || !strings.Contains(op.Description, "write") {
		return abruptWitness{}, false, fmt.Errorf("request %q of object %s is not a write to PG %s", request, object, pg)
	}
	return abruptWitness{Object: object, Request: request, Flag: op.TypeData.FlagPoint, Events: op.events()}, true, nil
}

// abruptWitnesses reads the OSD's in-flight operations and returns the unique
// request of each submitted object that the OSD tracks so far. The
// daemon's process generation must be the same before and after the dump.
func abruptWitnesses(t *testing.T, ctx context.Context, cluster *ceph.Container, docker *mobycl.Client, osd *ceph.OSDContainer, daemon, pg, client string) []abruptWitness {
	t.Helper()
	before := osdMemoryInspect(t, ctx, docker, osd.GetContainerID())
	data, err := cluster.AdminSocket(ctx, daemon, "dump_ops_in_flight")
	if err != nil {
		t.Fatal("abrupt in-flight observation", err)
	}
	after := osdMemoryInspect(t, ctx, docker, osd.GetContainerID())
	if !before.State.Running || before.State.Pid != after.State.Pid || before.State.StartedAt != after.State.StartedAt {
		t.Fatalf("daemon generation changed while its requests were read: %+v then %+v", before.State, after.State)
	}
	var witnesses []abruptWitness
	for _, object := range []string{"fresh", "replacement"} {
		witness, ok, err := abruptFindWitness(data, object, client, pg)
		if err != nil {
			t.Fatal("abrupt request witness refused", err)
		}
		if ok {
			witnesses = append(witnesses, witness)
		}
	}
	return witnesses
}

func abruptDispatchDelay(t *testing.T, ctx context.Context, cluster *ceph.Container, osd string) func() {
	t.Helper()
	var handles []*ceph.ConfigOverrideHandle
	restore := func() {
		for i := len(handles) - 1; i >= 0; i-- {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
			if err := handles[i].Restore(cleanup); err != nil {
				t.Error("dispatch delay restore", err)
			}
			cancel()
		}
		handles = nil
	}
	t.Cleanup(restore)
	for _, setting := range []ceph.ConfigSetting{
		{Section: osd, Name: "osd_debug_inject_dispatch_delay_duration", Value: "30"},
		{Section: osd, Name: "osd_debug_inject_dispatch_delay_probability", Value: "1"},
	} {
		handle, err := cluster.OverrideConfig(ctx, setting)
		if handle != nil {
			handles = append(handles, handle)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	// The central setting reaches a running daemon asynchronously. Require the
	// daemon's own value before any request is submitted, so that a request is
	// never counted against a delay that is not in effect yet.
	for _, setting := range []ceph.ConfigSetting{
		{Name: "osd_debug_inject_dispatch_delay_duration", Value: "30"},
		{Name: "osd_debug_inject_dispatch_delay_probability", Value: "1"},
	} {
		deadline := time.Now().Add(time.Minute)
		for {
			out, err := cluster.AdminSocket(ctx, osd, "config", "get", setting.Name)
			var reply map[string]string
			if err == nil && json.Unmarshal(out, &reply) == nil && abruptSameNumber(reply[setting.Name], setting.Value) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("daemon %s did not apply %s=%s: %s %v", osd, setting.Name, setting.Value, out, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	return restore
}

// abruptSameNumber compares two decimal renderings, such as "30.000000" and "30".
func abruptSameNumber(a, b string) bool {
	x, errA := strconv.ParseFloat(a, 64)
	y, errB := strconv.ParseFloat(b, 64)
	return errA == nil && errB == nil && x == y
}
