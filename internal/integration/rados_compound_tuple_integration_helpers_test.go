//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// A whole-state tuple of one object: its data (size and hash), one transaction
// xattr and one selected OMAP key. The tuple is written by ONE compound write
// operation and read by ONE compound read operation, so that neither the
// transition between two tuples nor an observation of it can be split.
//
// Binding facts at the pinned Ceph commit (Tentacle 20.2.4, src/pybind/rados):
// the Python WriteOp exposes write_full and set_xattr, and Ioctx.set_omap adds
// omap_set to the same write op, so the write side is one Python operation.
// The Python ReadOp exposes only cmpext, omap_get_vals/keys/by_keys and
// set_flags: it has no data read, no stat and no xattr read. The compound read
// therefore goes through the C API of the same librados (rados_read_op_stat,
// _read, _getxattrs and _omap_get_vals_by_keys2 on one rados_read_op_t), called
// with ctypes on a connection of its own. Independent setters or read calls do
// not substitute for either operation.
const (
	tupleObject    = "tuple-object"
	tupleXattrName = "tuple.xattr"
	tupleOMAPKey   = "tuple.key"
)

// tupleScript is the actor and the observer. Phases: seed (tuple A by one
// synchronous compound write), submit (tuple B by exactly one asynchronous
// compound write with a ledger), read (one compound read), ledger.
const tupleScript = `import ctypes,hashlib,json,os,queue,signal,sys,time
phase,pool,path=sys.argv[1:4]
OBJECT,XATTR,OMAPKEY='tuple-object','tuple.xattr','tuple.key'
LIMIT=1<<20
ACTIONS=['write_full','set_xattr','omap_set']
def tuple_of(tag):
    n={'A':65539,'B':98317}[tag]
    return dict(data=('tuple=%s;'%tag).encode()+bytes((i*13+ord(tag))%251 for i in range(n)),xattr=('xattr-%s'%tag).encode(),omap=('omap-%s'%tag).encode())
def describe(t): return dict(bytes=len(t['data']),sha256=hashlib.sha256(t['data']).hexdigest(),xattr=t['xattr'].hex(),omap=t['omap'].hex())
def emit(event,**kw):
    with open(path,'a') as f:
        f.write(json.dumps(dict(event=event,ns=time.monotonic_ns(),**kw))+'\n'); f.flush(); os.fsync(f.fileno())
def sha(data): return hashlib.sha256(data).hexdigest()
def observe():
    P,VP=ctypes.POINTER,ctypes.c_void_p
    lib=None
    for name in ('librados.so.2','librados.so'):
        try: lib=ctypes.CDLL(name); break
        except OSError: pass
    if lib is None: raise OSError('librados is not loadable')
    for name,res,args in (
        ('rados_create2',ctypes.c_int,[P(VP),ctypes.c_char_p,ctypes.c_char_p,ctypes.c_uint64]),
        ('rados_conf_read_file',ctypes.c_int,[VP,ctypes.c_char_p]),
        ('rados_conf_set',ctypes.c_int,[VP,ctypes.c_char_p,ctypes.c_char_p]),
        ('rados_connect',ctypes.c_int,[VP]),
        ('rados_shutdown',None,[VP]),
        ('rados_get_instance_id',ctypes.c_uint64,[VP]),
        ('rados_cluster_fsid',ctypes.c_int,[VP,ctypes.c_char_p,ctypes.c_size_t]),
        ('rados_ioctx_create',ctypes.c_int,[VP,ctypes.c_char_p,P(VP)]),
        ('rados_ioctx_get_id',ctypes.c_int64,[VP]),
        ('rados_ioctx_destroy',None,[VP]),
        ('rados_create_read_op',VP,[]),
        ('rados_release_read_op',None,[VP]),
        ('rados_read_op_stat',None,[VP,P(ctypes.c_uint64),P(ctypes.c_int64),P(ctypes.c_int)]),
        ('rados_read_op_read',None,[VP,ctypes.c_uint64,ctypes.c_size_t,VP,P(ctypes.c_size_t),P(ctypes.c_int)]),
        ('rados_read_op_getxattrs',None,[VP,P(VP),P(ctypes.c_int)]),
        ('rados_read_op_omap_get_vals_by_keys2',None,[VP,P(ctypes.c_char_p),ctypes.c_size_t,P(ctypes.c_size_t),P(VP),P(ctypes.c_int)]),
        ('rados_read_op_operate',ctypes.c_int,[VP,VP,ctypes.c_char_p,ctypes.c_int]),
        ('rados_getxattrs_next',ctypes.c_int,[VP,P(VP),P(VP),P(ctypes.c_size_t)]),
        ('rados_getxattrs_end',None,[VP]),
        ('rados_omap_get_next2',ctypes.c_int,[VP,P(VP),P(VP),P(ctypes.c_size_t),P(ctypes.c_size_t)]),
        ('rados_omap_get_end',None,[VP])):
        fn=getattr(lib,name); fn.restype=res; fn.argtypes=args
    ptr=ctypes.pointer
    def ok(ret,what):
        if ret<0: raise OSError('%s failed with %d'%(what,ret))
    # prval slots start at a value no action returns, so an action that was not
    # executed can never count as a success.
    pr={k:ctypes.c_int(-99999) for k in ('stat','read','getxattrs','omap_get_vals_by_keys')}
    obs=dict(object=OBJECT,pool=pool,compound=True,actions=['stat','read','getxattrs','omap_get_vals_by_keys'],operates=0,subresults={})
    cluster,io=VP(),VP()
    ok(lib.rados_create2(ptr(cluster),None,b'client.admin',0),'rados_create2')
    try:
        ok(lib.rados_conf_read_file(cluster,b'/etc/ceph/ceph.conf'),'rados_conf_read_file')
        for k,v in (('keyring','/etc/ceph/ceph.client.admin.keyring'),('rados_osd_op_timeout','0'),('rados_mon_op_timeout','10')):
            ok(lib.rados_conf_set(cluster,k.encode(),v.encode()),'rados_conf_set '+k)
        ok(lib.rados_connect(cluster),'rados_connect')
        fsid=ctypes.create_string_buffer(64)
        ok(lib.rados_cluster_fsid(cluster,fsid,64),'rados_cluster_fsid')
        obs.update(fsid=fsid.value.decode(),client='client.%d'%lib.rados_get_instance_id(cluster))
        ok(lib.rados_ioctx_create(cluster,pool.encode(),ptr(io)),'rados_ioctx_create')
        try:
            obs['pool_id']=lib.rados_ioctx_get_id(io)
            op=lib.rados_create_read_op()
            try:
                size,mtime,nread=ctypes.c_uint64(),ctypes.c_int64(),ctypes.c_size_t()
                buf=ctypes.create_string_buffer(LIMIT)
                xit,oit=VP(),VP()
                keys=(ctypes.c_char_p*1)(OMAPKEY.encode()); lens=(ctypes.c_size_t*1)(len(OMAPKEY))
                lib.rados_read_op_stat(op,ptr(size),ptr(mtime),ptr(pr['stat']))
                lib.rados_read_op_read(op,0,LIMIT,ctypes.addressof(buf),ptr(nread),ptr(pr['read']))
                lib.rados_read_op_getxattrs(op,ptr(xit),ptr(pr['getxattrs']))
                lib.rados_read_op_omap_get_vals_by_keys2(op,keys,1,lens,ptr(oit),ptr(pr['omap_get_vals_by_keys']))
                obs['operates']=1
                obs['operate_ret']=lib.rados_read_op_operate(op,io,OBJECT.encode(),0)
                obs['subresults']={k:v.value for k,v in pr.items()}
                if obs['operate_ret']==0 and all(v.value==0 for v in pr.values()):
                    obs.update(size=size.value,bytes_read=nread.value,eof=nread.value<LIMIT,sha256=sha(buf.raw[:nread.value]))
                    name,val,ln=VP(),VP(),ctypes.c_size_t()
                    xattrs={}
                    while True:
                        name.value=None; val.value=None; ln.value=0
                        ok(lib.rados_getxattrs_next(xit,ptr(name),ptr(val),ptr(ln)),'rados_getxattrs_next')
                        if not name.value: break
                        xattrs[ctypes.string_at(name.value).decode()]=(ctypes.string_at(val.value,ln.value) if ln.value else b'').hex()
                    lib.rados_getxattrs_end(xit)
                    obs['xattrs']=xattrs
                    key,kl,vl=VP(),ctypes.c_size_t(),ctypes.c_size_t()
                    omap={}
                    while True:
                        key.value=None; val.value=None; kl.value=0; vl.value=0
                        ok(lib.rados_omap_get_next2(oit,ptr(key),ptr(val),ptr(kl),ptr(vl)),'rados_omap_get_next2')
                        if not key.value: break
                        omap[ctypes.string_at(key.value,kl.value).decode()]=(ctypes.string_at(val.value,vl.value) if vl.value else b'').hex()
                    lib.rados_omap_get_end(oit)
                    obs['omap']=omap
            finally: lib.rados_release_read_op(op)
        finally: lib.rados_ioctx_destroy(io)
    finally: lib.rados_shutdown(cluster)
    return obs
if phase=='clock': print(time.monotonic_ns()); sys.exit(0)
if phase=='ledger':
    print(json.dumps([json.loads(line) for line in open(path)] if os.path.exists(path) else [])); sys.exit(0)
if phase=='read':
    try: obs=observe()
    except Exception as e: obs=dict(error=repr(e))
    print(json.dumps(obs)); sys.exit(0)
import rados
def connect():
    c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_osd_op_timeout':'0','rados_mon_op_timeout':'10'})
    c.connect(); return c
def build(io,op,t):
    # Every action goes into this one write operation.
    op.write_full(t['data']); op.set_xattr(XATTR,t['xattr']); io.set_omap(op,(OMAPKEY,),(t['omap'],))
c=connect()
try:
    with c.open_ioctx(pool) as io:
        client='client.%d'%c.get_instance_id()
        if phase=='seed':
            t=tuple_of('A')
            with rados.WriteOpCtx() as op:
                build(io,op,t)
                io.operate_write_op(op,OBJECT)
            emit('seed',object=OBJECT,ret=0,actions=ACTIONS,client=client,**describe(t))
        elif phase=='submit':
            # The callback only enqueues; this one writer serializes the ledger.
            t=tuple_of('B'); callbacks=queue.SimpleQueue(); stopping=[False]
            signal.signal(signal.SIGTERM,lambda *_: stopping.__setitem__(0,True))
            comp=[None]; recorded=[False]; called=[False]
            def record(via):
                if recorded[0]: return
                recorded[0]=True
                emit('completed',object=OBJECT,ret=comp[0].get_return_value(),via=via)
            def drain():
                while True:
                    try: ns=callbacks.get_nowait()
                    except queue.Empty: return
                    called[0]=True; record('callback')
                    emit('callback',object=OBJECT,callback_ns=ns,ret=comp[0].get_return_value())
            with rados.WriteOpCtx() as op:
                try: build(io,op,t)
                except Exception as e:
                    emit('rejected',object=OBJECT,error=repr(e),submissions=0); sys.exit(0)
                # Intent precedes the API call. The operation is submitted once
                # and never retried or replayed.
                emit('intent',object=OBJECT,actions=ACTIONS,client=client,**describe(t))
                try: comp[0]=io.operate_aio_write_op(op,OBJECT,oncomplete=lambda _: callbacks.put(time.monotonic_ns()))
                except Exception as e: emit('api_return',object=OBJECT,error=repr(e))
                else:
                    emit('api_return',object=OBJECT,error='')
                    deadline=time.monotonic()+90
                    while not comp[0].is_complete() and not stopping[0] and time.monotonic()<deadline:
                        drain(); time.sleep(.01)
                    if comp[0].is_complete(): record('poll')
                    for _ in range(200):
                        drain()
                        if called[0] or not recorded[0]: break
                        time.sleep(.01)
                    drain()
                    emit('channel',object=OBJECT,pending=[] if recorded[0] else [OBJECT],callbacks=[OBJECT] if called[0] else [],stopped=stopping[0])
            emit('stop',object=OBJECT,stopped=stopping[0])
        else: raise ValueError('unknown phase')
finally: c.shutdown()
`

// tupleEvent is one ledger record: "seed" (tuple A written), "intent" (before
// the submission of tuple B), "api_return" (the call returned, or raised
// Error), "completed" (the completion became observable, with its return
// code), "callback", "channel" and "stop" (the actor ended).
type tupleEvent struct {
	Event      string   `json:"event"`
	NS         int64    `json:"ns"`
	CallbackNS int64    `json:"callback_ns"`
	Object     string   `json:"object"`
	Client     string   `json:"client"`
	Ret        int      `json:"ret"`
	Bytes      int      `json:"bytes"`
	SHA256     string   `json:"sha256"`
	Xattr      string   `json:"xattr"`
	OMAP       string   `json:"omap"`
	Error      string   `json:"error"`
	Via        string   `json:"via"`
	Actions    []string `json:"actions"`
	Pending    []string `json:"pending"`
	Callbacks  []string `json:"callbacks"`
	Stopped    bool     `json:"stopped"`
}

// tupleValue is one whole object state. Xattr and OMAP values are hex.
type tupleValue struct {
	Size   int
	SHA256 string
	Xattrs map[string]string
	OMAP   map[string]string
}

func (v tupleValue) equal(o tupleValue) bool {
	return v.Size == o.Size && v.SHA256 == o.SHA256 && maps.Equal(v.Xattrs, o.Xattrs) && maps.Equal(v.OMAP, o.OMAP)
}

// tupleIdentity is the object and store identity an observation must retain.
type tupleIdentity struct {
	Object, Pool, FSID string
	PoolID             int64
}

// tupleResult is a verdict. "accepted": the observed tuple is a member of the
// set the outcome permits. "refused": a coherent observation or the ledger
// contradicts the outcome (including any component-wise mixture). "inconclusive":
// the observation is not proven coherent or complete; it is no evidence either way.
type tupleResult struct {
	Status  string
	Outcome string
	Matched string
	Reason  string
}

func tupleRefuse(outcome, format string, args ...any) tupleResult {
	return tupleResult{Status: "refused", Outcome: outcome, Reason: fmt.Sprintf(format, args...)}
}

func tupleInconclusive(outcome, format string, args ...any) tupleResult {
	return tupleResult{Status: "inconclusive", Outcome: outcome, Reason: fmt.Sprintf(format, args...)}
}

func tupleOfEvent(e tupleEvent) (tupleValue, error) {
	for _, h := range []string{e.Xattr, e.OMAP} {
		if raw, err := hex.DecodeString(h); err != nil || len(raw) == 0 {
			return tupleValue{}, fmt.Errorf("%s record has an empty or invalid xattr/omap value", e.Event)
		}
	}
	if raw, err := hex.DecodeString(e.SHA256); err != nil || len(raw) != 32 || e.Bytes <= 0 {
		return tupleValue{}, fmt.Errorf("%s record has an invalid size or hash", e.Event)
	}
	return tupleValue{Size: e.Bytes, SHA256: e.SHA256, Xattrs: map[string]string{tupleXattrName: e.Xattr}, OMAP: map[string]string{tupleOMAPKey: e.OMAP}}, nil
}

// tupleDistinct requires every component to differ, so that a component-wise
// mixture of A and B is distinguishable from both.
func tupleDistinct(a, b tupleValue) error {
	if a.Size == b.Size || a.SHA256 == b.SHA256 || maps.Equal(a.Xattrs, b.Xattrs) || maps.Equal(a.OMAP, b.OMAP) {
		return errors.New("tuples A and B share a component, so a mixture would be indistinguishable")
	}
	return nil
}

// tupleLedger derives tuple A, tuple B and the outcome of the one compound
// write from the ledger. Outcomes: "pre_submission" (no intent, nothing else),
// "known_success" (the original completion or callback returned 0) and
// "unknown" (everything else that was submitted: no completion, a failed
// completion, an API error). The outcome is never promoted by a readback.
func tupleLedger(events []tupleEvent) (a, b tupleValue, outcome string, err error) {
	var seeds, intents, returns []tupleEvent
	var completions, callbacks []tupleEvent
	for _, e := range events {
		switch e.Event {
		case "seed":
			seeds = append(seeds, e)
		case "intent":
			intents = append(intents, e)
		case "api_return":
			returns = append(returns, e)
		case "completed":
			completions = append(completions, e)
		case "callback":
			callbacks = append(callbacks, e)
		}
	}
	compound := []string{"write_full", "set_xattr", "omap_set"}
	if len(seeds) != 1 || seeds[0].Ret != 0 || seeds[0].Object != tupleObject || !slices.Equal(seeds[0].Actions, compound) {
		return a, b, "", errors.New("the ledger needs exactly one successful seed of tuple A by one compound operation")
	}
	if a, err = tupleOfEvent(seeds[0]); err != nil {
		return a, b, "", err
	}
	switch {
	case len(intents) == 0:
		if len(returns)+len(completions)+len(callbacks) != 0 {
			return a, b, "", errors.New("submission records without a submission intent")
		}
		return a, tupleValue{}, "pre_submission", nil
	case len(intents) > 1 || len(returns) > 1 || len(completions) > 1 || len(callbacks) > 1:
		return a, b, "", errors.New("the compound write was submitted or recorded more than once (replay)")
	}
	in := intents[0]
	if in.Object != tupleObject || !slices.Equal(in.Actions, compound) || in.NS <= seeds[0].NS {
		return a, b, "", fmt.Errorf("invalid submission intent %+v", in)
	}
	if b, err = tupleOfEvent(in); err != nil {
		return a, b, "", err
	}
	if err = tupleDistinct(a, b); err != nil {
		return a, b, "", err
	}
	if len(returns) == 1 && (returns[0].Object != tupleObject || returns[0].NS < in.NS) {
		return a, b, "", fmt.Errorf("api return does not follow its intent %+v", returns[0])
	}
	if (len(completions) > 0 || len(callbacks) > 0) && (len(returns) == 0 || returns[0].Error != "") {
		return a, b, "", errors.New("a completion was recorded for a submission that did not return cleanly")
	}
	var rets []int
	for _, e := range append(slices.Clone(completions), callbacks...) {
		if e.Object != tupleObject {
			return a, b, "", fmt.Errorf("completion of another object %+v", e)
		}
		rets = append(rets, e.Ret)
	}
	switch {
	case len(rets) == 0:
		return a, b, "unknown", nil
	case slices.Min(rets) != slices.Max(rets):
		return a, b, "", errors.New("the completion and the callback disagree about the return code")
	case rets[0] == 0:
		return a, b, "known_success", nil
	}
	return a, b, "unknown", nil
}

// tupleChannel names the state of the actor's callback channel apart from the
// outcome: "complete" when it ended with its completion recorded and its
// callback delivered, "incomplete" when it ended with the request outstanding
// or the callback missing, and "missing" when it reported nothing.
func tupleChannel(events []tupleEvent) string {
	for _, e := range events {
		if e.Event == "channel" {
			if len(e.Pending) == 0 && len(e.Callbacks) == 1 {
				return "complete"
			}
			return "incomplete"
		}
	}
	return "missing"
}

// tupleStopped reports whether the actor's stop was recorded: it wrote its
// stop record and the native process was joined with a complete, successful
// result. The client's termination does not show that a server-side pending
// write has settled, so this is recorded apart from any outcome.
func tupleStopped(events []tupleEvent, exitCode int, complete bool) bool {
	if !complete || exitCode != 0 {
		return false
	}
	for _, e := range events {
		if e.Event == "stop" {
			return true
		}
	}
	return false
}

// tupleObservation is the strict shape of the read phase. Every field is a
// pointer or a nil-able container, so that a missing or null field is told
// apart from an empty or zero one.
type tupleObservation struct {
	Error      *string            `json:"error"`
	Object     *string            `json:"object"`
	Pool       *string            `json:"pool"`
	PoolID     *int64             `json:"pool_id"`
	FSID       *string            `json:"fsid"`
	Client     *string            `json:"client"`
	Compound   *bool              `json:"compound"`
	Actions    []string           `json:"actions"`
	Operates   *int               `json:"operates"`
	OperateRet *int               `json:"operate_ret"`
	Subresults map[string]*int    `json:"subresults"`
	Size       *int               `json:"size"`
	BytesRead  *int               `json:"bytes_read"`
	EOF        *bool              `json:"eof"`
	SHA256     *string            `json:"sha256"`
	Xattrs     map[string]*string `json:"xattrs"`
	OMAP       map[string]*string `json:"omap"`
}

// tupleObserve validates one compound-read observation and returns the tuple it
// shows. Any error is an unproven observation, never evidence of a state.
func tupleObserve(data []byte, id tupleIdentity) (tupleValue, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil || raw == nil {
		return tupleValue{}, fmt.Errorf("unreadable observation: %v", err)
	}
	if actions, ok := raw["actions"]; !ok || string(actions) == "null" {
		return tupleValue{}, errors.New("observation lacks the list of read actions")
	}
	var obs tupleObservation
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&obs); err != nil {
		return tupleValue{}, fmt.Errorf("observation has a wrong shape: %v", err)
	}
	if obs.Error != nil {
		return tupleValue{}, fmt.Errorf("the observer reported an error: %s", *obs.Error)
	}
	if obs.Object == nil || obs.Pool == nil || obs.PoolID == nil || obs.FSID == nil || obs.Client == nil ||
		obs.Compound == nil || obs.Operates == nil || obs.OperateRet == nil || obs.Subresults == nil ||
		obs.Size == nil || obs.BytesRead == nil || obs.EOF == nil || obs.SHA256 == nil || obs.Xattrs == nil || obs.OMAP == nil {
		return tupleValue{}, errors.New("observation lacks a required field")
	}
	if *obs.Object != id.Object || *obs.Pool != id.Pool || *obs.PoolID != id.PoolID || *obs.FSID != id.FSID || !strings.HasPrefix(*obs.Client, "client.") {
		return tupleValue{}, errors.New("observation does not retain the expected object and store identity")
	}
	// The coherence fence: one compound read operation holding every action.
	if !*obs.Compound || *obs.Operates != 1 || !slices.Equal(obs.Actions, []string{"stat", "read", "getxattrs", "omap_get_vals_by_keys"}) {
		return tupleValue{}, errors.New("observation is not one compound read operation with every action")
	}
	if *obs.OperateRet != 0 || len(obs.Subresults) != len(obs.Actions) {
		return tupleValue{}, fmt.Errorf("compound read failed or has an unexpected subresult set (operate=%d)", *obs.OperateRet)
	}
	for _, action := range obs.Actions {
		if r := obs.Subresults[action]; r == nil || *r != 0 {
			return tupleValue{}, fmt.Errorf("compound read subresult %s is missing or failed", action)
		}
	}
	if !*obs.EOF || *obs.BytesRead != *obs.Size || *obs.Size < 0 {
		return tupleValue{}, errors.New("data read is not complete: size, bytes read and end of object disagree")
	}
	if raw, err := hex.DecodeString(*obs.SHA256); err != nil || len(raw) != 32 {
		return tupleValue{}, errors.New("observation hash is invalid")
	}
	value := tupleValue{Size: *obs.Size, SHA256: *obs.SHA256, Xattrs: map[string]string{}, OMAP: map[string]string{}}
	for field, source := range map[string]map[string]*string{"xattr": obs.Xattrs, "omap": obs.OMAP} {
		for key, v := range source {
			if v == nil {
				return tupleValue{}, fmt.Errorf("observation %s value for %q is null", field, key)
			}
			if _, err := hex.DecodeString(*v); err != nil {
				return tupleValue{}, fmt.Errorf("observation %s value for %q is not hex", field, key)
			}
		}
	}
	for k, v := range obs.Xattrs {
		value.Xattrs[k] = *v
	}
	for k, v := range obs.OMAP {
		value.OMAP[k] = *v
	}
	return value, nil
}

// tupleJudge is the oracle. A pre-submission control requires A; a known
// successful original completion requires B; an unknown result permits exactly
// A or B as whole tuples. Component-wise mixtures are no extra outcome.
func tupleJudge(events []tupleEvent, observation []byte, id tupleIdentity, stopped bool) tupleResult {
	a, b, outcome, err := tupleLedger(events)
	if err != nil {
		return tupleRefuse(outcome, "invalid ledger: %v", err)
	}
	observed, err := tupleObserve(observation, id)
	if err != nil {
		return tupleInconclusive(outcome, "%v", err)
	}
	switch outcome {
	case "pre_submission":
		if observed.equal(a) {
			return tupleResult{Status: "accepted", Outcome: outcome, Matched: "A"}
		}
	case "known_success":
		if observed.equal(b) {
			return tupleResult{Status: "accepted", Outcome: outcome, Matched: "B"}
		}
	case "unknown":
		if !stopped {
			return tupleInconclusive(outcome, "the actor's stop and join were not recorded, so a pending write may not have settled")
		}
		if observed.equal(a) {
			return tupleResult{Status: "accepted", Outcome: outcome, Matched: "A"}
		}
		if observed.equal(b) {
			return tupleResult{Status: "accepted", Outcome: outcome, Matched: "B"}
		}
	}
	return tupleRefuse(outcome, "the coherent observation %+v is not a permitted whole tuple", observed)
}

func tuplePhase(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, phase, pool, path string) ceph.NativeCommandResult {
	t.Helper()
	result, err := cluster.RunNativeCommand(ctx, client, tupleCommand(phase, pool, path))
	if err != nil || !result.Complete() || result.ExitCode != 0 {
		t.Fatalf("tuple phase %s error=%v exit=%d complete=%t stdout=%s stderr=%s", phase, err, result.ExitCode, result.Complete(), result.Stdout, result.Stderr)
	}
	return result
}

func tupleCommand(phase, pool, path string) ceph.NativeCommand {
	return ceph.NativeCommand{Argv: []string{"python3", "-c", tupleScript, phase, pool, path}, Timeout: 3 * time.Minute, MaxStdout: 1 << 20, MaxStderr: 1 << 20, Label: "compound tuple " + phase}
}

func tupleLedgerOf(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, pool, path string) []tupleEvent {
	t.Helper()
	var events []tupleEvent
	if err := json.Unmarshal(tuplePhase(t, ctx, cluster, client, "ledger", pool, path).Stdout, &events); err != nil {
		t.Fatal(err)
	}
	return events
}
