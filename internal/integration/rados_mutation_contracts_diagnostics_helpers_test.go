//go:build all || (integration && features)

package integration_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

const radosMutationTraceLimit = 32768

// This instrumentation is shared by the real embedded client and its fixture
// subprocesses. It never writes object bytes, xattr values or credentials.
const radosMutationDiagnosticsScript = `import os,json,sys,time
_diag=None
_diag_io={}
_diag_clients={}
def diag_emit_stderr(value):
    try:
        sys.stderr.write('RADOS_MUTATION_DIAGNOSTIC '+json.dumps(value,separators=(',',':'))+'\n');sys.stderr.flush()
    except Exception:
        pass
def diag_flush():
    if _diag is None: return
    try:
        encoded=json.dumps(_diag,separators=(',',':')).encode()
        while len(encoded)>32768 and _diag['events']:
            _diag['events'].pop(0);_diag['dropped_events']+=1
            encoded=json.dumps(_diag,separators=(',',':')).encode()
        if len(encoded)>32768: raise ValueError('checkpoint byte budget exceeded')
        temp=_diag['path']+'.tmp'
        with open(temp,'wb') as stream:
            stream.write(encoded);stream.flush();os.fsync(stream.fileno())
        os.replace(temp,_diag['path'])
    except Exception as error:
        _diag['write_failures']+=1
        if _diag['write_failures']==1: diag_emit_stderr({'incomplete':'checkpoint write failed','type':type(error).__name__})
def diag_start(phase,pool,namespace,path,token):
    global _diag
    start_ticks=''
    try:
        with open('/proc/self/stat') as stream: stat=stream.read()
        start_ticks=stat[stat.rfind(')')+2:].split()[19]
    except Exception:
        pass
    _diag={'schema':1,'byte_limit':32768,'event_limit':48,'phase':phase,'pool':pool,'namespace':namespace,'path':path,'token':token,
           'pid':os.getpid(),'start_ticks':start_ticks,
           'stage':'connecting','seq':0,'events':[],'dropped_events':0,'write_failures':0,
           'clients':[],'mutations':[],'cleanup_errors':[]}
    diag_flush()
def diag_stage(stage):
    if _diag is not None: _diag['stage']=stage;diag_flush()
def diag_connected(client,label,timeout,mon_timeout):
    if _diag is None: return
    record={'label':label,'osd_timeout':timeout,'mon_timeout':mon_timeout}
    try: record['client_id']=int(client.get_instance_id())
    except Exception as error: record['identity_error']=type(error).__name__
    _diag_clients[id(client)]=record;_diag['clients'].append(record);diag_flush()
def diag_opened(client,io):
    if _diag is not None:
        _diag_io[id(io)]={'ioctx_seq':len(_diag_io)+1,'client':_diag_clients.get(id(client),{}),'handle':client}
def diag_event(io,name,operation,state,extra=None):
    if _diag is None: return None
    actor=_diag_io.get(id(io),{})
    event={'seq':_diag['seq']+1,'wall':time.time(),'monotonic_ns':time.monotonic_ns(),
           'object':name,'operation':operation,'state':state,'ioctx_seq':actor.get('ioctx_seq'),
           'client_id':actor.get('client',{}).get('client_id')}
    _diag['seq']=event['seq']
    try:
        event['observed_client_id']=int(actor['handle'].get_instance_id())
        event['client_generation_changed']=event['observed_client_id']!=event['client_id']
    except Exception as error: event['identity_error']=type(error).__name__
    if extra: event.update(extra)
    _diag['events'].append(event)
    if len(_diag['events'])>48: _diag['events'].pop(0);_diag['dropped_events']+=1
    diag_flush()
    return event['monotonic_ns']
def diag_call(io,name,operation,call):
    if _diag is None: return call()
    started=diag_event(io,name,operation,'started')
    try: result=call()
    except Exception as error:
        diag_event(io,name,operation,'error',{'errno':getattr(error,'errno',None),'error_type':type(error).__name__,
                   'elapsed_ns':time.monotonic_ns()-started if started is not None else None})
        raise
    meta={}
    if operation=='stat': meta['size']=result[0]
    elif operation in ('read','get_xattr'): meta['bytes']=len(result)
    elif operation=='get_last_version': meta['version']=result
    if started is not None: meta['elapsed_ns']=time.monotonic_ns()-started
    diag_event(io,name,operation,'returned',meta)
    return result
def diag_mutation(io,name,key,errno,completed):
    if _diag is not None:
        _diag['mutations'].append({'operation':key,'object':name,'errno':errno,'completed':completed})
        diag_event(io,name,key,'returned',{'errno':errno})
def diag_cleanup(kind,error):
    if _diag is not None:
        _diag['cleanup_errors'].append({'kind':kind,'type':type(error).__name__,'errno':getattr(error,'errno',None)})
        diag_flush()
`

type radosMutationCheckpoint struct {
	Schema        int               `json:"schema"`
	ByteLimit     int               `json:"byte_limit"`
	EventLimit    int               `json:"event_limit"`
	CapturedBytes int               `json:"-"`
	Phase         string            `json:"phase"`
	Pool          string            `json:"pool"`
	Namespace     string            `json:"namespace"`
	Path          string            `json:"path"`
	Token         string            `json:"token"`
	PID           int               `json:"pid"`
	StartTicks    string            `json:"start_ticks"`
	Stage         string            `json:"stage"`
	Seq           int               `json:"seq"`
	Events        []json.RawMessage `json:"events"`
	Dropped       int               `json:"dropped_events"`
	WriteFailures int               `json:"write_failures"`
	Clients       []json.RawMessage `json:"clients"`
	Mutations     []json.RawMessage `json:"mutations"`
	CleanupErrors []json.RawMessage `json:"cleanup_errors"`
}

func decodeRadosMutationCheckpoint(raw []byte, phase, pool, namespace, path, token string) (*radosMutationCheckpoint, error) {
	if len(raw) > radosMutationTraceLimit {
		return nil, fmt.Errorf("checkpoint exceeds byte limit")
	}
	var got radosMutationCheckpoint
	if err := json.Unmarshal(raw, &got); err != nil {
		return nil, err
	}
	start, err := strconv.ParseUint(got.StartTicks, 10, 64)
	got.CapturedBytes = len(raw)
	if err != nil || start == 0 || got.PID <= 0 || got.Schema != 1 || got.ByteLimit != radosMutationTraceLimit || got.EventLimit != 48 || got.Phase != phase || got.Pool != pool || got.Namespace != namespace || got.Path != path || got.Token != token || token == "" || got.Seq < 0 || got.Dropped < 0 || got.WriteFailures < 0 || len(got.Events) > 48 || len(got.Mutations) > 3 {
		return nil, fmt.Errorf("checkpoint identity or bounds mismatch")
	}
	return &got, nil
}

func completeRadosMutationCheckpoint(got *radosMutationCheckpoint) error {
	if got == nil || got.Stage != "complete" || got.WriteFailures != 0 || len(got.CleanupErrors) != 0 || len(got.Mutations) != 3 {
		return fmt.Errorf("late checkpoint incomplete")
	}
	ids := map[uint64]bool{}
	for index, raw := range got.Clients {
		var client struct {
			Label      string `json:"label"`
			ID         uint64 `json:"client_id"`
			Timeout    string `json:"osd_timeout"`
			MonTimeout string `json:"mon_timeout"`
		}
		if err := json.Unmarshal(raw, &client); err != nil {
			return err
		}
		if index >= 3 || client.Label != []string{"writer", "reader", "late-writer"}[index] || client.ID == 0 || ids[client.ID] || client.MonTimeout != "10" || client.Timeout != []string{"10", "10", "2"}[index] {
			return fmt.Errorf("late client identity/config incomplete")
		}
		ids[client.ID] = true
	}
	if len(ids) != 3 {
		return fmt.Errorf("late clients missing")
	}
	for index, raw := range got.Mutations {
		var mutation struct {
			Operation string  `json:"operation"`
			Object    string  `json:"object"`
			Errno     int     `json:"errno"`
			Completed float64 `json:"completed"`
		}
		if err := json.Unmarshal(raw, &mutation); err != nil {
			return err
		}
		name := []string{"truncate", "remove", "compound"}[index]
		if mutation.Operation != name || mutation.Object != "late-"+name || mutation.Errno != 110 || mutation.Completed <= 0 {
			return fmt.Errorf("late mutation checkpoint contradicts timeout contract")
		}
	}
	if got.Dropped+len(got.Events) != got.Seq {
		return fmt.Errorf("trace loss counts do not join")
	}
	for _, raw := range got.Events {
		var event struct {
			Client   uint64 `json:"client_id"`
			Observed uint64 `json:"observed_client_id"`
			Changed  bool   `json:"client_generation_changed"`
			Error    string `json:"identity_error"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return err
		}
		if !ids[event.Client] || event.Observed != event.Client || event.Changed || event.Error != "" {
			return fmt.Errorf("trace client generation unavailable or changed")
		}
	}
	return nil
}

func allocateRadosMutationCheckpoint() (string, string, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return "", "", err
	}
	token := hex.EncodeToString(nonce)
	return "/tmp/rados-mutation-" + token + ".json", token, nil
}

// capture reads only the owned file; failure is diagnostic unavailability. It
// never changes the primary command result or repeats a mutation.
func captureRadosMutationCheckpoint(ctx context.Context, client testcontainers.Container, phase, pool, namespace, path, token string) (*radosMutationCheckpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("checkpoint unavailable: %w", err)
	}
	stream, err := client.CopyFileFromContainer(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("checkpoint unavailable: %w", err)
	}
	if stream == nil {
		return nil, fmt.Errorf("checkpoint unavailable: no stream")
	}
	defer func() { _ = stream.Close() }()
	closed := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = stream.Close(); close(closed) })
	defer func() {
		if !stop() {
			<-closed
		}
	}()
	raw, readErr := io.ReadAll(io.LimitReader(stream, radosMutationTraceLimit+1))
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("checkpoint read incomplete: %w", err)
	}
	if readErr != nil {
		return nil, fmt.Errorf("checkpoint read incomplete: %w", readErr)
	}
	return decodeRadosMutationCheckpoint(raw, phase, pool, namespace, path, token)
}

func logRadosMutationCheckpoint(t *testing.T, got *radosMutationCheckpoint, err error) {
	t.Helper()
	if err != nil {
		t.Logf("RADOS_MUTATION_CHECKPOINT incomplete=%q", err)
		return
	}
	// Path/token are transport ownership fields rather than observations.
	copy := *got
	copy.Path = ""
	copy.Token = ""
	encoded, _ := json.Marshal(copy)
	t.Logf("RADOS_MUTATION_CHECKPOINT captured_bytes=%d byte_limit=%d %s", got.CapturedBytes, radosMutationTraceLimit, encoded)
}

// No new native process is launched after failure: its mandatory independent
// stop/inspection allowance can outlive the caller's diagnostic time budget.
// The pinned archive-copy API carries ctx into its HTTP body. Closing and
// joining the reader is still settlement, not a universal backend wall clock.
func captureRadosMutationFailure(t *testing.T, parent context.Context, client testcontainers.Container, pool, namespace, path, token string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 8*time.Second)
	defer cancel()
	got, err := captureRadosMutationCheckpoint(ctx, client, "late", pool, namespace, path, token)
	logRadosMutationCheckpoint(t, got, err)
}
