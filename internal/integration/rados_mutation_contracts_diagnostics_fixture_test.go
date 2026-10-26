//go:build all || (integration && features && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// This stub implements the exact python-rados boundary used by the maintained
// embedded late/view program. The trace/view implementation is not copied.
const radosMutationFixtureModule = `import json,os,time,ctypes
class Error(Exception):
    errno=5
class TimedOut(Error):
    errno=110
class ObjectNotFound(Error):
    errno=2
class NoData(Error):
    errno=61
class CleanupFault(Error):
    errno=5
LIBRADOS_CMPXATTR_OP_EQ=1
LIBRADOS_CREATE_EXCLUSIVE=1
LIBRADOS_CREATE_IDEMPOTENT=0
MAX_ERRNO=4095
objects={'late-truncate':{'data':b'0123456789'*100,'xattr':b'v1','version':1},'late-remove':{'data':b'0123456789'*100,'xattr':b'v1','version':2},'late-compound':{'data':b'0123456789'*100,'xattr':b'v1','version':3}}
calls=[];next_client=100;lag=int(os.environ.get('STUB_LAG','0'))
def record(kind,**fields):
    calls.append(dict(kind=kind,**fields))
    target=os.environ['STUB_CALLS'];temporary=target+'.tmp'
    with open(temporary,'w') as output:
        json.dump(calls,output);output.flush();os.fsync(output.fileno())
    hold=os.environ.get('STUB_HOLD_KIND','')
    if hold and kind==hold:
        # Park between the complete temporary snapshot and its publication so
        # a controller can cancel exactly here; the published trace must stay
        # the previous complete snapshot.
        with open(os.environ['STUB_HELD'],'w') as held: held.write('held')
        time.sleep(60)
    os.replace(temporary,target)
class WriteOpCtx:
    def __init__(self):self.kind=''
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def truncate(self,size):self.kind='truncate'
    def remove(self):self.kind='remove'
    def cmpext(self,*args):pass
    def write_full(self,*args):self.kind='compound'
    def set_xattr(self,*args):pass
class Rados:
    def __init__(self,**kwargs):
        global next_client
        next_client+=1;self.ident=next_client;self.conf=kwargs['conf']
        record('client',client=self.ident,conf=self.conf)
    def connect(self):pass
    def get_instance_id(self):return self.ident
    def open_ioctx(self,pool):return IO(self,pool)
    def shutdown(self):record('shutdown',client=self.ident)
class IO:
    def __init__(self,client,pool):self.client=client;self.pool=pool;self.version=0
    def set_namespace(self,namespace):self.namespace=namespace
    def operate_write_op(self,op,name):
        record('mutation',client=self.client.ident,operation=op.kind,object=name)
        if op.kind=='truncate':objects[name].update(data=b'0123456789')
        if op.kind=='remove':del objects[name]
        if op.kind=='compound':objects[name].update(data=b'compound',xattr=b'v2')
        raise TimedOut('original mutation timeout')
    def fault(self,operation,name):
        record('read',client=self.client.ident,operation=operation,object=name)
        fault=os.environ.get('STUB_FAULT','')
        if name=='late-compound' and fault==operation:raise TimedOut('original reader timeout')
        if name=='late-compound' and fault=='blocked' and operation=='stat':ctypes.CDLL(None).usleep(2000000)
    def stat(self,name):
        global lag
        self.fault('stat',name)
        if name not in objects:raise ObjectNotFound('absent')
        item=objects[name];self.version=item['version']
        if name=='late-truncate' and lag>0:lag-=1;return 1000,None
        return len(item['data']),None
    def read(self,name,size,offset):self.fault('read',name);return objects[name]['data'][offset:offset+size]
    def get_xattr(self,name,key):
        self.fault('get_xattr',name)
        if os.environ.get('STUB_FAULT')=='nodata':raise NoData('absent optional xattr')
        return objects[name]['xattr']
    def get_last_version(self):return self.version
    def close(self):
        record('close',client=self.client.ident)
        if self.client.ident==102 and os.environ.get('STUB_CLEANUP')=='fail':raise CleanupFault('cleanup failed')
`

type radosMutationFixtureRun struct {
	checkpoint *radosMutationCheckpoint
	raw        []byte
	calls      []map[string]any
	output     string
	err        error
	contextErr error
}

func runRadosMutationFixture(t *testing.T, phase, fault, cleanup string, lag int, writeFailure bool, limit time.Duration) radosMutationFixtureRun {
	t.Helper()
	return runRadosMutationFixtureHeld(t, phase, fault, cleanup, lag, writeFailure, limit, "")
}

// runRadosMutationFixtureHeld additionally parks the stub inside the
// publication of its first call record of kind hold, then cancels the owned
// child there. An empty hold is the ordinary run.
func runRadosMutationFixtureHeld(t *testing.T, phase, fault, cleanup string, lag int, writeFailure bool, limit time.Duration, hold string) radosMutationFixtureRun {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("native diagnostic generation identity uses Linux /proc")
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "rados.py"), []byte(radosMutationFixtureModule), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "checkpoint.json")
	if writeFailure {
		path = filepath.Join(dir, "missing", "checkpoint.json")
	}
	token := "fixture-owned-token"
	args := []string{"-c", radosMutationContractScript, phase, "owned-pool", "owned-namespace"}
	if phase == "late" {
		args = append(args, "2", "3", path, token)
	} else {
		args = append(args, path, token)
	}
	ctx, cancel := context.WithTimeout(t.Context(), limit)
	defer cancel()
	cmd := exec.CommandContext(ctx, python, args...)
	cmd.Env = append(os.Environ(), "PYTHONPATH="+dir, "STUB_CALLS="+filepath.Join(dir, "calls.json"), "STUB_FAULT="+fault, "STUB_CLEANUP="+cleanup, "STUB_LAG="+strconv.Itoa(lag))
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	var runErr error
	if hold != "" {
		cmd.Env = append(cmd.Env, "STUB_HOLD_KIND="+hold, "STUB_HELD="+filepath.Join(dir, "held"))
		runErr = cmd.Start()
		if runErr == nil {
			for ctx.Err() == nil {
				if _, statErr := os.Stat(filepath.Join(dir, "held")); statErr == nil {
					cancel()
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			runErr = cmd.Wait()
		}
	} else if fault == "blocked" {
		runErr = cmd.Start()
		if runErr == nil {
			for ctx.Err() == nil {
				raw, readErr := os.ReadFile(path)
				if readErr == nil && strings.Contains(string(raw), `"object":"late-compound","operation":"stat","state":"started"`) {
					cancel()
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			runErr = cmd.Wait()
		}
	} else {
		runErr = cmd.Run()
	}
	got := radosMutationFixtureRun{output: output.String(), err: runErr, contextErr: ctx.Err()}
	if calls, err := os.ReadFile(filepath.Join(dir, "calls.json")); err == nil {
		if err := json.Unmarshal(calls, &got.calls); err != nil {
			t.Fatal(err)
		}
	}
	raw, readErr := os.ReadFile(path)
	got.raw = raw
	if readErr == nil {
		got.checkpoint, err = decodeRadosMutationCheckpoint(raw, phase, "owned-pool", "owned-namespace", path, token)
		if err != nil {
			t.Fatal(err)
		}
	}
	return got
}

func assertRadosMutationCalls(t *testing.T, run radosMutationFixtureRun, want int) {
	t.Helper()
	var names []string
	for _, call := range run.calls {
		if call["kind"] == "mutation" {
			names = append(names, call["operation"].(string))
		}
	}
	if len(names) != want {
		t.Fatalf("mutation calls %v want %d", names, want)
	}
	if want == 3 && strings.Join(names, ",") != "truncate,remove,compound" {
		t.Fatalf("mutations replayed/reordered: %v", names)
	}
}

func TestRadosMutationCheckpointPreservesPrimaryFailure(t *testing.T) {
	for _, fault := range []string{"stat", "read", "get_xattr"} {
		t.Run(fault, func(t *testing.T) {
			run := runRadosMutationFixture(t, "late", fault, "", 0, false, 5*time.Second)
			if run.err == nil || !strings.Contains(run.output, "rados.TimedOut: original reader timeout") {
				t.Fatalf("primary failure lost: %v %s", run.err, run.output)
			}
			assertRadosMutationCalls(t, run, 3)
			got := run.checkpoint
			if got == nil || got.Stage != "observing" || len(got.Mutations) != 3 {
				t.Fatalf("mutation partial receipt lost: %+v", got)
			}
			// JSON names differ from Go's default ErrorType field spelling.
			var event map[string]any
			if err := json.Unmarshal(got.Events[len(got.Events)-1], &event); err != nil {
				t.Fatal(err)
			}
			if event["operation"] != fault || event["object"] != "late-compound" || event["state"] != "error" || event["errno"] != float64(110) || event["error_type"] != "TimedOut" {
				t.Fatalf("failed native call not distinguished: %s", got.Events[len(got.Events)-1])
			}
			if err := completeRadosMutationCheckpoint(got); err == nil {
				t.Fatal("failed reader accepted as complete")
			}
		})
	}
}

func TestRadosMutationCheckpointBeforeBlockedCall(t *testing.T) {
	run := runRadosMutationFixture(t, "late", "blocked", "", 0, false, 5*time.Second)
	if run.err == nil || run.contextErr == nil {
		t.Fatalf("blocked fixture not bounded: %v", run.err)
	}
	assertRadosMutationCalls(t, run, 3)
	got := run.checkpoint
	if got == nil || len(got.Mutations) != 3 || got.Stage != "observing" {
		t.Fatalf("missing pre-call checkpoint: %+v", got)
	}
	var event map[string]any
	if err := json.Unmarshal(got.Events[len(got.Events)-1], &event); err != nil {
		t.Fatal(err)
	}
	if event["object"] != "late-compound" || event["operation"] != "stat" || event["state"] != "started" {
		t.Fatalf("blocked call mislabeled: %v", event)
	}
}

// The stub's call trace is a second JSON publication next to the diagnostic
// checkpoint. Cancelling the child inside that publication must leave the
// previous complete trace, never a truncated one.
func TestRadosMutationCallTraceCancelledDuringPublication(t *testing.T) {
	run := runRadosMutationFixtureHeld(t, "late", "", "", 0, false, 10*time.Second, "read")
	if run.err == nil || run.contextErr == nil {
		t.Fatalf("held fixture not cancelled by the controller: %v %v", run.err, run.contextErr)
	}
	assertRadosMutationCalls(t, run, 3)
	for _, call := range run.calls {
		if call["kind"] == "read" {
			t.Fatalf("unpublished read record visible: %v", call)
		}
	}
	got := run.checkpoint
	if got == nil || got.Stage != "observing" || len(got.Mutations) != 3 {
		t.Fatalf("pre-call checkpoint lost: %+v", got)
	}
	var event map[string]any
	if err := json.Unmarshal(got.Events[len(got.Events)-1], &event); err != nil {
		t.Fatal(err)
	}
	if event["state"] != "started" {
		t.Fatalf("held call not recorded as started before dispatch: %v", event)
	}
}

func TestRadosMutationDiagnosticFailuresDoNotReplacePrimary(t *testing.T) {
	t.Run("write unavailable", func(t *testing.T) {
		run := runRadosMutationFixture(t, "late", "stat", "", 0, true, 5*time.Second)
		if run.err == nil || !strings.Contains(run.output, "rados.TimedOut: original reader timeout") || !strings.Contains(run.output, "checkpoint write failed") || run.checkpoint != nil {
			t.Fatalf("diagnostic failure replaced native error: %v %s", run.err, run.output)
		}
		assertRadosMutationCalls(t, run, 3)
	})
	t.Run("cleanup fault with primary", func(t *testing.T) {
		run := runRadosMutationFixture(t, "late", "stat", "fail", 0, false, 5*time.Second)
		if run.err == nil || !strings.Contains(run.output, "rados.TimedOut: original reader timeout") || strings.Contains(run.output, "rados.CleanupFault: cleanup failed") || len(run.checkpoint.CleanupErrors) != 1 {
			t.Fatalf("cleanup replaced native error: %v %s", run.err, run.output)
		}
		assertRadosMutationCalls(t, run, 3)
	})
	t.Run("cleanup fault without primary", func(t *testing.T) {
		run := runRadosMutationFixture(t, "late", "", "fail", 0, false, 5*time.Second)
		if run.err == nil || !strings.Contains(run.output, "rados.CleanupFault: cleanup failed") || len(run.checkpoint.CleanupErrors) != 1 {
			t.Fatalf("cleanup failure hidden: %v %s", run.err, run.output)
		}
	})
}

func TestRadosMutationCheckpointCompleteAndBounded(t *testing.T) {
	run := runRadosMutationFixture(t, "late", "", "", 2, false, 5*time.Second)
	if run.err != nil {
		t.Fatalf("positive control: %v %s", run.err, run.output)
	}
	assertRadosMutationCalls(t, run, 3)
	if err := completeRadosMutationCheckpoint(run.checkpoint); err != nil {
		t.Fatal(err)
	}
	if len(run.raw) > radosMutationTraceLimit || len(run.checkpoint.Events) > 48 || run.checkpoint.Dropped == 0 {
		t.Fatalf("ring/byte loss not explicit: %+v", run.checkpoint)
	}
	if strings.Contains(string(run.raw), "636f6d706f756e64") || strings.Contains(string(run.raw), `"data"`) {
		t.Fatal("object payload dumped into diagnostic checkpoint")
	}
	raw := append([]byte(nil), run.raw...)
	var altered map[string]any
	if err := json.Unmarshal(raw, &altered); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"token", "phase", "pool", "namespace", "path", "start_ticks"} {
		t.Run("wrong "+field, func(t *testing.T) {
			saved := altered[field]
			altered[field] = "wrong"
			changed, _ := json.Marshal(altered)
			altered[field] = saved
			if _, err := decodeRadosMutationCheckpoint(changed, "late", "owned-pool", "owned-namespace", run.checkpoint.Path, run.checkpoint.Token); err == nil {
				t.Fatalf("stale/other %s adopted", field)
			}
		})
	}
	for _, change := range []string{"client changed", "identity unavailable", "loss count", "mutation missing"} {
		t.Run(change, func(t *testing.T) {
			copy := *run.checkpoint
			copy.Events = append([]json.RawMessage(nil), copy.Events...)
			switch change {
			case "loss count":
				copy.Dropped++
			case "mutation missing":
				copy.Mutations = copy.Mutations[:2]
			default:
				var event map[string]any
				if err := json.Unmarshal(copy.Events[0], &event); err != nil {
					t.Fatal(err)
				}
				if change == "client changed" {
					event["observed_client_id"] = 999
					event["client_generation_changed"] = true
				} else {
					delete(event, "observed_client_id")
					event["identity_error"] = "unavailable"
				}
				copy.Events[0], _ = json.Marshal(event)
			}
			if completeRadosMutationCheckpoint(&copy) == nil {
				t.Fatal("incomplete/mismatched metadata qualified as complete")
			}
		})
	}
	if _, err := decodeRadosMutationCheckpoint(make([]byte, radosMutationTraceLimit+1), "late", "owned-pool", "owned-namespace", run.checkpoint.Path, run.checkpoint.Token); err == nil {
		t.Fatal("oversized receipt adopted")
	}
}

type radosCheckpointContainer struct {
	testcontainers.Container
	copy func(context.Context, string) (io.ReadCloser, error)
}

func (c radosCheckpointContainer) CopyFileFromContainer(ctx context.Context, path string) (io.ReadCloser, error) {
	return c.copy(ctx, path)
}

type radosCheckpointBlockedStream struct {
	closed chan struct{}
	once   sync.Once
}

func (r *radosCheckpointBlockedStream) Read([]byte) (int, error) { <-r.closed; return 0, io.EOF }
func (r *radosCheckpointBlockedStream) Close() error {
	r.once.Do(func() { close(r.closed) })
	return nil
}

type radosCheckpointErrorStream struct{ data []byte }

func (r *radosCheckpointErrorStream) Read(p []byte) (int, error) {
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, io.ErrUnexpectedEOF
}
func (*radosCheckpointErrorStream) Close() error { return nil }

func TestRadosMutationCheckpointCaptureBoundaries(t *testing.T) {
	run := runRadosMutationFixture(t, "late", "stat", "", 0, false, 5*time.Second)
	t.Run("unavailable copy", func(t *testing.T) {
		ctr := radosCheckpointContainer{copy: func(context.Context, string) (io.ReadCloser, error) { return nil, errors.New("copy unavailable") }}
		if got, err := captureRadosMutationCheckpoint(t.Context(), ctr, "late", "owned-pool", "owned-namespace", run.checkpoint.Path, run.checkpoint.Token); err == nil || got != nil {
			t.Fatalf("unavailable read adopted: %+v %v", got, err)
		}
	})
	t.Run("valid prefix and read failure", func(t *testing.T) {
		ctr := radosCheckpointContainer{copy: func(context.Context, string) (io.ReadCloser, error) {
			return &radosCheckpointErrorStream{data: run.raw}, nil
		}}
		if got, err := captureRadosMutationCheckpoint(t.Context(), ctr, "late", "owned-pool", "owned-namespace", run.checkpoint.Path, run.checkpoint.Token); err == nil || got != nil {
			t.Fatalf("incomplete stream adopted: %+v %v", got, err)
		}
	})
	t.Run("blocked HTTP body closes and joins", func(t *testing.T) {
		stream := &radosCheckpointBlockedStream{closed: make(chan struct{})}
		ctr := radosCheckpointContainer{copy: func(context.Context, string) (io.ReadCloser, error) { return stream, nil }}
		ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
		defer cancel()
		if got, err := captureRadosMutationCheckpoint(ctx, ctr, "late", "owned-pool", "owned-namespace", run.checkpoint.Path, run.checkpoint.Token); err == nil || got != nil {
			t.Fatalf("blocked stream adopted: %+v %v", got, err)
		}
		select {
		case <-stream.closed:
		default:
			t.Fatal("owned stream not closed")
		}
	})
	t.Run("matching body after deadline refused", func(t *testing.T) {
		ctr := radosCheckpointContainer{copy: func(ctx context.Context, _ string) (io.ReadCloser, error) {
			<-ctx.Done()
			return io.NopCloser(strings.NewReader(string(run.raw))), nil
		}}
		ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
		defer cancel()
		if got, err := captureRadosMutationCheckpoint(ctx, ctr, "late", "owned-pool", "owned-namespace", run.checkpoint.Path, run.checkpoint.Token); err == nil || got != nil {
			t.Fatalf("late matching stream adopted: %+v %v", got, err)
		}
	})
	t.Run("byte cap", func(t *testing.T) {
		ctr := radosCheckpointContainer{copy: func(context.Context, string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(strings.Repeat("x", radosMutationTraceLimit+1))), nil
		}}
		if got, err := captureRadosMutationCheckpoint(t.Context(), ctr, "late", "owned-pool", "owned-namespace", run.checkpoint.Path, run.checkpoint.Token); err == nil || got != nil {
			t.Fatalf("oversized body adopted: %+v %v", got, err)
		}
	})
	t.Run("failure capture gets fresh context", func(t *testing.T) {
		called := false
		ctr := radosCheckpointContainer{copy: func(ctx context.Context, _ string) (io.ReadCloser, error) {
			called = true
			if ctx.Err() != nil {
				t.Fatal("expired operation context reused")
			}
			return io.NopCloser(strings.NewReader(string(run.raw))), nil
		}}
		parent, cancel := context.WithCancel(t.Context())
		cancel()
		captureRadosMutationFailure(t, parent, ctr, "owned-pool", "owned-namespace", run.checkpoint.Path, run.checkpoint.Token)
		if !called {
			t.Fatal("failure checkpoint not attempted")
		}
	})
}

// Extract the maintained definitions, not a second implementation of view.
// The driver only supplies one stub ioctx so optional absence can be tested
// independently of the late convergence predicate's requirement for xattr v2.
func TestRadosMutationNativeViewAbsenceClassifications(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("native diagnostic generation identity uses Linux /proc")
	}
	index := strings.Index(radosMutationContractScript, "clients=[]; ioctxs=[]")
	if index < 0 {
		t.Fatal("maintained view definitions unavailable")
	}
	driver := radosMutationContractScript[:index] + `
c=connected('10','reader');i=c.open_ioctx(pool);i.set_namespace(namespace);diag_opened(c,i)
try:
 value=view(i,sys.argv[8]);diag_stage('complete');print(json.dumps(value))
finally:
 i.close();c.shutdown()
`
	for _, tc := range []struct {
		name, fault, object string
		exists              bool
		errno               int
	}{
		{"missing optional xattr", "nodata", "late-compound", true, 61},
		{"absent object", "", "absent", false, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			python, err := exec.LookPath("python3")
			if err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "rados.py"), []byte(radosMutationFixtureModule), 0600); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "checkpoint.json")
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, python, "-c", driver, "late", "owned-pool", "owned-namespace", "2", "3", path, "owned-token", tc.object)
			cmd.Env = append(os.Environ(), "PYTHONPATH="+dir, "STUB_CALLS="+filepath.Join(dir, "calls.json"), "STUB_FAULT="+tc.fault)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("actual view: %v %s", err, out)
			}
			var view struct {
				Exists bool    `json:"exists"`
				Xattr  *string `json:"xattr"`
			}
			if json.Unmarshal(out, &view) != nil || view.Exists != tc.exists || view.Xattr != nil {
				t.Fatalf("absence classification: %s", out)
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			got, err := decodeRadosMutationCheckpoint(raw, "late", "owned-pool", "owned-namespace", path, "owned-token")
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, raw := range got.Events {
				var event map[string]any
				if json.Unmarshal(raw, &event) != nil {
					t.Fatal("bad event")
				}
				if event["state"] == "error" && event["errno"] == float64(tc.errno) {
					found = true
				}
			}
			if !found {
				t.Fatalf("absence/native error observation lost: %s", raw)
			}
		})
	}
}
