//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

const ecFourTwoActorScript = `import hashlib,json,os,sys,time,rados
pool,path,size=sys.argv[1],sys.argv[2],int(sys.argv[3])
assert 0<size<=524319
os.mkdir(path)
def payload(value): return bytes((i*13+value*41)%251 for i in range(size))
def digest(data): return hashlib.sha256(data).hexdigest()
def connect():
 c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'rados_osd_op_timeout':'5','rados_mon_op_timeout':'10'});c.connect();return c
writer,reader=connect(),connect()
identity=dict(pid=os.getpid(),writer=writer.get_instance_id(),reader=reader.get_instance_id())
assert identity['writer']!=identity['reader']
def emit(seq,phase,**fields):
 out=dict(identity,seq=seq,phase=phase,**fields)
 temporary=path+'/'+str(seq)+'.tmp'
 with open(temporary,'w') as f:json.dump(out,f);f.flush();os.fsync(f.fileno())
 os.replace(temporary,path+'/'+str(seq)+'.json')
def attempt(fn):
 started=time.monotonic_ns()
 try:fn();return dict(outcome='acknowledged',errno=0,elapsed_ns=time.monotonic_ns()-started)
 except rados.Error as e:return dict(outcome='unknown',errno=getattr(e,'errno',None),error=type(e).__name__,elapsed_ns=time.monotonic_ns()-started)
seq=0
try:
 with writer.open_ioctx(pool) as a,reader.open_ioctx(pool) as b:
  a.write_full('four-two',payload(0));snap=a.create_self_managed_snap()
  def views(value):
   b.set_read(rados.LIBRADOS_SNAP_HEAD);head=b.read('four-two',size+1,0);hv=b.get_last_version()
   b.set_read(snap);clone=b.read('four-two',size+1,0);sv=b.get_last_version();b.set_read(rados.LIBRADOS_SNAP_HEAD)
   assert head==payload(value) and clone==payload(0),(len(head),len(clone),value)
   return dict(size=len(head),snap=snap,head_sha256=digest(head),head_version=hv,snap_sha256=digest(clone),snap_version=sv,value=value)
  try:b.stat('four-two-unknown');raise AssertionError('uncertain object already exists')
  except rados.ObjectNotFound:pass
  emit(0,'seed',**views(0))
  for line in sys.stdin:
   command=json.loads(line);seq+=1
   assert seq<=2 and command['seq']==seq,command
   if command['phase']=='write':
    value=command['value'];assert value==seq
    a.set_self_managed_snap_write([snap])
    try:a.write_full('four-two',payload(value))
    finally:a.set_self_managed_snap_write([])
    emit(seq,'ack',**views(value))
   elif command['phase']=='blocked':
    assert seq==2
    read=attempt(lambda:b.read('four-two',size+1,0))
    write=attempt(lambda:a.write_full('four-two-unknown',payload(2)))
    emit(seq,'blocked',read=read,write=write)
   else:raise AssertionError(command)
finally:
 writer.shutdown();reader.shutdown()
print(json.dumps(dict(identity,shutdown=True,commands=seq)))
`

const ecFourTwoVerifyScript = `import hashlib,json,sys,rados
pool,size,snap,value=sys.argv[1],int(sys.argv[2]),int(sys.argv[3]),int(sys.argv[4])
def payload(v):return bytes((i*13+v*41)%251 for i in range(size))
def digest(data):return hashlib.sha256(data).hexdigest()
c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'rados_osd_op_timeout':'15','rados_mon_op_timeout':'10'});c.connect()
try:
 with c.open_ioctx(pool) as b:
  b.set_read(rados.LIBRADOS_SNAP_HEAD);head=b.read('four-two',size+1,0);hv=b.get_last_version();hs,_=b.stat('four-two')
  b.set_read(snap);clone=b.read('four-two',size+1,0);sv=b.get_last_version();ss,_=b.stat('four-two');b.set_read(rados.LIBRADOS_SNAP_HEAD)
  assert head==payload(value) and clone==payload(0) and hs==ss==size,(len(head),len(clone),hs,ss)
  out=dict(reader=c.get_instance_id(),snap=snap,size=size,head_sha256=digest(head),head_version=hv,snap_sha256=digest(clone),snap_version=sv)
  try:
   us,_=b.stat('four-two-unknown');unknown=b.read('four-two-unknown',size+1,0)
   assert us==size and unknown==payload(2),(us,len(unknown))
   out['unknown']=dict(present=True,size=us,sha256=digest(unknown),version=b.get_last_version())
  except rados.ObjectNotFound:out['unknown']=dict(present=False)
  print(json.dumps(out))
finally:c.shutdown()
`

type ecFourTwoOutcome struct {
	result ceph.NativeCommandResult
	err    error
}
type ecFourTwoActor struct {
	input    *ceph.NativeInput
	done     chan ecFourTwoOutcome
	finished *ecFourTwoOutcome
	written  int64
}

func startECFourTwoActor(ctx context.Context, cluster *ceph.Container, client testcontainers.Container, pool, path string, size int) *ecFourTwoActor {
	a := &ecFourTwoActor{input: ceph.NewNativeInput(), done: make(chan ecFourTwoOutcome, 1)}
	go func() {
		r, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{Argv: []string{"python3", "-c", ecFourTwoActorScript, pool, path, strconv.Itoa(size)}, Timeout: 12 * time.Minute, MaxStdout: 1 << 16, MaxStderr: 1 << 16, Label: "retained 4+2 actor", Input: a.input, MaxInput: 4096})
		a.done <- ecFourTwoOutcome{r, err}
	}()
	return a
}

func (a *ecFourTwoActor) send(ctx context.Context, seq, value int, phase string) error {
	data, err := json.Marshal(map[string]any{"seq": seq, "value": value, "phase": phase})
	if err != nil {
		return err
	}
	data = append(data, '\n')
	// A write whose delivery is uncertain is never retried.
	if err := a.input.Write(ctx, data); err != nil {
		return err
	}
	a.written += int64(len(data))
	return nil
}

func (a *ecFourTwoActor) finish(ctx context.Context) error {
	if a.finished == nil {
		if err := a.input.Close(); err != nil {
			return err
		}
		select {
		case out := <-a.done:
			a.finished = &out
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	out := a.finished
	var unstopped *ceph.NativeCommandUnstoppedError
	if errors.As(out.err, &unstopped) {
		return errors.Join(out.err, unstopped.Stop(ctx))
	}
	if out.err != nil || !out.result.Complete() || out.result.ExitCode != 0 || !out.result.InputEOF || out.result.InputWritten != a.written || out.result.InputDelivered != a.written {
		return fmt.Errorf("retained actor did not finish its delivered commands: exit %d complete %t bytes %d/%d/%d eof %t error %v stderr %.2048s", out.result.ExitCode, out.result.Complete(), a.written, out.result.InputWritten, out.result.InputDelivered, out.result.InputEOF, out.err, out.result.Stderr)
	}
	return nil
}

func ecFourTwoReadStep(t *testing.T, parent context.Context, cluster *ceph.Container, client testcontainers.Container, path string, seq int) ecFourTwoIO {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 90*time.Second)
	defer cancel()
	const read = `import json,os,sys; p=sys.argv[1]; print(open(p).read() if os.path.exists(p) else 'null')`
	for {
		r, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{Argv: []string{"python3", "-c", read, fmt.Sprintf("%s/%d.json", path, seq)}, Timeout: 15 * time.Second, MaxStdout: 1 << 16, MaxStderr: 1 << 16, Label: "read 4+2 actor receipt"})
		var step *ecFourTwoIO
		if err != nil || !r.Complete() || r.ExitCode != 0 || json.Unmarshal(r.Stdout, &step) != nil {
			t.Fatalf("actor receipt %d unavailable: %v exit %d %.2048s", seq, err, r.ExitCode, r.Stderr)
		}
		if step != nil {
			if step.Seq != seq {
				t.Fatalf("wrong actor sequence: %+v", step)
			}
			t.Logf("EC_FOUR_TWO_CLIENT %s", r.Stdout)
			return *step
		}
		select {
		case <-ctx.Done():
			t.Fatalf("actor receipt %d absent: %v", seq, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}
