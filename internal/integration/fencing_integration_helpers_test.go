//go:build all || (integration && features)

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func testClientFencing(t *testing.T, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()
	options := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1)}
	if host {
		options = append(options, ceph.WithHostNetwork())
	}
	cluster, client := newServiceCluster(t, options...)
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-fencing", Application: "rados", PGNum: 1}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	const other = "192.0.2.245:0/9999"
	cephCommand(t, ctx, cluster, "osd", "blocklist", "add", other, "300")
	cephCommand(t, ctx, cluster, "osd", "blocklist", "range", "add", "198.51.100.0/24", "300")
	prior, err := cluster.BlocklistEntries(ctx)
	if err != nil || len(prior) != 2 {
		t.Fatalf("native entries and ranges: %+v %v", prior, err)
	}
	if existing, err := cluster.TemporaryBlocklist(ctx, other, time.Minute); err == nil || existing != nil {
		t.Fatal("outside entry was adopted")
	}
	if err := client.CopyToContainer(ctx, []byte(clientFencingScript), "/tmp/tc-fencing.py", 0o600); err != nil {
		t.Fatal(err)
	}
	fencingExec(t, ctx, client, []string{"sh", "-c", "python3 /tmp/tc-fencing.py >/tmp/tc-fencing.log 2>&1 &"})
	status := fencingWait(t, ctx, client, "ready")
	if len(status.Addresses) != 2 || status.Addresses[0] == status.Addresses[1] {
		t.Fatal("native clients did not have distinct session addresses")
	}
	change, err := cluster.TemporaryBlocklist(ctx, status.Addresses[0], time.Minute)
	if change != nil {
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := change.Restore(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	fencingPhase(t, ctx, client, "fenced")
	status = fencingWait(t, ctx, client, "fenced")
	if status.ReturnCode != -108 || !status.OtherClientOK {
		t.Fatalf("native ESHUTDOWN/other session proof: %+v", status)
	}
	copy := *change
	if err := copy.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := change.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := cluster.BlocklistEntries(ctx)
	if err != nil || !slices.Equal(prior, after) {
		t.Fatal("restoration changed an unrelated exact entry or range")
	}
	fencingPhase(t, ctx, client, "restored")
	status = fencingWait(t, ctx, client, "restored")
	if status.ReturnCode != 0 || !status.OtherClientOK || !status.FreshClientOK {
		t.Fatalf("restored session/reconnect proof: %+v", status)
	}
	expires, err := cluster.TemporaryBlocklist(ctx, status.Addresses[0], 12*time.Second)
	if expires != nil {
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := expires.Restore(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	fencingPhase(t, ctx, client, "expiring")
	status = fencingWait(t, ctx, client, "expiring")
	if status.ReturnCode != -108 || !status.OtherClientOK {
		t.Fatalf("TTL fence proof: %+v", status)
	}
	deadline := time.Now().Add(40 * time.Second)
	for {
		entries, err := cluster.BlocklistEntries(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.ContainsFunc(entries, func(entry ceph.BlocklistEntry) bool { return !entry.Range && entry.Address == expires.Address() }) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("native client blocklist did not expire")
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}
	if err := expires.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	fencingPhase(t, ctx, client, "expired")
	status = fencingWait(t, ctx, client, "expired")
	if !status.OtherClientOK || !status.FreshClientOK {
		t.Fatal("new session did not recover after native expiration")
	}
	final, err := cluster.BlocklistEntries(ctx)
	if err != nil || !slices.Equal(prior, final) {
		t.Fatal("TTL reconciliation changed another native entry")
	}
	fencingPhase(t, ctx, client, "finish")
	_ = fencingWait(t, ctx, client, "finish")
	t.Log("native librados: exact nonzero nonce fenced one of two clients on the same host; ESHUTDOWN, unaffected second client, fresh nonce recovery, explicit restore and natural TTL expiry, unrelated entries/ranges preserved")
}

type fencingStatus struct {
	Phase         string   `json:"phase"`
	Addresses     []string `json:"addresses"`
	ReturnCode    int      `json:"return_code"`
	OtherClientOK bool     `json:"other_client_ok"`
	FreshClientOK bool     `json:"fresh_client_ok"`
	Error         string   `json:"error"`
}

func fencingExec(t *testing.T, ctx context.Context, client testcontainers.Container, command []string) []byte {
	t.Helper()
	code, reader, err := client.Exec(ctx, command, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil || code != 0 {
		t.Fatalf("native fencing command: exit=%d %s %v", code, data, err)
	}
	return data
}

func fencingPhase(t *testing.T, ctx context.Context, client testcontainers.Container, phase string) {
	t.Helper()
	if err := client.CopyToContainer(ctx, []byte(phase), "/tmp/tc-fencing-command", 0o600); err != nil {
		t.Fatal(err)
	}
}

func fencingWait(t *testing.T, parent context.Context, client testcontainers.Container, phase string) fencingStatus {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 50*time.Second)
	defer cancel()
	var status fencingStatus
	for {
		code, reader, err := client.Exec(ctx, []string{"cat", "/tmp/tc-fencing-state"}, tcexec.Multiplexed())
		if err == nil && code == 0 {
			data, readErr := io.ReadAll(reader)
			if readErr == nil && json.Unmarshal(data, &status) == nil {
				if status.Error != "" {
					t.Fatal(status.Error)
				}
				if status.Phase == phase {
					return status
				}
			}
		}
		select {
		case <-ctx.Done():
			diagnostics, stop := context.WithTimeout(context.WithoutCancel(parent), 10*time.Second)
			defer stop()
			log := fencingExec(t, diagnostics, client, []string{"cat", "/tmp/tc-fencing.log"})
			t.Fatal(fmt.Sprintf("native fencing phase %s not observed: %+v %v %s", phase, status, ctx.Err(), strings.TrimSpace(string(log))))
		case <-time.After(150 * time.Millisecond):
		}
	}
}

// This Linux-only consumer talks directly to the native C ABI using ctypes;
// the public Go module does not link a native library or import go-ceph.
const clientFencingScript = `import ctypes as c, errno, json, os, time, traceback
lib=c.CDLL('librados.so.2'); libc=c.CDLL(None)
def signature(name,result,*args):
    fn=getattr(lib,name); fn.restype=result; fn.argtypes=list(args); return fn
create=signature('rados_create',c.c_int,c.POINTER(c.c_void_p),c.c_char_p)
config=signature('rados_conf_read_file',c.c_int,c.c_void_p,c.c_char_p)
setting=signature('rados_conf_set',c.c_int,c.c_void_p,c.c_char_p,c.c_char_p)
connect=signature('rados_connect',c.c_int,c.c_void_p)
addresses=signature('rados_getaddrs',c.c_int,c.c_void_p,c.POINTER(c.c_void_p))
ioctx=signature('rados_ioctx_create',c.c_int,c.c_void_p,c.c_char_p,c.POINTER(c.c_void_p))
write=signature('rados_write_full',c.c_int,c.c_void_p,c.c_char_p,c.c_char_p,c.c_size_t)
read=signature('rados_read',c.c_int,c.c_void_p,c.c_char_p,c.c_void_p,c.c_size_t,c.c_uint64)
destroy=signature('rados_ioctx_destroy',None,c.c_void_p)
shutdown=signature('rados_shutdown',None,c.c_void_p)
libc.free.argtypes=[c.c_void_p]; libc.free.restype=None
clients=[]
def checked(result):
    assert result>=0, 'native errno '+str(result)
def open_client():
    cluster=c.c_void_p(); checked(create(c.byref(cluster),b'admin'))
    checked(config(cluster,b'/etc/ceph/ceph.conf'))
    for name,value in [(b'keyring',b'/etc/ceph/ceph.client.admin.keyring'),(b'rados_osd_op_timeout',b'6'),(b'rados_mon_op_timeout',b'10')]: checked(setting(cluster,name,value))
    checked(connect(cluster)); address=c.c_void_p(); checked(addresses(cluster,c.byref(address)))
    text=c.string_at(address).decode(); libc.free(address)
    context=c.c_void_p(); checked(ioctx(cluster,b'tc-fencing',c.byref(context)))
    result=(cluster,context,text); clients.append(result); return result
payload=b'client-fencing-native-payload\n'*2048
def prove(client,name=b'b'):
    checked(write(client[1],name,payload,len(payload)))
    data=c.create_string_buffer(len(payload)); length=read(client[1],name,data,len(payload),0)
    assert length==len(payload) and data.raw==payload, 'native payload mismatch'
    return True
def fenced(client,other):
    deadline=time.monotonic()+30
    while time.monotonic()<deadline:
        result=write(client[1],b'a',payload,len(payload)); prove(other)
        if result == -errno.ESHUTDOWN: return result
        assert result>=0, 'unexpected native fence errno '+str(result)
        time.sleep(.1)
    raise AssertionError('exact client never received native ESHUTDOWN')
def restored(client):
    deadline=time.monotonic()+30
    while time.monotonic()<deadline:
        result=write(client[1],b'a',payload,len(payload))
        if result==0: prove(client,b'a'); return result
        assert result==-errno.ESHUTDOWN, 'unexpected restore errno '+str(result)
        time.sleep(.1)
    raise AssertionError('native librados session did not recover after restore')
def report(phase,**data):
    data['phase']=phase
    with open('/tmp/tc-fencing-state.next','w') as stream: json.dump(data,stream)
    os.replace('/tmp/tc-fencing-state.next','/tmp/tc-fencing-state')
try:
    a=open_client(); b=open_client(); prove(a,b'a'); prove(b)
    report('ready',addresses=[a[2],b[2]],other_client_ok=True)
    phase='ready'; fresh=None; deadline=time.monotonic()+260
    while time.monotonic()<deadline:
        try: command=open('/tmp/tc-fencing-command').read().strip()
        except FileNotFoundError: command=''
        if command and command!=phase:
            if command=='fenced': report(command,return_code=fenced(a,b),other_client_ok=prove(b))
            elif command=='restored':
                fresh=open_client(); assert fresh[2] not in [a[2],b[2]]
                report(command,return_code=restored(a),other_client_ok=prove(b),fresh_client_ok=prove(fresh,b'a'),addresses=[fresh[2]])
            elif command=='expiring': report(command,return_code=fenced(fresh,b),other_client_ok=prove(b))
            elif command=='expired':
                newer=open_client(); assert newer[2] not in [a[2],b[2],fresh[2]]
                report(command,other_client_ok=prove(b),fresh_client_ok=prove(newer,b'a'))
            elif command=='finish': report(command); break
            else: raise AssertionError('unexpected native client phase')
            phase=command
        time.sleep(.1)
    else: raise AssertionError('native consumer harness deadline exceeded')
except Exception as error:
    report('failed',error=str(error)); traceback.print_exc()
finally:
    for cluster,context,_ in clients: destroy(context); shutdown(cluster)
`
