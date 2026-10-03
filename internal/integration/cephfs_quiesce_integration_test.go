//go:build integration && features

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strconv"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestCephFSQuiesceCheckpoints(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) { testCephFSQuiesceCheckpoints(t, host) })
	}
}

func testCephFSQuiesceCheckpoints(t *testing.T, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 7*time.Minute)
	defer cancel()
	const filesystem = "tc-quiesce"
	opts := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1), ceph.WithCephFS(ceph.CephFSConfig{Name: filesystem})}
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
	cluster, client := newServiceCluster(t, opts...)
	fs := cluster.Filesystems()[0]
	volumes := make([]*ceph.CephFSSubvolume, 3)
	for i, name := range []string{"paused-a", "paused-b", "outside"} {
		var err error
		volumes[i], err = fs.CreateSubvolume(ctx, ceph.CephFSSubvolumeConfig{Name: name, NamespaceIsolated: true})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.CopyToContainer(ctx, []byte(cephFSQuiesceIOScript), "/tmp/tc-quiesce.py", 0o600); err != nil {
		t.Fatal(err)
	}
	// A fixed shell program receives each variable as a positional argument.
	fencingExec(t, ctx, client, []string{"sh", "-c", `python3 /tmp/tc-quiesce.py "$@" >/tmp/tc-quiesce.log 2>&1 &`, "quiesce", filesystem, volumes[0].Path, volumes[1].Path, volumes[2].Path})
	quiesceWait(t, ctx, client, "ready")
	q, err := fs.QuiesceSubvolumes(ctx, volumes[:2], ceph.CephFSQuiesceConfig{Timeout: 20 * time.Second, Expiration: 60 * time.Second})
	if err != nil {
		t.Fatalf("quiesce initial: %v", err)
	}
	native, err := q.Status(ctx)
	wantMembers := []string{"file:" + volumes[0].Path, "file:" + volumes[1].Path}
	slices.Sort(wantMembers)
	if err != nil || native.State != "QUIESCED" || !slices.Equal(native.Members, wantMembers) {
		t.Fatalf("native pause %+v: %v", native, err)
	}
	quiescePhase(t, ctx, client, "pause")
	blocked := quiesceWait(t, ctx, client, "paused")
	if blocked.Completed != 0 || !blocked.OutsideOK {
		t.Fatalf("writes not paused or unrelated volume blocked: %+v", blocked)
	}
	snapshots := make([]*ceph.CephFSSubvolumeSnapshot, 2)
	for i, volume := range volumes[:2] {
		snapshots[i], err = fs.CreateSubvolumeSnapshot(ctx, volume, "frozen")
		if err != nil {
			t.Fatal(err)
		}
	}
	copy := *q
	if err := copy.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.Release(ctx); err != nil {
		t.Fatal(err)
	}
	quiescePhase(t, ctx, client, "release")
	released := quiesceWait(t, ctx, client, "released")
	if released.Completed != 2 {
		t.Fatalf("native writers did not resume: %+v", released)
	}
	for i, snapshot := range snapshots {
		// This new native session reads both the changed head and frozen bytes;
		// successful CLI state alone is insufficient checkpoint evidence.
		fencingExec(t, ctx, client, []string{"python3", "-c", cephFSQuiesceVerifyScript, filesystem, volumes[i].Path, snapshot.Path})
		if err := fs.RemoveSubvolumeSnapshot(ctx, snapshot); err != nil {
			t.Fatal(err)
		}
	}
	expires, err := fs.QuiesceSubvolumes(ctx, volumes[:2], ceph.CephFSQuiesceConfig{Timeout: 20 * time.Second, Expiration: 8 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	quiescePhase(t, ctx, client, "expire")
	blocked = quiesceWait(t, ctx, client, "expiring")
	if blocked.Completed != 0 || !blocked.OutsideOK {
		t.Fatalf("TTL pause not enforced: %+v", blocked)
	}
	expired := quiesceWait(t, ctx, client, "expired")
	if expired.Completed != 2 {
		t.Fatal("native expiration did not resume both writers")
	}
	native, err = expires.Status(ctx)
	if err != nil || native.State != "EXPIRED" {
		t.Fatalf("native expiration %+v: %v", native, err)
	}
	if err := expires.Release(ctx); err == nil {
		t.Fatal("expired checkpoint reported consistent release")
	}
	// A readonly query never adopts a new version. Preserve an outside edit,
	// then use an explicit raw command with its fresh version for cleanup.
	changed, err := fs.QuiesceSubvolumes(ctx, volumes[:2], ceph.CephFSQuiesceConfig{Timeout: 20 * time.Second, Expiration: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	cephCommand(t, ctx, cluster, "fs", "quiesce", filesystem, "--set-id", changed.ID(), "--expiration", "40")
	native, err = changed.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := changed.Release(ctx); err == nil {
		t.Fatal("outside version edit was adopted by release")
	}
	cephCommand(t, ctx, cluster, "fs", "quiesce", filesystem, "--set-id", changed.ID(), "--release", "--if-version", strconv.FormatUint(native.Version, 10), "--await-for", "20")
	quiescePhase(t, ctx, client, "finish")
	quiesceWait(t, ctx, client, "finish")
	for _, volume := range volumes {
		if err := fs.RemoveSubvolume(ctx, volume); err != nil {
			t.Fatal(err)
		}
	}
	t.Log("two independent native clients: durable writes paused, outside subvolume I/O retained, frozen snapshot bytes and changed heads verified, explicit versioned release and natural TTL recovery, outside version edit preserved, owned cleanup")
}

type quiesceIOState struct {
	Phase     string `json:"phase"`
	Completed int    `json:"completed"`
	OutsideOK bool   `json:"outside_ok"`
	Error     string `json:"error"`
}

func quiescePhase(t *testing.T, ctx context.Context, client testcontainers.Container, phase string) {
	t.Helper()
	if err := client.CopyToContainer(ctx, []byte(phase), "/tmp/tc-quiesce-command", 0o600); err != nil {
		t.Fatal(err)
	}
}

func quiesceWait(t *testing.T, parent context.Context, client testcontainers.Container, phase string) quiesceIOState {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 45*time.Second)
	defer cancel()
	var state quiesceIOState
	for {
		code, reader, err := client.Exec(ctx, []string{"cat", "/tmp/tc-quiesce-state"}, tcexec.Multiplexed())
		if err == nil && code == 0 {
			data, err := io.ReadAll(reader)
			if err == nil && json.Unmarshal(data, &state) == nil {
				if state.Error != "" {
					t.Fatal(state.Error)
				}
				if state.Phase == phase {
					return state
				}
			}
		}
		select {
		case <-ctx.Done():
			diagnostic, stop := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
			defer stop()
			log := fencingExec(t, diagnostic, client, []string{"cat", "/tmp/tc-quiesce.log"})
			t.Fatal(fmt.Sprintf("quiesce native phase %s missing: %+v %v %s", phase, state, ctx.Err(), log))
		case <-time.After(150 * time.Millisecond):
		}
	}
}

const cephFSQuiesceIOScript = `import cephfs, json, os, sys, threading, time, traceback
filesystem, root_a, root_b, outside = sys.argv[1:]
deadline=threading.Timer(250, lambda: os._exit(124)); deadline.daemon=True; deadline.start()
def session():
    fs=cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
    fs.conf_set('client_mount_timeout','20'); fs.mount(filesystem_name=filesystem.encode()); return fs
def emit(phase,completed=0,outside_ok=False,error=''):
    with open('/tmp/tc-quiesce-state.new','w') as f: json.dump(dict(phase=phase,completed=completed,outside_ok=outside_ok,error=error),f)
    os.replace('/tmp/tc-quiesce-state.new','/tmp/tc-quiesce-state')
def command(expected):
    while True:
        try:
            with open('/tmp/tc-quiesce-command') as f:
                if f.read().strip()==expected: return
        except FileNotFoundError: pass
        time.sleep(.05)
def write(fs,path,data):
    fd=fs.open(path,os.O_CREAT|os.O_WRONLY|os.O_TRUNC,0o600)
    try:
        assert fs.write(fd,data,0)==len(data); fs.fsync(fd,0)
    finally: fs.close(fd)
try:
    sessions=[session(),session()]; observer=session(); roots=[root_a,root_b]
    original=bytes(range(256))*256; changed=bytes(x^0x5a for x in range(256))*256
    for fs,root in zip(sessions,roots): write(fs,root+'/data',original)
    write(observer,outside+'/data',b'outside initial')
    emit('ready')
    for mode,waiting,finished,payload in [('pause','paused','released',changed),('expire','expiring','expired',original)]:
        command(mode); completed=[]; errors=[]; started=[threading.Event(),threading.Event()]
        def worker(i):
            started[i].set()
            try: write(sessions[i],roots[i]+'/data',payload); completed.append(i)
            except BaseException: errors.append(traceback.format_exc())
        threads=[threading.Thread(target=worker,args=(i,),daemon=True) for i in range(2)]
        for thread in threads: thread.start()
        for event in started: assert event.wait(2)
        write(observer,outside+'/data',mode.encode())
        fd=observer.open(outside+'/data',os.O_RDONLY)
        try: assert observer.read(fd,0,1024)==mode.encode()
        finally: observer.close(fd)
        time.sleep(2)
        assert not errors, str(errors); assert not completed,'quiesced durable write completed'
        emit(waiting,len(completed),True)
        if mode=='pause': command('release')
        for thread in threads: thread.join(35)
        assert not errors,str(errors); assert len(completed)==2,'writes did not resume'
        emit(finished,len(completed),True)
    command('finish')
    for fs in sessions+[observer]: fs.shutdown()
    emit('finish')
except BaseException:
    error=traceback.format_exc(); print(error,flush=True); emit('error',error=error); raise
`

const cephFSQuiesceVerifyScript = `import cephfs, hashlib, os, sys, threading
filesystem,root,snapshot=sys.argv[1:]
deadline=threading.Timer(30,lambda:os._exit(124)); deadline.daemon=True; deadline.start()
fs=cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf',auth_id='admin'); fs.mount(filesystem_name=filesystem.encode())
def read(path):
    fd=fs.open(path+'/data',os.O_RDONLY)
    try:return fs.read(fd,0,65537)
    finally:fs.close(fd)
original=bytes(range(256))*256; changed=bytes(x^0x5a for x in range(256))*256
assert read(root)==changed,'resumed head bytes differ'; assert read(snapshot)==original,'quiesced snapshot bytes differ'
print('native frozen checkpoint SHA256',hashlib.sha256(original).hexdigest()); fs.shutdown()
`
