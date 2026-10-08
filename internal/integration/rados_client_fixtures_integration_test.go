//go:build integration && features

package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// These are client operations. The fixture supplies an ordinary authenticated
// replicated pool and native OSD classes; it does not duplicate librados CRUD.
func TestRADOSClientFixtures(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
			defer cancel()
			opts := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1)}
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, client := newServiceCluster(t, opts...)
			const pool = "tc-rados-fixtures"
			if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rados", PGNum: 1}); err != nil {
				t.Fatal(err)
			}
			state, err := cluster.PoolStatus(ctx, pool)
			if err != nil || state.ID <= 0 {
				t.Fatalf("native detail pool identity: %+v %v", state, err)
			}
			// osd dump uses pool, whereas pool ls detail uses pool_id. Compare
			// independent native forms so a silently zero ID cannot pass.
			var native struct {
				Pools []struct {
					ID   int64  `json:"pool"`
					Name string `json:"pool_name"`
				} `json:"pools"`
			}
			data, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
			if err != nil || json.Unmarshal(data, &native) != nil {
				t.Fatal("native OSDMap comparison failed", err)
			}
			found := false
			for _, entry := range native.Pools {
				if entry.Name == pool {
					found = entry.ID == state.ID
				}
			}
			if !found {
				t.Fatal("public PoolStatus identity differs from native OSDMap")
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			outer := `import subprocess,sys; subprocess.run(["python3","-c",sys.argv[1]],check=True,timeout=100)`
			proof := fencingExec(t, ctx, client, []string{"python3", "-c", outer, radosClientFixtureProbe})
			t.Log(string(proof))
		})
	}
}

const radosClientFixtureProbe = `import rados,json,subprocess,threading,tempfile,hashlib,os
pool='tc-rados-fixtures'; namespace='fixture'
def connected():
    client=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_osd_op_timeout':'10','rados_mon_op_timeout':'10'})
    client.connect(); return client
a=connected(); b=connected()
io=a.open_ioctx(pool); other=b.open_ioctx(pool)
io.set_namespace(namespace); other.set_namespace(namespace)
payload=b'atomic-client-fixture\0'*4096
watch=None
try:
    with rados.WriteOpCtx() as op:
        op.write_full(payload)
        op.set_xattr('tc-fixture',b'original')
        io.set_omap(op,('meta',),(b'value',))
        io.operate_write_op(op,'compound',0,0)
    assert io.read('compound',len(payload),0)==payload
    assert io.get_xattr('compound','tc-fixture')==b'original'
    with rados.ReadOpCtx() as op:
        items,_=io.get_omap_vals(op,'','',10,bytes)
        io.operate_read_op(op,'compound',0)
        assert dict(items)=={b'meta':b'value'}
    rejected=False
    try:
        with rados.WriteOpCtx() as op:
            op.cmpext(b'wrong',0)
            op.write_full(b'destroyed')
            op.set_xattr('tc-fixture',b'destroyed')
            io.operate_write_op(op,'compound',0,0)
    except rados.Error:
        rejected=True
    assert rejected and io.read('compound',len(payload),0)==payload
    assert io.get_xattr('compound','tc-fixture')==b'original'
    with rados.WriteOpCtx() as op:
        op.execute('hello','record_hello',b'fixture')
        io.operate_write_op(op,'hello-object',0,0)
    result,hello=io.execute('hello-object','hello','replay',b'',4096)
    assert result==len(hello) and hello==b'Hello, fixture!',(result,hello)
    io.write_full('watched',b'watch-fixture')
    event=threading.Event(); notifications=[]; errors=[]
    def notified(notify_id,notifier_id,cookie,data):
        notifications.append((notify_id,notifier_id,cookie,data)); event.set()
    watch=io.watch('watched',notified,errors.append,10)
    assert other.notify('watched','fixture-notification',5000)
    assert event.wait(5) and not errors,errors
    assert len(notifications)==1 and notifications[0][3]==b'fixture-notification',notifications
    watch.close(); watch=None
    io.create_snap('checkpoint')
    snap=io.lookup_snap('checkpoint')
    io.write_full('compound',b'changed-head')
    io.set_read(snap.snap_id)
    assert io.read('compound',len(payload),0)==payload
    io.set_read(rados.LIBRADOS_SNAP_HEAD)
    assert io.read('compound',len(payload),0)==b'changed-head'
    io.remove_snap('checkpoint')
    # The native CLI links libradosstriper and uses the OSD lock class. A
    # payload larger than its default object size verifies multiple shards.
    striped=b'native-striped-payload\0'*(420000)
    with tempfile.TemporaryDirectory() as directory:
        source=directory+'/source'; target=directory+'/target'
        open(source,'wb').write(striped)
        def strip(*args):
            result=subprocess.run(['rados','--striper','-p',pool,'--namespace',namespace,*args],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=25)
            assert result.returncode==0,(args,result.returncode,result.stderr)
            return result.stdout
        strip('put','striped',source); strip('get','striped',target)
        assert open(target,'rb').read()==striped
        shards=[obj.key for obj in io.list_objects() if obj.key.startswith('striped.')]
        assert len(shards)>=3,shards
        strip('rm','striped')
    for name in ('compound','hello-object','watched'): io.remove_object(name)
    assert list(io.list_objects())==[]
    print(json.dumps({'compound_atomicity':True,'xattr_omap':True,'cls_hello':hello.decode(),'watch_notify_clients':2,'pool_snapshot_frozen_bytes':len(payload),'striper_bytes':len(striped),'striper_shards':len(shards),'striper_sha256':hashlib.sha256(striped).hexdigest(),'native_objects_cleaned':True}))
finally:
    if watch is not None: watch.close()
    io.close(); other.close(); a.shutdown(); b.shutdown()
`
