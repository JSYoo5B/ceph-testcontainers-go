//go:build all || (integration && features)

package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// radosErasureCodedScript is the native oracle of writes to erasure-coded
// pools. A writer client mutates; an independent reader client decides every
// outcome by reading bytes and the versions of its own reads. It prints one
// JSON receipt, and both clients shut down before it exits.
//
// Phases, each given the pool:
//   - opcodes OVERWRITES: each write operation on an object of unaligned
//     size, with the errno that the OSD returned and the reader's view right
//     after it. Without overwrites, also appends and writes at the end of
//     the object, aligned and not. A self-managed snapshot preserves the
//     object before a write.
//   - shard-write: an object and a self-managed snapshot of it.
//   - shard-degraded RECEIPT: read both while a shard is missing, then write
//     under the snapshot's context.
//   - shard-verify RECEIPT: read both again, then remove the snapshot.
const radosErasureCodedScript = `import hashlib,json,sys,rados
phase,pool=sys.argv[1],sys.argv[2]
HEAD=rados.LIBRADOS_SNAP_HEAD
EOPNOTSUPP=95
def connected():
    c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_osd_op_timeout':'15','rados_mon_op_timeout':'10'})
    c.connect(); return c
def digest(data): return hashlib.sha256(data).hexdigest()
def read(ioctx,name,snap=HEAD):
    ioctx.set_read(snap)
    try:
        data=ioctx.read(name,1<<20,0)
        return data,ioctx.get_last_version()
    finally: ioctx.set_read(HEAD)
def errno_of(fn):
    try: fn()
    except rados.Error as e: return getattr(e,'errno',None)
    return 0
def operate(ioctx,name,build):
    with rados.WriteOpCtx() as op:
        build(op); ioctx.operate_write_op(op,name)
writer,reader=connected(),connected()
a,b=writer.open_ioctx(pool),reader.open_ioctx(pool)
try:
    if phase=='opcodes':
        overwrites=sys.argv[3]=='1'
        out={'alignment':a.alignment()}
        assert (out['alignment'] is None)==overwrites,out
        data=bytes(range(256))*4+b'xyz'
        a.write_full('object',data)
        assert read(b,'object')[0]==data
        # Each operation on the object of unaligned size, and what the reader
        # sees right after it.
        steps=[
            ('append',lambda: a.append('object',b'A'*13),lambda d: d+b'A'*13),
            ('write',lambda: a.write('object',b'W'*5,3),lambda d: d[:3]+b'W'*5+d[8:]),
            ('truncate',lambda: a.trunc('object',100),lambda d: d[:100]),
            ('zero',lambda: operate(a,'object',lambda op: op.zero(0,10)),lambda d: b'\0'*10+d[10:]),
            ('omap',lambda: operate(a,'object',lambda op: a.set_omap(op,('key',),(b'value',))),lambda d: d),
            ('xattr',lambda: a.set_xattr('object','key',b'value'),lambda d: d),
        ]
        refused={}
        for name,operation,change in steps:
            refused[name]=errno_of(operation)
            if refused[name]==0: data=change(data)
            seen=read(b,'object')[0]
            assert seen==data,(name,len(seen),len(data))
        if overwrites:
            assert refused=={'append':0,'write':0,'truncate':0,'zero':0,'omap':EOPNOTSUPP,'xattr':0},refused
        else:
            assert refused=={'append':EOPNOTSUPP,'write':EOPNOTSUPP,'truncate':EOPNOTSUPP,'zero':EOPNOTSUPP,'omap':EOPNOTSUPP,'xattr':0},refused
        seen,version=read(b,'object')
        assert b.get_xattr('object','key')==b'value'
        out.update({'errno':refused,'size':len(seen),'sha256':digest(seen),'version':version})
        if not overwrites:
            # Data goes only to the end of the object, and only while the end
            # is a multiple of the alignment: an append or a write there
            # works, and the next one at the unaligned end does not; nor does
            # overwriting existing bytes at an aligned offset.
            stripe=out['alignment']
            a.write_full('appended',b'a'*stripe)
            a.write_full('written',b'a'*stripe)
            ends=[errno_of(lambda: a.append('appended',b'b'*13)),errno_of(lambda: a.write('written',b'b'*13,stripe)),
                  errno_of(lambda: a.append('appended',b'c'*stripe)),errno_of(lambda: a.write('written',b'c'*13,stripe+13)),
                  errno_of(lambda: a.write('written',b'd'*13,stripe))]
            assert ends==[0,0,EOPNOTSUPP,EOPNOTSUPP,EOPNOTSUPP],ends
            assert read(b,'appended')[0]==read(b,'written')[0]==b'a'*stripe+b'b'*13
            out['ends']=ends
        # A write under a self-managed snapshot context preserves the object
        # first, as in a replicated pool.
        a.write_full('snapshot',b'before;'*100)
        snap=a.create_self_managed_snap()
        try:
            a.set_self_managed_snap_write([snap])
            after=b'before;'*100
            if overwrites:
                a.write('snapshot',b'AFTER',7); after=after[:7]+b'AFTER'+after[12:]
            else:
                a.write_full('snapshot',b'after;'*100); after=b'after;'*100
            assert read(b,'snapshot',snap)[0]==b'before;'*100
            assert read(b,'snapshot')[0]==after
        finally:
            a.set_self_managed_snap_write([])
            a.remove_self_managed_snap(snap)
        print(json.dumps(out))
    elif phase=='shard-write':
        data=b''.join(hashlib.sha256(b'%d'%n).digest() for n in range(625))
        a.write_full('shard',data)
        snap=a.create_self_managed_snap()
        seen,version=read(b,'shard')
        assert seen==data
        print(json.dumps({'snap':snap,'sha256':[digest(data),digest(data)],'versions':[version,version]}))
    else:
        receipt=json.loads(sys.argv[3]); snap=receipt['snap']
        views=[read(b,'shard',snap),read(b,'shard')]
        assert [digest(d) for d,_ in views]==receipt['sha256'],receipt
        assert [v for _,v in views]==receipt['versions'],([v for _,v in views],receipt)
        if phase=='shard-degraded':
            a.set_self_managed_snap_write([snap])
            a.write('shard',b'DEGRADED',4097)
            a.set_self_managed_snap_write([])
            original=views[0][0]
            seen,version=read(b,'shard')
            assert seen==original[:4097]+b'DEGRADED'+original[4105:]
            snapshot=read(b,'shard',snap)
            assert snapshot[0]==original
            receipt={'snap':snap,'sha256':[digest(original),digest(seen)],'versions':[snapshot[1],version]}
        else:
            a.remove_self_managed_snap(snap)
            a.remove_object('shard')
        print(json.dumps(receipt))
finally:
    a.close(); b.close(); writer.shutdown(); reader.shutdown()
`

// erasureCodedPhase runs one phase of radosErasureCodedScript and returns its
// receipt.
func erasureCodedPhase(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, args ...string) json.RawMessage {
	t.Helper()
	result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
		Argv: append([]string{"python3", "-c", radosErasureCodedScript}, args...), Timeout: 2 * time.Minute,
		MaxStdout: 1 << 20, MaxStderr: 1 << 20, Label: "erasure-coded " + args[0],
	})
	if err != nil {
		t.Fatalf("%s phase: %v", args[0], err)
	}
	if !result.Complete() || result.ExitCode != 0 || !json.Valid(result.Stdout) {
		t.Fatalf("%s phase: exit %d, complete %t\nstdout: %s\nstderr: %s", args[0], result.ExitCode, result.Complete(), result.Stdout, result.Stderr)
	}
	t.Logf("%s %s receipt: %s", args[0], args[1], result.Stdout)
	return result.Stdout
}
