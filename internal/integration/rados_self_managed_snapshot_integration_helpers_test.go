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

// radosSelfManagedSnapshotScript is the native oracle of self-managed
// snapshot write contexts. A writer client mutates objects under explicit
// snapshot write contexts; a second, independent client reads every snapshot
// and HEAD and reports the version that each of its own reads returned, with
// the size and modification time at each. It prints one JSON receipt and
// shuts both clients down.
//
// Phases, each given the pool and namespace:
//   - write: create snapshots s1 and s2 around nonaligned writes, an append,
//     a shrinking and a zero-extending truncate, and read HEAD after each of
//     them. A failure removes what the phase created.
//   - verify RECEIPT: read the snapshots and HEAD of a write receipt again
//     with fresh clients, for example after an OSD restart.
//   - contexts RECEIPT: verify, then the contexts that name an older
//     sequence (with and without ORDERSNAP), a removed snapshot or an id that
//     the MONs never allocated, the removal of s1 until its clone is trimmed,
//     and a HEAD delete that keeps s2 readable. It removes every snapshot
//     that it and the write phase created, even when it fails.
const radosSelfManagedSnapshotScript = `import hashlib,json,subprocess,sys,time,rados
phase,pool,namespace=sys.argv[1],sys.argv[2],sys.argv[3]
HEAD=rados.LIBRADOS_SNAP_HEAD
def connected():
    c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_osd_op_timeout':'10','rados_mon_op_timeout':'10'})
    c.connect(); return c
def digest(data): return hashlib.sha256(data).hexdigest()
def listsnaps(name):
    r=subprocess.run(['rados','-p',pool,'--namespace',namespace,'listsnaps',name,'--format','json'],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=20)
    assert r.returncode==0,(name,r.returncode,r.stderr[-300:])
    return [[str(c['id']),sorted(s['id'] for s in c['snapshots']),c['size']] for c in json.loads(r.stdout)['clones']]
def read(ioctx,name,snap):
    # The bytes and the object version that this read itself returned.
    ioctx.set_read(snap)
    try:
        data=ioctx.read(name,4096,0)
        return data,ioctx.get_last_version()
    finally: ioctx.set_read(HEAD)
def stat(ioctx,name,snap):
    # The size and the modification time, in seconds, at snap.
    ioctx.set_read(snap)
    try:
        size,mtime=ioctx.stat(name)
        return [size,int(time.mktime(mtime))]
    finally: ioctx.set_read(HEAD)
def absent(ioctx,name,snap):
    try: read(ioctx,name,snap)
    except rados.ObjectNotFound: return True
    return False
def view(ioctx,snaps):
    reads=[read(ioctx,'object',snap) for snap in snaps+[HEAD]]
    return {'sizes':[len(d) for d,_ in reads],'sha256':[digest(d) for d,_ in reads],'versions':[v for _,v in reads],
            'stat':[stat(ioctx,'object',snap) for snap in snaps+[HEAD]]}
def remove_snaps(snaps):
    a.set_self_managed_snap_write([])
    for snap in snaps:
        try: a.remove_self_managed_snap(snap)
        except rados.Error: pass
writer=connected(); oracle=connected()
a=writer.open_ioctx(pool); b=oracle.open_ioctx(pool)
a.set_namespace(namespace); b.set_namespace(namespace)
out={}
try:
    if phase=='write':
        created=[]; versions=[]
        def mutated(data):
            # The writer's version of its own write, and the reader's view
            # of HEAD after it.
            versions.append(a.get_last_version())
            seen=read(b,'object',HEAD)
            assert seen==(data,versions[-1]),(len(seen[0]),seen[1],len(data),versions[-1])
        try:
            original=(bytes(range(256))*5)[:1031]
            a.write_full('object',original); mutated(original)
            s1=a.create_self_managed_snap(); created.append(s1)
            a.set_self_managed_snap_write([s1])
            data=original[:7]+b'AFTER'+original[12:]
            a.write('object',b'AFTER',7); mutated(data)
            data+=b'APPEND'
            a.append('object',b'APPEND'); mutated(data)
            data=data[:64]
            a.trunc('object',64); mutated(data)
            first=data.ljust(100,b'\0')
            a.trunc('object',100); mutated(first)
            s2=a.create_self_managed_snap(); created.append(s2)
            a.set_self_managed_snap_write([s2,s1])
            head=first[:3]+b'SECOND'+first[9:]
            a.write('object',b'SECOND',3); mutated(head)
            assert versions==sorted(set(versions)),versions
            seen=view(b,[s1,s2])
            assert seen['sha256']==[digest(original),digest(first),digest(head)],seen
            # A clone keeps the version that HEAD had before the write that
            # preserved it.
            assert seen['versions']==[versions[0],versions[4],versions[5]],(seen,versions)
            assert [size for size,_ in seen['stat']]==seen['sizes'],seen
            clones=listsnaps('object')
            assert clones==[[str(s1),[s1],len(original)],[str(s2),[s2],len(first)],['head',[],len(head)]],clones
        except BaseException:
            remove_snaps(created)
            try: a.remove_object('object')
            except rados.Error: pass
            raise
        out={'snaps':[s1,s2],'write_versions':versions,'view':seen,'clones':clones}
    else:
        receipt=json.loads(sys.argv[4]); s1,s2=receipt['snaps']
        seen=view(b,[s1,s2])
        assert seen==receipt['view'],(seen,receipt['view'])
        clones=listsnaps('object')
        assert clones==receipt['clones'],clones
        out={'snaps':[s1,s2],'view':seen,'clones':clones}
        if phase=='contexts':
            created=[]
            try:
                # A context older than the object's snapset is accepted and
                # preserves nothing.
                a.set_self_managed_snap_write([])
                a.write_full('older',b'o'*64); before=a.get_last_version()
                s3=a.create_self_managed_snap(); created.append(s3)
                a.set_self_managed_snap_write([s3])
                a.write('older',b'NEW',0)
                a.set_self_managed_snap_write([s1])
                a.write('older',b'OLD',8)
                older=listsnaps('older')
                assert older==[[str(s3),[s3],64],['head',[],64]],older
                assert read(b,'older',s3)==(b'o'*64,before)
                expected,version=read(b,'older',HEAD)
                assert expected==b'NEW'+b'o'*5+b'OLD'+b'o'*53 and version==a.get_last_version()
                # With LIBRADOS_OPERATION_ORDERSNAP (512) the OSD refuses it
                # with EOLDSNAPC (85) and changes nothing.
                ordersnap=None
                try:
                    with rados.WriteOpCtx() as op:
                        op.write(b'ORDER',16)
                        a.operate_write_op(op,'older',flags=512)
                except rados.Error as e:
                    ordersnap=getattr(e,'errno',None)
                assert ordersnap==85,ordersnap
                assert read(b,'older',HEAD)==(expected,version)
                # A context that names a removed snapshot is accepted; the
                # clone holds only the snapshots that still exist, and a
                # context of removed snapshots alone makes no clone.
                a.set_self_managed_snap_write([])
                a.write_full('removed',b'r'*64)
                a.write_full('removed-only',b'q'*64)
                s4=a.create_self_managed_snap(); created.append(s4)
                s5=a.create_self_managed_snap(); created.append(s5)
                a.remove_self_managed_snap(s4)
                a.set_self_managed_snap_write([s5,s4])
                a.write('removed',b'R',0)
                removed=listsnaps('removed')
                assert removed==[[str(s5),[s5],64],['head',[],64]],removed
                assert read(b,'removed',s5)[0]==b'r'*64 and read(b,'removed',HEAD)[0]==b'R'+b'r'*63
                a.set_self_managed_snap_write([s4])
                a.write('removed-only',b'Q',0)
                removed_only=listsnaps('removed-only')
                assert removed_only==[['head',[],64]],removed_only
                assert read(b,'removed-only',HEAD)[0]==b'Q'+b'q'*63
                # The OSD takes the client's context as given: an id that the
                # MONs never allocated becomes a clone id.
                unallocated=s5+1000
                a.set_self_managed_snap_write([])
                a.write_full('unallocated',b'u'*64)
                a.set_self_managed_snap_write([unallocated])
                a.write('unallocated',b'U',0); before=a.get_last_version()
                assert listsnaps('unallocated')==[[str(unallocated),[unallocated],64],['head',[],64]]
                assert read(b,'unallocated',unallocated)[0]==b'u'*64
                # The object's snapset sequence is now the unallocated id, so
                # a snapshot that the MONs allocate later is not preserved:
                # the next write changes HEAD without a clone, and a read at
                # the snapshot finds nothing.
                s6=a.create_self_managed_snap(); created.append(s6)
                a.set_self_managed_snap_write([s6])
                a.write('unallocated',b'L',1)
                data,version=read(b,'unallocated',HEAD)
                assert data==b'UL'+b'u'*62 and version==a.get_last_version()>before,(data,version,before)
                unallocated_clones=listsnaps('unallocated')
                assert unallocated_clones==[[str(unallocated),[unallocated],64],['head',[],64]],unallocated_clones
                assert absent(b,'unallocated',s6)
                # The MONs refuse to remove the id with ENOENT, so no trim
                # ever removes its clone; only removing the pool does.
                a.set_self_managed_snap_write([])
                refused=False
                try: a.remove_self_managed_snap(unallocated)
                except rados.ObjectNotFound: refused=True
                assert refused and listsnaps('unallocated')==unallocated_clones
                # The OSDs trim s1's clone asynchronously. Until then a read
                # at s1 may still return the snapshot's bytes.
                a.remove_self_managed_snap(s1)
                try:
                    data,_=read(b,'object',s1)
                    assert digest(data)==receipt['view']['sha256'][0]
                    right_after='data'
                except rados.ObjectNotFound:
                    right_after='ENOENT'
                deadline=time.monotonic()+60; polls=0
                while True:
                    polls+=1
                    clones=listsnaps('object')
                    if all(s1 not in snaps for _,snaps,_ in clones) and absent(b,'object',s1): break
                    assert time.monotonic()<deadline,('snapshot not trimmed',clones)
                    time.sleep(0.5)
                assert clones==receipt['clones'][1:],clones
                # Deleting HEAD under the context keeps s2 readable.
                a.set_self_managed_snap_write([s2])
                a.remove_object('object')
                assert absent(b,'object',HEAD)
                try: b.stat('object'); stat_absent=False
                except rados.ObjectNotFound: stat_absent=True
                assert stat_absent
                data,version=read(b,'object',s2)
                assert digest(data)==receipt['view']['sha256'][1] and version==receipt['view']['versions'][1],version
                assert stat(b,'object',s2)==receipt['view']['stat'][1]
                out.update({'older_seq_clones':older,'ordersnap_errno':ordersnap,'removed_snap_clones':removed,
                            'removed_only_clones':removed_only,'unallocated_snap':unallocated,
                            'unallocated_clones':unallocated_clones,'later_snap':s6,
                            'read_right_after_removal':right_after,'trim_polls':polls,'head_deleted':True})
            finally:
                remove_snaps(created+[s1,s2])
    print(json.dumps(out))
finally:
    a.close(); b.close(); writer.shutdown(); oracle.shutdown()
`

// selfManagedSnapshotPhase runs one phase of radosSelfManagedSnapshotScript
// and returns its receipt.
func selfManagedSnapshotPhase(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, pool, namespace, phase string, receipt json.RawMessage) json.RawMessage {
	t.Helper()
	argv := []string{"python3", "-c", radosSelfManagedSnapshotScript, phase, pool, namespace}
	if receipt != nil {
		argv = append(argv, string(receipt))
	}
	result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
		Argv: argv, Timeout: 2 * time.Minute, MaxStdout: 1 << 20, MaxStderr: 1 << 20,
		Label: "self-managed snapshot " + phase,
	})
	if err != nil {
		t.Fatalf("%s phase: %v", phase, err)
	}
	if !result.Complete() || result.ExitCode != 0 || !json.Valid(result.Stdout) {
		t.Fatalf("%s phase: exit %d, complete %t\nstdout: %s\nstderr: %s", phase, result.ExitCode, result.Complete(), result.Stdout, result.Stderr)
	}
	t.Logf("%s receipt: %s", phase, result.Stdout)
	return result.Stdout
}
