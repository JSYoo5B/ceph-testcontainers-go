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

// radosObjectReceiptScript is the native oracle of the RADOS object
// operations that python-rados and the rados CLI of the official image
// reach. A writer client changes objects; a second, independent client (or a
// rados CLI process) reads every result back. It asserts what it can itself,
// prints one JSON receipt and shuts both clients down.
//
// Phases, each given the pool and namespace:
//   - xattr: binary values listed by getxattrs; rmxattr alone and inside a
//     compound write; the errno of a missing name and of a missing object.
//   - omap: keys-only and value pages with max_return and start_after until a
//     page comes back empty, a prefix filter, values by keys, a binary
//     header, single-key and key-range removal, clear, and the OSD's own
//     per-request cap (osd_max_omap_entries_per_request) over a request that
//     asks for more.
//   - rollback: an object rolled back to a self-managed snapshot regains the
//     snapshot's bytes, xattrs and omap at a new version; a later write
//     changes HEAD only; an object born after the snapshot is deleted.
//   - hints: allocation hints set by the rados CLI, read back from the object
//     info when the image can decode it; fadvise flags on a write, a read and
//     the CLI's copy-from.
//   - stats POOLSIZE: pool statistics and cluster statfs against objects of
//     known sizes, after the writes and after removing some of them.
const radosObjectReceiptScript = `import json,os,shutil,subprocess,sys,tempfile,time,rados
phase,pool,namespace=sys.argv[1],sys.argv[2],sys.argv[3]
HEAD=rados.LIBRADOS_SNAP_HEAD
def connected():
    c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_osd_op_timeout':'20','rados_mon_op_timeout':'20'})
    c.connect(); return c
def run(argv,data=None,ok=True):
    r=subprocess.run(argv,input=data,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=60)
    assert not ok or r.returncode==0,(argv,r.returncode,r.stderr[-400:])
    return r
def cli(*args,ok=True): return run([b'rados',b'-p',pool.encode(),b'--namespace',namespace.encode()]+[x if isinstance(x,bytes) else x.encode() for x in args],ok=ok)
def code(fn):
    # 0 when the call succeeds, else the errno that the client returned.
    try: fn(); return 0
    except rados.Error as e: return getattr(e,'errno',None)
def read(io,name,snap=HEAD):
    io.set_read(snap)
    try: return io.read(name,1<<20,0),io.get_last_version()
    finally: io.set_read(HEAD)
def absent(io,name,snap=HEAD):
    try: read(io,name,snap)
    except rados.ObjectNotFound: return True
    return False
def xattrs(io,name): return dict(io.get_xattrs(name))
def hexed(d): return {k:(v.hex() if v is not None else None) for k,v in d.items()}
def omap_keys(io,name,after,limit):
    with rados.ReadOpCtx() as op:
        it,_=io.get_omap_keys(op,after,limit); io.operate_read_op(op,name)
        return list(it)
def omap_vals(io,name,after,limit,prefix=''):
    with rados.ReadOpCtx() as op:
        it,_=io.get_omap_vals(op,after,prefix,limit); io.operate_read_op(op,name)
        return list(it)
def omap_by_keys(io,name,keys):
    with rados.ReadOpCtx() as op:
        it,_=io.get_omap_vals_by_keys(op,tuple(keys)); io.operate_read_op(op,name)
        return dict(it)
def omap_all(io,name): return dict(omap_vals(io,name,'',100000))
def pages(fetch,limit):
    # Pages of at most limit entries, each continuing after the last key of
    # the one before, until a page comes back empty.
    out=[]; after=''
    while True:
        page=fetch(after,limit); out.append([k for k,_ in page])
        if not page: return out
        after=page[-1][0]
        assert len(out)<1000
def write(io,name,build):
    with rados.WriteOpCtx() as op:
        build(op); io.operate_write_op(op,name)
def header(name):
    with tempfile.TemporaryDirectory() as d:
        path=os.path.join(d,'h'); cli('getomapheader',name,path)
        with open(path,'rb') as f: return f.read()
` + radosHeaderWriterScript + `def wait_for(what,check,timeout=120):
    deadline=time.monotonic()+timeout; polls=0
    while True:
        polls+=1; seen=check()
        if seen[0]: return seen[1],polls
        assert time.monotonic()<deadline,(what,seen[1])
        time.sleep(1)
writer=connected(); oracle=connected()
a=writer.open_ioctx(pool); b=oracle.open_ioctx(pool)
a.set_namespace(namespace); b.set_namespace(namespace)
out={}
try:
    if phase=='xattr':
        values={'binary':bytes(range(256)),'nul':b'a\0b\0','empty':b'','text':b'hello'}
        a.write_full('xattr',b'payload')
        for k,v in values.items(): a.set_xattr('xattr',k,v)
        listed=xattrs(b,'xattr')
        assert listed==values,hexed(listed)
        a.rm_xattr('xattr','binary')
        after_rm=xattrs(b,'xattr'); expected=dict(values); del expected['binary']
        assert after_rm==expected,hexed(after_rm)
        removed_get=code(lambda: b.get_xattr('xattr','binary'))
        # A removed or never set name: getxattr refuses it, rmxattr of it is a
        # write that changes nothing else.
        before=read(b,'xattr')[1]
        absent_rm=code(lambda: a.rm_xattr('xattr','never-set'))
        absent_version=a.get_last_version()
        assert xattrs(b,'xattr')==expected and read(b,'xattr')[0]==b'payload'
        # rmxattr and setxattr in one compound write apply together.
        write(a,'xattr',lambda op:(op.rm_xattr('text'),op.set_xattr('added',b'\xff\x00')))
        compound=xattrs(b,'xattr'); expected.pop('text'); expected['added']=b'\xff\x00'
        assert compound==expected,hexed(compound)
        out={'listed':hexed(listed),'after_rmxattr':hexed(after_rm),'getxattr_removed':removed_get,
             'rmxattr_never_set':absent_rm,'rmxattr_never_set_version':[before,absent_version],
             'after_compound':hexed(compound),
             'getxattrs_missing_object':code(lambda: xattrs(b,'missing')),
             'rmxattr_missing_object':code(lambda: a.rm_xattr('missing','text')),
             'getxattr_missing_object':code(lambda: b.get_xattr('missing','text'))}
    elif phase=='omap':
        keys=['k%02d'%i for i in range(25)]
        vals={k:k.encode()+b'\0'+bytes([200+i]) for i,k in enumerate(keys)}
        a.write_full('omap',b'payload')
        write(a,'omap',lambda op: a.set_omap(op,tuple(keys),tuple(vals[k] for k in keys)))
        assert omap_all(b,'omap')==vals
        keys_only=omap_keys(b,'omap','',100)
        assert [k for k,_ in keys_only]==keys and all(v is None for _,v in keys_only),keys_only[:3]
        key_pages=pages(lambda after,n: omap_keys(b,'omap',after,n),7)
        val_pages=pages(lambda after,n: omap_vals(b,'omap',after,n),7)
        assert key_pages==val_pages==[keys[0:7],keys[7:14],keys[14:21],keys[21:25],[]],key_pages
        prefixed=[k for k,_ in omap_vals(b,'omap','',100,'k1')]
        assert prefixed==keys[10:20],prefixed
        by_keys=omap_by_keys(b,'omap',['k02','k17','absent'])
        assert by_keys=={'k02':vals['k02'],'k17':vals['k17']},by_keys
        assert header('omap')==b''
        head_bytes=bytes(range(255,0,-1))
        header_via=set_header('omap',head_bytes)
        assert header('omap')==head_bytes
        # The OSD caps every listing at osd_max_omap_entries_per_request,
        # whatever max_return asks for; paging continues past the cap.
        run(['ceph','config','set','osd','osd_max_omap_entries_per_request','10'])
        try:
            capped,polls=wait_for('omap entry cap',lambda: (lambda n:(n==10,n))(len(omap_keys(b,'omap','',100))),60)
            capped_pages=pages(lambda after,n: omap_vals(b,'omap',after,n),100)
            assert capped_pages==[keys[0:10],keys[10:20],keys[20:25],[]],capped_pages
        finally:
            run(['ceph','config','rm','osd','osd_max_omap_entries_per_request'])
        write(a,'omap',lambda op: a.remove_omap_keys(op,('k03','absent')))
        cli('rmomapkey','omap','k04')
        write(a,'omap',lambda op: a.remove_omap_range2(op,'k10','k15'))
        left=omap_all(b,'omap')
        expected={k:v for k,v in vals.items() if k not in ('k03','k04') and not 'k10'<=k<'k15'}
        assert left==expected,sorted(left)
        assert header('omap')==head_bytes
        write(a,'omap',lambda op: a.clear_omap(op))
        cleared=omap_all(b,'omap')
        cleared_header=header('omap')
        assert cleared=={} and read(b,'omap')[0]==b'payload',cleared
        out={'keys':len(keys),'keys_only_values':sorted({repr(v) for _,v in keys_only}),'pages_of_7':[len(p) for p in key_pages],
             'value_pages_of_7':[len(p) for p in val_pages],'prefix_k1':prefixed,'by_keys':sorted(by_keys),
             'header_bytes':len(head_bytes),'setomapheader_via':header_via,'cap':10,'cap_polls':polls,'capped_pages_of_100':[len(p) for p in capped_pages],
             'after_removals':sorted(left),'after_clear':len(cleared),'header_after_clear':cleared_header.hex(),
             'by_keys_missing_object':code(lambda: omap_by_keys(b,'missing',['k00'])),
             'keys_missing_object':code(lambda: omap_keys(b,'missing','',10))}
    elif phase=='rollback':
        created=[]
        try:
            a.set_self_managed_snap_write([])
            original=bytes(range(256))*4
            a.write_full('object',original); a.set_xattr('object','attr',b'before')
            write(a,'object',lambda op: a.set_omap(op,('key',),(b'before',)))
            snapped_version=a.get_last_version()
            s1=a.create_self_managed_snap(); created.append(s1)
            a.set_self_managed_snap_write([s1])
            a.write('object',b'AFTER',100); a.append('object',b'TAIL')
            a.set_xattr('object','attr',b'after'); a.set_xattr('object','added',b'x')
            write(a,'object',lambda op: a.set_omap(op,('key','key2'),(b'after',b'new')))
            changed,changed_version=read(b,'object')
            assert changed!=original and len(changed)==len(original)+4
            a.write_full('born',b'born after the snapshot'); born_version=a.get_last_version()
            # The writer's last version stays at its previous write after a
            # rollback; the reader's HEAD version is the rollback's own.
            a.rollback_self_managed_snap('object',s1)
            head,rollback_version=read(b,'object')
            assert head==original and rollback_version>born_version>changed_version,(rollback_version,born_version,changed_version)
            snap,snap_version=read(b,'object',s1)
            assert snap==original and snap_version==snapped_version,(snap_version,snapped_version)
            assert xattrs(b,'object')=={'attr':b'before'},xattrs(b,'object')
            assert omap_all(b,'object')=={'key':b'before'},omap_all(b,'object')
            clones=json.loads(cli('listsnaps','object','--format','json').stdout)['clones']
            clones=[[str(c['id']),sorted(s['id'] for s in c['snapshots']),c['size']] for c in clones]
            assert clones==[[str(s1),[s1],len(original)],['head',[],len(original)]],clones
            # HEAD after the rollback is an ordinary object: a write changes
            # it, and the snapshot keeps its bytes.
            a.write('object',b'NEXT',0)
            next_head,next_version=read(b,'object')
            assert next_head==b'NEXT'+original[4:] and next_version==a.get_last_version()>rollback_version
            assert read(b,'object',s1)==(original,snapped_version)
            # Rolling back an object that did not exist at the snapshot
            # deletes HEAD.
            born=code(lambda: a.rollback_self_managed_snap('born',s1))
            assert born==0 and absent(b,'born'),born
            out={'snap':s1,'versions':{'snapped':snapped_version,'changed':changed_version,'born':born_version,'rollback':rollback_version,'next':next_version},
                 'head_after_rollback_bytes':len(head),'clones':clones,'born_after_snapshot':'deleted'}
        finally:
            a.set_self_managed_snap_write([])
            for snap in created:
                try: a.remove_self_managed_snap(snap)
                except rados.Error: pass
    elif phase=='hints':
        def object_info(name):
            # The OSD stores the object info as the xattr named "_", which a
            # getxattr of the empty name returns; ceph-dencoder decodes it.
            try: raw=b.get_xattr(name,'')
            except rados.Error as e: return {'unavailable':'getxattr of the empty name: errno %s'%e.errno}
            if not shutil.which('ceph-dencoder'): return {'unavailable':'no ceph-dencoder'}
            with tempfile.TemporaryDirectory() as d:
                path=os.path.join(d,'oi')
                with open(path,'wb') as f: f.write(raw)
                r=run(['ceph-dencoder','type','object_info_t','import',path,'decode','dump_json'],ok=False)
            if r.returncode!=0: return {'unavailable':'ceph-dencoder: '+r.stderr.decode(errors='replace')[-200:]}
            info=json.loads(r.stdout)
            return {k:info.get(k) for k in ('size','expected_object_size','expected_write_size','alloc_hint_flags')}
        a.write_full('hinted',b'h'*8192); before=a.get_last_version()
        cli('set-alloc-hint','hinted','4194304','65536')
        data,after=read(b,'hinted')
        assert data==b'h'*8192 and after>before,(after,before)
        cli('set-alloc-hint','hinted-new','1048576','4096')
        new_size=b.stat('hinted-new')[0]
        assert new_size==0,new_size
        hinted,hinted_new=object_info('hinted'),object_info('hinted-new')
        if 'unavailable' not in hinted:
            assert hinted['expected_object_size']==4194304 and hinted['expected_write_size']==65536,hinted
            assert hinted_new['expected_object_size']==1048576 and hinted_new['expected_write_size']==4096,hinted_new
        # fadvise flags apply to the operation they follow.
        payload=bytes(range(256))*16
        flags_write=rados.LIBRADOS_OP_FLAG_FADVISE_DONTNEED|rados.LIBRADOS_OP_FLAG_FADVISE_NOCACHE
        write(a,'advised',lambda op:(op.write_full(payload),op.set_flags(flags_write)))
        assert read(b,'advised')[0]==payload
        flags_read=rados.LIBRADOS_OP_FLAG_FADVISE_RANDOM|rados.LIBRADOS_OP_FLAG_FADVISE_WILLNEED
        def compared(data):
            with rados.ReadOpCtx() as op:
                op.cmpext(data,0); op.set_flags(flags_read); b.operate_read_op(op,'advised')
        match=code(lambda: compared(payload[:64]))
        mismatch=code(lambda: compared(payload[:9]+b'X'))
        assert match==0 and mismatch==rados.MAX_ERRNO+9,(match,mismatch)
        # rados cp (into the same namespace) copies with copy-from under SEQUENTIAL|NOCACHE on the
        # source and SEQUENTIAL|DONTNEED on the target.
        a.set_xattr('advised','attr',b'\x01\x02')
        write(a,'advised',lambda op: a.set_omap(op,('key',),(b'value',)))
        cli('cp','advised','copied','--target-nspace',namespace)
        copied=[read(b,'copied')[0]==payload,xattrs(b,'copied')=={'attr':b'\x01\x02'},omap_all(b,'copied')=={'key':b'value'}]
        assert copied==[True,True,True],copied
        out={'alloc_hint_existing':{'version':[before,after],'size':len(data),'object_info':hinted},
             'alloc_hint_missing':{'size':new_size,'object_info':hinted_new},
             'fadvise_write_flags':flags_write,'fadvise_read_flags':flags_read,'cmpext_match':match,
             'cmpext_mismatch':mismatch,'max_errno':rados.MAX_ERRNO,'copy_from_data_xattrs_omap':copied,
             'sparse_read':'not reachable: python-rados and the rados CLI issue no sparse read'}
    elif phase=='stats':
        size=int(sys.argv[4])
        def ceph_df():
            df=json.loads(run(['ceph','df','--format','json']).stdout)
            mine=[p['stats'] for p in df['pools'] if p['name']==pool][0]
            return df['stats'],mine,sum(p['stats']['objects'] for p in df['pools'])
        def rados_df():
            df=json.loads(run(['rados','df','--format','json']).stdout)
            return [p for p in df['pools'] if p['name']==pool][0]
        def view():
            stats=b.get_stats(); cluster=oracle.get_cluster_stats()
            total,mine,objects=ceph_df(); cli_view=rados_df()
            return {'pool':{k:stats[k] for k in ('num_objects','num_object_copies','num_bytes','num_kb','num_wr','num_wr_kb','num_object_clones')},
                    'cluster':cluster,'rados_df':{k:cli_view[k] for k in ('num_objects','size_bytes') if k in cli_view},
                    'ceph_df':{'objects':mine['objects'],'stored':mine['stored'],'total_bytes':total['total_bytes'],
                               'total_avail_bytes':total['total_avail_bytes'],'all_pool_objects':objects}}
        def settled(objects,stored):
            def check():
                v=view(); p,c,d=v['pool'],v['cluster'],v['ceph_df']
                ok=(p['num_objects']==objects and p['num_object_copies']==objects*size and p['num_bytes']>=stored
                    and p['num_kb']==(p['num_bytes']+1023)//1024 and v['rados_df'].get('num_objects')==objects
                    and d['objects']==objects and d['stored']==stored and c['num_objects']==d['all_pool_objects']
                    and c['kb']==d['total_bytes']>>10 and 0<c['kb_avail']<=c['kb'])
                return ok,v
            return wait_for('statistics of %d objects, %d bytes'%(objects,stored),check)
        baseline,_=settled(0,0)
        sizes={'stat-%d'%i:4096*i for i in range(1,7)}
        for name,n in sizes.items(): a.write_full(name,bytes([i%251 for i in range(n)]))
        written,polls_written=settled(len(sizes),sum(sizes.values()))
        for name in ('stat-1','stat-2','stat-6'): a.remove_object(name)
        removed,polls_removed=settled(3,4096*(3+4+5))
        assert written['pool']['num_wr']-baseline['pool']['num_wr']>=len(sizes),(written['pool'],baseline['pool'])
        delta=lambda x,y,k: x['cluster'][k]-y['cluster'][k]
        out={'size':size,'baseline':baseline,'written':written,'removed':removed,'polls':[polls_written,polls_removed],
             'cluster_object_delta':[delta(written,baseline,'num_objects'),delta(removed,written,'num_objects')],
             'pool_wr_delta':written['pool']['num_wr']-baseline['pool']['num_wr']}
    else:
        raise SystemExit('unknown phase '+phase)
    print(json.dumps(out))
finally:
    a.close(); b.close(); writer.shutdown(); oracle.shutdown()
`

// radosObjectReceiptPhase runs one phase of radosObjectReceiptScript and
// decodes its receipt into out.
func radosObjectReceiptPhase(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, out any, phase string, args ...string) json.RawMessage {
	t.Helper()
	result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
		Argv: append([]string{"python3", "-c", radosObjectReceiptScript, phase}, args...), Timeout: 5 * time.Minute,
		MaxStdout: 1 << 20, MaxStderr: 1 << 20, Label: "rados object " + phase,
	})
	if err != nil {
		t.Fatalf("%s phase: %v", phase, err)
	}
	if !result.Complete() || result.ExitCode != 0 || json.Unmarshal(result.Stdout, out) != nil {
		t.Fatalf("%s phase: exit %d, complete %t\nstdout: %s\nstderr: %s", phase, result.ExitCode, result.Complete(), result.Stdout, result.Stderr)
	}
	return result.Stdout
}
