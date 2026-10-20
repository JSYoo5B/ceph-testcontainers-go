//go:build all || (integration && features)

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// radosEnumerationScript is the native oracle of PG object enumeration,
// built from python-rados, the rados and ceph CLIs and the image's librados
// through ctypes. Given a replicated pool with a power-of-two pg_num, it:
//
//   - maps candidate object names with the MON's osd map command until every
//     PG of the pool holds at least seven objects of namespace enum-a, writes
//     them and eight objects of namespace enum-b, and tags some with the xattr
//     tag=blue;
//   - lowers osd_max_pgls on every OSD to 3 with ceph tell and reads it back,
//     so that one PGNLS reply carries at most three entries;
//   - lists namespace enum-a with rados ls under debug_ms=1 and
//     debug_objecter=20 and reads every PGNLS request from the CLI's log: the
//     cursor (pos) the Objecter sent, the PG and namespace of the osd_op, the
//     reply's result, entries and handle. It checks that PGs are visited in
//     hash order, each continued from the previous handle until a result of 1
//     with the handle at the next PG's start, and each PG's entries against
//     the osd map placement and rados --pgid ls;
//   - lists every namespace with rados ls --all;
//   - walks each PG's cursor range from rados_object_list_slice with
//     rados_object_list in chunks of two under a plain xattr filter
//     (PGNLS_FILTER), comparing successive cursors, and an unknown filter
//     type that the OSD refuses.
//
// Capabilities are read from the tools, not inferred from the release: the
// rados CLI's usage text must list --pgid and --all, and librados must
// export the cursor API, or the script fails before it writes anything. The
// osd_max_pgls value read before the change is restored. Both clients are
// shut down; nothing is retried.
const radosEnumerationScript = `import ctypes,json,math,re,shutil,struct,subprocess,sys,tempfile,rados
pool=sys.argv[1]
CAP,PER_PG,CHUNK,NS,OTHER='3',7,2,'enum-a','enum-b'
def run(argv,ok=True):
    r=subprocess.run(argv,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=60)
    assert not ok or r.returncode==0,(argv,r.returncode,r.stderr[-400:])
    return r
def ceph_json(*args): return json.loads(run(['ceph','--connect-timeout','20']+list(args)+['--format','json']).stdout)
def rev32(x): return int('{:032b}'.format(x)[::-1],2)
def seed(pg): return int(pg.split('.')[1],16)
usage=run(['rados','--help'],ok=False)
usage=(usage.stdout+usage.stderr).decode(errors='replace')
assert '--pgid' in usage and '--all' in usage,'the rados CLI lists no --pgid or --all option'
lib=ctypes.CDLL('librados.so.2')
for name in ('rados_object_list','rados_object_list_slice','rados_object_list_cursor_cmp','rados_object_list_is_end'):
    assert hasattr(lib,name),'librados exports no '+name
VP,SZ=ctypes.c_void_p,ctypes.c_size_t
class Item(ctypes.Structure):
    _fields_=[('oid_length',SZ),('oid',VP),('nspace_length',SZ),('nspace',VP),('locator_length',SZ),('locator',VP)]
def sig(name,res,*args):
    f=getattr(lib,name); f.restype=res; f.argtypes=list(args); return f
create2=sig('rados_create2',ctypes.c_int,ctypes.POINTER(VP),ctypes.c_char_p,ctypes.c_char_p,ctypes.c_uint64)
conf_read=sig('rados_conf_read_file',ctypes.c_int,VP,ctypes.c_char_p)
conf_set=sig('rados_conf_set',ctypes.c_int,VP,ctypes.c_char_p,ctypes.c_char_p)
connect=sig('rados_connect',ctypes.c_int,VP)
shutdown=sig('rados_shutdown',None,VP)
ioctx_create=sig('rados_ioctx_create',ctypes.c_int,VP,ctypes.c_char_p,ctypes.POINTER(VP))
ioctx_destroy=sig('rados_ioctx_destroy',None,VP)
set_namespace=sig('rados_ioctx_set_namespace',None,VP,ctypes.c_char_p)
list_begin=sig('rados_object_list_begin',VP,VP)
list_end=sig('rados_object_list_end',VP,VP)
is_end=sig('rados_object_list_is_end',ctypes.c_int,VP,VP)
cursor_free=sig('rados_object_list_cursor_free',None,VP,VP)
cursor_cmp=sig('rados_object_list_cursor_cmp',ctypes.c_int,VP,VP,VP)
object_list=sig('rados_object_list',ctypes.c_int,VP,VP,VP,SZ,ctypes.c_char_p,SZ,ctypes.POINTER(Item),ctypes.POINTER(VP))
object_list_free=sig('rados_object_list_free',None,SZ,ctypes.POINTER(Item))
object_list_slice=sig('rados_object_list_slice',None,VP,VP,VP,SZ,SZ,ctypes.POINTER(VP),ctypes.POINTER(VP))
def text(p,n): return ctypes.string_at(p,n).decode() if n else ''
def encoded(s): return struct.pack('<I',len(s))+s
# A PGNLS osd_op the client sent (tid, PG, target object with the
# namespace, operation) and the OSD's reply (tid, operation, result).
SENT=r'--> \S+ -- osd_op\(\S+:(\d+) (\d+\.[0-9a-f]+) (\S+) \[(pgnls(?:-filter)?) start_epoch \d+[^\]\n]*\]'
REPLY=r'<== osd\.\d+ \S+ \d+ ==== osd_op_reply\((\d+) [^\[\n]*\[(pgnls(?:-filter)?) [^\]\n]*\][^=\n]*= (-?\d+)'
def wire(log):
    with open(log,errors='replace') as f: t=f.read()
    replies={m.group(1):[m.group(2),int(m.group(3))] for m in re.finditer(REPLY,t)}
    sent=[m.groups() for m in re.finditer(SENT,t)]
    assert all(replies.get(tid,[None])[0]==op for tid,_,_,op in sent),(sent,replies)
    return t,[{'tid':int(tid),'pg':pg,'nspace':target.split(':')[2],'op':op,'result':replies[tid][1]} for tid,pg,target,op in sent]
writer=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_osd_op_timeout':'20','rados_mon_op_timeout':'20'})
writer.connect()
d=tempfile.mkdtemp()
cluster,io=VP(),VP()
ioctxs=[]; cursors=[]; out={}
try:
    pool_id=writer.pool_lookup(pool)
    pg_num=ceph_json('osd','pool','get',pool,'pg_num')['pg_num']
    assert pg_num>1 and pg_num&(pg_num-1)==0,pg_num
    listed_pgs=ceph_json('pg','ls-by-pool',pool)
    listed_pgs=listed_pgs['pg_stats'] if isinstance(listed_pgs,dict) else listed_pgs
    pgs={p['pgid'] for p in listed_pgs}
    assert pgs=={'%d.%x'%(pool_id,s) for s in range(pg_num)},sorted(pgs)
    assert all('active' in p['state'] and 'clean' in p['state'] for p in listed_pgs),[p['state'] for p in listed_pgs]
    # Bitwise hash order: a PG starts at the bit-reversed seed and ends where
    # the next PG starts (MAX for the last).
    order=sorted(pgs,key=lambda pg: rev32(seed(pg)))
    span=(1<<32)//pg_num
    def pg_end(pg):
        e=rev32(seed(pg))+span
        return 'MAX' if e==1<<32 else '%d:%08x::::head'%(pool_id,e)
    def placed(nspace,name):
        r,o,e=writer.mon_command(json.dumps({'prefix':'osd map','pool':pool,'object':name,'nspace':nspace,'format':'json'}),b'')
        assert r==0,(r,e)
        return json.loads(o)['pgid']
    placement={pg:{NS:[],OTHER:[],'blue':[]} for pg in pgs}
    i=0
    while min(len(v[NS]) for v in placement.values())<PER_PG:
        name='obj-%03d'%i; placement[placed(NS,name)][NS].append(name); i+=1
        assert i<400,placement
    for j in range(8):
        name='other-%02d'%j; placement[placed(OTHER,name)][OTHER].append(name)
    a=writer.open_ioctx(pool); b=writer.open_ioctx(pool); ioctxs+=[a,b]
    a.set_namespace(NS); b.set_namespace(OTHER)
    tags={}
    for pg,v in placement.items():
        for k,name in enumerate(v[NS]):
            a.write_full(name,name.encode())
            tag=(b'blue',b'red',None)[k%3]
            if tag: a.set_xattr(name,'tag',tag)
            if tag==b'blue': v['blue'].append(name)
            tags[NS+'/'+name]=tag
        for name in v[OTHER]:
            b.write_full(name,name.encode()); b.set_xattr(name,'tag',b'blue')
            tags[OTHER+'/'+name]=b'blue'
    osds=[int(x) for x in ceph_json('osd','ls')]
    def osd_cap(o): return str(ceph_json('tell','osd.%d'%o,'config','get','osd_max_pgls')['osd_max_pgls'])
    cap_before={o:osd_cap(o) for o in osds}
    try:
        for o in osds: run(['ceph','--connect-timeout','20','tell','osd.%d'%o,'config','set','osd_max_pgls',CAP])
        cap_after={o:osd_cap(o) for o in osds}
        assert all(v==CAP for v in cap_after.values()),cap_after
        cap=int(CAP)
        # Pool listing of one namespace: the CLI's Objecter chains PGNLS
        # requests PG by PG; its log has the cursor and reply of each.
        log=d+'/ls.log'
        listed=run(['rados','-p',pool,'-N',NS,'ls','--log_file='+log,'--log_to_file=true','--log_to_stderr=false','--err_to_stderr=false','--debug_ms=1','--debug_objecter=20']).stdout.decode().split()
        t,sent=wire(log)
        positions=re.findall(r'list_nobjects pool_id \d+ pool_snap_seq \S+ max_entries \d+ list_context \S+ onfinish \S+ current_pg \d+ pos (\S+)',t)
        responses=re.findall(r' response\.entries\.size (\d+), response\.entries \[([^\]\n]*)\], handle (\S+), tentative new pos (\S+)',t)
        ends=[p for p in positions if p=='MAX']
        positions=[p for p in positions if p!='MAX']
        assert ends and len(positions)==len(sent)==len(responses),(len(positions),len(sent),len(responses),len(ends),[l for l in t.splitlines() if 'pgnls' in l][:6])
        steps=[]
        for pos,s,(size,entries,handle,_) in zip(positions,sent,responses):
            entries=[e for e in entries.split(',') if e]
            assert len(entries)==int(size) and len(entries)<=cap,(size,entries)
            assert s['op']=='pgnls' and s['nspace']==NS and all(e.startswith(NS+'/') for e in entries),(s,entries)
            steps.append({'pg':s['pg'],'pos':pos,'tid':s['tid'],'op':s['op'],'result':s['result'],'entries':entries,'handle':handle})
        visited=[pg for k,pg in enumerate(s['pg'] for s in steps) if k==0 or steps[k-1]['pg']!=pg]
        assert visited==order,(visited,order)
        assert steps[0]['pos']=='MIN' and steps[-1]['handle']=='MAX',(steps[0],steps[-1])
        for k,s in enumerate(steps):
            if k: assert s['pos']==steps[k-1]['handle'],(steps[k-1],s)
            last=k==len(steps)-1 or steps[k+1]['pg']!=s['pg']
            if last:
                assert s['result']==1 and s['handle']==pg_end(s['pg']),s
                continue
            assert s['result']==0,s
            # A continuation handle names the first object not yet returned:
            # one of this PG, in either namespace; one of enum-a is the next
            # reply's first entry.
            _,_,hns,_,hname,_=s['handle'].split(':')
            assert hname in placement[s['pg']].get(hns,[]),s
            if hns==NS: assert steps[k+1]['entries'][0]==NS+'/'+hname,(s,steps[k+1])
        per_pg_cli={}
        for pg in order:
            got=[e.split('/',1)[1] for s in steps if s['pg']==pg for e in s['entries']]
            v=placement[pg]; total=len(v[NS])+len(v[OTHER])
            replies=sum(1 for s in steps if s['pg']==pg)
            assert sorted(got)==sorted(v[NS]) and len(got)==len(set(got)),(pg,got,v)
            assert len(v[NS])>cap and math.ceil(total/cap)<=replies<=math.ceil(total/cap)+1,(pg,replies,total)
            per_pg_cli[pg]=sorted(run(['rados','-p',pool,'-N',NS,'--pgid',pg,'ls']).stdout.decode().split())
            assert per_pg_cli[pg]==sorted(v[NS]),(pg,per_pg_cli[pg])
        assert sorted(listed)==sorted(n for v in placement.values() for n in v[NS]),listed
        every=sorted(o['namespace']+'/'+o['name'] for o in json.loads(run(['rados','-p',pool,'--all','ls','--format','json']).stdout))
        assert every==sorted(tags),every
        # The cursor API over each PG's range, in chunks of two, under a
        # plain filter (xattr "_tag", the OSD's name for the user xattr tag,
        # equal to blue) and in namespace enum-a.
        log=d+'/cursor.log'
        assert create2(ctypes.byref(cluster),b'ceph',b'client.admin',0)==0
        assert conf_read(cluster,b'/etc/ceph/ceph.conf')==0
        for k,v in (('keyring','/etc/ceph/ceph.client.admin.keyring'),('log_file',log),('log_to_file','true'),('log_to_stderr','false'),
                    ('err_to_stderr','false'),('debug_ms','1'),('rados_osd_op_timeout','20'),('rados_mon_op_timeout','20')):
            assert conf_set(cluster,k.encode(),v.encode())==0,k
        assert connect(cluster)==0
        assert ioctx_create(cluster,pool.encode(),ctypes.byref(io))==0
        set_namespace(io,NS.encode())
        def cursor():
            c=VP(list_begin(io)); cursors.append(c); return c
        first=cursor(); last=VP(list_end(io)); cursors.append(last)
        blue=encoded(b'plain')+encoded(b'_tag')+encoded(b'blue')
        walks={}
        for n in range(pg_num):
            start,finish=cursor(),cursor()
            object_list_slice(io,first,last,n,pg_num,ctypes.byref(start),ctypes.byref(finish))
            pg='%d.%x'%(pool_id,rev32(n*span))
            calls=[]; cmps=[]; cur=start
            while True:
                nxt=cursor(); items=(Item*CHUNK)()
                r=object_list(io,cur,finish,CHUNK,blue,len(blue),items,ctypes.byref(nxt))
                assert 0<=r<=CHUNK,(pg,r)
                calls.append([text(items[x].nspace,items[x].nspace_length)+'/'+text(items[x].oid,items[x].oid_length) for x in range(r)])
                object_list_free(CHUNK,items)
                cmps.append(cursor_cmp(io,cur,nxt)); cur=nxt
                if cursor_cmp(io,cur,finish)==0: break
                assert len(calls)<64,calls
            got=[e for c in calls for e in c]
            assert sorted(got)==sorted(NS+'/'+x for x in placement[pg]['blue']) and all(c==-1 for c in cmps),(pg,calls,cmps)
            walks[pg]={'slice':n,'calls':calls,'cursor_cmp':cmps,'finish_cmp':cursor_cmp(io,cur,finish),'is_end':is_end(io,cur)}
        assert [walks[pg]['is_end'] for pg in order]==[0]*(pg_num-1)+[1],walks
        nxt=cursor(); items=(Item*CHUNK)()
        unknown=encoded(b'nosuch')
        refused=object_list(io,first,last,CHUNK,unknown,len(unknown),items,ctypes.byref(nxt))
        object_list_free(CHUNK,items)
        for c in cursors: cursor_free(io,c)
        cursors=[]
        ioctx_destroy(io); io=VP()
        shutdown(cluster); cluster=VP()
        _,sent=wire(log)
        refused_wire=[[s['pg'],s['op'],s['result']] for s in sent if s['result']<0]
        sent=[s for s in sent if s['result']>=0]
        assert refused==-22 and refused_wire==[[order[0],'pgnls-filter',-22]],(refused,refused_wire)
        for pg in order:
            ops=[[s['op'],s['result']] for s in sent if s['pg']==pg]
            walks[pg]['wire']=ops
            assert ops and all(op=='pgnls-filter' for op,_ in ops) and [r for _,r in ops]==[0]*(len(ops)-1)+[1],(pg,ops)
            assert len(ops)>=len(walks[pg]['calls']),(pg,ops)
        assert all(s['nspace']==NS for s in sent),sent
        out={'pool_id':pool_id,'pg_num':pg_num,'cap':cap,'cap_before':cap_before,'pgs':order,'pg_ends':{pg:pg_end(pg) for pg in order},
             'placement':placement,'listing':steps,'listing_end_requests':len(ends),'listed':sorted(listed),
             'per_pg_cli':per_pg_cli,'all_namespaces':every,'cursor':walks,
             'filter':{'type':'plain','xattr':'tag','value':'blue','unknown':{'type':'nosuch','result':refused,'wire':refused_wire}},
             'rados_ls_filter':'not reachable: the rados CLI and python-rados take no listing filter; the filter goes through rados_object_list'}
    finally:
        for o,v in cap_before.items(): run(['ceph','--connect-timeout','20','tell','osd.%d'%o,'config','set','osd_max_pgls',v])
    print(json.dumps(out))
finally:
    for c in cursors: cursor_free(io,c)
    if io.value: ioctx_destroy(io)
    if cluster.value: shutdown(cluster)
    for x in ioctxs: x.close()
    writer.shutdown()
    shutil.rmtree(d,ignore_errors=True)
`

// radosEnumerationStep is one PGNLS request of a pool listing as the
// client's log reports it.
type radosEnumerationStep struct {
	PG      string   `json:"pg"`
	Pos     string   `json:"pos"`
	Op      string   `json:"op"`
	Result  int      `json:"result"`
	Entries []string `json:"entries"`
	Handle  string   `json:"handle"`
}

// radosEnumerationReceipt is the receipt radosEnumerationScript prints.
type radosEnumerationReceipt struct {
	Cap       int                            `json:"cap"`
	PGs       []string                       `json:"pgs"`
	PGEnds    map[string]string              `json:"pg_ends"`
	Placement map[string]map[string][]string `json:"placement"`
	Listing   []radosEnumerationStep         `json:"listing"`
	PerPGCLI  map[string][]string            `json:"per_pg_cli"`
	AllNS     []string                       `json:"all_namespaces"`
	Cursor    map[string]struct {
		Calls     [][]string `json:"calls"`
		CursorCmp []int      `json:"cursor_cmp"`
		FinishCmp int        `json:"finish_cmp"`
		IsEnd     int        `json:"is_end"`
		Wire      [][]any    `json:"wire"`
	} `json:"cursor"`
	Filter struct {
		Unknown struct {
			Result int `json:"result"`
		} `json:"unknown"`
	} `json:"filter"`
}

// problem checks the receipt independently of the script and names the
// first condition it breaks, or returns "".
func (r *radosEnumerationReceipt) problem() string {
	const ns = "enum-a/"
	sorted := func(names []string) []string { return slices.Sorted(slices.Values(names)) }
	if r.Cap != 3 || len(r.PGs) != 4 || len(r.Listing) == 0 || r.Listing[0].Pos != "MIN" {
		return "pool, cap or first cursor"
	}
	visited, objects := []string{}, 0
	for i, step := range r.Listing {
		if step.Op != "pgnls" || len(step.Entries) > r.Cap || (i > 0 && step.Pos != r.Listing[i-1].Handle) {
			return fmt.Sprintf("listing step %d", i)
		}
		if len(visited) == 0 || visited[len(visited)-1] != step.PG {
			visited = append(visited, step.PG)
		}
	}
	if !slices.Equal(visited, r.PGs) {
		return "PG order"
	}
	for _, pg := range r.PGs {
		var steps []radosEnumerationStep
		var listed []string
		for _, step := range r.Listing {
			if step.PG == pg {
				steps = append(steps, step)
				for _, entry := range step.Entries {
					listed = append(listed, strings.TrimPrefix(entry, ns))
				}
			}
		}
		placed := r.Placement[pg]["enum-a"]
		objects += len(placed) + len(r.Placement[pg]["enum-b"])
		last := steps[len(steps)-1]
		if len(placed) <= r.Cap || len(steps)*r.Cap < len(placed) || last.Result != 1 || last.Handle != r.PGEnds[pg] ||
			!slices.Equal(sorted(listed), sorted(placed)) || !slices.Equal(r.PerPGCLI[pg], sorted(placed)) {
			return "listing of " + pg
		}
		for _, step := range steps[:len(steps)-1] {
			if step.Result != 0 {
				return "continuation in " + pg
			}
		}
		walk := r.Cursor[pg]
		var filtered, blue []string
		for _, call := range walk.Calls {
			if len(call) > 2 {
				return "cursor chunk in " + pg
			}
			filtered = append(filtered, call...)
		}
		for _, name := range r.Placement[pg]["blue"] {
			blue = append(blue, ns+name)
		}
		if !slices.Equal(sorted(filtered), sorted(blue)) || walk.FinishCmp != 0 || (walk.IsEnd == 1) != (pg == r.PGs[len(r.PGs)-1]) ||
			len(walk.Wire) < len(walk.Calls) || slices.ContainsFunc(walk.CursorCmp, func(c int) bool { return c != -1 }) {
			return "cursor walk of " + pg
		}
		// Every filtered request but the PG's last continues (0); the last
		// ends the PG (1).
		for i, op := range walk.Wire {
			want := 0.0
			if i == len(walk.Wire)-1 {
				want = 1
			}
			if len(op) != 2 || op[0] != "pgnls-filter" || op[1] != want {
				return "filtered PGNLS of " + pg
			}
		}
	}
	if len(r.AllNS) != objects || r.Filter.Unknown.Result != -22 {
		return "all namespaces or unknown filter"
	}
	return ""
}

// radosEnumerationPhase runs radosEnumerationScript against pool and decodes
// its receipt into out.
func radosEnumerationPhase(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, out any, pool string) json.RawMessage {
	t.Helper()
	result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
		Argv: []string{"python3", "-c", radosEnumerationScript, pool}, Timeout: 5 * time.Minute,
		MaxStdout: 1 << 20, MaxStderr: 1 << 20, Label: "rados enumeration",
	})
	if err != nil {
		t.Fatalf("enumeration phase: %v", err)
	}
	if !result.Complete() || result.ExitCode != 0 || json.Unmarshal(result.Stdout, out) != nil {
		t.Fatalf("enumeration phase: exit %d, complete %t\nstdout: %s\nstderr: %s", result.ExitCode, result.Complete(), result.Stdout, result.Stderr)
	}
	return result.Stdout
}
