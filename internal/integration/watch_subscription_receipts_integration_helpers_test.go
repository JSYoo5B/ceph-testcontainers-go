//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// watchSubscriptionScript is the native oracle of watch/notify and of a
// client's MON subscriptions, built from python-rados and the rados and ceph
// CLIs of the official image. Each phase asserts what it can itself, prints
// one JSON receipt and shuts every client down.
//
// Phases, each given the pool:
//   - watch: two acknowledging watchers (one with a payload naming the notify
//     id, one with an empty payload) and a notifier, each its own client.
//     Notify acks, cookies, notifier and notify ids as both sides report
//     them; rados listwatchers lists the same clients and cookies. A third,
//     silent watcher never acknowledges: python-rados reports only ETIMEDOUT,
//     and a rados notify process lists the two acks and the silent watcher's
//     timeout. Blocklisting one watcher fires its error callback with its own
//     cookie and drops its watch; removing the object fires the error
//     callback of the remaining watchers.
//   - subscriptions: one client with an admin socket and a messenger log
//     holds a watch (so its Objecter keeps asking for the next OSD map) and
//     subscribes to the cluster log. Its sent mon_subscribe messages and the
//     maps, config and log entries the MON delivers are read from its log;
//     an OSD map epoch bump reaches it, as its Objecter counters and the MON's
//     own session dump both report; a client config change and a cluster log
//     entry arrive while it runs. Then ceph mon set_location bumps the
//     MonMap epoch, and its MonClient decodes that epoch with the new
//     location; ceph mgr fail bumps the MgrMap epoch, and its MgrClient takes
//     each epoch up to the replacement active MGR and opens a session with
//     it. Both arrive as map messages from the MON that holds its session.
//
// Capabilities are detected, not inferred from the release: the
// python-rados calls, the rados CLI commands and the MON commands must exist
// (the CLI's usage text and the MONs' command list are read, read-only
// checks), or the phase fails before it changes anything. Nothing is retried
// after a failed command.
const watchSubscriptionScript = `import json,os,random,re,shutil,subprocess,sys,tempfile,threading,time,rados
phase,pool=sys.argv[1],sys.argv[2]
def connected(extra=None):
    conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_osd_op_timeout':'20','rados_mon_op_timeout':'20'}
    conf.update(extra or {})
    c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf=conf)
    c.connect(); return c
def run(argv,ok=True):
    r=subprocess.run(argv,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=90)
    assert not ok or r.returncode==0,(argv,r.returncode,r.stderr[-400:])
    return r
def ceph_json(*args): return json.loads(run(['ceph','--connect-timeout','20']+list(args)+['--format','json']).stdout)
def wait_for(what,check,timeout=90):
    deadline=time.monotonic()+timeout; polls=0; last=None
    while True:
        polls+=1; ok,last=check()
        if ok: return last,polls
        assert time.monotonic()<deadline,('timed out waiting for '+what,last)
        time.sleep(0.5)
for name in ('watch','aio_notify','remove_object'):
    assert hasattr(rados.Ioctx,name),'python-rados lacks Ioctx.'+name
assert hasattr(rados.Rados,'monitor_log2'),'python-rados lacks Rados.monitor_log2'
usage=run(['rados','--help'],ok=False)
usage=(usage.stdout+usage.stderr).decode(errors='replace')
for command in ('listwatchers <obj-name>','notify <obj-name> <message>'):
    assert command in usage,'the rados CLI lists no '+command
clients=[]; watches=[]
try:
    if phase=='watch':
        obj='watched'; lock=threading.Lock()
        notifier=connected(); clients.append(notifier)
        nio=notifier.open_ioctx(pool); nio.write_full(obj,b'watched')
        def watcher(name,reply):
            c=connected(); clients.append(c); io=c.open_ioctx(pool)
            w={'name':name,'rados':c,'io':io,'id':c.get_instance_id(),'seen':[],'errors':[]}
            def on_notify(notify_id,notifier_id,cookie,data):
                with lock: w['seen'].append({'notify_id':notify_id,'notifier':notifier_id,'cookie':cookie,'data':(data or b'').hex()})
                if reply is None:
                    # No return, no notify_ack: python-rados acknowledges only
                    # after the callback returns.
                    raise RuntimeError(name+' does not acknowledge')
                return reply(notify_id)
            def on_error(cookie,err):
                with lock: w['errors'].append({'cookie':cookie,'err':err,'at':time.monotonic()})
            w['watch']=io.watch(obj,on_notify,on_error); watches.append(w['watch']); w['cookie']=w['watch'].get_id()
            return w
        def listed():
            out=run(['rados','-p',pool,'listwatchers',obj]).stdout.decode()
            return sorted((int(m.group(2)),int(m.group(3)),m.group(1)) for m in re.finditer(r'watcher=(\S+) client\.(\d+) cookie=(\d+)',out))
        def pairs(ws): return sorted((w['id'],w['cookie']) for w in ws)
        def notify(msg,timeout_ms):
            done=threading.Event(); res={}
            def cb(comp,r,acks,timeouts):
                res.update(r=r,acks=acks,timeouts=timeouts); done.set()
            nio.aio_notify(obj,cb,msg=msg,timeout_ms=timeout_ms)
            assert done.wait(timeout_ms/1000+60),'notify did not complete'
            acks=None if res['acks'] is None else sorted([a[0],a[1],(a[2] or b'').hex() if isinstance(a[2],bytes) else a[2].encode().hex()] for a in res['acks'])
            timeouts=None if res['timeouts'] is None else sorted([t[0],t[1]] for t in res['timeouts'])
            return res['r'],acks,timeouts
        w1=watcher('w1',lambda nid: b'w1:%d'%nid)
        w2=watcher('w2',lambda nid: b'')
        acking=[w1,w2]
        assert [(i,c) for i,c,_ in listed()]==pairs(acking),(listed(),pairs(acking))
        out={'notifier':notifier.get_instance_id(),'watchers':{w['name']:{'client':w['id'],'cookie':w['cookie']} for w in acking}}
        rounds=[]
        # python-rados takes the message as str and sends len(str) bytes of
        # its UTF-8 encoding, so the payload stays ASCII (with a NUL byte).
        for msg in ('n1\x00z','n2'):
            r,acks,timeouts=notify(msg,10000)
            seen=[w['seen'][-1] for w in acking]
            nid=seen[0]['notify_id']
            assert r==0 and timeouts==[],(r,acks,timeouts)
            assert all(s['notify_id']==nid and s['notifier']==out['notifier'] and s['data']==msg.encode().hex() for s in seen),seen
            assert [s['cookie'] for s in seen]==[w1['cookie'],w2['cookie']],seen
            assert acks==sorted([[w1['id'],w1['cookie'],(b'w1:%d'%nid).hex()],[w2['id'],w2['cookie'],'']]),(acks,nid)
            rounds.append({'notify_id':nid,'payload':msg.encode().hex(),'acks':acks,'timeouts':timeouts})
        assert rounds[0]['notify_id']!=rounds[1]['notify_id'],rounds
        out['all_acked']=rounds
        w3=watcher('w3',None)
        assert [(i,c) for i,c,_ in listed()]==pairs(acking+[w3]),listed()
        out['watchers']['w3']={'client':w3['id'],'cookie':w3['cookie']}
        start=time.monotonic(); r,acks,timeouts=notify('n3',3000)
        # python-rados decodes acks and timeouts only for a notify that
        # returned 0, so it reports the missed ack as ETIMEDOUT alone.
        assert r==-110 and acks is None and timeouts is None,(r,acks,timeouts)
        python_missed={'r':r,'seconds':round(time.monotonic()-start,1),'notify_id':w3['seen'][-1]['notify_id']}
        cli=run(['rados','-p',pool,'notify',obj,'n4'],ok=False)
        text=cli.stdout.decode(errors='replace'); err=cli.stderr.decode(errors='replace')
        replies=sorted([int(m.group(1)),int(m.group(2)),int(m.group(3))] for m in re.finditer(r'^reply client\.(\d+) cookie (\d+) : (\d+) bytes$',text,re.M))
        missed=sorted([int(m.group(1)),int(m.group(2))] for m in re.finditer(r'^timeout client\.(\d+) cookie (\d+)$',text,re.M))
        cli_nid=w3['seen'][-1]['notify_id']
        # The CLI encodes its message as a length-prefixed string.
        assert all(w['seen'][-1]['notify_id']==cli_nid and w['seen'][-1]['data']==(b'\x02\x00\x00\x00n4').hex() for w in acking+[w3]),[w['seen'][-1] for w in acking+[w3]]
        assert cli.returncode!=0 and '(110)' in err,(cli.returncode,err)
        assert replies==sorted([[w1['id'],w1['cookie'],len(b'w1:%d'%cli_nid)],[w2['id'],w2['cookie'],0]]),(replies,text)
        assert missed==[[w3['id'],w3['cookie']]],(missed,text)
        assert [(i,c) for i,c,_ in listed()]==pairs(acking+[w3]),listed()
        out['missed_ack']={'python':python_missed,'cli':{'notify_id':cli_nid,'exit':cli.returncode,'error':err.strip().splitlines()[-1],'replies':replies,'timeouts':missed},
                           'silent_watcher_notifies':len(w3['seen']),'silent_watcher_still_listed':True}
        # Blocklisting w2 breaks its session: its error callback fires with
        # its own cookie and the OSD drops its watch; the others keep theirs.
        addr=[a for i,c,a in listed() if i==w2['id']][0]
        blocked=time.monotonic()
        run(['ceph','--connect-timeout','20','osd','blocklist','add',addr,'600'])
        try:
            errors,_=wait_for('the blocklisted watcher error callback',lambda:(len(w2['errors'])>0,list(w2['errors'])))
            after,_=wait_for('the blocklisted watch to go',lambda:(all(i!=w2['id'] for i,_,_ in listed()),listed()))
            assert errors[0]['cookie']==w2['cookie'] and errors[0]['err'] in (-107,-108),errors
            assert not w1['errors'] and not w3['errors'],(w1['errors'],w3['errors'])
            assert [(i,c) for i,c,_ in after]==pairs([w1,w3]),after
            out['blocklisted']={'client':w2['id'],'cookie':w2['cookie'],'error':errors[0]['err'],'after_seconds':round(errors[0]['at']-blocked,1),
                                'listed_after':[[i,c] for i,c,_ in after]}
        finally:
            run(['ceph','--connect-timeout','20','osd','blocklist','rm',addr])
        # Removing the object disconnects every remaining watch.
        nio.remove_object(obj)
        errors,_=wait_for('the error callbacks after removal',lambda:(bool(w1['errors']) and bool(w3['errors']),[w1['errors'],w3['errors']]))
        assert [(e[0]['cookie'],e[0]['err']) for e in errors]==[(w1['cookie'],-107),(w3['cookie'],-107)],errors
        assert len(w2['errors'])==1,w2['errors']
        out['object_removed']={w['name']:{'cookie':w['errors'][0]['cookie'],'error':w['errors'][0]['err']} for w in (w1,w3)}
        for w in (w1,w2,w3):
            try: w['watch'].close()
            except rados.Error as e: out.setdefault('close_errors',{})[w['name']]=getattr(e,'errno',None)
            w['io'].close()
        nio.close()
    elif phase=='subscriptions':
        # The MonMap and MgrMap changes need these MON commands; the MONs'
        # own command list is read, which changes nothing.
        for prefix,command in (('mon','mon set_location <name>'),('mgr','mgr fail')):
            listed_commands=run(['ceph','--connect-timeout','20',prefix,'-h'],ok=False)
            assert command in (listed_commands.stdout+listed_commands.stderr).decode(errors='replace'),'the MONs list no '+command
        d=tempfile.mkdtemp(); asok=d+'/sub.asok'; log=d+'/sub.log'
        s=connected({'admin_socket':asok,'log_file':log,'log_to_file':'true','log_to_stderr':'false','err_to_stderr':'false','debug_ms':'1','debug_monc':'10','debug_mgrc':'4','debug_objecter':'3'}); clients.append(s)
        gid=s.get_instance_id()
        io=s.open_ioctx(pool); io.write_full('anchor',b'anchor')
        # A watch is a linger op; while one exists the Objecter asks for the
        # next OSD map on every tick, as a onetime subscription.
        anchor=io.watch('anchor',lambda *a: b''); watches.append(anchor)
        entries=[]
        s.monitor_log2('info',lambda arg,line,*rest: entries.append(rest[-1].decode(errors='replace') if isinstance(rest[-1],bytes) else str(rest[-1])),None)
        def objecter(): return json.loads(run(['ceph','--admin-daemon',asok,'perf','dump','objecter']).stdout)['objecter']
        def mon_session():
            found=[]
            for mon in ceph_json('mon','dump')['mons']:
                for sess in ceph_json('tell','mon.'+mon['name'],'sessions'):
                    if sess.get('global_id')==gid: found.append(dict(sess,mon=mon['name']))
            return found
        def wire():
            with open(log,errors='replace') as f: text=f.read()
            subs=[]
            for m in re.finditer(r'--> \S+ -- mon_subscribe\(\{([^}]*)\}\)',text):
                subs.append({k:v for k,v in (i.split('=',1) for i in m.group(1).split(',') if i)})
            got={'osd_map':[],'config':[],'log':[],'mon_map':0,'subscribe_ack':0,
                 'decoded':[[m.group(1),int(m.group(2))] for m in re.finditer(r'handle_osd_map decoding (incremental|full) epoch (\d+)',text)]}
            for m in re.finditer(r'<== ((?:mon|osd)\.\d+) \S+ \d+ ==== (osd_map\((\d+)\.\.(\d+)[^)]*\)|config\((\d+) keys\)|log\((\d+)? ?[^)]*\)|mon_map magic|mon_subscribe_ack)',text):
                if m.group(3): got['osd_map'].append({'from':m.group(1),'first':int(m.group(3)),'last':int(m.group(4))})
                elif m.group(5): got['config'].append(int(m.group(5)))
                elif m.group(2).startswith('log('): got['log'].append(int(m.group(6) or 0))
                elif m.group(2)=='mon_map magic': got['mon_map']+=1
                else: got['subscribe_ack']+=1
            return subs,got
        # The Objecter's map counters start with the first map after the one
        # it connected with, so the client's epoch is read from its log.
        def current():
            e=max([m['last'] for m in wire()[1]['osd_map']] or [0]); return e==ceph_json('osd','dump')['epoch'],e
        e0,_=wait_for('the client at the cluster epoch',current)
        before=objecter()
        _,polls=wait_for('a onetime osdmap=%d subscription'%(e0+1),lambda:(any(sub.get('osdmap')==str(e0+1) for sub in wire()[0]),wire()[0][-3:]))
        session0=mon_session()
        run(['ceph','--connect-timeout','20','osd','pool','set-quota',pool,'max_objects','100000'])
        e1=ceph_json('osd','dump')['epoch']; assert e1>e0,(e0,e1)
        after,_=wait_for('the client at epoch %d'%e1,lambda:(objecter()['map_epoch']>=e1,objecter()))
        session1,_=wait_for('the MON session at epoch %d'%e1,lambda:(any(x['osd_epoch']>=e1 for x in mon_session()),mon_session()))
        subs,got=wire()
        assert any(m['from'].startswith('mon.') and m['first']<=e1<=m['last'] for m in got['osd_map']),got
        assert any(e==e1 for _,e in got['decoded']),got['decoded']
        option,value='debug_rados','3/7'
        initial=s.conf_get(option); configs=len(got['config'])
        run(['ceph','--connect-timeout','20','config','set','client',option,value])
        try:
            wait_for('the client config change',lambda:(s.conf_get(option)==value,s.conf_get(option)))
        finally:
            run(['ceph','--connect-timeout','20','config','rm','client',option])
        restored,_=wait_for('the client config restore',lambda:(s.conf_get(option)==initial,s.conf_get(option)))
        marker='tc-subscription-%08x'%random.getrandbits(32)
        run(['ceph','--connect-timeout','20','log',marker])
        wait_for('the cluster log entry',lambda:(any(marker in e for e in entries),entries[-3:]))
        subs,got=wire()
        assert len(got['config'])>=configs+2,(configs,got['config'])
        assert got['log'],got
        got_subs=got
        names={}
        for sub in subs:
            for k,v in sub.items(): names.setdefault(k,set()).add(v)
        for k in ('monmap','mgrmap','config','osdmap','log-info'):
            assert k in names,(k,subs)
        for k in ('monmap','mgrmap','config','log-info'):
            assert any(v.endswith('+') for v in names[k]),(k,names)
        # MonMap and MgrMap epoch changes, followed from the MON commands that
        # make them to the client: the map messages its MON session delivered
        # (debug_ms), the MonMap its MonClient decoded (debug_monc) and the
        # MgrMap versions and active MGR its MgrClient took (debug_mgrc).
        stamp=re.compile(r'^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d')
        def maps():
            with open(log,errors='replace') as f: lines=f.read().split('\n')
            got={'mon_map':[],'mgrmap':[],'monmap_decoded':[],'mgr_versions':[],'mgr_sessions':[]}
            for i,line in enumerate(lines):
                m=re.search(r'<== (mon\.\d+) \S+ \d+ ==== (?:mon_map magic|mgrmap\(e (\d+)\))',line)
                if m:
                    if m.group(2): got['mgrmap'].append([m.group(1),int(m.group(2))])
                    else: got['mon_map'].append(m.group(1))
                    continue
                m=re.search(r' got monmap (\d+) from ',line)
                if m:
                    # MonClient dumps the map it decoded in its next entry,
                    # whose continuation lines carry no timestamp.
                    dump=[]
                    for j in range(i+1,min(i+40,len(lines))):
                        if lines[j].endswith(' dump:'):
                            for k in range(j+1,len(lines)):
                                if stamp.match(lines[k]): break
                                if lines[k]: dump.append(lines[k])
                            break
                    got['monmap_decoded'].append({'epoch':int(m.group(1)),'dump':dump})
                    continue
                m=re.search(r'Got map version (\d+)$',line)
                if m: got['mgr_versions'].append([int(m.group(1)),None]); continue
                m=re.search(r'Active mgr is now (.*)$',line)
                if m and got['mgr_versions']: got['mgr_versions'][-1][1]=m.group(1); continue
                m=re.search(r'Starting new session with (.*)$',line)
                if m: got['mgr_sessions'].append(m.group(1))
            return got
        def addrs(mgrmap): return ['%s:%s/%d'%(a['type'],a['addr'],a['nonce']) for a in mgrmap['active_addrs']['addrvec']]
        rank={m['name']:m['rank'] for m in ceph_json('mon','dump')['mons']}
        home=[x['mon'] for x in mon_session()]; assert len(home)==1,home
        home=home[0]; via='mon.%d'%rank[home]
        def decoded_at(epoch):
            found=[x for x in maps()['monmap_decoded'] if x['epoch']==epoch]
            return bool(found),found
        m0=ceph_json('mon','dump')['epoch']
        wait_for('the client at MonMap %d'%m0,lambda:decoded_at(m0))
        mon_maps=len(maps()['mon_map'])
        location='tc-monmap-%08x'%random.getrandbits(32)
        run(['ceph','--connect-timeout','20','mon','set_location',home,'datacenter='+location])
        changed=ceph_json('mon','dump'); m1=changed['epoch']
        assert m1==m0+1 and location in json.dumps([x for x in changed['mons'] if x['name']==home]),(m0,changed)
        decoded,_=wait_for('the client at MonMap %d'%m1,lambda:decoded_at(m1))
        dump=decoded[0]['dump']
        assert 'epoch %d'%m1 in dump and any(l.endswith(' mon.%s; crush_location {datacenter=%s}'%(home,location)) for l in dump),dump
        got=maps()
        assert len(got['mon_map'])>mon_maps and set(got['mon_map'][mon_maps:])=={via},(via,got['mon_map'])
        monmap={'before':m0,'after':m1,'location':location,'mon_map_messages':got['mon_map'][mon_maps:],
                'client_decoded':[x['epoch'] for x in got['monmap_decoded']],'client_dump':dump}
        def mgr_current():
            cur=ceph_json('mgr','dump'); got=maps()
            return bool(cur['available'] and got['mgr_versions'] and got['mgr_versions'][-1][0]==cur['epoch']),cur
        g0,_=wait_for('the client at the current MgrMap',mgr_current)
        mgr_maps=len(maps()['mgrmap'])
        run(['ceph','--connect-timeout','20','mgr','fail',g0['active_name']])
        g1=ceph_json('mgr','dump')
        assert g1['epoch']>g0['epoch'] and g1['active_gid']!=g0['active_gid'],(g0['epoch'],g0['active_gid'],g1['epoch'],g1['active_gid'])
        # The failed MGR respawns and comes back under a new GID; the client
        # follows each MgrMap and opens a session with the new active MGR.
        def replaced():
            cur=ceph_json('mgr','dump'); got=maps()
            ok=(cur['available'] and cur['active_gid'] not in (0,g0['active_gid']) and got['mgr_versions'] and
                got['mgr_versions'][-1][0]==cur['epoch'] and any(all(a in x for a in addrs(cur)) for x in got['mgr_sessions']))
            return bool(ok),(cur,got)
        (gf,got),_=wait_for('the client with the replacement MGR',replaced,180)
        received=got['mgrmap'][mgr_maps:]
        versions={e:a for e,a in got['mgr_versions']}
        assert {f for f,_ in received}=={via} and {g1['epoch'],gf['epoch']}<={e for _,e in received},(via,g1['epoch'],gf['epoch'],received)
        assert g1['epoch'] in versions and all(a in versions[gf['epoch']] for a in addrs(gf)),(versions,addrs(gf))
        assert addrs(gf)!=addrs(g0) and not any(a in versions[gf['epoch']] for a in addrs(g0)),(addrs(g0),addrs(gf))
        session2=[x['mon'] for x in mon_session()]
        assert session2==[home],(home,session2)
        mgrmap={'before':g0['epoch'],'failed':{'name':g0['active_name'],'gid':g0['active_gid'],'addrs':addrs(g0)},
                'after_fail':{'epoch':g1['epoch'],'active_gid':g1['active_gid'],'available':g1['available']},
                'after':{'epoch':gf['epoch'],'name':gf['active_name'],'gid':gf['active_gid'],'addrs':addrs(gf)},
                'mgrmap_messages':received,'client_versions':[[e,versions[e]] for e in sorted(versions) if e>g0['epoch']],
                'client_session':[x for x in got['mgr_sessions'] if all(a in x for a in addrs(gf))][-1]}
        out={'global_id':gid,'subscriptions':{k:sorted(v,key=lambda x:(int(x.rstrip('+')),x)) for k,v in names.items()},
             'onetime_next_osdmap':str(e0+1),'subscribe_polls':polls,
             'epochs':{'before':e0,'after':e1,'client_before':before['map_epoch'],'client_after':after['map_epoch'],
                       'client_map_inc':after['map_inc']-before['map_inc'],'client_map_full':after['map_full']-before['map_full'],
                       'mon_session_before':[x['osd_epoch'] for x in session0],'mon_session_after':[x['osd_epoch'] for x in session1 if x['global_id']==gid]},
             'received':got_subs,'config':{'option':option,'set':value,'restored':restored},'log':{'marker':marker,'delivered':True},
             'maps':{'mon_session':home,'via':via,'monmap':monmap,'mgrmap':mgrmap}}
        anchor.close(); io.close()
        shutil.rmtree(d,ignore_errors=True)
    else:
        raise SystemExit('unknown phase '+phase)
    print(json.dumps(out))
finally:
    # A watch left open would unwatch from its finalizer after shutdown.
    for w in watches:
        try: w.close()
        except rados.Error: pass
    for c in clients: c.shutdown()
`

// watchSubscriptionPhase runs one phase of watchSubscriptionScript and
// decodes its receipt into out.
func watchSubscriptionPhase(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, out any, phase, pool string) json.RawMessage {
	t.Helper()
	result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
		Argv: []string{"python3", "-c", watchSubscriptionScript, phase, pool}, Timeout: 8 * time.Minute,
		MaxStdout: 1 << 20, MaxStderr: 1 << 20, Label: "watch subscription " + phase,
	})
	if err != nil {
		t.Fatalf("%s phase: %v", phase, err)
	}
	if !result.Complete() || result.ExitCode != 0 || json.Unmarshal(result.Stdout, out) != nil {
		t.Fatalf("%s phase: exit %d, complete %t\nstdout: %s\nstderr: %s", phase, result.ExitCode, result.Complete(), result.Stdout, result.Stderr)
	}
	return result.Stdout
}
