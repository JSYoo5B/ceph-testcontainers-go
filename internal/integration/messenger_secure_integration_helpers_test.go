//go:build all || (integration && topology)

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func checkSecureMessengerTopology(t *testing.T, ctx context.Context, cluster *ceph.Container) {
	t.Helper()
	addresses, err := cluster.MonitorBootstrapAddresses(ctx)
	if err != nil || !strings.Contains(addresses, "v2:") || strings.Contains(addresses, "v1:") {
		t.Fatal("secure MON bootstrap is not v2-only", err)
	}
	config, _, err := cluster.ConnectionConfig()
	if err != nil || !strings.Contains(string(config), "v2:") || strings.Contains(string(config), "v1:") {
		t.Fatal("secure connection config is not v2-only", err)
	}
	var osdMap struct {
		FSID string `json:"fsid"`
		OSDs []struct {
			ID     int `json:"osd"`
			Public struct {
				Addrs []struct {
					Type string `json:"type"`
				} `json:"addrvec"`
			} `json:"public_addrs"`
			Cluster struct {
				Addrs []struct {
					Type string `json:"type"`
				} `json:"addrvec"`
			} `json:"cluster_addrs"`
		} `json:"osds"`
	}
	data, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil || json.Unmarshal(data, &osdMap) != nil || osdMap.FSID == "" || len(osdMap.OSDs) != len(cluster.OSDs()) {
		t.Fatal("secure native OSDMap unavailable", err)
	}
	for _, osd := range osdMap.OSDs {
		if len(osd.Public.Addrs) != 1 || len(osd.Cluster.Addrs) != 1 || osd.Public.Addrs[0].Type != "v2" || osd.Cluster.Addrs[0].Type != "v2" {
			t.Fatalf("OSD %d does not advertise v2-only public/cluster addresses", osd.ID)
		}
	}
	control := cluster.ControlContainer()
	var daemons []string
	for _, mon := range cluster.Monitors() {
		daemons = append(daemons, "mon."+mon.DaemonName)
	}
	for _, osd := range cluster.OSDs() {
		daemons = append(daemons, fmt.Sprintf("osd.%d", osd.ID))
	}
	// tell forwards admin-socket commands; the OSD role needs no ceph CLI.
	for _, daemon := range daemons {
		data := topologyExecOutput(t, ctx, control, "python3", "-c", secureMessengerDaemonProbe, daemon, osdMap.FSID, "")
		t.Logf("MSGR2_NATIVE %s", data)
	}
	// Standby MGRs have an admin socket but do not serve forwarded tell commands.
	for _, mgr := range cluster.Managers() {
		data := topologyExecOutput(t, ctx, mgr.Container, "python3", "-c", secureMessengerDaemonProbe, "mgr."+mgr.DaemonName, osdMap.FSID, "/var/run/ceph/ceph-mgr."+mgr.DaemonName+".asok")
		t.Logf("MSGR2_NATIVE %s", data)
	}
}

func secureMessengerClient(t *testing.T, ctx context.Context, client testcontainers.Container, phase string) {
	t.Helper()
	data := topologyExecOutput(t, ctx, client, "python3", "-c", secureMessengerClientProbe, phase)
	t.Logf("MSGR2_CLIENT %s", data)
}

const secureMessengerDumpPython = `
def ready_connections(record):
    record=record.get('status',record)
    messenger=record['messenger']; result=[]
    connections=[item['async_connection'] for item in messenger['connections']]+messenger.get('anon_conns',[])
    for connection in connections:
        status=connection['status']
        if not status['connected'] or status['loopback']: continue
        protocol=connection['protocol']
        assert 'v2' in protocol,('legacy ready connection',connection['peer'])
        v2=protocol['v2']
        if v2['state']!='READY': continue
        # Reused connections may lose auth_meta diagnostics while retaining
        # the live encrypted session handlers. Explicit CRC still fails.
        assert v2['con_mode'] in ('secure','unknown'),v2
        assert v2['crypto']['rx']=='AES-128-GCM' and v2['crypto']['tx']=='AES-128-GCM',v2
        peer=connection['peer']
        result.append({'peer_type':peer['type'],'peer_id':peer['id'],'global_id':peer['global_id'],'mode':v2['con_mode'],'rx':v2['crypto']['rx'],'tx':v2['crypto']['tx'],'encrypted_transport':True})
    return result
`

const secureMessengerDaemonProbe = `import subprocess,json,sys
daemon,expected_fsid,socket=sys.argv[1:]
def native(*args):
    result=subprocess.run(['ceph','--connect-timeout','5',*args],capture_output=True,text=True,timeout=12)
    assert result.returncode==0,('native command failed',args,result.returncode,result.stderr[-500:])
    return json.loads(result.stdout)
assert native('fsid','--format','json')['fsid']==expected_fsid
def probe(*args):
    return native('--admin-daemon',socket,*args) if socket else native('tell',daemon,*args,'--format','json')
config=probe('config','show')
for key in ('ms_cluster_mode','ms_service_mode','ms_client_mode','ms_mon_cluster_mode','ms_mon_service_mode','ms_mon_client_mode'):
    assert config[key]=='secure',(daemon,key,config[key])
assert config['ms_bind_msgr1']=='false' and config['ms_bind_msgr2']=='true'
` + secureMessengerDumpPython + `
catalog=probe('messenger','dump'); catalog=catalog.get('status',catalog)
assert catalog['messengers'],daemon
ready=[]
for name in catalog['messengers']:
    ready.extend(ready_connections(probe('messenger','dump',name)))
assert ready,('no native ready secure peer',daemon)
assert native('fsid','--format','json')['fsid']==expected_fsid
print(json.dumps({'daemon':daemon,'fsid':expected_fsid,'effective_modes':'secure-only','ready_remote':ready}))
`

const secureMessengerClientProbe = `import rados,sys,json,subprocess,tempfile,hashlib
phase=sys.argv[1]; payload=bytes(range(256))*256
` + secureMessengerDumpPython + `
with tempfile.TemporaryDirectory() as directory:
    socket=directory+'/client.asok'
    client=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','admin_socket':socket,'client_mount_timeout':'8','rados_mon_op_timeout':'8','rados_osd_op_timeout':'10'})
    try:
        client.connect()
        for key in ('ms_client_mode','ms_mon_client_mode'): assert client.conf_get(key)=='secure'
        with client.open_ioctx('tc-messenger') as io:
            if phase=='seed': io.write_full('retained',payload)
            assert io.read('retained',len(payload)+1)==payload
            io.write_full('fresh-'+phase,payload[::-1]); assert io.read('fresh-'+phase,len(payload)+1)==payload[::-1]
            def local(*args):
                result=subprocess.run(['ceph','--admin-daemon',socket,'messenger','dump',*args],capture_output=True,text=True,timeout=8)
                assert result.returncode==0,('client dump failed',result.returncode,result.stderr[-500:])
                return json.loads(result.stdout)
            catalog=local(); catalog=catalog.get('status',catalog)
            ready=[]
            for name in catalog['messengers']: ready.extend(ready_connections(local(name)))
            types={peer['peer_type'] for peer in ready}
            assert 'mon' in types and 'osd' in types,ready
            print(json.dumps({'phase':phase,'bytes':len(payload),'sha256':hashlib.sha256(payload).hexdigest(),'ready_remote':ready}))
    finally: client.shutdown()
`

const secureMessengerCRCProbe = `import subprocess,json,re
result=subprocess.run(['ceph','--connect-timeout','6','--ms-mon-client-mode','crc','--ms-client-mode','crc','--debug-ms','1','status','--format','json'],capture_output=True,text=True,timeout=15)
assert result.returncode!=0, 'CRC-only client connected to secure-only cluster'
assert re.search(r'handle_auth_bad_method.*allowed modes=\[2\]',result.stderr),result.stderr[-1500:]
print(json.dumps({'crc_only_refused':True,'native_exit':result.returncode,'server_allowed_modes':[2],'watchdog_timeout':False}))
`

const secureMessengerCephFSProbe = `import cephfs,os,sys,json,subprocess,tempfile
` + secureMessengerDumpPython + `
with tempfile.TemporaryDirectory() as directory:
    socket=directory+'/cephfs.asok'
    fs=cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf',auth_id='admin')
    fs.conf_set('admin_socket',socket); fs.conf_set('client_mount_timeout','10'); fs.conf_set('rados_osd_op_timeout','10')
    try:
        fs.mount(filesystem_name=sys.argv[1].encode())
        payload=b'secure-cephfs-native-data\0'*4096
        fd=fs.open('/secure-fixture',os.O_CREAT|os.O_RDWR|os.O_TRUNC,0o600)
        try:
            assert fs.write(fd,payload,0)==len(payload); fs.fsync(fd,False)
            assert fs.read(fd,0,len(payload)+1)==payload
        finally: fs.close(fd)
        def local(*args):
            result=subprocess.run(['ceph','--admin-daemon',socket,'messenger','dump',*args],capture_output=True,text=True,timeout=8)
            assert result.returncode==0,('CephFS client dump failed',result.returncode)
            return json.loads(result.stdout)
        catalog=local(); catalog=catalog.get('status',catalog); ready=[]
        for name in catalog['messengers']: ready.extend(ready_connections(local(name)))
        assert {'mon','mds','osd'}.issubset({peer['peer_type'] for peer in ready}),ready
        fs.unlink('/secure-fixture')
        print(json.dumps({'filesystem':sys.argv[1],'bytes':len(payload),'ready_remote':ready}))
    finally: fs.shutdown()
`

const secureMessengerRGWProbe = `import subprocess,sys,json,time,urllib.parse
hostname,frontend=sys.argv[1:]; endpoint=urllib.parse.urlsplit(frontend)
` + secureMessengerDumpPython + `
def native(*args):
    result=subprocess.run(['ceph','--connect-timeout','5',*args],capture_output=True,text=True,timeout=8)
    assert result.returncode==0,('native RGW connection query failed',args,result.returncode)
    return json.loads(result.stdout)
deadline=time.monotonic()+35
while True:
    service=native('service','dump','--format','json')['services'].get('rgw',{}).get('daemons',{})
    matches=[]
    for name,daemon in service.items():
        if name=='summary': continue
        metadata=daemon['metadata']; config=metadata.get('frontend_config#0','')
        # The unique listener also distinguishes host-network PID 1 gateways.
        port='port='+str(endpoint.port); bind='endpoint='+endpoint.netloc
        if metadata.get('hostname')==hostname and metadata.get('pid')=='1' and (port in config.split() or bind in config.split()):
            matches.append(daemon)
    assert len(matches)<=1,'ambiguous owned RGW registration'
    if matches and matches[0]['gid']>0: break
    assert time.monotonic()<deadline,'owned RGW native registration absent'
    time.sleep(.5)
gid=matches[0]['gid']; peers=[]
osds=native('osd','dump','--format','json')['osds']
for osd in osds:
    target='osd.'+str(osd['osd']); catalog=native('tell',target,'messenger','dump','--format','json'); catalog=catalog.get('status',catalog)
    for name in catalog['messengers']:
        for peer in ready_connections(native('tell',target,'messenger','dump',name,'--format','json')):
            if peer['peer_type']=='client' and peer['global_id']==gid: peers.append({'osd':osd['osd'],**peer})
assert peers,('no RGW→OSD secure connection for registered native GID',gid)
print(json.dumps({'rgw_gid':gid,'hostname':hostname,'ready_osd_peers':peers,'payload_verified':True}))
`
