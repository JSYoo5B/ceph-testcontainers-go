//go:build all || (integration && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_messenger_secure_mix))))

//ci: timeout=30m job-timeout=40

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/cephfs"
	"github.com/jsyoo5b/ceph-testcontainers-go/rbd"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// Default permits native negotiation; it is not a CRC-only cluster. These
// cases verify compatibility with secure-only in both replication directions.
// Each case owns a fresh pair and runs sequentially within the Docker budget.
func TestRBDMessengerSecureDefaultMix(t *testing.T) {
	for _, secureSource := range []bool{true, false} {
		t.Run(mixedMessengerDirection(secureSource), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 7*time.Minute)
			defer cancel()
			started := time.Now()
			source, destination, sourceClient, destinationClient := newMixedMessengerPair(t, ctx, secureSource)
			const pool, name = "tc-messenger-rbd", "replicated"
			const image = pool + "/" + name
			for _, cluster := range []*ceph.Container{source, destination} {
				if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 4, Replicas: 1, MinSize: 1}); err != nil {
					t.Fatal(err)
				}
				if err := rbd.InitPool(ctx, cluster, pool); err != nil {
					t.Fatal(err)
				}
				if err := cluster.WaitForClean(ctx); err != nil {
					t.Fatal(err)
				}
			}
			mirror, err := rbd.RunMirror(ctx, source.ControlImage(), rbd.MirrorConfig{
				Source: source, Destination: destination, Pool: pool,
			})
			if mirror != nil {
				t.Cleanup(func() {
					cleanup, done := context.WithTimeout(context.Background(), time.Minute)
					defer done()
					if err := mirror.Terminate(cleanup); err != nil {
						t.Error(err)
					}
				})
			}
			if err != nil {
				t.Fatal(err)
			}
			payload := rbdMultiClusterPayload(1<<20, 37)
			if err := sourceClient.CopyToContainer(ctx, payload, "/tmp/messenger-rbd-payload", 0o600); err != nil {
				t.Fatal(err)
			}
			mixedMessengerExec(t, ctx, sourceClient, "rbd", "import", "/tmp/messenger-rbd-payload", image,
				"--object-size", "1M", "--image-feature", "layering,exclusive-lock", "--no-progress")
			if err := mirror.EnableImage(ctx, name); err != nil {
				t.Fatal(err)
			}
			rbdMirrorReplayReady(t, ctx, mirror, name, rbd.MirrorModeSnapshot, "", "")
			rbdMultiClusterWaitMirror(t, ctx, destinationClient, image, payload)
			// Native bootstrap names the remote CephX user separately from the
			// destination-owned daemon. Observe those exact authenticated users.
			var policy struct {
				Peers []struct {
					ClientName string `json:"client_name"`
				} `json:"peers"`
			}
			data, err := mirror.DestinationRBD(ctx, "mirror", "pool", "info", pool, "--format", "json")
			if err != nil || json.Unmarshal(data, &policy) != nil || len(policy.Peers) != 1 || policy.Peers[0].ClientName == "" {
				t.Fatal("native imported RBD mirror peer identity unavailable", err)
			}
			daemons := mirror.Daemons()
			if len(daemons) != 1 {
				t.Fatal("expected one owned RBD receiver")
			}
			checkMixedMessengerPeers(t, ctx, source, policy.Peers[0].ClientName, daemons[0], nil)
			checkMixedMessengerPeers(t, ctx, destination, daemons[0].ClientName, daemons[0], nil)
			mixedMessengerCatalog(t, ctx, source, daemons[0], "/tmp/rbd-mirror.asok")
			patch := rbdMultiClusterPayload(16<<10, 83)
			copy(payload[256<<10:], patch)
			rbdMultiClusterWriteRange(t, ctx, sourceClient, image, uint64(len(payload)), 256<<10, patch)
			if _, err := mirror.SourceRBD(ctx, "mirror", "image", "snapshot", image); err != nil {
				t.Fatal(err)
			}
			rbdMirrorReplayReady(t, ctx, mirror, name, rbd.MirrorModeSnapshot, "", "")
			rbdMultiClusterWaitMirror(t, ctx, destinationClient, image, payload)
			checkMixedMessengerPeers(t, ctx, source, policy.Peers[0].ClientName, daemons[0], nil)
			checkMixedMessengerPeers(t, ctx, destination, daemons[0].ClientName, daemons[0], nil)
			t.Logf("MSGR_MIX_RBD direction=%s bytes=%d checkpoints=2 elapsed=%s", mixedMessengerDirection(secureSource), len(payload), time.Since(started))
		})
	}
}

func TestCephFSMessengerSecureDefaultMix(t *testing.T) {
	for _, secureSource := range []bool{true, false} {
		t.Run(mixedMessengerDirection(secureSource), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 7*time.Minute)
			defer cancel()
			started := time.Now()
			source, destination, sourceClient, destinationClient := newMixedMessengerPair(t, ctx, secureSource)
			filesystems := make([]*cephfs.Filesystem, 2)
			for index, cluster := range []*ceph.Container{source, destination} {
				filesystem, err := cephfs.Start(ctx, cluster, cephfs.Config{
					Name:         []string{"tc-messenger-source", "tc-messenger-destination"}[index],
					MetadataPool: ceph.PoolConfig{PGNum: 4, Replicas: 1, MinSize: 1},
					DataPool:     ceph.PoolConfig{PGNum: 4, Replicas: 1, MinSize: 1},
				})
				if err != nil {
					t.Fatal(err)
				}
				filesystems[index] = filesystem
				if err := cluster.WaitForClean(ctx); err != nil {
					t.Fatal(err)
				}
			}
			const script = "/tmp/messenger-cephfs.py"
			for _, client := range []testcontainers.Container{sourceClient, destinationClient} {
				if err := client.CopyToContainer(ctx, []byte(mixedMessengerCephFSProbe), script, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			mixedMessengerExec(t, ctx, sourceClient, "python3", script, filesystems[0].FilesystemName, "write", "initial")
			mirror, err := cephfs.RunMirror(ctx, source.ControlImage(), cephfs.MirrorConfig{
				Source: source, Destination: destination, SourceFilesystem: filesystems[0].FilesystemName,
				DestinationFilesystem: filesystems[1].FilesystemName, DestinationSite: "messenger-destination", Directories: []string{"/messenger"},
			})
			if mirror != nil {
				t.Cleanup(func() {
					cleanup, done := context.WithTimeout(context.Background(), time.Minute)
					defer done()
					if err := mirror.Terminate(cleanup); err != nil {
						t.Error(err)
					}
				})
			}
			if err != nil {
				t.Fatal(err)
			}
			expected := cephFSObservedFilesystemIdentity(t, ctx, filesystems[0], filesystems[1])
			peers, err := mirror.PeerIDs(ctx)
			if err != nil || len(peers) != 1 {
				t.Fatal("owned CephFS mirror peer unavailable", err)
			}
			expected.PeerID = peers[0]
			daemons := mirror.Daemons()
			if len(daemons) != 1 {
				t.Fatal("expected one owned CephFS mirror daemon")
			}
			for index, checkpointName := range []string{"initial", "changed"} {
				if index != 0 {
					mixedMessengerExec(t, ctx, sourceClient, "python3", script, filesystems[0].FilesystemName, "write", checkpointName)
				}
				checkpoint := cephFSReadSourceSnapshot(t, ctx, sourceClient, filesystems[0].FilesystemName, script, "/messenger", checkpointName)
				cephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, "/messenger", checkpoint)
				mixedMessengerExec(t, ctx, destinationClient, "python3", script, filesystems[1].FilesystemName, "verify", checkpointName)
				checkMixedMessengerPeers(t, ctx, source, mirror.SourceClientEntity, daemons[0], filesystems[0])
				checkMixedMessengerPeers(t, ctx, destination, mirror.DestinationClientEntity, daemons[0], filesystems[1])
				t.Logf("MSGR_MIX_CEPHFS checkpoint=%d/%s source_snapshot_id=%d", index+1, checkpointName, checkpoint.ID)
			}
			mixedMessengerCatalog(t, ctx, source, daemons[0], "/var/run/ceph/cephfs-mirror.asok")
			t.Logf("MSGR_MIX_CEPHFS direction=%s bytes=8192 checkpoints=2 elapsed=%s", mixedMessengerDirection(secureSource), time.Since(started))
		})
	}
}

func mixedMessengerDirection(secureSource bool) string {
	if secureSource {
		return "secure-to-default"
	}
	return "default-to-secure"
}

func newMixedMessengerPair(t *testing.T, ctx context.Context, secureSource bool) (*ceph.Container, *ceph.Container, testcontainers.Container, testcontainers.Container) {
	t.Helper()
	image, roleOptions := integrationImages(t)
	clusters := make([]*ceph.Container, 2)
	clients := make([]testcontainers.Container, 2)
	identities := make([]string, 2)
	keyrings := make([][]byte, 2)
	for index := range clusters {
		mode := ceph.MessengerDefault
		if (index == 0) == secureSource {
			mode = ceph.MessengerV2Secure
		}
		options := append([]testcontainers.ContainerCustomizer{}, roleOptions...)
		options = append(options, ceph.WithNoInitialOSDs(), ceph.WithPoolDefaults(1, 1), ceph.WithOSDBlockSize(512<<20), ceph.WithMessengerMode(mode))
		cluster, client := newServiceClusterWithOptions(t, image, options...)
		if cluster.MessengerMode() != mode {
			t.Fatal("cluster bootstrap mode changed")
		}
		if _, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{Section: "osd", Name: "osd_mclock_skip_benchmark", Value: "true"}); err != nil {
			t.Fatal(err)
		}
		if _, err := cluster.AddOSD(ctx); err != nil {
			t.Fatal(err)
		}
		status, err := cluster.Status(ctx)
		parsed, parseErr := uuid.Parse(status.FSID)
		if err != nil || parseErr != nil || parsed == uuid.Nil || parsed.String() != status.FSID || status.OSDMap.NumOSDs != 1 {
			t.Fatal("independent one-OSD native identity unavailable", err)
		}
		var observed struct {
			FSID string `json:"fsid"`
		}
		if json.Unmarshal(mixedMessengerExec(t, ctx, client, "ceph", "fsid", "--format", "json"), &observed) != nil || observed.FSID != status.FSID {
			t.Fatal("exported client connected to a different cluster")
		}
		_, keyring, err := cluster.ConnectionConfig()
		if err != nil {
			t.Fatal(err)
		}
		clusters[index], clients[index], identities[index], keyrings[index] = cluster, client, status.FSID, keyring
	}
	if identities[0] == identities[1] || clusters[0].NetworkName() == clusters[1].NetworkName() || bytes.Equal(keyrings[0], keyrings[1]) {
		t.Fatal("mixed-policy fixtures do not have independent FSIDs, networks and credentials")
	}
	t.Logf("MSGR_MIX_PAIR source_fsid=%s destination_fsid=%s source_mode=%d destination_mode=%d osds_per_cluster=1 block_size=536870912 replicas=1 min_size=1", identities[0], identities[1], clusters[0].MessengerMode(), clusters[1].MessengerMode())
	return clusters[0], clusters[1], clients[0], clients[1]
}

func mixedMessengerExec(t *testing.T, parent context.Context, container testcontainers.Container, command ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 55*time.Second)
	defer cancel()
	args := append([]string{"python3", "-c", `import subprocess,sys
result=subprocess.run(sys.argv[1:],timeout=50,check=False)
sys.exit(result.returncode if result.returncode>=0 else 128-result.returncode)`}, command...)
	code, reader, err := container.Exec(ctx, args, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	if err != nil || code != 0 {
		t.Fatalf("bounded mixed-policy native probe: exit=%d error=%v output=%s", code, err, output)
	}
	return output
}

func checkMixedMessengerPeers(t *testing.T, ctx context.Context, cluster *ceph.Container, clientName string, mirror testcontainers.Container, filesystem *cephfs.Filesystem) {
	t.Helper()
	status, err := cluster.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	addresses, err := mirror.ContainerIPs(ctx)
	if err != nil || len(addresses) == 0 {
		t.Fatal("owned mirror native Docker endpoint addresses unavailable", err)
	}
	encodedAddresses, err := json.Marshal(addresses)
	if err != nil {
		t.Fatal(err)
	}
	daemons := []string{"mon.a", fmt.Sprintf("osd.%d", cluster.OSDs()[0].ID)}
	if filesystem != nil {
		for _, mds := range filesystem.MDSs() {
			daemons = append(daemons, "mds."+mds.ID)
		}
	}
	mode := "default"
	if cluster.MessengerMode() == ceph.MessengerV2Secure {
		mode = "secure"
	}
	for _, daemon := range daemons {
		data := mixedMessengerExec(t, ctx, cluster.ControlContainer(), "python3", "-c", mixedMessengerPeerProbe, status.FSID, mode, daemon, clientName, string(encodedAddresses), messengerDumpArg(cluster))
		t.Logf("MSGR_MIX_NATIVE %s", data)
	}
}

func mixedMessengerCatalog(t *testing.T, ctx context.Context, cluster *ceph.Container, daemon testcontainers.Container, socket string) {
	t.Helper()
	if messengerDumpArg(cluster) != "1" {
		// Ceph 19 has no messenger dump; prove the owned socket answers.
		data := mixedMessengerExec(t, ctx, daemon, "ceph", "--admin-daemon", socket, "version")
		t.Logf("MSGR_MIX_MIRROR socket=%s native_version=%s", socket, bytes.TrimSpace(data))
		return
	}
	data := mixedMessengerExec(t, ctx, daemon, "ceph", "--admin-daemon", socket, "messenger", "dump")
	var catalog struct {
		Messengers []string `json:"messengers"`
	}
	if err := json.Unmarshal(data, &catalog); err != nil || len(catalog.Messengers) == 0 {
		t.Fatal("owned mirror admin-socket messenger catalog unavailable", err)
	}
	t.Logf("MSGR_MIX_MIRROR socket=%s native_messengers=%v", socket, catalog.Messengers)
}

// Bind the mirror's exact CephX principal to authenticated MON session GIDs,
// then query the server's actual messenger socket via tell. Matching both GIDs
// and native container addresses excludes an unrelated CLI connection, and
// covers library contexts absent from the mirror daemon's main socket.
// Default-side CRC is allowed; every ready owned secure-side peer must use AES.
// A reused READY connection may dump a cleared auth mode while retaining its
// session handlers. Accept that diagnostic only with both native AES handlers.
const mixedMessengerPeerProbe = `import json,subprocess,sys,time
expected_fsid,mode,daemon,entity,encoded_addresses,dump=sys.argv[1:]
expected_addresses=set(json.loads(encoded_addresses))
def native(*args):
    result=subprocess.run(['ceph','--connect-timeout','5',*args],capture_output=True,text=True,timeout=8)
    assert result.returncode==0,('native command failed',daemon,args,result.returncode,result.stderr[-500:])
    return json.loads(result.stdout)
def native_addresses(record):
    return {entry['addr'].removeprefix('v2:').removeprefix('v1:').split('/')[0].rsplit(':',1)[0]
            for entry in record['addrvec']}
assert native('fsid','--format','json')['fsid']==expected_fsid
deadline=time.monotonic()+40
while True:
    sessions=native('tell','mon.a','sessions','--format','json')
    if isinstance(sessions,dict): sessions=sessions.get('status',sessions)
    assert isinstance(sessions,list),('native MON session schema differs',sessions)
    authenticated_gids={session['global_id'] for session in sessions
                        if session['entity_name']==entity and session['open'] and session['authenticated']
                        and session['global_id']>0 and native_addresses(session['addrs']).intersection(expected_addresses)}
    if dump!='1':
        # Ceph 19 has no messenger dump. A secure-side server that accepts
        # only secure sessions proves the owned mirror's sessions encrypted.
        key='ms_mon_service_mode' if daemon.startswith('mon.') else 'ms_service_mode'
        service=native('tell',daemon,'config','get',key,'--format','json')[key]
        assert mode!='secure' or service=='secure',('secure-side service mode differs',daemon,service)
        if authenticated_gids:
            ready=[{'global_id':gid,'service_mode':service} for gid in sorted(authenticated_gids)]
            break
        assert time.monotonic()<deadline,('no live owned mirror client session',daemon,entity)
        time.sleep(1)
        continue
    catalog=native('tell',daemon,'messenger','dump','--format','json'); catalog=catalog.get('status',catalog)
    assert catalog['messengers'],daemon
    ready=[]
    for name in catalog['messengers']:
        record=native('tell',daemon,'messenger','dump',name,'connections','anon_conns','--format','json')
        messenger=record.get('status',record)['messenger']
        for item in messenger['connections']+messenger.get('anon_conns',[]):
            connection=item.get('async_connection',item); peer=connection['peer']; status=connection['status']
            if not status['connected'] or status['loopback'] or peer['type']!='client': continue
            if peer['global_id'] not in authenticated_gids: continue
            protocol=connection['protocol']; v2=protocol.get('v2')
            if v2 is not None and v2['state']!='READY': continue
            addresses=native_addresses(peer['addr'])
            assert addresses.intersection(expected_addresses),('owned mirror peer address differs',daemon,entity,addresses,expected_addresses)
            assert peer['global_id']>0,('authenticated mirror global id absent',daemon,entity)
            encrypted=v2 is not None and v2['crypto']['rx']=='AES-128-GCM' and v2['crypto']['tx']=='AES-128-GCM'
            if mode=='secure':
                assert v2 is not None,('owned secure-side peer uses v1',daemon,entity)
                assert v2['con_mode'] in ('secure','unknown'),('owned secure-side mode differs',daemon,entity,v2)
                assert encrypted,('owned secure-side session handlers differ',daemon,entity,v2)
            elif v2 is not None:
                assert v2['con_mode'] in ('crc','secure') or (v2['con_mode']=='unknown' and encrypted),v2
            ready.append({'messenger':name,'global_id':peer['global_id'],'peer_addresses':sorted(addresses),'mode':v2['con_mode'] if v2 else 'v1',
                          'encrypted_transport':encrypted,
                          'rx':v2['crypto']['rx'] if v2 else None,'tx':v2['crypto']['tx'] if v2 else None})
    if ready: break
    assert time.monotonic()<deadline,('no live owned mirror client connection',daemon,entity,sorted(authenticated_gids),catalog['messengers'])
    time.sleep(1)
assert native('fsid','--format','json')['fsid']==expected_fsid
print(json.dumps({'fsid':expected_fsid,'policy':mode,'daemon':daemon,'client':entity,'authenticated_gids':sorted(authenticated_gids),'ready':ready}))
`

const mixedMessengerCephFSProbe = `import cephfs,json,os,sys
filesystem,phase,*args=sys.argv[1:]
fs=cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf',auth_id='admin')
fs.conf_set('client_mount_timeout','10')
fs.mount(filesystem_name=filesystem.encode())
def payload(checkpoint):
    original=bytes(range(256))*32
    return original if checkpoint=='initial' else original[::-1]
try:
    if phase=='write':
        checkpoint=args[0]
        if checkpoint=='initial': fs.mkdir('/messenger',0o750)
        content=payload(checkpoint)
        fd=fs.open('/messenger/data',os.O_CREAT|os.O_WRONLY|os.O_TRUNC,0o640)
        try:
            assert fs.write(fd,content,0)==len(content)
            fs.fsync(fd,False)
        finally: fs.close(fd)
        fs.sync_fs()
        fs.mkdir('/messenger/.snap/'+checkpoint,0o755)
    elif phase=='snapshot-checkpoint':
        directory,checkpoint=args
        info=fs.snap_info(directory+'/.snap/'+checkpoint)
        assert type(info['id']) is int and info['id']>0
        print(json.dumps({'id':info['id'],'name':checkpoint}))
    elif phase=='verify':
        checkpoint=args[0]; expected=payload(checkpoint)
        fd=fs.open('/messenger/.snap/'+checkpoint+'/data',os.O_RDONLY,0)
        try: assert fs.read(fd,0,len(expected)+1)==expected
        finally: fs.close(fd)
        print(json.dumps({'filesystem':filesystem,'checkpoint':checkpoint,'bytes':len(expected)}))
    else: raise AssertionError('unknown phase: '+phase)
finally:
    fs.unmount()
    fs.shutdown()
`
