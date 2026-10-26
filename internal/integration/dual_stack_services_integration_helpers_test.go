//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// dualStackSockets is the Python helper of the held clients: it lists the
// established TCP connections of the container as [local ip, local port,
// remote ip, remote port], read from /proc/net/tcp{,6}, and then waits for
// the test to release the session, so that the test can read the daemons'
// socket tables while the client's connections are still open.
const dualStackSockets = `import json,os,socket,time
def sockets():
    found=[]
    def endpoint(text,v6):
        host,port=text.split(':')
        raw=bytes.fromhex(host)
        raw=b''.join(raw[i:i+4][::-1] for i in range(0,len(raw),4))
        ip=socket.inet_ntop(socket.AF_INET6 if v6 else socket.AF_INET,raw)
        if ip.startswith('::ffff:'):
            ip=ip[7:]
        return [ip,int(port,16)]
    for path,v6 in (('/proc/net/tcp',False),('/proc/net/tcp6',True)):
        for line in open(path).read().splitlines()[1:]:
            f=line.split()
            if f[3]=='01':
                found.append(endpoint(f[1],v6)+endpoint(f[2],v6))
    return found
def hold(tag):
    with open('/tmp/'+tag+'.json.tmp','w') as out:
        json.dump({'sockets':sockets()},out)
    os.rename('/tmp/'+tag+'.json.tmp','/tmp/'+tag+'.json')
    deadline=time.time()+90
    while not os.path.exists('/tmp/'+tag+'.release'):
        assert time.time()<deadline,'not released'
        time.sleep(0.2)
`

// dualStackRadosHeld writes and reads one object per name in argv[4:] with a
// librados session whose MON session mon_host_override keeps on argv[1],
// sends a pg stat MGR command, and holds the session.
const dualStackRadosHeld = dualStackSockets + `import sys,rados
tag,mon_host,pool=sys.argv[1:4]
conf={'mon_host':mon_host,'mon_host_override':mon_host,'rados_mon_op_timeout':'10','rados_osd_op_timeout':'10','client_mount_timeout':'10'}
c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf=conf)
c.connect()
try:
    r,_,err=c.mgr_command(json.dumps({'prefix':'pg stat','format':'json'}),b'')
    assert r==0,('mgr',r,err)
    io=c.open_ioctx(pool)
    for name in sys.argv[4:]:
        data=(tag+'/'+name).encode()*64
        io.write_full(name,data)
        assert io.read(name,len(data)+1)==data,'readback '+name
    io.close()
    hold(tag)
finally:
    c.shutdown()
`

// dualStackCephFSHeld mounts the filesystem argv[2] with libcephfs, writes,
// fsyncs and reads back a binary file, and holds the mount.
const dualStackCephFSHeld = dualStackSockets + `import sys,cephfs
tag,filesystem=sys.argv[1:3]
fs=cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf',auth_id='admin')
fs.conf_set('client_mount_timeout','30')
fs.mount(filesystem_name=filesystem.encode())
try:
    data=bytes(range(256))*16+tag.encode()
    fd=fs.open('/'+tag,os.O_CREAT|os.O_RDWR|os.O_TRUNC,0o600)
    try:
        assert fs.write(fd,data,0)==len(data),'short write'
        fs.fsync(fd,False)
        assert fs.read(fd,0,len(data)+1)==data,'readback'
    finally:
        fs.close(fd)
    hold(tag)
finally:
    fs.shutdown()
`

// dualStackS3 sends SigV4-signed S3 requests with http.client to the
// explicit gateway endpoints argv[1] (IPv4) and argv[2] (IPv6): it creates a
// bucket over IPv4, writes one random object over each family and reads both
// objects back over both families, printing one line per completed request.
// It keeps the payload written over IPv6 in /tmp/s3-ipv6. It does not use
// urllib, which adds a form Content-Type to PUT requests that RGW denies.
const dualStackS3 = `import datetime,hashlib,hmac,http.client,os,sys
v4,v6,access,secret,region=sys.argv[1:6]
def request(host,method,path,body=b''):
    stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    digest=hashlib.sha256(body).hexdigest()
    signed='host;x-amz-content-sha256;x-amz-date'
    canonical='\n'.join([method,path,'','host:'+host+'\nx-amz-content-sha256:'+digest+'\nx-amz-date:'+stamp+'\n',signed,digest])
    scope=stamp[:8]+'/'+region+'/s3/aws4_request'
    text='AWS4-HMAC-SHA256\n'+stamp+'\n'+scope+'\n'+hashlib.sha256(canonical.encode()).hexdigest()
    key=('AWS4'+secret).encode()
    for part in (stamp[:8],region,'s3','aws4_request'):
        key=hmac.new(key,part.encode(),hashlib.sha256).digest()
    signature=hmac.new(key,text.encode(),hashlib.sha256).hexdigest()
    headers={'Host':host,'X-Amz-Date':stamp,'X-Amz-Content-Sha256':digest,
        'Authorization':'AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s'%(access,scope,signed,signature)}
    connection=http.client.HTTPConnection(host,timeout=30)
    try:
        connection.request(method,path,body=body if method=='PUT' else None,headers=headers)
        response=connection.getresponse()
        return response.status,response.read()
    finally:
        connection.close()
status,body=request(v4,'PUT','/tc-dual-stack')
assert status==200,('create bucket',status,body)
payloads={'ipv4':os.urandom(1024),'ipv6':os.urandom(1024)}
with open('/tmp/s3-ipv6','wb') as out:
    out.write(payloads['ipv6'])
for family,host in (('ipv4',v4),('ipv6',v6)):
    status,body=request(host,'PUT','/tc-dual-stack/'+family,payloads[family])
    assert status==200,('put',family,status,body)
    print('PUT',family,'via',host)
for host in (v4,v6):
    for name,data in payloads.items():
        status,body=request(host,'GET','/tc-dual-stack/'+name)
        assert status==200 and body==data,('get',name,host,status,len(body))
        print('GET',name,'via',host)
`

// tcpSocket is one established TCP connection seen from one container.
type tcpSocket struct{ local, remote netip.AddrPort }

func (s tcpSocket) String() string { return s.local.String() + "->" + s.remote.String() }

// dualStackSession is a client process that keeps its sessions open until
// release.
type dualStackSession struct {
	client  testcontainers.Container
	tag     string
	sockets []tcpSocket
	done    chan heldResult
}

type heldResult struct {
	code   int
	output []byte
	err    error
}

// startHeldSession runs script in client and waits until it has listed its
// established connections.
func startDualStackSession(t *testing.T, ctx context.Context, client testcontainers.Container, script, tag string, args ...string) *dualStackSession {
	t.Helper()
	session := &dualStackSession{client: client, tag: tag, done: make(chan heldResult, 1)}
	go func() {
		code, reader, err := client.Exec(ctx, append([]string{"timeout", "150", "python3", "-c", script, tag}, args...), tcexec.Multiplexed())
		var output []byte
		if err == nil {
			output, _ = io.ReadAll(reader)
		}
		session.done <- heldResult{code, output, err}
	}()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		select {
		case result := <-session.done:
			t.Fatalf("%s client ended before holding its session: exit %d: %s %v", tag, result.code, result.output, result.err)
		default:
		}
		code, reader, err := client.Exec(ctx, []string{"cat", "/tmp/" + tag + ".json"}, tcexec.Multiplexed())
		if err == nil && code == 0 {
			output, _ := io.ReadAll(reader)
			session.sockets = parseSocketListing(t, tag, output)
			return session
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s client did not list its sockets", tag)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// parseSocketListing decodes the {"sockets": [...]} file that a held client
// writes.
func parseSocketListing(t *testing.T, tag string, output []byte) []tcpSocket {
	t.Helper()
	var listing struct {
		Sockets [][4]any `json:"sockets"`
	}
	if err := json.Unmarshal(output, &listing); err != nil {
		t.Fatalf("%s socket listing: %s", tag, output)
	}
	var sockets []tcpSocket
	for _, entry := range listing.Sockets {
		local, lok := pythonEndpoint(entry[0], entry[1])
		remote, rok := pythonEndpoint(entry[2], entry[3])
		if lok && rok {
			sockets = append(sockets, tcpSocket{local, remote})
		}
	}
	return sockets
}

func pythonEndpoint(ip, port any) (netip.AddrPort, bool) {
	text, _ := ip.(string)
	number, _ := port.(float64)
	addr, err := netip.ParseAddr(text)
	if err != nil {
		return netip.AddrPort{}, false
	}
	return netip.AddrPortFrom(addr.Unmap(), uint16(number)), true
}

// release lets the client close its sessions and checks that it succeeded.
func (s *dualStackSession) release(t *testing.T, ctx context.Context) {
	t.Helper()
	if code, _, err := s.client.Exec(ctx, []string{"touch", "/tmp/" + s.tag + ".release"}); err != nil || code != 0 {
		t.Fatalf("release %s: exit %d: %v", s.tag, code, err)
	}
	result := <-s.done
	if result.err != nil || result.code != 0 {
		t.Fatalf("%s client: exit %d: %s %v", s.tag, result.code, result.output, result.err)
	}
}

// to returns the client's connection to endpoint.
func (s *dualStackSession) to(endpoint string) (tcpSocket, bool) {
	target, err := netip.ParseAddrPort(endpoint)
	if err != nil {
		return tcpSocket{}, false
	}
	for _, socket := range s.sockets {
		if socket.remote == target {
			return socket, true
		}
	}
	return tcpSocket{}, false
}

// containerSockets reads the established TCP connections of a daemon
// container from its /proc/net/tcp{,6}.
func containerSockets(t *testing.T, ctx context.Context, ctr testcontainers.Container) []tcpSocket {
	t.Helper()
	code, reader, err := ctr.Exec(ctx, []string{"cat", "/proc/net/tcp", "/proc/net/tcp6"}, tcexec.Multiplexed())
	if err != nil || code != 0 {
		t.Fatalf("read the socket table of %s: exit %d: %v", ctr.GetContainerID(), code, err)
	}
	output, _ := io.ReadAll(reader)
	var sockets []tcpSocket
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 || fields[3] != "01" {
			continue
		}
		local, lok := procEndpoint(fields[1])
		remote, rok := procEndpoint(fields[2])
		if lok && rok {
			sockets = append(sockets, tcpSocket{local, remote})
		}
	}
	return sockets
}

// procEndpoint decodes a /proc/net/tcp{,6} address: 32-bit words in host
// (little-endian) order followed by a hexadecimal port.
func procEndpoint(text string) (netip.AddrPort, bool) {
	host, portText, ok := strings.Cut(text, ":")
	raw, err := hex.DecodeString(host)
	port, perr := strconv.ParseUint(portText, 16, 16)
	if !ok || err != nil || perr != nil || (len(raw) != 4 && len(raw) != 16) {
		return netip.AddrPort{}, false
	}
	for i := 0; i < len(raw); i += 4 {
		raw[i], raw[i+1], raw[i+2], raw[i+3] = raw[i+3], raw[i+2], raw[i+1], raw[i]
	}
	addr, _ := netip.AddrFromSlice(raw)
	return netip.AddrPortFrom(addr.Unmap(), uint16(port)), true
}

// familyOf names the family of an advertised address.
func familyOf(endpoint string) string {
	if netip.MustParseAddrPort(endpoint).Addr().Is4() {
		return "ipv4"
	}
	return "ipv6"
}

// checkCompletedSession checks that the held client has a connection to the
// first v2 entry of a daemon's advertised vector, which is the entry a native
// client dials, and that the daemon's container holds the same connection
// with the endpoints reversed. It returns the family of that connection.
func checkCompletedSession(t *testing.T, ctx context.Context, session *dualStackSession, daemon string, vector []ceph.EntityAddress, ctr testcontainers.Container) string {
	t.Helper()
	first := ""
	for _, entry := range vector {
		if entry.Type == "v2" {
			first = entry.Addr
			break
		}
	}
	if _, ok := session.to(first); !ok {
		t.Fatalf("%s: the client has no connection to %s, the first v2 entry of %s's vector %v; its connections: %v", session.tag, first, daemon, vector, session.sockets)
	}
	for _, other := range vector {
		if other.Addr != first {
			if extra, ok := session.to(other.Addr); ok {
				t.Fatalf("%s: the client also connected to %s's %s: %v", session.tag, daemon, other.Addr, extra)
			}
		}
	}
	return checkSessionTo(t, ctx, session, daemon, first, ctr)
}

// checkSessionTo checks that the held client has a connection to endpoint
// and that the daemon's container holds the same connection with the
// endpoints reversed.
func checkSessionTo(t *testing.T, ctx context.Context, session *dualStackSession, daemon, endpoint string, ctr testcontainers.Container) string {
	t.Helper()
	client, ok := session.to(endpoint)
	if !ok {
		t.Fatalf("%s: the client has no connection to %s's %s; its connections: %v", session.tag, daemon, endpoint, session.sockets)
	}
	reversed := tcpSocket{local: client.remote, remote: client.local}
	if !slices.Contains(containerSockets(t, ctx, ctr), reversed) {
		t.Fatalf("%s: %s's container holds no connection %s <- %s", session.tag, daemon, reversed.local, reversed.remote)
	}
	return fmt.Sprintf("%s/%s(%s<-%s)", daemon, familyOf(endpoint), reversed.local, reversed.remote)
}

// dualStackOrderConfig writes a WithConfigFile file that makes the named
// daemon sections advertise their IPv6 address first.
func dualStackOrderConfig(t *testing.T, sections ...string) string {
	t.Helper()
	var text strings.Builder
	for _, section := range sections {
		fmt.Fprintf(&text, "[%s]\nms bind prefer ipv4 = false\n", section)
	}
	path := filepath.Join(t.TempDir(), "dual-stack-order.conf")
	if err := os.WriteFile(path, []byte(text.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// objectsPerPrimary returns one object name of pool for each OSD, chosen by
// the OSD that is its acting primary.
func objectsPerPrimary(t *testing.T, ctx context.Context, cluster *ceph.Container, pool string, osds int) map[int]string {
	t.Helper()
	names := map[int]string{}
	for i := 0; i < 256 && len(names) < osds; i++ {
		name := fmt.Sprintf("dual-stack-%d", i)
		output, err := cluster.Ceph(ctx, "osd", "map", pool, name, "--format", "json")
		if err != nil {
			t.Fatal(err)
		}
		var mapping struct {
			Primary int `json:"acting_primary"`
		}
		if err := json.Unmarshal(output, &mapping); err != nil {
			t.Fatalf("osd map: %s", output)
		}
		if _, ok := names[mapping.Primary]; !ok {
			names[mapping.Primary] = name
		}
	}
	if len(names) != osds {
		t.Fatalf("objects of %s map to primaries %v, want %d OSDs", pool, names, osds)
	}
	return names
}

// mdsVector is one MDS of a filesystem with its advertised vector.
type mdsVector struct {
	Name  string
	GID   uint64
	State string
	// Rank is the MDS's rank, -1 for a standby. Incarnation is the MDSMap
	// epoch in which the daemon took its rank.
	Rank        int
	Incarnation uint64
	Addrs       []ceph.EntityAddress
}

// filesystemMDSs reads the filesystem's active MDS and its standbys with
// their advertised vectors from fs dump.
func filesystemMDSs(t *testing.T, ctx context.Context, cluster *ceph.Container, name string) (active *mdsVector, standbys []mdsVector) {
	t.Helper()
	output, err := cluster.Ceph(ctx, "fs", "dump", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	type nativeMDS struct {
		Name        string `json:"name"`
		GID         uint64 `json:"gid"`
		State       string `json:"state"`
		Rank        int    `json:"rank"`
		Incarnation uint64 `json:"incarnation"`
		Addrs       struct {
			Addrvec []ceph.EntityAddress `json:"addrvec"`
		} `json:"addrs"`
	}
	var dump struct {
		Standbys    []nativeMDS `json:"standbys"`
		Filesystems []struct {
			MDSMap struct {
				Name string               `json:"fs_name"`
				Up   map[string]uint64    `json:"up"`
				Info map[string]nativeMDS `json:"info"`
			} `json:"mdsmap"`
		} `json:"filesystems"`
	}
	if err := json.Unmarshal(output, &dump); err != nil {
		t.Fatalf("fs dump: %v: %s", err, output)
	}
	for _, fs := range dump.Filesystems {
		if fs.MDSMap.Name != name {
			continue
		}
		for _, info := range fs.MDSMap.Info {
			entry := mdsVector{info.Name, info.GID, info.State, info.Rank, info.Incarnation, info.Addrs.Addrvec}
			if gid, ok := fs.MDSMap.Up["mds_0"]; ok && gid == info.GID && info.State == "up:active" {
				active = &entry
			} else {
				standbys = append(standbys, entry)
			}
		}
	}
	for _, info := range dump.Standbys {
		standbys = append(standbys, mdsVector{info.Name, info.GID, info.State, info.Rank, info.Incarnation, info.Addrs.Addrvec})
	}
	return active, standbys
}

// waitActiveMDS waits until rank 0 of the filesystem is active on an MDS
// other than previous and one standby is registered.
func waitActiveMDS(t *testing.T, ctx context.Context, cluster *ceph.Container, name, previous string) (mdsVector, []mdsVector) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for {
		active, standbys := filesystemMDSs(t, ctx, cluster, name)
		if active != nil && active.Name != previous && len(standbys) == 1 && len(active.Addrs) != 0 && len(standbys[0].Addrs) != 0 {
			return *active, standbys
		}
		if time.Now().After(deadline) {
			t.Fatalf("filesystem %s has no new active MDS: active %+v, standbys %+v", name, active, standbys)
		}
		time.Sleep(time.Second)
	}
}

// mdsContainer returns the owned container of the named MDS.
func mdsContainer(t *testing.T, cluster *ceph.Container, name string) testcontainers.Container {
	t.Helper()
	for _, fs := range cephfs.Filesystems(cluster) {
		for _, mds := range fs.MDSs() {
			if mds.ID == name {
				return mds.Container
			}
		}
	}
	t.Fatalf("no owned MDS %s", name)
	return nil
}

// managerContainer returns the owned container of the named MGR.
func managerContainer(t *testing.T, cluster *ceph.Container, name string) testcontainers.Container {
	t.Helper()
	for _, mgr := range clusteraccess.Managers(t, cluster) {
		if mgr.DaemonName == name {
			return mgr.Container
		}
	}
	t.Fatalf("no owned MGR %s", name)
	return nil
}

// osdContainer returns the owned container of the OSD.
func osdContainer(t *testing.T, cluster *ceph.Container, id int) testcontainers.Container {
	t.Helper()
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		if osd.ID == id {
			return osd.Container
		}
	}
	t.Fatalf("no owned osd.%d", id)
	return nil
}

// bridgeAddresses returns the IPv4 and IPv6 address of a container on the
// cluster's public bridge.
func bridgeAddresses(t *testing.T, ctx context.Context, cluster *ceph.Container, ctr testcontainers.Container) (netip.Addr, netip.Addr) {
	t.Helper()
	inspection, err := ctr.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := inspection.NetworkSettings.Networks[cluster.NetworkName()]
	if endpoint == nil || !endpoint.IPAddress.Is4() || !endpoint.GlobalIPv6Address.Is6() {
		t.Fatalf("endpoint of %s on %s: %+v", ctr.GetContainerID(), cluster.NetworkName(), endpoint)
	}
	return endpoint.IPAddress, endpoint.GlobalIPv6Address
}

// gatewayListeners returns the listening sockets of the gateway container
// on port 7480.
func gatewayListeners(t *testing.T, ctx context.Context, gateway *rgw.Gateway) []string {
	t.Helper()
	code, reader, err := gateway.Exec(ctx, []string{"cat", "/proc/net/tcp", "/proc/net/tcp6"}, tcexec.Multiplexed())
	if err != nil || code != 0 {
		t.Fatalf("read the gateway's socket table: exit %d: %v", code, err)
	}
	output, _ := io.ReadAll(reader)
	var listeners []string
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 || fields[3] != "0A" {
			continue
		}
		if local, ok := procEndpoint(fields[1]); ok && local.Port() == 7480 {
			listeners = append(listeners, local.String())
		}
	}
	slices.Sort(listeners)
	return listeners
}

// firstV2 returns the first v2 entry of a vector.
func firstV2(vector []ceph.EntityAddress) string {
	for _, entry := range vector {
		if entry.Type == "v2" {
			return entry.Addr
		}
	}
	return ""
}

// testDualStackServices starts a dual-stack CephFS and RGW cluster with two
// MGRs, two OSDs and two MDSs. ms_bind_prefer_ipv4 = false in [mgr.b],
// [osd.1] and [mds.tc-cephfs-1] makes those daemons list their IPv6 address
// first. A native client dials the first entry of a vector, so the same
// librados and libcephfs clients complete their MGR, OSD and MDS sessions
// over IPv4 with the default daemons and over IPv6 with the others. Each
// session is checked on both ends: the client's connection to the first v2
// entry and the same connection in the daemon container's socket table.
func testDualStackServices(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	image, roles := integrationImages(t)
	const filesystem = "tc-cephfs"
	opts := append(roles, ceph.WithOSDCount(2), ceph.WithManagerCount(2),
		ceph.WithAddressFamily(ceph.AddressDualStack),
		ceph.WithConfigFile(dualStackOrderConfig(t, "mgr.b", "osd.1", "mds."+filesystem+"-1")),
		cephfs.WithFilesystems(cephfs.Config{Name: filesystem, ActiveMDS: 1, StandbyMDS: 1}),
		rgw.WithGateways(rgw.Config{}))
	cluster, client := newServiceClusterRun(t, cephfsRun, image, opts...)
	addresses, endpoints := waitDualStackAddresses(t, ctx, cluster, 1, 2)
	ipv6First := map[string]bool{"mgr.b": true, "osd.1": true, "mds." + filesystem + "-1": true}
	checkOrder := func(daemon string, vector []ceph.EntityAddress) {
		t.Helper()
		want := "ipv4"
		if ipv6First[daemon] {
			want = "ipv6"
		}
		if got := familyOf(vector[0].Addr); got != want {
			t.Fatalf("%s lists %s first, want an %s address: %v", daemon, vector[0].Addr, want, vector)
		}
	}
	for _, osd := range addresses.OSDs {
		name := fmt.Sprintf("osd.%d", osd.ID)
		for _, vector := range [][]ceph.EntityAddress{osd.Public, osd.Cluster, osd.HeartbeatFront, osd.HeartbeatBack} {
			checkOrder(name, vector)
		}
	}

	// MGR and OSD sessions of librados, before and after a MGR failover.
	const pool = "tc-dual-stack"
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	objects := objectsPerPrimary(t, ctx, cluster, pool, 2)
	monHost := "[v2:" + endpoints["mon."+addresses.Monitors[0].Name].v4 + "]"
	families := map[string]map[string]bool{"mgr": {}, "osd": {}, "mds": {}}
	var receipts []string
	radosSessions := func(tag string) {
		t.Helper()
		session := startDualStackSession(t, ctx, client, dualStackRadosHeld, tag, monHost, pool, objects[0], objects[1])
		mgr := addresses.ActiveManager
		checkOrder("mgr."+mgr.Name, mgr.Addrs)
		receipts = append(receipts, tag+":"+checkCompletedSession(t, ctx, session, "mgr."+mgr.Name, mgr.Addrs, managerContainer(t, cluster, mgr.Name)))
		families["mgr"][familyOf(firstV2(mgr.Addrs))] = true
		for _, osd := range addresses.OSDs {
			name := fmt.Sprintf("osd.%d", osd.ID)
			receipts = append(receipts, tag+":"+checkCompletedSession(t, ctx, session, name, osd.Public, osdContainer(t, cluster, osd.ID)))
			families["osd"][familyOf(firstV2(osd.Public))] = true
		}
		session.release(t, ctx)
	}
	radosSessions("rados-first-mgr")
	first := addresses.ActiveManager.Name
	if err := cluster.RemoveManager(ctx, first); err != nil {
		t.Fatalf("fail over from mgr.%s: %v", first, err)
	}
	addresses, _ = waitDualStackAddresses(t, ctx, cluster, 1, 2)
	if addresses.ActiveManager.Name == first {
		t.Fatalf("mgr.%s is still active", first)
	}
	radosSessions("rados-second-mgr")

	// MDS sessions of libcephfs, before and after an MDS failover.
	cephfsSession := func(tag, previous string) string {
		t.Helper()
		active, standbys := waitActiveMDS(t, ctx, cluster, filesystem, previous)
		for _, mds := range append(standbys, active) {
			checkDualStackVector(t, "mds."+mds.Name, mds.Addrs)
			checkOrder("mds."+mds.Name, mds.Addrs)
		}
		session := startDualStackSession(t, ctx, client, dualStackCephFSHeld, tag, filesystem)
		receipts = append(receipts, tag+":"+checkCompletedSession(t, ctx, session, "mds."+active.Name, active.Addrs, mdsContainer(t, cluster, active.Name)))
		families["mds"][familyOf(firstV2(active.Addrs))] = true
		session.release(t, ctx)
		return active.Name
	}
	firstMDS := cephfsSession("cephfs-first-mds", "")
	if _, err := cluster.Ceph(ctx, "mds", "fail", firstMDS); err != nil {
		t.Fatal(err)
	}
	cephfsSession("cephfs-second-mds", firstMDS)
	for role, seen := range families {
		if !seen["ipv4"] || !seen["ipv6"] {
			t.Fatalf("%s sessions completed over %v, want both families; %v", role, seen, receipts)
		}
	}

	// S3 over explicit IPv4 and IPv6 gateway addresses, and over the
	// published host port.
	gateways := clusteraccess.Gateways(t, cluster)
	if len(gateways) != 1 {
		t.Fatalf("%d gateways", len(gateways))
	}
	gateway := gateways[0]
	listeners := gatewayListeners(t, ctx, gateway)
	if !slices.Equal(listeners, []string{"0.0.0.0:7480", "[::]:7480"}) {
		t.Fatalf("gateway listens on %v, want 0.0.0.0:7480 and [::]:7480", listeners)
	}
	gateway4, gateway6 := bridgeAddresses(t, ctx, cluster, gateway)
	v4, v6 := netip.AddrPortFrom(gateway4, 7480).String(), netip.AddrPortFrom(gateway6, 7480).String()
	code, reader, err := client.Exec(ctx, []string{"timeout", "120", "python3", "-c", dualStackS3, v4, v6, gateway.AccessKey, gateway.SecretKey, gateway.Region}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, _ := io.ReadAll(reader)
	s3 := strings.Split(strings.TrimSpace(string(output)), "\n")
	if code != 0 || len(s3) != 6 {
		t.Fatalf("S3 over %s and %s: exit %d: %s", v4, v6, code, output)
	}
	// The gateway's access log names the client address of each family.
	client4, client6 := bridgeAddresses(t, ctx, cluster, client)
	var accessLog []string
	for _, want := range []string{
		"beast: .*: " + regexp.QuoteMeta(client4.String()) + ` - .*"PUT /tc-dual-stack/ipv4 HTTP/1.1" 200`,
		"beast: .*: " + regexp.QuoteMeta(client6.String()) + ` - .*"PUT /tc-dual-stack/ipv6 HTTP/1.1" 200`,
	} {
		pattern := regexp.MustCompile(want)
		deadline := time.Now().Add(30 * time.Second)
		for {
			logs, err := gateway.Logs(ctx)
			if err != nil {
				t.Fatal(err)
			}
			data, _ := io.ReadAll(logs)
			_ = logs.Close()
			if line := pattern.Find(data); line != nil {
				accessLog = append(accessLog, string(line))
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the gateway logged no request matching %s", want)
			}
			time.Sleep(time.Second)
		}
	}
	code, reader, err = client.Exec(ctx, []string{"cat", "/tmp/s3-ipv6"}, tcexec.Multiplexed())
	if err != nil || code != 0 {
		t.Fatalf("read the written S3 payload: exit %d: %v", code, err)
	}
	written, _ := io.ReadAll(reader)
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	host := s3HTTPClient{endpoint: endpoint, accessKey: gateway.AccessKey, secretKey: gateway.SecretKey, region: gateway.Region, http: &http.Client{Timeout: 45 * time.Second}}
	if body := host.request(t, ctx, http.MethodGet, "/tc-dual-stack/ipv6", nil, http.StatusOK); len(written) != 1024 || !bytes.Equal(body, written) {
		t.Fatalf("host S3 read %d bytes that differ from the %d written over IPv6", len(body), len(written))
	}
	t.Logf("DUAL_STACK_SERVICES_RECEIPT image=%s ceph=%s sessions=%v rgw_listeners=%v s3=%v rgw_access=%q host_s3=%s",
		cluster.ControlImage(), cluster.CephVersion(), receipts, listeners, s3, accessLog, endpoint)
}
