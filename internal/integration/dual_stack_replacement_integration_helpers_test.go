//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/netip"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// dualStackRetained keeps one librados session and one libcephfs mount open
// for the whole test. For each round n the test writes /tmp/<tag>.go.<n>, a
// JSON list of object names. The client then sends a MON and a MGR command,
// writes and reads each object, writes and fsyncs the file /<tag>-<n>, reads
// back the files of every round so far, and writes its established TCP
// connections to /tmp/<tag>.<n>.json. It closes its sessions when the test
// creates /tmp/<tag>.release.
const dualStackRetained = dualStackSockets + `import sys,rados,cephfs
tag,pool,filesystem=sys.argv[1:4]
def content(k):
    return bytes(range(256))*16+('%s/%d'%(tag,k)).encode()
c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'rados_mon_op_timeout':'30','rados_osd_op_timeout':'30'})
c.connect()
try:
    fs=cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf',auth_id='admin')
    fs.conf_set('client_mount_timeout','30')
    fs.mount(filesystem_name=filesystem.encode())
    try:
        io=c.open_ioctx(pool)
        try:
            n=0
            deadline=time.time()+1400
            while True:
                go='/tmp/%s.go.%d'%(tag,n)
                while not os.path.exists(go):
                    if os.path.exists('/tmp/'+tag+'.release'):
                        raise SystemExit(0)
                    assert time.time()<deadline,'not released'
                    time.sleep(0.2)
                objects=json.load(open(go))
                r,_,err=c.mon_command(json.dumps({'prefix':'fsid','format':'json'}),b'')
                assert r==0,('mon',n,r,err)
                r,_,err=c.mgr_command(json.dumps({'prefix':'pg stat','format':'json'}),b'')
                assert r==0,('mgr',n,r,err)
                for name in objects:
                    data=('%s/%d/%s'%(tag,n,name)).encode()*64
                    io.write_full(name,data)
                    assert io.read(name,len(data)+1)==data,('readback',n,name)
                fd=fs.open('/%s-%d'%(tag,n),os.O_CREAT|os.O_RDWR|os.O_TRUNC,0o600)
                try:
                    assert fs.write(fd,content(n),0)==len(content(n)),'short write'
                    fs.fsync(fd,False)
                finally:
                    fs.close(fd)
                for k in range(n+1):
                    fd=fs.open('/%s-%d'%(tag,k),os.O_RDONLY,0)
                    try:
                        assert fs.read(fd,0,len(content(k))+1)==content(k),('cephfs readback',n,k)
                    finally:
                        fs.close(fd)
                out='/tmp/%s.%d.json'%(tag,n)
                with open(out+'.tmp','w') as f:
                    json.dump({'sockets':sockets()},f)
                os.rename(out+'.tmp',out)
                n+=1
        finally:
            io.close()
    finally:
        fs.shutdown()
finally:
    c.shutdown()
`

// dualStackFreshRead mounts the filesystem argv[1] in a new process and
// checks the files that dualStackRetained wrote in rounds 0 to argv[3]-1,
// then reads the objects argv[6:] of pool argv[4] that it wrote in round
// argv[5].
const dualStackFreshRead = `import os,sys,cephfs,rados
filesystem,tag,rounds,pool,last=sys.argv[1],sys.argv[2],int(sys.argv[3]),sys.argv[4],int(sys.argv[5])
fs=cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf',auth_id='admin')
fs.conf_set('client_mount_timeout','30')
fs.mount(filesystem_name=filesystem.encode())
try:
    for k in range(rounds):
        want=bytes(range(256))*16+('%s/%d'%(tag,k)).encode()
        fd=fs.open('/%s-%d'%(tag,k),os.O_RDONLY,0)
        try:
            assert fs.read(fd,0,len(want)+1)==want,('cephfs',k)
        finally:
            fs.close(fd)
finally:
    fs.shutdown()
c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'rados_osd_op_timeout':'30'})
c.connect()
try:
    io=c.open_ioctx(pool)
    for name in sys.argv[6:]:
        want=('%s/%d/%s'%(tag,last,name)).encode()*64
        assert io.read(name,len(want)+1)==want,('rados',name)
    io.close()
finally:
    c.shutdown()
print('files',rounds,'objects',len(sys.argv[6:]))
`

// retainedClient is a running dualStackRetained process.
type retainedClient struct {
	ctr  testcontainers.Container
	tag  string
	done chan heldResult
}

func startRetainedClient(ctx context.Context, ctr testcontainers.Container, tag, pool, filesystem string) *retainedClient {
	client := &retainedClient{ctr: ctr, tag: tag, done: make(chan heldResult, 1)}
	go func() {
		code, reader, err := ctr.Exec(ctx, []string{"timeout", "1500", "python3", "-c", dualStackRetained, tag, pool, filesystem}, tcexec.Multiplexed())
		var output []byte
		if err == nil {
			output, _ = io.ReadAll(reader)
		}
		client.done <- heldResult{code, output, err}
	}()
	return client
}

// round runs round n of the retained client with the given objects and
// returns its established connections afterwards.
func (r *retainedClient) round(t *testing.T, ctx context.Context, n int, objects []string) *dualStackSession {
	t.Helper()
	data, err := json.Marshal(objects)
	if err != nil {
		t.Fatal(err)
	}
	trigger := fmt.Sprintf("/tmp/%s.go.%d", r.tag, n)
	if err := r.ctr.CopyToContainer(ctx, data, trigger+".tmp", 0o644); err != nil {
		t.Fatal(err)
	}
	if code, _, err := r.ctr.Exec(ctx, []string{"mv", trigger + ".tmp", trigger}); err != nil || code != 0 {
		t.Fatalf("start round %d: exit %d: %v", n, code, err)
	}
	tag := fmt.Sprintf("%s-%d", r.tag, n)
	deadline := time.Now().Add(3 * time.Minute)
	for {
		select {
		case result := <-r.done:
			t.Fatalf("retained client ended in round %d: exit %d: %s %v", n, result.code, result.output, result.err)
		default:
		}
		code, reader, err := r.ctr.Exec(ctx, []string{"cat", fmt.Sprintf("/tmp/%s.%d.json", r.tag, n)}, tcexec.Multiplexed())
		if err == nil && code == 0 {
			output, _ := io.ReadAll(reader)
			return &dualStackSession{client: r.ctr, tag: tag, sockets: parseSocketListing(t, tag, output)}
		}
		if time.Now().After(deadline) {
			t.Fatalf("retained client did not finish round %d", n)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// release lets the retained client close its sessions and checks that it
// succeeded.
func (r *retainedClient) release(t *testing.T, ctx context.Context) {
	t.Helper()
	if code, _, err := r.ctr.Exec(ctx, []string{"touch", "/tmp/" + r.tag + ".release"}); err != nil || code != 0 {
		t.Fatalf("release %s: exit %d: %v", r.tag, code, err)
	}
	result := <-r.done
	if result.err != nil || result.code != 0 {
		t.Fatalf("retained client: exit %d: %s %v", result.code, result.output, result.err)
	}
}

// listeningSockets returns the listening TCP sockets of a container, read
// from its /proc/net/tcp{,6}.
func listeningSockets(t *testing.T, ctx context.Context, ctr testcontainers.Container) []string {
	t.Helper()
	code, reader, err := ctr.Exec(ctx, []string{"cat", "/proc/net/tcp", "/proc/net/tcp6"}, tcexec.Multiplexed())
	if err != nil || code != 0 {
		t.Fatalf("read the socket table of %s: exit %d: %v", ctr.GetContainerID(), code, err)
	}
	output, _ := io.ReadAll(reader)
	var listeners []string
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 || fields[3] != "0A" {
			continue
		}
		if local, ok := procEndpoint(fields[1]); ok {
			listeners = append(listeners, local.String())
		}
	}
	slices.Sort(listeners)
	return listeners
}

// monitorContainer returns the owned container of the named MON.
func monitorContainer(t *testing.T, cluster *ceph.Container, name string) testcontainers.Container {
	t.Helper()
	for _, mon := range clusteraccess.Monitors(t, cluster) {
		if mon.DaemonName == name {
			return mon.Container
		}
	}
	t.Fatalf("no owned mon.%s", name)
	return nil
}

// vectorIPs returns the IPv4 and IPv6 address of a dual-stack vector.
func vectorIPs(vector []ceph.EntityAddress) (v4, v6 netip.Addr) {
	for _, entry := range vector {
		ip := netip.MustParseAddrPort(entry.Addr).Addr()
		if ip.Is4() {
			v4 = ip
		} else {
			v6 = ip
		}
	}
	return v4, v6
}

// checkNewAddresses checks that no entry of a replacement's vector can be
// mistaken for one of the replaced daemon's: an entry with the same address
// must have another nonce. Docker hands a removed or stopped container's
// address to a later container, so a replacement can get the old IP address
// of its own predecessor or of another removed daemon. It returns the
// families whose IP address the replacement reuses.
func checkNewAddresses(t *testing.T, old, replacement string, before, after []ceph.EntityAddress) []string {
	t.Helper()
	for _, entry := range after {
		for _, previous := range before {
			if entry == previous {
				t.Fatalf("%s advertises %s/%d, the same entry as %s's %v", replacement, entry.Addr, entry.Nonce, old, before)
			}
		}
	}
	old4, old6 := vectorIPs(before)
	new4, new6 := vectorIPs(after)
	var reused []string
	if old4 == new4 {
		reused = append(reused, "ipv4")
	}
	if old6 == new6 {
		reused = append(reused, "ipv6")
	}
	return reused
}

// vectorString prints a vector with each entry's nonce.
func vectorString(vector []ceph.EntityAddress) string {
	entries := make([]string, len(vector))
	for i, entry := range vector {
		entries[i] = fmt.Sprintf("%s:%s/%d", entry.Type, entry.Addr, entry.Nonce)
	}
	return "[" + strings.Join(entries, ",") + "]"
}

// fsMapEpoch reads the FSMap epoch.
func fsMapEpoch(t *testing.T, ctx context.Context, cluster *ceph.Container) int {
	t.Helper()
	output, err := cluster.Ceph(ctx, "fs", "dump", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var dump struct {
		Epoch int `json:"epoch"`
	}
	if err := json.Unmarshal(output, &dump); err != nil || dump.Epoch == 0 {
		t.Fatalf("fs dump epoch: %v: %s", err, output)
	}
	return dump.Epoch
}

// monHostEntries are the addresses in a mon_host value.
var monHostEntries = regexp.MustCompile(`v[12]:(\[[0-9a-f:]+\]:[0-9]+|[0-9.]+:[0-9]+)`)

// dualStackTopology is one observation of the cluster: the MON, MGR and OSD
// maps and the filesystem's only MDS.
type dualStackTopology struct {
	addresses ceph.DaemonAddresses
	mds       mdsVector
	fsEpoch   int
}

// testDualStackReplacement starts a dual-stack CephFS cluster with two MONs,
// one MGR, two OSDs and one MDS and replaces mon.b, osd.0, the MDS and mgr.a
// with new containers in that order. ms_bind_prefer_ipv4 = false makes the
// replacements osd.2, tc-cephfs-1 and mgr.b list their IPv6 address first,
// while the daemons they replace list IPv4 first, so a client that reaches a
// replacement over IPv6 has opened a new session to it. One retained client
// process runs a round of MON, MGR, object and CephFS operations before the
// replacements and after each one. MGR is replaced last, so that the
// dual-stack admission check of RemoveManager reads the replaced MON and
// OSD vectors too.
func testDualStackReplacement(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	image, roles := integrationImages(t)
	const filesystem, pool = "tc-cephfs", "tc-dual-stack"
	replacementMDS := filesystem + "-1"
	ipv6First := map[string]bool{"osd.2": true, "mds." + replacementMDS: true, "mgr.b": true}
	opts := append(roles, ceph.WithOSDCount(2), ceph.WithMonitorCount(2), ceph.WithManagerCount(1),
		ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithAddressFamily(ceph.AddressDualStack)),
		ceph.WithConfigFile(dualStackOrderConfig(t, "osd.2", "mds."+replacementMDS, "mgr.b")),
		cephfs.WithFilesystems(cephfs.Config{Name: filesystem, ActiveMDS: 1}))
	cluster, client := newServiceClusterRun(t, cephfsRun, image, opts...)
	quorum, err := observeCluster(cluster).QuorumStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fsid := quorum.MonMap.FSID
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rados"}); err != nil {
		t.Fatal(err)
	}

	// stale maps every address entry of a replaced daemon, address and
	// nonce, to its name. Docker can give a replaced daemon's IP address to
	// a later container, so an equal address alone is not stale; the nonce
	// tells the daemon instances apart.
	type entryKey struct {
		addr  netip.AddrPort
		nonce uint32
	}
	stale := map[entryKey]string{}
	addStale := func(daemon string, vectors ...[]ceph.EntityAddress) {
		for _, vector := range vectors {
			for _, entry := range vector {
				stale[entryKey{netip.MustParseAddrPort(entry.Addr), entry.Nonce}] = daemon
			}
		}
	}
	checkNoStale := func(where string, vector []ceph.EntityAddress) {
		t.Helper()
		for _, entry := range vector {
			if daemon, ok := stale[entryKey{netip.MustParseAddrPort(entry.Addr), entry.Nonce}]; ok {
				t.Fatalf("%s advertises %s/%d, an entry of the replaced %s", where, entry.Addr, entry.Nonce, daemon)
			}
		}
	}
	// staleAddress returns the replaced daemon that advertised addr when
	// no current daemon advertises it.
	staleAddress := func(addr netip.AddrPort, current []ceph.EntityAddress) (string, bool) {
		for _, entry := range current {
			if netip.MustParseAddrPort(entry.Addr) == addr {
				return "", false
			}
		}
		for key, daemon := range stale {
			if key.addr == addr {
				return daemon, true
			}
		}
		return "", false
	}
	checkOrder := func(daemon string, vector []ceph.EntityAddress) {
		t.Helper()
		want := "ipv4"
		if ipv6First[daemon] {
			want = "ipv6"
		}
		if got := familyOf(firstV2(vector)); got != want {
			t.Fatalf("%s lists %s first, want an %s address: %v", daemon, firstV2(vector), want, vector)
		}
	}

	// observe waits for the expected daemons and checks every vector: both
	// families, concrete, in the configured order, and none with an address
	// of a replaced daemon.
	observe := func(monitors []string, osds []int, mgr, mds string) dualStackTopology {
		t.Helper()
		addresses, _ := waitDualStackAddresses(t, ctx, cluster, len(monitors), len(osds))
		var names []string
		for _, mon := range addresses.Monitors {
			names = append(names, mon.Name)
			checkNoStale("mon."+mon.Name, mon.Addrs)
		}
		slices.Sort(names)
		if !slices.Equal(names, monitors) {
			t.Fatalf("monmap e%d members %v, want %v", addresses.MonMapEpoch, names, monitors)
		}
		var ids []int
		for _, osd := range addresses.OSDs {
			ids = append(ids, osd.ID)
			name := fmt.Sprintf("osd.%d", osd.ID)
			for _, vector := range [][]ceph.EntityAddress{osd.Public, osd.Cluster, osd.HeartbeatFront, osd.HeartbeatBack} {
				checkOrder(name, vector)
				checkNoStale(name, vector)
			}
		}
		if !slices.Equal(ids, osds) {
			t.Fatalf("osdmap e%d has OSDs %v, want %v", addresses.OSDMapEpoch, ids, osds)
		}
		active := addresses.ActiveManager
		if active.Name != mgr || len(addresses.StandbyManagers) != 0 {
			t.Fatalf("mgrmap e%d: active %s, standbys %v, want only %s", addresses.MgrMapEpoch, active.Name, addresses.StandbyManagers, mgr)
		}
		checkOrder("mgr."+active.Name, active.Addrs)
		checkNoStale("mgr."+active.Name, active.Addrs)
		current, standbys := filesystemMDSs(t, ctx, cluster, filesystem)
		if current == nil || current.Name != mds || len(standbys) != 0 {
			t.Fatalf("filesystem %s: active %+v, standbys %+v, want only %s", filesystem, current, standbys, mds)
		}
		checkDualStackVector(t, "mds."+current.Name, current.Addrs)
		checkOrder("mds."+current.Name, current.Addrs)
		checkNoStale("mds."+current.Name, current.Addrs)
		return dualStackTopology{addresses: addresses, mds: *current, fsEpoch: fsMapEpoch(t, ctx, cluster)}
	}

	retainedCtr, err := testcontainers.Run(ctx, nativeClientImage(t, staticFacts(cluster).Images.Client), cluster.WithClient(), ceph.WithIdleEntrypoint())
	testcontainers.CleanupContainer(t, retainedCtr)
	if err != nil {
		t.Fatal(err)
	}
	retained := startRetainedClient(ctx, retainedCtr, "retained", pool, filesystem)
	var rounds []string
	var lastObjects []string
	// round runs round n of the retained client and checks its connections:
	// a MON session of librados and one of libcephfs over IPv6, the first
	// entry of the monmap vectors, and sessions to the first v2 entry of the
	// active MGR, every OSD and the MDS, each also present in the daemon
	// container's socket table, and no connection to an address of a
	// replaced daemon.
	round := func(n int, topology dualStackTopology) {
		t.Helper()
		addresses := topology.addresses
		// Objects map to their acting primaries only once peering settled.
		if err := cluster.WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
		primaries := objectsPerPrimary(t, ctx, cluster, pool, len(addresses.OSDs))
		lastObjects = lastObjects[:0]
		for _, osd := range addresses.OSDs {
			lastObjects = append(lastObjects, primaries[osd.ID])
		}
		session := retained.round(t, ctx, n, lastObjects)
		current := append([]ceph.EntityAddress{}, addresses.ActiveManager.Addrs...)
		current = append(current, topology.mds.Addrs...)
		for _, mon := range addresses.Monitors {
			current = append(current, mon.Addrs...)
		}
		for _, osd := range addresses.OSDs {
			current = append(current, osd.Public...)
		}
		for _, socket := range session.sockets {
			if daemon, ok := staleAddress(socket.remote, current); ok {
				t.Fatalf("%s: the retained client still has a connection to %s of the replaced %s", session.tag, socket.remote, daemon)
			}
		}
		var paths []string
		// librados and libcephfs each hold one MON session.
		monitors := map[netip.AddrPort]string{}
		for _, mon := range addresses.Monitors {
			for _, entry := range mon.Addrs {
				monitors[netip.MustParseAddrPort(entry.Addr)] = mon.Name
			}
		}
		var monSessions []string
		for _, socket := range session.sockets {
			name, ok := monitors[socket.remote]
			if !ok {
				continue
			}
			if !socket.remote.Addr().Is6() {
				t.Fatalf("%s: a MON session goes to mon.%s's %s, not to its first, IPv6, entry", session.tag, name, socket.remote)
			}
			reversed := tcpSocket{local: socket.remote, remote: socket.local}
			if !slices.Contains(containerSockets(t, ctx, monitorContainer(t, cluster, name)), reversed) {
				t.Fatalf("%s: mon.%s's container holds no connection %s <- %s", session.tag, name, reversed.local, reversed.remote)
			}
			monSessions = append(monSessions, fmt.Sprintf("mon.%s/ipv6(%s<-%s)", name, reversed.local, reversed.remote))
		}
		if len(monSessions) != 2 {
			t.Fatalf("%s: MON sessions %v, want one of librados and one of libcephfs; connections %v", session.tag, monSessions, session.sockets)
		}
		paths = append(paths, monSessions...)
		mgr := addresses.ActiveManager
		paths = append(paths, checkCompletedSession(t, ctx, session, "mgr."+mgr.Name, mgr.Addrs, managerContainer(t, cluster, mgr.Name)))
		for _, osd := range addresses.OSDs {
			paths = append(paths, checkCompletedSession(t, ctx, session, fmt.Sprintf("osd.%d", osd.ID), osd.Public, osdContainer(t, cluster, osd.ID)))
		}
		paths = append(paths, checkCompletedSession(t, ctx, session, "mds."+topology.mds.Name, topology.mds.Addrs, mdsContainer(t, cluster, topology.mds.Name)))
		rounds = append(rounds, fmt.Sprintf("round%d:%s", n, strings.Join(paths, ",")))
	}

	var steps []string
	initial := observe([]string{"a", "b"}, []int{0, 1}, "a", "a")
	round(0, initial)

	// MON: join mon.c, then remove mon.b.
	var oldMon ceph.DaemonAddress
	for _, mon := range initial.addresses.Monitors {
		if mon.Name == "b" {
			oldMon = mon
		}
	}
	if _, err := cluster.AddMonitor(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	if err := monitorHandle(t, ctx, cluster, "b").Remove(ctx); err != nil {
		t.Fatal(err)
	}
	addStale("mon.b", oldMon.Addrs)
	afterMon := observe([]string{"a", "c"}, []int{0, 1}, "a", "a")
	var newMon ceph.DaemonAddress
	for _, mon := range afterMon.addresses.Monitors {
		if mon.Name == "c" {
			newMon = mon
		}
	}
	reused := map[string][]string{"mon.c": checkNewAddresses(t, "mon.b", "mon.c", oldMon.Addrs, newMon.Addrs)}
	if afterMon.addresses.MonMapEpoch <= initial.addresses.MonMapEpoch {
		t.Fatalf("monmap epoch %d after the replacement, %d before", afterMon.addresses.MonMapEpoch, initial.addresses.MonMapEpoch)
	}
	bootstrap, err := observeCluster(cluster).MonitorBootstrapAddresses(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var bootstrapEntries []string
	for _, match := range monHostEntries.FindAllStringSubmatch(bootstrap, -1) {
		bootstrapEntries = append(bootstrapEntries, netip.MustParseAddrPort(match[1]).String())
	}
	for _, entry := range newMon.Addrs {
		if !slices.Contains(bootstrapEntries, netip.MustParseAddrPort(entry.Addr).String()) {
			t.Fatalf("mon_host %s lacks mon.c's %s", bootstrap, entry.Addr)
		}
	}
	var monitorEntries []ceph.EntityAddress
	for _, mon := range afterMon.addresses.Monitors {
		monitorEntries = append(monitorEntries, mon.Addrs...)
	}
	for _, entry := range bootstrapEntries {
		if daemon, ok := staleAddress(netip.MustParseAddrPort(entry), monitorEntries); ok {
			t.Fatalf("mon_host %s still lists %s of %s", bootstrap, entry, daemon)
		}
	}
	// New clients whose MON session mon_host_override pins to one family of
	// mon.c.
	monC := checkDualStackVector(t, "mon.c", newMon.Addrs)
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	primaries := objectsPerPrimary(t, ctx, cluster, pool, 2)
	var freshMon []string
	for _, endpoint := range []string{monC.v4, monC.v6} {
		tag := "fresh-mon-c-" + familyOf(endpoint)
		session := startDualStackSession(t, ctx, client, dualStackRadosHeld, tag, "[v2:"+endpoint+"]", pool, primaries[0], primaries[1])
		for _, mon := range afterMon.addresses.Monitors {
			for _, entry := range mon.Addrs {
				if _, ok := session.to(entry.Addr); ok && entry.Addr != endpoint {
					t.Fatalf("%s: a MON connection to %s besides %s", tag, entry.Addr, endpoint)
				}
			}
		}
		freshMon = append(freshMon, tag+":"+checkSessionTo(t, ctx, session, "mon.c", endpoint, monitorContainer(t, cluster, "c")))
		session.release(t, ctx)
	}
	steps = append(steps, fmt.Sprintf("mon.b%s->mon.c%s monmap_e%d->e%d mon_host=%s",
		vectorString(oldMon.Addrs), vectorString(newMon.Addrs), initial.addresses.MonMapEpoch, afterMon.addresses.MonMapEpoch, bootstrap))
	round(1, afterMon)

	// OSD: add osd.2, then remove osd.0, which drains it first.
	var oldOSD ceph.OSDAddress
	for _, osd := range afterMon.addresses.OSDs {
		if osd.ID == 0 {
			oldOSD = osd
		}
	}
	added, err := cluster.AddOSD(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if added.ID != 2 {
		t.Fatalf("the new OSD is osd.%d, want osd.2", added.ID)
	}
	if err := osdHandle(t, ctx, cluster, 0).Remove(ctx); err != nil {
		t.Fatal(err)
	}
	addStale("osd.0", oldOSD.Public, oldOSD.Cluster, oldOSD.HeartbeatFront, oldOSD.HeartbeatBack)
	afterOSD := observe([]string{"a", "c"}, []int{1, 2}, "a", "a")
	newOSD := afterOSD.addresses.OSDs[1]
	reused["osd.2"] = checkNewAddresses(t, "osd.0", "osd.2", oldOSD.Public, newOSD.Public)
	checkNewAddresses(t, "osd.0", "osd.2", oldOSD.Cluster, newOSD.Cluster)
	if afterOSD.addresses.OSDMapEpoch <= afterMon.addresses.OSDMapEpoch {
		t.Fatalf("osdmap epoch %d after the replacement, %d before", afterOSD.addresses.OSDMapEpoch, afterMon.addresses.OSDMapEpoch)
	}
	steps = append(steps, fmt.Sprintf("osd.0%s->osd.2%s osdmap_e%d->e%d",
		vectorString(oldOSD.Public), vectorString(newOSD.Public), afterMon.addresses.OSDMapEpoch, afterOSD.addresses.OSDMapEpoch))
	round(2, afterOSD)

	// MDS: stop the only MDS, fail its GID and start a replacement.
	filesystems := cephfs.Filesystems(cluster)
	if len(filesystems) != 1 || len(filesystems[0].MDSs()) != 1 {
		t.Fatalf("want one filesystem with one MDS")
	}
	fs := filesystems[0]
	oldMDS := fs.MDSs()[0]
	before := afterOSD.mds
	if oldMDS.ID != before.Name {
		t.Fatalf("owned MDS %s, active MDS %s", oldMDS.ID, before.Name)
	}
	timeout := 10 * time.Second
	if err := oldMDS.Stop(ctx, &timeout); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.Ceph(ctx, "mds", "fail", strconv.FormatUint(before.GID, 10)); err != nil {
		t.Fatal(err)
	}
	replacement, err := oldMDS.AddReplacement(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.ID != replacementMDS {
		t.Fatalf("the replacement MDS is %s, want %s", replacement.ID, replacementMDS)
	}
	if err := oldMDS.RemoveStopped(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	addStale("mds."+before.Name, before.Addrs)
	afterMDS := observe([]string{"a", "c"}, []int{1, 2}, "a", replacementMDS)
	if afterMDS.mds.GID == before.GID {
		t.Fatalf("the replacement MDS has the old GID %d", before.GID)
	}
	// The MDS kept rank 0 and its incarnation through the MON and OSD
	// replacements; the replacement took rank 0 in a later incarnation.
	for _, step := range []dualStackTopology{initial, afterMon} {
		if step.mds.GID != before.GID || step.mds.Incarnation != before.Incarnation {
			t.Fatalf("mds.%s changed from gid %d incarnation %d to gid %d incarnation %d before its replacement",
				before.Name, step.mds.GID, step.mds.Incarnation, before.GID, before.Incarnation)
		}
	}
	if before.Rank != 0 || afterMDS.mds.Rank != 0 || afterMDS.mds.Incarnation <= before.Incarnation {
		t.Fatalf("rank %d incarnation %d before the replacement, rank %d incarnation %d after",
			before.Rank, before.Incarnation, afterMDS.mds.Rank, afterMDS.mds.Incarnation)
	}
	reused["mds."+replacementMDS] = checkNewAddresses(t, "mds."+before.Name, "mds."+replacementMDS, before.Addrs, afterMDS.mds.Addrs)
	if afterMDS.fsEpoch <= afterOSD.fsEpoch {
		t.Fatalf("FSMap epoch %d after the replacement, %d before", afterMDS.fsEpoch, afterOSD.fsEpoch)
	}
	steps = append(steps, fmt.Sprintf("mds.%s(gid %d rank %d incarnation %d)%s->mds.%s(gid %d rank %d incarnation %d)%s fsmap_e%d->e%d",
		before.Name, before.GID, before.Rank, before.Incarnation, vectorString(before.Addrs),
		afterMDS.mds.Name, afterMDS.mds.GID, afterMDS.mds.Rank, afterMDS.mds.Incarnation, vectorString(afterMDS.mds.Addrs), afterOSD.fsEpoch, afterMDS.fsEpoch))
	round(3, afterMDS)

	// MGR: add mgr.b as a standby, then remove the active mgr.a. Its
	// admission check reads every MON, MGR and OSD vector.
	oldMgr := *afterMDS.addresses.ActiveManager
	statusBefore, err := observeCluster(cluster).ManagerStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.AddManager(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	if err := managerHandle(t, ctx, cluster, "a").Remove(ctx); err != nil {
		t.Fatal(err)
	}
	addStale("mgr.a", oldMgr.Addrs)
	afterMgr := observe([]string{"a", "c"}, []int{1, 2}, "b", replacementMDS)
	if afterMgr.mds.GID != afterMDS.mds.GID || afterMgr.mds.Incarnation != afterMDS.mds.Incarnation {
		t.Fatalf("mds.%s changed from gid %d incarnation %d to gid %d incarnation %d during the MGR replacement",
			replacementMDS, afterMDS.mds.GID, afterMDS.mds.Incarnation, afterMgr.mds.GID, afterMgr.mds.Incarnation)
	}
	statusAfter, err := observeCluster(cluster).ManagerStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if statusAfter.ActiveName != "b" || statusAfter.ActiveGID == statusBefore.ActiveGID {
		t.Fatalf("active MGR %s gid %d, before %s gid %d", statusAfter.ActiveName, statusAfter.ActiveGID, statusBefore.ActiveName, statusBefore.ActiveGID)
	}
	reused["mgr.b"] = checkNewAddresses(t, "mgr.a", "mgr.b", oldMgr.Addrs, afterMgr.addresses.ActiveManager.Addrs)
	if afterMgr.addresses.MgrMapEpoch <= afterMDS.addresses.MgrMapEpoch {
		t.Fatalf("mgrmap epoch %d after the replacement, %d before", afterMgr.addresses.MgrMapEpoch, afterMDS.addresses.MgrMapEpoch)
	}
	steps = append(steps, fmt.Sprintf("mgr.a(gid %d)%s->mgr.b(gid %d)%s mgrmap_e%d->e%d",
		statusBefore.ActiveGID, vectorString(oldMgr.Addrs), statusAfter.ActiveGID, vectorString(afterMgr.addresses.ActiveManager.Addrs), afterMDS.addresses.MgrMapEpoch, afterMgr.addresses.MgrMapEpoch))
	round(4, afterMgr)
	retained.release(t, ctx)

	// The sockets each replacement listens on. They are not checked: the
	// MGR binds its IPv6 listener to [::] although it advertises a concrete
	// IPv6 address (see the dual-stack section of docs/ceph.md).
	listening := map[string][]string{
		"mon.c":                 listeningSockets(t, ctx, monitorContainer(t, cluster, "c")),
		"osd.2":                 listeningSockets(t, ctx, osdContainer(t, cluster, 2)),
		"mds." + replacementMDS: listeningSockets(t, ctx, mdsContainer(t, cluster, replacementMDS)),
		"mgr.b":                 listeningSockets(t, ctx, managerContainer(t, cluster, "b")),
	}

	// A new process reads every file and the last round's objects.
	code, reader, err := client.Exec(ctx, append([]string{"timeout", "120", "python3", "-c", dualStackFreshRead, filesystem, "retained", "5", pool, "4"}, lastObjects...), tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, _ := io.ReadAll(reader)
	if code != 0 {
		t.Fatalf("fresh readback: exit %d: %s", code, output)
	}
	t.Logf("DUAL_STACK_REPLACEMENT_RECEIPT image=%s ceph=%s fsid=%s steps=%q fresh_mon_c=%v retained=%q fresh_read=%q listening=%v reused_ip=%v",
		staticFacts(cluster).Images.Essential, staticFacts(cluster).CephVersion, fsid, steps, freshMon, rounds, strings.TrimSpace(string(output)), listening, reused)
}
